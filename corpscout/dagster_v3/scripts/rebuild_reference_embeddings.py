"""Rebuild the CommonCrawl reference embeddings against the CURRENT embedding endpoint.

Self-contained: reads NACE source text straight from ClickHouse `corpscout.nace_categories`
(no local `nace_source.duckdb` needed), embeds via `EmbeddingClient.from_env()` (the DGX
endpoint in `COMMONCRAWL_EMBED_BASE_URL`), embeds the committed page-type seed, and atomically
swaps both reference tables back into ClickHouse:

    corpscout.nace_category_embeddings   (migration 000044)
    corpscout.page_type_exemplars        (migration 000045)

Run this whenever the served embedding model/precision changes (e.g. bf16 -> NVFP4) so the
reference vectors match the vectors the worker produces for pages. Both must come from the
same model. Run from a host that reaches BOTH the DGX endpoint and ClickHouse (e.g. the EC2
worker box):

    set -a; . ./.env; set +a
    uv run python scripts/rebuild_reference_embeddings.py

The schema is owned by the migrations; this only replaces row data. Stage+EXCHANGE keeps the
swap atomic (no empty window for a worker that loads the reference at startup).
"""
import os
from datetime import datetime, timezone

from clickhouse_driver import Client

from commoncrawl_enrich import nace_embed
from dagster_v3.defs.commoncrawl_classify import tables

# Source text for the NACE matrix, read from ClickHouse instead of the local DuckDB stage.
# Mirrors commoncrawl_classify/build.py:_NACE_SQL (self-joins for section/parent description).
_NACE_SQL = """
    select c.code, c.level,
           coalesce(c.section_code, '') as section_code,
           coalesce(c.parent_code, '') as parent_code,
           c.description_en as label,
           sec.description_en as section_desc,
           par.description_en as parent_desc,
           coalesce(c.classification_version, '') as classification_version
    from corpscout.nace_categories c
    left join corpscout.nace_categories sec
           on sec.code = c.section_code and sec.is_current
    left join corpscout.nace_categories par
           on par.code = c.parent_code and par.is_current
    where c.is_current and coalesce(c.description_en, '') <> ''
      and c.level <> 'section'
    order by c.code
"""


def _ch_client() -> Client:
    secure = os.environ.get("CLICKHOUSE_SECURE", "").lower() in {"1", "true", "yes", "on"}
    return Client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_NATIVE_PORT", "9002")),
        user=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=tables.DATABASE,
        secure=secure,
    )


def _atomic_replace(client: Client, table: str, columns: tuple[str, ...], rows: list[tuple]) -> None:
    """Insert into a fresh stage clone, then EXCHANGE — no empty window on the live table."""
    qualified = f"{tables.DATABASE}.{table}"
    stage = f"{qualified}_rebuild_stage"
    client.execute(f"DROP TABLE IF EXISTS {stage}")
    client.execute(f"CREATE TABLE {stage} AS {qualified}")
    try:
        collist = ", ".join(columns)
        client.execute(f"INSERT INTO {stage} ({collist}) VALUES", rows)
        client.execute(f"EXCHANGE TABLES {qualified} AND {stage}")
    finally:
        client.execute(f"DROP TABLE IF EXISTS {stage}")


def rebuild_nace(client: Client, embedder, *, model: str, run_id: str, now: datetime) -> int:
    src = client.execute(_NACE_SQL)
    if not src:
        raise ValueError("no NACE categories in corpscout.nace_categories")
    texts = [
        nace_embed.reference_text((code, label, section_desc, parent_desc),
                                  tables.DEFAULT_NACE_VARIANT)
        for (code, _lvl, _sec, _par, label, section_desc, parent_desc, _cv) in src
    ]
    matrix = embedder.embed(texts)  # documents: no instruction prefix, L2-normalized
    dim = int(matrix.shape[1])
    rows = [
        (code, level, section_code, parent_code, nace_embed.division(code), label,
         text, matrix[i].tolist(), dim, model, tables.DEFAULT_NACE_VARIANT,
         classification_version, run_id, now)
        for i, ((code, level, section_code, parent_code, label, _sd, _pd,
                 classification_version), text) in enumerate(zip(src, texts))
    ]
    _atomic_replace(client, tables.NACE_EMBEDDINGS_TABLE, tables.NACE_EMBEDDINGS_COLUMNS, rows)
    print(f"nace_category_embeddings: {len(rows)} rows  dim={dim}  model={model}")
    return len(rows)


def rebuild_page_types(client: Client, embedder, *, model: str, run_id: str, now: datetime) -> int:
    seed_lines = tables.PAGE_TYPE_SEED_PATH.read_text().splitlines()
    import json
    seed = [json.loads(s) for s in seed_lines if s.strip()]
    if not seed:
        raise ValueError("empty page-type seed")
    texts = [r["text"] for r in seed]
    matrix = embedder.embed(texts)
    dim = int(matrix.shape[1])
    rows = [
        (r["page_type"], r.get("root_domain", ""), r.get("source_url", ""), "keyword",
         text, matrix[i].tolist(), dim, model, run_id, now)
        for i, (r, text) in enumerate(zip(seed, texts))
    ]
    _atomic_replace(client, tables.PAGE_TYPE_EXEMPLARS_TABLE, tables.PAGE_TYPE_EXEMPLARS_COLUMNS, rows)
    print(f"page_type_exemplars: {len(rows)} rows  dim={dim}  model={model}")
    return len(rows)


def main() -> None:
    embedder = nace_embed.EmbeddingClient.from_env()
    model = embedder.model
    now = datetime.now(timezone.utc)
    run_id = f"rebuild-{now:%Y%m%dT%H%M%SZ}"
    client = _ch_client()
    print(f"endpoint model={model}  run_id={run_id}")
    rebuild_nace(client, embedder, model=model, run_id=run_id, now=now)
    rebuild_page_types(client, embedder, model=model, run_id=run_id, now=now)
    print("done — reference tables rebuilt with the current endpoint model.")


if __name__ == "__main__":
    main()
