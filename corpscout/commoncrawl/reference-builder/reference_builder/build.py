"""Build the CommonCrawl reference embeddings into ClickHouse.

Reads NACE source text straight from `corpscout.nace_categories` (populated by the dagster
`nace` source), embeds via the env-configured endpoint, embeds the committed page-type seed,
and atomically swaps both reference tables:

    corpscout.nace_category_embeddings   (migration 000044)
    corpscout.page_type_exemplars        (migration 000045)

Run whenever the served embedding model changes, so the reference vectors match the vectors
the Go worker produces for pages — both MUST come from the same model. The schema is owned by
the migrations; this only replaces row data. Stage+EXCHANGE keeps the swap atomic.
"""
import json
import os
from datetime import datetime
from pathlib import Path

from clickhouse_driver import Client

from .embed import division, reference_text

DATABASE = "corpscout"
NACE_EMBEDDINGS_TABLE = "nace_category_embeddings"
PAGE_TYPE_EXEMPLARS_TABLE = "page_type_exemplars"
DEFAULT_NACE_VARIANT = "hier"
PAGE_TYPE_SEED_PATH = Path(__file__).parent / "seeds" / "page_type_exemplars.jsonl"

# Export column order — MUST match migrations 000044/000045 exactly.
NACE_EMBEDDINGS_COLUMNS = (
    "code", "level", "section_code", "parent_code", "division", "label",
    "embedding_text", "embedding", "embedding_dim", "embedding_model",
    "embedding_variant", "classification_version", "source_run_id", "resolved_at",
)
PAGE_TYPE_EXEMPLARS_COLUMNS = (
    "page_type", "root_domain", "source_url", "signal_source", "text",
    "embedding", "embedding_dim", "embedding_model", "source_run_id", "resolved_at",
)

# Self-joins for the section/parent description (the 'hier' embedding text).
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


def ch_client() -> Client:
    secure = os.environ.get("CLICKHOUSE_SECURE", "").lower() in {"1", "true", "yes", "on"}
    return Client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_NATIVE_PORT", "9002")),
        user=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=DATABASE,
        secure=secure,
    )


def _atomic_replace(client, table: str, columns: tuple, rows: list) -> None:
    """Insert into a fresh stage clone, then EXCHANGE — no empty window on the live table."""
    qualified = f"{DATABASE}.{table}"
    stage = f"{qualified}_rebuild_stage"
    client.execute(f"DROP TABLE IF EXISTS {stage}")
    client.execute(f"CREATE TABLE {stage} AS {qualified}")
    try:
        collist = ", ".join(columns)
        client.execute(f"INSERT INTO {stage} ({collist}) VALUES", rows)
        client.execute(f"EXCHANGE TABLES {qualified} AND {stage}")
    finally:
        client.execute(f"DROP TABLE IF EXISTS {stage}")


def rebuild_nace(client, embedder, *, model: str, run_id: str, now: datetime) -> int:
    src = client.execute(_NACE_SQL)
    if not src:
        raise ValueError("no NACE categories in corpscout.nace_categories")
    texts = [
        reference_text((code, label, section_desc, parent_desc), DEFAULT_NACE_VARIANT)
        for (code, _lvl, _sec, _par, label, section_desc, parent_desc, _cv) in src
    ]
    matrix = embedder.embed(texts)  # documents: no instruction prefix, L2-normalized
    dim = int(matrix.shape[1])
    rows = [
        (code, level, section_code, parent_code, division(code), label,
         text, matrix[i].tolist(), dim, model, DEFAULT_NACE_VARIANT,
         classification_version, run_id, now)
        for i, ((code, level, section_code, parent_code, label, _sd, _pd,
                 classification_version), text) in enumerate(zip(src, texts))
    ]
    _atomic_replace(client, NACE_EMBEDDINGS_TABLE, NACE_EMBEDDINGS_COLUMNS, rows)
    print(f"nace_category_embeddings: {len(rows)} rows  dim={dim}  model={model}")
    return len(rows)


def rebuild_page_types(client, embedder, *, model: str, run_id: str, now: datetime) -> int:
    seed = [json.loads(s) for s in PAGE_TYPE_SEED_PATH.read_text().splitlines() if s.strip()]
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
    _atomic_replace(client, PAGE_TYPE_EXEMPLARS_TABLE, PAGE_TYPE_EXEMPLARS_COLUMNS, rows)
    print(f"page_type_exemplars: {len(rows)} rows  dim={dim}  model={model}")
    return len(rows)
