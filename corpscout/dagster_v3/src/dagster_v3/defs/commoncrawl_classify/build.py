"""Build the reference embedding matrices into the per-source DuckDB file.

Each builder embeds source text (NACE categories / committed page-type seed) via an
injected embedder and writes a DuckDB table whose columns match the ClickHouse
migration, with `embedding` as a `FLOAT[]` (-> ClickHouse `Array(Float32)`). The
embedder is injected so these are unit-testable with a fake; assets pass a live
`EmbeddingClient.from_env()`.
"""
import json
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from commoncrawl_enrich import nace_embed
from dagster_v3.defs.commoncrawl_classify import tables

# NACE source rows (current, non-section) + the hierarchy text for augmentation.
_NACE_SQL = """
    select c.code, c.level,
           coalesce(c.section_code, '') as section_code,
           coalesce(c.parent_code, '') as parent_code,
           c.description_en as label,
           sec.description_en as section_desc,
           par.description_en as parent_desc,
           coalesce(c.classification_version, '') as classification_version
    from nace_stage.nace_categories c
    left join nace_stage.nace_categories sec
           on sec.code = c.section_code and sec.is_current
    left join nace_stage.nace_categories par
           on par.code = c.parent_code and par.is_current
    where c.is_current and coalesce(c.description_en, '') <> ''
      and c.level <> 'section'
    order by c.code
"""


def _embedding_array(matrix) -> pa.Array:
    return pa.array([v.tolist() for v in matrix], type=pa.list_(pa.float32()))


def _write_table(duckdb_path: Path, schema: str, table: str, arrow: pa.Table) -> int:
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(duckdb_path)) as con:
        con.execute(f"create schema if not exists {schema}")
        con.register("incoming", arrow)
        con.execute(f"create or replace table {schema}.{table} as select * from incoming")
        con.unregister("incoming")
    return arrow.num_rows


def build_nace_embeddings_duckdb(
    duckdb_path: Path, embedder: nace_embed.Embedder, *,
    nace_source: Path = tables.NACE_SOURCE_DUCKDB, variant: str = tables.DEFAULT_NACE_VARIANT,
    embedding_model: str, source_run_id: str, resolved_at: datetime,
) -> int:
    with duckdb.connect(str(nace_source), read_only=True) as con:
        rows = con.execute(_NACE_SQL).fetchall()
    if not rows:
        raise ValueError("no NACE categories to embed")
    texts = [nace_embed.reference_text((r[0], r[4], r[5], r[6]), variant) for r in rows]
    matrix = embedder.embed(texts)  # documents: no instruction prefix, normalized
    dim = int(matrix.shape[1])
    n = len(rows)
    arrow = pa.table({
        "code": [r[0] for r in rows],
        "level": [r[1] for r in rows],
        "section_code": [r[2] for r in rows],
        "parent_code": [r[3] for r in rows],
        "division": [nace_embed.division(r[0]) for r in rows],
        "label": [r[4] for r in rows],
        "embedding_text": texts,
        "embedding": _embedding_array(matrix),
        "embedding_dim": pa.array([dim] * n, type=pa.uint32()),
        "embedding_model": [embedding_model] * n,
        "embedding_variant": [variant] * n,
        "classification_version": [r[7] for r in rows],
        "source_run_id": [source_run_id] * n,
        "resolved_at": pa.array([resolved_at] * n, type=pa.timestamp("us", tz="UTC")),
    })
    return _write_table(duckdb_path, tables.DUCKDB_SCHEMA, tables.NACE_EMBEDDINGS_TABLE, arrow)


def _read_seed(seed_path: Path) -> list[dict]:
    lines = Path(seed_path).read_text().splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def build_page_type_exemplars_duckdb(
    duckdb_path: Path, embedder: nace_embed.Embedder, *,
    seed_path: Path = tables.PAGE_TYPE_SEED_PATH,
    embedding_model: str, source_run_id: str, resolved_at: datetime,
) -> int:
    seed = _read_seed(seed_path)
    if not seed:
        raise ValueError("empty page-type seed")
    texts = [r["text"] for r in seed]
    matrix = embedder.embed(texts)  # same modality as pages, no instruction prefix
    dim = int(matrix.shape[1])
    n = len(seed)
    arrow = pa.table({
        "page_type": [r["page_type"] for r in seed],
        "root_domain": [r.get("root_domain", "") for r in seed],
        "source_url": [r.get("source_url", "") for r in seed],
        "signal_source": ["keyword"] * n,  # committed seed is keyword-mined
        "text": texts,
        "embedding": _embedding_array(matrix),
        "embedding_dim": pa.array([dim] * n, type=pa.uint32()),
        "embedding_model": [embedding_model] * n,
        "source_run_id": [source_run_id] * n,
        "resolved_at": pa.array([resolved_at] * n, type=pa.timestamp("us", tz="UTC")),
    })
    return _write_table(duckdb_path, tables.DUCKDB_SCHEMA, tables.PAGE_TYPE_EXEMPLARS_TABLE, arrow)
