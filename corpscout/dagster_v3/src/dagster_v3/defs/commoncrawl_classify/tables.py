"""Constants for the CommonCrawl classification reference data.

Reference matrices are built in a per-source DuckDB file then exported to the
ClickHouse `corpscout` tables owned by migrations 000044/000045. Column tuples
must match the migration column order (pinned by a contract test).
"""
from pathlib import Path

DATABASE = "corpscout"

# Single-writer DuckDB file; stem differs from the schema name (DuckDB binder rule).
DUCKDB_PATH = Path("data/commoncrawl_classify_source.duckdb")
DUCKDB_SCHEMA = "commoncrawl_classify"
POOL = "commoncrawl_classify_duckdb"

NACE_EMBEDDINGS_TABLE = "nace_category_embeddings"
PAGE_TYPE_EXEMPLARS_TABLE = "page_type_exemplars"

# Source inputs
NACE_SOURCE_DUCKDB = Path("data/nace_source.duckdb")
PAGE_TYPE_SEED_PATH = Path(__file__).parent / "seeds" / "page_type_exemplars.jsonl"

DEFAULT_NACE_VARIANT = "hier"

QUALIFIED_NACE_EMBEDDINGS_TABLE = f"{DATABASE}.{NACE_EMBEDDINGS_TABLE}"
QUALIFIED_PAGE_TYPE_EXEMPLARS_TABLE = f"{DATABASE}.{PAGE_TYPE_EXEMPLARS_TABLE}"

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
