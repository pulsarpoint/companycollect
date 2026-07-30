CPV_VOCABULARY_TABLE = "cpv_vocabulary"

CPV_VOCABULARY_TABLES = (CPV_VOCABULARY_TABLE,)

# Column order is the contract with migration 000220 and is asserted by
# tests/test_clickhouse_migrations.py.
CPV_VOCABULARY_COLUMNS = (
    "code",
    "label_en",
    "significant_digits",
    "parent_code",
    "source_url",
    "source_run_id",
    "retrieved_at",
)

CPV_VOCABULARY_TABLE_COLUMNS = {
    CPV_VOCABULARY_TABLE: CPV_VOCABULARY_COLUMNS,
}

CPV_VOCABULARY_DUCKDB_SCHEMA = "cpv_vocabulary"

# 9,454 in CPV 2008. Held as a floor rather than an equality so a future
# revision of the regulation does not fail the load, while a truncated or
# partial download still does.
MIN_CPV_VOCABULARY_ROWS = 9_000
