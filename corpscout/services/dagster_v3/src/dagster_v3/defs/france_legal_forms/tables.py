FR_LEGAL_FORMS_TABLE = "fr_legal_forms"

FR_LEGAL_FORMS_TABLES = (FR_LEGAL_FORMS_TABLE,)

# Column order is the contract with migration 000229 and is asserted by
# tests/test_clickhouse_migrations.py.
FR_LEGAL_FORMS_COLUMNS = (
    "code",
    "level",
    "label_fr",
    "parent_code",
    "source_url",
    "source_run_id",
    "retrieved_at",
)

FR_LEGAL_FORMS_TABLE_COLUMNS = {
    FR_LEGAL_FORMS_TABLE: FR_LEGAL_FORMS_COLUMNS,
}

FR_LEGAL_FORMS_DUCKDB_SCHEMA = "france_legal_forms"
