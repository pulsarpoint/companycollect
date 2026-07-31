CZ_LEGAL_FORMS_TABLE = "cz_legal_forms"

CZ_LEGAL_FORMS_TABLES = (CZ_LEGAL_FORMS_TABLE,)

# Column order is the contract with migration 000232 and is asserted by
# tests/test_clickhouse_migrations.py.
CZ_LEGAL_FORMS_COLUMNS = (
    "code",
    "label_cs",
    "valid_from",
    "valid_to",
    "source_url",
    "source_run_id",
    "retrieved_at",
)

CZ_LEGAL_FORMS_TABLE_COLUMNS = {
    CZ_LEGAL_FORMS_TABLE: CZ_LEGAL_FORMS_COLUMNS,
}

CZ_LEGAL_FORMS_DUCKDB_SCHEMA = "czech_legal_forms"
