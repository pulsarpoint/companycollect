from pathlib import Path

from dagster_v3.defs.brazil_financial.cvm import tables as brazil_cvm_tables

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"


def test_brazil_fin_cvm_dfp_tables_migration_covers_exported_columns() -> None:
    sql = _migration_sql("000087_corpscout_br_cvm_dfp_tables.up.sql")
    down_sql = _migration_sql("000087_corpscout_br_cvm_dfp_tables.down.sql")

    table_columns = {
        brazil_cvm_tables.QUALIFIED_BR_CVM_DFP_DOCUMENTS_TABLE: (
            brazil_cvm_tables.BR_CVM_DFP_DOCUMENTS_EXPORT_COLUMNS
        ),
        brazil_cvm_tables.QUALIFIED_BR_CVM_DFP_STATEMENT_ROWS_TABLE: (
            brazil_cvm_tables.BR_CVM_DFP_STATEMENT_ROWS_EXPORT_COLUMNS
        ),
        brazil_cvm_tables.QUALIFIED_BR_CVM_DFP_CAPITAL_COMPOSITION_TABLE: (
            brazil_cvm_tables.BR_CVM_DFP_CAPITAL_COMPOSITION_EXPORT_COLUMNS
        ),
        brazil_cvm_tables.QUALIFIED_BR_CVM_DFP_AUDITOR_REPORTS_TABLE: (
            brazil_cvm_tables.BR_CVM_DFP_AUDITOR_REPORTS_EXPORT_COLUMNS
        ),
    }

    for table_name, column_names in table_columns.items():
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in sql
        for column_name in column_names:
            assert f"    {column_name} " in sql, (
                f"missing {column_name} in {table_name}"
            )
        assert f"DROP TABLE IF EXISTS {table_name}" in down_sql

    assert sql.count("ENGINE = ReplacingMergeTree(resolved_at)") == 4
    assert "ORDER BY (cnpj, reference_date, version, document_id)" in sql
    assert "ORDER BY (cnpj, reference_date, version)" in sql
    assert (
        "ORDER BY (cnpj, reference_date, version, opinion_statement_type, "
        "opinion_item_number)"
    ) in sql
    assert "ifNull(period_end_date, toDate32('1970-01-01'))" in sql
    assert "amount_original Nullable(Decimal(38, 10))" in sql
    assert "amount_usd Nullable(Decimal(38, 6))" in sql
    assert "fx_rate_to_usd Nullable(Decimal(38, 12))" in sql
    assert "fx_rate_date Nullable(Date)" in sql
    assert "fx_source String" in sql
    assert "report_text_original String" in sql


def test_brazil_fin_cvm_companies_migration_covers_exported_columns() -> None:
    sql = _migration_sql("000091_corpscout_br_cvm_companies.up.sql")
    down_sql = _migration_sql("000091_corpscout_br_cvm_companies.down.sql")

    assert (
        f"CREATE TABLE IF NOT EXISTS {brazil_cvm_tables.QUALIFIED_BR_CVM_COMPANIES_TABLE}"
        in sql
    )
    for column_name in brazil_cvm_tables.BR_CVM_COMPANIES_EXPORT_COLUMNS:
        assert f"    {column_name} " in sql, (
            f"missing {column_name} in "
            f"{brazil_cvm_tables.QUALIFIED_BR_CVM_COMPANIES_TABLE}"
        )
    assert (
        f"DROP TABLE IF EXISTS {brazil_cvm_tables.QUALIFIED_BR_CVM_COMPANIES_TABLE}"
        in down_sql
    )
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql
    assert "ORDER BY (cnpj, cvm_code)" in sql
    assert "source_url String" in sql
    assert "registration_date Nullable(Date32)" in sql
    assert "auditor_cnpj String" in sql


def test_brazil_fin_cvm_itr_tables_migration_covers_exported_columns() -> None:
    sql = _migration_sql("000092_corpscout_br_cvm_itr_tables.up.sql")
    down_sql = _migration_sql("000092_corpscout_br_cvm_itr_tables.down.sql")

    table_columns = {
        brazil_cvm_tables.QUALIFIED_BR_CVM_ITR_DOCUMENTS_TABLE: (
            brazil_cvm_tables.BR_CVM_ITR_DOCUMENTS_EXPORT_COLUMNS
        ),
        brazil_cvm_tables.QUALIFIED_BR_CVM_ITR_STATEMENT_ROWS_TABLE: (
            brazil_cvm_tables.BR_CVM_ITR_STATEMENT_ROWS_EXPORT_COLUMNS
        ),
        brazil_cvm_tables.QUALIFIED_BR_CVM_ITR_CAPITAL_COMPOSITION_TABLE: (
            brazil_cvm_tables.BR_CVM_ITR_CAPITAL_COMPOSITION_EXPORT_COLUMNS
        ),
        brazil_cvm_tables.QUALIFIED_BR_CVM_ITR_AUDITOR_REPORTS_TABLE: (
            brazil_cvm_tables.BR_CVM_ITR_AUDITOR_REPORTS_EXPORT_COLUMNS
        ),
    }

    for table_name, column_names in table_columns.items():
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in sql
        for column_name in column_names:
            assert f"    {column_name} " in sql, (
                f"missing {column_name} in {table_name}"
            )
        assert f"DROP TABLE IF EXISTS {table_name}" in down_sql

    assert sql.count("ENGINE = ReplacingMergeTree(resolved_at)") == 4
    assert "ORDER BY (cnpj, reference_date, version, document_id)" in sql
    assert "ORDER BY (cnpj, reference_date, version)" in sql
    assert "ifNull(period_end_date, toDate32('1970-01-01'))" in sql
    assert "itr_year UInt16" in sql
    assert "amount_usd Nullable(Decimal(38, 6))" in sql


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text()
