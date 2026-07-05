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
    sql = _migration_sql("000094_corpscout_br_cvm_itr_tables.up.sql")
    down_sql = _migration_sql("000094_corpscout_br_cvm_itr_tables.down.sql")

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


def test_brazil_fin_cvm_fre_tables_migration_covers_exported_columns() -> None:
    sql = _migration_sql("000100_corpscout_br_cvm_fre_tables.up.sql")
    down_sql = _migration_sql("000100_corpscout_br_cvm_fre_tables.down.sql")

    table_columns = {
        brazil_cvm_tables.QUALIFIED_BR_CVM_FRE_DOCUMENTS_TABLE: (
            brazil_cvm_tables.BR_CVM_FRE_DOCUMENTS_EXPORT_COLUMNS
        ),
        brazil_cvm_tables.QUALIFIED_BR_CVM_FRE_CAPITAL_SOCIAL_TABLE: (
            brazil_cvm_tables.BR_CVM_FRE_CAPITAL_SOCIAL_EXPORT_COLUMNS
        ),
        brazil_cvm_tables.QUALIFIED_BR_CVM_FRE_CAPITAL_SOCIAL_CLASSES_TABLE: (
            brazil_cvm_tables.BR_CVM_FRE_CAPITAL_SOCIAL_CLASSES_EXPORT_COLUMNS
        ),
        brazil_cvm_tables.QUALIFIED_BR_CVM_FRE_CAPITAL_DISTRIBUTION_TABLE: (
            brazil_cvm_tables.BR_CVM_FRE_CAPITAL_DISTRIBUTION_EXPORT_COLUMNS
        ),
        brazil_cvm_tables.QUALIFIED_BR_CVM_FRE_AUDITORS_TABLE: (
            brazil_cvm_tables.BR_CVM_FRE_AUDITORS_EXPORT_COLUMNS
        ),
        brazil_cvm_tables.QUALIFIED_BR_CVM_FRE_RESPONSIBLES_TABLE: (
            brazil_cvm_tables.BR_CVM_FRE_RESPONSIBLES_EXPORT_COLUMNS
        ),
        brazil_cvm_tables.QUALIFIED_BR_CVM_FRE_RELATED_PARTY_TRANSACTIONS_TABLE: (
            brazil_cvm_tables.BR_CVM_FRE_RELATED_PARTY_TRANSACTIONS_EXPORT_COLUMNS
        ),
        brazil_cvm_tables.QUALIFIED_BR_CVM_FRE_REMUNERATION_TOTAL_ORGANS_TABLE: (
            brazil_cvm_tables.BR_CVM_FRE_REMUNERATION_TOTAL_ORGANS_EXPORT_COLUMNS
        ),
        brazil_cvm_tables.QUALIFIED_BR_CVM_FRE_SHAREHOLDERS_TABLE: (
            brazil_cvm_tables.BR_CVM_FRE_SHAREHOLDERS_EXPORT_COLUMNS
        ),
    }

    for table_name, column_names in table_columns.items():
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in sql
        for column_name in column_names:
            assert f"    {column_name} " in sql, (
                f"missing {column_name} in {table_name}"
            )
        assert f"DROP TABLE IF EXISTS {table_name}" in down_sql

    assert sql.count("ENGINE = ReplacingMergeTree(resolved_at)") == 9
    assert "fre_year UInt16" in sql
    assert "capital_amount Nullable(Decimal(38, 6))" in sql
    assert "transaction_amount Nullable(Decimal(38, 6))" in sql
    assert "total_remuneration Nullable(Decimal(38, 6))" in sql
    assert "ORDER BY (cnpj, reference_date, version, document_id)" in sql
    assert (
        "ORDER BY (cnpj, reference_date, version, related_party, "
        "coalesce(data_transaction, toDate32('1900-01-01')), source_record_id)" in sql
    )
    assert (
        "ORDER BY (cnpj, reference_date, version, document_id, administration_body, "
        "coalesce(fiscal_year_end_date, toDate32('1900-01-01')), source_record_id)"
        in sql
    )


def test_brazil_fin_cvm_financial_metrics_migration_creates_view() -> None:
    sql = _migration_sql("000095_corpscout_br_cvm_financial_metrics.up.sql")
    down_sql = _migration_sql("000095_corpscout_br_cvm_financial_metrics.down.sql")

    assert (
        f"CREATE VIEW IF NOT EXISTS "
        f"{brazil_cvm_tables.QUALIFIED_BR_CVM_FINANCIAL_METRICS_TABLE}" in sql
    )
    for column_name in brazil_cvm_tables.BR_CVM_FINANCIAL_METRICS_EXPORT_COLUMNS:
        assert f" as {column_name}" in sql or f" AS {column_name}" in sql, (
            f"missing {column_name} in "
            f"{brazil_cvm_tables.QUALIFIED_BR_CVM_FINANCIAL_METRICS_TABLE}"
        )
    assert (
        f"DROP VIEW IF EXISTS "
        f"{brazil_cvm_tables.QUALIFIED_BR_CVM_FINANCIAL_METRICS_TABLE}" in down_sql
    )
    assert (
        f"DROP TABLE IF EXISTS "
        f"{brazil_cvm_tables.QUALIFIED_BR_CVM_FINANCIAL_METRICS_TABLE}" in sql
    )
    lower_sql = sql.lower()
    assert "from corpscout.br_cvm_dfp_statement_rows" in lower_sql
    assert "from corpscout.br_cvm_itr_statement_rows" in lower_sql
    assert "union all" in lower_sql
    assert "metric_name" in sql
    assert "is_latest_version" in sql
    assert "ENGINE = ReplacingMergeTree" not in sql
    assert "FROM statement_rows AS source_rows" in sql
    assert "source_rows.amount_original IS NOT NULL" in sql
    assert "source_rows.currency != ''" in sql
    assert "FROM statement_rows AS liability_source_rows" in sql
    assert "liability_source_rows.amount_original IS NOT NULL" in sql
    assert "liability_source_rows.currency != ''" in sql
    assert "groupArray(liability_source_rows.source_run_id)" in sql
    assert "sum(liability_source_rows.amount_original)" in sql
    assert "groupArray(tuple(liability_source_rows.account_code" in sql
    assert "WHERE amount_original IS NOT NULL" not in sql
    assert "AND amount_original IS NOT NULL" not in sql


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text()
