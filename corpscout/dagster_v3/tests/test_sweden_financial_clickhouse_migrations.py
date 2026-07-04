from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"

REPORT_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "statement_key",
    "company_id",
    "report_period_start",
    "report_period_end",
    "fiscal_year",
    "reported_company_name",
    "report_language",
    "source_archive_key",
    "source_archive_name",
    "nested_zip_name",
    "xhtml_object_key",
    "xhtml_sha256",
    "xhtml_size_bytes",
    "taxonomy_entrypoint",
    "schema_refs",
    "contexts_count",
    "units_count",
    "facts_count",
    "parser_version",
    "source_payload_hash",
    "resolved_at",
)

FACT_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "statement_key",
    "company_id",
    "report_period_end",
    "fact_ordinal",
    "concept_qname",
    "concept_namespace",
    "concept_local_name",
    "context_id",
    "unit_id",
    "decimals",
    "precision",
    "value_kind",
    "raw_value",
    "amount_original",
    "amount_usd",
    "date_value",
    "text_value",
    "currency",
    "dimensions",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "parser_version",
    "source_payload_hash",
    "resolved_at",
)

METRIC_AMOUNT_NAMES = (
    "revenue",
    "operating_profit_loss",
    "profit_loss",
    "total_assets",
    "equity",
    "liabilities",
    "cash_and_bank",
    "current_assets",
    "current_liabilities",
    "personnel_expenses",
    "wages_and_salaries",
)

METRIC_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "statement_key",
    "company_id",
    "report_period_start",
    "report_period_end",
    "fiscal_year",
    "reported_company_name",
    "currency",
    *(
        column_name
        for metric_name in METRIC_AMOUNT_NAMES
        for column_name in (
            f"{metric_name}_amount_original",
            f"{metric_name}_amount_usd",
        )
    ),
    "employees",
    "source_fact_count",
    "mapped_fact_count",
    "unmapped_numeric_fact_count",
    "metric_warnings",
    "mapping_version",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "source_payload_hash",
    "resolved_at",
)


def test_sweden_financial_tables_migration_covers_reports_facts_and_usd_metrics() -> None:
    sql = _migration_sql("000088_corpscout_se_financial_tables.up.sql")
    down_sql = _migration_sql("000088_corpscout_se_financial_tables.down.sql")

    expected_columns_by_table = {
        "corpscout.se_financial_reports": REPORT_COLUMNS,
        "corpscout.se_financial_facts": FACT_COLUMNS,
        "corpscout.se_financial_metrics": METRIC_COLUMNS,
    }

    for table_name, column_names in expected_columns_by_table.items():
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in sql
        assert f"DROP TABLE IF EXISTS {table_name}" in down_sql
        for column_name in column_names:
            assert f"    {column_name} " in sql, f"missing {column_name} in {table_name}"

    assert "amount_original Nullable(Decimal(38, 10))" in sql
    assert "amount_usd Nullable(Decimal(38, 10))" in sql

    for metric_name in METRIC_AMOUNT_NAMES:
        assert f"{metric_name}_amount_original Nullable(Decimal(38, 6))" in sql
        assert f"{metric_name}_amount_usd Nullable(Decimal(38, 6))" in sql

    assert sql.count("ENGINE = ReplacingMergeTree(resolved_at)") == 3
    assert "ORDER BY (company_id, report_period_end, statement_key)" in sql
    assert (
        "ORDER BY (company_id, report_period_end, statement_key, fact_ordinal)"
    ) in sql


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text()
