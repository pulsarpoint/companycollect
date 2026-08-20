from pathlib import Path


MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"


def test_sweden_financial_metrics_provenance_migration_adds_source_links_and_fact_view() -> (
    None
):
    up_sql = _migration_sql("000134_corpscout_se_financial_metrics_provenance.up.sql")
    down_sql = _migration_sql(
        "000134_corpscout_se_financial_metrics_provenance.down.sql"
    )

    for column in (
        "source_archive_url",
        "source_archive_key",
        "source_archive_name",
        "nested_zip_name",
        "xhtml_object_key",
        "xhtml_source_uri",
        "taxonomy_entrypoint",
        "current_receivables_amount_original",
        "current_receivables_amount_usd",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column}" in up_sql
        assert f"DROP COLUMN IF EXISTS {column}" in down_sql

    assert "CREATE OR REPLACE VIEW corpscout.se_financial_facts_with_source" in up_sql
    assert "FROM corpscout.se_financial_facts AS facts" in up_sql
    assert "LEFT ANY JOIN corpscout.se_financial_reports AS reports" in up_sql
    assert "s3://source-sweden-financial/" in up_sql
    assert "DROP VIEW IF EXISTS corpscout.se_financial_facts_with_source" in down_sql


def test_sweden_financial_metrics_unified_years_migration_replaces_history_table() -> (
    None
):
    up_sql = _migration_sql(
        "000284_corpscout_se_financial_metrics_unified_years.up.sql"
    )
    down_sql = _migration_sql(
        "000284_corpscout_se_financial_metrics_unified_years.down.sql"
    )

    assert "ALTER TABLE corpscout.se_financial_metrics" in up_sql
    assert "ADD COLUMN IF NOT EXISTS observation_kind" in up_sql
    assert "ADD COLUMN IF NOT EXISTS source_fiscal_year" in up_sql
    assert "DROP TABLE IF EXISTS corpscout.se_financial_history" in up_sql

    assert "CREATE TABLE IF NOT EXISTS corpscout.se_financial_history" in down_sql
    assert "DROP COLUMN IF EXISTS source_fiscal_year" in down_sql
    assert "DROP COLUMN IF EXISTS observation_kind" in down_sql


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text(encoding="utf-8")
