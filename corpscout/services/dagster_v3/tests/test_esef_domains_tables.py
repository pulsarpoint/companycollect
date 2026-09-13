"""corpscout.esef_domains (migration 000404): the export-column tuple pins the
CREATE TABLE's column order, the view entry renders into the same migration."""

import re
from pathlib import Path

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.country_views import build_se_esef_view_sql

MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
UP = MIGRATIONS / "000404_corpscout_esef_domains.up.sql"
DOWN = MIGRATIONS / "000404_corpscout_esef_domains.down.sql"


def _normalized(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip().rstrip(";")


def test_export_columns_are_the_eighteen_insert_columns() -> None:
    assert tables.ESEF_DOMAINS_TABLE == "esef_domains"
    assert tables.QUALIFIED_ESEF_DOMAINS_TABLE == "corpscout.esef_domains"
    assert tables.ESEF_DOMAINS_EXPORT_COLUMNS == (
        "domain_id",
        "source_document_id",
        "package_sha256",
        "lei",
        "period_end",
        "fiscal_year",
        "extraction_status",
        "registrable_domain",
        "hosts_json",
        "normalized_urls_json",
        "roles_json",
        "evidence_json",
        "evidence_count",
        "corroborated",
        "error_message",
        "extractor_version",
        "source_run_id",
        "extracted_at",
    )


def test_migration_declares_the_table_columns_in_export_order() -> None:
    sql = UP.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS corpscout.esef_domains" in sql
    positions = [sql.index(f"\n    {column} ") for column in tables.ESEF_DOMAINS_EXPORT_COLUMNS]
    assert positions == sorted(positions)
    assert "\n    source_record_uid String DEFAULT lower(hex(SHA256(concat(" in sql
    assert "\n    resolved_at DateTime64(3) DEFAULT now64(3)" in sql
    assert "ENGINE = ReplacingMergeTree(extracted_at)" in sql
    assert "ORDER BY (lei, source_document_id, registrable_domain)" in sql


def test_migration_embeds_the_rendered_view_and_the_down_drops_both() -> None:
    view = next(v for v in tables.SE_ESEF_VIEWS if v.table == "esef_domains")
    assert view.view == "se_esef_domains"
    assert view.final is True
    assert view.columns == (
        "domain_id",
        "source_document_id",
        "source_record_uid",
        *tables.ESEF_DOMAINS_EXPORT_COLUMNS[2:],
        "resolved_at",
    )
    assert _normalized(build_se_esef_view_sql(view)) in _normalized(UP.read_text(encoding="utf-8"))
    down = DOWN.read_text(encoding="utf-8")
    assert "DROP VIEW IF EXISTS corpscout.se_esef_domains" in down
    assert "DROP TABLE IF EXISTS corpscout.esef_domains" in down
