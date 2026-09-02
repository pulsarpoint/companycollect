"""The ESEF candidate extractor: newest filing text + the ESEF financial view."""

from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.se_company.fields.candidates import esef
from dagster_v3.defs.se_company.fields.candidates.common import CandidateRow

HB = "5020077862"
OBSERVED = datetime(2025, 4, 2, tzinfo=UTC)


def test_scope_scans_the_artifact_and_the_metrics_by_lei() -> None:
    sql = esef.build_scope_sql()
    assert "SELECT company_id, observed_at AS changed_at FROM corpscout.se_company_info_esef" in sql
    assert "SELECT identifiers.company_id AS company_id, toDateTime64(metrics.resolved_at, 3, 'UTC') AS changed_at\n    FROM corpscout.esef_financial_metrics AS metrics" in sql
    assert "INNER JOIN corpscout.company_identifier AS identifiers\n        ON identifiers.issuer_scheme = 'lei' AND identifiers.issuer_id = upperUTF8(trimBoth(metrics.lei))" in sql
    assert "WHERE identifiers.country_code = 'SE' AND identifiers.is_current = 1" in sql
    # The shared scan (Task 1's changed_companies_scope_sql): per-company watermark, since floor.
    assert "FROM corpscout.se_company_field_candidate\n    WHERE source = '" in sql
    assert "changes.changed_at > greatest(ifNull(watermark.extracted_at, toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC')), parseDateTime64BestEffort(%(since)s, 3, 'UTC'))" in sql
    assert sql.endswith("ORDER BY company_id\nLIMIT %(page_size)s")


def test_candidates_take_the_newest_filing_text_and_the_view() -> None:
    sql = esef.build_candidates_sql()
    assert "FROM corpscout.se_company_info_esef FINAL\n    WHERE company_id IN %(company_ids)s AND trim(company_description) != ''\n    ORDER BY fiscal_year DESC, observed_at DESC, source_record_uid DESC\n    LIMIT 1 BY company_id" in sql
    assert "if(language = '', 'en', language)" in sql
    assert "FROM corpscout.se_financials_esef_current\n    WHERE company_id IN %(company_ids)s AND report_period_end IS NOT NULL AND notEmpty(source_record_uids)" in sql
    for field in ("description", "employee_count", "latest_revenue"):
        assert f"'{field}'" in sql, field
    assert sql.count("UNION ALL") == 2
    assert "%" not in sql.replace("%(company_ids)s", "")


def test_rows_from_result_binds_the_esef_source_and_version() -> None:
    rows = esef.rows_from_result([(HB, "description", "uid", OBSERVED, "A bank.", '{"compare_key":"a bank.","language":"en"}')])
    assert rows == [CandidateRow(HB, "description", "esef", "uid", "A bank.", '{"compare_key":"a bank.","language":"en"}', OBSERVED,
                                 "esef-candidates-v1")]


def test_asset_is_registered_with_the_artifact_and_the_metrics() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset = load_defs().get_repository_def().asset_graph.get(dg.AssetKey("se_company_field_candidates_esef"))
    assert asset.parent_keys == {
        dg.AssetKey("se_company_info_esef_clickhouse"),
        dg.AssetKey("esef_financial_metrics_clickhouse"),
        dg.AssetKey("company_identifier_clickhouse"),
    }
    assert asset.group_name == "se_company_fields"
    assert asset.metadata["source"] == "esef"
