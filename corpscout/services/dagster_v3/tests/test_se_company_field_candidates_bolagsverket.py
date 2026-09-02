"""The Bolagsverket candidate extractor: registry row + annual accounts view."""

from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.se_company.fields.candidates import bolagsverket
from dagster_v3.defs.se_company.fields.candidates.common import CandidateRow

HB = "5020077862"
OBSERVED = datetime(2024, 12, 31, tzinfo=UTC)


def test_scope_scans_the_registry_row_and_the_metrics_table() -> None:
    sql = bolagsverket.build_scope_sql()
    assert "SELECT company_id, observed_at AS changed_at FROM corpscout.se_company_registry_current\n    WHERE source = 'bolagsverket' AND has_company = 1" in sql
    assert "SELECT company_id, resolved_at AS changed_at FROM corpscout.se_bolagsverket_financial_metrics" in sql
    assert "se_financials_bolagsverket_current" not in sql  # the view has no change stamp; the table behind it does
    assert "FINAL" not in sql
    # The shared scan (Task 1's changed_companies_scope_sql): per-company watermark, since floor.
    assert "FROM corpscout.se_company_field_candidate\n    WHERE source = '" in sql
    assert "changes.changed_at > greatest(ifNull(watermark.extracted_at, toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC')), parseDateTime64BestEffort(%(since)s, 3, 'UTC'))" in sql
    assert sql.endswith("ORDER BY company_id\nLIMIT %(page_size)s")


def test_candidates_read_the_register_row_with_the_scb_status_beside_it() -> None:
    sql = bolagsverket.build_candidates_sql()
    assert "FROM corpscout.se_company_registry_current AS bv" in sql
    assert "WHERE source = 'scb' AND has_company = 1 AND company_id IN %(company_ids)s" in sql
    assert "WHERE bv.source = 'bolagsverket' AND bv.has_company = 1 AND bv.company_id IN %(company_ids)s" in sql
    assert "if(scb_status != '' AND scb_status != status, 'true', 'false')" in sql
    assert "FROM corpscout.se_financials_bolagsverket_current\n    WHERE company_id IN %(company_ids)s AND report_period_end IS NOT NULL AND notEmpty(source_record_uids)" in sql
    assert "se_company_registry_current AS bv FINAL" not in sql and "se_financials_bolagsverket_current FINAL" not in sql
    for field in ("legal_name", "legal_form_code", "status", "incorporation_date", "employee_count", "latest_revenue"):
        assert f"'{field}'" in sql, field
    assert sql.count("UNION ALL") == 5
    assert "%" not in sql.replace("%(company_ids)s", "")


def test_rows_from_result_binds_the_bolagsverket_source_and_version() -> None:
    rows = bolagsverket.rows_from_result([(HB, "employee_count", "uid", OBSERVED, "11950", '{"compare_key":"11950"}')])
    assert rows == [CandidateRow(HB, "employee_count", "bolagsverket", "uid", "11950", '{"compare_key":"11950"}', OBSERVED,
                                 "bolagsverket-candidates-v1")]


def test_asset_is_registered_with_the_register_and_the_metrics() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset = load_defs().get_repository_def().asset_graph.get(dg.AssetKey("se_company_field_candidates_bolagsverket"))
    assert asset.parent_keys == {
        dg.AssetKey("sweden_company_profile_history_clickhouse"),
        dg.AssetKey("se_bolagsverket_financial_metrics_clickhouse"),
    }
    assert asset.group_name == "se_company_fields"
    assert asset.metadata["source"] == "bolagsverket"
