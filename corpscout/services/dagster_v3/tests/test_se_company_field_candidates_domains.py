"""The domains candidate extractor: one website per company from company_domains."""

from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.se_company.fields.candidates import domains
from dagster_v3.defs.se_company.fields.candidates.common import CandidateRow

HB = "5020077862"
OBSERVED = datetime(2026, 8, 12, tzinfo=UTC)


def test_scope_scans_the_swedish_partition_by_resolved_at() -> None:
    sql = domains.build_scope_sql()
    assert "SELECT company_id, resolved_at AS changed_at FROM corpscout.company_domains WHERE country_code = 'SE'" in sql
    assert "FINAL" not in sql
    # The shared scan (Task 1's changed_companies_scope_sql): per-company watermark, since floor.
    assert "FROM corpscout.se_company_field_candidate\n    WHERE source = '" in sql
    assert "changes.changed_at > greatest(ifNull(watermark.extracted_at, toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC')), parseDateTime64BestEffort(%(since)s, 3, 'UTC'))" in sql
    assert sql.endswith("ORDER BY company_id\nLIMIT %(page_size)s")


def test_candidates_prefer_the_confirmed_primary_then_the_best_suggestion() -> None:
    sql = domains.build_candidates_sql()
    assert "FROM corpscout.company_domains FINAL" in sql
    assert "WHERE country_code = 'SE' AND company_id IN %(company_ids)s AND is_active = 1" in sql
    assert "AND (review_status = 'confirmed_primary' OR (suggested_primary = 1 AND review_status != 'rejected'))" in sql
    assert "ORDER BY (review_status = 'confirmed_primary') DESC, suggested_confidence DESC, root_domain ASC\n    LIMIT 1 BY company_id" in sql
    assert "SELECT company_id, 'website', source_record_uid, observed_at, website_url" in sql
    assert "UNION ALL" not in sql
    assert "%" not in sql.replace("%(company_ids)s", "")


def test_rows_from_result_binds_the_domains_source_and_version() -> None:
    rows = domains.rows_from_result([(HB, "website", "fp", OBSERVED, "https://x/", '{"compare_key":"x"}')])
    assert rows == [CandidateRow(HB, "website", "domains", "fp", "https://x/", '{"compare_key":"x"}', OBSERVED, "domains-candidates-v1")]


def test_asset_is_registered_downstream_of_the_serving_build() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset = load_defs().get_repository_def().asset_graph.get(dg.AssetKey("se_company_field_candidates_domains"))
    assert asset.parent_keys == {dg.AssetKey("company_serving_current")}
    assert asset.group_name == "se_company_fields"
    assert asset.metadata["source"] == "domains"
