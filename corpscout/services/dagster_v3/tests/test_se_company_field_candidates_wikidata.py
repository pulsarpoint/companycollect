"""The Wikidata candidate extractor: artifact facts, the entity's employee date, the website."""

from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.se_company.fields.candidates import wikidata
from dagster_v3.defs.se_company.fields.candidates.common import CandidateRow

HB = "5020077862"
OBSERVED = datetime(2026, 7, 15, tzinfo=UTC)


def test_scope_scans_the_artifact_and_both_entity_tables_through_it() -> None:
    sql = wikidata.build_scope_sql()
    assert "SELECT company_id, observed_at AS changed_at FROM corpscout.se_company_info_wikidata" in sql
    assert "SELECT artifact.company_id AS company_id, websites.resolved_at AS changed_at\n    FROM corpscout.wikidata_company_websites AS websites" in sql
    assert "SELECT artifact.company_id AS company_id, entities.resolved_at AS changed_at\n    FROM corpscout.wikidata_companies AS entities" in sql
    assert sql.count("INNER JOIN (SELECT company_id, wikidata_id FROM corpscout.se_company_info_wikidata) AS artifact") == 2
    # The shared scan (Task 1's changed_companies_scope_sql): per-company watermark, since floor.
    assert "FROM corpscout.se_company_field_candidate\n    WHERE source = '" in sql
    assert "changes.changed_at > greatest(ifNull(watermark.extracted_at, toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC')), parseDateTime64BestEffort(%(since)s, 3, 'UTC'))" in sql
    assert sql.endswith("ORDER BY company_id\nLIMIT %(page_size)s")


def test_candidates_read_the_artifact_the_entity_and_one_website_per_entity() -> None:
    sql = wikidata.build_candidates_sql()
    assert "FROM corpscout.se_company_info_wikidata FINAL\n    WHERE company_id IN %(company_ids)s" in sql
    assert "FROM corpscout.wikidata_companies FINAL\n    WHERE wikidata_id IN (SELECT wikidata_id FROM artifact)" in sql
    assert "FROM corpscout.wikidata_company_websites FINAL\n    WHERE wikidata_id IN (SELECT wikidata_id FROM artifact) AND trim(website_url) != ''\n    ORDER BY is_primary_candidate DESC, website_normalized_url ASC\n    LIMIT 1 BY wikidata_id" in sql
    assert "LEFT JOIN entities ON entities.wikidata_id = artifact.wikidata_id" in sql
    assert "INNER JOIN websites ON websites.wikidata_id = artifact.wikidata_id" in sql
    for field in ("description", "legal_name", "incorporation_date", "industry_label_en", "employee_count", "website"):
        assert f"'{field}'" in sql, field
    assert sql.count("UNION ALL") == 5
    assert "%" not in sql.replace("%(company_ids)s", "")


def test_rows_from_result_binds_the_wikidata_source_and_version() -> None:
    rows = wikidata.rows_from_result([(HB, "website", "wikidata:Q1", OBSERVED, "https://x/", '{"compare_key":"x"}')])
    assert rows == [CandidateRow(HB, "website", "wikidata", "wikidata:Q1", "https://x/", '{"compare_key":"x"}', OBSERVED,
                                 "wikidata-candidates-v1")]


def test_asset_is_registered_with_the_artifact_and_the_entity_tables() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset = load_defs().get_repository_def().asset_graph.get(dg.AssetKey("se_company_field_candidates_wikidata"))
    assert asset.parent_keys == {
        dg.AssetKey("se_company_info_wikidata_clickhouse"),
        dg.AssetKey("wikidata_companies_clickhouse"),
        dg.AssetKey("wikidata_company_websites_clickhouse"),
    }
    assert asset.group_name == "se_company_fields"
    assert asset.metadata["source"] == "wikidata"
