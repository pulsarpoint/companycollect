"""The SCB candidate extractor: SQL pinned as text, rows bound by position, asset wired."""

from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.se_company.fields.candidates import scb
from dagster_v3.defs.se_company.fields.candidates.common import CandidateRow

HB = "5020077862"
OBSERVED = datetime(2026, 8, 1, tzinfo=UTC)


def test_scope_scans_the_artifact_and_the_industries_since_the_watermark() -> None:
    sql = scb.build_scope_sql()
    assert "SELECT company_id, observed_at AS changed_at FROM corpscout.se_company_info_scb" in sql
    assert "SELECT company_id, updated_from_raw_at AS changed_at FROM corpscout.se_industries" in sql
    assert "se_company_registry_current" not in sql  # the SCB side of the registry is the artifact
    assert "changes.company_id > %(after_company_id)s" in sql
    # The shared scan (Task 1's changed_companies_scope_sql): per-company watermark, since floor.
    assert "FROM corpscout.se_company_field_candidate\n    WHERE source = '" in sql
    assert "changes.changed_at > greatest(ifNull(watermark.extracted_at, toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC')), parseDateTime64BestEffort(%(since)s, 3, 'UTC'))" in sql
    assert sql.endswith("ORDER BY company_id\nLIMIT %(page_size)s")
    assert "FINAL" not in sql  # max(observed_at) IS the version column; no FINAL needed


def test_candidates_read_the_artifact_and_the_primary_industry() -> None:
    sql = scb.build_candidates_sql()
    assert "se_company_registry_current" not in sql
    assert "FROM corpscout.se_company_info_scb FINAL\n    WHERE company_id IN %(company_ids)s\n    ORDER BY observed_at DESC, source_record_uid DESC\n    LIMIT 1 BY company_id" in sql
    # The legal facts the old publisher copied verbatim: same columns, same fallback.
    assert "if(legal_name_clean != '', legal_name_clean, legal_name_raw_clean) AS legal_name" in sql
    assert "trim(ifNull(legal_form_code, '')) AS legal_form_code" in sql
    assert "trim(toString(status)) AS status" in sql
    assert "ifNull(toString(incorporation_date), '') AS incorporation_date" in sql
    assert "FROM corpscout.se_industries FINAL\n    WHERE is_primary = 1 AND company_id IN %(company_ids)s\n    GROUP BY company_id" in sql
    assert "WHERE level = 'class' AND is_current = 1" in sql
    assert "LEFT JOIN labels ON labels.classification_version = 'NACE_REV_2' AND labels.normalized_code = substring(industry.sni_code, 1, 4)" in sql
    assert "replaceAll(trim(industry.nace_code), '.', '') AS nace_code" in sql  # published dot-less, as today
    assert "'primary_nace_code', source_record_uid, observed_at, nace_code,\n    concat('{\"compare_key\":', toJSONString(nace_code), '}')" in sql
    # English preferred, Swedish otherwise -- and the language says which.
    assert "if(description_en != '', description_en, description_sv) AS description" in sql
    assert "if(description_en != '', 'en', 'sv') AS language" in sql
    for field in ("legal_name", "legal_form_code", "status", "incorporation_date", "description",
                  "description_sv", "primary_sni_code", "primary_nace_code", "industry_label_en"):
        assert f"'{field}', source_record_uid, observed_at" in sql, field
    assert sql.count("UNION ALL") == 8
    assert sql.count("FROM artifact WHERE") == 6 and sql.count("FROM industry_labelled WHERE") == 3
    assert "%" not in sql.replace("%(company_ids)s", "")  # clickhouse-driver renders with Python %


def test_rows_from_result_binds_the_scb_source_and_version() -> None:
    rows = scb.rows_from_result([(HB, "status", "uid", OBSERVED, "active", '{"compare_key":"active"}')])
    assert rows == [CandidateRow(HB, "status", "scb", "uid", "active", '{"compare_key":"active"}', OBSERVED, "scb-candidates-v1")]


def test_asset_is_registered_with_its_upstream_artifacts() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset = load_defs().get_repository_def().asset_graph.get(dg.AssetKey("se_company_field_candidates_scb"))
    assert asset.parent_keys == {
        dg.AssetKey("se_company_info_scb_clickhouse"),
        dg.AssetKey("sweden_company_industries_clickhouse"),
        dg.AssetKey("nace_categories_clickhouse"),
    }
    assert asset.group_name == "se_company_fields"
    assert asset.metadata["table"] == "corpscout.se_company_field_candidate"
    assert asset.metadata["source"] == "scb"
