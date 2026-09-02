"""The shared candidate contract: value_json twins, the positional row mapper, the
anti-join publish and the paging driver. Pure unit tests over the scripted FakeClient."""

from datetime import UTC, datetime
from functools import partial

import dagster as dg
import pytest

from dagster_v3.defs.se_company.common import EPOCH
from dagster_v3.defs.se_company.fields.candidates import common as cc
from dagster_v3.defs.se_company.fields.tables import SE_COMPANY_FIELD_CANDIDATE_COLUMNS
from tests.test_se_company_common import FakeClickhouse, FakeClient

HB = "5020077862"
SOLO = "5560125220"
NOW = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
OBSERVED = datetime(2026, 8, 1, tzinfo=UTC)
EXISTING_TABLES = [("se_company_info_scb",), ("se_company_field_candidate",)]


def test_candidate_columns_are_the_positional_insert_list_publish_candidates_binds() -> None:
    assert SE_COMPANY_FIELD_CANDIDATE_COLUMNS == (
        "company_id", "field", "source", "source_record_uid", "value", "value_json",
        "observed_at", "extracted_at", "extractor_version", "source_run_id",
    )
    assert cc.CANDIDATE_SELECT_COLUMNS == ("company_id", "field", "source_record_uid", "observed_at", "value", "value_json")
    assert cc.CANDIDATE_ANTI_JOIN_COLUMNS == ("company_id", "field", "source", "source_record_uid", "evidence_hash")


def test_compare_key_text_normalises_nfkc_whitespace_and_case() -> None:
    assert cc.compare_key_text("  Svenska Handelsbanken \n AB ") == "svenska handelsbanken ab"
    assert cc.compare_key_text("ﬁnans") == "finans"  # NFKC folds the ligature


def test_value_json_for_sorts_keys_and_keeps_nulls() -> None:
    assert cc.value_json_for(compare_key="x", language="en") == '{"compare_key":"x","language":"en"}'
    assert cc.value_json_for(compare_key="12000", period=None, count=12000, as_of="2024-12-31") == (
        '{"as_of":"2024-12-31","compare_key":"12000","count":12000,"period":null}'
    )
    with pytest.raises(ValueError, match="compare_key"):
        cc.value_json_for(compare_key="")


def test_json_object_sql_renders_sorted_members_from_json_token_expressions() -> None:
    assert cc.json_object_sql({"language": "toJSONString('en')", "compare_key": "toJSONString(ck)"}) == (
        "concat('{\"compare_key\":', toJSONString(ck), ',\"language\":', toJSONString('en'), '}')"
    )
    assert cc.json_string_sql("value") == "toJSONString(value)"
    assert cc.compare_key_text_sql("value") == (
        "lowerUTF8(trim(replaceRegexpAll(normalizeUTF8NFKC(value), '[[:space:]]+', ' ')))"
    )
    assert cc.nace_digits_sql("c") == "replaceAll(c, '.', '')"
    assert cc.clean_text_sql("legal_name") == (
        "if(lowerUTF8(trim(ifNull(legal_name, ''))) IN ('', '-', '--', '.', 'n/a', 'null', 'none'), '', "
        "trim(ifNull(legal_name, '')))"
    )


def test_financial_sql_helpers_render_the_documented_members() -> None:
    assert cc.employee_count_json_sql(count="employees", as_of="period_end", period="toString(fiscal_year)") == (
        "concat('{\"as_of\":', toJSONString(period_end), ',\"compare_key\":', toJSONString(toString(employees)), "
        "',\"count\":', toString(employees), ',\"period\":', toJSONString(toString(fiscal_year)), '}')"
    )
    assert cc.revenue_value_sql(amount="amount", currency="currency", fiscal_year="fiscal_year") == (
        "concat(currency, ' ', toString(amount), ' FY', toString(fiscal_year))"
    )
    revenue_json = cc.latest_revenue_json_sql(
        amount="amount", currency="currency", amount_usd="amount_usd", fiscal_year="fiscal_year", period_end="period_end")
    assert revenue_json.startswith("concat('{\"amount\":', toString(amount), ',\"amount_usd\":', ifNull(toString(amount_usd), 'null'), ',\"compare_key\":', ")
    assert "toJSONString(concat(lowerUTF8(currency), ':', toString(amount), ':', toString(fiscal_year)))" in revenue_json
    assert revenue_json.endswith(", ',\"fiscal_year\":', toString(fiscal_year), ',\"period_end\":', toJSONString(period_end), '}')")
    ctes = cc.financial_view_ctes_sql("se_financials_esef_current")
    assert "FROM corpscout.se_financials_esef_current\n    WHERE company_id IN %(company_ids)s AND report_period_end IS NOT NULL AND notEmpty(source_record_uids)" in ctes
    assert ctes.count("LIMIT 1 BY company_id") == 2
    assert "FINAL" not in ctes  # a view has no FINAL
    assert cc.FINANCIAL_MEMBERS_SQL.startswith("SELECT company_id, 'employee_count' AS field, source_record_uid, observed_at, toString(employees) AS value")
    assert "SELECT company_id, 'latest_revenue', source_record_uid, observed_at, concat(currency, ' ', toString(amount), ' FY', toString(fiscal_year))" in cc.FINANCIAL_MEMBERS_SQL
    assert cc.nace_labels_cte_sql() == (
        "SELECT classification_version, normalized_code, "
        "replaceRegexpOne(description_en, '^[0-9][0-9.]*[[:space:]]+', '') AS label_en\n"
        "    FROM corpscout.nace_categories FINAL\n"
        "    WHERE level = 'class' AND is_current = 1"
    )


def test_candidate_rows_from_result_binds_positionally_and_refuses_empty_values() -> None:
    rows = cc.candidate_rows_from_result(
        [(HB, "legal_name", "uid-1", OBSERVED, "Svenska Handelsbanken AB", '{"compare_key":"svenska handelsbanken ab"}')],
        source="scb", extractor_version="scb-candidates-v1")
    assert rows == [cc.CandidateRow(HB, "legal_name", "scb", "uid-1", "Svenska Handelsbanken AB",
                                    '{"compare_key":"svenska handelsbanken ab"}', OBSERVED, "scb-candidates-v1")]
    with pytest.raises(ValueError, match="empty value"):
        cc.candidate_rows_from_result([(HB, "legal_name", "uid-1", OBSERVED, "  ", "{}")], source="scb", extractor_version="v")


def _staged(client: FakeClient) -> list[tuple]:
    return [row for sql, params in client.executed
            if sql.startswith("INSERT INTO `corpscout`.`_tmp_se_company_field_candidate_") for row in params]


def test_publish_candidates_stages_rows_in_column_order_and_anti_joins_on_five_columns() -> None:
    client = FakeClient(answers=[[(1, 0)], [(0,)], [(1,)], [(1,)]])
    row = cc.CandidateRow(HB, "legal_name", "scb", "uid-1", "Svenska Handelsbanken AB",
                          '{"compare_key":"svenska handelsbanken ab"}', OBSERVED, "scb-candidates-v1")
    inserted = cc.publish_candidates(FakeClickhouse(client), [row], source_run_id="run-1", extracted_at=NOW)
    assert inserted == 1
    assert _staged(client) == [(HB, "legal_name", "scb", "uid-1", "Svenska Handelsbanken AB",
                                '{"compare_key":"svenska handelsbanken ab"}', OBSERVED, NOW, "scb-candidates-v1", "run-1")]
    validation = next(s for s, _ in client.executed if s.startswith("SELECT count(), countIf("))
    assert cc.CANDIDATE_INVALID_CONDITION in validation
    insert_sql = next(s for s, _ in client.executed if s.startswith("INSERT INTO `corpscout`.`se_company_field_candidate`"))
    assert "existing.field = stage.field AND existing.source = stage.source" in insert_sql
    assert cc.publish_candidates(FakeClickhouse(FakeClient(answers=[])), [], source_run_id="run-1", extracted_at=NOW) == 0


def _extractor() -> cc.CandidateExtractor:
    return cc.CandidateExtractor(
        source="scb", extractor_version="scb-candidates-v1", source_tables=("se_company_info_scb",),
        build_scope_sql=lambda: "SELECT company_id FROM scope WHERE company_id > %(after_company_id)s AND changed_at > %(since)s LIMIT %(page_size)s",
        build_candidates_sql=lambda: "WITH x AS (SELECT 1) SELECT company_id, field, source_record_uid, observed_at, value, value_json FROM x WHERE company_id IN %(company_ids)s",
    )


CANDIDATE_RESULT = [
    (HB, "legal_name", "uid-1", OBSERVED, "Svenska Handelsbanken AB", '{"compare_key":"svenska handelsbanken ab"}'),
    (HB, "status", "uid-1", OBSERVED, "active", '{"compare_key":"active"}'),
    (SOLO, "legal_name", "uid-2", OBSERVED, "Beta AB", '{"compare_key":"beta ab"}'),
]


def test_materialize_candidates_preview_scans_from_the_watermark_and_writes_nothing() -> None:
    client = FakeClient(answers=[
        EXISTING_TABLES,
        [(datetime(2026, 8, 20, 6, 0, 0, 123000, tzinfo=UTC),)],  # max(extracted_at) for the source
        [(HB,), (SOLO,)],                                         # the one (short) scope page
        CANDIDATE_RESULT,
    ])
    config = cc.CandidateExtractConfig()
    metadata = cc.materialize_candidates(
        clickhouse=FakeClickhouse(client), extractor=_extractor(), config=config,
        source_run_id="run-1", extracted_at=NOW)
    assert metadata["preview"] is True
    assert metadata["since"] == "2026-08-20 06:00:00.123"
    assert metadata["selected_company_count"] == 2
    assert metadata["candidate_row_count"] == 3
    assert metadata["rows_per_field"] == {"legal_name": 2, "status": 1}
    assert metadata["stopped_at_cap"] is False
    scope_sql, scope_params = client.executed[2]
    assert scope_params == {"after_company_id": "", "page_size": 20_000, "since": "2026-08-20 06:00:00.123"}
    assert client.executed[3][1] == {"company_ids": (HB, SOLO)}
    assert not any(sql.startswith(("CREATE", "INSERT")) for sql, _ in client.executed)


def test_materialize_candidates_execute_publishes_each_page() -> None:
    client = FakeClient(answers=[
        EXISTING_TABLES,
        [(datetime(1970, 1, 1, tzinfo=UTC),)],  # empty candidate table -> EPOCH
        [(HB,), (SOLO,)],
        CANDIDATE_RESULT,
        [(3, 0)], [(0,)], [(3,)], [(3,)],       # publish_with_stage: validation, existing, anti-join count, total
    ])
    metadata = cc.materialize_candidates(
        clickhouse=FakeClickhouse(client), extractor=_extractor(), config=cc.CandidateExtractConfig(execute=True),
        source_run_id="run-1", extracted_at=NOW)
    assert metadata["preview"] is False
    assert metadata["since"] == EPOCH
    assert metadata["inserted_count"] == 3
    assert len(_staged(client)) == 3
    assert _staged(client)[0][7:] == (NOW, "scb-candidates-v1", "run-1")


def test_materialize_candidates_explicit_scope_skips_the_scan_and_honours_the_cap() -> None:
    client = FakeClient(answers=[EXISTING_TABLES, CANDIDATE_RESULT[:2]])
    metadata = cc.materialize_candidates(
        clickhouse=FakeClickhouse(client), extractor=_extractor(),
        config=cc.CandidateExtractConfig(company_ids=[SOLO, HB], max_companies=1),
        source_run_id="run-1", extracted_at=NOW)
    # No watermark query and no scope page: the explicit ids are the scope, sorted and capped.
    assert [sql[:4] for sql, _ in client.executed] == ["\n   ", "WITH"]
    assert client.executed[1][1] == {"company_ids": (HB,)}
    assert metadata["selected_company_count"] == 1
    assert metadata["stopped_at_cap"] is True
    assert metadata["company_scope"] == [HB, SOLO]


def test_materialize_candidates_pages_the_scan_until_a_short_page() -> None:
    client = FakeClient(answers=[
        EXISTING_TABLES, [(datetime(1970, 1, 1, tzinfo=UTC),)],
        [(HB,)], CANDIDATE_RESULT[:2],   # a full page of 1
        [(SOLO,)], CANDIDATE_RESULT[2:], # a second full page of 1
        [],                              # the empty page that ends the scan
    ])
    metadata = cc.materialize_candidates(
        clickhouse=FakeClickhouse(client), extractor=_extractor(), config=cc.CandidateExtractConfig(company_batch_size=1),
        source_run_id="run-1", extracted_at=NOW)
    assert metadata["selected_company_count"] == 2
    pages = [params for sql, params in client.executed if sql.startswith("SELECT company_id FROM scope")]
    assert [p["after_company_id"] for p in pages] == ["", HB, SOLO]


def test_materialize_candidates_rejects_malformed_ids_before_touching_clickhouse() -> None:
    with pytest.raises(ValueError, match="10 or 12 digits"):
        cc.materialize_candidates(
            clickhouse=FakeClickhouse(FakeClient(answers=[])), extractor=_extractor(),
            config=cc.CandidateExtractConfig(company_ids=["abc"]), source_run_id="run-1", extracted_at=NOW)


def test_define_candidate_asset_names_group_and_deps() -> None:
    asset = cc.define_candidate_asset(_extractor(), deps=("se_company_info_scb_clickhouse",), description="d")
    assert asset.key == dg.AssetKey("se_company_field_candidates_scb")
    spec = asset.get_asset_spec()
    assert spec.group_name == "se_company_fields"
    assert {dep.asset_key for dep in spec.deps} == {dg.AssetKey("se_company_info_scb_clickhouse")}
    assert spec.metadata["table"] == "corpscout.se_company_field_candidate"
    assert spec.metadata["source"] == "scb"
