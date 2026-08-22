"""The se_company_info final asset: change detection, artifact reads, the model
step and the wiring (jobs, sensor, schedule, freshness leaves).

The ClickHouse-facing helpers are asserted as SQL text (this repo has no live
ClickHouse in CI); the resolution loop is exercised end-to-end through
``materialize_se_company_info`` with the scripted fake client from
``test_se_company_common``.
"""

import json
import re
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace

import dagster as dg
import pytest

from dagster_v3.defs.se_company.info import (
    DESCRIPTION_PROMPT_VERSION,
    DescriptionSuggestion,
    build_artifact_rows_sql,
    build_changed_companies_sql,
    build_description_request,
    parse_description_suggestion,
)
from dagster_v3.defs.se_company.info_rules import ArtifactRow, merge_company_info

NOW = datetime(2026, 8, 22, 12, tzinfo=UTC)
COMPANY = "5565200028"

# assert_clickhouse_tables_exist runs its own SELECT against system.tables first,
# so every scripted answer list starts with the tables it asks about.
EXISTING_TABLES = [
    (table,)
    for table in (
        "se_company_info_scb",
        "se_company_info_esef",
        "se_company_info_wikidata",
        "se_company_info",
        "se_company_info_correction",
        "se_company_info_enrichment_observation",
    )
]

ARTIFACT_ROWS = [
    (
        "scb",
        COMPANY,
        "scb:1",
        "a" * 64,
        NOW,
        json.dumps(
            {
                "legal_name": "Alpha AB",
                "legal_name_raw": "",
                "legal_form_code": "AB",
                "status": "active",
                "incorporation_date": "",
                "dissolution_date": "",
                "activity_description": "IT-konsulter.",
                "primary_sni_code": "62010",
                "primary_nace_code": "62.01",
            }
        ),
    ),
    (
        "wikidata",
        COMPANY,
        "wikidata:Q1",
        "c" * 64,
        NOW,
        json.dumps(
            {
                "wikidata_id": "Q1",
                "wikidata_url": "",
                "name": "Alpha",
                "official_name": "",
                "company_description": "Swedish fintech company",
                "inception_date": "",
                "legal_form_label": "",
                "industry_wikidata_id": "",
                "industry_label": "",
                "headquarters_label": "",
                "employee_count": "",
            }
        ),
    ),
]


def _outcome():
    scb = ArtifactRow("scb", "scb:1", "a" * 64, NOW, {
        "legal_name": "Alpha AB", "legal_name_raw": None, "legal_form_code": "AB",
        "status": "active", "incorporation_date": None, "dissolution_date": None,
        "activity_description": "IT-konsulter.", "primary_sni_code": "62010",
        "primary_nace_code": "62.01"})
    wiki = ArtifactRow("wikidata", "wikidata:Q1", "c" * 64, NOW, {
        "wikidata_id": "Q1", "wikidata_url": "", "name": "Alpha", "official_name": None,
        "company_description": "Swedish fintech company", "inception_date": None,
        "legal_form_label": None, "industry_wikidata_id": None, "industry_label": None,
        "headquarters_label": None, "employee_count": None})
    return merge_company_info(COMPANY, [scb, wiki])


class _FakeCompletions:
    def __init__(self, content: str) -> None:
        self.content = content
        self.requests: list[dict] = []

    def create(self, **request):
        self.requests.append(request)
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=self.content))],
            usage=SimpleNamespace(prompt_tokens=11, completion_tokens=7),
        )


class FakeLlm:
    """Just enough of the OpenAI client for _request_description."""

    def __init__(self, content: str) -> None:
        self.completions = _FakeCompletions(content)
        self.chat = SimpleNamespace(completions=self.completions)


def test_changed_companies_sql_compares_artifact_versions_and_ledger_with_the_final() -> None:
    sql = build_changed_companies_sql()
    for table in ("se_company_info_scb", "se_company_info_esef", "se_company_info_wikidata"):
        assert f"FROM corpscout.{table}" in sql
    assert "FROM corpscout.se_company_info AS final FINAL" in sql
    assert "%(pending_model_only)s = 1 AND published.description_source_count > 1 AND published.suggestion_id IS NULL" in sql
    # A company is changed when it has never been published, when an artifact carries a
    # newer observation than the published resolution, or when the ledger gained a row
    # after it. Deliberately NOT a published-vs-live correction_ids comparison: a stale
    # or malformed correction is never applied, so that predicate would re-select the
    # same company on every run forever.
    assert "published.company_id = ''" in sql
    assert "artifacts.latest_observed_at > published.resolved_at" in sql
    assert "ledger.latest_correction_at > published.resolved_at" in sql
    assert "arraySort(groupArrayIf(toString(correction_id), NOT superseded))" not in sql
    # ClickHouse 26.5: after the LEFT JOINs every company_id reference is qualified.
    assert "AND artifacts.company_id > %(after_company_id)s" in sql
    assert "ORDER BY artifacts.company_id" in sql
    assert "\nORDER BY company_id" not in sql and "\n  AND company_id " not in sql
    assert "LIMIT %(max_companies)s" in sql


def test_artifact_rows_sql_unions_the_three_artifacts_with_a_source_column() -> None:
    sql = build_artifact_rows_sql()
    assert sql.count("UNION ALL") == 2
    for source in ("'scb' AS source", "'esef' AS source", "'wikidata' AS source"):
        assert source in sql
    assert "WHERE company_id IN %(company_ids)s" in sql
    assert "toString(evidence_hash) AS evidence_hash" in sql
    assert "toJSONString(map('legal_name'" in sql and "'activity_description'" in sql
    # ClickHouse 26.5 has no common type for a date/number and '': the cast must be
    # inside the ifNull, never around it.
    for column in ("fiscal_year", "description_confidence", "incorporation_date",
                   "dissolution_date", "inception_date", "employee_count"):
        assert f"ifNull(toString({column}), '')" in sql
        assert f"toString(ifNull({column}" not in sql
    assert "* EXCEPT" not in sql and " * " not in sql  # explicit read contract, never star


def test_artifact_reads_are_derived_from_each_artifact_modules_column_list() -> None:
    from dagster_v3.defs.se_company.esef import SE_COMPANY_INFO_ESEF_COLUMNS
    from dagster_v3.defs.se_company.info import ARTIFACT_READS, ARTIFACT_TABLES
    from dagster_v3.defs.se_company.scb import SE_COMPANY_INFO_SCB_COLUMNS
    from dagster_v3.defs.se_company.wikidata import SE_COMPANY_INFO_WIKIDATA_COLUMNS

    envelope = {"company_id", "source_record_uid", "observed_at", "source_run_id"}
    assert ARTIFACT_READS["scb"] == tuple(c for c in SE_COMPANY_INFO_SCB_COLUMNS if c not in envelope)
    assert ARTIFACT_READS["wikidata"] == tuple(c for c in SE_COMPANY_INFO_WIKIDATA_COLUMNS if c not in envelope)
    # ESEF's two JSON blobs are the one payload this module deliberately does not read.
    assert ARTIFACT_READS["esef"] == tuple(
        c for c in SE_COMPANY_INFO_ESEF_COLUMNS if c not in envelope and not c.endswith("_json")
    )
    assert ARTIFACT_TABLES == {
        "scb": "se_company_info_scb",
        "esef": "se_company_info_esef",
        "wikidata": "se_company_info_wikidata",
    }


def test_description_request_is_json_only_and_lists_every_source() -> None:
    outcome = _outcome()
    assert outcome.needs_model
    request = build_description_request(outcome, model="deepseek-v4-flash")
    payload = json.loads(request["messages"][1]["content"])
    assert payload["company_id"] == COMPANY and payload["legal_name"] == "Alpha AB"
    assert [c["source"] for c in payload["sources"]] == ["wikidata", "scb"]
    assert request["response_format"] == {"type": "json_object"} and request["temperature"] == 0
    assert "untrusted" in request["messages"][0]["content"].lower()


def test_parse_description_suggestion_validates_shape() -> None:
    suggestion = parse_description_suggestion(
        '{"description": "Alpha AB is a Swedish fintech company offering IT consulting.",'
        ' "language": "en", "rationale": "both"}'
    )
    assert isinstance(suggestion, DescriptionSuggestion) and suggestion.language == "en"
    with pytest.raises(ValueError):
        parse_description_suggestion('{"description": "", "language": "en", "rationale": ""}')
    with pytest.raises(ValueError):
        parse_description_suggestion(None)
    assert DESCRIPTION_PROMPT_VERSION == "se-company-info-description-v1"


def test_initial_load_can_publish_multi_source_companies_without_the_model() -> None:
    """resolve_multi_source_with_llm=False publishes the provisional pick and records the
    contributing sources; the model is never constructed."""
    from dagster_v3.defs.se_company.info import INSERT_COLUMNS, materialize_se_company_info
    from tests.test_se_company_common import FakeClickhouse, FakeClient  # reuse the scripted fake

    client = FakeClient(answers=[
        EXISTING_TABLES,        # assert_clickhouse_tables_exist
        [(COMPANY,)],           # changed companies
        ARTIFACT_ROWS,          # artifact rows
        [],                     # ledger
        [],                     # observations
        [(1, 0)],               # final stage validation
        [(0,)],                 # target row count before the insert
        [(1,)],                 # target row count after the insert
    ])
    metadata = materialize_se_company_info(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[COMPANY], max_companies=1, company_batch_size=1, timeout_seconds=10,
        llm_client=None, llm_model=None, llm_provider=None, log=None,
        resolve_multi_source_with_llm=False)

    assert metadata["multi_source_count"] == 1 and metadata.get("llm_request_count", 0) == 0
    # The observation table is read (a stored suggestion may exist) but never written.
    assert not any(sql.startswith("INSERT") and "enrichment_observation" in sql
                   for sql, _ in client.executed)
    assert "observation_inserted_count" not in metadata
    staged_insert = next(params for sql, params in client.executed
                         if sql.startswith("INSERT INTO `corpscout`.`_tmp_se_company_info_"))
    row = dict(zip(INSERT_COLUMNS, staged_insert[0], strict=True))
    assert row["description_source_count"] == 2 and row["description_sources"] == ["wikidata", "scb"]
    assert row["description_source"] == "wikidata"
    assert row["suggestion_id"] is None and row["model_provider"] == "deterministic"


def test_model_pass_records_the_observation_before_publishing_its_description() -> None:
    from dagster_v3.defs.se_company.info import (
        INSERT_COLUMNS,
        OBSERVATION_COLUMNS,
        materialize_se_company_info,
    )
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    llm = FakeLlm(json.dumps({
        "description": "Alpha AB is a Swedish fintech company providing IT consulting.",
        "language": "en", "rationale": "both sources"}))
    client = FakeClient(answers=[
        EXISTING_TABLES, [(COMPANY,)], ARTIFACT_ROWS, [], [],
        [(1, 0)], [(0,)], [(1,)],   # observation publish
        [(1, 0)], [(0,)], [(1,)],   # final publish
    ])
    metadata = materialize_se_company_info(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[COMPANY], max_companies=1, company_batch_size=1, timeout_seconds=10,
        llm_client=llm, llm_model="fake-model", llm_provider="fake-provider", log=None)

    assert metadata["llm_request_count"] == 1 and metadata["observation_inserted_count"] == 1
    assert llm.completions.requests[0]["model"] == "fake-model"
    statements = [sql for sql, _ in client.executed]
    observation_stage = next(i for i, sql in enumerate(statements)
                             if sql.startswith("INSERT INTO `corpscout`.`_tmp_se_company_info_enrichment_observation_"))
    observation_target = next(i for i, sql in enumerate(statements)
                              if sql.startswith("INSERT INTO `corpscout`.`se_company_info_enrichment_observation`"))
    final_stage = next(i for i, sql in enumerate(statements)
                       if re.match(r"^INSERT INTO `corpscout`\.`_tmp_se_company_info_[0-9a-f]{32}`", sql))
    assert observation_target < final_stage  # durable before the description it justifies

    observation = dict(zip(OBSERVATION_COLUMNS, client.executed[observation_stage][1][0], strict=True))
    row = dict(zip(INSERT_COLUMNS, client.executed[final_stage][1][0], strict=True))
    assert row["description"] == "Alpha AB is a Swedish fintech company providing IT consulting."
    assert row["description_source"] == "llm" and row["description_language"] == "en"
    assert row["description_sources"] == ["wikidata", "scb"] and row["description_source_count"] == 2
    assert row["model_provider"] == "fake-provider" and row["model_name"] == "fake-model"
    assert row["prompt_version"] == DESCRIPTION_PROMPT_VERSION
    assert isinstance(row["suggestion_id"], uuid.UUID)
    assert observation["suggestion_id"] == row["suggestion_id"]
    assert re.fullmatch(r"[0-9a-f]{64}", observation["input_hash"])
    assert json.loads(observation["suggestion"])["language"] == "en"


def test_insert_columns_match_the_migration_in_order() -> None:
    from dagster_v3.defs.se_company.info import INSERT_COLUMNS
    from tests.se_company_ddl import declared_columns

    assert list(INSERT_COLUMNS) == [
        c for c in declared_columns("se_company_info") if c != "evidence_set_hash"
    ]


def test_definitions_wire_final_jobs_sensor_schedule_and_leaves() -> None:
    from dagster_v3.definitions import defs as load_defs
    from dagster_v3.defs.common.clickhouse_checks import CLICKHOUSE_LEAVES

    repository = load_defs().get_repository_def()
    final = repository.asset_graph.get(dg.AssetKey("se_company_info_clickhouse"))
    assert final.parent_keys == {
        dg.AssetKey("se_company_info_scb_clickhouse"),
        dg.AssetKey("se_company_info_esef_clickhouse"),
        dg.AssetKey("se_company_info_wikidata_clickhouse"),
    }
    assert final.group_name == "se_company"
    keys = {k.path[-1] for k in repository.get_job("se_company_info_job").asset_layer.executable_asset_keys}
    assert keys == {"se_company_info_scb_clickhouse", "se_company_info_esef_clickhouse",
                    "se_company_info_wikidata_clickhouse", "se_company_info_clickhouse"}
    assert {k.path[-1] for k in repository.get_job("se_company_info_review_job").asset_layer.executable_asset_keys} == {
        "se_company_info_clickhouse"}
    sensor = repository.get_sensor_def("se_company_info_correction_sensor")
    assert sensor.job_name == "se_company_info_review_job"
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED
    schedule = repository.get_schedule_def("se_company_info_weekly")
    # 06:45 Monday would collide with the existing "45 6 * * 6" slot the cron contract guards.
    assert schedule.cron_schedule == "50 6 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    leaves = {leaf.asset_key: leaf for leaf in CLICKHOUSE_LEAVES}
    assert leaves["se_company_info_clickhouse"].tables == ("se_company_info",)
    assert leaves["se_company_info_scb_clickhouse"].tables == ("se_company_info_scb",)
    assert leaves["se_company_info_esef_clickhouse"].tables == ("se_company_info_esef",)
    assert leaves["se_company_info_wikidata_clickhouse"].tables == ("se_company_info_wikidata",)
    # The weekly schedule ships STOPPED, so a freshness bound would only be noise today.
    assert all(leaves[key].max_age is None for key in (
        "se_company_info_clickhouse", "se_company_info_scb_clickhouse",
        "se_company_info_esef_clickhouse", "se_company_info_wikidata_clickhouse"))
