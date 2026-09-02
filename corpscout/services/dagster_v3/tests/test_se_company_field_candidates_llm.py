"""The LLM candidate extractor: pass 2 of info.py behind the candidate contract."""

import json
import uuid
from datetime import UTC, datetime

import dagster as dg
import pytest
from pydantic import ValidationError

from dagster_v3.defs.se_company.common import input_hash_for
from dagster_v3.defs.se_company.fields.candidates import llm
from dagster_v3.defs.se_company.info import LlmProfileConfig, build_description_request as info_request
from dagster_v3.defs.se_company.info_rules import InfoOutcome
from tests.test_se_company_common import FakeClickhouse, FakeClient
from tests.test_se_company_info import GOOD_REPLY, FakeLlm

HB = "5020077862"
SOLO = "5560125220"
NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
STORED_AT = datetime(2026, 8, 20, tzinfo=UTC)
EXISTING_TABLES = [("se_company_field_candidate",), ("se_company_info_enrichment_observation",)]
PROFILE = llm.LlmCandidateProfile(provider="fake-provider", model="fake-model")
CONTEXT_ROWS = [
    # (company_id, field, source, source_record_uid, value, value_json)
    (HB, "description", "esef", "esef-art-hb-2024", "Handelsbanken is a Nordic bank.", '{"compare_key":"handelsbanken is a nordic bank.","language":"en"}'),
    (HB, "description", "scb", "scb-art-hb", "Banking operations.", '{"compare_key":"banking operations.","language":"en"}'),
    (HB, "description", "wikidata", "wikidata:Q1421630", "Swedish bank", '{"compare_key":"swedish bank","language":"en"}'),
    (HB, "description_sv", "scb", "scb-art-hb", "Bankverksamhet.", '{"compare_key":"bankverksamhet.","language":"sv"}'),
    (HB, "legal_name", "bolagsverket", "bv-uid", "Svenska Handelsbanken AB (bv)", '{"compare_key":"x"}'),
    (HB, "legal_name", "scb", "scb-art-hb", "Svenska Handelsbanken AB", '{"compare_key":"svenska handelsbanken ab"}'),
    (HB, "primary_nace_code", "ratsit", "ratsit-uid", "6420", '{"compare_key":"6420"}'),
    (HB, "primary_nace_code", "scb", "ind-uid", "6419", '{"compare_key":"6419"}'),
    (SOLO, "description", "scb", "scb-art-solo", "Handel med datorer.", '{"compare_key":"handel med datorer.","language":"sv"}'),
    (SOLO, "description_sv", "scb", "scb-art-solo", "Handel med datorer.", '{"compare_key":"handel med datorer.","language":"sv"}'),
]


def test_scope_requires_two_text_sources_newer_than_the_last_llm_row() -> None:
    sql = llm.build_scope_sql()
    assert "FROM corpscout.se_company_field_candidate\nWHERE field = 'description' AND company_id > %(after_company_id)s\nGROUP BY company_id" in sql
    assert "HAVING uniqExactIf(source, source != 'llm') >= 2" in sql
    assert ("AND maxIf(extracted_at, source != 'llm') > greatest(maxIf(extracted_at, source = 'llm'), "
            "parseDateTime64BestEffort(%(since)s, 3, 'UTC'))") in sql
    assert sql.endswith("ORDER BY company_id\nLIMIT %(page_size)s")


def test_context_sql_reads_the_newest_non_llm_candidate_per_field_and_source() -> None:
    sql = llm.build_context_sql()
    assert "argMax(source_record_uid, (observed_at, source_record_uid, extracted_at)) AS source_record_uid" in sql
    assert "WHERE company_id IN %(company_ids)s AND field IN ('description', 'description_sv', 'legal_name', 'primary_nace_code') AND source != 'llm'" in sql
    assert sql.endswith("GROUP BY company_id, field, source\nORDER BY company_id, field, source")


def test_companies_from_context_orders_sources_and_drops_single_source_companies() -> None:
    companies = llm.companies_from_context(CONTEXT_ROWS)
    assert set(companies) == {HB}  # SOLO has one text source
    company = companies[HB]
    assert company.candidates == (
        ("esef", "esef-art-hb-2024", "Handelsbanken is a Nordic bank."),
        ("wikidata", "wikidata:Q1421630", "Swedish bank"),
        ("scb", "scb-art-hb", "Banking operations."),
    )
    # Registry order (revised 4.2: scb first) picks the scb legal name and the scb NACE code.
    assert company.legal_name == "Svenska Handelsbanken AB"
    assert company.primary_nace_code == "6419"
    assert company.description_sv == "Bankverksamhet."


def test_request_is_byte_identical_to_info_py_so_stored_observations_are_reused() -> None:
    """Delete this test together with info.py: it pins the cutover's reuse of every stored
    observation, which needs the same input_hash, which needs the same request."""
    company = llm.companies_from_context(CONTEXT_ROWS)[HB]
    outcome = InfoOutcome(
        company_id=HB, legal_name="Svenska Handelsbanken AB", legal_form_code=None, legal_form_label_en="",
        legal_form_label_sv="", status="active", incorporation_date=None, description=None, description_sv="Bankverksamhet.",
        description_language="", llm_enhanced=False, description_sources=(), description_source_record_uids=(),
        primary_nace_code="6419", primary_sni_code="64190", wikidata_id=None, lei=None, source_record_uids=(),
        evidence_hashes=(), needs_model=True, description_candidates=company.candidates,
        description_sv_candidate="Bankverksamhet.")
    profile = LlmProfileConfig(provider="fake-provider", model="fake-model")
    assert llm.build_description_request(company, PROFILE) == info_request(outcome, profile)
    assert llm.build_description_request(company, PROFILE)["messages"][0]["content"] == llm.DESCRIPTION_SYSTEM_PROMPT


def test_candidate_rows_for_emits_both_languages_under_the_suggestion_id() -> None:
    from dagster_v3.defs.se_company.common import ObservationResult

    suggestion_id = uuid.uuid4()
    result = ObservationResult(suggestion=json.loads(GOOD_REPLY), raw_response=GOOD_REPLY, model_provider="p",
                               model_name="m", prompt_version="v", prompt_tokens=1, completion_tokens=1,
                               suggestion_id=suggestion_id)
    rows = llm.candidate_rows_for(HB, result, observed_at=STORED_AT)
    assert [(r.field, r.source, r.source_record_uid, r.value, r.observed_at, r.extractor_version) for r in rows] == [
        ("description", "llm", str(suggestion_id), json.loads(GOOD_REPLY)["description"], STORED_AT, "llm-candidates-v1"),
        ("description_sv", "llm", str(suggestion_id), json.loads(GOOD_REPLY)["description_sv"], STORED_AT, "llm-candidates-v1"),
    ]
    assert json.loads(rows[0].value_json) == {"compare_key": json.loads(GOOD_REPLY)["description"].casefold(), "language": "en"}
    assert json.loads(rows[1].value_json)["language"] == "sv"


def test_config_requires_provider_and_model() -> None:
    with pytest.raises(ValidationError):
        llm.LlmCandidateConfig()
    with pytest.raises(ValidationError):
        llm.LlmCandidateConfig(llm={"provider": "deepseek"})
    config = llm.LlmCandidateConfig(llm={"provider": "deepseek", "model": "deepseek-v4-flash"})
    assert config.llm.prompt_version == "se-company-info-description-v3"
    assert config.execute is False and config.company_batch_size == 5_000


def _stored_row(company: llm.LlmCompany, *, suggestion_id: uuid.UUID) -> tuple:
    request = llm.build_description_request(company, PROFILE)
    return (suggestion_id, HB, input_hash_for(request, PROFILE.prompt_version), GOOD_REPLY,
            "fake-provider", "fake-model", PROFILE.prompt_version, STORED_AT)


def _candidate_stage_rows(client: FakeClient) -> list[tuple]:
    return [row for sql, params in client.executed
            if sql.startswith("INSERT INTO `corpscout`.`_tmp_se_company_field_candidate_") for row in params]


def _observation_stage_rows(client: FakeClient) -> list[tuple]:
    return [row for sql, params in client.executed
            if sql.startswith("INSERT INTO `corpscout`.`_tmp_se_company_info_enrichment_observation_") for row in params]


def test_materialize_reuses_a_stored_observation_without_calling_the_model() -> None:
    company = llm.companies_from_context(CONTEXT_ROWS)[HB]
    suggestion_id = uuid.uuid4()
    client = FakeClient(answers=[
        EXISTING_TABLES, [(HB,)], CONTEXT_ROWS, [_stored_row(company, suggestion_id=suggestion_id)],
        [(2, 0)], [(0,)], [(2,)], [(2,)],  # publish_candidates
    ])
    model = FakeLlm()  # no scripted replies: any call would raise
    metadata = llm.materialize_llm_candidates(
        clickhouse=FakeClickhouse(client), config=llm.LlmCandidateConfig(execute=True, llm=PROFILE),
        llm_client=model, source_run_id="run-1", extracted_at=NOW)
    assert metadata["llm_reused_count"] == 1 and metadata["llm_request_count"] == 0
    assert metadata["inserted_count"] == 2
    assert _observation_stage_rows(client) == []
    staged = _candidate_stage_rows(client)
    assert [(row[1], row[2], row[3], row[6], row[7]) for row in staged] == [
        ("description", "llm", str(suggestion_id), STORED_AT, NOW),
        ("description_sv", "llm", str(suggestion_id), STORED_AT, NOW),
    ]


def test_materialize_calls_the_model_and_persists_the_observation_before_the_candidates() -> None:
    client = FakeClient(answers=[
        EXISTING_TABLES, [(HB,)], CONTEXT_ROWS, [],   # no stored observation
        [(1, 0)], [(0,)], [(1,)],                     # publish_observations (no new_versions_only: 3 reads)
        [(2, 0)], [(0,)], [(2,)], [(2,)],             # publish_candidates
    ])
    metadata = llm.materialize_llm_candidates(
        clickhouse=FakeClickhouse(client), config=llm.LlmCandidateConfig(execute=True, llm=PROFILE),
        llm_client=FakeLlm(GOOD_REPLY), source_run_id="run-1", extracted_at=NOW)
    assert metadata["llm_request_count"] == 1 and metadata["observation_inserted_count"] == 1
    observations = _observation_stage_rows(client)
    assert len(observations) == 1 and observations[0][1] == HB and observations[0][-1] == NOW
    candidates = _candidate_stage_rows(client)
    assert {row[3] for row in candidates} == {str(observations[0][0])}  # uid = the new suggestion_id
    assert {row[6] for row in candidates} == {NOW}                     # observed_at = its created_at
    statements = [sql for sql, _ in client.executed]
    first_observation = next(i for i, s in enumerate(statements) if "_tmp_se_company_info_enrichment_observation_" in s)
    first_candidate = next(i for i, s in enumerate(statements) if "_tmp_se_company_field_candidate_" in s)
    assert first_observation < first_candidate


def test_preview_builds_requests_but_never_calls_or_writes() -> None:
    company = llm.companies_from_context(CONTEXT_ROWS)[HB]
    client = FakeClient(answers=[EXISTING_TABLES, [(HB,), (SOLO,)], CONTEXT_ROWS, [_stored_row(company, suggestion_id=uuid.uuid4())]])
    metadata = llm.materialize_llm_candidates(
        clickhouse=FakeClickhouse(client), config=llm.LlmCandidateConfig(llm=PROFILE),
        llm_client=None, source_run_id="run-1", extracted_at=NOW)
    assert metadata["preview"] is True
    assert metadata["selected_company_count"] == 2 and metadata["skipped_single_source_count"] == 1
    assert metadata["would_reuse_count"] == 1 and metadata["would_call_model_count"] == 0
    assert not any(sql.startswith(("CREATE", "INSERT")) for sql, _ in client.executed)


def test_model_failure_skips_the_company_and_leaves_it_for_the_next_run() -> None:
    client = FakeClient(answers=[EXISTING_TABLES, [(HB,)], CONTEXT_ROWS, []])
    metadata = llm.materialize_llm_candidates(
        clickhouse=FakeClickhouse(client), config=llm.LlmCandidateConfig(execute=True, llm=PROFILE),
        llm_client=FakeLlm("not json at all"), source_run_id="run-1", extracted_at=NOW)
    assert metadata["model_failed_count"] == 1 and metadata.get("inserted_count", 0) == 0
    assert _candidate_stage_rows(client) == []


def test_execute_without_a_client_is_refused() -> None:
    with pytest.raises(ValueError, match="LLM client"):
        llm.materialize_llm_candidates(
            clickhouse=FakeClickhouse(FakeClient(answers=[])), config=llm.LlmCandidateConfig(execute=True, llm=PROFILE),
            llm_client=None, source_run_id="run-1", extracted_at=NOW)


def test_asset_is_registered_downstream_of_the_text_extractors() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset = load_defs().get_repository_def().asset_graph.get(dg.AssetKey("se_company_field_candidates_llm"))
    assert asset.parent_keys == {
        dg.AssetKey("se_company_field_candidates_bolagsverket"),
        dg.AssetKey("se_company_field_candidates_esef"),
        dg.AssetKey("se_company_field_candidates_ratsit"),
        dg.AssetKey("se_company_field_candidates_scb"),
        dg.AssetKey("se_company_field_candidates_wikidata"),
    }
    assert asset.group_name == "se_company_fields"
    assert asset.metadata["source"] == "llm"
