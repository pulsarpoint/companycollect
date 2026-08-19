import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import dagster as dg
import pytest
from pydantic import ValidationError

from dagster_v3.defs.company_people.normalization import (
    DIRECT_PROMPT_VERSION,
    PERSON_COLUMNS,
    PROMPT_VERSION,
    CompanyObservationBatch,
    CompanyPersonStatus,
    CompanyPersonWork,
    DraftPersonObservation,
    ExistingPersonProfile,
    LlmCompanyPeopleResponse,
    LlmCompanyPeopleResult,
    LlmCompanyPersonSuggestion,
    batch_company_observations,
    build_company_people_request,
    build_company_statistics_sql,
    build_pending_companies_sql,
    normalize_companies,
    observation_role_bucket,
    request_company_people,
    validate_company_people_response,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
NOW = datetime(2026, 8, 19, 12, tzinfo=UTC)
COMPANY_ID = "5565200028"


def _observation(
    source: str,
    *,
    name: str,
    role: str,
    index: int,
) -> DraftPersonObservation:
    source_value: dict[str, object]
    if source == "bolagsverket":
        first_name, _, last_name = name.partition(" ")
        source_value = {
            "first_name": first_name,
            "last_name": last_name,
            "role_kind": role,
            "role_original": role,
        }
    elif source == "wikidata":
        source_value = {
            "name": name,
            "description": f"Profile for {name}",
            "role_property": role,
        }
    else:
        source_value = {
            "name": name,
            "role": role,
            "role_category": role,
        }
    return DraftPersonObservation(
        draft_id=uuid.UUID(int=index),
        source=source,
        fiscal_year=2025,
        source_observed_at=NOW + timedelta(seconds=index),
        source_value=source_value,
    )


def _company(
    *observations: DraftPersonObservation,
    previous_profiles: tuple[ExistingPersonProfile, ...] = (),
) -> CompanyPersonWork:
    return CompanyPersonWork(
        status=CompanyPersonStatus(
            company_id=COMPANY_ID,
            source_count=len({observation.source for observation in observations}),
            observation_count=len(observations),
            draft_ids=tuple(sorted(item.draft_id for item in observations)),
        ),
        observations=tuple(observations),
        previous_profiles=previous_profiles,
    )


def _llm_result(
    people: list[LlmCompanyPersonSuggestion],
    *,
    prompt_tokens: int = 10,
    completion_tokens: int = 5,
) -> LlmCompanyPeopleResult:
    return LlmCompanyPeopleResult(
        response=LlmCompanyPeopleResponse(people=people),
        model_provider="deepseek",
        model_name="deepseek-v4-flash",
        prompt_version=PROMPT_VERSION,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
    )


def test_main_table_migration_matches_insert_contract() -> None:
    sql = (MIGRATIONS_DIR / "000291_corpscout_se_company_person.up.sql").read_text(
        encoding="utf-8"
    )

    for column in PERSON_COLUMNS:
        assert f"    {column} " in sql
    assert "draft_set_hash FixedString(64) MATERIALIZED" in sql
    assert "profile_hash FixedString(64) MATERIALIZED" in sql


def test_company_sql_compares_all_draft_ids_at_company_boundary() -> None:
    statistics_sql = build_company_statistics_sql()
    pending_sql = build_pending_companies_sql()

    for sql in (statistics_sql, pending_sql):
        assert "GROUP BY company_id" in sql
        assert "uniqExact(source) AS source_count" in sql
        assert "arraySort(groupUniqArray(draft_id)) AS draft_ids" in sql
        assert "arrayFlatten(groupArray(draft_ids))" in sql
        assert "published.draft_ids = drafts.draft_ids AS is_unchanged" in sql
    assert "countIf(is_unchanged) AS skipped_company_count" in statistics_sql
    assert "LIMIT %(max_companies)s" in pending_sql


@pytest.mark.parametrize(
    ("source", "role", "expected"),
    [
        ("bolagsverket", "chairman", "board_chair"),
        ("esef", "chief_executive", "chief_executive_officer"),
        ("wikidata", "P3320", "board_member"),
        ("wikidata", "P112", "founder"),
    ],
)
def test_observation_role_bucket_uses_source_role_fields(
    source: str,
    role: str,
    expected: str,
) -> None:
    observation = _observation(
        source,
        name="Test Person",
        role=role,
        index=1,
    )

    assert observation_role_bucket(observation) == expected


def test_small_company_is_one_request_even_with_multiple_roles() -> None:
    observations = (
        _observation("esef", name="Anna Andersson", role="board_member", index=1),
        _observation("esef", name="Erik Eriksson", role="executive", index=2),
        _observation("wikidata", name="Anna Andersson", role="P3320", index=3),
    )

    batches = batch_company_observations(
        observations,
        maximum_observations_per_request=3,
    )

    assert len(batches) == 1
    assert batches[0].role_bucket == "all"
    assert {item.draft_id for item in batches[0].observations} == {
        item.draft_id for item in observations
    }


def test_large_company_splits_by_role_then_chunks_without_dropping_rows() -> None:
    observations = (
        _observation("esef", name="Board One", role="board_member", index=1),
        _observation("esef", name="Board Two", role="board_member", index=2),
        _observation("esef", name="Board Three", role="board_member", index=3),
        _observation("esef", name="Exec One", role="executive", index=4),
        _observation("wikidata", name="Exec Two", role="executive", index=5),
    )

    batches = batch_company_observations(
        observations,
        maximum_observations_per_request=2,
    )

    assert [(batch.role_bucket, len(batch.observations)) for batch in batches] == [
        ("board_member", 2),
        ("board_member", 1),
        ("executive", 2),
    ]
    assert [batch.batch_index for batch in batches] == [1, 2, 3]
    assert all(batch.batch_count == 3 for batch in batches)
    returned_ids = [
        observation.draft_id for batch in batches for observation in batch.observations
    ]
    assert len(returned_ids) == len(set(returned_ids)) == len(observations)
    assert set(returned_ids) == {item.draft_id for item in observations}


def test_request_formatter_contains_batch_previous_profiles_and_provenance() -> None:
    observation = _observation(
        "esef", name="David Mindus", role="chief_executive", index=1
    )
    batch = batch_company_observations(
        [observation], maximum_observations_per_request=10
    )[0]
    previous = ExistingPersonProfile(
        person_id=uuid.UUID(int=99),
        name="David Gustaf Mindus",
        description="Previous description.",
        draft_ids=(uuid.UUID(int=98),),
        created_at=NOW,
    )

    request = build_company_people_request(
        company_id=COMPANY_ID,
        batch=batch,
        previous_profiles=[previous],
        model="deepseek-v4-flash",
    )
    payload = json.loads(request["messages"][1]["content"])

    assert payload["company_id"] == COMPANY_ID
    assert payload["batch"] == {
        "role_bucket": "all",
        "batch_index": 1,
        "batch_count": 1,
    }
    assert payload["previous_profiles"][0]["person_id"] == str(previous.person_id)
    assert payload["source_observations"][0]["draft_id"] == str(observation.draft_id)
    assert payload["source_observations"][0]["role_bucket"] == (
        "chief_executive_officer"
    )
    assert request["response_format"] == {"type": "json_object"}


def test_pydantic_response_rejects_duplicate_draft_assignment() -> None:
    draft_id = uuid.uuid4()

    with pytest.raises(ValidationError, match="assigned to only one person"):
        LlmCompanyPeopleResponse(
            people=[
                LlmCompanyPersonSuggestion(
                    name="First Person",
                    draft_ids=[draft_id],
                ),
                LlmCompanyPersonSuggestion(
                    name="Second Person",
                    draft_ids=[draft_id],
                ),
            ]
        )


def test_response_validation_rejects_missing_or_unexpected_draft_ids() -> None:
    observation = _observation(
        "esef", name="David Mindus", role="chief_executive", index=1
    )
    batch = CompanyObservationBatch(
        role_bucket="all",
        batch_index=1,
        batch_count=1,
        observations=(observation,),
    )
    response = LlmCompanyPeopleResponse(
        people=[
            LlmCompanyPersonSuggestion(
                name="David Mindus",
                draft_ids=[uuid.UUID(int=2)],
            )
        ]
    )

    with pytest.raises(ValueError, match="missing=.*unexpected="):
        validate_company_people_response(
            response,
            batch=batch,
            previous_profiles=[],
        )


def test_response_validation_rejects_unknown_previous_person_id() -> None:
    observation = _observation(
        "esef", name="David Mindus", role="chief_executive", index=1
    )
    batch = CompanyObservationBatch("all", 1, 1, (observation,))
    response = LlmCompanyPeopleResponse(
        people=[
            LlmCompanyPersonSuggestion(
                existing_person_id=uuid.UUID(int=88),
                name="David Mindus",
                draft_ids=[observation.draft_id],
            )
        ]
    )

    with pytest.raises(ValueError, match="unknown existing_person_id"):
        validate_company_people_response(
            response,
            batch=batch,
            previous_profiles=[],
        )


def test_llm_contract_failure_is_retried_with_exact_required_draft_ids() -> None:
    observations = (
        _observation("esef", name="First Person", role="auditor", index=1),
        _observation("wikidata", name="Second Person", role="P3320", index=2),
    )
    batch = CompanyObservationBatch("all", 1, 1, observations)
    contents = iter(
        (
            json.dumps(
                {
                    "people": [
                        {
                            "name": "First Person",
                            "description": None,
                            "draft_ids": [str(observations[0].draft_id)],
                        }
                    ]
                }
            ),
            json.dumps(
                {
                    "people": [
                        {
                            "name": "First Person",
                            "description": None,
                            "draft_ids": [str(observations[0].draft_id)],
                        },
                        {
                            "name": "Second Person",
                            "description": None,
                            "draft_ids": [str(observations[1].draft_id)],
                        },
                    ]
                }
            ),
        )
    )
    requests: list[dict[str, object]] = []

    class FakeCompletions:
        def create(self, **request: object) -> SimpleNamespace:
            requests.append(request)
            return SimpleNamespace(
                choices=[
                    SimpleNamespace(message=SimpleNamespace(content=next(contents)))
                ],
                usage=SimpleNamespace(prompt_tokens=10, completion_tokens=5),
            )

    client = SimpleNamespace(
        chat=SimpleNamespace(completions=FakeCompletions()),
    )

    result = request_company_people(
        client,
        company_id=COMPANY_ID,
        batch=batch,
        previous_profiles=(),
        model="deepseek-v4-flash",
    )

    assert len(requests) == 2
    assert result.contract_retry_count == 1
    assert result.prompt_tokens == 20
    assert result.completion_tokens == 10
    correction = requests[1]["messages"][-1]["content"]
    assert "failed validation" in correction
    assert all(str(item.draft_id) in correction for item in observations)


def test_single_source_company_is_copied_without_llm() -> None:
    observation = _observation(
        "esef", name="David Mindus", role="chief_executive", index=1
    )
    company = _company(observation)

    writes, metrics = normalize_companies(
        [company],
        llm_suggester=None,
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert len(writes) == 1
    assert writes[0].name == "David Mindus"
    assert writes[0].model_provider == "deterministic"
    assert writes[0].prompt_version == DIRECT_PROMPT_VERSION
    assert metrics["direct_company_count"] == 1
    assert metrics["directly_inserted_count"] == 1
    assert metrics["llm_company_count"] == 0
    assert metrics["llm_request_count"] == 0


def test_multi_source_company_is_sent_once_when_below_limit() -> None:
    observations = (
        _observation(
            "bolagsverket",
            name="David Mindus",
            role="ceo",
            index=1,
        ),
        _observation(
            "esef",
            name="David Mindus",
            role="chief_executive",
            index=2,
        ),
    )
    company = _company(*observations)
    calls: list[CompanyObservationBatch] = []

    def suggest(
        company_id: str,
        batch: CompanyObservationBatch,
        previous_profiles: tuple[ExistingPersonProfile, ...],
    ) -> LlmCompanyPeopleResult:
        assert company_id == COMPANY_ID
        assert previous_profiles == ()
        calls.append(batch)
        return _llm_result(
            [
                LlmCompanyPersonSuggestion(
                    name="David Gustaf Mindus",
                    description="Chief executive officer of the company.",
                    draft_ids=[item.draft_id for item in batch.observations],
                )
            ]
        )

    writes, metrics = normalize_companies(
        [company],
        llm_suggester=suggest,
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert len(calls) == 1
    assert calls[0].role_bucket == "all"
    assert len(writes) == 1
    assert set(writes[0].draft_ids) == {item.draft_id for item in observations}
    assert writes[0].model_name == "deepseek-v4-flash"
    assert metrics["llm_company_count"] == 1
    assert metrics["llm_request_count"] == 1
    assert metrics["llm_observation_count"] == 2
    assert metrics["llm_prompt_tokens"] == 10
    assert metrics["llm_completion_tokens"] == 5


def test_large_company_sends_role_batches_and_all_observations() -> None:
    observations = (
        _observation("esef", name="Board One", role="board_member", index=1),
        _observation("wikidata", name="Board Two", role="P3320", index=2),
        _observation("esef", name="Board Three", role="board_member", index=3),
        _observation("esef", name="Exec One", role="executive", index=4),
        _observation("wikidata", name="Exec Two", role="executive", index=5),
    )
    company = _company(*observations)
    received_ids: list[uuid.UUID] = []

    def suggest(
        company_id: str,
        batch: CompanyObservationBatch,
        previous_profiles: tuple[ExistingPersonProfile, ...],
    ) -> LlmCompanyPeopleResult:
        del company_id, previous_profiles
        received_ids.extend(item.draft_id for item in batch.observations)
        return _llm_result(
            [
                LlmCompanyPersonSuggestion(
                    name=str(item.source_value["name"]),
                    draft_ids=[item.draft_id],
                )
                for item in batch.observations
            ]
        )

    writes, metrics = normalize_companies(
        [company],
        llm_suggester=suggest,
        maximum_observations_per_request=2,
        created_at=NOW,
    )

    assert set(received_ids) == {item.draft_id for item in observations}
    assert len(received_ids) == len(set(received_ids)) == 5
    assert len(writes) == 5
    assert metrics["llm_request_count"] == 3
    assert metrics["llm_role_batch_count"] == 3
    assert metrics["llm_observation_count"] == 5


def test_main_asset_depends_on_draft_and_combined_job_runs_both() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    person_asset = repository.asset_graph.get(
        dg.AssetKey("se_company_person_clickhouse")
    )

    assert person_asset.parent_keys == {
        dg.AssetKey("se_company_person_draft_clickhouse")
    }
    job_keys = {
        key.path[-1]
        for key in repository.get_job(
            "se_company_person_job"
        ).asset_layer.executable_asset_keys
    }
    assert job_keys == {
        "se_company_person_draft_clickhouse",
        "se_company_person_clickhouse",
    }
    publish_job_keys = {
        key.path[-1]
        for key in repository.get_job(
            "se_company_person_publish_job"
        ).asset_layer.executable_asset_keys
    }
    assert publish_job_keys == {"se_company_person_clickhouse"}
