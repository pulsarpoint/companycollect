import json
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import dagster as dg
import pytest
from pydantic import ValidationError

from dagster_v3.defs.company_people.corrections import (
    PersonCorrection,
    StoredSuggestion,
)
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
    SuggestionWrite,
    batch_company_observations,
    build_company_people_request,
    build_company_statistics_sql,
    build_pending_companies_sql,
    normalize_companies,
    observation_role_bucket,
    person_id_for,
    request_company_people,
    request_input_hash,
    validate_company_people_response,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
NOW = datetime(2026, 8, 19, 12, tzinfo=UTC)
COMPANY_ID = "5565200028"
# Insert columns added to se_company_person by migration 000295, in the order
# that migration's AFTER clauses place them.
CORRECTION_PERSON_COLUMNS = (
    "correction_ids",
    "suggestion_id",
    "merged_into_person_id",
)


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
    # Every insert column is declared by the migration that introduced it: the
    # original table in 000291, the three provenance columns in 000295.
    created = (MIGRATIONS_DIR / "000291_corpscout_se_company_person.up.sql").read_text(
        encoding="utf-8"
    )
    altered = (
        MIGRATIONS_DIR / "000295_corpscout_se_company_person_corrections.up.sql"
    ).read_text(encoding="utf-8")

    for column in PERSON_COLUMNS:
        if column in CORRECTION_PERSON_COLUMNS:
            assert f"ADD COLUMN IF NOT EXISTS {column} " in altered
        else:
            assert f"    {column} " in created
    assert "draft_set_hash FixedString(64) MATERIALIZED" in created
    assert "profile_hash FixedString(64) MATERIALIZED" in created


def test_company_sql_compares_all_draft_ids_at_company_boundary() -> None:
    statistics_sql = build_company_statistics_sql()
    pending_sql = build_pending_companies_sql()

    for sql in (statistics_sql, pending_sql):
        assert "GROUP BY company_id" in sql
        assert "uniqExact(source) AS source_count" in sql
        assert "arraySort(groupUniqArray(draft_id)) AS draft_ids" in sql
        assert "arrayFlatten(groupArray(draft_ids))" in sql
        # The published draft set is compared in full; the correction half of
        # is_unchanged is pinned by the effective-corrections test below.
        assert "published.draft_ids = drafts.draft_ids" in sql
        assert "AS is_unchanged" in sql
    assert "countIf(is_unchanged) AS skipped_company_count" in statistics_sql
    assert "LIMIT %(max_companies)s" in pending_sql


def test_company_status_projects_the_join_key_instead_of_drafts_star() -> None:
    """`drafts.*` drops company_id after the second `USING` join (ClickHouse 26.5)."""
    for sql in (build_company_statistics_sql(), build_pending_companies_sql()):
        assert "drafts.*" not in sql
        for column in (
            "company_id",
            "source_count",
            "observation_count",
            "draft_ids",
        ):
            assert f"drafts.{column} AS {column}," in sql


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
    # The recorded hash is taken before any repair turn is appended, so it still
    # matches the hash normalization uses to look up stored suggestions.
    assert result.input_hash == request_input_hash(
        build_company_people_request(
            company_id=COMPANY_ID,
            batch=batch,
            previous_profiles=(),
            model="deepseek-v4-flash",
        )
    )
    correction = requests[1]["messages"][-1]["content"]
    assert "failed validation" in correction
    assert all(str(item.draft_id) in correction for item in observations)


def test_single_source_company_is_copied_without_llm() -> None:
    observation = _observation(
        "esef", name="David Mindus", role="chief_executive", index=1
    )
    company = _company(observation)

    writes, _suggestions, metrics, _notes = normalize_companies(
        [company],
        llm_suggester=None,
        llm_model=None,
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
        request: dict[str, object],
    ) -> LlmCompanyPeopleResult:
        assert company_id == COMPANY_ID
        assert previous_profiles == ()
        assert request["model"] == "deepseek-v4-flash"
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

    writes, _suggestions, metrics, _notes = normalize_companies(
        [company],
        llm_suggester=suggest,
        llm_model="deepseek-v4-flash",
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
        request: dict[str, object],
    ) -> LlmCompanyPeopleResult:
        del company_id, previous_profiles, request
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

    writes, _suggestions, metrics, _notes = normalize_companies(
        [company],
        llm_suggester=suggest,
        llm_model="deepseek-v4-flash",
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
        "se_company_person_role_draft_clickhouse",
        "se_company_person_clickhouse",
        "se_company_person_role_clickhouse",
    }
    publish_job_keys = {
        key.path[-1]
        for key in repository.get_job(
            "se_company_person_publish_job"
        ).asset_layer.executable_asset_keys
    }
    assert publish_job_keys == {"se_company_person_clickhouse"}


def test_request_input_hash_is_stable_and_covers_every_message() -> None:
    observation = _observation("esef", name="David Mindus", role="chief_executive", index=1)
    batch = batch_company_observations([observation], maximum_observations_per_request=10)[0]
    request = build_company_people_request(
        company_id=COMPANY_ID, batch=batch, previous_profiles=[], model="deepseek-v4-flash"
    )

    first = request_input_hash(request)
    request["messages"].append({"role": "assistant", "content": "{}"})

    assert len(first) == 64
    assert request_input_hash(request) != first  # an extra message is a new payload
    assert request_input_hash(
        build_company_people_request(
            company_id=COMPANY_ID, batch=batch, previous_profiles=[], model="deepseek-v4-flash"
        )
    ) == first


def test_multi_source_company_records_one_suggestion_per_person() -> None:
    observations = (
        _observation("bolagsverket", name="David Mindus", role="ceo", index=1),
        _observation("esef", name="David Mindus", role="chief_executive", index=2),
    )
    company = _company(*observations)

    def suggest(company_id, batch, previous_profiles, request):
        return _llm_result(
            [
                LlmCompanyPersonSuggestion(
                    name="David Gustaf Mindus",
                    description="CEO.",
                    draft_ids=[item.draft_id for item in batch.observations],
                )
            ]
        )

    writes, suggestions, metrics, _notes = normalize_companies(
        [company],
        llm_suggester=suggest,
        llm_model="deepseek-v4-flash",
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert len(writes) == 1
    assert len(suggestions) == 1
    assert isinstance(suggestions[0], SuggestionWrite)
    assert suggestions[0].person_id == writes[0].person_id
    assert writes[0].suggestion_id == suggestions[0].suggestion_id
    assert json.loads(suggestions[0].suggestion_json)["name"] == "David Gustaf Mindus"
    assert len(suggestions[0].input_hash) == 64
    assert metrics["llm_request_count"] == 1


def test_stored_suggestion_with_current_input_hash_skips_the_llm() -> None:
    observations = (
        _observation("bolagsverket", name="David Mindus", role="ceo", index=1),
        _observation("esef", name="David Mindus", role="chief_executive", index=2),
    )
    batch = batch_company_observations(observations, maximum_observations_per_request=10)[0]
    input_hash = request_input_hash(
        build_company_people_request(
            company_id=COMPANY_ID, batch=batch, previous_profiles=[], model="deepseek-v4-flash"
        )
    )
    stored = StoredSuggestion(
        suggestion_id=uuid.UUID(int=500),
        company_id=COMPANY_ID,
        person_id=uuid.UUID(int=1000),
        input_hash=input_hash,
        draft_ids=tuple(sorted(item.draft_id for item in observations)),
        name="David Gustaf Mindus",
        description="CEO.",
        existing_person_id=None,
        model_provider="deepseek",
        model_name="deepseek-v4-flash",
        prompt_version=PROMPT_VERSION,
        created_at=NOW,
    )
    company = CompanyPersonWork(
        status=_company(*observations).status,
        observations=observations,
        previous_profiles=(),
        suggestions=(stored,),
    )
    calls: list[str] = []

    def suggest(company_id, batch, previous_profiles, request):
        calls.append(company_id)
        raise AssertionError("LLM must not be called when a suggestion is current")

    writes, suggestions, metrics, _notes = normalize_companies(
        [company],
        llm_suggester=suggest,
        llm_model="deepseek-v4-flash",
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert calls == []
    assert suggestions == []
    assert len(writes) == 1
    assert writes[0].name == "David Gustaf Mindus"
    assert writes[0].suggestion_id == uuid.UUID(int=500)
    assert writes[0].model_provider == "deepseek"
    assert writes[0].model_name == "deepseek-v4-flash"
    assert writes[0].prompt_version == PROMPT_VERSION
    assert metrics["llm_reused_batch_count"] == 1
    assert metrics["llm_request_count"] == 0


def test_suggester_answering_a_different_request_is_rejected() -> None:
    observations = (
        _observation("bolagsverket", name="David Mindus", role="ceo", index=1),
        _observation("esef", name="David Mindus", role="chief_executive", index=2),
    )
    company = _company(*observations)

    def suggest(company_id, batch, previous_profiles, request):
        result = _llm_result(
            [
                LlmCompanyPersonSuggestion(
                    name="David Gustaf Mindus",
                    draft_ids=[item.draft_id for item in batch.observations],
                )
            ]
        )
        return replace(result, input_hash="f" * 64)

    with pytest.raises(ValueError, match="LLM input hash mismatch"):
        normalize_companies(
            [company],
            llm_suggester=suggest,
            llm_model="deepseek-v4-flash",
            maximum_observations_per_request=10,
            created_at=NOW,
        )


def _correction(
    index: int,
    kind: str,
    *,
    subject: uuid.UUID,
    target: uuid.UUID | None = None,
    draft_ids: tuple[uuid.UUID, ...] = (),
    payload: dict[str, object] | None = None,
    evidence_hash: str = "0" * 64,
) -> PersonCorrection:
    return PersonCorrection(
        correction_id=uuid.UUID(int=9000 + index),
        company_id=COMPANY_ID,
        kind=kind,
        subject_person_id=subject,
        target_person_id=target,
        draft_ids=draft_ids,
        payload=payload or {},
        evidence_hash=evidence_hash,
        supersedes_correction_id=None,
        created_at=NOW + timedelta(minutes=index),
    )


def _previous(
    name: str,
    draft_ids: tuple[uuid.UUID, ...],
    draft_set_hash: str,
) -> ExistingPersonProfile:
    return ExistingPersonProfile(
        person_id=person_id_for(COMPANY_ID, name),
        name=name,
        description=None,
        draft_ids=tuple(sorted(draft_ids)),
        created_at=NOW - timedelta(days=1),
        draft_set_hash=draft_set_hash,
    )


def test_override_field_wins_over_deterministic_name_and_records_provenance() -> None:
    observation = _observation("bolagsverket", name="Anna Svensson", role="ceo", index=1)
    subject = person_id_for(COMPANY_ID, "Anna Svensson")
    previous = _previous("Anna Svensson", (observation.draft_id,), "a" * 64)
    company = CompanyPersonWork(
        status=_company(observation).status,
        observations=(observation,),
        previous_profiles=(previous,),
        corrections=(
            _correction(
                1,
                "override_field",
                subject=subject,
                payload={"name": "Anna K. Svensson"},
                evidence_hash="a" * 64,
            ),
        ),
    )

    writes, _suggestions, metrics, _notes = normalize_companies(
        [company],
        llm_suggester=None,
        llm_model=None,
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert len(writes) == 1
    assert writes[0].name == "Anna K. Svensson"
    assert writes[0].correction_ids == (uuid.UUID(int=9001),)
    assert writes[0].model_provider == "deterministic"
    assert metrics["applied_correction_count"] == 1
    assert metrics["stale_correction_count"] == 0


def test_override_is_stale_when_evidence_hash_moved() -> None:
    observation = _observation("bolagsverket", name="Anna Svensson", role="ceo", index=1)
    subject = person_id_for(COMPANY_ID, "Anna Svensson")
    previous = _previous("Anna Svensson", (observation.draft_id,), "b" * 64)
    company = CompanyPersonWork(
        status=_company(observation).status,
        observations=(observation,),
        previous_profiles=(previous,),
        corrections=(
            _correction(
                1,
                "override_field",
                subject=subject,
                payload={"name": "Anna K. Svensson"},
                evidence_hash="a" * 64,
            ),
        ),
    )

    writes, _suggestions, metrics, _notes = normalize_companies(
        [company],
        llm_suggester=None,
        llm_model=None,
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert writes == []  # unchanged profile, nothing applied
    assert metrics["stale_correction_count"] == 1
    assert metrics["applied_correction_count"] == 0


def test_merge_persons_moves_evidence_and_tombstones_the_subject() -> None:
    first = _observation("bolagsverket", name="Anna Svensson", role="ceo", index=1)
    second = _observation(
        "bolagsverket", name="Anna Svensson-Berg", role="board_member", index=2
    )
    # The two names must key differently (anna|svensson vs anna|svensson-berg),
    # otherwise the deterministic pass folds them into one profile and the merge
    # subject never exists in the run.
    subject = person_id_for(COMPANY_ID, "Anna Svensson-Berg")
    target = person_id_for(COMPANY_ID, "Anna Svensson")
    previous_subject = _previous("Anna Svensson-Berg", (second.draft_id,), "c" * 64)
    previous_target = _previous("Anna Svensson", (first.draft_id,), "d" * 64)
    company = CompanyPersonWork(
        status=_company(first, second).status,
        observations=(first, second),
        previous_profiles=(previous_target, previous_subject),
        corrections=(
            _correction(
                1,
                "merge_persons",
                subject=subject,
                target=target,
                evidence_hash="c" * 64,
            ),
        ),
    )

    writes, _suggestions, metrics, _notes = normalize_companies(
        [company],
        llm_suggester=None,
        llm_model=None,
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    by_id = {write.person_id: write for write in writes}
    assert set(by_id[target].draft_ids) == {first.draft_id, second.draft_id}
    assert by_id[target].correction_ids == (uuid.UUID(int=9001),)
    assert by_id[subject].merged_into_person_id == target
    assert by_id[subject].draft_ids == (second.draft_id,)
    assert metrics["applied_correction_count"] == 1


def test_reassign_draft_requires_the_draft_on_the_subject() -> None:
    first = _observation("bolagsverket", name="Anna Svensson", role="ceo", index=1)
    second = _observation(
        "bolagsverket", name="Erik Eriksson", role="board_member", index=2
    )
    anna = person_id_for(COMPANY_ID, "Anna Svensson")
    erik = person_id_for(COMPANY_ID, "Erik Eriksson")
    company = CompanyPersonWork(
        status=_company(first, second).status,
        observations=(first, second),
        previous_profiles=(
            _previous("Anna Svensson", (first.draft_id,), "a" * 64),
            _previous("Erik Eriksson", (second.draft_id,), "b" * 64),
        ),
        corrections=(
            _correction(
                1,
                "reassign_draft",
                subject=anna,
                target=erik,
                draft_ids=(uuid.UUID(int=77),),
                evidence_hash="a" * 64,
            ),
        ),
    )

    writes, _suggestions, metrics, _notes = normalize_companies(
        [company],
        llm_suggester=None,
        llm_model=None,
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert writes == []
    assert metrics["stale_correction_count"] == 1


def test_split_person_creates_a_new_deterministic_person_from_payload_name() -> None:
    first = _observation("bolagsverket", name="Anna Svensson", role="ceo", index=1)
    second = _observation("bolagsverket", name="Anna Svensson", role="auditor", index=2)
    anna = person_id_for(COMPANY_ID, "Anna Svensson")
    company = CompanyPersonWork(
        status=_company(first, second).status,
        observations=(first, second),
        previous_profiles=(
            _previous("Anna Svensson", (first.draft_id, second.draft_id), "a" * 64),
        ),
        corrections=(
            _correction(
                1,
                "split_person",
                subject=anna,
                draft_ids=(second.draft_id,),
                payload={"name": "Anna Svensson (auditor)"},
                evidence_hash="a" * 64,
            ),
        ),
    )

    writes, _suggestions, _metrics, _notes = normalize_companies(
        [company],
        llm_suggester=None,
        llm_model=None,
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    by_id = {write.person_id: write for write in writes}
    new_id = person_id_for(COMPANY_ID, "Anna Svensson (auditor)")
    assert by_id[anna].draft_ids == (first.draft_id,)
    assert by_id[new_id].draft_ids == (second.draft_id,)
    assert by_id[new_id].name == "Anna Svensson (auditor)"


def test_company_status_sql_includes_effective_corrections_and_processed_guard() -> None:
    statistics_sql = build_company_statistics_sql()
    pending_sql = build_pending_companies_sql()

    for sql in (statistics_sql, pending_sql):
        assert "effective_company_corrections AS (" in sql
        assert "arrayMap(id -> toString(id), correction_ids)" in sql
        assert "published.correction_ids = corrections.correction_ids" in sql
    assert "AND company_id > %(after_company_id)s" in pending_sql


def _stored_suggestion(
    observations: tuple[DraftPersonObservation, ...],
) -> StoredSuggestion:
    """A suggestion whose input_hash is the one this company's batch produces."""
    batch = batch_company_observations(
        observations, maximum_observations_per_request=10
    )[0]
    return StoredSuggestion(
        suggestion_id=uuid.UUID(int=500),
        company_id=COMPANY_ID,
        person_id=uuid.UUID(int=1000),
        input_hash=request_input_hash(
            build_company_people_request(
                company_id=COMPANY_ID,
                batch=batch,
                previous_profiles=[],
                model="deepseek-v4-flash",
            )
        ),
        draft_ids=tuple(sorted(item.draft_id for item in observations)),
        name="David Gustaf Mindus",
        description="CEO.",
        existing_person_id=None,
        model_provider="deepseek",
        model_name="deepseek-v4-flash",
        prompt_version=PROMPT_VERSION,
        created_at=NOW,
    )


def _refuse_llm(company_id, batch, previous_profiles, request):
    raise AssertionError("the stored suggestion must be reused, not re-requested")


def test_approve_suggestion_pins_the_suggestion_and_ignores_an_unknown_one() -> None:
    observations = (
        _observation("bolagsverket", name="David Mindus", role="ceo", index=1),
        _observation("esef", name="David Mindus", role="chief_executive", index=2),
    )
    stored = _stored_suggestion(observations)
    company = CompanyPersonWork(
        status=_company(*observations).status,
        observations=observations,
        previous_profiles=(),
        suggestions=(stored,),
        corrections=(
            _correction(
                1,
                "approve_suggestion",
                subject=stored.person_id,
                payload={"suggestion_id": str(uuid.UUID(int=404))},
            ),
            _correction(
                2,
                "approve_suggestion",
                subject=stored.person_id,
                payload={"suggestion_id": str(stored.suggestion_id)},
            ),
        ),
    )

    writes, _suggestions, metrics, _notes = normalize_companies(
        [company],
        llm_suggester=_refuse_llm,
        llm_model="deepseek-v4-flash",
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert len(writes) == 1
    assert writes[0].person_id == stored.person_id
    assert writes[0].name == "David Gustaf Mindus"
    assert writes[0].suggestion_id == stored.suggestion_id
    assert writes[0].correction_ids == (uuid.UUID(int=9002),)
    assert metrics["applied_correction_count"] == 1
    assert metrics["stale_correction_count"] == 1  # the unknown suggestion id


def test_reject_suggestion_falls_back_to_the_deterministic_name() -> None:
    observations = (
        _observation("bolagsverket", name="David Mindus", role="ceo", index=1),
        _observation("esef", name="David Mindus", role="chief_executive", index=2),
    )
    stored = _stored_suggestion(observations)
    company = CompanyPersonWork(
        status=_company(*observations).status,
        observations=observations,
        previous_profiles=(),
        suggestions=(stored,),
        corrections=(
            _correction(
                1,
                "reject_suggestion",
                subject=stored.person_id,
                payload={"suggestion_id": str(stored.suggestion_id)},
            ),
        ),
    )

    writes, _suggestions, metrics, _notes = normalize_companies(
        [company],
        llm_suggester=_refuse_llm,
        llm_model="deepseek-v4-flash",
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert len(writes) == 1
    assert writes[0].name == "David Mindus"
    assert writes[0].description is None
    assert writes[0].suggestion_id is None
    assert writes[0].model_provider == "deterministic"
    assert writes[0].model_name == "rejected-suggestion"
    assert writes[0].correction_ids == (uuid.UUID(int=9001),)
    assert metrics["applied_correction_count"] == 1


def test_a_later_approval_overrides_an_earlier_rejection() -> None:
    """Spec §4.1: approve and reject share a step, so the newer decision wins."""
    observations = (
        _observation("bolagsverket", name="David Mindus", role="ceo", index=1),
        _observation("esef", name="David Mindus", role="chief_executive", index=2),
    )
    stored = _stored_suggestion(observations)
    company = CompanyPersonWork(
        status=_company(*observations).status,
        observations=observations,
        previous_profiles=(),
        suggestions=(stored,),
        corrections=(
            _correction(
                1,
                "reject_suggestion",
                subject=stored.person_id,
                payload={"suggestion_id": str(stored.suggestion_id)},
            ),
            _correction(
                2,
                "approve_suggestion",
                subject=stored.person_id,
                payload={"suggestion_id": str(stored.suggestion_id)},
            ),
        ),
    )

    writes, _suggestions, metrics, _notes = normalize_companies(
        [company],
        llm_suggester=_refuse_llm,
        llm_model="deepseek-v4-flash",
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert len(writes) == 1
    assert writes[0].name == "David Gustaf Mindus"
    assert writes[0].suggestion_id == stored.suggestion_id
    assert writes[0].correction_ids == (
        uuid.UUID(int=9001),
        uuid.UUID(int=9002),
    )
    assert metrics["applied_correction_count"] == 2
    assert metrics["stale_correction_count"] == 0


def test_approve_is_stale_when_the_suggestion_input_hash_is_not_current() -> None:
    observations = (
        _observation("bolagsverket", name="David Mindus", role="ceo", index=1),
        _observation("esef", name="David Mindus", role="chief_executive", index=2),
    )
    current = _stored_suggestion(observations)
    outdated = replace(current, suggestion_id=uuid.UUID(int=501), input_hash="f" * 64)
    company = CompanyPersonWork(
        status=_company(*observations).status,
        observations=observations,
        previous_profiles=(),
        suggestions=(current, outdated),
        corrections=(
            _correction(
                1,
                "approve_suggestion",
                subject=current.person_id,
                payload={"suggestion_id": str(outdated.suggestion_id)},
            ),
        ),
    )

    writes, _suggestions, metrics, _notes = normalize_companies(
        [company],
        llm_suggester=_refuse_llm,
        llm_model="deepseek-v4-flash",
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert len(writes) == 1
    assert writes[0].suggestion_id == current.suggestion_id
    assert writes[0].correction_ids == ()
    assert metrics["applied_correction_count"] == 0
    assert metrics["stale_correction_count"] == 1


def test_approve_that_would_strip_another_person_bare_is_stale() -> None:
    first = _observation("bolagsverket", name="David Mindus", role="ceo", index=1)
    second = _observation("esef", name="Erik Eriksson", role="executive", index=2)
    observations = (first, second)
    published = (
        ExistingPersonProfile(
            person_id=uuid.UUID(int=1000),
            name="David Gustaf Mindus",
            description="CEO.",
            draft_ids=(first.draft_id,),
            created_at=NOW - timedelta(days=1),
            draft_set_hash="a" * 64,
            suggestion_id=uuid.UUID(int=500),
        ),
        ExistingPersonProfile(
            person_id=uuid.UUID(int=2000),
            name="Erik Eriksson",
            description=None,
            draft_ids=(second.draft_id,),
            created_at=NOW - timedelta(days=1),
            draft_set_hash="b" * 64,
            suggestion_id=uuid.UUID(int=501),
        ),
    )
    batch = batch_company_observations(
        observations, maximum_observations_per_request=10
    )[0]
    input_hash = request_input_hash(
        build_company_people_request(
            company_id=COMPANY_ID,
            batch=batch,
            previous_profiles=published,
            model="deepseek-v4-flash",
        )
    )
    stored = tuple(
        StoredSuggestion(
            suggestion_id=profile.suggestion_id,
            company_id=COMPANY_ID,
            person_id=profile.person_id,
            input_hash=input_hash,
            draft_ids=profile.draft_ids,
            name=profile.name,
            description=profile.description,
            existing_person_id=None,
            model_provider="deepseek",
            model_name="deepseek-v4-flash",
            prompt_version=PROMPT_VERSION,
            created_at=NOW,
        )
        for profile in published
    )
    company = CompanyPersonWork(
        status=_company(*observations).status,
        observations=observations,
        previous_profiles=published,
        suggestions=stored,
        corrections=(
            # David's profile would take Erik's only observation.
            _correction(
                1,
                "approve_suggestion",
                subject=published[0].person_id,
                payload={"suggestion_id": str(published[1].suggestion_id)},
            ),
        ),
    )

    writes, _suggestions, metrics, _notes = normalize_companies(
        [company],
        llm_suggester=_refuse_llm,
        llm_model="deepseek-v4-flash",
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert writes == []  # nothing applied, so both profiles are unchanged
    assert metrics["applied_correction_count"] == 0
    assert metrics["stale_correction_count"] == 1
    assert metrics["invalid_profile_count"] == 0


def test_a_merge_tombstone_never_reaches_the_model_or_the_empty_guard() -> None:
    """After a merge both rows hold the drafts; the tombstone must not fail the run."""
    observations = (
        _observation("bolagsverket", name="David Mindus", role="ceo", index=1),
        _observation("esef", name="David Mindus", role="chief_executive", index=2),
    )
    draft_ids = tuple(sorted(item.draft_id for item in observations))
    target = ExistingPersonProfile(
        person_id=uuid.UUID(int=2000),
        name="David Gustaf Mindus",
        description="CEO.",
        draft_ids=draft_ids,
        created_at=NOW - timedelta(days=1),
        draft_set_hash="b" * 64,
    )
    tombstone = ExistingPersonProfile(
        person_id=uuid.UUID(int=1000),
        name="David Mindus",
        description=None,
        draft_ids=draft_ids,
        created_at=NOW - timedelta(days=2),
        draft_set_hash="a" * 64,
        merged_into_person_id=target.person_id,
    )
    company = CompanyPersonWork(
        status=_company(*observations).status,
        observations=observations,
        previous_profiles=(target, tombstone),
    )
    seen_profiles: list[tuple[ExistingPersonProfile, ...]] = []

    def suggest(
        company_id: str,
        batch: CompanyObservationBatch,
        previous_profiles: tuple[ExistingPersonProfile, ...],
        request: dict[str, object],
    ) -> LlmCompanyPeopleResult:
        seen_profiles.append(previous_profiles)
        return _llm_result(
            [
                LlmCompanyPersonSuggestion(
                    existing_person_id=target.person_id,
                    name="David Gustaf Mindus",
                    description="Chief executive officer.",
                    draft_ids=[item.draft_id for item in batch.observations],
                )
            ]
        )

    writes, _suggestions, metrics, notes = normalize_companies(
        [company],
        llm_suggester=suggest,
        llm_model="deepseek-v4-flash",
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert [profile.person_id for profile in seen_profiles[0]] == [target.person_id]
    assert [write.person_id for write in writes] == [target.person_id]
    assert writes[0].draft_ids == draft_ids
    assert metrics["emptied_profile_count"] == 0
    assert metrics["invalid_profile_count"] == 0
    assert notes[0].emptied_person_ids == ()


def test_a_model_that_drops_a_live_profile_skips_it_instead_of_failing() -> None:
    """The published row simply stays; one company must not fail the whole batch."""
    observations = (
        _observation("bolagsverket", name="David Mindus", role="ceo", index=1),
        _observation("esef", name="Erik Eriksson", role="executive", index=2),
    )
    dropped = ExistingPersonProfile(
        person_id=uuid.UUID(int=3000),
        name="Erik Eriksson",
        description=None,
        draft_ids=(observations[1].draft_id,),
        created_at=NOW - timedelta(days=1),
        draft_set_hash="c" * 64,
    )
    company = CompanyPersonWork(
        status=_company(*observations).status,
        observations=observations,
        previous_profiles=(dropped,),
    )

    def suggest(
        company_id: str,
        batch: CompanyObservationBatch,
        previous_profiles: tuple[ExistingPersonProfile, ...],
        request: dict[str, object],
    ) -> LlmCompanyPeopleResult:
        return _llm_result(
            [
                LlmCompanyPersonSuggestion(
                    name="David Gustaf Mindus",
                    description="One person after all.",
                    draft_ids=[item.draft_id for item in batch.observations],
                )
            ]
        )

    writes, _suggestions, metrics, notes = normalize_companies(
        [company],
        llm_suggester=suggest,
        llm_model="deepseek-v4-flash",
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert dropped.person_id not in {write.person_id for write in writes}
    assert len(writes) == 1
    assert metrics["emptied_profile_count"] == 1
    assert metrics["invalid_profile_count"] == 0
    assert notes[0].emptied_person_ids == (dropped.person_id,)
    assert notes[0].company_id == COMPANY_ID


def test_normalization_notes_carry_the_stale_correction_ids_per_company() -> None:
    observations = (
        _observation("bolagsverket", name="David Mindus", role="ceo", index=1),
    )
    company = CompanyPersonWork(
        status=_company(*observations).status,
        observations=observations,
        previous_profiles=(),
        corrections=(
            _correction(
                1,
                "override_field",
                subject=uuid.UUID(int=4242),
                payload={"name": "Nobody"},
            ),
        ),
    )

    _writes, _suggestions, metrics, notes = normalize_companies(
        [company],
        llm_suggester=None,
        llm_model=None,
        maximum_observations_per_request=10,
        created_at=NOW,
    )

    assert metrics["stale_correction_count"] == 1
    assert notes[0].company_id == COMPANY_ID
    assert notes[0].stale_correction_ids == (uuid.UUID(int=9001),)
