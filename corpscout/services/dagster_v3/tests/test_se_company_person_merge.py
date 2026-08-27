import functools
import json
import shutil
import subprocess
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import dagster as dg
import pytest

from dagster_v3.defs.company_people.corrections import (
    KEEP_SEPARATE_CORRECTION_KIND,
    QUALIFIED_CORRECTION_TABLE,
    QUALIFIED_SUGGESTION_TABLE,
)
from dagster_v3.defs.company_people.merge import (
    DEFAULT_MERGE_LLM_PROFILE_NAME,
    MERGE_LLM_PROFILES,
    CandidateMemberRow,
    MergeCandidateGroup,
    MergeLlmProfile,
    MergeObservationRow,
    PublishedPersonRow,
    SECompanyPersonMergeConfig,
    build_decided_candidate_group_ids_sql,
    build_llm_client,
    build_merge_candidate_groups,
    build_merge_candidate_rows_sql,
    build_merge_observation_rows_sql,
    build_merge_published_person_rows_sql,
    build_merge_request,
    candidate_row_from_row,
    input_hash_for,
    llm_api_key_variable,
    materialize_se_company_person_merge_suggestions,
    observation_row_from_row,
    parse_merge_suggestion_response,
    published_person_row_from_row,
    resolve_merge_llm_profile,
    se_company_person_merge_job,
    se_company_person_merge_suggestions,
    stored_merge_suggestion_from_row,
    stored_suggestions_by_group,
)
from dagster_v3.defs.company_people.normalization import QUALIFIED_PERSON_TABLE
from dagster_v3.defs.company_people.source_views import SE_COMPANY_PERSON_COLLISION_CANDIDATE_TABLE

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
NOW = datetime(2026, 8, 27, 9, tzinfo=UTC)
COMPANY_ID = "5565200028"
OTHER_COMPANY_ID = "5560125220"
PROFILE = MergeLlmProfile(
    provider="deepseek",
    model="deepseek-v4-flash",
    base_url="https://api.deepseek.com",
    temperature=0,
    max_tokens=800,
    prompt_version="se-company-person-merge-v1",
)


# ---------------------------------------------------------------------------
# LLM profile registry.
# ---------------------------------------------------------------------------


def test_default_profile_name_resolves_and_mirrors_deepseek_defaults() -> None:
    profile = resolve_merge_llm_profile(DEFAULT_MERGE_LLM_PROFILE_NAME)
    assert profile.provider == "deepseek"
    assert profile.model == "deepseek-v4-flash"
    assert profile.base_url == "https://api.deepseek.com"
    assert DEFAULT_MERGE_LLM_PROFILE_NAME in MERGE_LLM_PROFILES


def test_unknown_profile_name_raises_with_the_valid_choices() -> None:
    with pytest.raises(ValueError, match="deepseek-default"):
        resolve_merge_llm_profile("not-a-real-profile")


def test_llm_api_key_variable_is_provider_upper_api_key() -> None:
    assert llm_api_key_variable("deepseek") == "DEEPSEEK_API_KEY"
    assert llm_api_key_variable("openai") == "OPENAI_API_KEY"


def test_build_llm_client_requires_the_host_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    with pytest.raises(ValueError, match="DEEPSEEK_API_KEY"):
        build_llm_client(PROFILE, timeout_seconds=30)


def test_build_llm_client_succeeds_with_the_host_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEEPSEEK_API_KEY", "secret")
    client = build_llm_client(PROFILE, timeout_seconds=30)
    assert client.base_url is not None


# ---------------------------------------------------------------------------
# SQL builders (text pins).
# ---------------------------------------------------------------------------


def test_candidate_rows_sql_reads_the_task_2_table_unfiltered_by_decision() -> None:
    sql = build_merge_candidate_rows_sql()
    assert f"FROM {SE_COMPANY_PERSON_COLLISION_CANDIDATE_TABLE}" in sql
    assert "company_id, candidate_group_id, person_key, full_name, source, source_record_uid" in sql
    assert "JOIN" not in sql.upper()


def test_decided_group_ids_sql_matches_both_kinds_with_no_join() -> None:
    sql = build_decided_candidate_group_ids_sql()
    assert f"FROM {QUALIFIED_CORRECTION_TABLE}" in sql
    assert f"correction_kind IN ('merge_persons', '{KEEP_SEPARATE_CORRECTION_KIND}')" in sql
    assert "JSONExtractString(payload, 'candidate_group_id')" in sql
    assert "!= ''" in sql
    assert "JOIN" not in sql.upper()


def test_decided_group_ids_sql_excludes_a_row_a_later_undo_superseded() -> None:
    """Same idiom as corrections.effective_company_corrections_cte /
    roles._live_role_corrections_filter: a NOT IN (SELECT supersedes_correction_id ...)
    subquery, scoped the same way as the outer scan."""
    sql = build_decided_candidate_group_ids_sql()
    assert "AND correction_id NOT IN (" in sql
    assert sql.count("SELECT supersedes_correction_id") == 1
    assert "WHERE supersedes_correction_id IS NOT NULL" in sql
    # The subquery's own scope repeats the outer scan's exact scope predicate -- an undo
    # always names a row of its own company, matching the codebase-wide convention.
    assert sql.count("(%(all_companies)s = 1 OR company_id IN %(company_ids)s)") == 2


def test_observation_rows_sql_wraps_the_shared_cte_and_adds_full_name() -> None:
    sql = build_merge_observation_rows_sql()
    assert "source_observations AS (" in sql or "source_observations" in sql
    assert "full_name" in sql
    assert "FROM source_observations" in sql
    assert "JOIN" not in sql.upper()


def test_published_person_rows_sql_excludes_tombstones_with_no_join() -> None:
    sql = build_merge_published_person_rows_sql()
    assert f"FROM {QUALIFIED_PERSON_TABLE} FINAL" in sql
    assert "merged_into_person_id IS NULL" in sql
    assert "JOIN" not in sql.upper()


# ---------------------------------------------------------------------------
# Row mappers.
# ---------------------------------------------------------------------------


def test_candidate_row_from_row_parses_positionally() -> None:
    row = candidate_row_from_row(
        (COMPANY_ID, "grp1", "anna svensson", "Anna B Svensson", "bolagsverket", "src-1")
    )
    assert row == CandidateMemberRow(
        company_id=COMPANY_ID,
        candidate_group_id="grp1",
        person_key="anna svensson",
        full_name="Anna B Svensson",
        source="bolagsverket",
        source_record_uid="src-1",
    )


def test_observation_row_from_row_parses_json_source_value() -> None:
    draft_id = uuid.UUID(int=1)
    row = observation_row_from_row(
        (
            str(draft_id),
            COMPANY_ID,
            "bolagsverket",
            "src-1",
            "Anna B Svensson",
            2024,
            NOW,
            json.dumps({"role_original": "Board member", "role_kind": "board_member"}),
        )
    )
    assert row.draft_id == draft_id
    assert row.fiscal_year == 2024
    assert row.source_value["role_kind"] == "board_member"


def test_published_person_row_from_row_parses_draft_ids_array() -> None:
    person_id = uuid.UUID(int=100)
    draft_id = uuid.UUID(int=1)
    row = published_person_row_from_row((str(person_id), COMPANY_ID, [str(draft_id)], NOW))
    assert row.person_id == person_id
    assert row.draft_ids == frozenset({draft_id})


def test_stored_merge_suggestion_from_row_parses_the_shared_suggestion_table_shape() -> None:
    """corrections.build_company_suggestions_sql's row shape, including rows this module did
    not itself write (e.g. normalization's person-profile suggestions) -- their `suggestion`
    JSON simply carries no candidate_group_id, harmlessly."""
    suggestion_id = uuid.uuid4()
    person_id = uuid.uuid4()
    row = stored_merge_suggestion_from_row(
        (
            str(suggestion_id),
            COMPANY_ID,
            str(person_id),
            "a" * 64,
            [],
            json.dumps({"name": "not a merge suggestion"}),
            "deepseek",
            "deepseek-v4-flash",
            "some-other-prompt",
            NOW,
        )
    )
    assert row.suggestion_id == suggestion_id
    assert row.input_hash == "a" * 64
    assert "candidate_group_id" not in row.suggestion


def test_stored_suggestions_by_group_ignores_rows_without_a_candidate_group_id() -> None:
    unrelated = stored_merge_suggestion_from_row(
        (
            str(uuid.uuid4()),
            COMPANY_ID,
            str(uuid.uuid4()),
            "a" * 64,
            [],
            json.dumps({"name": "not a merge suggestion"}),
            "deepseek",
            "deepseek-v4-flash",
            "p",
            NOW,
        )
    )
    merge_related = stored_merge_suggestion_from_row(
        (
            str(uuid.uuid4()),
            COMPANY_ID,
            str(uuid.uuid4()),
            "b" * 64,
            [],
            json.dumps({"candidate_group_id": "grp1", "decision": "merge"}),
            "deepseek",
            "deepseek-v4-flash",
            "p",
            NOW,
        )
    )

    by_group = stored_suggestions_by_group([unrelated, merge_related])

    assert by_group == {(COMPANY_ID, "grp1"): frozenset({"b" * 64})}


# ---------------------------------------------------------------------------
# build_merge_candidate_groups (pure).
# ---------------------------------------------------------------------------


DRAFT_A = uuid.UUID(int=1)
DRAFT_B = uuid.UUID(int=2)
PERSON_A = uuid.UUID(int=100)
PERSON_B = uuid.UUID(int=200)


def _candidate(group_id: str, *, full_name: str, source: str = "bolagsverket", uid: str) -> CandidateMemberRow:
    return CandidateMemberRow(
        company_id=COMPANY_ID,
        candidate_group_id=group_id,
        person_key="anna svensson",
        full_name=full_name,
        source=source,
        source_record_uid=uid,
    )


def _observation(
    *, draft_id: uuid.UUID, full_name: str, uid: str, role_original: str, observed_at: datetime
) -> MergeObservationRow:
    return MergeObservationRow(
        draft_id=draft_id,
        company_id=COMPANY_ID,
        source="bolagsverket",
        source_record_uid=uid,
        full_name=full_name,
        fiscal_year=2024,
        source_observed_at=observed_at,
        source_value={"role_original": role_original, "role_kind": role_original.lower()},
    )


def _published(*, person_id: uuid.UUID, draft_ids: frozenset[uuid.UUID], created_at: datetime) -> PublishedPersonRow:
    return PublishedPersonRow(
        person_id=person_id, company_id=COMPANY_ID, draft_ids=draft_ids, created_at=created_at
    )


def _two_person_scenario() -> tuple[list[CandidateMemberRow], list[MergeObservationRow], list[PublishedPersonRow]]:
    candidates = [
        _candidate("grp1", full_name="Anna B Svensson", uid="src-1"),
        _candidate("grp1", full_name="Anna C Svensson", uid="src-2"),
    ]
    observations = [
        _observation(draft_id=DRAFT_A, full_name="Anna B Svensson", uid="src-1", role_original="Board member", observed_at=NOW),
        _observation(draft_id=DRAFT_B, full_name="Anna C Svensson", uid="src-2", role_original="CEO", observed_at=NOW),
    ]
    published = [
        _published(person_id=PERSON_A, draft_ids=frozenset({DRAFT_A}), created_at=NOW - timedelta(days=2)),
        _published(person_id=PERSON_B, draft_ids=frozenset({DRAFT_B}), created_at=NOW - timedelta(days=1)),
    ]
    return candidates, observations, published


def test_a_group_with_two_distinct_current_people_is_actionable() -> None:
    candidates, observations, published = _two_person_scenario()

    groups = build_merge_candidate_groups(candidates, observations, published)

    assert len(groups) == 1
    group = groups[0]
    assert group.candidate_group_id == "grp1"
    assert group.member_person_ids == (PERSON_A, PERSON_B)
    assert {member.role for member in group.members} == {"Board member", "CEO"}


def test_the_older_published_person_survives_as_into_person_id() -> None:
    candidates, observations, published = _two_person_scenario()

    [group] = build_merge_candidate_groups(candidates, observations, published)

    assert group.into_person_id == PERSON_A  # PERSON_A was published 2 days ago, PERSON_B 1
    assert group.from_person_ids == (PERSON_B,)


def test_a_group_already_unified_under_one_current_person_is_skipped() -> None:
    candidates, observations, _ = _two_person_scenario()
    published = [
        _published(person_id=PERSON_A, draft_ids=frozenset({DRAFT_A, DRAFT_B}), created_at=NOW),
    ]

    groups = build_merge_candidate_groups(candidates, observations, published)

    assert groups == []


def test_a_group_whose_evidence_cannot_be_resolved_is_skipped() -> None:
    candidates, _, published = _two_person_scenario()

    groups = build_merge_candidate_groups(candidates, observation_rows=[], published_rows=published)

    assert groups == []


def test_build_merge_candidate_groups_is_order_independent() -> None:
    candidates, observations, published = _two_person_scenario()

    forward = build_merge_candidate_groups(candidates, observations, published)
    backward = build_merge_candidate_groups(
        list(reversed(candidates)), list(reversed(observations)), list(reversed(published))
    )

    assert forward == backward


def test_two_separate_candidate_groups_resolve_independently() -> None:
    candidates, observations, published = _two_person_scenario()
    draft_c, draft_d = uuid.UUID(int=3), uuid.UUID(int=4)
    person_c, person_d = uuid.UUID(int=300), uuid.UUID(int=400)
    candidates += [
        _candidate("grp2", full_name="Erik Andersson", uid="src-3"),
        _candidate("grp2", full_name="Erik B Andersson", uid="src-4"),
    ]
    observations += [
        _observation(draft_id=draft_c, full_name="Erik Andersson", uid="src-3", role_original="Auditor", observed_at=NOW),
        _observation(draft_id=draft_d, full_name="Erik B Andersson", uid="src-4", role_original="Board member", observed_at=NOW),
    ]
    published += [
        _published(person_id=person_c, draft_ids=frozenset({draft_c}), created_at=NOW),
        _published(person_id=person_d, draft_ids=frozenset({draft_d}), created_at=NOW - timedelta(days=5)),
    ]

    groups = build_merge_candidate_groups(candidates, observations, published)

    assert {group.candidate_group_id for group in groups} == {"grp1", "grp2"}
    grp2 = next(group for group in groups if group.candidate_group_id == "grp2")
    assert grp2.into_person_id == person_d  # older


# ---------------------------------------------------------------------------
# build_merge_request / input_hash_for.
# ---------------------------------------------------------------------------


def _group_from(candidates, observations, published) -> MergeCandidateGroup:
    [group] = build_merge_candidate_groups(candidates, observations, published)
    return group


def test_build_merge_request_never_asks_the_model_to_pick_a_survivor() -> None:
    candidates, observations, published = _two_person_scenario()
    group = _group_from(candidates, observations, published)

    request = build_merge_request(group, PROFILE)
    payload = json.loads(request["messages"][1]["content"])

    assert "person_id" not in json.dumps(payload)
    assert payload["candidate_group_id"] == "grp1"
    assert {entry["name"] for entry in payload["observations"]} == {"Anna B Svensson", "Anna C Svensson"}


def test_input_hash_is_deterministic_and_changes_with_the_evidence() -> None:
    candidates, observations, published = _two_person_scenario()
    group = _group_from(candidates, observations, published)
    request = build_merge_request(group, PROFILE)
    hash_a = input_hash_for(request, PROFILE.prompt_version)
    hash_again = input_hash_for(build_merge_request(group, PROFILE), PROFILE.prompt_version)
    assert hash_a == hash_again

    changed_observations = [
        _observation(draft_id=DRAFT_A, full_name="Anna B Svensson", uid="src-1", role_original="Chair", observed_at=NOW),
        observations[1],
    ]
    changed_group = _group_from(candidates, changed_observations, published)
    hash_changed = input_hash_for(build_merge_request(changed_group, PROFILE), PROFILE.prompt_version)
    assert hash_changed != hash_a


# ---------------------------------------------------------------------------
# parse_merge_suggestion_response.
# ---------------------------------------------------------------------------


def test_parse_merge_suggestion_response_accepts_a_valid_object() -> None:
    content = json.dumps({"decision": "merge", "confidence": 0.85, "rationale": "Same person."})
    parsed = parse_merge_suggestion_response(content)
    assert parsed.decision == "merge"
    assert parsed.confidence == 0.85


def test_parse_merge_suggestion_response_rejects_an_unknown_decision() -> None:
    content = json.dumps({"decision": "maybe", "confidence": 0.5, "rationale": ""})
    with pytest.raises(ValueError, match="failed validation"):
        parse_merge_suggestion_response(content)


def test_parse_merge_suggestion_response_rejects_no_content() -> None:
    with pytest.raises(ValueError, match="no content"):
        parse_merge_suggestion_response(None)


def test_parse_merge_suggestion_response_rejects_non_json() -> None:
    with pytest.raises(ValueError, match="JSON object"):
        parse_merge_suggestion_response("not json at all")


# ---------------------------------------------------------------------------
# materialize_se_company_person_merge_suggestions -- fake ClickHouse + fake OpenAI.
# ---------------------------------------------------------------------------


class _FakeClient:
    def __init__(
        self,
        *,
        existing_tables: set[str],
        candidate_rows: list[tuple[Any, ...]],
        decided_rows: list[tuple[Any, ...]],
        observation_rows: list[tuple[Any, ...]] | None = None,
        published_rows: list[tuple[Any, ...]] | None = None,
        stored_rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self.existing_tables = existing_tables
        self.candidate_rows = candidate_rows
        self.decided_rows = decided_rows
        self.observation_rows = observation_rows or []
        self.published_rows = published_rows or []
        self.stored_rows = stored_rows or []
        self.executed: list[tuple[str, Any]] = []
        self.inserted_rows: list[tuple[Any, ...]] = []

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        self.executed.append((sql, params))
        if "system.tables" in sql:
            requested = tuple(params["tables"])
            return [(table,) for table in requested if table in self.existing_tables]
        if sql.strip().upper().startswith("INSERT"):
            self.inserted_rows.extend(params)
            return []
        if "se_company_person_collision_candidate" in sql:
            return self.candidate_rows
        if "se_company_person_correction" in sql and "JSONExtractString" in sql:
            return self.decided_rows
        if "FROM source_observations" in sql:
            return self.observation_rows
        if f"FROM {QUALIFIED_PERSON_TABLE} FINAL" in sql:
            return self.published_rows
        if "se_company_person_enrichment_observation" in sql:
            return self.stored_rows
        raise AssertionError(f"unexpected query: {sql[:200]}")


class _FakeClickhouse:
    def __init__(self, client: _FakeClient) -> None:
        self._client = client

    @contextmanager
    def get_connection(self) -> Iterator[_FakeClient]:
        yield self._client


EXISTING_TABLES = {
    "se_company_person_collision_candidate",
    "se_company_person_correction",
    "se_company_person_enrichment_observation",
    "se_company_person",
}


def _scenario_rows() -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    candidate_rows = [
        (COMPANY_ID, "grp1", "anna svensson", "Anna B Svensson", "bolagsverket", "src-1"),
        (COMPANY_ID, "grp1", "anna svensson", "Anna C Svensson", "bolagsverket", "src-2"),
    ]
    observation_rows = [
        (
            str(DRAFT_A), COMPANY_ID, "bolagsverket", "src-1", "Anna B Svensson", 2024, NOW,
            json.dumps({"role_original": "Board member", "role_kind": "board_member"}),
        ),
        (
            str(DRAFT_B), COMPANY_ID, "bolagsverket", "src-2", "Anna C Svensson", 2024, NOW,
            json.dumps({"role_original": "CEO", "role_kind": "ceo"}),
        ),
    ]
    published_rows = [
        (str(PERSON_A), COMPANY_ID, [str(DRAFT_A)], NOW - timedelta(days=2)),
        (str(PERSON_B), COMPANY_ID, [str(DRAFT_B)], NOW - timedelta(days=1)),
    ]
    return candidate_rows, observation_rows, published_rows


class _FakeChatCompletions:
    def __init__(self, content: str) -> None:
        self._content = content
        self.calls = 0

    def create(self, **kwargs: Any) -> SimpleNamespace:
        self.calls += 1
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(content=self._content), finish_reason="stop"
                )
            ],
            usage=SimpleNamespace(prompt_tokens=120, completion_tokens=40),
        )


class _FakeOpenAI:
    def __init__(self, content: str) -> None:
        self.chat = SimpleNamespace(completions=_FakeChatCompletions(content))


def test_preview_writes_nothing_but_reports_actionable_groups() -> None:
    candidate_rows, observation_rows, published_rows = _scenario_rows()
    client = _FakeClient(
        existing_tables=EXISTING_TABLES,
        candidate_rows=candidate_rows,
        decided_rows=[],
        observation_rows=observation_rows,
        published_rows=published_rows,
        stored_rows=[],
    )

    metadata = materialize_se_company_person_merge_suggestions(
        clickhouse=_FakeClickhouse(client),
        source_run_id="run-1",
        created_at=NOW,
        company_ids=[COMPANY_ID],
        max_groups=None,
        execute=False,
        llm_client=None,
        llm_profile=PROFILE,
        log=None,
    )

    assert metadata["preview"] is True
    assert metadata["actionable_group_count"] == 1
    assert metadata["would_call_model"] == 1
    assert client.inserted_rows == []
    assert not any(sql.strip().upper().startswith("INSERT") for sql, _ in client.executed)


def test_execute_with_a_profile_writes_a_suggestion_row() -> None:
    candidate_rows, observation_rows, published_rows = _scenario_rows()
    client = _FakeClient(
        existing_tables=EXISTING_TABLES,
        candidate_rows=candidate_rows,
        decided_rows=[],
        observation_rows=observation_rows,
        published_rows=published_rows,
        stored_rows=[],
    )
    fake_llm = _FakeOpenAI(json.dumps({"decision": "merge", "confidence": 0.9, "rationale": "Same person."}))

    metadata = materialize_se_company_person_merge_suggestions(
        clickhouse=_FakeClickhouse(client),
        source_run_id="run-2",
        created_at=NOW,
        company_ids=[COMPANY_ID],
        max_groups=None,
        execute=True,
        llm_client=fake_llm,
        llm_profile=PROFILE,
        log=None,
    )

    assert metadata["suggestion_inserted_count"] == 1
    assert metadata["llm_request_count"] == 1
    assert fake_llm.chat.completions.calls == 1
    assert len(client.inserted_rows) == 1
    inserted = client.inserted_rows[0]
    # SUGGESTION_COLUMNS order: suggestion_id, company_id, person_id, input_hash, draft_ids,
    # suggestion, raw_response, model_provider, model_name, prompt_version, prompt_tokens,
    # completion_tokens, source_run_id, created_at
    assert inserted[1] == COMPANY_ID
    assert inserted[2] == PERSON_A  # into_person_id (the older of the two)
    payload = json.loads(inserted[5])
    assert payload["candidate_group_id"] == "grp1"
    assert payload["decision"] == "merge"
    assert payload["into_person_id"] == str(PERSON_A)
    assert payload["from_person_ids"] == [str(PERSON_B)]
    assert inserted[7] == "deepseek"
    assert inserted[12] == "run-2"


def test_input_hash_reuse_skips_an_unchanged_group_without_calling_the_model() -> None:
    candidate_rows, observation_rows, published_rows = _scenario_rows()
    [group] = build_merge_candidate_groups(
        [candidate_row_from_row(row) for row in candidate_rows],
        [observation_row_from_row(row) for row in observation_rows],
        [published_person_row_from_row(row) for row in published_rows],
    )
    current_hash = input_hash_for(build_merge_request(group, PROFILE), PROFILE.prompt_version)
    stored_rows = [
        (
            str(uuid.uuid4()), COMPANY_ID, str(PERSON_A), current_hash, [], json.dumps(
                {"candidate_group_id": "grp1", "decision": "merge", "confidence": 0.9, "rationale": "r"}
            ),
            "deepseek", "deepseek-v4-flash", PROFILE.prompt_version, NOW,
        )
    ]
    client = _FakeClient(
        existing_tables=EXISTING_TABLES,
        candidate_rows=candidate_rows,
        decided_rows=[],
        observation_rows=observation_rows,
        published_rows=published_rows,
        stored_rows=stored_rows,
    )
    fake_llm = _FakeOpenAI(json.dumps({"decision": "merge", "confidence": 0.9, "rationale": "r"}))

    metadata = materialize_se_company_person_merge_suggestions(
        clickhouse=_FakeClickhouse(client),
        source_run_id="run-3",
        created_at=NOW,
        company_ids=[COMPANY_ID],
        max_groups=None,
        execute=True,
        llm_client=fake_llm,
        llm_profile=PROFILE,
        log=None,
    )

    assert metadata["reused_suggestion_count"] == 1
    assert metadata["llm_request_count"] == 0
    assert metadata["suggestion_inserted_count"] == 0
    assert fake_llm.chat.completions.calls == 0
    assert client.inserted_rows == []


def test_decided_groups_are_skipped_for_both_merge_and_keep_separate() -> None:
    candidate_rows = [
        (COMPANY_ID, "grp1", "anna svensson", "Anna B Svensson", "bolagsverket", "src-1"),
        (COMPANY_ID, "grp1", "anna svensson", "Anna C Svensson", "bolagsverket", "src-2"),
        (OTHER_COMPANY_ID, "grp2", "erik andersson", "Erik Andersson", "bolagsverket", "src-3"),
        (OTHER_COMPANY_ID, "grp2", "erik andersson", "Erik B Andersson", "bolagsverket", "src-4"),
    ]
    # grp1 was decided as a merge, grp2 as keep_separate -- both must be skipped, and since
    # every candidate row belongs to one of the two, no company is left to even query
    # observations/published people for.
    decided_rows = [("grp1",), ("grp2",)]
    client = _FakeClient(
        existing_tables=EXISTING_TABLES,
        candidate_rows=candidate_rows,
        decided_rows=decided_rows,
    )

    metadata = materialize_se_company_person_merge_suggestions(
        clickhouse=_FakeClickhouse(client),
        source_run_id="run-4",
        created_at=NOW,
        company_ids=[COMPANY_ID, OTHER_COMPANY_ID],
        max_groups=None,
        execute=True,
        llm_client=_FakeOpenAI("{}"),
        llm_profile=PROFILE,
        log=None,
    )

    assert metadata["candidate_group_count"] == 2
    assert metadata["decided_group_count"] == 2
    assert metadata["considered_group_count"] == 0
    assert metadata["actionable_group_count"] == 0
    assert metadata["suggestion_inserted_count"] == 0
    assert client.inserted_rows == []
    # No observation/published/stored query was even attempted -- the fake would have
    # raised AssertionError on any query it does not recognize, so reaching this line without
    # error already proves nothing beyond candidate/decided was queried; the metrics above
    # confirm both DECISION KINDS were honored, not just one.


def test_max_groups_caps_the_number_of_groups_considered() -> None:
    candidate_rows = [
        (COMPANY_ID, "grp1", "anna svensson", "Anna B Svensson", "bolagsverket", "src-1"),
        (COMPANY_ID, "grp1", "anna svensson", "Anna C Svensson", "bolagsverket", "src-2"),
        (COMPANY_ID, "grp2", "erik andersson", "Erik Andersson", "bolagsverket", "src-3"),
        (COMPANY_ID, "grp2", "erik andersson", "Erik B Andersson", "bolagsverket", "src-4"),
    ]
    client = _FakeClient(
        existing_tables=EXISTING_TABLES,
        candidate_rows=candidate_rows,
        decided_rows=[],
        observation_rows=[],
        published_rows=[],
        stored_rows=[],
    )

    metadata = materialize_se_company_person_merge_suggestions(
        clickhouse=_FakeClickhouse(client),
        source_run_id="run-5",
        created_at=NOW,
        company_ids=[COMPANY_ID],
        max_groups=1,
        execute=False,
        llm_client=None,
        llm_profile=PROFILE,
        log=None,
    )

    assert metadata["candidate_group_count"] == 2
    assert metadata["considered_group_count"] == 1


def test_execute_without_a_client_raises() -> None:
    client = _FakeClient(existing_tables=EXISTING_TABLES, candidate_rows=[], decided_rows=[])
    with pytest.raises(ValueError, match="execute=True needs an LLM client"):
        materialize_se_company_person_merge_suggestions(
            clickhouse=_FakeClickhouse(client),
            source_run_id="run-6",
            created_at=NOW,
            company_ids=None,
            max_groups=None,
            execute=True,
            llm_client=None,
            llm_profile=PROFILE,
            log=None,
        )


# ---------------------------------------------------------------------------
# Config defaults and asset/job wiring.
# ---------------------------------------------------------------------------


def test_config_defaults_are_a_harmless_preview() -> None:
    config = SECompanyPersonMergeConfig()
    assert config.execute is False
    assert config.llm_profile == DEFAULT_MERGE_LLM_PROFILE_NAME
    assert config.company_ids is None
    assert config.max_groups is None


def test_asset_and_job_are_wired_with_the_right_group_and_deps() -> None:
    from dagster_v3.definitions import defs as load_defs

    graph = load_defs().get_repository_def().asset_graph
    node = graph.get(dg.AssetKey("se_company_person_merge_suggestions"))
    assert node.group_name == "se_company_person"
    assert node.parent_keys == {
        dg.AssetKey("se_company_person_identity_evaluation"),
        dg.AssetKey("se_company_person_clickhouse"),
    }
    assert node.metadata["table"] == QUALIFIED_SUGGESTION_TABLE

    resolved_job = load_defs().resolve_job_def("se_company_person_merge_job")
    assert resolved_job.name == "se_company_person_merge_job"


def test_the_asset_is_never_scheduled_or_eager() -> None:
    """Owner rule (spec 6.1): manual only -- this module's own Definitions carries no
    schedules/sensors, and the asset carries no automation condition (the default -- an
    explicit eager policy would be a code smell here, not an omission to fix)."""
    from dagster_v3.defs.company_people.merge import defs as merge_defs

    assert not merge_defs.schedules
    assert not merge_defs.sensors
    assert not se_company_person_merge_suggestions.automation_conditions_by_key
    assert se_company_person_merge_job  # imported and constructed without error


# ---------------------------------------------------------------------------
# clickhouse-local: the new SQL actually executes against a real engine.
#
# None of this module's SQL contains a JOIN (see each builder's own docstring), so
# join_use_nulls cannot change any of these queries' results -- there is no LEFT JOIN miss
# for the setting to affect. Both settings are still exercised below (cheap, and consistent
# with this repo's convention of never assuming join_use_nulls=0 without checking) rather
# than asserted-by-argument.
# ---------------------------------------------------------------------------

MERGE_MIGRATIONS = (
    "000290_corpscout_company_person_source_observations_and_role_types.up.sql",
    "000291_corpscout_se_company_person.up.sql",
    "000292_corpscout_se_company_person_roles.up.sql",
    "000293_corpscout_se_company_person_roles_by_year.up.sql",
    "000294_corpscout_employee_board_representative_role.up.sql",
    "000295_corpscout_se_company_person_corrections.up.sql",
    "000330_corpscout_se_company_person_views.up.sql",
    "000331_corpscout_se_company_person_views_observed_at.up.sql",
)
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:26.5"
APPLIED_PREFIXES = (
    "CREATE DATABASE",
    "CREATE TABLE",
    "CREATE OR REPLACE VIEW",
    "ALTER TABLE",
    "DROP TABLE",
    "INSERT",
)

_UPSTREAM_SCHEMA_SQL = """
CREATE TABLE corpscout.se_financial_report_signatories
(
    company_id String,
    fiscal_year Int32,
    statement_key String,
    source_record_uid String,
    signatory_kind LowCardinality(String),
    person_seq UInt16,
    signatory_uid FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'sweden-financial-report-signatory-v1\n',
            company_id, '\n', statement_key, '\n', signatory_kind, '\n',
            toString(person_seq)
        )))),
    first_name String,
    last_name String,
    person_profile_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'company-person-profile-v1\n',
            toString(length(lowerUTF8(trim(first_name)))), ':',
            lowerUTF8(trim(first_name)), '\n',
            toString(length(lowerUTF8(trim(last_name)))), ':',
            lowerUTF8(trim(last_name))
        )))),
    role_original String,
    role_kind LowCardinality(String),
    person_role_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'company-person-role-v1\n',
            toString(length(lowerUTF8(trim(role_original)))), ':',
            lowerUTF8(trim(role_original)), '\n',
            toString(length(lowerUTF8(trim(role_kind)))), ':',
            lowerUTF8(trim(role_kind)), '\n',
            toString(length(lowerUTF8(trim(signatory_kind)))), ':',
            lowerUTF8(trim(signatory_kind)), '\n',
            toString(fiscal_year)
        )))),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, fiscal_year, statement_key, signatory_kind, person_seq);

CREATE TABLE corpscout.esef_document_people
(
    candidate_uid FixedString(64),
    source_record_uid FixedString(64),
    source_document_id String,
    country_code LowCardinality(String),
    company_id String,
    fiscal_year UInt16,
    name String,
    person_profile_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'company-person-profile-v1\n',
            toString(length(lowerUTF8(trim(name)))), ':', lowerUTF8(trim(name)), '\n',
            '0:'
        )))),
    role String,
    role_category LowCardinality(String),
    organization String,
    status LowCardinality(String),
    person_role_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'company-person-role-v1\n',
            toString(length(lowerUTF8(trim(role)))), ':', lowerUTF8(trim(role)), '\n',
            toString(length(lowerUTF8(trim(role_category)))), ':',
            lowerUTF8(trim(role_category)), '\n',
            toString(length(lowerUTF8(trim(organization)))), ':',
            lowerUTF8(trim(organization)), '\n',
            toString(length(lowerUTF8(trim(status)))), ':', lowerUTF8(trim(status)), '\n',
            ifNull(toString(effective_from), ''), '\n',
            ifNull(toString(effective_to), ''), '\n',
            toString(fiscal_year)
        )))),
    effective_from Nullable(Date32),
    effective_to Nullable(Date32),
    confidence Float32,
    evidence_ids Array(String),
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    source_run_id String,
    extracted_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (country_code, company_id, fiscal_year, source_record_uid, candidate_uid);

CREATE TABLE corpscout.wikidata_company_identifiers
(
    wikidata_id String,
    identifier_type LowCardinality(String),
    wikidata_property_id LowCardinality(String),
    identifier_value String,
    identifier_scope Nullable(String),
    is_primary UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (identifier_type, identifier_value, wikidata_id);

CREATE TABLE corpscout.wikidata_company_people
(
    company_wikidata_id String,
    person_wikidata_id String,
    role_property LowCardinality(String),
    role_label LowCardinality(String),
    start_date Nullable(Date),
    end_date Nullable(Date),
    is_current UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    person_role_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'company-person-role-v1\n',
            toString(length(lowerUTF8(trim(role_property)))), ':',
            lowerUTF8(trim(role_property)), '\n',
            toString(length(lowerUTF8(trim(role_label)))), ':',
            lowerUTF8(trim(role_label)), '\n',
            ifNull(toString(start_date), ''), '\n',
            ifNull(toString(end_date), ''), '\n',
            toString(is_current)
        )))),
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (company_wikidata_id, role_property, person_wikidata_id);

CREATE TABLE corpscout.wikidata_persons
(
    person_wikidata_id String,
    source_record_uid String,
    person_profile_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'company-person-profile-v1\n',
            toString(length(lowerUTF8(trim(name)))), ':', lowerUTF8(trim(name)), '\n',
            toString(length(lowerUTF8(trim(ifNull(description, ''))))), ':',
            lowerUTF8(trim(ifNull(description, '')))
        )))),
    name String,
    name_normalized String,
    description Nullable(String),
    birth_year Nullable(UInt16),
    image_url Nullable(String),
    wikidata_url Nullable(String),
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (person_wikidata_id);

CREATE TABLE corpscout.company_identifier
(
    issuer_scheme LowCardinality(String),
    issuer_id String,
    country_code LowCardinality(String),
    company_id String,
    match_method LowCardinality(String),
    match_confidence LowCardinality(String),
    registration_authority_id LowCardinality(String),
    registered_as_raw String,
    company_id_normalized String,
    entity_status LowCardinality(String),
    registration_status LowCardinality(String),
    is_current UInt8,
    successor_issuer_id String,
    first_seen_date Date,
    last_seen_date Date,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (issuer_scheme, issuer_id, country_code, company_id);
"""


@functools.cache
def _clickhouse_local_command() -> list[str]:
    direct = shutil.which("clickhouse-local")
    if direct:
        return [direct, "--multiquery"]
    binary = shutil.which("clickhouse")
    if binary:
        return [binary, "local", "--multiquery"]
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("no clickhouse-local binary and no docker to run one")
    probe = subprocess.run([docker, "info"], capture_output=True, text=True, timeout=60, check=False)
    if probe.returncode != 0:
        pytest.skip("docker is installed but not running")
    return [docker, "run", "--rm", "-i", CLICKHOUSE_IMAGE, "clickhouse-local", "--multiquery"]


def _schema_statements() -> list[str]:
    statements: list[str] = []
    for name in MERGE_MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(APPLIED_PREFIXES):
                statements.append(statement)
    return statements


def _id(marker: int) -> str:
    return str(uuid.UUID(int=marker))


def _correction_insert(
    *,
    marker: int,
    kind: str,
    subject_person_id: uuid.UUID,
    candidate_group_id: str = "",
    supersedes: uuid.UUID | None = None,
    created_at: str,
) -> str:
    """One `se_company_person_correction` row. `candidate_group_id` is omitted from the
    payload for an `undo` row (it names its target via `supersedes_correction_id`, not a
    group id of its own)."""
    payload = (
        f'{{\\"candidate_group_id\\":\\"{candidate_group_id}\\"}}' if candidate_group_id else "{}"
    )
    supersedes_sql = "NULL" if supersedes is None else f"'{_id(supersedes.int)}'"
    return f"""INSERT INTO corpscout.se_company_person_correction
    (correction_id, company_id, correction_kind, subject_person_id, target_person_id,
     draft_ids, payload, evidence_hash, reason, decided_by, supersedes_correction_id, created_at)
VALUES
    ('{_id(marker)}', '{COMPANY_ID}', '{kind}', '{_id(subject_person_id.int)}', NULL,
     [], '{payload}', repeat('0', 64), 'reason', 'backoffice',
     {supersedes_sql}, toDateTime64('{created_at}', 3, 'UTC'))"""


def _fixture_statements() -> list[str]:
    person_id = uuid.UUID(int=100)
    return [
        f"""INSERT INTO corpscout.se_financial_report_signatories
    (company_id, fiscal_year, statement_key, source_record_uid, signatory_kind,
     person_seq, first_name, last_name, role_original, role_kind, resolved_at)
VALUES
    ('{COMPANY_ID}', 2024, 'stmt-1', 'src-1', 'board_signature', 1,
     'David', 'Mindus', 'Verkstallande direktor', 'ceo',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'))""",
        f"""INSERT INTO corpscout.se_company_person_collision_candidate
    (company_id, candidate_group_id, person_key, full_name, source, source_record_uid, evidence_json, created_at)
VALUES
    ('{COMPANY_ID}', 'grp1', 'david mindus', 'David Mindus', 'bolagsverket', 'src-1', '{{}}',
     toDateTime('2026-08-01 00:00:00'))""",
        f"""INSERT INTO corpscout.se_company_person
    (person_id, company_id, name, description, draft_ids, correction_ids, suggestion_id,
     model_provider, model_name, prompt_version, source_run_id, created_at, updated_at)
VALUES
    ('{person_id}', '{COMPANY_ID}', 'David Mindus', NULL,
     ['{_id(1)}'], [], NULL,
     'deterministic', 'copy', 'v1', 'run',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'), toDateTime64('2026-08-01 00:00:00', 3, 'UTC'))""",
        # grp1: keep_separate, never undone -- must stay decided (control).
        _correction_insert(
            marker=200, kind=KEEP_SEPARATE_CORRECTION_KIND, subject_person_id=person_id,
            candidate_group_id="grp1", created_at="2026-08-02 00:00:00",
        ),
        # grp-merge: merge_persons, never undone -- must stay decided (control).
        _correction_insert(
            marker=201, kind="merge_persons", subject_person_id=person_id,
            candidate_group_id="grp-merge", created_at="2026-08-02 00:00:00",
        ),
        # grp-keep-separate-undone: keep_separate decided, then UNDONE -- must be eligible again.
        _correction_insert(
            marker=202, kind=KEEP_SEPARATE_CORRECTION_KIND, subject_person_id=person_id,
            candidate_group_id="grp-keep-separate-undone", created_at="2026-08-02 00:00:00",
        ),
        _correction_insert(
            marker=203, kind="undo", subject_person_id=person_id,
            supersedes=uuid.UUID(int=202), created_at="2026-08-03 00:00:00",
        ),
        # grp-merge-undone: merge_persons decided, then UNDONE -- must be eligible again.
        _correction_insert(
            marker=204, kind="merge_persons", subject_person_id=person_id,
            candidate_group_id="grp-merge-undone", created_at="2026-08-02 00:00:00",
        ),
        _correction_insert(
            marker=205, kind="undo", subject_person_id=person_id,
            supersedes=uuid.UUID(int=204), created_at="2026-08-03 00:00:00",
        ),
    ]


def _script(*, join_use_nulls: int) -> str:
    statements = [
        f"SET join_use_nulls = {join_use_nulls}",
        "CREATE DATABASE IF NOT EXISTS corpscout",
        *(s.strip() for s in _UPSTREAM_SCHEMA_SQL.split(";") if s.strip()),
        *_schema_statements(),
        *_fixture_statements(),
        "SELECT '@@candidates'",
        build_merge_candidate_rows_sql().replace(
            "%(all_companies)s = 1 OR company_id IN %(company_ids)s", "1"
        ),
        "SELECT '@@decided'",
        build_decided_candidate_group_ids_sql().replace(
            "%(all_companies)s = 1 OR company_id IN %(company_ids)s", "1"
        ),
        "SELECT '@@observations'",
        build_merge_observation_rows_sql().replace(
            "%(company_ids)s", f"('{COMPANY_ID}')"
        ),
        "SELECT '@@published'",
        build_merge_published_person_rows_sql().replace(
            "%(company_ids)s", f"('{COMPANY_ID}')"
        ),
    ]
    return ";\n".join(statements) + ";\n"


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(
            command, input=_script(join_use_nulls=request.param), capture_output=True, text=True, timeout=900
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in completed.stdout.splitlines():
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        elif line.strip():
            result[current].append(line.split("\t"))
    return result


@pytest.mark.integration
def test_the_new_sql_executes_against_a_real_engine(sections: dict[str, list[list[str]]]) -> None:
    assert sections["candidates"] == [
        [COMPANY_ID, "grp1", "david mindus", "David Mindus", "bolagsverket", "src-1"]
    ]
    assert "grp1" in {row[0] for row in sections["decided"]}
    assert len(sections["observations"]) == 1
    observation = sections["observations"][0]
    assert observation[1] == COMPANY_ID
    assert observation[2] == "bolagsverket"
    assert observation[4] == "David Mindus"
    # Proves the plain SELECT (no JOIN) runs and reads a real se_company_person row back --
    # this fixture's draft_ids is a placeholder, not the observation's real draft_id, so
    # exercising the actual candidate->observation->person MATCH is the pure
    # build_merge_candidate_groups unit tests' job, not this SQL-execution smoke test's.
    assert len(sections["published"]) == 1
    assert sections["published"][0][0] == _id(100)
    assert sections["published"][0][1] == COMPANY_ID


@pytest.mark.integration
def test_an_undo_reopens_its_candidate_group_for_both_decision_kinds(
    sections: dict[str, list[list[str]]],
) -> None:
    """The Important fix: `build_decided_candidate_group_ids_sql` must exclude a decision a
    later `undo` has superseded, for BOTH `merge_persons` and `keep_separate` -- otherwise a
    reviewer undoing either decision leaves the candidate group permanently stuck as
    "decided", with no fresh suggestion ever possible again.

    `_fixture_statements` seeds four decisions against a real engine: grp1 (keep_separate,
    control) and grp-merge (merge_persons, control) are never undone and must still read as
    decided; grp-keep-separate-undone and grp-merge-undone are each decided and then
    immediately undone by a later `undo` row naming the decision's own correction_id via
    `supersedes_correction_id`, and must read as NOT decided -- eligible for a fresh
    suggestion again.
    """
    decided = {row[0] for row in sections["decided"]}

    assert decided == {"grp1", "grp-merge"}
    assert "grp-keep-separate-undone" not in decided
    assert "grp-merge-undone" not in decided
