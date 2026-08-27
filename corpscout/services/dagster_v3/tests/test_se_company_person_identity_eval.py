"""K1/K2/K3 identity-rule evaluation (spec 3.2) -- pure functions, the merge/collision
classifier, and the manual analysis asset.

Part one is plain unit tests for `identity_key_k1`, `identity_key_k2`, `k3_merge_groups`, and
`evaluate_se_company_person_identity` -- the plan's required cases (superset merge, ambiguous
three-way collision, diacritics, QID-link merge, blank-name exclusion) plus the K1 parity pin
against `normalization.py`'s current `_name_match_key`.

Part two is the executed clickhouse-local test of "the asset body": one script builds the
same upstream fixtures + the three real SE person source views (migration 000330) as
`test_se_company_person_views.py`, SELECTs from them exactly as the asset does, and this
module's `evaluate_se_company_person_identity` runs over the parsed rows -- proving the real
view SQL feeds the real Python evaluation correctly. A second script executes the REAL
candidate-table DDL (migration 000330) against the candidate rows that evaluation just
produced, closing Task 1's deferred minor (the table had never been exercised for real).
"""

from __future__ import annotations

import functools
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.company_people.identity_eval import (
    SE_COMPANY_PERSON_COLLISION_CANDIDATE_COLUMNS,
    CollisionCandidateGroup,
    CollisionCandidateMember,
    IdentityEvaluationResult,
    MergeDecision,
    PersonObservationRow,
    _candidate_table_rows,
    evaluate_se_company_person_identity,
    identity_key_k1,
    identity_key_k2,
    k3_merge_groups,
)
from dagster_v3.defs.company_people.normalization import _name_match_key

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000330_corpscout_se_company_person_views"
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:26.5"

BOLAGSVERKET_VIEW = "corpscout.se_company_person_bolagsverket"
ESEF_VIEW = "corpscout.se_company_person_esef"
WIKIDATA_VIEW = "corpscout.se_company_person_wikidata"
CANDIDATE_TABLE = "corpscout.se_company_person_collision_candidate"


# ---------------------------------------------------------------------------
# Shared migration-text helpers (mirrors test_se_company_person_views.py).
# ---------------------------------------------------------------------------


def _sql(suffix: str) -> str:
    return (MIGRATIONS_DIR / f"{MIGRATION}.{suffix}.sql").read_text(encoding="utf-8")


def _statements(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def _body(statement: str) -> str:
    lines = statement.splitlines()
    while lines and (not lines[0].strip() or lines[0].lstrip().startswith("--")):
        lines.pop(0)
    body = "\n".join(lines).strip()
    assert body, f"no statement left after stripping comments: {statement[:80]!r}"
    return body


def _create_view_statement(sql: str, view: str) -> str:
    [statement] = [
        _body(s) for s in _statements(sql) if f"CREATE OR REPLACE VIEW {view}" in s
    ]
    return statement


def _create_candidate_table_statement(sql: str) -> str:
    [statement] = [
        _body(s)
        for s in _statements(sql)
        if f"CREATE TABLE IF NOT EXISTS {CANDIDATE_TABLE}" in s
    ]
    return statement


def _candidate_table_ddl_columns(statement: str) -> tuple[str, ...]:
    """The column names in the DDL, in declared order -- stops at the CONSTRAINT line."""
    lines = statement.splitlines()
    start = next(i for i, line in enumerate(lines) if line.strip() == "(") + 1
    columns: list[str] = []
    for line in lines[start:]:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("CONSTRAINT"):
            break
        columns.append(stripped.rstrip(",").split()[0])
    return tuple(columns)


# ---------------------------------------------------------------------------
# Part one: pure functions.
# ---------------------------------------------------------------------------


class TestIdentityKeyK1:
    def test_two_tokens_join_first_and_last(self) -> None:
        assert identity_key_k1("Anna Svensson") == "anna|svensson"

    def test_more_than_two_tokens_drop_the_middle(self) -> None:
        assert identity_key_k1("Anna Maria Svensson") == "anna|svensson"

    def test_single_token_is_itself(self) -> None:
        assert identity_key_k1("Cher") == "cher"

    def test_blank_name_is_empty_string(self) -> None:
        assert identity_key_k1("   ") == ""
        assert identity_key_k1("") == ""

    def test_whitespace_and_case_are_normalized(self) -> None:
        assert identity_key_k1("  ANNA   svensson  ") == "anna|svensson"

    @pytest.mark.parametrize(
        "name",
        [
            "Anna Svensson",
            "Anna Maria Svensson",
            "Cher",
            "",
            "   ",
            "  ANNA   svensson  ",
            "Åsa Öberg",
            "Erik B. Karlsson",
            "van der Berg Jan",
        ],
    )
    def test_k1_matches_normalizations_current_key(self, name: str) -> None:
        """THE PIN (controller ruling): K1 is defined LOCALLY (copied, not imported) so a
        future Task 3 change to normalization's key cannot silently change this frozen
        baseline. This test documents that today the two independent implementations agree;
        it is expected to need updating (or intentional removal) once Task 3 changes
        normalization's key."""
        assert identity_key_k1(name) == _name_match_key(name)


class TestIdentityKeyK2:
    def test_all_tokens_are_kept_space_joined(self) -> None:
        assert identity_key_k2("Anna Maria Svensson") == "anna maria svensson"

    def test_whitespace_and_case_are_normalized(self) -> None:
        assert identity_key_k2("  ANNA   Maria   svensson  ") == "anna maria svensson"

    def test_blank_name_is_empty_string(self) -> None:
        assert identity_key_k2("   ") == ""

    def test_diacritics_are_preserved_and_distinguish_names(self) -> None:
        assert identity_key_k2("Åsa Öberg") == "åsa öberg"
        assert identity_key_k2("Asa Oberg") == "asa oberg"
        assert identity_key_k2("Åsa Öberg") != identity_key_k2("Asa Oberg")
        # ... and even the K1 keys differ, since å/ö are not stripped to a/o there either.
        assert identity_key_k1("Åsa Öberg") != identity_key_k1("Asa Oberg")


def _row(
    *,
    company_id: str = "5560000001",
    source: str = "bolagsverket",
    uid: str,
    name: str,
    qid: str = "",
) -> PersonObservationRow:
    return PersonObservationRow(
        company_id=company_id,
        source=source,
        source_record_uid=uid,
        full_name=name,
        person_wikidata_id=qid,
    )


class TestK3MergeGroupsSupersetMerge:
    """Required case: "Anna Maria Svensson" vs "Anna Svensson" -- K1 merges, K3 merges via
    the unique-minimal-superset rule (not a collision)."""

    def test_merges_into_one_group(self) -> None:
        rows = [
            _row(uid="b1", source="bolagsverket", name="Anna Maria Svensson"),
            _row(uid="e1", source="esef", name="Anna Svensson"),
        ]
        decisions = k3_merge_groups(rows)
        assert len(decisions) == 1
        [decision] = decisions
        assert decision.is_collision_candidate is False
        assert decision.candidate_group_id == ""
        assert decision.k1_keys == frozenset({"anna|svensson"})
        assert {row.source_record_uid for row in decision.rows} == {"b1", "e1"}
        assert decision.k3_person_key == "anna maria svensson|anna svensson"


class TestK3MergeGroupsAmbiguousCollision:
    """Required case: "Anna Svensson" vs "Anna B Svensson" vs "Anna C Svensson" -- K1 merges
    all three; K3 cannot pick a unique superset for "Anna Svensson" (two incomparable
    candidates), so all three stay separate and become one collision-candidate group."""

    def test_three_names_stay_separate_and_collide(self) -> None:
        rows = [
            _row(uid="b1", name="Anna Svensson"),
            _row(uid="b2", name="Anna B Svensson"),
            _row(uid="b3", name="Anna C Svensson"),
        ]
        decisions = k3_merge_groups(rows)
        assert len(decisions) == 3
        assert all(decision.is_collision_candidate for decision in decisions)
        group_ids = {decision.candidate_group_id for decision in decisions}
        assert len(group_ids) == 1, "all three share one candidate_group_id"
        assert "" not in group_ids
        person_keys = {decision.k3_person_key for decision in decisions}
        assert person_keys == {"anna svensson", "anna b svensson", "anna c svensson"}

    def test_candidate_group_id_is_deterministic(self) -> None:
        rows = [
            _row(uid="b1", name="Anna Svensson"),
            _row(uid="b2", name="Anna B Svensson"),
            _row(uid="b3", name="Anna C Svensson"),
        ]
        first = {d.candidate_group_id for d in k3_merge_groups(rows)}
        second = {d.candidate_group_id for d in k3_merge_groups(list(reversed(rows)))}
        assert first == second


class TestK3MergeGroupsDiacritics:
    """Required case: "Åsa Öberg" != "Asa Oberg" under K2/K3 -- distinct K1 buckets too, so
    they are just two ordinary unrelated persons, never a collision."""

    def test_diacritic_and_ascii_names_never_merge_or_collide(self) -> None:
        rows = [
            _row(uid="b1", name="Åsa Öberg"),
            _row(uid="b2", name="Asa Oberg"),
        ]
        decisions = k3_merge_groups(rows)
        assert len(decisions) == 2
        assert all(not decision.is_collision_candidate for decision in decisions)
        assert {decision.k3_person_key for decision in decisions} == {
            "åsa öberg",
            "asa oberg",
        }


class TestK3MergeGroupsQidLink:
    """Required case: a shared Wikidata QID merges two rows even though their names (and K1
    buckets) are entirely different -- an authoritative cross-bucket link."""

    def test_shared_qid_merges_across_different_k1_buckets(self) -> None:
        rows = [
            _row(uid="w1", source="wikidata", name="Erik Svensson", qid="Q42"),
            _row(uid="w2", source="wikidata", name="E Svensson", qid="Q42"),
        ]
        decisions = k3_merge_groups(rows)
        assert len(decisions) == 1
        [decision] = decisions
        assert decision.k1_keys == frozenset({"erik|svensson", "e|svensson"})
        assert {row.source_record_uid for row in decision.rows} == {"w1", "w2"}
        # A confirmed QID link is not ambiguous -- never flagged for review.
        assert decision.is_collision_candidate is False

    def test_unrelated_qids_do_not_merge(self) -> None:
        rows = [
            _row(uid="w1", source="wikidata", name="Erik Svensson", qid="Q1"),
            _row(uid="w2", source="wikidata", name="Lars Andersson", qid="Q2"),
        ]
        decisions = k3_merge_groups(rows)
        assert len(decisions) == 2


class TestK3MergeGroupsBlankNames:
    def test_blank_and_whitespace_only_names_are_dropped(self) -> None:
        rows = [
            _row(uid="b1", name="Anna Svensson"),
            _row(uid="b2", name=""),
            _row(uid="b3", name="   "),
        ]
        decisions = k3_merge_groups(rows)
        assert len(decisions) == 1
        assert decisions[0].rows[0].source_record_uid == "b1"

    def test_all_blank_returns_no_decisions(self) -> None:
        rows = [_row(uid="b1", name=""), _row(uid="b2", name="   ")]
        assert k3_merge_groups(rows) == []


class TestK3MergeGroupsCompanyScope:
    def test_rejects_rows_from_multiple_companies(self) -> None:
        rows = [
            _row(company_id="5560000001", uid="b1", name="Anna Svensson"),
            _row(company_id="5560000002", uid="b2", name="Anna Svensson"),
        ]
        with pytest.raises(ValueError, match="company-scoped"):
            k3_merge_groups(rows)


class TestEvaluateSeCompanyPersonIdentity:
    def test_counts_and_blank_exclusion(self) -> None:
        rows = [
            # Company A: superset merge (K1 merges, K3 merges -- not a collision).
            _row(company_id="5560000001", uid="a-b1", name="Anna Maria Svensson"),
            _row(company_id="5560000001", uid="a-e1", source="esef", name="Anna Svensson"),
            # Company B: three-way ambiguous collision.
            _row(company_id="5560000002", uid="b-b1", name="Anna Svensson"),
            _row(company_id="5560000002", uid="b-b2", name="Anna B Svensson"),
            _row(company_id="5560000002", uid="b-b3", name="Anna C Svensson"),
            # Excluded: blank name.
            _row(company_id="5560000002", uid="b-b4", name="   "),
        ]
        result = evaluate_se_company_person_identity(rows)
        assert isinstance(result, IdentityEvaluationResult)
        assert result.excluded_blank_name_count == 1
        # K1 persons: {A: anna|svensson} + {B: anna|svensson} = 2 distinct (company, key).
        assert result.k1_person_count == 2
        # K2 persons: A has 2 spellings, B has 3 spellings = 5.
        assert result.k2_person_count == 5
        # K3 persons: A resolves to 1 group, B stays 3 groups = 4.
        assert result.k3_person_count == 4
        assert result.merge_count == 1  # company A's K1 bucket
        assert result.split_count == 1  # company B's K1 bucket
        assert result.collision_candidate_count == 1
        [group] = result.candidate_groups
        assert group.company_id == "5560000002"
        assert len(group.members) == 3

    def test_no_rows_is_empty_result(self) -> None:
        result = evaluate_se_company_person_identity([])
        assert result.k1_person_count == 0
        assert result.k3_person_count == 0
        assert result.collision_candidate_count == 0
        assert result.excluded_blank_name_count == 0


class TestCandidateTableColumns:
    def test_column_order_matches_migration_ddl(self) -> None:
        statement = _create_candidate_table_statement(_sql("up"))
        assert (
            _candidate_table_ddl_columns(statement)
            == SE_COMPANY_PERSON_COLLISION_CANDIDATE_COLUMNS
        )

    def test_candidate_table_rows_follow_the_pinned_column_order(self) -> None:
        decision = MergeDecision(
            company_id="5560000001",
            k3_person_key="anna maria svensson|anna svensson",
            k1_keys=frozenset({"anna|svensson"}),
            rows=(_row(uid="b1", name="Anna Svensson"),),
            is_collision_candidate=True,
            candidate_group_id="deadbeef",
        )
        group = CollisionCandidateGroup(
            company_id="5560000001",
            candidate_group_id="deadbeef",
            members=(
                CollisionCandidateMember(
                    person_key=decision.k3_person_key, row=decision.rows[0]
                ),
            ),
        )
        created_at = datetime(2026, 8, 27, tzinfo=UTC)
        [row] = _candidate_table_rows([group], created_at=created_at)
        assert row == (
            "5560000001",
            "deadbeef",
            "anna maria svensson|anna svensson",
            "Anna Svensson",
            "bolagsverket",
            "b1",
            row[6],
            created_at,
        )
        assert '"k1_key": "anna|svensson"' in row[6]


# ---------------------------------------------------------------------------
# Part two: executed against a real ClickHouse engine (clickhouse-local).
# ---------------------------------------------------------------------------


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
    probe = subprocess.run(
        [docker, "info"], capture_output=True, text=True, timeout=60, check=False
    )
    if probe.returncode != 0:
        pytest.skip("docker is installed but not running")
    return [docker, "run", "--rm", "-i", CLICKHOUSE_IMAGE, "clickhouse-local", "--multiquery"]


def _run_clickhouse_local(script: str) -> str:
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(
            command, input=script, capture_output=True, text=True, timeout=900
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return completed.stdout


# Minimal upstream schema -- only the columns the three views actually project (the fuller
# fixture with materialized hashes lives in test_se_company_person_views.py; this test's
# concern is the identity evaluation over view OUTPUT, not the source hashes).
_UPSTREAM_SCHEMA_SQL = """
CREATE TABLE corpscout.se_financial_report_signatories
(
    company_id String,
    fiscal_year Int32,
    statement_key String,
    source_record_uid String,
    signatory_kind LowCardinality(String),
    person_seq UInt16,
    first_name String,
    last_name String,
    person_profile_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(first_name, '|', last_name)))),
    role_original String,
    role_kind LowCardinality(String),
    person_role_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(role_original, '|', role_kind)))),
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
    person_profile_hash FixedString(64) MATERIALIZED lower(hex(SHA256(name))),
    role String,
    role_category LowCardinality(String),
    organization String,
    status LowCardinality(String),
    person_role_hash FixedString(64) MATERIALIZED lower(hex(SHA256(role))),
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
    person_role_hash FixedString(64) MATERIALIZED lower(hex(SHA256(role_property))),
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (company_wikidata_id, role_property, person_wikidata_id);

CREATE TABLE corpscout.wikidata_persons
(
    person_wikidata_id String,
    source_record_uid String,
    person_profile_hash FixedString(64) MATERIALIZED lower(hex(SHA256(name))),
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

COMPANY_COLLISION = "5560000009"  # ambiguous 3-way K1 collision, exercised via the views

_FIXTURE_SQL = f"""
INSERT INTO corpscout.se_financial_report_signatories
    (company_id, fiscal_year, statement_key, source_record_uid, signatory_kind,
     person_seq, first_name, last_name, role_original, role_kind, resolved_at)
VALUES
    ('{COMPANY_COLLISION}', 2024, 'stmt-1', 'src-1', 'board_signature', 1,
     'Anna', 'Svensson', 'Styrelseledamot', 'board_member',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC')),
    ('{COMPANY_COLLISION}', 2024, 'stmt-1', 'src-2', 'board_signature', 2,
     'Anna B', 'Svensson', 'Styrelseledamot', 'board_member',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'));

INSERT INTO corpscout.esef_document_people
    (candidate_uid, source_record_uid, source_document_id, country_code, company_id,
     fiscal_year, name, role, role_category, organization, status, effective_from,
     effective_to, confidence, evidence_ids, model_provider, model_name, prompt_version,
     source_run_id, extracted_at)
VALUES
    ('{'esefcand1'.ljust(64, '0')}', '{'esefsrc1'.ljust(64, '0')}', 'doc-1', 'SE',
     '{COMPANY_COLLISION}', 2024, 'Anna C Svensson', 'CEO', 'executive', 'Acme AB',
     'active', '2020-01-01', NULL, 0.9, [], 'openai', 'gpt', 'v1', 'run-esef-1',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'));
"""


def _view_creation_statements() -> list[str]:
    up_sql = _sql("up")
    return [
        _create_view_statement(up_sql, view)
        for view in (BOLAGSVERKET_VIEW, ESEF_VIEW, WIKIDATA_VIEW)
    ]


def _read_script() -> str:
    statements = [
        "CREATE DATABASE IF NOT EXISTS corpscout",
        *(s.strip() for s in _UPSTREAM_SCHEMA_SQL.split(";") if s.strip()),
        *(s.strip() for s in _FIXTURE_SQL.split(";") if s.strip()),
        *_view_creation_statements(),
        "SELECT '@@bolagsverket'",
        f"SELECT company_id, full_name, source_record_uid FROM {BOLAGSVERKET_VIEW} "
        "ORDER BY source_record_uid",
        "SELECT '@@esef'",
        f"SELECT company_id, full_name, source_record_uid FROM {ESEF_VIEW} "
        "ORDER BY source_record_uid",
        "SELECT '@@wikidata'",
        f"SELECT company_id, full_name, source_record_uid, person_wikidata_id "
        f"FROM {WIKIDATA_VIEW} ORDER BY source_record_uid",
    ]
    return ";\n".join(statements) + ";\n"


def _parse_sections(stdout: str) -> dict[str, list[list[str]]]:
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in stdout.splitlines():
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        elif line.strip():
            result[current].append(line.split("\t"))
    return result


@pytest.mark.integration
def test_asset_body_evaluates_real_view_output_into_a_collision_candidate() -> None:
    """Executed clickhouse-local test of the asset body, part 1: the real migration-embedded
    view SQL feeds `evaluate_se_company_person_identity` exactly as the asset's own read step
    would, producing the expected 3-way ambiguous collision for `COMPANY_COLLISION`."""
    stdout = _run_clickhouse_local(_read_script())
    sections = _parse_sections(stdout)

    rows = [
        PersonObservationRow(
            company_id=row[0], source="bolagsverket", source_record_uid=row[2],
            full_name=row[1],
        )
        for row in sections["bolagsverket"]
    ] + [
        PersonObservationRow(
            company_id=row[0], source="esef", source_record_uid=row[2], full_name=row[1]
        )
        for row in sections["esef"]
    ] + [
        PersonObservationRow(
            company_id=row[0], source="wikidata", source_record_uid=row[2],
            full_name=row[1], person_wikidata_id=row[3],
        )
        for row in sections["wikidata"]
    ]
    assert len(rows) == 3, "two Bolagsverket signatories + one ESEF person"

    result = evaluate_se_company_person_identity(rows)
    assert result.excluded_blank_name_count == 0
    assert result.k1_person_count == 1
    assert result.k2_person_count == 3
    assert result.k3_person_count == 3
    assert result.split_count == 1
    assert result.merge_count == 0
    assert result.collision_candidate_count == 1
    [group] = result.candidate_groups
    assert group.company_id == COMPANY_COLLISION
    assert len(group.members) == 3


@pytest.mark.integration
def test_asset_body_writes_candidates_through_the_real_migration_ddl() -> None:
    """Executed clickhouse-local test of the asset body, part 2: closes Task 1's deferred
    minor by actually running migration 000330's `CREATE TABLE ... se_company_person_
    collision_candidate` DDL (CHECK constraint included) and inserting the exact rows
    `_candidate_table_rows` would send, then reading them back."""
    stdout = _run_clickhouse_local(_read_script())
    sections = _parse_sections(stdout)
    rows = [
        PersonObservationRow(
            company_id=row[0], source="bolagsverket", source_record_uid=row[2],
            full_name=row[1],
        )
        for row in sections["bolagsverket"]
    ] + [
        PersonObservationRow(
            company_id=row[0], source="esef", source_record_uid=row[2], full_name=row[1]
        )
        for row in sections["esef"]
    ]
    result = evaluate_se_company_person_identity(rows)
    created_at = datetime(2026, 8, 27, 12, 0, 0, tzinfo=UTC)
    candidate_rows = _candidate_table_rows(result.candidate_groups, created_at=created_at)
    assert len(candidate_rows) == 3

    def _sql_literal(value: object) -> str:
        if isinstance(value, datetime):
            # created_at is a plain DateTime column (migration 000330), not DateTime64.
            return f"toDateTime('{value.strftime('%Y-%m-%d %H:%M:%S')}')"
        text = str(value).replace("\\", "\\\\").replace("'", "\\'")
        return f"'{text}'"

    values_sql = ",\n    ".join(
        "(" + ", ".join(_sql_literal(value) for value in row) + ")" for row in candidate_rows
    )
    columns_sql = ", ".join(SE_COMPANY_PERSON_COLLISION_CANDIDATE_COLUMNS)

    up_sql = _sql("up")
    write_script = ";\n".join(
        [
            "CREATE DATABASE IF NOT EXISTS corpscout",
            _create_candidate_table_statement(up_sql),
            f"INSERT INTO {CANDIDATE_TABLE} ({columns_sql}) VALUES\n    {values_sql}",
            "SELECT '@@candidates'",
            f"SELECT company_id, candidate_group_id, person_key, full_name, source, "
            f"source_record_uid FROM {CANDIDATE_TABLE} ORDER BY person_key",
        ]
    ) + ";\n"

    stdout = _run_clickhouse_local(write_script)
    sections = _parse_sections(stdout)
    assert len(sections["candidates"]) == 3
    group_ids = {row[1] for row in sections["candidates"]}
    assert len(group_ids) == 1, "all three rows share one candidate_group_id"
    for row in sections["candidates"]:
        assert row[0] == COMPANY_COLLISION
