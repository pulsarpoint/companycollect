"""Migration 000330: the three SE person source views, pinned and executed.

Part one is the drift pin (the `se_address_geocodes_served` pattern,
tests/test_se_address_geocodes_served_view.py): each migration-embedded
`CREATE OR REPLACE VIEW` statement is extracted, whitespace-normalized, and compared
against a FRESH render of its builder in
`dagster_v3.defs.company_people.source_views`. A hand-edit on either side goes red here
before it ever reaches a real database.

Part two runs the actual migration file (schema-relevant statements only) plus hand-built
upstream fixture tables -- assembled directly from the CURRENT column set of
`se_financial_report_signatories` (migrations 000143 + 000244 + 000289),
`esef_document_people` (000244 + 000289), `wikidata_company_people` +
`wikidata_persons` (000152 + 000244 + 000268 + 000289), `wikidata_company_identifiers`
(000013 + 000018), and `company_identifier` (000174) -- through `clickhouse-local`, exactly
as tests/test_se_company_person_clickhouse_local.py does for the older draft pipeline. The
fixture rows cover the three cases the plan calls out: a Wikidata row bridging via LEI, an
invalid-orgnr Wikidata row that must be filtered out, and an ESEF row in a non-SE country
that must be excluded. The script runs twice, once per `join_use_nulls` setting, and must
answer identically both times.

000331 (Task 3) widened all three views with one more column, `source_observed_at` --
following the `se_address_geocodes_served` precedent (000327 widening 000325 in place), it
is a SEPARATE migration that re-issues `CREATE OR REPLACE VIEW`, not a hand-edit of 000330's
already-committed rendering. The drift pin below therefore targets 000331 (the CURRENT
definition); 000330 still creates the views and the collision-candidate table, and is
exercised by the down-migration-parity test instead.
"""

from __future__ import annotations

import functools
import shutil
import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.company_people.source_views import (
    build_se_company_person_bolagsverket_view_sql,
    build_se_company_person_esef_view_sql,
    build_se_company_person_wikidata_view_sql,
)

# Only the executed clickhouse-local tests at the bottom of this module need the
# `integration` marker (they shell out to a real engine, possibly via Docker); the drift
# pin and migration-text checks above are plain string/file comparisons and always run.

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000331_corpscout_se_company_person_views_observed_at"
PRIOR_MIGRATION = "000330_corpscout_se_company_person_views"
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:26.5"

BOLAGSVERKET_VIEW = "corpscout.se_company_person_bolagsverket"
ESEF_VIEW = "corpscout.se_company_person_esef"
WIKIDATA_VIEW = "corpscout.se_company_person_wikidata"
CANDIDATE_TABLE = "corpscout.se_company_person_collision_candidate"

BUILDERS = {
    BOLAGSVERKET_VIEW: build_se_company_person_bolagsverket_view_sql,
    ESEF_VIEW: build_se_company_person_esef_view_sql,
    WIKIDATA_VIEW: build_se_company_person_wikidata_view_sql,
}


# ---------------------------------------------------------------------------
# Shared statement-extraction helpers (mirrors test_se_address_geocodes_served_view.py).
# ---------------------------------------------------------------------------


def _sql(suffix: str, migration: str = MIGRATION) -> str:
    return (MIGRATIONS_DIR / f"{migration}.{suffix}.sql").read_text(encoding="utf-8")


def _statements(sql: str) -> list[str]:
    """The statements the runner sees: `migrate/migrate` splits on `;` under
    x-multi-statement=true (corpscout/Makefile), so splitting the same way is what the
    server is actually asked to execute."""
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def _body(statement: str) -> str:
    """A statement with its leading `--` commentary stripped, so an assertion can anchor on
    the statement's first VERB and not find it inside the migration's rationale."""
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


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _executable(sql: str) -> str:
    """The file with its `--` commentary stripped: what the server is told to do."""
    return "\n".join(line.split("--")[0] for line in sql.splitlines())


# ---------------------------------------------------------------------------
# Part one: the drift pin.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("view", [BOLAGSVERKET_VIEW, ESEF_VIEW, WIKIDATA_VIEW])
def test_the_view_has_not_drifted_from_its_builder(view: str) -> None:
    """THE PIN. Red here means the builder and the deployed view have parted company.

    To fix it, do NOT edit the SQL file by hand to match -- write the next migration that
    replaces the view with the new rendering, and point this pin at it.
    """
    embedded = _normalized(_create_view_statement(_sql("up"), view))
    rendered = _normalized(BUILDERS[view]())
    assert embedded == rendered


def test_the_pins_are_not_vacuous() -> None:
    """Guards the extraction: each embedded body is real, non-trivial SQL naming the source
    table it reads, so a broken extractor cannot make the pin pass on two empty strings."""
    up_sql = _sql("up")
    bolagsverket = _normalized(_create_view_statement(up_sql, BOLAGSVERKET_VIEW))
    esef = _normalized(_create_view_statement(up_sql, ESEF_VIEW))
    wikidata = _normalized(_create_view_statement(up_sql, WIKIDATA_VIEW))

    assert len(bolagsverket) > 100
    assert "se_financial_report_signatories" in bolagsverket
    assert "trim(concat(first_name" in bolagsverket
    assert "resolved_at AS source_observed_at" in bolagsverket

    assert len(esef) > 100
    assert "esef_document_people FINAL" in esef
    assert "country_code = 'SE'" in esef
    assert "extracted_at AS source_observed_at" in esef

    assert len(wikidata) > 300
    assert "wikidata_company_identifiers" in wikidata
    assert "company_identifier" in wikidata
    assert "'se_orgnr'" in wikidata
    assert "'lei'" in wikidata
    assert "match(company_id, '^[0-9]{10}([0-9]{2})?$')" in wikidata
    assert "greatest(links.resolved_at, persons.resolved_at) AS source_observed_at" in wikidata


def test_up_migration_creates_database_first_and_only_touches_corpscout() -> None:
    statements = _statements(_sql("up"))
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert "DROP" not in _executable(_sql("up")).upper()


def test_up_migration_replaces_the_views_in_place_and_creates_nothing_new() -> None:
    """000331 (Task 3) only widens the three views. The collision-candidate table is
    000330's object -- unchanged and not touched here."""
    up_sql = _sql("up")
    for view in (BOLAGSVERKET_VIEW, ESEF_VIEW, WIKIDATA_VIEW):
        assert _body(_create_view_statement(up_sql, view)).startswith(
            f"CREATE OR REPLACE VIEW {view} AS\n"
        )
    assert "CREATE TABLE" not in _executable(up_sql).upper()


def test_the_prior_migration_still_creates_the_collision_candidate_table() -> None:
    """Unchanged by Task 3 -- pinned here so a future edit to 000330 is caught by this
    module too, not only by the migration-file-list test."""
    prior_up = _sql("up", migration=PRIOR_MIGRATION)
    [statement] = [
        _body(s)
        for s in _statements(prior_up)
        if f"CREATE TABLE IF NOT EXISTS {CANDIDATE_TABLE}" in s
    ]
    for column in (
        "company_id String",
        "candidate_group_id String",
        "person_key String",
        "full_name String",
        "source LowCardinality(String)",
        "source_record_uid String",
        "evidence_json String",
        "created_at DateTime DEFAULT now()",
    ):
        assert column in statement
    assert "CHECK match(company_id, '^[0-9]{10}([0-9]{2})?$')" in statement
    assert "ENGINE = MergeTree" in statement
    assert "ORDER BY (company_id, candidate_group_id)" in statement


def test_down_migration_restores_the_prior_migrations_original_rendering() -> None:
    """000331's down-file does not DROP the views -- 000330 owns their creation, and this
    migration only widened their definitions. Reverting means putting 000330's exact
    original renderings back with CREATE OR REPLACE VIEW, source_observed_at removed again."""
    executable_down = _executable(_sql("down"))
    assert "DROP" not in executable_down.upper()
    up_sql = _sql("up")
    prior_up_sql = _sql("up", migration=PRIOR_MIGRATION)
    down_sql = _sql("down")
    for view in (BOLAGSVERKET_VIEW, ESEF_VIEW, WIKIDATA_VIEW):
        restored = _normalized(_body(_create_view_statement(down_sql, view)))
        original = _normalized(_body(_create_view_statement(prior_up_sql, view)))
        widened = _normalized(_body(_create_view_statement(up_sql, view)))
        assert restored == original
        assert "source_observed_at" not in restored
        assert restored != widened


def test_prior_migrations_down_file_still_drops_every_object_it_created() -> None:
    """Unchanged by Task 3 -- 000330's own down-migration still fully tears down the views
    and the collision-candidate table; nothing here needed to change when 000331 was added."""
    executable_down = _executable(_sql("down", migration=PRIOR_MIGRATION))
    for view in (BOLAGSVERKET_VIEW, ESEF_VIEW, WIKIDATA_VIEW):
        assert f"DROP VIEW IF EXISTS {view}" in executable_down
    assert f"DROP TABLE IF EXISTS {CANDIDATE_TABLE}" in executable_down
    # Never touches an upstream source table.
    for upstream in (
        "se_financial_report_signatories",
        "esef_document_people",
        "wikidata_company_people",
        "wikidata_persons",
        "wikidata_company_identifiers",
        "company_identifier",
    ):
        assert f"DROP TABLE IF EXISTS corpscout.{upstream}" not in executable_down


# ---------------------------------------------------------------------------
# Part two: executed against a real ClickHouse engine.
# ---------------------------------------------------------------------------

# Assembled directly from the CURRENT column set of each upstream table (see module
# docstring for the migrations each block reflects). Materialized columns
# (signatory_uid/person_profile_hash/person_role_hash) are declared here exactly as
# migration 000289 defines them, so an INSERT that omits them exercises the real
# expression rather than a stand-in.
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


def _hex64(tag: str) -> str:
    """A deterministic, exactly-64-character FixedString(64) filler -- long enough that two
    tags never collide, and never padded by ClickHouse (which would introduce NUL bytes into
    the TSV output this harness parses)."""
    body = (tag + "0" * 64)[:64]
    assert len(body) == 64
    return body


PENDING_A = "5560000001"  # bolagsverket + esef + wikidata se_orgnr bridge
LEI_COMPANY_B = "5560000002"  # wikidata LEI bridge only
INVALID_ORGNR = "123"  # too short: must never reach a view row


_FIXTURE_SQL = f"""
INSERT INTO corpscout.se_financial_report_signatories
    (company_id, fiscal_year, statement_key, source_record_uid, signatory_kind,
     person_seq, first_name, last_name, role_original, role_kind, resolved_at)
VALUES
    ('{PENDING_A}', 2024, 'stmt-bv-1', 'src-bv-1', 'board_signature', 1,
     'Erik', 'Svensson', 'Styrelseledamot', 'board_member',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'));

INSERT INTO corpscout.esef_document_people
    (candidate_uid, source_record_uid, source_document_id, country_code, company_id,
     fiscal_year, name, role, role_category, organization, status, effective_from,
     effective_to, confidence, evidence_ids, model_provider, model_name, prompt_version,
     source_run_id, extracted_at)
VALUES
    ('{_hex64("esefcand1")}', '{_hex64("esefsrc1")}', 'doc-1', 'SE', '{PENDING_A}', 2024,
     'Anna Karlsson', 'CEO', 'executive', 'Acme AB', 'active', '2020-01-01', NULL, 0.9,
     [], 'openai', 'gpt', 'v1', 'run-esef-1',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC')),
    ('{_hex64("esefcand2")}', '{_hex64("esefsrc2")}', 'doc-2', 'DK', 'DK12345678', 2024,
     'Lars Hansen', 'Chairman', 'board', 'Acme DK', 'active', NULL, NULL, 0.8,
     [], 'openai', 'gpt', 'v1', 'run-esef-2',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'));

INSERT INTO corpscout.company_identifier
    (issuer_scheme, issuer_id, country_code, company_id, match_method, match_confidence,
     registration_authority_id, registered_as_raw, company_id_normalized, entity_status,
     registration_status, is_current, successor_issuer_id, first_seen_date, last_seen_date,
     source_run_id, resolved_at)
VALUES
    ('lei', '549300ABCDEFGHIJ123', 'SE', '{LEI_COMPANY_B}', 'manual', 'high',
     'bolagsverket', '{LEI_COMPANY_B}', '{LEI_COMPANY_B}', 'active', 'active', 1, '',
     '2020-01-01', '2026-08-01', 'run-ci-1',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'));

INSERT INTO corpscout.wikidata_company_identifiers
    (wikidata_id, identifier_type, wikidata_property_id, identifier_value,
     identifier_scope, is_primary, source_system, source_run_id, source_record_id,
     source_payload_hash, retrieved_at, resolved_at)
VALUES
    ('Q100001', 'se_orgnr', 'P1454', '556-000-0001', NULL, 1, 'wikidata', 'run-wd-1',
     'rec-wd-1o', '{_hex64("wdid1o")}', toDateTime64('2026-08-01 00:00:00', 3, 'UTC'),
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC')),
    ('Q100002', 'lei', 'P1278', '549300abcdefghij123', NULL, 1, 'wikidata', 'run-wd-1',
     'rec-wd-2', '{_hex64("wdid2")}', toDateTime64('2026-08-01 00:00:00', 3, 'UTC'),
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC')),
    ('Q100003', 'se_orgnr', 'P1454', '{INVALID_ORGNR}', NULL, 1, 'wikidata', 'run-wd-1',
     'rec-wd-3', '{_hex64("wdid3")}', toDateTime64('2026-08-01 00:00:00', 3, 'UTC'),
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'));

INSERT INTO corpscout.wikidata_company_people
    (company_wikidata_id, person_wikidata_id, role_property, role_label, start_date,
     end_date, is_current, source_system, source_run_id, source_record_id,
     source_payload_hash, retrieved_at, resolved_at)
VALUES
    ('Q100001', 'Q200001', 'P488', 'chairperson', '2019-01-01', '2023-01-01', 0,
     'wikidata', 'run-wd-1', 'rec-link-1', '{_hex64("wdlink1")}',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'),
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC')),
    ('Q100002', 'Q200002', 'P169', 'chief executive officer', '2020-01-01', NULL, 1,
     'wikidata', 'run-wd-1', 'rec-link-2', '{_hex64("wdlink2")}',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'),
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC')),
    ('Q100003', 'Q200003', 'P169', 'chief executive officer', NULL, NULL, 1,
     'wikidata', 'run-wd-1', 'rec-link-3', '{_hex64("wdlink3")}',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'),
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'));

INSERT INTO corpscout.wikidata_persons
    (person_wikidata_id, source_record_uid, name, name_normalized, description,
     birth_year, image_url, wikidata_url, source_system, source_run_id, source_record_id,
     source_payload_hash, retrieved_at, resolved_at)
VALUES
    ('Q200001', '{_hex64("wdpsrc1")}', 'Karl Karlsson', 'karl karlsson',
     'Board chairperson', 1960, NULL, 'https://www.wikidata.org/wiki/Q200001',
     'wikidata', 'run-wd-1', 'rec-person-1', '{_hex64("wdppay1")}',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'),
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC')),
    ('Q200002', '{_hex64("wdpsrc2")}', 'Anna Andersson', 'anna andersson',
     'Swedish executive', 1975, 'https://example.com/img2.jpg',
     'https://www.wikidata.org/wiki/Q200002', 'wikidata', 'run-wd-1', 'rec-person-2',
     '{_hex64("wdppay2")}', toDateTime64('2026-08-01 00:00:00', 3, 'UTC'),
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC')),
    ('Q200003', '{_hex64("wdpsrc3")}', 'Invalid Orgnr Person', 'invalid orgnr person',
     NULL, NULL, NULL, 'https://www.wikidata.org/wiki/Q200003', 'wikidata', 'run-wd-1',
     'rec-person-3', '{_hex64("wdppay3")}', toDateTime64('2026-08-01 00:00:00', 3, 'UTC'),
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'));
"""


@functools.cache
def _clickhouse_local_command() -> list[str]:
    """A `clickhouse-local` invocation, or skip when the machine has none."""
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


def _view_creation_statements() -> list[str]:
    """The CREATE OR REPLACE VIEW statements straight out of the migration file, so the
    executed test proves the actual migration -- not just the builder in isolation -- runs
    against a real engine."""
    up_sql = _sql("up")
    return [
        _create_view_statement(up_sql, view)
        for view in (BOLAGSVERKET_VIEW, ESEF_VIEW, WIKIDATA_VIEW)
    ]


def _script(*, join_use_nulls: int) -> str:
    statements = [
        f"SET join_use_nulls = {join_use_nulls}",
        "CREATE DATABASE IF NOT EXISTS corpscout",
        *(s.strip() for s in _UPSTREAM_SCHEMA_SQL.split(";") if s.strip()),
        *(s.strip() for s in _FIXTURE_SQL.split(";") if s.strip()),
        *_view_creation_statements(),
        "SELECT '@@bolagsverket'",
        "SELECT company_id, source_record_uid, full_name, first_name, last_name, "
        "role_original, role_kind, signatory_kind, fiscal_year, "
        "length(person_profile_hash), length(person_role_hash), "
        "toString(source_observed_at) "
        f"FROM {BOLAGSVERKET_VIEW} ORDER BY company_id",
        "SELECT '@@esef'",
        "SELECT company_id, full_name, role, role_category, organization, status, "
        "toString(effective_from), confidence, length(person_profile_hash), "
        "length(person_role_hash), toString(source_observed_at) "
        f"FROM {ESEF_VIEW} ORDER BY company_id",
        "SELECT '@@wikidata'",
        "SELECT company_id, full_name, person_wikidata_id, role_property, "
        "ifNull(toString(birth_year), ''), ifNull(external_url, ''), "
        "length(source_record_uid), length(person_profile_hash), "
        "length(person_role_hash), toString(source_observed_at) "
        f"FROM {WIKIDATA_VIEW} ORDER BY person_wikidata_id",
    ]
    return ";\n".join(statements) + ";\n"


@pytest.fixture(
    scope="module",
    params=(0, 1),
    ids=("join_use_nulls_off", "join_use_nulls_on"),
)
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(
            command,
            input=_script(join_use_nulls=request.param),
            capture_output=True,
            text=True,
            timeout=900,
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
def test_bolagsverket_view_projects_split_names_and_concatenated_full_name(
    sections: dict[str, list[list[str]]],
) -> None:
    assert sections["bolagsverket"] == [
        [
            PENDING_A,
            "src-bv-1",
            "Erik Svensson",
            "Erik",
            "Svensson",
            "Styrelseledamot",
            "board_member",
            "board_signature",
            "2024",
            "64",
            "64",
            "2026-08-01 00:00:00.000",
        ]
    ]


@pytest.mark.integration
def test_esef_view_excludes_non_se_country(
    sections: dict[str, list[list[str]]],
) -> None:
    """The required fixture case: a DK row must never reach the view."""
    assert len(sections["esef"]) == 1
    row = sections["esef"][0]
    assert row[0] == PENDING_A
    assert row[1] == "Anna Karlsson"
    assert row[2] == "CEO"
    assert row[10] == "2026-08-01 00:00:00.000"  # extracted_at AS source_observed_at
    # No row carries the DK company or the Danish person's name.
    assert all(r[0] != "DK12345678" for r in sections["esef"])
    assert all("Hansen" not in r[1] for r in sections["esef"])


@pytest.mark.integration
def test_wikidata_view_bridges_via_orgnr_and_lei_and_filters_invalid_orgnr(
    sections: dict[str, list[list[str]]],
) -> None:
    rows = {row[2]: row for row in sections["wikidata"]}  # keyed by person_wikidata_id
    assert set(rows) == {"Q200001", "Q200002"}, (
        "Q200003 bridges via an invalid se_orgnr ('123') and must be filtered out"
    )

    orgnr_row = rows["Q200001"]
    assert orgnr_row[0] == PENDING_A
    assert orgnr_row[1] == "Karl Karlsson"
    assert orgnr_row[3] == "P488"
    assert orgnr_row[4] == "1960"

    lei_row = rows["Q200002"]
    assert lei_row[0] == LEI_COMPANY_B, "LEI bridge must resolve to the SE company_id"
    assert lei_row[1] == "Anna Andersson"
    assert lei_row[3] == "P169"
    assert lei_row[4] == "1975"
    assert lei_row[5] == "https://www.wikidata.org/wiki/Q200002"
    assert lei_row[6] == "64"  # source_record_uid length
    assert lei_row[7] == "64"  # person_profile_hash length
    assert lei_row[8] == "64"  # person_role_hash length
    assert lei_row[9] == "2026-08-01 00:00:00.000"  # greatest(link, person resolved_at)
