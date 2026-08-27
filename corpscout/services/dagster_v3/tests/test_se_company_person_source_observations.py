"""The shared source_observations CTE (source_views.py) -- Task 3's replacement for
se_company_person_draft as normalization.py's and roles.py's read.

Part one is text/unit pins: the SQL blank-name filter on all three branches, the v2 draft_id
hash domain, and the Python/SQL draft_id parity helper (`source_observation_id`) against a
known input -- ClickHouse's `reinterpretAsUUID(unhex(hex_string))` byte-reverses each 8-byte
half relative to a plain `uuid.UUID(bytes=...)` parse, confirmed empirically against a real
engine; `source_observation_id` replicates that so a test (not production code, which never
recomputes draft_id in Python) can assert the two independent implementations agree.

Part two executes the real SQL against clickhouse-local: one blank-name and one real row per
source, proving the CTE excludes the blank rows, the companion blank-count query counts
exactly what the CTE excluded, and the query-returned draft_id equals what
`source_observation_id` computes from the SAME row's own identity fields.
"""

import functools
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest

from dagster_v3.defs.company_people.source_views import (
    SOURCE_OBSERVATION_HASH_DOMAIN,
    build_se_company_person_blank_full_name_count_sql,
    build_se_company_person_source_observations_sql,
    source_observation_id,
)

CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:26.5"


def test_hash_domain_is_v2() -> None:
    assert SOURCE_OBSERVATION_HASH_DOMAIN == "se-company-person-source-observation-v2"


def test_source_observation_id_matches_a_known_clickhouse_result() -> None:
    """Empirically confirmed against a real engine:
    SELECT reinterpretAsUUID(unhex(substring(hex(SHA256(concat(
        'se-company-person-source-observation-v2\\n',
        '5560000001', '\\n', 'bolagsverket', '\\n', 'src', '\\n',
        repeat('a', 64), '\\n', repeat('b', 64)
    ))), 1, 32))) = '74574618-5e8c-1aea-f27e-a26dd07cd279'
    """
    result = source_observation_id(
        company_id="5560000001",
        source="bolagsverket",
        source_record_uid="src",
        person_profile_hash="a" * 64,
        person_role_hash="b" * 64,
    )
    assert result == uuid.UUID("74574618-5e8c-1aea-f27e-a26dd07cd279")


def test_shared_cte_excludes_blank_names_on_every_branch() -> None:
    sql = build_se_company_person_source_observations_sql()
    assert sql.count("WHERE trim(full_name) != ''") == 3
    assert "source_observations AS (" in sql
    assert sql.count(SOURCE_OBSERVATION_HASH_DOMAIN) == 3  # once per UNION ALL branch


def test_shared_cte_custom_name_is_used_throughout() -> None:
    sql = build_se_company_person_source_observations_sql(cte_name="draft_observations")
    assert sql.startswith("draft_observations AS (")
    assert "source_observations AS (" not in sql


def test_blank_full_name_count_sql_reads_the_unfiltered_views() -> None:
    sql = build_se_company_person_blank_full_name_count_sql()
    assert "countIf(trim(full_name) = '')" in sql
    assert "FROM corpscout.se_company_person_bolagsverket" in sql
    assert "FROM corpscout.se_company_person_esef" in sql
    assert "FROM corpscout.se_company_person_wikidata" in sql
    assert "WHERE" not in sql  # unfiltered -- the count is over every raw view row


# ---------------------------------------------------------------------------
# Part two: executed against a real ClickHouse engine.
# ---------------------------------------------------------------------------

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
VIEW_MIGRATIONS = (
    "000330_corpscout_se_company_person_views.up.sql",
    "000331_corpscout_se_company_person_views_observed_at.up.sql",
)

_UPSTREAM_SCHEMA_SQL = """
CREATE DATABASE IF NOT EXISTS corpscout;

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

COMPANY = "5560000099"

_FIXTURE_SQL = f"""
INSERT INTO corpscout.se_financial_report_signatories
    (company_id, fiscal_year, statement_key, source_record_uid, signatory_kind,
     person_seq, first_name, last_name, role_original, role_kind, resolved_at)
VALUES
    ('{COMPANY}', 2024, 'stmt-1', 'src-bv-1', 'board_signature', 1,
     'Erik', 'Svensson', 'Styrelseledamot', 'board_member',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC')),
    ('{COMPANY}', 2024, 'stmt-1', 'src-bv-1', 'board_signature', 2,
     '', '', 'Styrelseledamot', 'board_member',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'));

INSERT INTO corpscout.esef_document_people
    (candidate_uid, source_record_uid, source_document_id, country_code, company_id,
     fiscal_year, name, role, role_category, organization, status, effective_from,
     effective_to, confidence, evidence_ids, model_provider, model_name, prompt_version,
     source_run_id, extracted_at)
VALUES
    ('{"a" * 64}', '{"b" * 64}', 'doc-1', 'SE', '{COMPANY}', 2024,
     'Anna Karlsson', 'CEO', 'executive', 'Acme AB', 'active', NULL, NULL, 0.9,
     [], 'openai', 'gpt', 'v1', 'run-esef-1',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC')),
    ('{"c" * 64}', '{"d" * 64}', 'doc-2', 'SE', '{COMPANY}', 2024,
     '  ', 'CEO', 'executive', 'Acme AB', 'active', NULL, NULL, 0.9,
     [], 'openai', 'gpt', 'v1', 'run-esef-2',
     toDateTime64('2026-08-01 00:00:00', 3, 'UTC'));
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
    probe = subprocess.run(
        [docker, "info"], capture_output=True, text=True, timeout=60, check=False
    )
    if probe.returncode != 0:
        pytest.skip("docker is installed but not running")
    return [docker, "run", "--rm", "-i", CLICKHOUSE_IMAGE, "clickhouse-local", "--multiquery"]


def _view_statements() -> list[str]:
    statements: list[str] = []
    for name in VIEW_MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(("CREATE DATABASE", "CREATE OR REPLACE VIEW")):
                statements.append(statement)
    return statements


def _script() -> str:
    statements = [
        *(s.strip() for s in _UPSTREAM_SCHEMA_SQL.split(";") if s.strip()),
        *(s.strip() for s in _FIXTURE_SQL.split(";") if s.strip()),
        *_view_statements(),
        "SELECT '@@blank_count'",
        build_se_company_person_blank_full_name_count_sql(),
        "SELECT '@@observations'",
        f"WITH {build_se_company_person_source_observations_sql()}\n"
        "SELECT source, toString(draft_id), source_record_uid, toString(person_profile_hash), "
        "toString(person_role_hash) FROM source_observations "
        f"WHERE company_id = '{COMPANY}' ORDER BY source",
    ]
    return ";\n".join(statements) + ";\n"


@pytest.mark.integration
def test_blank_names_excluded_and_counted_and_draft_id_matches_python() -> None:
    completed = subprocess.run(
        _clickhouse_local_command(),
        input=_script(),
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    sections: dict[str, list[list[str]]] = {}
    current = ""
    for line in completed.stdout.splitlines():
        if line.startswith("@@"):
            current = line[2:]
            sections[current] = []
        elif line.strip():
            sections[current].append(line.split("\t"))

    # Two blank-name rows in the fixture (one bolagsverket, one esef) -- neither reaches
    # source_observations, and the blank-count query names exactly two.
    assert sections["blank_count"] == [["2"]]
    assert len(sections["observations"]) == 2  # only the two non-blank rows

    for source, draft_id, source_record_uid, person_profile_hash, person_role_hash in sections[
        "observations"
    ]:
        expected = source_observation_id(
            company_id=COMPANY,
            source=source,
            source_record_uid=source_record_uid,
            person_profile_hash=person_profile_hash,
            person_role_hash=person_role_hash,
        )
        assert uuid.UUID(draft_id) == expected
