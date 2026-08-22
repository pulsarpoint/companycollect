"""Execute the generated company-people SQL against a real ClickHouse engine.

The SQL-text tests pin wording; this one pins that the statements parse and
return the right rows. It runs the whole script through `clickhouse-local`: a
local binary when there is one, otherwise the pinned server image under Docker,
otherwise the module skips.

The script runs twice, once per `join_use_nulls` setting, and must answer the
same both times. A LEFT JOIN miss is the column's default with the setting off
and NULL with it on, and every guard in the role SQL reads one of those misses.
"""

import functools
import shutil
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from dagster_v3.defs.company_people.normalization import (
    build_company_statistics_sql,
    build_pending_companies_sql,
)
from dagster_v3.defs.company_people.roles import (
    _publish_role_assignments_sql,
    _role_assignment_quality_sql,
    build_role_assignments_insert_sql,
    build_stale_role_corrections_sql,
)

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATIONS = (
    "000290_corpscout_company_person_source_observations_and_role_types.up.sql",
    "000291_corpscout_se_company_person.up.sql",
    "000292_corpscout_se_company_person_roles.up.sql",
    "000293_corpscout_se_company_person_roles_by_year.up.sql",
    "000294_corpscout_employee_board_representative_role.up.sql",
    "000295_corpscout_se_company_person_corrections.up.sql",
)
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:26.5"
APPLIED_PREFIXES = ("CREATE DATABASE", "CREATE TABLE", "ALTER TABLE", "DROP TABLE", "INSERT")

PENDING_COMPANY = "5565200028"
SETTLED_COMPANY = "5560125220"
STAGE_TABLE = "`corpscout`.`stage_roles`"
NOW = datetime(2026, 8, 22, 9, tzinfo=UTC)


def _id(marker: int) -> str:
    return str(uuid.UUID(int=marker))


DRAFT_CEO = _id(1)
DRAFT_BOARD = _id(2)
DRAFT_SETTLED = _id(3)
PERSON = _id(1000)
PERSON_SETTLED = _id(1001)
UNKNOWN_DRAFT = _id(9999)


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


def _literal(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        stamp = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return f"toDateTime64('{stamp}', 3, 'UTC')"
    if isinstance(value, tuple | list):
        return "(" + ", ".join(_literal(item) for item in value) + ")"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _render(sql: str, parameters: dict[str, Any]) -> str:
    """Inline clickhouse-driver's `%(name)s` placeholders for the CLI."""
    for name, value in parameters.items():
        sql = sql.replace(f"%({name})s", _literal(value))
    assert "%(" not in sql, sql
    return sql


def _schema_statements() -> list[str]:
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(APPLIED_PREFIXES):
                statements.append(statement)
    return statements


def _fixture_statements() -> list[str]:
    """One pending company, one settled company, and three ledger rows."""
    drafts = ", ".join(
        f"('{draft_id}', '{company}', '{source}', 'e', 'u-{draft_id}', "
        f"repeat('0', 64), repeat('0', 64), '{{}}', 2024, "
        "toDateTime64('2026-08-01 00:00:00', 3, 'UTC'), 'run', "
        "toDateTime64('2026-08-01 00:00:00', 3, 'UTC'))"
        for draft_id, company, source in (
            (DRAFT_CEO, PENDING_COMPANY, "bolagsverket"),
            (DRAFT_BOARD, PENDING_COMPANY, "esef"),
            (DRAFT_SETTLED, SETTLED_COMPANY, "bolagsverket"),
        )
    )
    people = ", ".join(
        f"('{person}', '{company}', 'David Mindus', NULL, [{draft_ids}], [], "
        "NULL, NULL, 'deterministic', 'copy', 'v1', 'run', "
        "toDateTime64('2026-08-01 00:00:00', 3, 'UTC'), "
        "toDateTime64('2026-08-01 00:00:00', 3, 'UTC'))"
        for person, company, draft_ids in (
            (PERSON, PENDING_COMPANY, f"'{DRAFT_CEO}', '{DRAFT_BOARD}'"),
            (PERSON_SETTLED, SETTLED_COMPANY, f"'{DRAFT_SETTLED}'"),
        )
    )
    role_drafts = ", ".join(
        f"('{_id(100 + index)}', '{draft_id}', '{PENDING_COMPANY}', '{source}', "
        f"'u-{draft_id}', repeat('0', 64), '{source_role}', '{source_role}', "
        f"'{role_code}', 2024, toDateTime64('2026-08-01 00:00:00', 3, 'UTC'), "
        "'run', toDateTime64('2026-08-01 00:00:00', 3, 'UTC'))"
        for index, (draft_id, source, source_role, role_code) in enumerate(
            (
                (DRAFT_CEO, "bolagsverket", "VD", "chief_executive_officer"),
                (DRAFT_BOARD, "esef", "board", "board_member"),
            )
        )
    )
    corrections = ", ".join(
        f"('{_id(200 + index)}', '{PENDING_COMPANY}', '{kind}', '{PERSON}', NULL, "
        f"[{draft_ids}], '{payload}', repeat('0', 64), 'reason', 'backoffice', "
        "NULL, toDateTime64('2026-08-02 00:00:00', 3, 'UTC'))"
        for index, (kind, draft_ids, payload) in enumerate(
            (
                ("override_field", "", '{\\"name\\":\\"David G. Mindus\\"}'),
                ("remove_role", f"'{DRAFT_BOARD}'", "{}"),
                ("set_role", f"'{UNKNOWN_DRAFT}'", '{\\"role_code\\":\\"board_chair\\"}'),
            )
        )
    )
    return [
        f"INSERT INTO corpscout.se_company_person_draft VALUES {drafts}",
        f"INSERT INTO corpscout.se_company_person VALUES {people}",
        f"INSERT INTO corpscout.se_company_person_role_draft VALUES {role_drafts}",
        f"INSERT INTO corpscout.se_company_person_correction VALUES {corrections}",
    ]


def _script(*, join_use_nulls: int) -> str:
    scope = {"all_companies": True, "company_ids": ("",)}
    role_parameters = {"source_run_id": "run", "updated_at": NOW}
    statements = [
        f"SET join_use_nulls = {join_use_nulls}",
        *_schema_statements(),
        *_fixture_statements(),
        f"CREATE TABLE {STAGE_TABLE} AS corpscout.se_company_person_role",
        "SELECT '@@statistics'",
        _render(build_company_statistics_sql(), scope),
        "SELECT '@@pending'",
        _render(
            build_pending_companies_sql(),
            {**scope, "after_company_id": "", "max_companies": 10},
        ),
        _render(
            build_role_assignments_insert_sql(STAGE_TABLE, [PENDING_COMPANY]),
            role_parameters,
        ),
        "SELECT '@@staged'",
        f"SELECT role_code, fiscal_year FROM {STAGE_TABLE} ORDER BY role_code",
        "SELECT '@@quality'",
        _role_assignment_quality_sql(STAGE_TABLE, [PENDING_COMPANY]),
        _render(
            _publish_role_assignments_sql(STAGE_TABLE, [PENDING_COMPANY]),
            role_parameters,
        ),
        "SELECT '@@published'",
        "SELECT role_code, is_current FROM corpscout.se_company_person_role FINAL "
        "ORDER BY role_code",
        "SELECT '@@stale'",
        build_stale_role_corrections_sql([PENDING_COMPANY]),
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


def test_company_status_queries_run_and_separate_pending_from_settled(
    sections: dict[str, list[list[str]]],
) -> None:
    """Pins C1: naming company_id in the outer scope must not fail to resolve."""
    assert sections["statistics"] == [["2", "1", "0", "1"]]

    assert len(sections["pending"]) == 1
    company_id, source_count, observation_count, draft_ids = sections["pending"][0]
    assert company_id == PENDING_COMPANY
    assert (source_count, observation_count) == ("2", "2")
    assert DRAFT_CEO in draft_ids and DRAFT_BOARD in draft_ids


def test_role_path_keeps_uncorrected_roles_and_applies_remove_role(
    sections: dict[str, list[list[str]]],
) -> None:
    """Pins I1: an uncorrected role survives the LEFT JOIN to the ledger."""
    assert sections["staged"] == [["chief_executive_officer", "2024"]]
    staged_count, _companies, _people, _roles, inserted, _updated, _skipped, deactivated = (
        sections["quality"][0]
    )
    assert (staged_count, inserted, deactivated) == ("1", "1", "0")
    assert sections["published"] == [["chief_executive_officer", "1"]]


def test_stale_role_corrections_are_separated_from_applied_ones(
    sections: dict[str, list[list[str]]],
) -> None:
    assert sections["stale"] == [["1", "1", "2"]]
