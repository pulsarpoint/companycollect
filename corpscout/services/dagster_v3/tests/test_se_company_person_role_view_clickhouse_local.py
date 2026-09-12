"""The roles-as-rows SELECT against a real ClickHouse (clickhouse-local).

Two claims a text pin cannot settle (spec 2026-09-09 section 11):

1. THE DDL IS LEGAL. The view declares its engine inline and sorts on
   (company_id, person_key, role_year, role_code, source, slot), while role_code and
   role_year are Nullable on the normalized row and allow_nullable_key is off. This file
   builds the view's inner table the way ClickHouse builds it -- CREATE TABLE ... ENGINE =
   MergeTree ORDER BY (...) AS <the SELECT> -- so a Nullable column reaching the sort key
   fails HERE, not on the prod CREATE, and the column types are read back out of
   system.columns.
2. THE SELECT SAYS WHAT IT MEANS. One row per published ACTIVE person and per
   role-carrying observation the fold built them from: a roleless observation contributes
   nothing, an observation no published person was folded from contributes nothing, an
   inactive person contributes nothing, a dateless role lands under year 0, and
   is_current is 1 exactly for the codes on the person's current_roles.

Both join_use_nulls settings run, and here that is not a formality: this SELECT makes a
real INNER JOIN, which is the statement the setting changes.

The tables come from migration 000396's own DDL, with 000398's rename replayed over the
main table (the ledger policy leaves 000396 declaring se_company_person_v2), exactly as
tests/test_se_company_person_fold_clickhouse_local.py does it.
"""

import json
import subprocess

import pytest

from dagster_v3.defs.se_company.person import tables
from tests.clickhouse_local import clickhouse_local_command
from tests.se_company_ddl import table_block

pytestmark = pytest.mark.integration

COMPANY = "5560000001"
ANNA = "a" * 64          # active, four members, three of them with a role
CARL = "b" * 64          # active, one dateless Wikidata role
ERIK = "c" * 64          # INACTIVE (hidden): contributes no row at all
N_BOARD = "1" * 64       # Anna, bolagsverket, board_member 2025 -- a current role
N_CHAIR = "2" * 64       # Anna, esef, board_chair 2025 -- a current role
N_AUDIT = "3" * 64       # Anna, bolagsverket, auditor 2019 -- held once, not current
N_NOROLE = "4" * 64      # Anna, bolagsverket, role_code NULL -- 2.0M such rows on prod
N_CEO = "5" * 64         # Carl, wikidata, no year at all, an open span from 2019-05-01
N_HIDDEN = "6" * 64      # Erik's only observation
N_ORPHAN = "7" * 64      # a normalized row no published person was folded from
FOLDED_AT = "2026-09-11 09:00:00.000"

MAIN_INSERT_COLUMNS = (
    "company_id, person_key, display_name, first_name, last_name, birth_year, "
    "sources, slots, normalized_ids, current_roles, active, inactive_reason, "
    "text_source, folded_at, fold_version, source_run_id"
)
NORMALIZED_INSERT_COLUMNS = (
    "company_id, source, slot, suggestion_id, normalized_id, normalizer_version, "
    "parse_status, display_first, display_last, display_name, birth_year, role_code, "
    "role_key, role_year, role_from, role_to, normalized_at"
)


def _main_row(
    person_key: str,
    display_name: str,
    birth_year: str,
    slots: str,
    normalized_ids: str,
    current_roles: str,
    active: int,
    inactive_reason: str,
) -> str:
    return (
        f"('{COMPANY}', '{person_key}', '{display_name}', 'Anna', 'Svensson', {birth_year}, "
        f"['bolagsverket'], {slots}, {normalized_ids}, {current_roles}, {active}, "
        f"'{inactive_reason}', 'bolagsverket', toDateTime64('{FOLDED_AT}', 3, 'UTC'), "
        "'se-person-fold-v1', 'run-fold')"
    )


def _normalized_row(
    source: str,
    slot: str,
    normalized_id: str,
    display_name: str,
    role_code: str,
    role_year: str,
    role_from: str = "NULL",
    role_to: str = "NULL",
) -> str:
    return (
        f"('{COMPANY}', '{source}', '{slot}', '{normalized_id}', '{normalized_id}', "
        f"'se-person-normalizer-v1', 'ok', 'Anna', 'Svensson', '{display_name}', NULL, "
        f"{role_code}, NULL, {role_year}, {role_from}, {role_to}, "
        "toDateTime64('2026-09-11 08:00:00.000', 3, 'UTC'))"
    )


def _script(join_use_nulls: int) -> str:
    parts = [
        f"SET join_use_nulls = {join_use_nulls};",
        "CREATE DATABASE IF NOT EXISTS corpscout;",
        # 000396 declares the main table under its build name; 000398 renames the DEPLOYED
        # table and never edits that file, so the local schema replays the rename.
        table_block("se_company_person_v2").replace(
            "corpscout.se_company_person_v2", tables.QUALIFIED_MAIN_TABLE
        ),
        table_block("se_company_person_normalized"),
        f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} ({MAIN_INSERT_COLUMNS}) VALUES\n"
        + ",\n".join(
            (
                _main_row(
                    ANNA, "Anna Svensson", "1975",
                    "['uid-1:sig-1', 'doc-9:cand-1', 'uid-1:sig-9', 'uid-1:sig-7']",
                    f"['{N_BOARD}', '{N_CHAIR}', '{N_AUDIT}', '{N_NOROLE}']",
                    "['board_chair', 'board_member']", 1, "",
                ),
                _main_row(
                    CARL, "Carl von Essen", "NULL", "['Q1:P169:Q7']",
                    f"['{N_CEO}']", "['chief_executive_officer']", 1, "",
                ),
                _main_row(
                    ERIK, "Erik Larsson", "NULL", "['uid-2:sig-1']",
                    f"['{N_HIDDEN}']", "['auditor']", 0, "hidden",
                ),
            )
        )
        + ";",
        f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} ({NORMALIZED_INSERT_COLUMNS}) VALUES\n"
        + ",\n".join(
            (
                _normalized_row("bolagsverket", "uid-1:sig-1", N_BOARD, "Anna Svensson", "'board_member'", "2025"),
                _normalized_row("esef", "doc-9:cand-1", N_CHAIR, "Anna Maria Svensson", "'board_chair'", "2025"),
                _normalized_row("bolagsverket", "uid-1:sig-9", N_AUDIT, "Anna Svensson", "'auditor'", "2019"),
                _normalized_row("bolagsverket", "uid-1:sig-7", N_NOROLE, "Anna Svensson", "NULL", "2024"),
                _normalized_row(
                    "wikidata", "Q1:P169:Q7", N_CEO, "Carl von Essen",
                    "'chief_executive_officer'", "NULL", "toDate('2019-05-01')",
                ),
                _normalized_row("bolagsverket", "uid-2:sig-1", N_HIDDEN, "Erik Larsson", "'auditor'", "2022"),
                _normalized_row("bolagsverket", "uid-9:sig-9", N_ORPHAN, "Orphan Row", "'board_member'", "2025"),
            )
        )
        + ";",
        # The view's inner table, built exactly as an engine-in-view refreshable MV builds
        # it. This is the statement that fails if a Nullable column reaches the sort key.
        f"CREATE TABLE {tables.QUALIFIED_ROLE_VIEW}\nENGINE = MergeTree\n"
        f"ORDER BY ({', '.join(tables.ROLE_VIEW_ORDER_BY)})\n"
        f"AS {tables.build_se_company_person_role_sql()};",
        # The ORDER BY has to sit in a subquery: `position` is not an aggregate and
        # ClickHouse refuses it beside groupArray (code 215, NOT_AN_AGGREGATE).
        "SELECT groupArray(concat(name, ' ', type)) AS columns FROM (SELECT name, type "
        f"FROM system.columns WHERE database = 'corpscout' AND table = '{tables.ROLE_VIEW}' "
        "ORDER BY position) FORMAT JSONEachRow;",
        f"SELECT * FROM {tables.QUALIFIED_ROLE_VIEW} "
        "ORDER BY person_key, role_year, role_code FORMAT JSONEachRow;",
    ]
    return "\n".join(parts) + "\n"


@pytest.fixture(
    scope="module",
    params=(0, 1),
    ids=("join_use_nulls_off", "join_use_nulls_on"),
)
def view(request: pytest.FixtureRequest) -> tuple[list[str], list[dict]]:
    command = clickhouse_local_command()
    try:
        completed = subprocess.run(
            command,
            input=_script(request.param),
            capture_output=True,
            text=True,
            timeout=900,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    columns: list[str] = []
    rows: list[dict] = []
    for line in completed.stdout.splitlines():
        if not line.strip():
            continue
        parsed = json.loads(line)
        if "columns" in parsed:
            columns = parsed["columns"]
        else:
            rows.append(parsed)
    assert columns, completed.stdout
    return columns, rows


def test_the_sort_key_columns_are_not_nullable_and_the_rest_are_as_declared(
    view: tuple[list[str], list[dict]],
) -> None:
    columns, _ = view
    types = dict(column.split(" ", 1) for column in columns)

    assert list(types) == list(tables.ROLE_VIEW_COLUMNS)
    for key_column in tables.ROLE_VIEW_ORDER_BY:
        assert not types[key_column].startswith("Nullable"), key_column
    # The two the SELECT unwraps, exactly: a String role code and a UInt16 year.
    assert types["role_code"] == "String"
    assert types["role_year"] == "UInt16"
    # Outside the key, Nullable survives on purpose -- a missing birth year and an open
    # span must read as NULL, never as a zero or an empty date.
    assert types["birth_year"] == "Nullable(UInt16)"
    assert types["role_from"] == "Nullable(Date)"
    assert types["role_to"] == "Nullable(Date)"
    assert types["is_current"] == "UInt8"


def test_one_row_per_active_person_and_role_carrying_observation(
    view: tuple[list[str], list[dict]],
) -> None:
    _, rows = view

    assert [
        (row["person_key"], row["role_code"], row["role_year"], row["source"], row["is_current"])
        for row in rows
    ] == [
        (ANNA, "auditor", 2019, "bolagsverket", 0),
        (ANNA, "board_chair", 2025, "esef", 1),
        (ANNA, "board_member", 2025, "bolagsverket", 1),
        # A role with no year at all lands under 0 -- the view's way of saying "dateless",
        # where the person row's role_years array says "held now" instead.
        (CARL, "chief_executive_officer", 0, "wikidata", 1),
    ]
    anna = rows[0]
    assert anna["company_id"] == COMPANY
    assert anna["display_name"] == "Anna Svensson"
    assert anna["birth_year"] == 1975
    assert anna["normalized_id"] == N_AUDIT
    assert anna["slot"] == "uid-1:sig-9"
    assert anna["folded_at"].startswith("2026-09-11 09:00:00")
    carl = rows[3]
    assert carl["birth_year"] is None
    assert carl["role_from"] == "2019-05-01"
    assert carl["role_to"] is None


def test_roleless_unfolded_and_inactive_observations_contribute_nothing(
    view: tuple[list[str], list[dict]],
) -> None:
    _, rows = view
    ids = {row["normalized_id"] for row in rows}

    assert N_NOROLE not in ids      # role_code IS NULL -- the WHERE drops it
    assert N_ORPHAN not in ids      # no person names it in normalized_ids -- the JOIN drops it
    assert N_HIDDEN not in ids      # its person is inactive -- the WHERE drops it
    assert all(row["person_key"] != ERIK for row in rows)
