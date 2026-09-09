"""The person entity's normalize SQL against a real ClickHouse (spec 2026-09-09 section 4).

Claims a fake client cannot settle:
1. Migration 000395 applies (its six `se_company_person_*` tables, filtered out of the same
   file that also carries the serving view's stopped/re-pointed ALTER, which this fixture
   does not build) and the raw/normalized tables accept the row shapes `normalize.py` sends
   -- including the tombstone shape (`data` '{}', every person column NULL).
2. The `valid_data` constraint (`CHECK JSONType(data) = 'Object'`) actually bites: a row
   whose `data` is a JSON array is rejected with `Code: 469`, which is what the read's
   coercion in `normalize.py` exists to keep from ever happening on a normal page.
3. `changed_scope_sql()`/`changed_rows_sql()` select exactly the raw rows that have never
   been normalized, whose normalized row is on a different `suggestion_id`, or whose
   normalized row is on an older `normalizer_version` -- and `all_scope_sql()`/`all_rows_sql()`
   ignore all of that and return everything -- under both `join_use_nulls` settings, so the
   two-branch UNION ALL (chosen over a LEFT JOIN precisely to avoid that setting) really is
   setting-proof.
4. `normalized_insert_sql()` accepts the exact tuple `normalized_row()` builds, and a later,
   stale (older-version or mismatched-suggestion_id) normalized row for the same key really
   does make ReplacingMergeTree(FINAL) answer with that stale version -- which is what
   re-triggers the two conditions of the change scan.

Fixture: the RAW_BV/RAW_ESEF/RAW_TOMBSTONE raw rows of test_se_company_person_normalize.py,
imported rather than duplicated so the two files can't drift. A fourth row (same shape as the
tombstone: `data` '{}', `role_key` NULL, every person column NULL) is added here, on the
tombstone's own company but a second wikidata slot, purely to exercise the constraint and the
coercion on a row this file owns.
"""

import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.normalize import (
    RAW_ROW_COLUMNS,
    all_rows_sql,
    all_scope_sql,
    changed_rows_sql,
    changed_scope_sql,
    normalized_insert_sql,
    normalized_row,
)
from dagster_v3.defs.se_company.person.normalize_se import NORMALIZER_VERSION
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command
from tests.test_se_company_person_normalize import RAW_BV, RAW_ESEF, RAW_TOMBSTONE, STAMP

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION_FILE = "000395_corpscout_se_company_person_entity.up.sql"

COMPANY_1 = RAW_BV[0]  # "5561552760" -- also RAW_ESEF's company
COMPANY_2 = RAW_TOMBSTONE[0]  # "5560125220"
COMPANY_IDS = [COMPANY_1, COMPANY_2]

# Same tombstone shape as RAW_TOMBSTONE (every person column NULL, data '{}', role_key NULL),
# a second wikidata slot on RAW_TOMBSTONE's own company -- proves that shape lands, twice.
FOURTH_ROW = (
    COMPANY_2, "wikidata", "Q9:l2", "d" * 64, None, None, None,
    None, None, None, None, None, None, None, "{}",
)
# Same shape again, but `data` is a JSON array: the row the valid_data constraint must reject.
BAD_DATA_ROW = (
    COMPANY_1, "reviewer", "bad:1", "e" * 64, None, "Test", "Person",
    None, None, None, None, None, None, None, "[1,2]",
)

RAW_ROWS_WITH_DATES: tuple[tuple[tuple[Any, ...], datetime], ...] = (
    (RAW_BV, datetime(2026, 9, 1, tzinfo=UTC)),
    (RAW_ESEF, datetime(2026, 9, 2, tzinfo=UTC)),
    (RAW_TOMBSTONE, datetime(2026, 9, 3, tzinfo=UTC)),
)
FOURTH_ROW_WITH_DATE = (FOURTH_ROW, datetime(2026, 9, 4, tzinfo=UTC))

STAMP_OLD_VERSION = datetime(2026, 9, 9, 13, 0, 0, tzinfo=UTC)  # after STAMP
STAMP_DIFFERENT_SUGGESTION = datetime(2026, 9, 9, 14, 0, 0, tzinfo=UTC)  # after that
OLD_VERSION = "se-person-normalizer-v0"
DIFFERENT_SUGGESTION_ID = "f" * 64
NORMALIZER_VERSION_INDEX = tables.NORMALIZED_COLUMNS.index("normalizer_version")
SUGGESTION_ID_INDEX = tables.NORMALIZED_COLUMNS.index("suggestion_id")


def _schema_statements() -> list[str]:
    """`CREATE DATABASE` plus every `CREATE TABLE IF NOT EXISTS corpscout.se_company_person_`
    statement in 000395 -- never the `SYSTEM STOP/START VIEW` or `ALTER TABLE ... MODIFY
    QUERY` statements, which name `se_companies_serving`, a table this fixture does not
    build."""
    text = (MIGRATIONS_DIR / MIGRATION_FILE).read_text(encoding="utf-8")
    statements: list[str] = []
    for raw in text.split(";"):
        statement = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if statement.upper().startswith("CREATE DATABASE") or (
            "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_" in statement
        ):
            statements.append(statement)
    return statements


def test_the_migration_filters_to_the_database_and_exactly_six_create_tables() -> None:
    statements = _schema_statements()
    assert statements[0].upper().startswith("CREATE DATABASE")
    create_tables = [statement for statement in statements if statement.upper().startswith("CREATE TABLE")]
    assert len(create_tables) == 6


def _value_literal(value: Any) -> str:
    """One column value inside a VALUES row. Array(String) columns (parse_notes,
    first_tokens, middle_tokens, last_tokens) need `[...]`, not the `(...)` a `%(name)s`
    parameter binding uses -- see `_param_literal` below for that, different, case."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        stamp = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return f"toDateTime64('{stamp}', 3, 'UTC')"
    if isinstance(value, list):
        return "[" + ", ".join(_value_literal(item) for item in value) + "]"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _row_literal(row: Any) -> str:
    return "(" + ", ".join(_value_literal(item) for item in row) + ")"


def _param_literal(value: Any) -> str:
    """A `%(name)s` parameter value rendered the way clickhouse-driver renders it for an
    `IN` clause: a Python list or tuple as a parenthesized literal. `IN` accepts either
    form, so this is fine even though it differs from an Array column's `[...]`."""
    if value is None:
        return "NULL"
    if isinstance(value, datetime):
        stamp = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return f"toDateTime64('{stamp}', 3, 'UTC')"
    if isinstance(value, (list, tuple)):
        return "(" + ", ".join(_param_literal(item) for item in value) + ")"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _bind(sql: str, **params: object) -> str:
    """Render normalize.py's %(name)s parameters the way clickhouse-driver does."""
    rendered = sql
    for name, value in params.items():
        rendered = rendered.replace(f"%({name})s", _param_literal(value))
    assert "%(" not in rendered, rendered
    return rendered


def _ordered(sql: str, order_by: str) -> str:
    """Wrap a (possibly UNION ALL) statement so an ORDER BY unambiguously applies to it."""
    return f"SELECT * FROM ({sql}) AS ordered ORDER BY {order_by}"


def _raw_insert_all(entries: tuple[tuple[tuple[Any, ...], datetime], ...]) -> str:
    """RAW_ROW_COLUMNS-shaped rows -> one INSERT VALUES statement in SUGGESTION_COLUMNS
    order, with suggested_at from the caller and the columns RAW_ROW_COLUMNS does not carry
    filled in as the spec's tombstone/no-document defaults."""
    ordered_rows = []
    for raw_row, suggested_at in entries:
        values = dict(zip(RAW_ROW_COLUMNS, raw_row, strict=True))
        values.update(suggested_at=suggested_at, source_record_id="", document_ref=None)
        ordered_rows.append(tuple(values[column] for column in tables.SUGGESTION_COLUMNS))
    rows_sql = ", ".join(_row_literal(row) for row in ordered_rows)
    return (
        f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE} "
        f"({', '.join(tables.SUGGESTION_COLUMNS)}) VALUES {rows_sql}"
    )


def _normalized_insert(row: Any) -> str:
    return f"{normalized_insert_sql()} {_row_literal(row)}"


def _sections(lines: list[str]) -> dict[str, list[list[str]]]:
    """Split a flat line list on the `SELECT '@@name'` markers in the script."""
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in lines:
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        else:
            result[current].append(line.split("\t"))
    return result


def _statements() -> list[str]:
    esef_old_version = list(normalized_row(RAW_ESEF, STAMP_OLD_VERSION))
    esef_old_version[NORMALIZER_VERSION_INDEX] = OLD_VERSION

    esef_different_suggestion = list(normalized_row(RAW_ESEF, STAMP_DIFFERENT_SUGGESTION))
    esef_different_suggestion[SUGGESTION_ID_INDEX] = DIFFERENT_SUGGESTION_ID

    changed_rows = _ordered(
        _bind(changed_rows_sql(), company_ids=COMPANY_IDS, normalizer_version=NORMALIZER_VERSION),
        "company_id, source, slot",
    )
    changed_scope = _bind(changed_scope_sql(), normalizer_version=NORMALIZER_VERSION) + " ORDER BY company_id"

    return [
        *_schema_statements(),
        _raw_insert_all(RAW_ROWS_WITH_DATES),
        _raw_insert_all((FOURTH_ROW_WITH_DATE,)),
        # Before anything is normalized, all_scope/all_rows and changed_scope/changed_rows
        # agree: every raw row still needs normalizing.
        "SELECT '@@all_scope'",
        all_scope_sql() + " ORDER BY company_id",
        "SELECT '@@all_rows'",
        _ordered(_bind(all_rows_sql(), company_ids=COMPANY_IDS), "company_id, source, slot"),
        "SELECT '@@changed_scope_1'",
        changed_scope,
        "SELECT '@@changed_rows_1'",
        changed_rows,
        # Normalize the Bolagsverket raw row: it drops out of `changed`.
        _normalized_insert(normalized_row(RAW_BV, STAMP)),
        "SELECT '@@changed_rows_2'",
        changed_rows,
        # Normalize the ESEF row too, but on an older normalizer_version -- the version
        # branch of the UNION re-selects it.
        _normalized_insert(tuple(esef_old_version)),
        "SELECT '@@changed_rows_3'",
        changed_rows,
        # Re-normalize ESEF on the current version but with a suggestion_id that does not
        # match the raw row's own -- the id branch re-selects it this time.
        _normalized_insert(tuple(esef_different_suggestion)),
        "SELECT '@@changed_rows_4'",
        changed_rows,
        "SELECT '@@changed_scope_2'",
        changed_scope,
        "SELECT '@@final_count'",
        f"SELECT count() FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL",
        "SELECT '@@bv_row'",
        f"SELECT data, role_key FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL WHERE source = 'bolagsverket'",
    ]


@pytest.fixture(
    scope="module",
    params=(0, 1),
    ids=("join_use_nulls_off", "join_use_nulls_on"),
)
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    """Runs the whole script once per join_use_nulls setting (one clickhouse-local
    invocation each), shared across the test functions below rather than re-run per test."""
    script = f"SET join_use_nulls = {request.param};\n" + ";\n".join(_statements()) + ";\n"
    try:
        completed = subprocess.run(
            _clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return _sections([line for line in completed.stdout.splitlines() if line.strip()])


def test_changed_and_all_selection_agree_before_anything_is_normalized(
    sections: dict[str, list[list[str]]],
) -> None:
    both_companies = [[COMPANY_2], [COMPANY_1]]
    assert sections["all_scope"] == both_companies
    assert sections["changed_scope_1"] == both_companies
    four_rows = [
        [COMPANY_2, "wikidata", "Q9:l1"],
        [COMPANY_2, "wikidata", "Q9:l2"],
        [COMPANY_1, "bolagsverket", "rec1:sig1"],
        [COMPANY_1, "esef", "doc7:2"],
    ]
    assert [row[:3] for row in sections["all_rows"]] == four_rows
    assert [row[:3] for row in sections["changed_rows_1"]] == four_rows


def test_the_tombstone_shape_survives_the_read_coercion_unchanged(
    sections: dict[str, list[list[str]]],
) -> None:
    """The point of the constraint test below: a row that is genuinely `{}` already needs no
    coercion and comes back exactly as stored, for both the original tombstone and the
    fourth row this file adds on the same company."""
    by_key = {(row[0], row[1], row[2]): row for row in sections["changed_rows_1"]}
    assert by_key[(COMPANY_2, "wikidata", "Q9:l1")][-1] == "{}"
    assert by_key[(COMPANY_2, "wikidata", "Q9:l2")][-1] == "{}"


def test_a_normalized_row_drops_out_of_changed_until_its_version_or_id_goes_stale(
    sections: dict[str, list[list[str]]],
) -> None:
    remaining = [
        [COMPANY_2, "wikidata", "Q9:l1"],
        [COMPANY_2, "wikidata", "Q9:l2"],
        [COMPANY_1, "esef", "doc7:2"],
    ]
    # Bolagsverket is normalized on the current version with its own suggestion_id: neither
    # "never normalized" nor "id changed" nor "older version" holds, so it drops out of
    # `changed`; the two wikidata rows (never normalized) and esef (not yet) remain.
    assert [row[:3] for row in sections["changed_rows_2"]] == remaining
    # ESEF is now normalized too, but on OLD_VERSION: the version branch of the UNION
    # (not the anti-join branch) re-selects it, so the same three rows come back.
    assert [row[:3] for row in sections["changed_rows_3"]] == remaining
    # ESEF is re-normalized on the CURRENT version but with a suggestion_id that does not
    # match the raw row: the id branch re-selects it this time, same three rows again.
    assert [row[:3] for row in sections["changed_rows_4"]] == remaining
    assert sections["changed_scope_2"] == [[COMPANY_2], [COMPANY_1]]


def test_normalized_final_holds_one_row_per_normalized_key(
    sections: dict[str, list[list[str]]],
) -> None:
    # Bolagsverket (one insert) and ESEF (three inserts, one key -- ReplacingMergeTree(FINAL)
    # keeps the newest): two distinct (company_id, source, slot) keys. Neither wikidata row
    # was ever normalized in this script.
    assert sections["final_count"] == [["2"]]


def test_the_bolagsverket_normalized_row_keeps_its_extras_and_role_key(
    sections: dict[str, list[list[str]]],
) -> None:
    [[data, role_key]] = sections["bv_row"]
    assert data == '{"signatory_kind":"board"}'
    assert role_key == "board_member"


def test_the_valid_data_constraint_rejects_a_non_object() -> None:
    """The tombstone shape (`data` '{}', `role_key` NULL) passes -- proven by every row in
    `sections` above landing successfully, the fourth row included. A JSON array instead of
    an object is exactly what CONSTRAINT valid_data exists to catch, which is why
    normalize.py's read coerces anything that is not an object to '{}' before it ever
    reaches this insert."""
    script = ";\n".join(
        [*_schema_statements(), _raw_insert_all(((BAD_DATA_ROW, datetime(2026, 9, 5, tzinfo=UTC)),))]
    ) + ";\n"
    try:
        completed = subprocess.run(
            _clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode != 0
    assert "Code: 469" in (completed.stderr + completed.stdout)
