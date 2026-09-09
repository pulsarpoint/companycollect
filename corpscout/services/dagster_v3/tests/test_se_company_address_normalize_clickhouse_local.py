"""The address entity's normalize SQL against a real ClickHouse (spec 2026-09-06 section 4).

Claims a fake client cannot settle:
1. The six real migrations apply and the raw/normalized tables accept the row shapes
   `normalize.py` sends.
2. `changed_scope_sql()`/`changed_rows_sql()` select exactly the raw rows that have never
   been normalized, whose raw version is newer than their normalized row, or whose normalized
   row is on an older `normalizer_version` -- and `all_scope_sql()`/`all_rows_sql()` ignore all
   of that and return everything -- under both `join_use_nulls` settings, so the two-branch
   UNION ALL (chosen over a LEFT JOIN precisely to avoid that setting) really is setting-proof.
3. `normalized_insert_sql()` accepts the exact tuple `normalized_row()` builds, and a later,
   older-version normalized row for the same key really does make ReplacingMergeTree(FINAL)
   answer with that older version -- which is what re-triggers the version branch.

Fixture: the RAW_SCB/RAW_BV/RAW_EMPTY raw rows of test_se_company_address_normalize.py,
imported rather than duplicated so the two files can't drift.
"""

import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.normalize import (
    RAW_ROW_COLUMNS,
    all_rows_sql,
    all_scope_sql,
    changed_rows_sql,
    changed_scope_sql,
    normalized_insert_sql,
    normalized_row,
)
from dagster_v3.defs.se_company.address.normalize_se import NORMALIZER_VERSION
from tests.clickhouse_local import clickhouse_local_command
from tests.test_se_company_address_normalize import RAW_BV, RAW_EMPTY, RAW_SCB, STAMP

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATIONS = (
    "000382_corpscout_se_company_address_suggestion.up.sql",
    "000383_corpscout_se_company_address_normalized.up.sql",
    "000384_corpscout_se_company_address_v2.up.sql",
    "000385_corpscout_se_company_address_history.up.sql",
    "000386_corpscout_se_company_address_rule.up.sql",
    "000387_corpscout_se_company_address_precedence.up.sql",
)

SCB_BV_COMPANY = RAW_SCB[0]
RATSIT_COMPANY = RAW_EMPTY[0]
COMPANY_IDS = [SCB_BV_COMPANY, RATSIT_COMPANY]
STAMP_OLDER_VERSION = datetime(2026, 9, 6, 13, 0, 0, tzinfo=UTC)  # after STAMP
OLD_VERSION = "se-address-normalizer-v0"
NORMALIZER_VERSION_INDEX = tables.NORMALIZED_COLUMNS.index("normalizer_version")


def _schema_statements() -> list[str]:
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(("CREATE DATABASE", "CREATE TABLE")):
                statements.append(statement)
    return statements


def _run(statements: list[str], *, join_use_nulls: int) -> list[str]:
    script = f"SET join_use_nulls = {join_use_nulls};\n" + ";\n".join(statements) + ";\n"
    completed = subprocess.run(
        clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _literal(value: Any) -> str:
    """A parameter value rendered as ClickHouse SQL text. clickhouse-driver itself renders a
    Python `list` as an array literal (`['a', 'b']`) and a `tuple` as a parenthesized literal
    (`('a', 'b')`); this harness renders both the same way (parenthesized), which is fine
    because `IN` accepts either form -- the difference does not change what a statement
    matches."""
    if value is None:
        return "NULL"
    if isinstance(value, datetime):
        stamp = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return f"toDateTime64('{stamp}', 3, 'UTC')"
    if isinstance(value, (list, tuple)):
        return "(" + ", ".join(_literal(item) for item in value) + ")"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _bind(sql: str, **params: object) -> str:
    """Render normalize.py's %(name)s parameters the way clickhouse-driver does."""
    rendered = sql
    for name, value in params.items():
        rendered = rendered.replace(f"%({name})s", _literal(value))
    assert "%(" not in rendered, rendered
    return rendered


def _ordered(sql: str, order_by: str) -> str:
    """Wrap a (possibly UNION ALL) statement so an ORDER BY unambiguously applies to it."""
    return f"SELECT * FROM ({sql}) AS ordered ORDER BY {order_by}"


def _raw_insert(row: tuple[Any, ...]) -> str:
    """One RAW_ROW_COLUMNS-shaped row -> an INSERT VALUES clause in SUGGESTION_COLUMNS order."""
    values = dict(zip(RAW_ROW_COLUMNS, row, strict=True))
    values.update(
        source_record_uid="",
        observed_at=values["suggested_at"],
        decided_by=None,
        note=None,
        replaces_key=None,
        source_run_id="test",
        extractor_version="test-v1",
    )
    ordered = tuple(values[column] for column in tables.SUGGESTION_COLUMNS)
    return (
        f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE} "
        f"({', '.join(tables.SUGGESTION_COLUMNS)}) VALUES {_literal(ordered)}"
    )


def _normalized_insert(row: tuple[Any, ...]) -> str:
    return f"{normalized_insert_sql()} {_literal(tuple(row))}"


def _sections(lines: list[str]) -> dict[str, list[list[str]]]:
    """Split _run()'s flat line list on the `SELECT '@@name'` markers in the script."""
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
    bv_old_version = list(normalized_row(RAW_BV, STAMP_OLDER_VERSION))
    bv_old_version[NORMALIZER_VERSION_INDEX] = OLD_VERSION

    changed_rows = _ordered(
        _bind(changed_rows_sql(), company_ids=COMPANY_IDS, normalizer_version=NORMALIZER_VERSION),
        "company_id, source, slot",
    )
    changed_scope = _bind(changed_scope_sql(), normalizer_version=NORMALIZER_VERSION) + " ORDER BY company_id"

    return [
        *_schema_statements(),
        _raw_insert(RAW_SCB),
        _raw_insert(RAW_BV),
        _raw_insert(RAW_EMPTY),
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
        # Normalize the SCB raw row on the current version: it drops out of `changed`.
        _normalized_insert(normalized_row(RAW_SCB, STAMP)),
        "SELECT '@@changed_rows_2'",
        changed_rows,
        # Normalize the Bolagsverket raw row too, but on an older normalizer_version and a
        # later normalized_at -- ReplacingMergeTree(FINAL) picks this newer, stale-version row.
        _normalized_insert(tuple(bv_old_version)),
        "SELECT '@@changed_rows_3'",
        changed_rows,
        "SELECT '@@changed_scope_2'",
        changed_scope,
        "SELECT '@@final_count'",
        f"SELECT count() FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL",
    ]


@pytest.fixture(
    scope="module",
    params=(0, 1),
    ids=("join_use_nulls_off", "join_use_nulls_on"),
)
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    """Runs the whole script once per join_use_nulls setting (one clickhouse-local
    invocation each), shared across the test functions below rather than re-run per test."""
    return _sections(_run(_statements(), join_use_nulls=request.param))


def test_changed_and_all_selection_agree_before_anything_is_normalized(
    sections: dict[str, list[list[str]]],
) -> None:
    both_companies = [[RATSIT_COMPANY], [SCB_BV_COMPANY]]
    assert sections["all_scope"] == both_companies
    assert sections["changed_scope_1"] == both_companies
    three_rows = [
        [RATSIT_COMPANY, "ratsit", "company"],
        [SCB_BV_COMPANY, "bolagsverket", ""],
        [SCB_BV_COMPANY, "scb", ""],
    ]
    assert [row[:3] for row in sections["all_rows"]] == three_rows
    assert [row[:3] for row in sections["changed_rows_1"]] == three_rows


def test_a_normalized_row_drops_out_of_changed_until_its_version_goes_stale(
    sections: dict[str, list[list[str]]],
) -> None:
    remaining = [[RATSIT_COMPANY, "ratsit", "company"], [SCB_BV_COMPANY, "bolagsverket", ""]]
    # The SCB row was normalized on the current version with its own suggested_at: neither
    # "never normalized" nor "raw newer" nor "older version" holds, so it is gone from
    # `changed`; ratsit (never normalized) and bolagsverket (not yet normalized) remain.
    assert [row[:3] for row in sections["changed_rows_2"]] == remaining
    # The Bolagsverket row is now normalized too, but on OLD_VERSION: the version branch of
    # the UNION (not the anti-join branch) re-selects it, so the same two rows come back.
    assert [row[:3] for row in sections["changed_rows_3"]] == remaining
    assert sections["changed_scope_2"] == [[RATSIT_COMPANY], [SCB_BV_COMPANY]]


def test_normalized_final_holds_one_row_per_normalized_key(
    sections: dict[str, list[list[str]]],
) -> None:
    # Two normalized_insert_sql() calls, two distinct (company_id, source, slot) keys (scb and
    # bolagsverket); ratsit was never normalized in this script.
    assert sections["final_count"] == [["2"]]
