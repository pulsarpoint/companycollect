"""Migration 000406 on a real ClickHouse (spec 2026-09-13 sections 4.3 and 5).

Five claims a fake client cannot settle:

1. The pair table's combined `ADD COLUMN ... , MODIFY ORDER BY ...` applies to a POPULATED
   table: it is accepted, `system.tables.sorting_key` gains request_id, every stored row
   survives, and each one reads back with request_id = '' (String's zero value -- the column
   carries no DEFAULT clause, because a key column may not).
2. After it, a v2 answer for the SAME candidate pair under its own request_id is a SECOND
   row under FINAL rather than a replacement. That is the whole reason the key grew.
3. The gap view's SELECT -- the builder's render, run as a plain SELECT -- finds exactly the
   companies spec section 5.1 defines: a call-name pair no stored match closed, and a
   double-surname pair, and a call-name pair with birth year on only one side (production shape).
4. And it excludes the four cases that must never appear: a call-name pair already matched at
   or above 0.8, a pair whose persons share a source, a pair whose birth years conflict, and
   a company with a single machine source. A reviewer member is not a member at all.
5. The two new tables accept a row built from `tables.LLM_QUEUE_COLUMNS` and
   `tables.LLM_RESPONSE_COLUMNS` -- every type, the company-id constraint and both defaults --
   and read back under FINAL as one row per (request, company). The defaults are verified:
   a queue INSERT without `note` and a response INSERT without `error` both land with '',
   and system.columns declares both with default_kind = 'DEFAULT' and default_expression = '\'\''.

Both `join_use_nulls` settings run: the SELECT joins six times, so the parametrization is a
live risk here rather than a guard.
"""

import subprocess
from pathlib import Path
from typing import Any

import pytest

from dagster_v3.defs.se_company.person import tables
from tests.clickhouse_local import clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
ENTITY_MIGRATION = "000396_corpscout_se_company_person_entity.up.sql"
MATCH_MIGRATION = "000399_corpscout_se_company_person_match.up.sql"
ENHANCE_MIGRATION = "000406_corpscout_se_company_person_llm_enhance.up.sql"

STAMP = "toDateTime64('2026-09-13 00:00:00', 3, 'UTC')"
MATCHED_AT = "toDateTime64('2026-09-13 01:00:00', 3, 'UTC')"
V2_AT = "toDateTime64('2026-09-13 02:00:00', 3, 'UTC')"

GAP_CALL_NAME = "5560000001"      # erik bo bengtsson (ratsit) vs bo bengtsson (bolagsverket)
ALREADY_MATCHED = "5560000002"    # the same shape, with a stored pair at 0.9
GAP_DOUBLE = "5560000003"         # anna ek svensson (ratsit) vs anna svensson (bolagsverket)
SHARED_SOURCE = "5560000004"      # the persons' source sets overlap on ratsit
BIRTH_CONFLICT = "5560000005"     # 1970 against 1980
SINGLE_SOURCE = "5560000006"      # both persons come from bolagsverket alone
PAIR_ONLY = "5560000007"          # a pair row and no persons: the alter's second company
BIRTH_YEAR_ONLY_RATSIT = "5560000008"  # erik bo lindqvist (ratsit, 1966) vs bo lindqvist (bolagsverket, NULL)


def _id(tag: str) -> str:
    """A FixedString(64) whose prefix says what it is."""
    return tag.ljust(64, "0")


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return repr(value)
    if isinstance(value, list):
        return "[" + ", ".join(_literal(item) for item in value) + "]"
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


# (company_id, person_key, birth_year, sources, [(normalized_id, source, first, middle, last)])
PEOPLE = (
    (GAP_CALL_NAME, "p1a", None, ["ratsit"],
     [("n1a", "ratsit", ["erik"], ["bo"], ["bengtsson"])]),
    (GAP_CALL_NAME, "p1b", None, ["bolagsverket"],
     [("n1b", "bolagsverket", ["bo"], [], ["bengtsson"])]),
    # A reviewer-only person shaped to form a SECOND call-name pair with p1a if the member
    # leg ever stopped filtering on the machine sources. It must contribute nothing.
    (GAP_CALL_NAME, "p1r", None, ["reviewer"],
     [("n1r", "reviewer", ["bo"], [], ["bengtsson"])]),
    (ALREADY_MATCHED, "p2a", None, ["ratsit"],
     [("n2a", "ratsit", ["erik"], ["bo"], ["bengtsson"])]),
    (ALREADY_MATCHED, "p2b", None, ["bolagsverket"],
     [("n2b", "bolagsverket", ["bo"], [], ["bengtsson"])]),
    (GAP_DOUBLE, "p3a", None, ["ratsit"],
     [("n3a", "ratsit", ["anna"], [], ["ek", "svensson"])]),
    (GAP_DOUBLE, "p3b", None, ["bolagsverket"],
     [("n3b", "bolagsverket", ["anna"], [], ["svensson"])]),
    (SHARED_SOURCE, "p4a", None, ["ratsit", "esef"],
     [("n4a", "ratsit", ["erik"], ["bo"], ["bengtsson"]),
      ("n4c", "esef", ["erik"], ["bo"], ["bengtsson"])]),
    (SHARED_SOURCE, "p4b", None, ["ratsit"],
     [("n4b", "ratsit", ["bo"], [], ["bengtsson"])]),
    (BIRTH_CONFLICT, "p5a", 1970, ["ratsit"],
     [("n5a", "ratsit", ["erik"], ["bo"], ["bengtsson"])]),
    (BIRTH_CONFLICT, "p5b", 1980, ["bolagsverket"],
     [("n5b", "bolagsverket", ["bo"], [], ["bengtsson"])]),
    (SINGLE_SOURCE, "p6a", None, ["bolagsverket"],
     [("n6a", "bolagsverket", ["erik"], ["bo"], ["bengtsson"])]),
    (SINGLE_SOURCE, "p6b", None, ["bolagsverket"],
     [("n6b", "bolagsverket", ["bo"], [], ["bengtsson"])]),
    (BIRTH_YEAR_ONLY_RATSIT, "p8a", 1966, ["ratsit"],
     [("n8a", "ratsit", ["erik"], ["bo"], ["lindqvist"])]),
    (BIRTH_YEAR_ONLY_RATSIT, "p8b", None, ["bolagsverket"],
     [("n8b", "bolagsverket", ["bo"], [], ["lindqvist"])]),
)


def _schema() -> list[str]:
    """000396's two tables the view reads, 000399's two match tables, and 000406's two
    ALTERs -- never 000396's serving-view statements, and never 000406's own new tables or
    its refreshable view (the SELECT is run as a plain SELECT, which is what the migration
    installs as the view's body)."""
    statements: list[str] = []
    wanted_creates = (
        "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_normalized\n",
        "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_v2\n",
        "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_match\n",
        "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_match_state\n",
    )
    for name in (ENTITY_MIGRATION, MATCH_MIGRATION):
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith("CREATE DATABASE"):
                if statement not in statements:
                    statements.append(statement)
            elif statement.startswith(wanted_creates):
                # 000398 renames the deployed table; the builder reads the renamed name.
                statements.append(
                    statement.replace(
                        "corpscout.se_company_person_v2", tables.QUALIFIED_MAIN_TABLE
                    )
                )
    assert len(statements) == 5, statements
    return statements


def _alters() -> list[str]:
    """000406's two ALTER statements, exactly as the migration writes them."""
    text = (MIGRATIONS_DIR / ENHANCE_MIGRATION).read_text(encoding="utf-8")
    alters = []
    for raw in text.split(";"):
        statement = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if statement.startswith((
            f"ALTER TABLE {tables.QUALIFIED_MATCH_TABLE}\n",
            f"ALTER TABLE {tables.QUALIFIED_MATCH_STATE_TABLE}\n",
        )):
            alters.append(statement)
    assert len(alters) == 2, alters
    return alters


def _pattern_tables() -> list[str]:
    """000406's two CREATE TABLEs -- the pattern's queue and response pair."""
    text = (MIGRATIONS_DIR / ENHANCE_MIGRATION).read_text(encoding="utf-8")
    creates = []
    for raw in text.split(";"):
        statement = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if statement.startswith((
            f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_LLM_QUEUE_TABLE}\n",
            f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_LLM_RESPONSE_TABLE}\n",
        )):
            creates.append(statement)
    assert len(creates) == 2, creates
    return creates


def _queue_insert(*, request_id_suffix: str = "", omit_note: bool = False) -> str:
    """One queue row in tables.LLM_QUEUE_COLUMNS order.

    If omit_note=True, the `note` column is omitted from the INSERT, proving that
    the DEFAULT clause works (the column will read back as ''). The request_id_suffix
    allows distinct rows when multiple inserts are needed for the same company.
    """
    request_id = "'0123456789abcdef0123456789abcdef" + request_id_suffix + "'"
    values = {
        "request_id": request_id,
        "company_id": f"'{GAP_CALL_NAME}'",
        "queued_at": STAMP,
        "queued_by": "'backoffice'",
        "note": "'se-person-match-v2 gap'",
    }
    columns = [col for col in tables.LLM_QUEUE_COLUMNS if not (omit_note and col == "note")]
    return (
        f"INSERT INTO {tables.QUALIFIED_LLM_QUEUE_TABLE} "
        f"({', '.join(columns)}) VALUES "
        f"({', '.join(values[column] for column in columns)})"
    )


def _response_insert(*, request_id_suffix: str = "", omit_error: bool = False) -> str:
    """One response row in tables.LLM_RESPONSE_COLUMNS order.

    If omit_error=True, the `error` column is omitted from the INSERT, proving that
    the DEFAULT clause works (the column will read back as ''). The request_id_suffix
    allows distinct rows when multiple inserts are needed for the same company.
    """
    request_id = "'0123456789abcdef0123456789abcdef" + request_id_suffix + "'"
    values = {
        "request_id": request_id,
        "company_id": f"'{GAP_CALL_NAME}'",
        "provider": "'deepseek'",
        "model": "'deepseek-v4-flash'",
        "prompt_version": "'se-person-match-v2'",
        "input_hash": f"'{_id('hash1')}'",
        "candidates": "2",
        "prompt_tokens": "1128",
        "completion_tokens": "64",
        # raw_response is a plain String with no constraint -- the model's exact text. The
        # fixture keeps it simple on purpose: what is proved here is the column list and the
        # types, not the parser (that is slice 2's).
        "raw_response": "'no pairs'",
        "error": "''",
        "attempts": "1",
        "source_run_id": "'run-enhance-1'",
        "responded_at": V2_AT,
    }
    columns = [col for col in tables.LLM_RESPONSE_COLUMNS if not (omit_error and col == "error")]
    return (
        f"INSERT INTO {tables.QUALIFIED_LLM_RESPONSE_TABLE} "
        f"({', '.join(columns)}) VALUES "
        f"({', '.join(values[column] for column in columns)})"
    )


def _person_insert() -> str:
    rows = []
    for company_id, key, birth_year, sources, members in PEOPLE:
        rows.append(
            f"('{company_id}', '{_id(key)}', {_literal(birth_year)}, {_literal(sources)}, "
            f"{_literal([_id(member[0]) for member in members])}, 1, {STAMP}, 'se-person-fold-v2')"
        )
    return (
        f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} (company_id, person_key, birth_year, "
        "sources, normalized_ids, active, folded_at, fold_version) VALUES " + ", ".join(rows)
    )


def _normalized_insert() -> str:
    rows = []
    for company_id, _key, _birth_year, _sources, members in PEOPLE:
        for normalized_id, source, first, middle, last in members:
            rows.append(
                f"('{company_id}', '{source}', '{normalized_id}', '{_id(normalized_id)}', "
                f"'ok', {_literal(first)}, {_literal(middle)}, {_literal(last)}, {STAMP})"
            )
    return (
        f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} (company_id, source, slot, "
        "normalized_id, parse_status, first_tokens, middle_tokens, last_tokens, "
        "normalized_at) VALUES " + ", ".join(rows)
    )


def _pair_insert(*, company_id: str, members_a: str, members_b: str, confidence: float,
                 prompt_version: str, input_hash: str, matched_at: str,
                 request_id: str | None) -> str:
    """A pair row. `request_id=None` is the pre-ALTER shape: the column does not exist yet."""
    columns = [
        "company_id", "candidate_a", "candidate_b", "members_a", "members_b", "source_a",
        "source_b", "name_a", "name_b", "confidence", "reason", "model", "prompt_version",
        "input_hash", "matched_at",
    ]
    values = [
        f"'{company_id}'", f"'{_id('ca-' + company_id)}'", f"'{_id('cb-' + company_id)}'",
        f"['{_id(members_a)}']", f"['{_id(members_b)}']", "'ratsit'", "'bolagsverket'",
        "'Erik Bo Bengtsson'", "'Bo Bengtsson'", repr(confidence), "'same person'",
        "'deepseek-v4-flash'", f"'{prompt_version}'", f"'{_id(input_hash)}'", matched_at,
    ]
    if request_id is not None:
        columns.append("request_id")
        values.append(f"'{request_id}'")
    return (
        f"INSERT INTO {tables.QUALIFIED_MATCH_TABLE} ({', '.join(columns)}) "
        f"VALUES ({', '.join(values)})"
    )


def _state_insert(company_id: str, input_hash: str) -> str:
    return (
        f"INSERT INTO {tables.QUALIFIED_MATCH_STATE_TABLE} (company_id, input_hash, "
        "candidates, sources, pairs, model, prompt_version, prompt_tokens, "
        "completion_tokens, raw_response, error, source_run_id, matched_at) VALUES "
        f"('{company_id}', '{_id(input_hash)}', 2, 2, 1, 'deepseek-v4-flash', "
        f"'se-person-match-v1', 900, 60, '{{}}', '', 'run-1', {MATCHED_AT})"
    )


def _script(join_use_nulls: int) -> str:
    key_sql = (
        "SELECT sorting_key FROM system.tables "
        f"WHERE database = 'corpscout' AND name = '{tables.MATCH_TABLE}'"
    )
    statements = [
        f"SET join_use_nulls = {join_use_nulls}",
        *_schema(),
        *_pattern_tables(),
        _person_insert(),
        _normalized_insert(),
        # Two pair rows written BEFORE the alter, in the pre-000406 shape: one for the
        # already-matched company, one for a company with no persons at all.
        _pair_insert(company_id=ALREADY_MATCHED, members_a="n2a", members_b="n2b",
                     confidence=0.9, prompt_version="se-person-match-v1",
                     input_hash="hash2", matched_at=MATCHED_AT, request_id=None),
        _pair_insert(company_id=PAIR_ONLY, members_a="n7a", members_b="n7b",
                     confidence=0.95, prompt_version="se-person-match-v1",
                     input_hash="hash7", matched_at=MATCHED_AT, request_id=None),
        _state_insert(ALREADY_MATCHED, "hash2"),
        _state_insert(PAIR_ONLY, "hash7"),
        "SELECT '@@key_before'",
        key_sql,
        "SELECT '@@rows_before'",
        f"SELECT count() FROM {tables.QUALIFIED_MATCH_TABLE}",
        *_alters(),
        "SELECT '@@key_after'",
        key_sql,
        "SELECT '@@rows_after'",
        f"SELECT count(), countIf(request_id = '') FROM {tables.QUALIFIED_MATCH_TABLE}",
        "SELECT '@@state_after'",
        f"SELECT count(), countIf(request_id = '') FROM {tables.QUALIFIED_MATCH_STATE_TABLE}",
        # The v2 answer for the SAME candidate pair, under its own request.
        _pair_insert(company_id=ALREADY_MATCHED, members_a="n2a", members_b="n2b",
                     confidence=0.95, prompt_version="se-person-match-v2",
                     input_hash="hash2", matched_at=V2_AT, request_id="req-0002"),
        "SELECT '@@versions'",
        f"SELECT request_id, prompt_version, confidence FROM {tables.QUALIFIED_MATCH_TABLE} "
        f"FINAL WHERE company_id = '{ALREADY_MATCHED}' ORDER BY request_id",
        # The pattern's own two tables, written from the column tuples they own.
        _queue_insert(),
        _response_insert(),
        # Two more rows omitting the DEFAULT columns, to prove they default to ''.
        # Different request_ids to avoid deduplication under FINAL.
        _queue_insert(request_id_suffix="1", omit_note=True),
        _response_insert(request_id_suffix="1", omit_error=True),
        "SELECT '@@queue'",
        f"SELECT request_id, company_id, queued_by, note "
        f"FROM {tables.QUALIFIED_LLM_QUEUE_TABLE} FINAL ORDER BY request_id",
        "SELECT '@@response'",
        f"SELECT request_id, company_id, provider, model, prompt_version, "
        f"toString(input_hash), candidates, prompt_tokens, completion_tokens, "
        f"raw_response, error, attempts, source_run_id "
        f"FROM {tables.QUALIFIED_LLM_RESPONSE_TABLE} FINAL ORDER BY request_id",
        "SELECT '@@queue_defaults'",
        f"SELECT name, default_kind, default_expression FROM system.columns "
        f"WHERE database = 'corpscout' AND table = '{tables.LLM_QUEUE_TABLE}' AND name = 'note'",
        "SELECT '@@response_defaults'",
        f"SELECT name, default_kind, default_expression FROM system.columns "
        f"WHERE database = 'corpscout' AND table = '{tables.LLM_RESPONSE_TABLE}' AND name = 'error'",
        "SELECT '@@gap'",
        tables.build_se_company_person_match_gap_sql(),
    ]
    return ";\n".join(statements) + ";\n"


def _sections(lines: list[str]) -> dict[str, list[list[str]]]:
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in lines:
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        else:
            result[current].append(line.split("\t"))
    return result


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def run(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    script = _script(request.param)
    try:
        completed = subprocess.run(
            clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return _sections([line for line in completed.stdout.splitlines() if line.strip()])


def test_the_combined_alter_widens_the_sort_key_on_a_populated_table(run) -> None:
    """Spec section 4.3 and the one risk of section 12. The ALTER is accepted against a table
    that already holds rows -- it is metadata-only, because every stored row's new key
    component is the same empty string, so no part has to be re-sorted."""
    assert run["key_before"] == [["company_id, candidate_a, candidate_b"]]
    assert run["key_after"] == [["company_id, candidate_a, candidate_b, request_id"]]


def test_every_stored_pair_survives_the_alter_with_an_empty_request(run) -> None:
    assert run["rows_before"] == [["2"]]
    # count() and countIf(request_id = '') are equal: nothing was lost and nothing acquired a
    # request it never had.
    assert run["rows_after"] == [["2", "2"]]
    assert run["state_after"] == [["2", "2"]]


def test_two_prompt_versions_of_one_pair_coexist_after_the_alter(run) -> None:
    """The whole reason the key grew: without request_id in it, the v2 row REPLACES the v1
    row for the same candidate pair, and a v2 that scored it LOWER would silently unmerge a
    person at the next fold. With it, both rows stand and the fold takes the maximum."""
    assert run["versions"] == [
        ["", "se-person-match-v1", "0.9"],
        ["req-0002", "se-person-match-v2", "0.95"],
    ]


def test_the_two_new_tables_accept_the_pattern_insert_tuples(run) -> None:
    """Spec sections 4.1 and 4.2, proved against the real DDL: a row built from
    tables.LLM_QUEUE_COLUMNS and one from tables.LLM_RESPONSE_COLUMNS are accepted by the
    types, the valid_company_id constraint and both DEFAULT clauses, and each reads back as
    one row per (request, company) under FINAL. The DEFAULT clauses are verified: inserts
    omitting `note` and `error` columns read back with '' (the default), and system.columns
    declares both columns with default_kind = 'DEFAULT' and default_expression = '\'\''."""
    # Two rows in queue: one with note supplied, one where it defaults to ''.
    # Ordered by request_id, so the shorter request_id comes first, then the one with suffix "1".
    assert run["queue"] == [
        ["0123456789abcdef0123456789abcdef", GAP_CALL_NAME, "backoffice",
         "se-person-match-v2 gap"],
        ["0123456789abcdef0123456789abcdef1", GAP_CALL_NAME, "backoffice", ""],
    ]
    # Two rows in response: one with error supplied, one where it defaults to ''.
    assert run["response"] == [
        ["0123456789abcdef0123456789abcdef", GAP_CALL_NAME, "deepseek", "deepseek-v4-flash",
         "se-person-match-v2", _id("hash1"), "2", "1128", "64", "no pairs", "", "1",
         "run-enhance-1"],
        ["0123456789abcdef0123456789abcdef1", GAP_CALL_NAME, "deepseek", "deepseek-v4-flash",
         "se-person-match-v2", _id("hash1"), "2", "1128", "64", "no pairs", "", "1",
         "run-enhance-1"],
    ]
    # Verify the DEFAULT declarations in system.columns.
    # The server returns the default_expression as escaped single quotes: \'\'.
    assert run["queue_defaults"] == [["note", "DEFAULT", "\\'\\'"]]
    assert run["response_defaults"] == [["error", "DEFAULT", "\\'\\'"]]


def test_the_gap_view_finds_exactly_the_three_open_pairs(run) -> None:
    """Spec section 5.1's two definitions, and all four of its exclusions, in one readout.

    Included: two call-name companies (one with birth year on both sides, one with birth year
    only on the ratsit side -- the production shape) and one double-surname company (one pair,
    no call name) -- and never both counters for one pair, because the two rules disagree about
    the surnames by construction.

    Excluded: the company whose call-name pair a stored 0.9 match already closed; the company
    whose two persons share `ratsit`; the company whose birth years conflict; and the company
    whose only machine source is Bolagsverket. The reviewer-shaped third person of the
    call-name company is excluded too -- it would have made that company's count 2.
    """
    rows = sorted(row[:3] for row in run["gap"])

    assert rows == [
        [GAP_CALL_NAME, "1", "0"],
        [GAP_DOUBLE, "0", "1"],
        [BIRTH_YEAR_ONLY_RATSIT, "1", "0"],
    ]
    # computed_at is the fourth column and is stamped by now64, so it is only checked for
    # presence -- a missing value here would mean the SELECT lost a column.
    assert all(len(row) == 4 and row[3] for row in run["gap"])
