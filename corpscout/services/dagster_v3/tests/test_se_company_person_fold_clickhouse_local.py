"""The person fold end to end against a real ClickHouse (clickhouse-local).

Claims a fake client cannot settle (spec 2026-09-09 section 5, R9 of the slice-2 brief):

1. Migration 000396's six tables accept every shape this slice writes: raw suggestions,
   normalized rows built by normalize.normalized_row, a 29-value main tuple from
   PublishedPerson.as_tuple (arrays, Array(Array(String)), Array(Nullable(UInt16)),
   FixedString(64), Nullable(UInt16)), a 32-value history tuple, a rule row and the five
   precedence rows the export writes.
2. A company folds end to end through the REAL batch SQL -- selection, the four page reads
   under FINAL, history then main -- and the main row reads back as the fold built it.
3. Re-running the same fold selects NOTHING: the rewrite advanced max(folded_at) past every
   input watermark.
4. A hide rule deactivates its person; a merge rule joins two persons and withdraws the key
   it absorbed; a normalized row whose current version turns into a tombstone (no_person)
   withdraws its person.

`_LocalClient` is a clickhouse-driver-shaped client over `clickhouse-local`: the process is
stateless, so the session keeps every statement it has run and replays the whole script for
each SELECT. That costs one process per read (about ten per fold round) and keeps the test
honest -- batch.py runs unmodified, with its own SQL.

Both `join_use_nulls` settings run: none of these statements joins, so the parametrization is
a guard against a future one, not a live risk.
"""

import hashlib
import json
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from dagster_v3.defs.se_company.person import batch, tables, match_input
from dagster_v3.defs.se_company.person.assets import export_precedence
from dagster_v3.defs.se_company.person.fold import MATCH_THRESHOLD, person_key
from dagster_v3.defs.se_company.person.normalize import RAW_ROW_COLUMNS, normalized_row
from dagster_v3.defs.se_company.person.normalize_se import NORMALIZER_VERSION
from tests.clickhouse_local import clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION_FILE = "000396_corpscout_se_company_person_entity.up.sql"
MATCH_MIGRATION_FILE = "000399_corpscout_se_company_person_match.up.sql"
ENHANCE_MIGRATION_FILE = "000406_corpscout_se_company_person_llm_enhance.up.sql"

COMPANY = "5561552760"          # two persons: Anna Svensson (2 members) and Håkan Öberg
HIDE_CO = "5560000003"          # one person, hidden by a rule in round 3
GONE_CO = "5560000004"          # one person, tombstoned in round 3
SUGGESTED_AT = datetime(2026, 9, 9, 12, 0, tzinfo=UTC)
NORMALIZED_AT = datetime(2026, 9, 9, 13, 0, tzinfo=UTC)
TOMBSTONED_AT = datetime(2026, 9, 10, 10, 0, tzinfo=UTC)   # after FIRST_FOLD_AT, so round 3 selects GONE_CO
EXPORTED_AT = datetime(2026, 9, 10, 8, 0, tzinfo=UTC)
RULE_AT = datetime(2026, 9, 10, 10, 30, tzinfo=UTC)
FIRST_FOLD_AT = datetime(2026, 9, 10, 9, 0, tzinfo=UTC)
SECOND_FOLD_AT = datetime(2026, 9, 10, 11, 0, tzinfo=UTC)
FOURTH_FOLD_AT = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)   # after round 3's SECOND_FOLD_AT

# (company_id, source, slot, suggestion_id, full_name, first_name, last_name, birth_year,
#  wikidata_id, role_original, role_key, fiscal_year, role_from, role_to, data)
RAW = (
    (COMPANY, "bolagsverket", "rec1:sig1", "a" * 64, None, "Anna", "Svensson", None, None,
     "Styrelseledamot", "board_member", 2024, None, None, '{"signatory_kind":"board"}'),
    (COMPANY, "esef", "doc7:2", "b" * 64, "Anna Maria Svensson", None, None, None, None,
     "VD", "chief_executive", 2023, None, None, '{"organization":"ACME"}'),
    (COMPANY, "wikidata", "Q1:P169:Q9", "c" * 64, "Håkan Öberg", None, None, 1962, "Q9",
     "chief executive officer", "P169", None, date(2020, 1, 1), None, '{"description":"CEO"}'),
    (HIDE_CO, "bolagsverket", "rec2:sig1", "d" * 64, None, "Erik", "Larsson", None, None,
     "Revisor", "auditor", 2022, None, None, "{}"),
    (GONE_CO, "bolagsverket", "rec3:sig1", "e" * 64, None, "Karin", "Nilsson", None, None,
     "Ordförande", "chairman", 2021, None, None, "{}"),
)
# GONE_CO's slot after the source stopped delivering it: every person column NULL, which the
# normalizer classifies no_person and the fold therefore never sees.
TOMBSTONE = (GONE_CO, "bolagsverket", "rec3:sig1", "f" * 64, None, None, None, None, None,
             None, None, None, None, None, "{}")

COMPANY_IDS = [COMPANY, HIDE_CO, GONE_CO]


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, datetime):
        return f"toDateTime64('{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}', 3, 'UTC')"
    if isinstance(value, date):
        return f"toDate('{value.isoformat()}')"
    if isinstance(value, list):
        return "[" + ", ".join(_literal(item) for item in value) + "]"
    if isinstance(value, tuple):
        return "(" + ", ".join(_literal(item) for item in value) + ")"
    if isinstance(value, (int, float)):
        return repr(value)
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _schema_statements() -> list[str]:
    """CREATE DATABASE plus the six CREATE TABLEs of 000396 and the two of 000399, then the
    two ALTER TABLEs of 000406 -- never 000396's SYSTEM STOP/START VIEW or ALTER TABLE ...
    MODIFY QUERY, which name se_companies_serving, a view this fixture does not build, and
    never 000406's two new tables or its refreshable view, which the fold never reads.
    000396 declares the main table under its build name and 000398 renames the DEPLOYED table
    without touching that file, so the rename is replayed here: batch.py reads
    tables.QUALIFIED_MAIN_TABLE, which is the renamed name. The 000406 alters matter because
    tables.MATCH_COLUMNS and MATCH_STATE_COLUMNS are the DEPLOYED column lists and both end
    with request_id."""
    statements: list[str] = []
    for name in (MIGRATION_FILE, MATCH_MIGRATION_FILE, ENHANCE_MIGRATION_FILE,
                 "000407_corpscout_se_company_person_match_input.up.sql"):
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith("CREATE DATABASE") or (
                "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_" in statement
            ) or statement.startswith((
                f"ALTER TABLE {tables.QUALIFIED_MATCH_TABLE}\n",
                f"ALTER TABLE {tables.QUALIFIED_MATCH_STATE_TABLE}\n",
            )):
                statements.append(
                    statement.replace(
                        "corpscout.se_company_person_v2", tables.QUALIFIED_MAIN_TABLE
                    )
                )
    return statements


_QUERY_COLUMNS: dict[str, tuple[str, ...]] = {
    batch.bucket_company_ids_sql(): ("company_id",),
    batch.normalized_watermarks_sql(): ("company_id", "normalized_at", "foldable"),
    batch.main_watermarks_sql(): ("company_id", "folded_at"),
    batch.rule_watermarks_sql(): ("company_id", "created_at"),
    batch.company_precedence_watermarks_sql(): ("company_id", "decided_at"),
    batch.global_precedence_watermark_sql(): ("decided_at",),
    batch.current_normalized_sql(): batch.NORMALIZED_SELECT_COLUMNS,
    match_input.normalized_inputs_sql(): (*batch.NORMALIZED_SELECT_COLUMNS, "normalized_at"),
    batch.current_main_rows_sql(): batch.MAIN_SELECT_COLUMNS,
    batch.active_rules_sql(): batch.RULE_SELECT_COLUMNS,
    batch.company_precedence_sql(): ("company_id", "source", "precedence"),
    batch.match_watermarks_sql(): ("company_id", "matched_at"),
    batch.match_pairs_sql(): batch.MATCH_PAIR_SELECT_COLUMNS,
}
_DATETIME_COLUMNS = frozenset(
    {"normalized_at", "folded_at", "created_at", "decided_at", "matched_at"}
)
_DATE_COLUMNS = frozenset({"role_from", "role_to"})


def _value(column: str, value: Any) -> Any:
    if value is None:
        return None
    if column in _DATETIME_COLUMNS:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=UTC)
    if column in _DATE_COLUMNS:
        return date.fromisoformat(value)
    return value


class _LocalClient:
    """A clickhouse-driver-shaped client over clickhouse-local. Writes are remembered and
    replayed before every read, because each invocation starts with an empty server."""

    def __init__(self, setting: int) -> None:
        self.statements: list[str] = [f"SET join_use_nulls = {setting}", *_schema_statements()]

    def add(self, statement: str) -> None:
        self.statements.append(statement)

    def _run(self, query: str) -> list[str]:
        script = ";\n".join([*self.statements, query]) + ";\n"
        try:
            completed = subprocess.run(
                clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
            )
        except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
            pytest.skip(f"clickhouse-local is unusable here: {exc}")
        assert completed.returncode == 0, completed.stderr or completed.stdout
        return [line for line in completed.stdout.splitlines() if line.strip()]

    def execute(self, sql, params=None, settings=None):
        if sql.startswith("INSERT"):
            values = ", ".join(_literal(tuple(row)) for row in params)
            self.add(f"{sql} {values}")
            return []
        # Anything not in the map is a one-off read (export_precedence's stale count):
        # returned raw, with no per-column conversion.
        columns = _QUERY_COLUMNS.get(sql)
        rendered = sql
        for name, value in (params or {}).items():
            rendered = rendered.replace(
                f"%({name})s", _literal(tuple(value) if isinstance(value, list) else value)
            )
        assert "%(" not in rendered, rendered
        lines = self._run(f"SELECT * FROM ({rendered}) AS q FORMAT JSONCompactEachRow")
        if columns is None:
            return [tuple(json.loads(line)) for line in lines]
        return [
            tuple(_value(column, item) for column, item in zip(columns, json.loads(line), strict=True))
            for line in lines
        ]

    def read(self, query: str) -> list[list[str]]:
        """A read the batch does not make: the test's own assertions."""
        return [line.split("\t") for line in self._run(query)]


def _raw_insert(rows, suggested_at: datetime) -> str:
    ordered = []
    for raw in rows:
        values = dict(zip(RAW_ROW_COLUMNS, raw, strict=True))
        values.update(suggested_at=suggested_at, source_record_id="", document_ref=None)
        ordered.append(tuple(values[column] for column in tables.SUGGESTION_COLUMNS))
    return (
        f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE} "
        f"({', '.join(tables.SUGGESTION_COLUMNS)}) VALUES "
        + ", ".join(_literal(row) for row in ordered)
    )


def _normalized_insert(rows, normalized_at: datetime) -> str:
    values = ", ".join(_literal(tuple(normalized_row(raw, normalized_at))) for raw in rows)
    return (
        f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} "
        f"({', '.join(tables.NORMALIZED_COLUMNS)}) VALUES {values}"
    )


def _rule_insert(company_id: str, rule_id: str, kind: str, person_keys, slots, active: int) -> str:
    row = (company_id, rule_id, kind, list(person_keys), list(slots), active, "", RULE_AT, "reviewer")
    return (
        f"INSERT INTO {tables.QUALIFIED_RULE_TABLE} "
        f"({', '.join(tables.RULE_COLUMNS)}) VALUES {_literal(row)}"
    )


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def folded(request: pytest.FixtureRequest) -> dict[str, Any]:
    """Four rounds against one clickhouse-local session: the first fold, the re-run, a fold
    under a hide rule, a merge rule and a tombstone, and a changed_only=False refold that
    proves the round trip through real ClickHouse reproduces every row exactly."""
    client = _LocalClient(request.param)
    client.add(_raw_insert(RAW, SUGGESTED_AT))
    client.add(_normalized_insert(RAW, NORMALIZED_AT))
    export_precedence(client, EXPORTED_AT)          # the five global rows, through the real code

    first = batch.fold_companies(
        client, COMPANY_IDS, changed_only=True, source_run_id="run-1", folded_at=FIRST_FOLD_AT
    )
    # Captured HERE, before the merge rule below folds Håkan into Anna's set: round 3 also
    # touches COMPANY (its rule's created_at is newer than round 1's folded_at), so a read
    # against the live table from inside a later test would see round 3's merged row, not
    # what round 1 actually published.
    first_company_rows = client.read(
        f"SELECT person_key, display_name, text_source, arrayStringConcat(sources, ','), "
        f"arrayStringConcat(arrayMap(x -> toString(x), role_years), ','), "
        f"arrayStringConcat(current_roles, ','), active "
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL WHERE company_id = '{COMPANY}' "
        "ORDER BY display_name"
    )
    rerun = batch.fold_companies(
        client, COMPANY_IDS, changed_only=True, source_run_id="run-2", folded_at=SECOND_FOLD_AT
    )
    anna = person_key(COMPANY, ("anna", "maria", "svensson"))
    hakan = person_key(COMPANY, ("hakan", "oberg"))
    erik = person_key(HIDE_CO, ("erik", "larsson"))
    client.add(_rule_insert(HIDE_CO, "1" * 64, "hide", [erik], [], 1))
    client.add(_rule_insert(COMPANY, "2" * 64, "merge", [anna, hakan], [], 1))
    client.add(_raw_insert((TOMBSTONE,), TOMBSTONED_AT))
    client.add(_normalized_insert((TOMBSTONE,), TOMBSTONED_AT))
    third = batch.fold_companies(
        client, COMPANY_IDS, changed_only=True, source_run_id="run-3", folded_at=SECOND_FOLD_AT
    )
    # A full changed_only=False refold with nothing actually different since round 3: every
    # company is read back through the real clickhouse-driver-shaped round trip (FixedString
    # keys, Nullable(UInt16), the seven Array columns, `data`) and folded again, so a single
    # type mismatch on the way back would turn rows `updated` instead of `unchanged`.
    fourth = batch.fold_companies(
        client, COMPANY_IDS, changed_only=False, source_run_id="run-4", folded_at=FOURTH_FOLD_AT
    )
    return {
        "client": client, "first": first, "rerun": rerun, "third": third, "fourth": fourth,
        "first_company_rows": first_company_rows,
        "keys": {"anna": anna, "hakan": hakan, "erik": erik},
    }


def test_the_first_fold_publishes_every_person_with_its_members_and_roles(folded) -> None:
    counts = folded["first"]
    assert (counts.companies, counts.considered) == (3, 3)
    assert (counts.persons, counts.created, counts.withdrawn) == (4, 4, 0)
    assert counts.stale_rules == 0 and counts.sets_split_by_birth_year == 0
    # Read from the snapshot the fixture captured right after round 1 -- round 3's merge
    # rule later folds Håkan into Anna's set for this same company, so a fresh read here
    # (after the fixture's later rounds have already run) would see round 3's row instead.
    rows = folded["first_company_rows"]
    by_name = {row[1]: row for row in rows}
    anna = by_name["Anna Svensson"]                  # the spelling is bolagsverket's (900)
    assert anna[0] == folded["keys"]["anna"]
    assert anna[2] == "bolagsverket"                 # 900 beats esef 400 for the spelling
    assert anna[3] == "bolagsverket,esef"
    assert anna[4] == "2023,2024"
    assert anna[6] == "1"
    hakan = by_name["Håkan Öberg"]
    assert hakan[3] == "wikidata"
    # The Wikidata span 2020-01-01 with no end expands to every year up to the fold's year.
    assert hakan[4].split(",")[0] == "2020" and hakan[4].split(",")[-1] == str(FIRST_FOLD_AT.year)
    assert hakan[5] == "chief_executive_officer"


def test_the_history_of_the_first_fold_is_one_created_row_per_person(folded) -> None:
    rows = folded["client"].read(
        f"SELECT change_kind, count() FROM {tables.QUALIFIED_HISTORY_TABLE} "
        "WHERE fold_run_id = 'run-1' GROUP BY change_kind"
    )
    assert rows == [["created", "4"]]


def test_re_running_the_fold_selects_nothing(folded) -> None:
    """The selection converges: every input watermark is older than the folded_at the first
    run stamped on every row."""
    counts = folded["rerun"]
    assert counts.considered == 0 and counts.persons == 0 and counts.created == 0
    rows = folded["client"].read(
        f"SELECT count() FROM {tables.QUALIFIED_HISTORY_TABLE} WHERE fold_run_id = 'run-2'"
    )
    assert rows == [["0"]]


def test_a_rule_a_merge_and_a_tombstone_all_move_their_persons(folded) -> None:
    counts = folded["third"]
    assert counts.considered == 3 and counts.stale_rules == 0
    client = folded["client"]
    hidden = client.read(
        f"SELECT active, inactive_reason FROM {tables.QUALIFIED_MAIN_TABLE} FINAL "
        f"WHERE company_id = '{HIDE_CO}'"
    )
    assert hidden == [["0", "hidden"]]
    withdrawn = client.read(
        f"SELECT active, inactive_reason, length(sources) FROM {tables.QUALIFIED_MAIN_TABLE} "
        f"FINAL WHERE company_id = '{GONE_CO}'"
    )
    assert withdrawn == [["0", "withdrawn", "1"]]
    merged = client.read(
        f"SELECT person_key, length(member_slots), active, inactive_reason "
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL WHERE company_id = '{COMPANY}' "
        "ORDER BY active DESC"
    )
    assert merged[0][:2] == [folded["keys"]["anna"], "3"]      # the merge absorbed Håkan
    assert merged[1] == [folded["keys"]["hakan"], "1", "0", "withdrawn"]
    kinds = client.read(
        f"SELECT change_kind, count() FROM {tables.QUALIFIED_HISTORY_TABLE} "
        "WHERE fold_run_id = 'run-3' GROUP BY change_kind ORDER BY change_kind"
    )
    assert kinds == [["hidden", "1"], ["updated", "1"], ["withdrawn", "2"]]


def test_a_stable_refold_reproduces_every_row_the_driver_wrote(folded) -> None:
    """I-4: no test before this one ever proved that a row written to real ClickHouse and
    read back through current_main_rows_sql/main_row_from_row compares EQUAL to a freshly
    built one -- round 2 selects nothing (the main read never runs) and round 3 changes every
    company. A changed_only=False refold here forces every company in the fixture through
    that exact read-compare-write cycle with nothing actually different since round 3, so a
    single mismatched column (Array(Nullable(UInt16)), Array(Array(String)),
    FixedString(64), Nullable(String), ...) would turn every row `updated` and write a spurious
    history entry, while the rest of the suite stayed green."""
    counts = folded["fourth"]
    assert counts.considered == 3
    for kind in ("created", "updated", "hidden", "withdrawn", "reactivated"):
        assert getattr(counts, kind) == 0
    client = folded["client"]
    ids = ", ".join(f"'{company_id}'" for company_id in COMPANY_IDS)
    per_company = client.read(
        f"SELECT company_id, count() FROM {tables.QUALIFIED_MAIN_TABLE} FINAL "
        f"WHERE company_id IN ({ids}) GROUP BY company_id ORDER BY company_id"
    )
    # Every company in the fixture still has its rows, and every one of those rows -- active,
    # hidden and withdrawn alike -- came back unchanged: the ClickHouse row count for the
    # fixture equals what fourth reports as `unchanged`.
    assert {row[0] for row in per_company} == set(COMPANY_IDS)
    assert counts.unchanged == sum(int(count) for _, count in per_company)
    history = client.read(
        f"SELECT count() FROM {tables.QUALIFIED_HISTORY_TABLE} WHERE fold_run_id = 'run-4'"
    )
    assert history == [["0"]]


def test_no_company_ever_carries_one_person_key_twice(folded) -> None:
    """FINAL collapses a shared key silently, so the check cannot read the main table alone:
    every key the fold ever created is one `created` history row (the history table is a
    plain MergeTree, nothing collapses there), and the main table must carry exactly that
    many distinct (company_id, person_key) pairs."""
    created = folded["client"].read(
        f"SELECT count() FROM {tables.QUALIFIED_HISTORY_TABLE} WHERE change_kind = 'created'"
    )
    distinct = folded["client"].read(
        f"SELECT uniqExact((company_id, person_key)) FROM {tables.QUALIFIED_MAIN_TABLE} FINAL"
    )
    assert created == distinct and created != [["0"]]


def test_the_precedence_export_wrote_its_five_global_rows(folded) -> None:
    rows = folded["client"].read(
        f"SELECT source, precedence FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL "
        "WHERE company_id = '' AND field = 'name' ORDER BY precedence DESC"
    )
    assert rows == [["reviewer", "20000"], ["ratsit", "1000"], ["bolagsverket", "900"],
                    ["wikidata", "600"], ["esef", "400"]]


# --- a real stored match pair, read through the real SQL (spec 2026-09-11 section 4) -----

MATCH_CO = "5560000005"            # one Ratsit and one ESEF spelling of one person
MATCHED_AT = datetime(2026, 9, 10, 10, 45, tzinfo=UTC)
FIFTH_FOLD_AT = datetime(2026, 9, 10, 13, 0, tzinfo=UTC)

MATCH_RAW = (
    (MATCH_CO, "ratsit", "p1", "1" * 64, "Erik Bo Bengtsson", None, None, 1966, None,
     "Verkställande direktör", None, 2026, None, None, '{"age":"60"}'),
    (MATCH_CO, "esef", "doc9:1", "2" * 64, "Bo Bengtsson", None, None, None, None,
     "VD", "chief_executive", 2025, None, None, "{}"),
)


MATCH_HASH = "h" * 64
MATCH_ANSWER = '{"pairs":[{"a":"...","b":"...","confidence":0.93,"reason":"call name"}]}'


def _normalized_id(suggestion_id: str) -> str:
    """The id normalize.normalized_row computes, which is what a match row names."""
    return hashlib.sha256(f"{suggestion_id}\n{NORMALIZER_VERSION}".encode()).hexdigest()


def _match_insert(company_id: str, ratsit_id: str, esef_id: str) -> str:
    low, high = sorted((ratsit_id, esef_id))
    names = {ratsit_id: "Erik Bo Bengtsson", esef_id: "Bo Bengtsson"}
    sources = {ratsit_id: "ratsit", esef_id: "esef"}
    row = (company_id, low, high, [low], [high], sources[low], sources[high],
           names[low], names[high], 0.93, "call name",
           "deepseek-v4-flash", "se-person-match-v1", MATCH_HASH, MATCHED_AT, "")
    return (
        f"INSERT INTO {tables.QUALIFIED_MATCH_TABLE} "
        f"({', '.join(tables.MATCH_COLUMNS)}) VALUES {_literal(row)}"
    )


def _match_state_insert(company_id: str) -> str:
    row = (company_id, MATCH_HASH, 2, 2, 1, "deepseek-v4-flash", "se-person-match-v1",
           480, 60, MATCH_ANSWER, "", "run-match", MATCHED_AT, "")
    return (
        f"INSERT INTO {tables.QUALIFIED_MATCH_STATE_TABLE} "
        f"({', '.join(tables.MATCH_STATE_COLUMNS[:14])}) VALUES {_literal(row)}"
    )


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def matched(request: pytest.FixtureRequest) -> dict[str, Any]:
    """One company, two sources spelling one person, and a stored pair at 0.93: the fold's
    real SQL must read the pair through the hash join and publish ONE person."""
    client = _LocalClient(request.param)
    client.add(_raw_insert(MATCH_RAW, SUGGESTED_AT))
    client.add(_normalized_insert(MATCH_RAW, NORMALIZED_AT))
    export_precedence(client, EXPORTED_AT)
    before = batch.fold_companies(
        client, [MATCH_CO], changed_only=True, source_run_id="run-a", folded_at=FIRST_FOLD_AT
    )
    apart = client.read(
        f"SELECT count() FROM {tables.QUALIFIED_MAIN_TABLE} FINAL "
        f"WHERE company_id = '{MATCH_CO}' AND active = 1"
    )
    client.add(_match_insert(MATCH_CO, _normalized_id("1" * 64), _normalized_id("2" * 64)))
    client.add(_match_state_insert(MATCH_CO))
    after = batch.fold_companies(
        client, [MATCH_CO], changed_only=True, source_run_id="run-b", folded_at=FIFTH_FOLD_AT
    )
    return {"client": client, "before": before, "after": after, "apart": apart}


def test_a_stored_pair_joins_the_two_spellings_into_one_person(matched) -> None:
    assert matched["apart"] == [["2"]]                 # two persons before the match
    # The match stamp is newer than the first fold, so the fifth watermark selects it.
    assert matched["after"].considered == 1
    rows = matched["client"].read(
        f"SELECT display_name, text_source, arrayStringConcat(sources, ','), "
        f"JSONExtractString(data, 'llm_match', 'model'), "
        f"JSONExtractFloat(JSONExtractRaw(data, 'llm_match'), 'pairs', 1, 'confidence') "
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL "
        f"WHERE company_id = '{MATCH_CO}' AND active = 1"
    )
    assert len(rows) == 1
    assert rows[0][0] == "Erik Bo Bengtsson" and rows[0][1] == "ratsit"
    assert rows[0][2] == "ratsit,esef"
    assert rows[0][3] == "deepseek-v4-flash" and rows[0][4].startswith("0.93")
    withdrawn = matched["client"].read(
        f"SELECT count() FROM {tables.QUALIFIED_MAIN_TABLE} FINAL "
        f"WHERE company_id = '{MATCH_CO}' AND inactive_reason = 'withdrawn'"
    )
    assert withdrawn == [["1"]]


def test_the_stored_confidence_is_float64_and_compares_exactly(matched) -> None:
    """Fix wave F6: the fold admits a pair with `confidence >= MATCH_THRESHOLD` and
    match_pairs_sql repeats that comparison in SQL, so the column may not round the value.
    As Float32, 0.93 comes back as 0.9300000071525574 and `confidence = 0.93` is FALSE --
    and a threshold like 0.7 would be stored as 0.69999998807907104 and drop every pair
    scored at exactly it."""
    rows = matched["client"].read(
        f"SELECT toTypeName(confidence), confidence = 0.93, confidence >= {MATCH_THRESHOLD} "
        f"FROM {tables.QUALIFIED_MATCH_TABLE} FINAL WHERE company_id = '{MATCH_CO}'"
    )
    assert rows == [["Float64", "1", "1"]]


def test_re_running_the_matched_fold_selects_nothing(matched) -> None:
    counts = batch.fold_companies(
        matched["client"], [MATCH_CO], changed_only=True, source_run_id="run-c",
        folded_at=datetime(2026, 9, 10, 14, 0, tzinfo=UTC),
    )
    assert counts.considered == 0


def test_a_new_answer_with_no_pairs_supersedes_old_pairs_for_the_same_input() -> None:
    client = _LocalClient(0)
    client.add(_match_insert(MATCH_CO, _normalized_id("1" * 64), _normalized_id("2" * 64)))
    client.add(_match_state_insert(MATCH_CO))
    assert len(client.execute(batch.match_pairs_sql(), {"company_ids": [MATCH_CO]})) == 1
    # A forced re-match can return no pairs while its candidate/prompt hash stays identical.
    # Its state certifies this answer only, not old pairs omitted from the new response.
    stamp = datetime(2026, 9, 11, 15, 0, tzinfo=UTC)
    row = (MATCH_CO, MATCH_HASH, 2, 2, 0, "deepseek-v4-flash", "se-person-match-v1",
           480, 20, '{"pairs":[]}', "", "run-rematch", stamp, "")
    client.add(f"INSERT INTO {tables.QUALIFIED_MATCH_STATE_TABLE} "
               f"({', '.join(tables.MATCH_STATE_COLUMNS[:14])}) VALUES {_literal(row)}")
    assert client.execute(batch.match_pairs_sql(), {"company_ids": [MATCH_CO]}) == []
