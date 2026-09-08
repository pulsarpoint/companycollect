"""The chunked demand load reads the same rows the single query did -- on a real engine.

geocode_demand no longer streams the store's current resolver outcome for all ~2.09M
identities over one execute_iter (that read RESET once and HUNG 150 minutes). It walks the
result in address_id keyset pages instead. The load-bearing claim is that HOW it paginates
changed and WHAT it returns did not: the union of the pages is row-for-row the single-query
resolver read, with nothing dropped or repeated at a page boundary.

A substring test cannot prove that -- `LIMIT 1 BY`, the keyset `address_id >` bound and the
outer page `LIMIT` are ClickHouse semantics DuckDB will not run. So this executes both reads
against the store harness's own fixture (superseded resolver versions in RETRIED/REGRESSED, a
resolver-plus-adopted family in ADOPTED/RECLAIMED/TIED) in a disposable clickhouse-local and
compares them.
"""

import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.sweden_company import geocode_demand
from dagster_v3.defs.sweden_company.geocode_demand import (
    PREVIOUS_OUTCOME_COLUMNS,
    _outcome_page_sql,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    GEOCODED_STATUSES,
    LEGACY_ADOPTED_MATCH_METHOD,
    LEGACY_ADOPTED_POLICY_VERSION,
    QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
    STORE_COLUMNS,
    build_current_resolver_geocodes_sql,
)
from tests.test_se_company_person_clickhouse_local import (
    _clickhouse_local_command,
    _literal,
    _render,
)

pytestmark = pytest.mark.integration

# --- the store harness's own fixture -------------------------------------------------------
#
# It used to be imported from tests/test_sweden_geocode_store_clickhouse_local.py, which
# retired in slice 4b with the store-append asset and the four checks it executed. What the
# demand read needs is only the STORE side of that harness -- ten identities in every shape
# the read rule has to get right -- so that side is kept here, verbatim, and the identity and
# link tables it also built are not.

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
# 000317 creates the store, and the database with it. Nothing else is read here.
MIGRATIONS = ("000317_corpscout_se_address_geocodes_store.up.sql",)
NEEDED_TABLES = frozenset({"se_address_geocodes"})
_TABLE_RE = re.compile(
    r"^(?:CREATE TABLE(?: IF NOT EXISTS)?|ALTER TABLE)\s+corpscout\.(\w+)", re.IGNORECASE
)

STORE = QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE
POLICY = "se-address-resolution-policy-v5"

# address_id is a FixedString(64) fingerprint of normalized address text.
(
    SETTLED,
    RETRIED,
    REGRESSED,
    ADOPTED,
    RECLAIMED,
    DEMOTED_STREET,
    DEMOTED_AREA,
    SWALLOWED,
    RETIRED,
) = (character * 64 for character in "123456789")
# Outside the digit run because it arrived after the other nine were named; the shapes are
# named, not numbered, and renumbering them would churn every assertion below.
TIED = "b" * 64

# Four weekly references and the one-off import, each with the OSM snapshot it was computed
# against. reference_md5 keys the store; the two move together because a reference IS a
# snapshot.
MD5_1, MD5_2, MD5_3, MD5_4 = (f"md5-week-{week}" for week in range(1, 5))
MD5_LEGACY = "md5-legacy-import"
# The tie is ENGINEERED: no weekly pass lands on the import's own millisecond by itself.
MD5_TIE = "md5-tie"
SNAPSHOT_1 = datetime(2026, 7, 4, 1, tzinfo=UTC)
SNAPSHOT_2 = datetime(2026, 7, 11, 1, tzinfo=UTC)
SNAPSHOT_3 = datetime(2026, 7, 18, 1, tzinfo=UTC)
SNAPSHOT_4 = datetime(2026, 7, 25, 1, tzinfo=UTC)
SNAPSHOT_LEGACY = datetime(2024, 1, 6, 1, tzinfo=UTC)
SNAPSHOT_TIE = datetime(2026, 7, 14, 1, tzinfo=UTC)
T_1 = datetime(2026, 7, 4, 3, tzinfo=UTC)
T_2 = datetime(2026, 7, 11, 3, tzinfo=UTC)
T_IMPORT = datetime(2026, 7, 15, 12, tzinfo=UTC)
T_3 = datetime(2026, 7, 18, 3, 0, 0, 250_000, tzinfo=UTC)
T_4 = datetime(2026, 7, 25, 3, tzinfo=UTC)
RUN_1, RUN_2, RUN_3, RUN_4 = (f"run-week-{week}" for week in range(1, 5))
RUN_IMPORT = "run-adoption-import"
RUN_TIE = "run-tie"

# The store's own status/precision coupling, spelled out so the fixture cannot violate it by
# accident. Asserted exhaustive against GEOCODED_STATUSES: a new geocoded status has to be
# given a precision here rather than defaulting to ''.
PRECISION_BY_STATUS = {
    "matched_exact": "building",
    "matched_corrected": "building",
    "matched_site": "site",
    "matched_area": "area",
    "matched_street": "street",
}
assert set(PRECISION_BY_STATUS) == set(GEOCODED_STATUSES)

_STORE_DEFAULTS: dict[str, str] = {
    "address_id": "",
    "policy_version": f"'{POLICY}'",
    "reference_md5": f"'{MD5_1}'",
    "address_identity_run_id": "'identity-run-1'",
    "normalized_match_key": "'se|storgatan 1|11122|stockholm'",
    # The default row is a resolver `ambiguous`: no coordinate, no precision. Every
    # geocoded row says so explicitly through _geocoded().
    "match_status": "'ambiguous'",
    "candidate_count": "3",
    "candidate_record_ids": "[]",
    "candidate_record_urls": "[]",
    "match_method": "''",
    "match_confidence": "0.0",
    "latitude": "NULL",
    "longitude": "NULL",
    "geocode_provider": "'openstreetmap'",
    "geocode_precision": "''",
    "coordinate_method": "NULL",
    "coordinate_locality": "NULL",
    "coordinate_supporting_point_count": "0",
    "coordinate_spread_meters": "NULL",
    "source_record_id": "NULL",
    "source_record_url": "NULL",
    "source_url": "'https://download.geofabrik.de/sweden-latest.osm.pbf'",
    "source_object_key": "'osm/sweden-latest.osm.pbf'",
    "source_md5": f"'{MD5_1}'",
    "source_snapshot_at": _literal(SNAPSHOT_1),
    "source_retrieved_at": _literal(SNAPSHOT_1),
    "geocode_run_id": f"'{RUN_1}'",
    "matched_at": _literal(T_1),
}


def _row(defaults: dict[str, str], columns: tuple[str, ...], **overrides: str) -> str:
    """One VALUES tuple, bound POSITIONALLY to the module's own column list.

    The assert is the point: a column added to the migration and forgotten here fails the
    script rather than silently shifting every value one place to the left.
    """
    row = {**defaults, **overrides}
    assert set(row) == set(columns), set(row) ^ set(columns)
    return "(" + ", ".join(row[column] for column in columns) + ")"


def _store_row(address_id: str, **overrides: str) -> str:
    return _row(
        _STORE_DEFAULTS, STORE_COLUMNS, address_id=f"'{address_id}'", **overrides
    )


def _reference(md5: str, snapshot: datetime, run: str, matched_at: datetime) -> dict[str, str]:
    """One weekly matcher pass: its OSM reference, its provenance and its append instant."""
    return {
        "reference_md5": f"'{md5}'",
        "source_md5": f"'{md5}'",
        "source_snapshot_at": _literal(snapshot),
        "source_retrieved_at": _literal(snapshot),
        "geocode_run_id": f"'{run}'",
        "matched_at": _literal(matched_at),
    }


def _geocoded(status: str, latitude: float, longitude: float) -> dict[str, str]:
    """A row that DID geocode: coordinates present and the precision its status implies."""
    return {
        "match_status": f"'{status}'",
        "candidate_count": "1",
        "candidate_record_ids": "['osm/way/1']",
        "candidate_record_urls": "['https://www.openstreetmap.org/way/1']",
        "match_method": "'country_street_house_exact_unique'",
        "match_confidence": "1.0",
        "latitude": str(latitude),
        "longitude": str(longitude),
        "geocode_precision": f"'{PRECISION_BY_STATUS[status]}'",
        "coordinate_method": "'osm_record'",
        "coordinate_supporting_point_count": "1",
        "source_record_id": "'osm/way/1'",
        "source_record_url": "'https://www.openstreetmap.org/way/1'",
    }


def _adopted(latitude: float, longitude: float) -> dict[str, str]:
    """The import's own shape: a legacy exact, attributed to the matcher that decided it."""
    return _geocoded("matched_exact", latitude, longitude) | {
        "match_method": f"'{LEGACY_ADOPTED_MATCH_METHOD}'",
        "candidate_record_ids": "[]",
        "candidate_record_urls": "[]",
    }


WEEK_1 = _reference(MD5_1, SNAPSHOT_1, RUN_1, T_1)
WEEK_2 = _reference(MD5_2, SNAPSHOT_2, RUN_2, T_2)
WEEK_3 = _reference(MD5_3, SNAPSHOT_3, RUN_3, T_3)
WEEK_4 = _reference(MD5_4, SNAPSHOT_4, RUN_4, T_4)
IMPORT = _reference(MD5_LEGACY, SNAPSHOT_LEGACY, RUN_IMPORT, T_IMPORT) | {
    "policy_version": f"'{LEGACY_ADOPTED_POLICY_VERSION}'",
}
# A resolver pass whose append instant IS the import's, to the millisecond.
TIE = _reference(MD5_TIE, SNAPSHOT_TIE, RUN_TIE, T_IMPORT)

# The store as the fixture leaves it: eighteen rows over ten identities, every one of which
# carries a resolver row -- so the resolver read returns ten, sorted '1'*64 .. '9'*64 then
# 'b'*64.
FIXTURE_STORE_ROWS = (
    _store_row(SETTLED, **WEEK_1, **_geocoded("matched_exact", 59.30, 18.00)),
    _store_row(RETRIED, **WEEK_1),
    _store_row(RETRIED, **WEEK_2, **_geocoded("matched_exact", 59.31, 18.01)),
    _store_row(REGRESSED, **WEEK_1, **_geocoded("matched_exact", 59.32, 18.02)),
    _store_row(REGRESSED, **WEEK_2),
    _store_row(ADOPTED, **IMPORT, **_adopted(59.34, 18.04)),
    _store_row(ADOPTED, **WEEK_4),
    _store_row(RECLAIMED, **WEEK_1),
    _store_row(RECLAIMED, **IMPORT, **_adopted(59.35, 18.05)),
    _store_row(RECLAIMED, **WEEK_3, **_geocoded("matched_exact", 59.36, 18.06)),
    # Two rows, one instant: `IMPORT` and `TIE` both stamp T_IMPORT.
    _store_row(TIED, **IMPORT, **_adopted(59.43, 18.13)),
    _store_row(TIED, **TIE, **_geocoded("matched_exact", 59.44, 18.14)),
    _store_row(DEMOTED_STREET, **IMPORT, **_adopted(59.37, 18.07)),
    _store_row(DEMOTED_STREET, **WEEK_3, **_geocoded("matched_street", 59.38, 18.08)),
    _store_row(DEMOTED_AREA, **IMPORT, **_adopted(59.39, 18.09)),
    _store_row(DEMOTED_AREA, **WEEK_3, **_geocoded("matched_area", 59.40, 18.10)),
    _store_row(SWALLOWED, **WEEK_3, **_geocoded("matched_exact", 59.41, 18.11)),
    _store_row(RETIRED, **WEEK_1, **_geocoded("matched_exact", 59.42, 18.12)),
)


def _insert(table: str, columns: tuple[str, ...], rows: tuple[str, ...]) -> str:
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n" + ",\n".join(rows) + ";"


def _schema_statements() -> list[str]:
    """CREATE/ALTER TABLE statements for NEEDED_TABLES only, in migration order."""
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if not statement:
                continue
            if statement.upper().startswith("CREATE DATABASE"):
                statements.append(statement)
                continue
            match = _TABLE_RE.match(statement)
            if match and match.group(1) in NEEDED_TABLES:
                statements.append(statement)
    return statements

# All ten stored identities carry a resolver row, so the resolver read returns ten, sorted
# '1'*64 .. '9'*64 then 'b'*64 (CHURNED, 'a'*64, is unstored). At page size three the pages
# are 3+3+3+1 and the cursor after each full page is its last identity -- the 3rd, 6th and
# 9th in that order. A wrong cursor would drop or repeat an identity and the union would stop
# matching the baseline, so these are checked BY the parity assertion, not merely trusted.
PAGE_SIZE = 3
CURSORS = (None, REGRESSED, DEMOTED_STREET, RETIRED)

_BASELINE = "baseline"


def _baseline_sql() -> str:
    """The single-query resolver read this module replaced, wrapped so its order is ours."""
    inner = build_current_resolver_geocodes_sql(columns=PREVIOUS_OUTCOME_COLUMNS)
    projection = ", ".join(PREVIOUS_OUTCOME_COLUMNS)
    return f"SELECT {projection} FROM (\n{inner}\n) AS baseline ORDER BY address_id"


def _page_sql(cursor: str | None) -> str:
    """One keyset page exactly as the loader issues it, with the cursor inlined for the CLI."""
    sql = _outcome_page_sql(has_cursor=cursor is not None)
    if cursor is None:
        return sql
    return _render(sql, {"after_address_id": cursor})


def _marked(label: str, query: str) -> str:
    return f"SELECT '@@{label}';\n{query.rstrip().rstrip(';')} FORMAT TSV;"


def _script() -> str:
    parts = [f"{statement};" for statement in _schema_statements()]
    # A background merge would collapse ReplacingMergeTree parts and could turn the read into
    # an accident; the store harness stops merges for the same reason.
    parts.append(f"SYSTEM STOP MERGES {STORE};")
    parts.append(_insert(STORE, STORE_COLUMNS, FIXTURE_STORE_ROWS))
    parts.append(_marked(_BASELINE, _baseline_sql()))
    for index, cursor in enumerate(CURSORS):
        parts.append(_marked(f"page_{index}", _page_sql(cursor)))
    return "\n".join(parts) + "\n"


@pytest.fixture(scope="module")
def sections() -> dict[str, list[list[str]]]:
    command = _clickhouse_local_command()
    # PAGE_SIZE reaches the SQL through _outcome_page_sql's `LIMIT {QUERY_BATCH_SIZE}`.
    original = geocode_demand.QUERY_BATCH_SIZE
    geocode_demand.QUERY_BATCH_SIZE = PAGE_SIZE
    try:
        completed = subprocess.run(
            command,
            input=_script(),
            capture_output=True,
            text=True,
            timeout=900,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    finally:
        geocode_demand.QUERY_BATCH_SIZE = original
    assert completed.returncode == 0, completed.stderr or completed.stdout
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in completed.stdout.splitlines():
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        elif current and line.strip():
            result[current].append(line.split("\t"))
    return result


def test_the_pages_union_to_the_single_query_row_set(
    sections: dict[str, list[list[str]]],
) -> None:
    baseline = sections[_BASELINE]
    pages = [sections[f"page_{index}"] for index in range(len(CURSORS))]

    # Page sizes: the keyset walk fills every page but the last (3+3+3+1 over ten identities).
    assert [len(page) for page in pages] == [3, 3, 3, 1]

    union = [row for page in pages for row in page]
    # Row-for-row parity with the read this module replaced -- same rows, same order.
    assert union == baseline
    # And the boundaries themselves: no identity dropped, none repeated across the four pages.
    identities = [row[0] for row in union]
    assert identities == [row[0] for row in baseline]
    assert len(identities) == len(set(identities)) == len(baseline)


def test_the_baseline_read_excludes_the_adopted_family(
    sections: dict[str, list[list[str]]],
) -> None:
    """Guards the fixture, not the pagination: the read is the RESOLVER view, so no row
    carries the imported legacy_adopted_v1 policy. ADOPTED/RECLAIMED/TIED appear through
    their resolver rows only."""
    policy_column = PREVIOUS_OUTCOME_COLUMNS.index("policy_version")
    for row in sections[_BASELINE]:
        assert row[policy_column] != "legacy_adopted_v1"
