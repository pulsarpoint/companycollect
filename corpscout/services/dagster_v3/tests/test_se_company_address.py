"""The se_company_address final asset: change detection, the geocode read, set
replacement and the wiring (jobs, sensor, schedule, freshness leaves).

The ClickHouse-facing helpers are asserted as SQL text (this repo has no live
ClickHouse in CI); the resolution loop is exercised end-to-end through
``materialize_se_company_address`` with the scripted fake client from
``test_se_company_common``.
"""

import json
import re
import uuid
from datetime import UTC, datetime

import dagster as dg
import pytest

from dagster_v3.defs.se_company.address import (
    ARTIFACT_COLUMNS,
    GEOCODE_ADDRESS_FILTER_SQL,
    GEOCODE_COLUMNS,
    GEOCODE_PROJECTION,
    INSERT_COLUMNS,
    METRIC_KEYS,
    PUBLISHED_COLUMNS,
    PUBLISHED_PROJECTION,
    SE_COMPANY_ADDRESS_CORRECTION,
    SELECTION_REASONS,
    build_artifact_rows_sql,
    build_changed_companies_sql,
    build_geocodes_sql,
    build_published_rows_sql,
    materialize_se_company_address,
)
from dagster_v3.defs.se_company.address_rules import address_components, address_key_for
from dagster_v3.defs.se_company.common import build_ledger_sql
from dagster_v3.defs.sweden_company.geocode_store import build_current_geocodes_sql
from tests.se_company_ddl import declared_columns

EPOCH_SQL = "toDateTime64('1970-01-01 00:00:00', 3, 'UTC')"
PUBLISHED_AT_SQL = f"ifNull(published.resolved_at, {EPOCH_SQL})"

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)
GEOCODED_AT = datetime(2026, 8, 20, 3, 15, tzinfo=UTC)
COMPANY = "5565200028"
OTHER_COMPANY = "5560125220"

# assert_clickhouse_tables_exist runs its own SELECT against system.tables first, so every
# scripted answer list starts with the tables it asks about.
EXISTING_TABLES = [
    (table,)
    for table in (
        "se_company_address_bolagsverket",
        "se_company_address_scb",
        "se_company_address",
        "se_company_address_correction",
        "se_company_address_members_current",
        "se_company_address_links_current",
        "se_address_geocodes",
    )
]

BOLAGSVERKET_VALUES = {
    "address_type": "postal",
    "address_fingerprint": "fingerprint-postal",
    "care_of": "",
    "street_address": "Storgatan 1",
    "normalized_address": "storgatan 1, 111 22 stockholm, se",
    "postal_code": "111 22",
    "city": "Stockholm",
    "country_code": "SE",
}
SCB_VALUES = {
    **BOLAGSVERKET_VALUES,
    "address_type": "visiting_or_postal",
    "address_fingerprint": "fingerprint-visiting",
    "city": "STOCKHOLM",
}
POSTAL_KEY = address_key_for(address_components(BOLAGSVERKET_VALUES))
VISITING_KEY = address_key_for(address_components(SCB_VALUES))
VANISHED_KEY = "d" * 64


def _artifact_row(source: str, uid: str, evidence_hash: str, values: dict) -> tuple:
    """One row of build_artifact_rows_sql: every payload value arrives as text."""
    return (source, COMPANY, uid, evidence_hash, NOW, json.dumps(values))


ARTIFACT_ROWS = [
    _artifact_row("bolagsverket", "bv:1", "a" * 64, BOLAGSVERKET_VALUES),
    _artifact_row("scb", "scb:1", "b" * 64, SCB_VALUES),
]
# build_geocodes_sql projects every column as text; only the Bolagsverket observation
# ever reached the shared-identity chain.
GEOCODE_ROWS = [
    (COMPANY, "fingerprint-postal", "c" * 64, 1, "59.3326", "18.0649", "exact",
     "2026-08-20 03:15:00.000"),
]
# One published row for a key neither source produces any more -- the tombstone case.
PUBLISHED_ROWS = [
    (COMPANY, VANISHED_KEY, "postal", None, "Gamla vagen 9", "gamla vagen 9, 222 22 lund, se",
     "222 22", "Lund", "SE", True, ["bolagsverket"], ["bv:0"], ["e" * 64]),
]


def _where_disjuncts(sql: str) -> list[str]:
    """The scan's WHERE, split into its OR-ed terms. Parsed out of the block rather than
    substring-matched, so a DELETED term is caught -- a bare `assert term in sql` cannot
    tell the WHERE apart from the reasons projection, which spells the same expressions."""
    block = re.search(r"\nWHERE \(\n(.*?)\n      \)\n", sql, re.DOTALL)
    assert block is not None, "the scan's WHERE block moved -- this parser needs updating"
    return [term.strip() for term in re.split(r"\n\s+OR ", block.group(1))]


def _projected_reasons(sql: str) -> list[tuple[str, str]]:
    """(alias, expression) for every reason the scan projects, in projection order."""
    block = re.search(r"SELECT artifacts\.company_id AS company_id,\n(.*?)\nFROM artifacts",
                      sql, re.DOTALL)
    assert block is not None, "the scan's projection moved -- this parser needs updating"
    pairs = []
    for line in block.group(1).split(",\n"):
        expression, alias = line.strip().rsplit(" AS ", 1)
        pairs.append((alias.strip(), expression.strip()))
    return pairs


def _selected(company_id: str, **flags: int) -> tuple:
    """One change-scan row: the company id followed by its reason flags, in the order
    build_changed_companies_sql projects them. Written positionally from SELECTION_REASONS
    rather than by hand, so a new reason shifts every scripted row at once instead of
    silently misaligning the metadata counts the loop reads by position."""
    unknown = set(flags) - set(SELECTION_REASONS)
    assert not unknown, f"not scan reasons: {sorted(unknown)}"
    return (company_id, *(int(flags.get(name, 0)) for name in SELECTION_REASONS))


def _final_rows(client) -> list[tuple]:
    """Every row staged for the final table, in insert order."""
    rows = []
    for sql, params in client.executed:
        if re.match(r"^INSERT INTO `corpscout`\.`_tmp_se_company_address_[0-9a-f]{32}`", sql):
            rows.extend(params)
    return rows


def _published(rows: list[tuple]) -> dict[str, dict]:
    """Staged final rows keyed by address_key, each as a column-name -> value map."""
    return {
        str(row[INSERT_COLUMNS.index("address_key")]): dict(zip(INSERT_COLUMNS, row, strict=True))
        for row in rows
    }


def test_insert_columns_are_the_final_ddl_minus_the_materialized_hash() -> None:
    assert list(INSERT_COLUMNS) == [
        column for column in declared_columns("se_company_address") if column != "evidence_set_hash"
    ]


def test_every_where_disjunct_of_the_change_scan_is_pinned_exactly() -> None:
    """WHAT SELECTS a company, term by term, as an exact list.

    Substring asserts cannot do this job: every one of these expressions is ALSO spelled in
    the reasons projection (deliberately -- they come from one Python constant), so
    `assert term in sql` stays green when the term is deleted from the WHERE and the scan
    silently stops selecting on it. Parsing the WHERE block and comparing the whole list
    catches a deletion, an addition, a reordering and a flipped comparison at once.

    Every LEFT JOIN miss is read through ifNull. Bare comparisons work only while
    join_use_nulls = 0; under 1 a miss is NULL, the WHERE is NULL for every never-published
    company, and the scan returns zero rows -- the pipeline would silently stop resolving.
    """
    assert _where_disjuncts(build_changed_companies_sql()) == [
        # never published
        "ifNull(published.company_id, '') = ''",
        # a rules-only pass, and the CUTOFF that gives it memory. The direction is load
        # bearing: `<` skips a company this pass has already rewritten, `>` would skip
        # exactly the ones it still owes and the pass would never finish.
        f"(%(resolve_all)s = 1 AND {PUBLISHED_AT_SQL} < "
        "parseDateTime64BestEffort(%(resolve_all_before)s, 3, 'UTC'))",
        # evidence newer than the published resolution
        f"artifacts.latest_observed_at > {PUBLISHED_AT_SQL}",
        # the geocode store gained a newer outcome for it
        f"ifNull(geocodes.latest_geocoded_at, {EPOCH_SQL}) > {PUBLISHED_AT_SQL}",
        # the correction ledger gained a row after it
        f"ifNull(ledger.latest_correction_at, {EPOCH_SQL}) > {PUBLISHED_AT_SQL}",
    ]


def test_the_change_scan_projects_every_reason_in_the_order_the_loop_counts_them() -> None:
    """The reasons projection and SELECTION_REASONS are read POSITIONALLY (row[offset]), so
    the two orderings have to be one ordering -- swapping two lines in the SQL alone would
    transpose every count in the metadata with nothing else looking wrong. Each alias's
    expression is pinned too, so a reason cannot be fed by another reason's predicate."""
    sql = build_changed_companies_sql()
    assert SELECTION_REASONS == ("never_published", "new_evidence_bolagsverket",
                                 "new_evidence_scb", "new_geocode", "ledger_pending")
    assert _projected_reasons(sql) == [
        ("never_published", "ifNull(published.company_id, '') = ''"),
        ("new_evidence_bolagsverket", f"artifacts.bolagsverket_observed_at > {PUBLISHED_AT_SQL}"),
        ("new_evidence_scb", f"artifacts.scb_observed_at > {PUBLISHED_AT_SQL}"),
        ("new_geocode", f"ifNull(geocodes.latest_geocoded_at, {EPOCH_SQL}) > {PUBLISHED_AT_SQL}"),
        ("ledger_pending", f"ifNull(ledger.latest_correction_at, {EPOCH_SQL}) > {PUBLISHED_AT_SQL}"),
    ]
    # Per-source freshness needs the union to carry which artifact each maximum came from.
    for source in ("bolagsverket", "scb"):
        assert f"maxIf(source_observed_at, source = '{source}') AS {source}_observed_at" in sql
    # One page per call: the LIMIT is the page size, not the whole run's cap.
    assert "AND artifacts.company_id > %(after_company_id)s" in sql
    assert "ORDER BY artifacts.company_id" in sql and "LIMIT %(page_size)s" in sql


def test_the_change_scan_needs_final_only_on_the_final_table() -> None:
    """observed_at and matched_at ARE the version columns, so max() over the raw parts is
    already the newest; the final is keyed by (company_id, address_key) and appends a row
    per resolution, but max(resolved_at) per company is likewise version-safe."""
    sql = build_changed_companies_sql()
    assert "FROM corpscout.se_company_address_bolagsverket GROUP BY company_id" in sql
    assert "FINAL" not in sql


def test_the_geocode_cte_reads_the_store_and_does_not_alias_a_table_with_its_own_name() -> None:
    """Self-shadowing a WITH name inside its own body is analyzer-dependent: the outer
    ``geocodes.latest_geocoded_at`` reads the CTE, so the joined table must be called
    something else or the two names are one identifier with two meanings.

    The CTE reads the store RAW -- max(matched_at) over every stored row for the identity,
    not over the versioned read's chosen row. That is deliberate and is argued in
    build_changed_companies_sql's docstring: the raw maximum is always greater than or equal
    to the current outcome's, so the scan can over-select but can never MISS a company whose
    served coordinate moved. Ranking 2.09M identities on every scan page to avoid an
    occasional harmless re-resolution would be the wrong trade.
    """
    sql = build_changed_companies_sql()
    assert "corpscout.se_address_geocodes AS geocodes" not in sql
    assert "corpscout.se_address_geocodes AS points\n" in sql
    assert "max(points.matched_at) AS latest_geocoded_at" in sql
    assert "se_address_geocodes_current" not in sql
    # Raw, not ranked: no second copy of the read rule lives in this module.
    assert "LIMIT 1 BY" not in sql


def test_the_geocode_query_reads_the_versioned_read_and_gates_every_joined_column() -> None:
    sql = build_geocodes_sql()
    assert "toUInt8(ifNull(geocodes.geocode_run_id, '') != '') AS has_geocode" in sql
    assert "INNER JOIN corpscout.se_company_address_links_current AS links" in sql
    assert "toString(members.address_key) AS address_fingerprint" in sql
    # The geocode side is the store's ONE read rule, pulled in whole -- byte-identical to
    # what geocode_store builds, so this module cannot drift into a second ranking.
    expected_read = build_current_geocodes_sql(
        columns=("address_id", "match_status", "latitude", "longitude", "matched_at",
                 "geocode_run_id"),
        address_filter_sql=GEOCODE_ADDRESS_FILTER_SQL)
    assert f"LEFT JOIN (\n{expected_read}\n) AS geocodes ON geocodes.address_id = links.address_id" in sql
    # The filter prunes the store on its sorting key's leading column before ranking, so a
    # page of companies never pays for the whole store.
    #
    # Pinned WHOLE, not just at its opening. It is spliced in as the INNER read's WHERE with
    # no parentheses around it, so an `AND <anything>` welded onto the end would narrow
    # which rows enter the rank -- a different served outcome, silently, with no syntax
    # error anywhere.
    #
    # The clause that holds that shut is the VOCABULARY one below. A weld ends at a closing
    # paren just as happily as the constant does -- `AND match_status IN (...)` does -- so
    # startswith and endswith fix the SHAPE and nothing more; what a weld cannot do is avoid
    # naming a column outside {address_id, company_id}.
    #
    # The behavioural layer is tests/test_se_company_address_clickhouse_local.py, and the two
    # are PARTNERS rather than substitutes. Measured, both ways: welding
    # `AND policy_version != 'legacy_adopted_v1'` onto the constant kills four of that
    # harness's tests (the adopted coordinate stops being served) and this pin as well, while
    # welding `AND match_status IN ('matched_exact')` leaves the harness entirely green --
    # the one ambiguous row in its fixture is already outranked by stage 2, so dropping it
    # from the rank changes no answer there. That weld dies here and nowhere else.
    filter_sql = GEOCODE_ADDRESS_FILTER_SQL.strip()
    assert filter_sql.startswith("address_id IN (")
    assert filter_sql.endswith(")")
    assert "%(company_ids)s" in GEOCODE_ADDRESS_FILTER_SQL
    vocabulary = filter_sql.replace("%(company_ids)s", "?").replace(
        "corpscout.se_company_address_links_current", "")
    assert set(re.findall(r"[a-z_][a-z_0-9]*", vocabulary)) == {"address_id", "company_id"}
    # Nullable source columns are ifNull'd, never gated; joined non-Nullable ones are gated.
    assert "ifNull(toString(geocodes.latitude), '') AS latitude" in sql
    assert "toString(geocodes.match_status), '') AS geocode_status" in sql
    assert re.findall(r"AS (\w+)", sql[: sql.index("\nFROM ")]) == list(GEOCODE_COLUMNS)
    # Every alias, fed by its own source column -- all eight, unchanged by the repoint. An
    # alias wired to a neighbour would transpose the fact silently, and half of these are
    # same-typed text, so nothing downstream would notice. The map is exhaustive by
    # assertion, not by good intentions.
    sources = {
        "company_id": "members.company_id",
        # members.address_key IS the source observation's address_fingerprint -- the whole
        # reason the artifacts carry it.
        "address_fingerprint": "members.address_key",
        "address_id": "links.address_id",
        # The hit flag is the geocoder's RUN id, not a coordinate: an address can be
        # classified (unmatched, foreign, postal-box) without a point.
        "has_geocode": "geocodes.geocode_run_id",
        "latitude": "geocodes.latitude",
        "longitude": "geocodes.longitude",
        "geocode_status": "geocodes.match_status",
        "geocoded_at": "geocodes.matched_at",
    }
    assert set(sources) == set(GEOCODE_COLUMNS)
    assert len(sources) == 8
    for column, expression in GEOCODE_PROJECTION:
        assert sources[column] in expression, (column, expression)


def test_the_artifact_read_contract_names_its_columns() -> None:
    sql = build_artifact_rows_sql()
    for source in ("bolagsverket", "scb"):
        assert f"'{source}' AS source" in sql
        assert f"FROM corpscout.se_company_address_{source} FINAL" in sql
    assert "'address_fingerprint', ifNull(toString(address_fingerprint), '')" in sql
    assert "*" not in sql
    # The projection IS ARTIFACT_COLUMNS, in order, in every branch of the UNION -- the row
    # mapper reads it by that name list, so a branch aliasing them differently would build
    # a transposed ArtifactRow (and ClickHouse binds a UNION by position, not by name).
    for branch in sql.split("\nUNION ALL\n"):
        assert re.findall(r"AS (\w+)", branch[: branch.index("\nFROM ")]) == list(ARTIFACT_COLUMNS)


def test_published_rows_are_read_final_and_carry_the_tombstone_flag() -> None:
    sql = build_published_rows_sql()
    assert "FROM corpscout.se_company_address AS published FINAL" in sql
    assert "published.is_current AS is_current" in sql
    assert "WHERE published.company_id IN %(company_ids)s" in sql
    # The projection IS PUBLISHED_COLUMNS, in order. Reordering the pair list moves both
    # the SQL and the mapper together and is therefore harmless; what is NOT harmless is
    # an alias fed by the wrong column, so each expression must name its own.
    assert re.findall(r"AS (\w+)", sql[: sql.index("\nFROM ")]) == list(PUBLISHED_COLUMNS)
    for column, expression in PUBLISHED_PROJECTION:
        assert re.search(rf"\bpublished\.{column}\b", expression), (column, expression)
    # The geocode columns are deliberately absent: a tombstone republishes the address,
    # not a coordinate this resolution did not verify.
    for column in ("latitude", "longitude", "geocode_status", "geocoded_at", "address_id"):
        assert column not in PUBLISHED_COLUMNS


def test_a_preview_selects_companies_but_reads_nothing_else_and_writes_nothing() -> None:
    """A bare Materialize click in the Dagster UI carries no config at all.

    The scan page is deliberately NON-EMPTY: with an empty page the loop breaks before the
    execute gate is ever reached, so the test would pass with the gate deleted -- it would
    be asserting nothing about the gate at all.
    """
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    client = FakeClient(answers=[
        EXISTING_TABLES,
        [_selected(COMPANY, never_published=1, new_geocode=1)],
    ])
    metadata = materialize_se_company_address(
        clickhouse=FakeClickhouse(client), source_run_id="run-1", resolved_at=NOW,
        company_ids=[], max_companies=10, company_batch_size=10, execute=False, log=None)

    # It really did select -- and reported why.
    assert metadata["preview"] is True and metadata["selected_company_count"] == 1
    assert metadata["never_published"] == 1 and metadata["new_geocode"] == 1
    assert metadata["ledger_pending"] == 0 and metadata["new_evidence_scb"] == 0

    statements = [sql for sql, _ in client.executed]
    # ... and then did nothing else: the table check and the scan, in that order, full stop.
    assert len(statements) == 2, statements
    assert "system.tables" in statements[0]
    assert statements[1] == build_changed_companies_sql()
    for write in ("INSERT", "CREATE", "DROP", "ALTER", "TRUNCATE"):
        assert not any(write in statement for statement in statements), write
    for read in (build_artifact_rows_sql(), build_geocodes_sql(), build_published_rows_sql(),
                 build_ledger_sql(SE_COMPANY_ADDRESS_CORRECTION)):
        assert read not in statements
    # An execute run that resolved nothing still returns the same metadata SHAPE.
    for key in (*SELECTION_REASONS, *METRIC_KEYS):
        assert key in metadata


def test_an_execute_run_that_selects_nothing_still_reports_every_counter() -> None:
    """A defaultdict returns 0 on access but serialises only the keys someone touched, so a
    quiet run would otherwise hand the backoffice a DIFFERENT metadata shape from a busy
    one and every reader would have to guess whether a missing key means zero."""
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    client = FakeClient(answers=[EXISTING_TABLES, []])
    metadata = materialize_se_company_address(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[COMPANY], max_companies=10, company_batch_size=10, execute=True, log=None)

    assert "preview" not in metadata
    for key in (*SELECTION_REASONS, *METRIC_KEYS):
        assert metadata[key] == 0, key
    assert METRIC_KEYS == ("selected_company_count", "address_count", "tombstone_count",
                           "geocoded_count", "applied_correction_count",
                           "stale_correction_count", "inserted_count", "total_count")


def test_a_resolution_publishes_the_whole_set_the_geocode_and_the_tombstone() -> None:
    """One company, two sources, one geocoded observation and one address that left.

    Every position of every published tuple is asserted by NAME: AddressOutcome's
    dataclass field order is not the DDL's, so a positional _final_row would transpose
    same-typed columns with an otherwise-green suite.
    """
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    client = FakeClient(answers=[
        EXISTING_TABLES,                    # assert_clickhouse_tables_exist
        [_selected(COMPANY, never_published=1, new_evidence_bolagsverket=1)],
        ARTIFACT_ROWS,                      # artifact rows
        GEOCODE_ROWS,                       # geocodes
        PUBLISHED_ROWS,                     # already-published rows
        [],                                 # ledger
        [(3, 0)],                           # final stage validation
        [(1,)],                             # target row count before the insert
        [(4,)],                             # target row count after the insert
    ])
    metadata = materialize_se_company_address(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[COMPANY], max_companies=10, company_batch_size=10, execute=True, log=None)

    assert metadata["selected_company_count"] == 1
    assert metadata["never_published"] == 1 and metadata["new_evidence_bolagsverket"] == 1
    assert metadata["address_count"] == 2 and metadata["tombstone_count"] == 1
    assert metadata["geocoded_count"] == 1 and metadata["inserted_count"] == 3

    rows = _published(_final_rows(client))
    assert set(rows) == {POSTAL_KEY, VISITING_KEY, VANISHED_KEY}
    assert rows[POSTAL_KEY] == {
        "company_id": COMPANY, "address_key": POSTAL_KEY, "address_type": "postal",
        "care_of": None, "street_address": "Storgatan 1",
        "normalized_address": "storgatan 1, 111 22 stockholm, se",
        "postal_code": "111 22", "city": "Stockholm", "country_code": "SE",
        # The augmentation, looked up by this observation's own address_fingerprint.
        "address_id": "c" * 64, "latitude": 59.3326, "longitude": 18.0649,
        "geocode_status": "exact", "geocoded_at": GEOCODED_AT, "is_current": True,
        "sources": ["bolagsverket"], "source_record_uids": ["bv:1"],
        "evidence_hashes": ["a" * 64], "correction_ids": [],
        "source_run_id": "run", "resolved_at": NOW,
    }
    # SCB's observation never reached the address identity chain, so it publishes with no
    # coordinate rather than borrowing the other address's.
    assert rows[VISITING_KEY] | {
        "address_type": "visiting_or_postal", "city": "STOCKHOLM",
        "sources": ["scb"], "source_record_uids": ["scb:1"], "evidence_hashes": ["b" * 64],
        "address_id": None, "latitude": None, "longitude": None,
        "geocode_status": "", "geocoded_at": None, "is_current": True,
    } == rows[VISITING_KEY]
    # The tombstone republishes the vanished row's own provenance with is_current false,
    # and claims no geocode this resolution did not look up.
    assert rows[VANISHED_KEY] | {
        "is_current": False, "sources": ["bolagsverket"], "source_record_uids": ["bv:0"],
        "evidence_hashes": ["e" * 64], "correction_ids": [], "street_address": "Gamla vagen 9",
        "address_id": None, "latitude": None, "longitude": None, "geocoded_at": None,
        "resolved_at": NOW,
    } == rows[VANISHED_KEY]


@pytest.mark.parametrize("ungeocoded_first", [True, False])
def test_the_coordinate_survives_a_duplicate_row_for_the_same_fingerprint(ungeocoded_first) -> None:
    """One fingerprint can reach the chain under several canonical addresses (the member
    bridge keys on source and type too), so this page can hand the same observation two
    facts. Whichever order they arrive in, the one that says most has to win -- keeping the
    last would drop the coordinate half the time, and nothing downstream would look wrong.
    """
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    linked_only = (COMPANY, "fingerprint-postal", "f" * 64, 0, "", "", "", "")
    duplicates = [linked_only, *GEOCODE_ROWS] if ungeocoded_first else [*GEOCODE_ROWS, linked_only]
    client = FakeClient(answers=[
        EXISTING_TABLES, [_selected(COMPANY, new_geocode=1)],
        ARTIFACT_ROWS, duplicates, [], [], [(2, 0)], [(0,)], [(2,)],
    ])
    materialize_se_company_address(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[COMPANY], max_companies=10, company_batch_size=10, execute=True, log=None)

    postal = _published(_final_rows(client))[POSTAL_KEY]
    assert (postal["latitude"], postal["longitude"]) == (59.3326, 18.0649)
    assert postal["geocode_status"] == "exact" and postal["address_id"] == "c" * 64


def test_the_page_log_line_reports_the_page_not_the_running_total() -> None:
    """A line labelled "page:" that prints cumulative counters reads as a page that grew
    every time -- which is exactly how a resolution bug hides in a long run's logs."""
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    def _rows_for(company_id: str) -> list[tuple]:
        return [(source, company_id, uid, evidence_hash, NOW, json.dumps(values))
                for source, uid, evidence_hash, values in (
                    ("bolagsverket", f"bv:{company_id}", "a" * 64, BOLAGSVERKET_VALUES),
                    ("scb", f"scb:{company_id}", "b" * 64, SCB_VALUES))]

    # company_batch_size 1 splits the scope into two single-company chunks, so the loop
    # resolves two separate pages and logs twice.
    page = lambda company_id: [  # noqa: E731 - a scripted answer block, not a helper
        [_selected(company_id, never_published=1)], _rows_for(company_id), [], [], [],
        [(2, 0)], [(0,)], [(2,)], [],
    ]
    client = FakeClient(answers=[EXISTING_TABLES, *page(COMPANY), *page(OTHER_COMPANY)])
    logged: list[tuple] = []
    metadata = materialize_se_company_address(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[COMPANY, OTHER_COMPANY], max_companies=10, company_batch_size=1,
        execute=True, log=lambda *args: logged.append(args))

    pages = [args for args in logged if "se_company_address page:" in args[0]]
    assert len(pages) == 2
    # companies, rows, addresses, tombstones, geocoded, corrections, stale -- per page.
    assert pages[0][1:] == (1, 2, 2, 0, 0, 0, 0)
    assert pages[1][1:] == (1, 2, 2, 0, 0, 0, 0)  # NOT (1, 2, 4, ...) -- the run's total
    # ... while the metadata IS the running total of both pages.
    assert metadata["address_count"] == 4 and metadata["selected_company_count"] == 2


def test_a_reject_correction_publishes_the_address_as_not_current() -> None:
    """The ledger runs before the set replacement, so a rejected key is its own tombstone:
    one row per key per resolution, carrying the correction that decided it."""
    from dagster_v3.defs.se_company.address_rules import ZERO_HASH
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    correction_id = uuid.UUID(int=7)
    client = FakeClient(answers=[
        EXISTING_TABLES,
        [_selected(COMPANY, ledger_pending=1)],
        ARTIFACT_ROWS,
        [],   # no geocodes
        [],   # nothing published yet
        [(correction_id, COMPANY, "reject_address", json.dumps({"address_key": POSTAL_KEY}),
          ZERO_HASH, None, NOW)],
        [(2, 0)], [(0,)], [(2,)],
    ])
    metadata = materialize_se_company_address(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[COMPANY], max_companies=10, company_batch_size=10, execute=True, log=None)

    assert metadata["applied_correction_count"] == 1 and metadata["stale_correction_count"] == 0
    assert metadata["address_count"] == 1 and metadata["tombstone_count"] == 1
    rows = _published(_final_rows(client))
    assert rows[POSTAL_KEY]["is_current"] is False
    assert rows[POSTAL_KEY]["correction_ids"] == [correction_id]
    assert rows[VISITING_KEY]["is_current"] is True


def test_an_explicit_scope_is_chunked_so_the_rendered_query_stays_under_max_query_size() -> None:
    """The scan embeds %(company_ids)s four times and clickhouse-driver substitutes them
    client-side, so a scope larger than company_batch_size is paged chunk by chunk."""
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    scope = [str(5560000000 + index) for index in range(5)]
    client = FakeClient(answers=[EXISTING_TABLES, [], [], []])
    materialize_se_company_address(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=scope, max_companies=10, company_batch_size=2, execute=False, log=None)

    scans = [params for sql, params in client.executed if sql.startswith("WITH artifacts AS (")]
    assert [len(params["company_ids"]) for params in scans] == [2, 2, 1]
    assert all(params["all_companies"] == 0 for params in scans)


def test_the_cap_rather_than_exhaustion_is_reported_when_a_full_page_uses_it_up() -> None:
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    client = FakeClient(answers=[EXISTING_TABLES, [_selected(COMPANY, never_published=1)]])
    metadata = materialize_se_company_address(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[], max_companies=1, company_batch_size=1, execute=False, log=None)

    assert metadata["stopped_at_cap"] is True and metadata["selected_company_count"] == 1


def test_the_config_gates_the_run_and_bounds_the_weekly_population() -> None:
    """The two bounds that decide what an automated run actually does.

    Ruling A17, restated for the store: the weekly whole-table geocode restamp that used to
    make new_geocode select every geocoded company is gone, so an ordinary week now selects
    register churn plus real geocode changes -- from the SECOND post-repoint week, the first
    one still re-selecting everything once because the backfill's copied matched_at stamps
    postdate every pre-deploy publish. The bound stays wide for the reason that did
    NOT go away -- the scan has no memory. A run stopped by max_companies restarts at the
    first company_id, so a cap below the number of companies a run selects re-resolves the
    same leading slice forever and never reaches the tail, and nothing tells the weekly
    config that number in advance. The bound therefore has to admit an effectively uncapped
    weekly run.
    """
    from dagster_v3.defs.se_company.address import SECompanyAddressConfig

    # Preview by default: an empty config (a UI "Materialize" click) resolves nothing.
    assert SECompanyAddressConfig().execute is False
    assert SECompanyAddressConfig(execute=True).execute is True
    assert SECompanyAddressConfig().company_ids == [] and SECompanyAddressConfig().resolve_all is False
    assert SECompanyAddressConfig().resolve_all_before == ""

    assert SECompanyAddressConfig().max_companies == 1_000_000
    assert SECompanyAddressConfig(max_companies=5_000_000).max_companies == 5_000_000
    for bad in (0, 5_000_001):
        with pytest.raises(ValueError):
            SECompanyAddressConfig(max_companies=bad)

    # Both the scan page size and the chunk size for an explicit scope: the scan embeds the
    # id list four times and clickhouse-driver substitutes them client-side, against
    # ClickHouse's 262,144-byte default max_query_size.
    assert build_changed_companies_sql().count("%(company_ids)s") == 4
    assert SECompanyAddressConfig().company_batch_size == 5_000
    for bad in (0, 5_001):
        with pytest.raises(ValueError):
            SECompanyAddressConfig(company_batch_size=bad)


def test_the_asset_the_jobs_the_sensor_and_the_schedule_are_wired() -> None:
    from dagster_v3.definitions import defs as load_defs
    from dagster_v3.defs.common.clickhouse_checks import CLICKHOUSE_LEAVES, WEEKLY

    repository = load_defs().get_repository_def()
    asset = repository.asset_graph.get(dg.AssetKey("se_company_address_clickhouse"))
    assert asset.parent_keys == {dg.AssetKey("se_company_address_bolagsverket_clickhouse"),
                                 dg.AssetKey("se_company_address_scb_clickhouse")}
    assert asset.group_name == "se_company"
    assert asset.metadata["table"] == "corpscout.se_company_address"

    job_keys = {key.path[-1]
                for key in repository.get_job("se_company_address_job").asset_layer.executable_asset_keys}
    assert job_keys == {"se_company_address_bolagsverket_clickhouse",
                        "se_company_address_scb_clickhouse", "se_company_address_clickhouse"}
    review_keys = {key.path[-1]
                   for key in repository.get_job("se_company_address_review_job").asset_layer.executable_asset_keys}
    assert review_keys == {"se_company_address_clickhouse"}

    sensor = repository.get_sensor_def("se_company_address_correction_sensor")
    assert sensor.job_name == "se_company_address_review_job"
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED

    schedule = repository.get_schedule_def("se_company_address_weekly")
    assert schedule.cron_schedule == "55 6 * * 1"  # offset from se_company_info_weekly's 50 6
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    # An automated run must never fall back to the asset's own defaults, which are a
    # preview. Read off an evaluated tick -- the config the daemon would actually submit.
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2026, 8, 24, 6, 55, tzinfo=UTC))
    run_requests = schedule.evaluate_tick(context).run_requests
    assert run_requests is not None and run_requests[0].run_config == {
        "ops": {"se_company_address_clickhouse": {
            "config": {"execute": True, "max_companies": 5_000_000}}}}

    # Ruling A10: all three leaves registered; freshness now tracks the weekly schedule the
    # same as the info leaves above, ahead of the schedule itself switching on.
    leaves = {leaf.asset_key: leaf for leaf in CLICKHOUSE_LEAVES}
    assert leaves["se_company_address_clickhouse"].tables == ("se_company_address",)
    assert leaves["se_company_address_bolagsverket_clickhouse"].tables == (
        "se_company_address_bolagsverket",)
    assert leaves["se_company_address_scb_clickhouse"].tables == ("se_company_address_scb",)
    assert all(leaves[key].max_age == WEEKLY for key in (
        "se_company_address_clickhouse", "se_company_address_bolagsverket_clickhouse",
        "se_company_address_scb_clickhouse"))
    # The Sweden geocode leaves moved with the tables. The legacy per-company pair retired
    # (LEGACY_PAIR_RETIREMENT_DROP_SQL), and the versioned store took its place as the
    # weekly leaf. These
    # entries name tables by ASSET KEY, far from any sweden_company import, so a grep for
    # the dropped table names does not reach them -- which is why they are pinned here.
    assert leaves["sweden_address_geocode_store_clickhouse"].tables == (
        "se_address_geocodes",)
    assert leaves["sweden_address_geocode_store_clickhouse"].max_age == WEEKLY
    assert "sweden_company_address_geocodes_clickhouse" not in leaves
    assert "sweden_company_address_geocode_results_clickhouse" not in leaves


def test_the_correction_sensor_launches_a_real_run_not_a_preview(monkeypatch) -> None:
    """A ledger row must actually re-resolve its company. Without execute in the sensor's
    run config the review job would run the scan and write nothing -- the reviewer's
    correction would sit unapplied forever, and nothing would look broken."""
    from contextlib import contextmanager

    from dagster_clickhouse import ClickhouseResource

    from dagster_v3.defs.se_company.address import se_company_address_correction_sensor
    from tests.test_se_company_common import _FakeLedgerClient

    ledger = _FakeLedgerClient()
    ledger.append(COMPANY, str(uuid.UUID(int=7)), "2026-08-24 09:00:00.000")
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self):
        yield ledger

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    context = dg.build_sensor_context(cursor=None, resources={"clickhouse": resource})
    execution_data = se_company_address_correction_sensor.evaluate_tick(context)

    assert execution_data.run_requests is not None
    # max_companies rides along from AUTOMATED_RUN_CONFIG, shared with the schedule -- it is
    # inert here since company_ids already bounds the run to the ledger's touched companies.
    assert execution_data.run_requests[0].run_config == {
        "ops": {"se_company_address_clickhouse": {
            "config": {"execute": True, "max_companies": 5_000_000,
                       "company_ids": [COMPANY]}}}}


def test_the_module_documents_the_stale_address_property_and_its_mitigation() -> None:
    """Ruling A12: a source-vanished address appends no artifact row, so nothing re-triggers
    the change scan for it and the published row stays current until a resolve_all pass.
    That is a known property, and the module has to say so where a reader will find it."""
    from dagster_v3.defs.se_company import address

    assert "resolve_all" in address.__doc__
    assert re.search(r"vanish|disappear|stops? carrying", address.__doc__, re.IGNORECASE)
