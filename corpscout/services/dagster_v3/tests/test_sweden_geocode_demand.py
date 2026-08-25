"""What the weekly resolver run is allowed to match, and nothing else.

The rule lives twice -- as DuckDB SQL over the loaded previous outcomes, and as a pure
function. Both are pinned here; the SQL half is EXECUTED against a real in-memory DuckDB,
because the thing under test is a LEFT JOIN and a CASE, and a substring test cannot tell a
correct CASE from one whose branches are in the wrong order.
"""

import re
from collections.abc import Iterator
from datetime import UTC, datetime

import duckdb
import pytest

from dagster_v3.defs.sweden_company import geocode_demand
from dagster_v3.defs.sweden_company.geocode_demand import (
    PENDING_REASONS,
    QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE,
    create_empty_previous_outcomes_table,
    fresh_reference_md5,
    load_current_resolver_outcomes,
    pending_identity_count,
    pending_reason,
    replace_pending_address_identities,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    LEGACY_ADOPTED_POLICY_VERSION,
    QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE,
    StoredOutcome,
)

POLICY = "se-address-resolution-policy-v5"
OLD_POLICY = "se-address-resolution-policy-v4"
MD5_NOW, MD5_OLD = "md5-current", "md5-previous"
T1 = datetime(2026, 8, 1, tzinfo=UTC)


def _outcome(policy: str, md5: str, status: str) -> StoredOutcome:
    return StoredOutcome(
        address_id="a" * 64,
        policy_version=policy,
        reference_md5=md5,
        match_status=status,
        matched_at=T1,
    )


@pytest.mark.parametrize(
    ("name", "outcome", "rematch_all", "expected"),
    [
        # 1. New identities. Register churn keeps its fingerprint, so an unchanged address
        #    already has an outcome and is never selected.
        ("an identity with no resolver outcome at all", None, False, "no_outcome"),
        # 2. A policy bump is a full rematch: every stored outcome carries the old version.
        (
            "a bumped policy wakes a geocoded identity",
            _outcome(OLD_POLICY, MD5_NOW, "matched_exact"),
            False,
            "policy_changed",
        ),
        (
            "a bumped policy wakes a non-geocoded identity",
            _outcome(OLD_POLICY, MD5_NOW, "ambiguous"),
            False,
            "policy_changed",
        ),
        # 3. The retry pool: non-geocoded outcomes, and only when the reference moved.
        (
            "a stale reference wakes an ambiguous",
            _outcome(POLICY, MD5_OLD, "ambiguous"),
            False,
            "reference_changed",
        ),
        (
            "a stale reference wakes an unmatched",
            _outcome(POLICY, MD5_OLD, "unmatched"),
            False,
            "reference_changed",
        ),
        (
            "a stale reference wakes an invalid_address",
            _outcome(POLICY, MD5_OLD, "invalid_address"),
            False,
            "reference_changed",
        ),
        # ... and NOT when it did not.
        (
            "an unchanged reference leaves an ambiguous alone",
            _outcome(POLICY, MD5_NOW, "ambiguous"),
            False,
            "",
        ),
        # 4. A geocoded identity at a stale reference is NOT retried. This is the whole
        #    saving: a reference bump costs the non-geocoded population, not 2.09M rows.
        (
            "a stale reference does not wake a geocoded identity",
            _outcome(POLICY, MD5_OLD, "matched_exact"),
            False,
            "",
        ),
        (
            "a settled identity is not selected",
            _outcome(POLICY, MD5_NOW, "matched_exact"),
            False,
            "",
        ),
        # 5. The explicit operator action.
        (
            "rematch_all takes precedence over everything",
            _outcome(POLICY, MD5_NOW, "matched_exact"),
            True,
            "rematch_all",
        ),
        ("rematch_all selects an identity with no outcome", None, True, "rematch_all"),
    ],
)
def test_pending_reason(
    name: str,
    outcome: StoredOutcome | None,
    rematch_all: bool,
    expected: str,
) -> None:
    assert (
        pending_reason(
            outcome,
            policy_version=POLICY,
            reference_md5=MD5_NOW,
            rematch_all=rematch_all,
        )
        == expected
    ), name
    assert expected == "" or expected in PENDING_REASONS, name


def test_an_adopted_outcome_is_not_a_resolver_outcome() -> None:
    """The demand scan reads the RESOLVER view of the store, so an adopted row is simply
    absent from its input. Spelled out here because passing the SERVED outcome in would make
    every adopted identity look settled forever and the resolver would never try it again --
    which would quietly freeze 19,413 identities at the imported answer."""
    adopted = _outcome(LEGACY_ADOPTED_POLICY_VERSION, MD5_NOW, "matched_exact")
    # If an adopted row ever reached this function it would look like a policy bump, which
    # is loud rather than silent -- but the loader is what keeps it out.
    assert (
        pending_reason(
            adopted,
            policy_version=POLICY,
            reference_md5=MD5_NOW,
            rematch_all=False,
        )
        == "policy_changed"
    )


@pytest.fixture()
def connection() -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect()
    connection.execute("create schema if not exists sweden_company_enrichment")
    connection.execute("""
        create table sweden_company_enrichment.se_addresses_current (
            address_id varchar, address_kind varchar)
    """)
    connection.execute("""
        create table sweden_company_enrichment.se_address_geocodes_previous (
            address_id varchar, policy_version varchar, reference_md5 varchar,
            match_status varchar, match_method varchar, match_confidence double,
            candidate_record_ids varchar[], matched_at timestamptz)
    """)
    yield connection
    connection.close()


def _seed(
    connection: duckdb.DuckDBPyConnection,
    rows: list[tuple[str, str | None, str | None, str | None]],
) -> None:
    for address_id, policy, md5, status in rows:
        connection.execute(
            "insert into sweden_company_enrichment.se_addresses_current"
            " values (?, 'physical')",
            [address_id],
        )
        if policy is None:
            continue
        connection.execute(
            "insert into sweden_company_enrichment.se_address_geocodes_previous"
            " values (?, ?, ?, ?, 'exact', 1.0, [], ?)",
            [address_id, policy, md5, status, T1],
        )


def test_the_pending_table_is_the_rule_executed(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    _seed(
        connection,
        [
            ("new", None, None, None),
            ("settled", POLICY, MD5_NOW, "matched_exact"),
            ("stale-geocoded", POLICY, MD5_OLD, "matched_exact"),
            ("stale-ambiguous", POLICY, MD5_OLD, "ambiguous"),
            ("old-policy", OLD_POLICY, MD5_NOW, "matched_exact"),
        ],
    )
    counts = replace_pending_address_identities(
        connection=connection,
        policy_version=POLICY,
        reference_md5=MD5_NOW,
        rematch_all=False,
        log=None,
    )

    rows = dict(
        connection.execute(
            "select address_id, pending_reason"
            f" from {QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE}"
        ).fetchall()
    )
    assert rows == {
        "new": "no_outcome",
        "stale-ambiguous": "reference_changed",
        "old-policy": "policy_changed",
    }
    assert counts["pending_identities"] == 3
    assert counts["reason_counts"] == {
        "no_outcome": 1,
        "reference_changed": 1,
        "policy_changed": 1,
    }
    assert counts["short_circuit"] is False


def test_an_unchanged_week_selects_nothing_at_all(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """Goal 1, executed: unchanged snapshot plus unchanged policy plus no new identities
    means the resolver has nothing to do and the reference index is never built."""
    _seed(
        connection,
        [
            ("settled", POLICY, MD5_NOW, "matched_exact"),
            ("known-ambiguous", POLICY, MD5_NOW, "ambiguous"),
        ],
    )
    counts = replace_pending_address_identities(
        connection=connection,
        policy_version=POLICY,
        reference_md5=MD5_NOW,
        rematch_all=False,
        log=None,
    )
    assert counts["pending_identities"] == 0
    assert counts["short_circuit"] is True
    assert pending_identity_count(connection) == 0


def test_rematch_all_selects_every_identity(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    _seed(
        connection,
        [
            ("settled", POLICY, MD5_NOW, "matched_exact"),
            ("new", None, None, None),
        ],
    )
    counts = replace_pending_address_identities(
        connection=connection,
        policy_version=POLICY,
        reference_md5=MD5_NOW,
        rematch_all=True,
        log=None,
    )
    assert counts["pending_identities"] == 2
    assert counts["reason_counts"] == {"rematch_all": 2}
    assert counts["short_circuit"] is False


class _FakeClickhouseClient:
    """Answers the ONE statement the loader issues, and records it."""

    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.executed: list[str] = []

    def execute(
        self,
        sql: str,
        params: object = None,
        settings: object = None,
    ) -> list[tuple[object, ...]]:
        self.executed.append(sql)
        return list(self.rows)


def test_the_loader_reads_the_store_through_the_resolver_view_only() -> None:
    """The loader must not re-express the read rule: it issues geocode_store's own
    resolver-family SELECT, which excludes the imported legacy_adopted_v1 family."""
    connection = duckdb.connect()
    client = _FakeClickhouseClient(
        [
            ("new", POLICY, MD5_NOW, "matched_exact", "exact", 1.0, [], T1),
            ("known-ambiguous", POLICY, MD5_OLD, "ambiguous", "", 0.0, [], T1),
        ]
    )

    loaded = load_current_resolver_outcomes(
        connection=connection, clickhouse_client=client, log=None
    )

    assert loaded == 2
    [statement] = client.executed
    assert f"policy_version != '{LEGACY_ADOPTED_POLICY_VERSION}'" in statement
    assert (
        connection.execute(
            f"select count(*) from {QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE}"
        ).fetchone()[0]
        == 2
    )
    connection.close()


def test_the_fresh_reference_md5_is_read_the_way_the_promotion_stamps_it() -> None:
    """Same expression, same table: the demand scan and the stamp cannot disagree about
    which snapshot this run matched against."""
    connection = duckdb.connect()
    connection.execute("create schema sweden_address_osm")
    connection.execute(
        "create table sweden_address_osm.address_points"
        " (source_record_id varchar, source_md5 varchar)"
    )
    connection.execute(
        "insert into sweden_address_osm.address_points values ('b', 'md5-b'),"
        " ('a', 'md5-a')"
    )

    assert fresh_reference_md5(connection) == "md5-a"

    connection.execute("delete from sweden_address_osm.address_points")
    with pytest.raises(ValueError, match="carries no snapshot MD5"):
        fresh_reference_md5(connection)
    connection.close()


class _PagingClickhouseClient:
    """Serves the keyset-page contract of `_outcome_page_sql` against a fixed result set.

    The store's read has already reduced the resolver family to one row per address_id; this
    stands in for that reduced set. Each `execute` reads the `%(after_address_id)s` cursor and
    the trailing `LIMIT n` and returns the next slice in address_id order -- exactly what a
    correct engine returns for the wrapped page query -- so the loader's pagination loop
    (cursor advance, boundary, termination, insert) is what is under test.
    """

    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = sorted(rows, key=lambda row: row[0])
        self.calls: list[tuple[str, object, object]] = []

    def execute(
        self,
        sql: str,
        params: object = None,
        settings: object = None,
    ) -> list[tuple[object, ...]]:
        self.calls.append((sql, params, settings))
        cursor = None if params is None else params["after_address_id"]  # type: ignore[index]
        match = re.search(r"LIMIT (\d+)\s*$", sql)
        assert match is not None, sql
        limit = int(match.group(1))
        tail = [row for row in self.rows if cursor is None or row[0] > cursor]
        return tail[:limit]


def _outcome_row(address_id: str) -> tuple[object, ...]:
    # Positional to PREVIOUS_OUTCOME_COLUMNS: address_id first, then the eight-column shape
    # the loader inserts into the DuckDB previous-outcomes table.
    return (address_id, POLICY, MD5_NOW, "ambiguous", "", 0.0, [], T1)


@pytest.mark.parametrize(
    ("row_count", "page_size", "expected_cursors"),
    [
        # A partial final page (7 = 2+2+2+1): the last page is short and ends the walk.
        (7, 2, ["id-01", "id-03", "id-05"]),
        # An exact multiple (4 = 2+2): the walk needs one extra empty page to know it is done.
        (4, 2, ["id-01", "id-03"]),
        # A single page smaller than the batch: one query, no cursor, immediate stop.
        (3, 10, []),
    ],
)
def test_the_chunked_load_reproduces_the_single_query_row_set(
    monkeypatch: pytest.MonkeyPatch,
    row_count: int,
    page_size: int,
    expected_cursors: list[str],
) -> None:
    monkeypatch.setattr(geocode_demand, "QUERY_BATCH_SIZE", page_size)
    identities = [f"id-{index:02d}" for index in range(row_count)]
    # Hand them in reversed so only correct address_id ordering + keyset paging can rebuild
    # the set; a page that leaked ordering would drop or repeat one at a boundary.
    client = _PagingClickhouseClient([_outcome_row(i) for i in reversed(identities)])
    connection = duckdb.connect()

    loaded = load_current_resolver_outcomes(
        connection=connection, clickhouse_client=client, log=None
    )

    assert loaded == row_count
    stored = [
        address_id
        for (address_id,) in connection.execute(
            "select address_id from"
            f" {geocode_demand.QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE}"
            " order by address_id"
        ).fetchall()
    ]
    # Row-count and row-set parity with the single-query read, and no dup/drop at chunk edges.
    assert stored == identities
    assert len(stored) == len(set(stored)) == row_count
    # Keyset, not OFFSET: each page after the first carried the previous page's last id.
    cursors = [
        params["after_address_id"]  # type: ignore[index]
        for _, params, _ in client.calls
        if params is not None
    ]
    assert cursors == expected_cursors
    # The bound that makes a stalled page ERROR instead of hanging travels on every query.
    for _, _, settings in client.calls:
        assert settings == {
            "max_execution_time": geocode_demand.MAX_PAGE_EXECUTION_SECONDS,
            "max_block_size": page_size,
        }
    connection.close()


def test_rematch_all_skips_the_load_and_still_selects_every_identity() -> None:
    """The asset's rematch_all path executed: no current-outcome load runs, the previous
    table is created EMPTY, and replace_pending_address_identities still marks every identity
    'rematch_all' -- the CASE's first branch fires whatever the (all-NULL) previous side is."""
    connection = duckdb.connect()
    connection.execute("create schema if not exists sweden_company_enrichment")
    connection.execute(
        "create table sweden_company_enrichment.se_addresses_current"
        " (address_id varchar, address_kind varchar)"
    )
    for identity in ("alpha", "beta", "gamma"):
        connection.execute(
            "insert into sweden_company_enrichment.se_addresses_current"
            " values (?, 'physical')",
            [identity],
        )

    create_empty_previous_outcomes_table(connection)
    assert (
        connection.execute(
            "select count(*) from"
            f" {geocode_demand.QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE}"
        ).fetchone()[0]
        == 0
    )

    counts = replace_pending_address_identities(
        connection=connection,
        policy_version=POLICY,
        reference_md5=MD5_NOW,
        rematch_all=True,
        log=None,
    )

    assert counts["pending_identities"] == 3
    assert counts["reason_counts"] == {"rematch_all": 3}
    assert counts["short_circuit"] is False
    rows = dict(
        connection.execute(
            "select address_id, pending_reason"
            f" from {QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE}"
        ).fetchall()
    )
    assert rows == {
        "alpha": "rematch_all",
        "beta": "rematch_all",
        "gamma": "rematch_all",
    }
    connection.close()
