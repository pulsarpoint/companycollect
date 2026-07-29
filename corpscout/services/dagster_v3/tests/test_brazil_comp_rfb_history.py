import duckdb
import pytest

from dagster_v3.defs.brazil_companies.rfb import history


def test_out_of_order_snapshot_is_refused_loudly() -> None:
    """Manual runs mean months WILL arrive out of order. Absorbing a late one
    silently would corrupt the timeline and hide that the cadence slipped --
    and end_at precision is exactly the cadence."""
    with pytest.raises(ValueError, match="older than"):
        history.assert_snapshot_is_newer("2026-07", ["2026-06", "2026-08"])


def test_same_snapshot_merged_twice_is_refused() -> None:
    with pytest.raises(ValueError, match="already merged"):
        history.assert_snapshot_is_newer("2026-08", ["2026-06", "2026-08"])


def test_first_snapshot_into_an_empty_history_is_allowed() -> None:
    history.assert_snapshot_is_newer("2026-06", [])


def test_first_ever_snapshot_is_not_blocked_by_the_edge_count_guard() -> None:
    """No previous merged month means nothing to compare against -- refusing
    the very first snapshot on principle would be its own bug."""
    history.assert_snapshot_edge_count_is_plausible(1, None)


def test_a_snapshot_missing_most_of_its_edges_is_refused() -> None:
    """Simulates a truncated download: several of RFB's ~10 socios ZIP parts
    never landed, so the snapshot is well-formed but far short. Left
    unchecked, the merge reads every missing partner as 'gone' and silently,
    permanently closes their spells."""
    with pytest.raises(ValueError, match="truncated download"):
        history.assert_snapshot_edge_count_is_plausible(9_000_000, 20_000_000)


def test_a_snapshot_with_a_plausible_small_drop_is_allowed() -> None:
    """Real month-to-month churn (partners leaving, companies deregistered)
    must not be mistaken for corruption."""
    history.assert_snapshot_edge_count_is_plausible(19_000_000, 20_000_000)


def test_a_snapshot_exactly_at_the_threshold_is_allowed() -> None:
    """The guard is `< threshold`, not `<=` -- landing exactly on the ratio
    must pass, not be an off-by-one rejection."""
    history.assert_snapshot_edge_count_is_plausible(10_000_000, 20_000_000)


def test_edge_count_guard_ignores_a_zero_previous_count() -> None:
    """A previous merged month legitimately recorded with 0 edges (e.g. a
    still-empty history) must not make every subsequent snapshot look like a
    100% drop against a non-existent baseline."""
    history.assert_snapshot_edge_count_is_plausible(1, 0)


def test_first_ever_snapshot_is_not_blocked_by_the_part_count_guard() -> None:
    """No previous merged month means nothing to compare against -- refusing
    the very first snapshot on principle would be its own bug. Uses exactly
    EXPECTED_SOCIOS_PART_COUNT so it also clears the absolute floor below."""
    history.assert_snapshot_part_count_is_not_decreasing(10, None)


def test_first_ever_run_with_fewer_than_expected_socios_parts_is_refused() -> None:
    """BLOCKER 1: on an empty ledger (production's exact state today),
    previous_socios_part_count is None and the relative comparison above is
    a no-op -- so without an absolute floor, a first run carrying only 9 of
    RFB's measured 10 socios parts would be accepted silently, and that
    short count would become the permanent baseline every later month is
    judged against."""
    with pytest.raises(ValueError, match="socios ZIP parts"):
        history.assert_snapshot_part_count_is_not_decreasing(9, None)


def test_first_ever_run_with_expected_socios_parts_is_accepted() -> None:
    """The mirror case: exactly EXPECTED_SOCIOS_PART_COUNT (10) on the very
    first-ever merge must be accepted -- the floor must not be off-by-one."""
    history.assert_snapshot_part_count_is_not_decreasing(10, None)


def test_a_snapshot_missing_a_single_socios_part_is_refused() -> None:
    """The exact case MIN_SNAPSHOT_EDGE_RATIO cannot catch: one missing
    socios part out of RFB's ~10 is only a ~10% edge-count drop, comfortably
    inside the 50% ratio floor -- but the part count itself decreased, and
    this guard has no threshold to sail under."""
    with pytest.raises(ValueError, match="socios ZIP parts"):
        history.assert_snapshot_part_count_is_not_decreasing(9, 10)


def test_a_snapshot_with_the_same_part_count_is_allowed() -> None:
    history.assert_snapshot_part_count_is_not_decreasing(10, 10)


def test_a_snapshot_with_more_parts_than_the_previous_month_is_allowed() -> None:
    """Only a DECREASE is suspect -- RFB legitimately changing how many parts
    it splits socios into (more parts) must not be refused."""
    history.assert_snapshot_part_count_is_not_decreasing(11, 10)


def test_part_count_guard_ignores_a_zero_previous_count() -> None:
    """Covers a ledger row written before migration 000211 added this column
    (`ADD COLUMN ... DEFAULT 0`) -- it must not make the very next run look
    like a decrease against a baseline that was never actually recorded.
    Uses EXPECTED_SOCIOS_PART_COUNT (not some smaller value) because a count
    below that now fails BLOCKER 1's absolute floor regardless of the
    previous count -- this test isolates the zero-previous-count relative
    comparison, not the absolute floor (see the first-ever-run tests for
    that one)."""
    history.assert_snapshot_part_count_is_not_decreasing(10, 0)


def test_build_merge_select_sql_rejects_a_quote_injection_attempt() -> None:
    """build_merge_select_sql interpolates snapshot_year_month raw into the
    returned SQL text. Unvalidated, a trailing quote closes the string early
    and emits the broken/injectable literal '2026-06''. The merge is
    self-referential and writes are permanent, so this must be refused, not
    merely produce odd SQL."""
    with pytest.raises(ValueError, match="YYYY-MM"):
        history.build_merge_select_sql(
            state_table="state",
            snapshot_table="snap",
            snapshot_year_month="2026-06'",
            snapshot_date="2026-06-01",
        )


def test_build_merge_select_sql_rejects_an_unparseable_snapshot_date() -> None:
    """snapshot_date is interpolated raw into `date '{snapshot_date}'`.
    Unvalidated, snapshot_date="garbage" would only fail at execution time,
    deep inside the merge, in whichever engine happens to run it."""
    with pytest.raises(ValueError, match="ISO date"):
        history.build_merge_select_sql(
            state_table="state",
            snapshot_table="snap",
            snapshot_year_month="2026-06",
            snapshot_date="garbage",
        )


def test_build_merge_select_sql_rejects_an_unpadded_snapshot_year_month() -> None:
    """"2026-6" (unpadded) must not be silently accepted and written into
    first_seen_snapshot forever -- mirrors assert_snapshot_is_newer's own
    guard against the same malformed input."""
    with pytest.raises(ValueError, match="YYYY-MM"):
        history.build_merge_select_sql(
            state_table="state",
            snapshot_table="snap",
            snapshot_year_month="2026-6",
            snapshot_date="2026-06-01",
        )


def test_build_merge_select_sql_rejects_a_snapshot_date_that_disagrees_with_the_month() -> (
    None
):
    """Nothing else enforces that snapshot_year_month and snapshot_date agree.
    month=2026-07 with date=2026-06-01 would silently write history where
    end_at contradicts last_seen_snapshot -- permanently, since the merge is
    self-referential."""
    with pytest.raises(ValueError, match="first day"):
        history.build_merge_select_sql(
            state_table="state",
            snapshot_table="snap",
            snapshot_year_month="2026-07",
            snapshot_date="2026-06-01",
        )


def _merge(connection: duckdb.DuckDBPyConnection, month: str, date: str) -> None:
    """Wraps the shared SELECT the way DuckDB wants it. The export wraps the
    same SELECT in an INSERT -- see the interfaces note above."""
    select_sql = history.build_merge_select_sql(
        state_table="state",
        snapshot_table="snap",
        snapshot_year_month=month,
        snapshot_date=date,
    )
    connection.execute(f"create or replace table stage as {select_sql}")
    connection.execute("drop table state")
    connection.execute("alter table stage rename to state")


def _schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        create table state (
            country_iso2 varchar, source_slug varchar, cnpj_basico varchar,
            related_entity_kind varchar, related_tax_id varchar,
            relation_code varchar, relation_since_key varchar,
            related_name varchar, related_country varchar, age_band varchar,
            representative_tax_id varchar, representative_name varchar,
            representative_code varchar, relation_since date,
            first_seen_snapshot varchar, last_seen_snapshot varchar,
            start_at date, end_at date, is_current utinyint,
            observations uinteger, resolved_at timestamp
        )
        """
    )


def _snapshot(connection: duckdb.DuckDBPyConnection, rows: list[tuple]) -> None:
    connection.execute("drop table if exists snap")
    connection.execute(
        """
        create table snap (
            country_iso2 varchar, source_slug varchar, cnpj_basico varchar,
            related_entity_kind varchar, related_tax_id varchar,
            relation_code varchar, relation_since_key varchar,
            related_name varchar, related_country varchar, age_band varchar,
            representative_tax_id varchar, representative_name varchar,
            representative_code varchar, relation_since date
        )
        """
    )
    if rows:
        connection.executemany(
            "insert into snap values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
        )


def _edge(tax_id: str, code: str, since_key: str, name: str = "MARIA SOUZA") -> tuple:
    return (
        "BR", "brazil_rfb", "11111111", "2", tax_id, code, since_key,
        name, "", "0", "", "", "", None,
    )


def test_an_unchanged_edge_extends_rather_than_duplicating() -> None:
    connection = duckdb.connect(":memory:")
    _schema(connection)

    _snapshot(connection, [_edge("***456789**", "49", "20190701")])
    _merge(connection, "2026-06", "2026-06-01")
    _snapshot(connection, [_edge("***456789**", "49", "20190701")])
    _merge(connection, "2026-07", "2026-07-01")

    rows = connection.execute(
        "select first_seen_snapshot, last_seen_snapshot, observations, "
        "is_current, end_at from state"
    ).fetchall()
    assert rows == [("2026-06", "2026-07", 2, 1, None)]


def test_a_disappearing_edge_is_closed_not_deleted() -> None:
    """end_at means 'gone by this snapshot', never 'left on this date' -- the
    source never publishes a departure."""
    connection = duckdb.connect(":memory:")
    _schema(connection)

    _snapshot(connection, [_edge("***456789**", "49", "20190701")])
    _merge(connection, "2026-06", "2026-06-01")
    _snapshot(connection, [])
    _merge(connection, "2026-07", "2026-07-01")

    assert connection.execute(
        "select is_current, end_at, last_seen_snapshot from state"
    ).fetchall() == [(0, __import__("datetime").date(2026, 7, 1), "2026-06")]


def test_a_role_change_opens_a_second_spell() -> None:
    """Partner -> administrator is the control shift this table exists to show,
    so it must be two rows, not a mutated column."""
    connection = duckdb.connect(":memory:")
    _schema(connection)

    _snapshot(connection, [_edge("***456789**", "49", "20190701")])
    _merge(connection, "2026-06", "2026-06-01")
    _snapshot(connection, [_edge("***456789**", "22", "20190701")])
    _merge(connection, "2026-07", "2026-07-01")

    rows = connection.execute(
        "select relation_code, is_current, observations from state "
        "order by relation_code"
    ).fetchall()
    assert rows == [("22", 1, 1), ("49", 0, 1)]


def test_a_re_entry_opens_a_second_spell_on_the_new_entry_date() -> None:
    """A returning partner is visible from ONE snapshot because RFB stamps a new
    data_entrada_sociedade -- it does not depend on us having observed the gap."""
    connection = duckdb.connect(":memory:")
    _schema(connection)

    _snapshot(connection, [_edge("***456789**", "49", "20190701")])
    _merge(connection, "2026-06", "2026-06-01")
    _snapshot(connection, [_edge("***456789**", "49", "20260901")])
    _merge(connection, "2026-07", "2026-07-01")

    rows = connection.execute(
        "select relation_since_key, is_current from state "
        "order by relation_since_key"
    ).fetchall()
    assert rows == [("20190701", 0), ("20260901", 1)]


def test_a_closed_spell_is_not_reopened_or_recounted() -> None:
    connection = duckdb.connect(":memory:")
    _schema(connection)

    _snapshot(connection, [_edge("***456789**", "49", "20190701")])
    _merge(connection, "2026-06", "2026-06-01")
    _snapshot(connection, [])
    _merge(connection, "2026-07", "2026-07-01")
    _snapshot(connection, [])
    _merge(connection, "2026-08", "2026-08-01")

    assert connection.execute(
        "select is_current, observations, end_at from state"
    ).fetchall() == [(0, 1, __import__("datetime").date(2026, 7, 1))]


def test_a_reappearing_key_opens_a_new_spell_and_leaves_the_closed_one_untouched() -> (
    None
):
    """Simulates a truncated monthly download: RFB ships socios as ~10 ZIP
    parts and relations.py only refuses a COMPLETELY empty result, so a
    partner can be missing from one month's snapshot for infra reasons, not
    because they actually left. When that same key (same relation_since_key --
    RFB's own data never changed) reappears, the closed row must be left
    exactly as it was, and the reappearance must open a NEW spell -- not
    mutate the old one back open and not disappear from current state.
    """
    connection = duckdb.connect(":memory:")
    _schema(connection)

    partner_a = _edge("***111111**", "49", "20190701", "PARTNER A")
    partner_b = _edge("***222222**", "49", "20190701", "PARTNER B")
    partner_c = _edge("***333333**", "49", "20190701", "PARTNER C")

    # 2026-06: all three partners present.
    _snapshot(connection, [partner_a, partner_b, partner_c])
    _merge(connection, "2026-06", "2026-06-01")

    # 2026-07: truncated download -- partner B's part didn't make it in.
    _snapshot(connection, [partner_a, partner_c])
    _merge(connection, "2026-07", "2026-07-01")

    # 2026-08: the source healed. All three are back, including B, with the
    # SAME relation_since_key as before -- this is not a re-entry, it is our
    # own gap closing.
    _snapshot(connection, [partner_a, partner_b, partner_c])
    _merge(connection, "2026-08", "2026-08-01")

    current_edges = connection.execute(
        "select count(*) from state where is_current = 1"
    ).fetchone()[0]
    assert current_edges == 3, "all three partners must be current after the source healed"

    b_rows = connection.execute(
        "select is_current, end_at, last_seen_snapshot, observations, "
        "first_seen_snapshot from state where related_tax_id = '***222222**' "
        "order by is_current"
    ).fetchall()
    assert b_rows == [
        # the closed spell from the truncated month: untouched, every column.
        (0, __import__("datetime").date(2026, 7, 1), "2026-06", 1, "2026-06"),
        # a brand-new spell opened by the reappearance, sharing the old key.
        (1, None, "2026-08", 1, "2026-08"),
    ]


def test_duplicate_source_rows_do_not_fan_out() -> None:
    """SPELL_KEY is coarser than the pipeline's own record identity (e.g. two
    partners whose masked CPFs collide on the 6 of 11 visible digits, same
    role, same entry date, are two source rows but one spell key). If the
    join doesn't dedupe, N state rows x M snapshot rows for one key fans out
    every month it recurs -- and once state holds duplicates, even a clean
    single source row keeps them. Drive it across four months.
    """
    connection = duckdb.connect(":memory:")
    _schema(connection)

    duplicate_pair = [
        _edge("***456789**", "49", "20190701"),
        _edge("***456789**", "49", "20190701"),
    ]

    _snapshot(connection, duplicate_pair)
    _merge(connection, "2026-06", "2026-06-01")
    _snapshot(connection, duplicate_pair)
    _merge(connection, "2026-07", "2026-07-01")
    _snapshot(connection, duplicate_pair)
    _merge(connection, "2026-08", "2026-08-01")
    _snapshot(connection, duplicate_pair)
    _merge(connection, "2026-09", "2026-09-01")

    rows = connection.execute(
        "select is_current, observations from state where related_tax_id = '***456789**'"
    ).fetchall()
    assert rows == [(1, 4)], (
        "one row for the key, observed once per month -- not fanned out to "
        "2/4/8/16 rows by the join"
    )


def test_duplicate_key_with_null_and_populated_relation_since_does_not_crash_and_picks_populated() -> (
    None
):
    """Guards the Critical: production RFB data routinely has NULL
    relation_since (relations.py's try_strptime yields NULL for both '' and
    RFB's '00000000' sentinel -- see test_brazil_comp_rfb_relations.py's
    ("", None) case), and the every-row-relation_since_since=None fixtures
    elsewhere in this file cannot exercise a mix of NULL and populated in one
    dedup group. This test drives exactly that mix through the same snapshot
    key, alongside a populated/blank related_name so the winner is
    unambiguous.

    This passes in DuckDB whether or not the dedup ORDER BY casts
    relation_since -- DuckDB's CAST returns NULL from NULL input and coalesce
    absorbs it. It does NOT prove ClickHouse parity by itself; the real
    regression guard is the comment on `snapshot_dedup_order` in history.py,
    which this test cannot substitute for. What this test does prove: the
    merge does not blow up on mixed NULL/populated relation_since, and the
    populated row wins the tie -- both required by the fix.
    """
    connection = duckdb.connect(":memory:")
    _schema(connection)

    populated = (
        "BR", "brazil_rfb", "11111111", "2", "***456789**", "49", "20190701",
        "MARIA SOUZA", "", "0", "", "", "",
        __import__("datetime").date(2019, 7, 1),
    )
    blank = (
        "BR", "brazil_rfb", "11111111", "2", "***456789**", "49", "20190701",
        "", "", "0", "", "", "", None,
    )
    _snapshot(connection, [blank, populated])
    _merge(connection, "2026-06", "2026-06-01")

    rows = connection.execute(
        "select related_name, relation_since, is_current from state"
    ).fetchall()
    assert rows == [
        ("MARIA SOUZA", __import__("datetime").date(2019, 7, 1), 1)
    ], "the populated row must win the dedup tie-break, not the blank one"


def test_pre_existing_duplicate_open_state_rows_converge_to_one() -> None:
    """Before this fix, open_state and closed_state read from state
    unfiltered -- no dedup -- so two pre-existing OPEN rows for one spell key
    stayed two forever, even across clean snapshots: the merge is
    self-referential with no self-heal path. Seed state directly with a
    duplicate (as a retried `INSERT INTO stage` in the export could produce,
    something a clean merge cannot produce today since the table is deployed
    empty) and drive several clean months through it.
    """
    connection = duckdb.connect(":memory:")
    _schema(connection)

    connection.execute(
        """
        insert into state values
        ('BR','brazil_rfb','11111111','2','***456789**','49','20190701',
         'MARIA SOUZA','','0','','','',date '2019-07-01',
         '2026-05','2026-05',date '2019-07-01',NULL,1,1,now()),
        ('BR','brazil_rfb','11111111','2','***456789**','49','20190701',
         'MARIA SOUZA','','0','','','',date '2019-07-01',
         '2026-05','2026-05',date '2019-07-01',NULL,1,1,now())
        """
    )

    for month, date in (
        ("2026-06", "2026-06-01"),
        ("2026-07", "2026-07-01"),
        ("2026-08", "2026-08-01"),
        ("2026-09", "2026-09-01"),
    ):
        _snapshot(connection, [_edge("***456789**", "49", "20190701")])
        _merge(connection, month, date)

    count = connection.execute(
        "select count(*) from state where is_current = 1"
    ).fetchone()[0]
    assert count == 1, "duplicate OPEN state rows must converge to one, not stay two forever"


def test_pre_existing_duplicate_closed_state_rows_converge_to_one() -> None:
    """Same self-heal requirement as the OPEN case, for CLOSED duplicates:
    partitioned on SPELL_KEY + first_seen_snapshot + end_at, so TRUE
    duplicates (identical on all three) collapse to one row.
    """
    connection = duckdb.connect(":memory:")
    _schema(connection)

    connection.execute(
        """
        insert into state values
        ('BR','brazil_rfb','11111111','2','***456789**','49','20190701',
         'MARIA SOUZA','','0','','','',date '2019-07-01',
         '2026-05','2026-05',date '2019-07-01',date '2026-06-01',0,1,now()),
        ('BR','brazil_rfb','11111111','2','***456789**','49','20190701',
         'MARIA SOUZA','','0','','','',date '2019-07-01',
         '2026-05','2026-05',date '2019-07-01',date '2026-06-01',0,1,now())
        """
    )

    _snapshot(connection, [])
    _merge(connection, "2026-06", "2026-06-01")

    count = connection.execute(
        "select count(*) from state where is_current = 0"
    ).fetchone()[0]
    assert count == 1, "duplicate CLOSED state rows must converge to one"


def test_two_distinct_closed_spells_sharing_a_key_both_survive_the_dedup() -> None:
    """The closed_state dedup must partition on more than SPELL_KEY: 'seen,
    gone, seen again, gone again' is two legitimate closed spells sharing one
    key (same relation_since_key, since RFB stamped no new entry date on the
    second appearance -- see test_a_reappearing_key_opens_a_new_spell...). A
    key-only partition would wrongly collapse them into one.
    """
    connection = duckdb.connect(":memory:")
    _schema(connection)

    _snapshot(connection, [_edge("***456789**", "49", "20190701")])
    _merge(connection, "2026-06", "2026-06-01")
    _snapshot(connection, [])
    _merge(connection, "2026-07", "2026-07-01")
    _snapshot(connection, [_edge("***456789**", "49", "20190701")])
    _merge(connection, "2026-08", "2026-08-01")
    _snapshot(connection, [])
    _merge(connection, "2026-09", "2026-09-01")

    rows = connection.execute(
        "select first_seen_snapshot, end_at from state where is_current = 0 "
        "order by first_seen_snapshot"
    ).fetchall()
    assert rows == [
        ("2026-06", __import__("datetime").date(2026, 7, 1)),
        ("2026-08", __import__("datetime").date(2026, 9, 1)),
    ], "both closed spells must survive -- distinct first_seen/end_at, same key"
