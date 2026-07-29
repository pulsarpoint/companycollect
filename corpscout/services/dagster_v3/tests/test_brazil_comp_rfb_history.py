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
