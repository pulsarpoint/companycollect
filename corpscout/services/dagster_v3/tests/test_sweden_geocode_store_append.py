"""The append path: every stored outcome is attributable, and a re-append cannot go backwards.

The promotion's DuckDB half is exercised against a real in-memory DuckDB (the pattern
tests/test_address_resolution.py already uses for this module), because the thing under
test is a projection over three joined tables, not a Python branch.
"""

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, Iterator
from zoneinfo import ZoneInfo

import dagster as dg
import duckdb
import pytest
from clickhouse_driver.client import Client
from clickhouse_driver.columns.datetimecolumn import DateTime64Column

from dagster_v3.defs.sweden_company.address_geocoding_assets import (
    BACKFILL_PREFLIGHT_SQL,
    GEOCODE_STORE_BACKFILL_SQL,
    SwedenGeocodeStoreBackfillConfig,
    build_geocode_store_backfill_sql,
    build_store_append_regression_sql,
    epoch_milliseconds,
    sweden_address_geocode_store_backfill_clickhouse,
    sweden_address_geocode_store_clickhouse,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE,
    SERVING_COLUMNS,
    STORE_COLUMNS,
)

POLICY = "se-address-resolution-policy-v5"


def test_the_backfill_stamps_the_policy_and_promotes_source_md5_to_the_key() -> None:
    sql = build_geocode_store_backfill_sql()
    assert sql.startswith(
        "INSERT INTO corpscout.se_address_geocodes (" + ", ".join(STORE_COLUMNS) + ")"
    )
    assert "%(policy_version)s AS policy_version" in sql
    assert "ifNull(source_md5, '') AS reference_md5" in sql
    assert "FROM corpscout.se_address_geocodes_current" in sql
    # matched_at is COPIED, never restamped: the serving row's own instant is what that
    # outcome claims, and copying it is what makes a second backfill run a no-op in content
    # rather than a version bump on 2.09M rows.
    assert "now64" not in sql and "now(" not in sql
    assert GEOCODE_STORE_BACKFILL_SQL is sql or GEOCODE_STORE_BACKFILL_SQL == sql
    # Every serving column is carried across, none dropped.
    for column in SERVING_COLUMNS:
        assert column in sql


def test_the_append_regression_query_looks_for_rows_that_would_swallow_this_run() -> (
    None
):
    """ReplacingMergeTree keeps the row with the LARGEST matched_at per key. If a row for a
    key this run just appended already carries a newer instant, this run's outcome is
    invisible from the moment it lands -- a silent no-op that no row count would reveal."""
    sql = build_store_append_regression_sql()
    assert "FROM corpscout.se_address_geocodes" in sql
    assert "geocode_run_id = %(geocode_run_id)s" in sql
    assert "(address_id, policy_version, reference_md5) IN (" in sql
    # Integer ticks, not a datetime parameter -- see the driver-binding test below for why
    # a bound datetime makes this guard fire on every run.
    assert "toUnixTimestamp64Milli(matched_at) > %(matched_at_ms)s" in sql
    assert "%(matched_at)s" not in sql


def _promotable(source_md5: str | None = "osm-snapshot-md5") -> duckdb.DuckDBPyConnection:
    """A connection carrying a complete, promotable Sweden shadow run.

    `_create_sweden_shadow_fixture` is the ONE seeding helper for this pipeline and it
    already lives in tests/test_address_resolution.py -- imported rather than re-invented,
    because a second fixture would drift from the five promotion tests that share it. Step 3
    gives it the `source_md5` keyword this file needs; nothing else about it changes.
    """
    from tests.test_address_resolution import _create_sweden_shadow_fixture

    connection = duckdb.connect()
    _create_sweden_shadow_fixture(connection, source_md5=source_md5)
    return connection


def _promote(
    connection: duckdb.DuckDBPyConnection,
    *,
    matched_at: datetime = datetime(2026, 8, 24, tzinfo=UTC),
) -> dict[str, object]:
    from dagster_v3.defs.sweden_company.address_resolution_promotion import (
        replace_current_geocodes_from_address_resolution_shadow,
    )
    from dagster_v3.defs.sweden_company.address_resolution_shadow import (
        replace_sweden_address_resolution_shadow,
    )

    replace_sweden_address_resolution_shadow(
        connection=connection,
        evaluation_run_id="shadow-run",
        evaluated_at=datetime(2026, 8, 24, tzinfo=UTC),
        log=None,
    )
    return replace_current_geocodes_from_address_resolution_shadow(
        connection=connection,
        geocode_run_id="run-1",
        matched_at=matched_at,
        expected_policy_version=POLICY,
        log=None,
    )


def test_promotion_writes_both_tables_with_their_own_shapes() -> None:
    with _promotable(source_md5="md5-alpha") as connection:
        counts = _promote(connection)

        assert counts["reference_md5"] == "md5-alpha"
        assert counts["appended_rows"] == counts["rows"]
        # DuckDB's `describe` returns (column_name, column_type, ...) -- index 0 is the name.
        serving = [
            row[0]
            for row in connection.execute(
                "describe sweden_company_enrichment.se_address_geocodes_current"
            ).fetchall()
        ]
        appended = [
            row[0]
            for row in connection.execute(
                f"describe {QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE}"
            ).fetchall()
        ]
        assert serving == list(SERVING_COLUMNS)
        assert appended == list(STORE_COLUMNS)
        [(policies, references)] = connection.execute(
            "select count(distinct policy_version), count(distinct reference_md5)"
            f" from {QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE}"
        ).fetchall()
        assert int(policies) == 1 and int(references) == 1


def test_promotion_refuses_an_outcome_with_no_reference_identity() -> None:
    """The versioning contract's hard half: an outcome with no reference_md5 is not
    attributable, and the store's sorting key would carry an empty string for ever.

    The message is matched EXACTLY, not on the word "reference". A NULL source_md5 also
    trips the pre-existing provenance invariant, whose message ("missing OSM snapshot
    provenance") contains no such word -- so a loose match would pass whichever raise fired
    and would tell us nothing about the new one. Step 3 puts the reference raise FIRST for
    the same reason: the more specific diagnosis should be the one an operator sees.
    """
    with _promotable(source_md5=None) as connection:
        with pytest.raises(
            ValueError, match="Promoted geocodes are missing the OSM reference identity"
        ):
            _promote(connection)


# --------------------------------------------------------------------------------------
# The asset bodies, executed. The ClickHouse client is a fake, but everything above it is
# the real thing: the real promotion writes a real DuckDB hand-off table, and the real
# export helper reads it and issues the real INSERT. The fake deliberately has no
# `insert_arrow` / `insert_rows` attribute, so the helper takes the same
# `execute("INSERT ... VALUES", rows)` path the production clickhouse_driver client takes.
# --------------------------------------------------------------------------------------

MS_MOMENT = datetime(2026, 8, 24, 12, 0, 0, 123456, tzinfo=UTC)


class _FakeClickhouseClient:
    """Records every statement; answers system.tables and the assets' SELECTs by shape."""

    def __init__(
        self,
        *,
        existing_tables: set[str],
        regressions: int = 0,
        preflight: tuple[int, int] = (3, 0),
    ) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.inserted_rows: list[tuple] = []
        self.existing_tables = existing_tables
        self.regressions = regressions
        self.preflight = preflight

    def execute(self, sql: str, params: Any = None) -> list[tuple]:
        self.executed.append((sql, params))
        if "system.tables" in sql:
            return [
                (table,) for table in params["tables"] if table in self.existing_tables
            ]
        if sql.startswith("INSERT INTO"):
            self.inserted_rows.extend(params or [])
            return []
        if "toUnixTimestamp64Milli" in sql:
            return [(self.regressions,)]
        if "countIf(isNull(source_md5)" in sql:
            return [self.preflight]
        if "uniqExact(policy_version)" in sql:
            return [(len(self.inserted_rows), len(self.inserted_rows), 1)]
        if "uniqExact(address_id)" in sql:
            return [(len(self.inserted_rows), len(self.inserted_rows))]
        return []

    @property
    def statements(self) -> list[str]:
        return [sql for sql, _ in self.executed]

    def params_for(self, needle: str) -> Any:
        return next(params for sql, params in self.executed if needle in sql)


class _FakeResource:
    """Stands in for both DuckDBResource and ClickhouseResource: one live connection."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        yield self._connection


def _run_store_append(
    connection: duckdb.DuckDBPyConnection,
    client: _FakeClickhouseClient,
) -> dg.MaterializeResult:
    return sweden_address_geocode_store_clickhouse.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(),
        _FakeResource(connection),
        _FakeResource(client),
    )


def _store_client(**kwargs: Any) -> _FakeClickhouseClient:
    return _FakeClickhouseClient(existing_tables={"se_address_geocodes"}, **kwargs)


def test_the_append_asset_inserts_the_promoted_rows_and_never_replaces_the_store() -> (
    None
):
    """M2b and M5 die here.

    M2b: restamping `matched_at` anywhere in the append path (a `now()` in the promotion
    projection, a `column_expressions` override in the export) would put today's instant on
    rows this test promoted at a fixed past instant. M5: flipping `truncate=False` to True
    makes the export helper CREATE a stage table and EXCHANGE it with the target -- for a
    permanent store that silently throws away every earlier run's outcomes, weekly, and no
    row count of this run would ever show it.
    """
    with _promotable() as connection:
        counts = _promote(connection, matched_at=MS_MOMENT)
        client = _store_client()

        result = _run_store_append(connection, client)

        inserts = [sql for sql in client.statements if sql.startswith("INSERT INTO")]
        assert len(inserts) == 1
        assert inserts[0].startswith(
            "INSERT INTO `corpscout`.`se_address_geocodes` ("
            + ", ".join(f"`{column}`" for column in STORE_COLUMNS)
            + ") VALUES"
        )
        assert len(client.inserted_rows) == counts["appended_rows"] == counts["rows"]
        assert result.metadata["appended_rows"] == counts["rows"]
        # Every appended row carries the promotion's own instant, not "now".
        matched_at_index = STORE_COLUMNS.index("matched_at")
        assert {row[matched_at_index] for row in client.inserted_rows} == {MS_MOMENT}
        # ... and the append never swaps the permanent store away.
        assert not [
            sql
            for sql in client.statements
            if "EXCHANGE TABLES" in sql or sql.startswith("CREATE TABLE")
        ]


def test_the_append_guard_identifies_this_run_from_the_rows_not_the_dagster_run() -> (
    None
):
    """The hand-off table survives between runs, so this asset can materialize alone over
    an earlier promotion's rows. Keying the guard off `context.run_id` then matches nothing
    and the guard passes while measuring nothing -- green, and blind."""
    with _promotable() as connection:
        _promote(connection, matched_at=MS_MOMENT)
        client = _store_client()

        result = _run_store_append(connection, client)

        params = client.params_for("toUnixTimestamp64Milli")
        assert params["geocode_run_id"] == "run-1"
        assert result.metadata["geocode_run_id"] == "run-1"


def test_the_append_guard_survives_the_real_driver_binding_of_its_instant() -> None:
    """The blocker this file exists to keep dead.

    `clickhouse_driver` renders a bound datetime through `escape_datetime`:
    `strftime('%Y-%m-%d %H:%M:%S')` after `astimezone(server_tz)`. Sub-second precision is
    dropped and the literal moves into the server's timezone. Compared against a
    millisecond-stamped `DateTime64(3, 'UTC')` column, every row this run just appended is
    then strictly greater than its own truncated instant, the guard counts all of them, and
    the asset raises on essentially every run.

    Both halves below are the driver's own code: the write path that decides what tick the
    column stores, and the substitution path `Client.execute` uses for parameters.
    """
    with _promotable() as connection:
        _promote(connection, matched_at=MS_MOMENT)
        client = _store_client()

        _run_store_append(connection, client)

        bound = client.params_for("toUnixTimestamp64Milli")["matched_at_ms"]

        # What clickhouse_driver actually writes into DateTime64(3) for these rows.
        context = SimpleNamespace(
            server_info=SimpleNamespace(get_timezone=lambda: "Europe/Stockholm"),
            client_settings={"server_side_params": False},
        )
        stored = [MS_MOMENT]
        DateTime64Column(scale=3, context=context).before_write_items(stored)
        assert bound == stored[0]  # equal, so `stored > bound` is false: no false raise.

        driver = SimpleNamespace(connection=SimpleNamespace(context=context))
        rendered = Client.substitute_params(
            driver,
            build_store_append_regression_sql(),
            client.params_for("toUnixTimestamp64Milli"),
            context,
        )
        assert f"toUnixTimestamp64Milli(matched_at) > {stored[0]}" in rendered

        # The negative control: the datetime form the driver would have rendered instead --
        # milliseconds gone, and shifted two hours by the server timezone.
        lost = Client.substitute_params(
            driver, "%(matched_at)s", {"matched_at": MS_MOMENT}, context
        )
        # 12:00:00.123 UTC came back as 14:00:00: the .123 truncated away and the whole
        # instant moved into the server's timezone.
        assert lost == "'2026-08-24 14:00:00'"
        assert epoch_milliseconds(MS_MOMENT) == stored[0]


def test_the_instant_is_the_same_tick_however_duckdb_hands_it_over() -> None:
    """DuckDB returns TIMESTAMPTZ in the session's timezone -- a Europe/Belgrade box hands
    back 14:00+02:00 for a 12:00Z instant. The tick must not depend on which offset the
    same instant arrives in, or the guard's threshold would move with the server's locale.
    """
    belgrade = MS_MOMENT.astimezone(ZoneInfo("Europe/Belgrade"))
    assert belgrade.hour != MS_MOMENT.hour
    assert epoch_milliseconds(belgrade) == epoch_milliseconds(MS_MOMENT)
    assert epoch_milliseconds(MS_MOMENT.replace(tzinfo=None)) == epoch_milliseconds(
        MS_MOMENT
    )


def test_the_append_asset_raises_when_stored_outcomes_are_newer() -> None:
    with _promotable() as connection:
        _promote(connection, matched_at=MS_MOMENT)
        client = _store_client(regressions=4)

        with pytest.raises(ValueError, match="4 stored outcomes are newer"):
            _run_store_append(connection, client)


def test_the_append_asset_refuses_an_empty_hand_off_table() -> None:
    """An empty hand-off table is the second silent-no-op route: `max(matched_at)` is None,
    the driver renders NULL, and `> NULL` is NULL for every row -- the guard would pass
    having measured nothing."""
    with _promotable() as connection:
        _promote(connection)
        connection.execute(
            f"delete from {QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE}"
        )
        client = _store_client()

        with pytest.raises(ValueError, match="append table is empty"):
            _run_store_append(connection, client)
        assert not [sql for sql in client.statements if sql.startswith("INSERT INTO")]


def _run_backfill(
    client: _FakeClickhouseClient,
    *,
    execute: bool,
) -> dg.MaterializeResult:
    return (
        sweden_address_geocode_store_backfill_clickhouse.node_def.compute_fn.decorated_fn(
            dg.build_asset_context(),
            SwedenGeocodeStoreBackfillConfig(execute=execute),
            _FakeResource(client),
        )
    )


def _backfill_client(**kwargs: Any) -> _FakeClickhouseClient:
    return _FakeClickhouseClient(
        existing_tables={"se_address_geocodes", "se_address_geocodes_current"}, **kwargs
    )


def test_the_backfill_writes_nothing_without_the_execute_gate() -> None:
    """M3 dies here: deleting the preview branch, or defaulting `execute` to True, makes a
    bare Materialize click append 2.09M rows to a permanent store."""
    client = _backfill_client(preflight=(2_090_000, 0))

    result = _run_backfill(client, execute=False)

    assert result.metadata["preview"] is True
    assert result.metadata["serving_rows"] == 2_090_000
    assert client.statements[-1] == BACKFILL_PREFLIGHT_SQL
    assert not [sql for sql in client.statements if sql.startswith("INSERT INTO")]


def test_the_backfill_appends_only_when_the_gate_is_set() -> None:
    client = _backfill_client(preflight=(3, 0))

    result = _run_backfill(client, execute=True)

    inserts = [
        (sql, params)
        for sql, params in client.executed
        if sql.startswith("INSERT INTO")
    ]
    assert len(inserts) == 1
    assert inserts[0][0] == GEOCODE_STORE_BACKFILL_SQL
    assert inserts[0][1] == {"policy_version": POLICY}
    assert result.metadata["preview"] is False
    assert result.metadata["policy_version"] == POLICY


def test_the_backfill_refuses_serving_rows_with_no_snapshot_identity() -> None:
    client = _backfill_client(preflight=(2_090_000, 7))

    with pytest.raises(ValueError, match="7 serving rows carry no OSM snapshot MD5"):
        _run_backfill(client, execute=True)
    assert not [sql for sql in client.statements if sql.startswith("INSERT INTO")]


def test_the_backfill_refuses_an_empty_serving_table() -> None:
    client = _backfill_client(preflight=(0, 0))

    with pytest.raises(ValueError, match="serving geocode table is empty"):
        _run_backfill(client, execute=True)
