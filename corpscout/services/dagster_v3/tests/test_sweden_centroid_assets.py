"""Tests for the SE coarse-centroid publish asset (`centroid_assets.py`).

The publish path (`publish_sweden_geocode_centroids`) is exercised against a
real DuckDB connection seeded with a `sweden_address_osm.address_points` stand-in
and a FakeClient that records the ClickHouse statements. The assertions pin:

- the centroids are derived FROM `address_points`;
- the OSM `city` column feeds the `post_town` key the derivation expects
  (so a city centroid keys off the post town, uppercased with Swedish letters
  intact);
- each table is published via the staged-`EXCHANGE` helper (stage create,
  batched insert, `EXCHANGE TABLES`, stage drop);
- every published row carries the OSM snapshot time.

The second half of this file (below `_FakeAreaSanityClient`) covers Task 7's
`centroid_fallback_lands_in_the_right_area` check: the pure predicate directly, the SQL
constants it reads by substring, and the `dg.asset_check` wrapper against a FakeClient --
mirroring how `tests/test_sweden_geocode_checks.py` tests the other Sweden geocode
checks (predicate isolated from the query, wrapper called via
`check.node_def.compute_fn.decorated_fn(...)` so no real Dagster run is needed).
"""

from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

import dagster as dg
import duckdb

from dagster_v3.defs.sweden_company.centroid_assets import (
    AREA_SANITY_METERS,
    AREA_SANITY_SAMPLE_SQL,
    CITY_CENTROID_INVARIANTS_SQL,
    CITY_CENTROIDS_TABLE,
    MAX_OUT_OF_AREA_FRACTION,
    MIN_CENTROID_POINTS,
    POSTCODE_CENTROID_INVARIANTS_SQL,
    POSTCODE_CENTROIDS_TABLE,
    POSTCODE_CITY_MAP_SQL,
    QUALIFIED_CITY_CENTROIDS_TABLE,
    QUALIFIED_POSTCODE_CENTROIDS_TABLE,
    centroid_fallback_lands_in_the_right_area,
    publish_sweden_geocode_centroids,
    sweden_geocode_centroid_area_sanity_check,
    sweden_geocode_centroids_clickhouse,
)
from dagster_v3.defs.sweden_company.shared_addresses import (
    QUALIFIED_CLICKHOUSE_SHARED_ADDRESSES_TABLE,
)

_SNAPSHOT_AT = datetime(2026, 8, 20, 12, 0, 0)


class _FakeClickHouseClient:
    """Records executed ClickHouse statements and captures inserted rows.

    Deliberately exposes no ``insert_arrow``/``insert_rows`` so the publish
    helper falls back to ``execute("INSERT ... VALUES", rows)`` -- the second
    positional argument is the batch of row tuples, which we bucket by target
    table (the stage name embeds the base table name).
    """

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserts: dict[str, list[tuple[object, ...]]] = {
            "postcode": [],
            "city": [],
        }

    def execute(
        self,
        sql: str,
        params: object | None = None,
        settings: dict[str, int] | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if sql.lstrip().upper().startswith("INSERT INTO"):
            rows = list(params or [])
            if POSTCODE_CENTROIDS_TABLE in sql:
                self.inserts["postcode"].extend(rows)
            elif CITY_CENTROIDS_TABLE in sql:
                self.inserts["city"].extend(rows)
        return []


def _seed_address_points(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("create schema if not exists sweden_address_osm")
    connection.execute(
        """
        create table sweden_address_osm.address_points(
            postcode varchar,
            city varchar,
            latitude double,
            longitude double,
            source_snapshot_at timestamp
        )
        """
    )
    connection.execute(
        "insert into sweden_address_osm.address_points values (?,?,?,?,?)",
        # Trelleborg: 3 points sharing postcode '231 00' (spaced -> digit-key
        # '23100') and city 'Trelleborg' (-> post_town key 'TRELLEBORG'). Both the
        # postcode and city rungs clear the N>=3 gate.
        ["231 00", "Trelleborg", 55.375, 13.150, _SNAPSHOT_AT],
    )
    connection.execute(
        "insert into sweden_address_osm.address_points values (?,?,?,?,?)",
        ["231 00", "Trelleborg", 55.377, 13.152, _SNAPSHOT_AT],
    )
    connection.execute(
        "insert into sweden_address_osm.address_points values (?,?,?,?,?)",
        ["231 00", "Trelleborg", 55.373, 13.148, _SNAPSHOT_AT],
    )
    # Stockholm: 2 points -> below the 3-point gate, must NOT produce a centroid.
    connection.execute(
        "insert into sweden_address_osm.address_points values (?,?,?,?,?)",
        ["114 56", "Stockholm", 59.339, 18.05, _SNAPSHOT_AT],
    )
    connection.execute(
        "insert into sweden_address_osm.address_points values (?,?,?,?,?)",
        ["114 56", "Stockholm", 59.341, 18.06, _SNAPSHOT_AT],
    )


def test_publish_derives_from_address_points_and_exchanges_both_tables() -> None:
    connection = duckdb.connect()
    _seed_address_points(connection)
    client = _FakeClickHouseClient()

    result = publish_sweden_geocode_centroids(
        connection=connection, clickhouse_client=client
    )

    # Only Trelleborg clears the N>=3 gate, on both the postcode and city rungs.
    assert result["postcode_centroid_keys"] == 1
    assert result["city_centroid_keys"] == 1
    assert result["postcode_centroids_published"] == 1
    assert result["city_centroids_published"] == 1

    # City centroid keys off the OSM `city` column mapped to `post_town`, with
    # the Swedish city name uppercased -- proves the derivation reads
    # address_points and that city -> post_town mapping holds.
    city_keys = {row[0] for row in client.inserts["city"]}
    assert city_keys == {"TRELLEBORG"}
    assert "STOCKHOLM" not in city_keys

    # Postcode centroid keys off the digit-compacted postcode.
    postcode_keys = {row[0] for row in client.inserts["postcode"]}
    assert postcode_keys == {"23100"}
    assert "11456" not in postcode_keys

    # source_snapshot_at (last export column) is stamped from the OSM snapshot.
    for bucket in ("postcode", "city"):
        for row in client.inserts[bucket]:
            assert row[-1] == _SNAPSHOT_AT

    # Both reference tables are published via the staged-EXCHANGE helper: a
    # stage created AS the target, then a single EXCHANGE TABLES swap.
    exchanges = [s for s in client.statements if s.strip().startswith("EXCHANGE TABLES")]
    assert any(
        f"`{QUALIFIED_POSTCODE_CENTROIDS_TABLE.replace('.', '`.`')}`" in s
        for s in exchanges
    )
    assert any(
        f"`{QUALIFIED_CITY_CENTROIDS_TABLE.replace('.', '`.`')}`" in s
        for s in exchanges
    )
    assert len(exchanges) == 2

    stage_creates = [
        s
        for s in client.statements
        if s.startswith("CREATE TABLE") and " AS " in s
    ]
    assert len(stage_creates) == 2


# -----------------------------------------------------------------------------------
# Task 7: `centroid_fallback_lands_in_the_right_area`.


def test_the_predicate_passes_the_happy_path() -> None:
    """Well-populated tables, no invariant violations, nothing implausibly far apart."""
    assert centroid_fallback_lands_in_the_right_area(
        postcode_centroid_rows=500,
        city_centroid_rows=120,
        postcode_point_count_violations=0,
        city_point_count_violations=0,
        sample_size=480,
        out_of_area_count=0,
    )


def test_the_predicate_fails_when_a_postcode_centroid_lands_200km_from_its_city() -> None:
    """The negative branch the plan asks for by name.

    200km is a stand-in for what a live AREA_SANITY_SAMPLE_SQL run would have measured
    for a mis-keyed or mis-derived postcode centroid -- comfortably past
    AREA_SANITY_METERS (50km), so it lands in `countIf(... > AREA_SANITY_METERS)` as one
    out-of-area pair. One bad pair in a ten-pair sample is 10%, past the 1% budget.
    """
    assert 200_000 > AREA_SANITY_METERS
    assert not centroid_fallback_lands_in_the_right_area(
        postcode_centroid_rows=500,
        city_centroid_rows=120,
        postcode_point_count_violations=0,
        city_point_count_violations=0,
        sample_size=10,
        out_of_area_count=1,
    )


def test_the_predicate_tolerates_exactly_one_percent_out_of_area() -> None:
    """The plan's gate is '> 1%', not '>= 1%' -- exactly at the budget still passes."""
    assert centroid_fallback_lands_in_the_right_area(
        postcode_centroid_rows=500,
        city_centroid_rows=120,
        postcode_point_count_violations=0,
        city_point_count_violations=0,
        sample_size=100,
        out_of_area_count=1,
    )


def test_the_predicate_fails_just_past_the_one_percent_budget() -> None:
    assert not centroid_fallback_lands_in_the_right_area(
        postcode_centroid_rows=500,
        city_centroid_rows=120,
        postcode_point_count_violations=0,
        city_point_count_violations=0,
        sample_size=1000,
        out_of_area_count=11,
    )


def test_the_predicate_fails_on_a_postcode_point_count_violation() -> None:
    """Re-asserts the N>=3 invariant at the PUBLISHED grain -- a bug between derivation
    and the staged EXCHANGE publish is exactly what this term exists to catch."""
    assert not centroid_fallback_lands_in_the_right_area(
        postcode_centroid_rows=500,
        city_centroid_rows=120,
        postcode_point_count_violations=1,
        city_point_count_violations=0,
        sample_size=480,
        out_of_area_count=0,
    )


def test_the_predicate_fails_on_a_city_point_count_violation() -> None:
    assert not centroid_fallback_lands_in_the_right_area(
        postcode_centroid_rows=500,
        city_centroid_rows=120,
        postcode_point_count_violations=0,
        city_point_count_violations=1,
        sample_size=480,
        out_of_area_count=0,
    )


def test_the_predicate_fails_on_an_empty_postcode_table() -> None:
    assert not centroid_fallback_lands_in_the_right_area(
        postcode_centroid_rows=0,
        city_centroid_rows=120,
        postcode_point_count_violations=0,
        city_point_count_violations=0,
        sample_size=0,
        out_of_area_count=0,
    )


def test_the_predicate_fails_on_an_empty_city_table() -> None:
    assert not centroid_fallback_lands_in_the_right_area(
        postcode_centroid_rows=500,
        city_centroid_rows=0,
        postcode_point_count_violations=0,
        city_point_count_violations=0,
        sample_size=0,
        out_of_area_count=0,
    )


def test_the_predicate_fails_when_the_mapping_matched_nothing() -> None:
    """Both tables populated and clean, but the postcode->city join produced no pair at
    all -- a 0/0 fraction must not read as a silent 0% pass."""
    assert not centroid_fallback_lands_in_the_right_area(
        postcode_centroid_rows=500,
        city_centroid_rows=120,
        postcode_point_count_violations=0,
        city_point_count_violations=0,
        sample_size=0,
        out_of_area_count=0,
    )


def test_the_postcode_city_map_groups_on_the_shared_join_keys() -> None:
    """The mapping is keyed identically to the derivation/serving side (centroid_keys.py),
    grouped BEFORE the vote so two spellings of one postcode or city collapse first."""
    assert QUALIFIED_CLICKHOUSE_SHARED_ADDRESSES_TABLE in POSTCODE_CITY_MAP_SQL
    assert "argMax(city_key, n)" in POSTCODE_CITY_MAP_SQL
    assert "GROUP BY postcode_key, city_key" in POSTCODE_CITY_MAP_SQL
    assert "GROUP BY postcode_key" in POSTCODE_CITY_MAP_SQL
    # The accent-preserving/digits-only key fragments, applied to the raw address columns.
    assert "postal_code" in POSTCODE_CITY_MAP_SQL
    assert "post_town" in POSTCODE_CITY_MAP_SQL
    assert "upper(trim(coalesce(post_town" in POSTCODE_CITY_MAP_SQL


def test_the_area_sanity_sample_joins_both_centroid_tables_through_the_map() -> None:
    assert f"FROM {QUALIFIED_POSTCODE_CENTROIDS_TABLE} AS pc" in AREA_SANITY_SAMPLE_SQL
    assert (
        f"INNER JOIN {QUALIFIED_CITY_CENTROIDS_TABLE} AS cc" in AREA_SANITY_SAMPLE_SQL
    )
    assert "INNER JOIN postcode_city_map AS map" in AREA_SANITY_SAMPLE_SQL
    assert f"> {AREA_SANITY_METERS}" in AREA_SANITY_SAMPLE_SQL
    # The haversine fragment: same shape as centroid_derivation._HAVERSINE_METERS,
    # comparing pc.* against cc.* rather than a point against its own group.
    for term in ("asin(sqrt(", "radians(pc.latitude - cc.latitude)", "6371000"):
        assert term in AREA_SANITY_SAMPLE_SQL


def test_the_invariant_queries_read_the_min_centroid_points_gate() -> None:
    assert f"point_count < {MIN_CENTROID_POINTS}" in POSTCODE_CENTROID_INVARIANTS_SQL
    assert f"FROM {QUALIFIED_POSTCODE_CENTROIDS_TABLE}" in (
        POSTCODE_CENTROID_INVARIANTS_SQL
    )
    assert f"point_count < {MIN_CENTROID_POINTS}" in CITY_CENTROID_INVARIANTS_SQL
    assert f"FROM {QUALIFIED_CITY_CENTROIDS_TABLE}" in CITY_CENTROID_INVARIANTS_SQL


def test_the_check_is_registered_on_the_centroids_asset() -> None:
    assert {
        key.asset_key for key in sweden_geocode_centroid_area_sanity_check.check_keys
    } == {dg.AssetKey("sweden_geocode_centroids_clickhouse")}
    assert sweden_geocode_centroid_area_sanity_check.check_keys == {
        dg.AssetCheckKey(
            dg.AssetKey("sweden_geocode_centroids_clickhouse"),
            "centroid_fallback_lands_in_the_right_area",
        )
    }
    assert sweden_geocode_centroids_clickhouse.key == dg.AssetKey(
        "sweden_geocode_centroids_clickhouse"
    )


class _FakeAreaSanityClient:
    """Answers a query by whichever SQL constant it equals, and records every statement.

    Same shape as `tests/test_sweden_geocode_checks.py`'s `_FakeClickhouseClient`: keys
    are whole SQL constants (each of the three queries this check runs has a distinct
    shape, so an exact-constant key never collides), and the single-element unpacking
    fails loudly if a query answers twice or not at all.
    """

    def __init__(self, answers: dict[str, list[tuple[Any, ...]]]) -> None:
        self.answers = answers
        self.executed: list[str] = []

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        self.executed.append(sql)
        [answer] = [rows for key, rows in self.answers.items() if key == sql]
        return answer


class _FakeClickhouseResource:
    def __init__(self, client: Any) -> None:
        self._client = client

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        yield self._client


def _run_area_sanity_check(
    *,
    postcode_invariants: tuple[int, int] = (500, 0),
    city_invariants: tuple[int, int] = (120, 0),
    sample: tuple[int, int] = (480, 0),
) -> dict[str, Any]:
    client = _FakeAreaSanityClient(
        {
            POSTCODE_CENTROID_INVARIANTS_SQL: [postcode_invariants],
            CITY_CENTROID_INVARIANTS_SQL: [city_invariants],
            AREA_SANITY_SAMPLE_SQL: [sample],
        }
    )
    result = (
        sweden_geocode_centroid_area_sanity_check.node_def.compute_fn.decorated_fn(
            _FakeClickhouseResource(client)
        )
    )
    assert set(client.executed) == {
        POSTCODE_CENTROID_INVARIANTS_SQL,
        CITY_CENTROID_INVARIANTS_SQL,
        AREA_SANITY_SAMPLE_SQL,
    }
    return {
        "passed": result.passed,
        **{key: value.value for key, value in result.metadata.items()},
    }


def test_the_check_passes_the_happy_path() -> None:
    result = _run_area_sanity_check()

    assert result["passed"]
    assert result["sample_size"] == 480
    assert result["out_of_area_count"] == 0
    assert result["out_of_area_fraction"] == 0.0
    assert result["area_sanity_meters"] == AREA_SANITY_METERS
    assert result["max_out_of_area_fraction"] == MAX_OUT_OF_AREA_FRACTION


def test_the_check_fails_when_a_postcode_centroid_lands_200km_from_its_city() -> None:
    """The negative branch, exercised through the check wrapper: a live
    AREA_SANITY_SAMPLE_SQL that found one pair (of ten) over AREA_SANITY_METERS apart --
    a postcode centroid 200km from its own city's centroid -- fails the check."""
    result = _run_area_sanity_check(sample=(10, 1))

    assert not result["passed"]
    assert result["sample_size"] == 10
    assert result["out_of_area_count"] == 1
    assert result["out_of_area_fraction"] == 0.1


def test_the_check_fails_on_a_point_count_less_than_three_row() -> None:
    result = _run_area_sanity_check(postcode_invariants=(500, 3))

    assert not result["passed"]
    assert result["postcode_point_count_violations"] == 3


def test_the_check_fails_on_an_empty_centroid_table() -> None:
    result = _run_area_sanity_check(city_invariants=(0, 0), sample=(0, 0))

    assert not result["passed"]
    assert result["city_centroid_rows"] == 0
