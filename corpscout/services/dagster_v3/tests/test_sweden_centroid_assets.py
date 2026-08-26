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
"""

from datetime import datetime

import duckdb

from dagster_v3.defs.sweden_company.centroid_assets import (
    CITY_CENTROIDS_TABLE,
    POSTCODE_CENTROIDS_TABLE,
    QUALIFIED_CITY_CENTROIDS_TABLE,
    QUALIFIED_POSTCODE_CENTROIDS_TABLE,
    publish_sweden_geocode_centroids,
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
