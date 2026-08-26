"""Derive and publish the SE coarse-centroid reference tables.

`sweden_geocode_centroids_clickhouse` rebuilds two robust reference tables --
one postcode centroid per postal code, one city centroid per post town --
from the OSM address points (`sweden_address_osm.address_points`) and
publishes them atomically to ClickHouse (`corpscout.se_postcode_centroids`,
`corpscout.se_city_centroids`).

Serving-overlay architecture (see the plan's Global Constraints): these are
SEPARATE reference tables, NOT the geocode store. The serving read LEFT-JOINs
them at read time to fill `unmatched`/`ambiguous` identities that carry a
postcode or city; the store itself keeps only precise outcomes.

The derivation is the robust median-centroid with a >=3-point quality gate
(`centroid_derivation.py`); the join keys preserve Swedish letters
(`centroid_keys.py`). `address_points.city` is the SE post-town column, so it
is mapped to the `post_town` name the derivation contract expects when the
staging source-points table is built.

Publishing goes through `export_duckdb_connection_table_to_clickhouse` with
`truncate=True`: each table is staged into a `_tmp_*` table and swapped in with
a single `EXCHANGE TABLES`, and `allow_empty` stays False, so a 0-row build
raises rather than clobbering good reference data -- live readers never see a
partial or empty table.
"""

from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.sweden_address_osm import tables as osm_tables
from dagster_v3.defs.sweden_company import centroid_derivation

GROUP_NAME = "sweden_company"

CENTROIDS_ASSET_KEY = "sweden_geocode_centroids_clickhouse"
OSM_ADDRESSES_DUCKDB_ASSET_KEY = "sweden_osm_addresses_duckdb"

# ClickHouse reference tables (Task 3 migrations 000323/000324).
POSTCODE_CENTROIDS_TABLE = "se_postcode_centroids"
CITY_CENTROIDS_TABLE = "se_city_centroids"
QUALIFIED_POSTCODE_CENTROIDS_TABLE = f"{RESOLVED_DATABASE}.{POSTCODE_CENTROIDS_TABLE}"
QUALIFIED_CITY_CENTROIDS_TABLE = f"{RESOLVED_DATABASE}.{CITY_CENTROIDS_TABLE}"

# DuckDB staging inside the shared OSM workbench file. The derivation output
# tables are named exactly like their ClickHouse targets so the publish helper
# reads `{CENTROID_DUCKDB_SCHEMA}.{table}` with the matching table name.
CENTROID_DUCKDB_SCHEMA = "sweden_geocode_centroids"
SOURCE_POINTS_TABLE = f"{CENTROID_DUCKDB_SCHEMA}.centroid_source_points"

MIN_CENTROID_POINTS = 3

# key, latitude, longitude, point_count, spread_meters come from the derivation
# output; source_snapshot_at is stamped from the OSM snapshot before publish.
CENTROID_EXPORT_COLUMNS = (
    "key",
    "latitude",
    "longitude",
    "point_count",
    "spread_meters",
    "source_snapshot_at",
)


def _qualified_out_table(clickhouse_table: str) -> str:
    return f"{CENTROID_DUCKDB_SCHEMA}.{clickhouse_table}"


def publish_sweden_geocode_centroids(
    *,
    connection: Any,
    clickhouse_client: Any,
    min_points: int = MIN_CENTROID_POINTS,
    log: Any = None,
) -> dict[str, int]:
    """Derive both centroid tables in DuckDB and publish them to ClickHouse.

    Builds a `(postcode, post_town, latitude, longitude)` staging table from
    `sweden_address_osm.address_points` -- mapping the OSM `city` column to the
    `post_town` name the derivation expects -- derives the robust postcode and
    city centroids, stamps each with the OSM snapshot time, and publishes each
    to ClickHouse via the staged-`EXCHANGE` helper (atomic, refuses an empty
    replace). Returns the published row counts and the OSM snapshot time.
    """
    connection.execute(f"create schema if not exists {CENTROID_DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create or replace table {SOURCE_POINTS_TABLE} as
        select
            postcode,
            city as post_town,
            latitude,
            longitude
        from {osm_tables.QUALIFIED_ADDRESS_TABLE}
        """
    )
    [(source_snapshot_at,)] = connection.execute(
        f"select max(source_snapshot_at) from {osm_tables.QUALIFIED_ADDRESS_TABLE}"
    ).fetchall()

    postcode_out = _qualified_out_table(POSTCODE_CENTROIDS_TABLE)
    city_out = _qualified_out_table(CITY_CENTROIDS_TABLE)
    postcode_keys = centroid_derivation.replace_postcode_centroids(
        connection,
        source_points_table=SOURCE_POINTS_TABLE,
        out_table=postcode_out,
        min_points=min_points,
    )
    city_keys = centroid_derivation.replace_city_centroids(
        connection,
        source_points_table=SOURCE_POINTS_TABLE,
        out_table=city_out,
        min_points=min_points,
    )
    for out_table in (postcode_out, city_out):
        connection.execute(
            f"alter table {out_table} add column source_snapshot_at timestamp"
        )
        connection.execute(
            f"update {out_table} set source_snapshot_at = ?", [source_snapshot_at]
        )

    published_postcode = export_duckdb_connection_table_to_clickhouse(
        duckdb_connection=connection,
        clickhouse_client=clickhouse_client,
        duckdb_schema=CENTROID_DUCKDB_SCHEMA,
        duckdb_table=POSTCODE_CENTROIDS_TABLE,
        clickhouse_database=RESOLVED_DATABASE,
        clickhouse_table=POSTCODE_CENTROIDS_TABLE,
        columns=CENTROID_EXPORT_COLUMNS,
        truncate=True,
        log=log,
    )
    published_city = export_duckdb_connection_table_to_clickhouse(
        duckdb_connection=connection,
        clickhouse_client=clickhouse_client,
        duckdb_schema=CENTROID_DUCKDB_SCHEMA,
        duckdb_table=CITY_CENTROIDS_TABLE,
        clickhouse_database=RESOLVED_DATABASE,
        clickhouse_table=CITY_CENTROIDS_TABLE,
        columns=CENTROID_EXPORT_COLUMNS,
        truncate=True,
        log=log,
    )
    return {
        "postcode_centroid_keys": postcode_keys,
        "city_centroid_keys": city_keys,
        "postcode_centroids_published": published_postcode,
        "city_centroids_published": published_city,
        "source_snapshot_at": source_snapshot_at,
    }


@dg.asset(
    key=CENTROIDS_ASSET_KEY,
    deps=[dg.AssetKey(OSM_ADDRESSES_DUCKDB_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "openstreetmap"},
    pool=osm_tables.DUCKDB_POOL,
    metadata={
        "postcode_centroids_table": QUALIFIED_POSTCODE_CENTROIDS_TABLE,
        "city_centroids_table": QUALIFIED_CITY_CENTROIDS_TABLE,
    },
    description=(
        "Derives robust postcode and city centroids from the Sweden OSM address "
        "points and atomically republishes the two coarse-centroid reference "
        "tables (corpscout.se_postcode_centroids, corpscout.se_city_centroids) "
        "the geocode serving overlay reads."
    ),
)
def sweden_geocode_centroids_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=(POSTCODE_CENTROIDS_TABLE, CITY_CENTROIDS_TABLE),
    )
    with sweden_address_osm_duckdb.get_connection() as connection:
        with clickhouse.get_connection() as clickhouse_client:
            result = publish_sweden_geocode_centroids(
                connection=connection,
                clickhouse_client=clickhouse_client,
                log=context.log.info,
            )
    source_snapshot_at = result["source_snapshot_at"]
    return dg.MaterializeResult(
        metadata={
            "postcode_centroid_keys": result["postcode_centroid_keys"],
            "city_centroid_keys": result["city_centroid_keys"],
            "postcode_centroids_published": result["postcode_centroids_published"],
            "city_centroids_published": result["city_centroids_published"],
            "postcode_centroids_table": QUALIFIED_POSTCODE_CENTROIDS_TABLE,
            "city_centroids_table": QUALIFIED_CITY_CENTROIDS_TABLE,
            "source_snapshot_at": (
                source_snapshot_at.isoformat()
                if source_snapshot_at is not None
                else None
            ),
        }
    )
