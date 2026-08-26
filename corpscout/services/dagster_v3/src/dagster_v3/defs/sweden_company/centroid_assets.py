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

`centroid_fallback_lands_in_the_right_area` (below) is the correctness check on the
published tables: it re-asserts the >=3-point invariant at the published grain (a bug
between derivation and publish could still land a thin centroid), and asserts each
postcode centroid sits within a sane distance of its OWN city's centroid -- a postcode
centroid far from the city that postcode belongs to is bad data, not a coarse-but-honest
answer. There is no stored postcode->city mapping to join on, so it is derived from
`se_addresses_current` (the postcode's most-common post_town), keyed identically to the
derivation/serving side via `centroid_keys.py`.
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
from dagster_v3.defs.sweden_company import centroid_derivation, shared_addresses
from dagster_v3.defs.sweden_company.centroid_keys import city_key_sql, postcode_key_sql

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


# --------------------------------------------------------------------------------------
# Task 7: `centroid_fallback_lands_in_the_right_area` -- the published-table correctness
# check. Everything above this line derives and publishes the two reference tables;
# everything below asserts the result is sane, against ClickHouse, on a schedule
# independent of any one materialization.

# A postcode centroid this far from its OWN city's centroid cannot be right: Sweden's
# largest municipalities span tens of km, so 50km comfortably covers a postcode at the
# edge of a large city while still catching a centroid that landed in the wrong town
# entirely (a join-key collision, a data entry error in the source points, ...).
AREA_SANITY_METERS = 50_000.0

# A LOOSE budget, not a quality bar: a handful of postcodes legitimately straddling a
# municipal boundary (or a rare bad OSM point that still cleared the >=3-point gate)
# must not fail the whole reference table. More than 1% wildly out of area is no longer
# noise -- it means the postcode<->city mapping or the derivation itself is broken.
MAX_OUT_OF_AREA_FRACTION = 0.01

def _haversine_meters_sql(*, lat1: str, lon1: str, lat2: str, lon2: str) -> str:
    """Great-circle distance in meters between two lat/lon SQL expressions.

    Ported from `centroid_derivation._HAVERSINE_METERS` (itself ported from
    `scripts/geocode_centroid_coverage_experiment.py`) and parameterized: that fragment
    always compares a source point against its OWN group's centroid (`b.*` vs `c.*`),
    while this check compares two DIFFERENT tables' centroids (a postcode centroid
    against its city centroid), so the column expressions on each side have to be
    supplied rather than fixed aliases. 6371000 = Earth's mean radius in meters.
    """
    return (
        "6371000 * 2 * asin(sqrt("
        f"pow(sin(radians({lat1} - {lat2}) / 2), 2) "
        f"+ cos(radians({lat1})) * cos(radians({lat2})) "
        f"* pow(sin(radians({lon1} - {lon2}) / 2), 2)))"
    )


# There is no stored postcode->city mapping to join the two reference tables through, so
# one is derived here from `se_addresses_current`: for each postcode KEY, the city KEY it
# occurs with most often (`argMax(city_key, n)`). Grouping happens on the KEYS
# (`postcode_key_sql`/`city_key_sql` -- the identical fragments the derivation and the
# serving overlay use), not on the raw columns, so two spellings of the same postcode or
# city collapse into one bucket before the vote is taken -- matching exactly what the
# join below compares.
POSTCODE_CITY_MAP_SQL = f"""WITH by_key AS (
    SELECT
        {postcode_key_sql("postal_code")} AS postcode_key,
        {city_key_sql("post_town")} AS city_key,
        count() AS n
    FROM {shared_addresses.QUALIFIED_CLICKHOUSE_SHARED_ADDRESSES_TABLE}
    WHERE postal_code != '' AND post_town != ''
    GROUP BY postcode_key, city_key
)
SELECT postcode_key, argMax(city_key, n) AS city_key
FROM by_key
GROUP BY postcode_key"""

_AREA_SANITY_DISTANCE_METERS_SQL = _haversine_meters_sql(
    lat1="pc.latitude", lon1="pc.longitude", lat2="cc.latitude", lon2="cc.longitude"
)

# The sample: every postcode centroid whose postcode maps to a city that also has a
# published city centroid (an INNER JOIN on both hops -- a postcode with no mapped city,
# or a mapped city with no centroid of its own, contributes no pair and is not part of
# what this check can measure). `sample_size` is the pair count and `out_of_area_count`
# is how many of those pairs are farther apart than AREA_SANITY_METERS.
AREA_SANITY_SAMPLE_SQL = f"""WITH postcode_city_map AS (
{POSTCODE_CITY_MAP_SQL}
)
SELECT
    count(),
    countIf({_AREA_SANITY_DISTANCE_METERS_SQL} > {AREA_SANITY_METERS})
FROM {QUALIFIED_POSTCODE_CENTROIDS_TABLE} AS pc
INNER JOIN postcode_city_map AS map ON map.postcode_key = pc.key
INNER JOIN {QUALIFIED_CITY_CENTROIDS_TABLE} AS cc ON cc.key = map.city_key"""

# Re-asserts the >=3-point gate `centroid_derivation._replace_centroids` enforces at
# DERIVATION time, against the PUBLISHED table instead -- a check that only ever read the
# DuckDB build could never catch a bug introduced between derivation and the staged
# `EXCHANGE` publish. `count()` doubles as the "is this table empty" signal: a 0-row
# table can only ever report 0 violations, so it is asserted separately rather than
# folded into the violation term (see `centroid_fallback_lands_in_the_right_area`).
POSTCODE_CENTROID_INVARIANTS_SQL = f"""SELECT
    count(),
    countIf(point_count < {MIN_CENTROID_POINTS})
FROM {QUALIFIED_POSTCODE_CENTROIDS_TABLE}"""

CITY_CENTROID_INVARIANTS_SQL = f"""SELECT
    count(),
    countIf(point_count < {MIN_CENTROID_POINTS})
FROM {QUALIFIED_CITY_CENTROIDS_TABLE}"""


def centroid_fallback_lands_in_the_right_area(
    *,
    postcode_centroid_rows: int,
    city_centroid_rows: int,
    postcode_point_count_violations: int,
    city_point_count_violations: int,
    sample_size: int,
    out_of_area_count: int,
) -> bool:
    """The pure predicate: given already-fetched counts, is the published pair sane?

    Order matters for what a failure MEANS, not for correctness -- every term is
    independent -- but checking the invariants before the distance term means a caller
    reading top-to-bottom hits the more fundamental failure (a broken/empty table) before
    the softer one (a scatter of implausible pairs).

    `sample_size == 0` is ALSO a failure, not a vacuous pass: an out-of-area FRACTION of
    0/0 would otherwise read as "0% out of area" when it actually means the postcode<->
    city mapping matched nothing at all -- itself a sign something upstream is broken --
    and this check would then pass without having compared a single pair.
    """
    if postcode_centroid_rows == 0 or city_centroid_rows == 0:
        return False
    if postcode_point_count_violations > 0 or city_point_count_violations > 0:
        return False
    if sample_size == 0:
        return False
    return (out_of_area_count / sample_size) <= MAX_OUT_OF_AREA_FRACTION


@dg.asset_check(
    asset=sweden_geocode_centroids_clickhouse,
    name="centroid_fallback_lands_in_the_right_area",
    description=(
        "Fails when a published postcode centroid sits implausibly far from its own "
        "city's centroid (>1% of the sample farther than 50km), or when either "
        "centroid table holds a sub-invariant (point_count < 3) row or is empty."
    ),
)
def sweden_geocode_centroid_area_sanity_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """Correctness, not freshness: does a rescued coordinate land in the right area?

    The N>=3 quality gate (Task 2) and the accent-preserving join keys (Task 1) both
    guard the DERIVATION; nothing upstream of this check asserts the published tables
    still agree with each other. A postcode centroid computed from good points can still
    be WRONG in a way none of that catches -- a join-key collision that grouped two
    different towns' points under one key, or a postcode whose OSM points genuinely
    belong to a different town than its name-brand city (a rural postcode assigned to a
    distant municipal seat). Comparing each postcode centroid to its OWN city's centroid
    catches exactly that shape of bug: two robust, well-supported centroids that simply
    do not agree on where the area is.
    """
    with clickhouse.get_connection() as client:
        [(postcode_rows, postcode_violations)] = client.execute(
            POSTCODE_CENTROID_INVARIANTS_SQL
        )
        [(city_rows, city_violations)] = client.execute(CITY_CENTROID_INVARIANTS_SQL)
        [(sample_size, out_of_area_count)] = client.execute(AREA_SANITY_SAMPLE_SQL)

    postcode_rows = int(postcode_rows)
    city_rows = int(city_rows)
    postcode_violations = int(postcode_violations)
    city_violations = int(city_violations)
    sample_size = int(sample_size)
    out_of_area_count = int(out_of_area_count)

    passed = centroid_fallback_lands_in_the_right_area(
        postcode_centroid_rows=postcode_rows,
        city_centroid_rows=city_rows,
        postcode_point_count_violations=postcode_violations,
        city_point_count_violations=city_violations,
        sample_size=sample_size,
        out_of_area_count=out_of_area_count,
    )
    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "postcode_centroid_rows": postcode_rows,
            "city_centroid_rows": city_rows,
            "postcode_point_count_violations": postcode_violations,
            "city_point_count_violations": city_violations,
            "sample_size": sample_size,
            "out_of_area_count": out_of_area_count,
            "out_of_area_fraction": (
                out_of_area_count / sample_size if sample_size else None
            ),
            "area_sanity_meters": AREA_SANITY_METERS,
            "max_out_of_area_fraction": MAX_OUT_OF_AREA_FRACTION,
            "min_centroid_points": MIN_CENTROID_POINTS,
        },
    )
