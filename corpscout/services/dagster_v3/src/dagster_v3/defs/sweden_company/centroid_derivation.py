"""Robust postcode/city centroid derivation from SE address points.

Pure DuckDB SQL, ported from the proven measurement logic in
`scripts/geocode_centroid_coverage_experiment.py` (the coarse-centroid
coverage experiment). Two differences from that script, both required by
the design spec (see the plan's Global Constraints):

- The centroid is `median(latitude)`/`median(longitude)` -- a ROBUST
  centroid -- not `avg(...)`. A single gross outlier point must not move
  the group's assigned coordinate; a plain mean would let it.
- Groups are gated to `count(*) >= min_points` (default 3): a centroid
  computed from one or two points is not trustworthy enough to serve.

`spread_meters` is the MAX haversine distance from any source point in the
group to the group's centroid, so a poor (widely-scattered) centroid can be
filtered or downgraded downstream.

The join keys (`city_key_sql`, `postcode_key_sql`) come from
`centroid_keys.py` (Task 1) and are applied identically here (the DuckDB
derivation side) and on the ClickHouse serving side, so the two engines
compute the same key string for the same input.
"""

from typing import Any

from dagster_v3.defs.sweden_company.centroid_keys import city_key_sql, postcode_key_sql

# 6371000 = Earth's mean radius in meters.
_HAVERSINE_METERS = (
    "6371000 * 2 * asin(sqrt("
    "pow(sin(radians(b.latitude - c.latitude) / 2), 2) "
    "+ cos(radians(b.latitude)) * cos(radians(c.latitude)) "
    "* pow(sin(radians(b.longitude - c.longitude) / 2), 2)))"
)


def _replace_centroids(
    connection: Any,
    *,
    key_sql: str,
    source_points_table: str,
    out_table: str,
    min_points: int,
) -> int:
    """Derive one robust centroid per `key_sql` group and materialize it into
    `out_table` as `(key, latitude, longitude, point_count, spread_meters)`.

    Returns the row count of `out_table` (one row per key that cleared the
    `min_points` gate).
    """
    connection.execute(
        f"""
        create or replace table {out_table} as
        with base as (
            select {key_sql} as key, latitude, longitude
            from {source_points_table}
            where latitude is not null and longitude is not null and {key_sql} != ''
        ),
        centroids as (
            select key, median(latitude) as latitude, median(longitude) as longitude,
                   count(*) as point_count
            from base
            group by key
            having count(*) >= {int(min_points)}
        )
        select c.key, c.latitude, c.longitude, c.point_count,
               max({_HAVERSINE_METERS}) as spread_meters
        from base b
        join centroids c using (key)
        group by c.key, c.latitude, c.longitude, c.point_count
        """
    )
    [(row_count,)] = connection.execute(f"select count(*) from {out_table}").fetchall()
    return int(row_count)


def replace_postcode_centroids(
    connection: Any,
    *,
    source_points_table: str,
    out_table: str,
    min_points: int = 3,
) -> int:
    """(Re)build `out_table` with one robust centroid per postcode.

    `source_points_table` must expose `postcode`, `post_town`, `latitude`,
    `longitude` columns (only `postcode`/`latitude`/`longitude` are read
    here). Returns the number of postcodes that cleared the `min_points`
    gate.
    """
    return _replace_centroids(
        connection,
        key_sql=postcode_key_sql("postcode"),
        source_points_table=source_points_table,
        out_table=out_table,
        min_points=min_points,
    )


def replace_city_centroids(
    connection: Any,
    *,
    source_points_table: str,
    out_table: str,
    min_points: int = 3,
) -> int:
    """(Re)build `out_table` with one robust centroid per city (post_town).

    `source_points_table` must expose `postcode`, `post_town`, `latitude`,
    `longitude` columns (only `post_town`/`latitude`/`longitude` are read
    here). Returns the number of cities that cleared the `min_points` gate.
    """
    return _replace_centroids(
        connection,
        key_sql=city_key_sql("post_town"),
        source_points_table=source_points_table,
        out_table=out_table,
        min_points=min_points,
    )
