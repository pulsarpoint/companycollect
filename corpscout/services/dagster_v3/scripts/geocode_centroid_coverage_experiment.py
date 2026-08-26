r"""MEASUREMENT ONLY: coarse-centroid (postcode / city) fallback coverage experiment.

Quantifies how many of the currently-unmatched Sweden addresses a COARSE-CENTROID
fallback tier would rescue -- assigning an AREA coordinate (the average of nearby OSM
address points sharing a postcode, or sharing a city/post_town) when v6/v7's precise
street-level matcher finds nothing. This is a scoping experiment for a fallback tier
that does not exist yet: no production code is touched, nothing is written back to the
geocode store, and no Dagster asset runs. See
``.superpowers/sdd/2026-08-24-se-geocode-simplification/centroid-coverage-brief.md``.

Motivating case (from the brief): STAVSTENSV 3, postal_code 23100, post_town TRELLEBORG.
Postcode 23100 has ZERO OSM address points (its street is absent from OSM -- the ~500k
residual-unmatched gap no street matcher fixes), but Trelleborg the CITY has hundreds of
OSM address points elsewhere in town, so a city-centroid gives a useful, honestly-coarse
coordinate. This script measures exactly that ladder: postcode-centroid (fine, but the
worst gaps have zero local points) vs city-centroid (broad rescue, coarser precision).

What it does
------------
1. Pulls a FRESH unmatched pool from prod ClickHouse (SELECT only): every
   ``se_addresses_current`` (SE) identity whose current servable
   ``se_address_geocodes`` outcome is NOT in ``geocode_store.GEOCODED_STATUSES``, with
   its ``match_status``, ``normalized_postal_code``, ``normalized_post_town``. Bounds
   the fallback's reach by postal_code/post_town presence and match_status.
2. Derives postcode- and city-centroids from the LOCAL workbench's
   ``sweden_address_osm.address_points`` (the CH se_osm_* tables were dropped -- OSM
   coords only exist locally). Reports the per-group point count and coordinate-spread
   distribution (mean/median/max haversine distance from each point to the group's
   centroid) at N>=1, N>=3, N>=10 thresholds.
3. Derives an ALTERNATE city-centroid from the CH matched-geocode pool (addresses the
   real matcher already placed, grouped by their own post_town) as a second source,
   since the matched pool's spatial distribution is exactly the target distribution.
4. LADDER: for each unmatched address, the finest available coarse coordinate --
   postcode-centroid (if the postcode clears the quality bar) else city-centroid (OSM
   or CH-matched, whichever is available) else nothing. Reports cumulative coverage and
   the precision breakdown, at each N-threshold.
5. Correctness sanity: post_town strings that collide across OSM cities/that are absent
   from OSM city names entirely (abbreviation gaps, e.g. "UPP VÄSBY" vs "UPPLANDS
   VÄSBY"), plus a spot sample of rescued addresses with their assigned centroid for
   eyeball plausibility (does postal_code 23100 -> a point near Trelleborg?).

Run::

    uv run python scripts/geocode_centroid_coverage_experiment.py --help
    uv run python scripts/geocode_centroid_coverage_experiment.py
    uv run python scripts/geocode_centroid_coverage_experiment.py --refresh-pool

Credentials come from ``.env`` (``CLICKHOUSE_HOST`` / ``_HTTP_PORT`` / ``_USER`` /
``_PASSWORD`` / ``_DATABASE``); ClickHouse access is SELECT-only throughout.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

_SERVICE_ROOT = Path(__file__).resolve().parents[1]
if str(_SERVICE_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_SERVICE_ROOT / "src"))

import duckdb
from dotenv import load_dotenv

from dagster_v3.defs.sweden_address_osm import tables as osm_tables
from dagster_v3.defs.sweden_company.address_canonicalization import ENRICHMENT_SCHEMA
from dagster_v3.defs.sweden_company.geocode_store import (
    GEOCODED_STATUSES,
    build_current_geocodes_sql,
)

WORKBENCH_PATH = _SERVICE_ROOT / "data" / "geocode_workbench_local.duckdb"

_GEOCODED_SET_SQL = ", ".join(f"'{status}'" for status in GEOCODED_STATUSES)
_QUALIFIED_ADDRESS_POINTS = osm_tables.QUALIFIED_ADDRESS_TABLE

UNMATCHED_POOL_TABLE = f"{ENRICHMENT_SCHEMA}.se_centroid_unmatched_pool"
MATCHED_ALT_TABLE = f"{ENRICHMENT_SCHEMA}.se_centroid_matched_alt"
POSTCODE_STATS_TABLE = f"{ENRICHMENT_SCHEMA}.se_centroid_postcode_stats"
CITY_STATS_OSM_TABLE = f"{ENRICHMENT_SCHEMA}.se_centroid_city_stats_osm"
CITY_STATS_ALT_TABLE = f"{ENRICHMENT_SCHEMA}.se_centroid_city_stats_alt"
LADDER_TABLE = f"{ENRICHMENT_SCHEMA}.se_centroid_ladder"

N_THRESHOLDS = (1, 3, 10)
# The threshold used for the single headline ladder (item 4's "finest available, chosen
# threshold" number). Sensitivity at 1/3/10 is reported separately (item 2/3).
HEADLINE_N = 3

SPREAD_BUCKETS_M = (100, 500, 1_000, 5_000, 25_000)


def _log(message: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)


# --------------------------------------------------------------------------- #
# ClickHouse pulls (SELECT only)                                              #
# --------------------------------------------------------------------------- #


def _clickhouse_client() -> Any:
    import os

    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ.get("CLICKHOUSE_DATABASE", "corpscout"),
    )


def _load_table(
    *, connection: Any, clickhouse_client: Any, select_sql: str, target_table: str
) -> int:
    started = time.monotonic()
    arrow_table = clickhouse_client.query_arrow(select_sql)
    view = "_ch_pull_arrow"
    connection.register(view, arrow_table)
    try:
        connection.execute(
            f"create or replace table {target_table} as select * from {view}"
        )
    finally:
        connection.unregister(view)
    [(count,)] = connection.execute(f"select count(*) from {target_table}").fetchall()
    _log(
        f"loaded {target_table}: rows={int(count)} "
        f"elapsed={time.monotonic() - started:.1f}s"
    )
    return int(count)


def _unmatched_pool_sql() -> str:
    """The full UNMATCHED pool: SE identities not currently servable, WITH match_status.

    Reuses geocode_store.build_current_geocodes_sql -- the store's own two-stage
    servable read -- rather than re-deriving the ranking, so this always agrees with
    what production considers "the current outcome for an identity".
    """
    servable = build_current_geocodes_sql(columns=["address_id", "match_status"])
    return f"""
        SELECT
            toString(a.address_id) AS address_id,
            unmatched.match_status AS match_status,
            a.street_name AS street_name,
            a.postal_code AS postal_code,
            a.post_town AS post_town,
            a.normalized_postal_code AS normalized_postal_code,
            a.normalized_post_town AS normalized_post_town
        FROM (
            SELECT address_id, match_status
            FROM (
{servable}
            ) AS servable
            WHERE match_status NOT IN ({_GEOCODED_SET_SQL})
        ) AS unmatched
        INNER JOIN corpscout.se_addresses_current a
            ON a.address_id = unmatched.address_id
        WHERE a.country_code = 'SE'
    """


def _matched_alt_sql() -> str:
    """GEOCODED addresses with their served lat/lon + post_town: the alternate city-centroid
    source. Spatial distribution of THIS pool is exactly the distribution we want for a
    city-centroid, since it is other real company addresses, not raw OSM density.

    Selects the RAW ``post_town`` (not ``normalized_post_town``): production's
    normalization regex (``address_canonicalization.py``, ``[^[:alnum:]]+``) is
    ASCII-only under ClickHouse's regex engine and strips Swedish diacritics as
    "non-alnum" -- "GÖTEBORG" normalizes to "g teborg", "UMEÅ" to "ume". That silently
    breaks any join keyed on it against OSM's diacritic-preserving city names. This
    script normalizes its own join key (``upper(trim(post_town))``, computed locally in
    DuckDB where upper/trim ARE Unicode-aware) instead of relying on the broken
    production column. See section 5's report for the measured impact of that bug.
    """
    servable = build_current_geocodes_sql(
        columns=["address_id", "match_status", "latitude", "longitude"]
    )
    return f"""
        SELECT
            toString(a.address_id) AS address_id,
            matched.match_status AS match_status,
            matched.latitude AS latitude,
            matched.longitude AS longitude,
            a.post_town AS post_town,
            a.normalized_post_town AS normalized_post_town
        FROM (
            SELECT address_id, match_status, latitude, longitude
            FROM (
{servable}
            ) AS servable
            WHERE match_status IN ({_GEOCODED_SET_SQL})
              AND latitude IS NOT NULL AND longitude IS NOT NULL
              AND NOT isNaN(latitude) AND NOT isNaN(longitude)
        ) AS matched
        INNER JOIN corpscout.se_addresses_current a
            ON a.address_id = matched.address_id
        WHERE a.country_code = 'SE'
    """


def refresh_pool(connection: Any, *, refresh: bool) -> None:
    connection.execute(f"create schema if not exists {ENRICHMENT_SCHEMA}")
    if not refresh:
        try:
            [(pool,)] = connection.execute(
                f"select count(*) from {UNMATCHED_POOL_TABLE}"
            ).fetchall()
            [(matched,)] = connection.execute(
                f"select count(*) from {MATCHED_ALT_TABLE}"
            ).fetchall()
            if pool and matched:
                _log(
                    f"pool already cached (unmatched={int(pool)}, matched_alt="
                    f"{int(matched)}); skipping ClickHouse pull (use --refresh-pool)"
                )
                _add_town_key(connection, UNMATCHED_POOL_TABLE)
                _add_town_key(connection, MATCHED_ALT_TABLE)
                return
        except duckdb.CatalogException:
            pass
    _log("pulling fresh unmatched pool + matched-alt pool from ClickHouse (SELECT only)")
    client = _clickhouse_client()
    _load_table(
        connection=connection,
        clickhouse_client=client,
        select_sql=_unmatched_pool_sql(),
        target_table=UNMATCHED_POOL_TABLE,
    )
    _load_table(
        connection=connection,
        clickhouse_client=client,
        select_sql=_matched_alt_sql(),
        target_table=MATCHED_ALT_TABLE,
    )
    _add_town_key(connection, UNMATCHED_POOL_TABLE)
    _add_town_key(connection, MATCHED_ALT_TABLE)


def _add_town_key(connection: Any, table: str) -> None:
    """``town_key`` = ``upper(trim(post_town))``, computed locally (DuckDB's upper/trim
    ARE Unicode-aware) rather than reused from CH's ``normalized_post_town`` -- see
    ``_matched_alt_sql`` docstring for the diacritics bug that makes the latter unsafe.
    """
    connection.execute(f"alter table {table} add column if not exists town_key varchar")
    connection.execute(f"update {table} set town_key = upper(trim(post_town))")


# --------------------------------------------------------------------------- #
# Centroid derivation (local workbench only -- OSM coords live only here)     #
# --------------------------------------------------------------------------- #

_HAVERSINE_M = (
    "6371000 * 2 * asin(sqrt("
    "pow(sin(radians({lat1} - {lat2}) / 2), 2) "
    "+ cos(radians({lat1})) * cos(radians({lat2})) "
    "* pow(sin(radians({lon1} - {lon2}) / 2), 2)))"
)


def build_postcode_stats(connection: Any) -> None:
    """Postcode-centroid = AVG(lat), AVG(lon) over OSM points sharing a valid 5-digit
    postcode. Spread = distance from each point to its group's centroid (mean/median/max).
    """
    connection.execute(
        f"""
        create or replace table {POSTCODE_STATS_TABLE} as
        with pts as (
            select
                regexp_replace(postcode, '\\s', '', 'g') as postcode_norm,
                latitude, longitude
            from {_QUALIFIED_ADDRESS_POINTS}
            where latitude is not null and longitude is not null
              and regexp_matches(regexp_replace(postcode, '\\s', '', 'g'), '^[0-9]{{5}}$')
        ),
        centroids as (
            select postcode_norm, avg(latitude) as lat_c, avg(longitude) as lon_c,
                   count(*) as n_points
            from pts group by postcode_norm
        ),
        dists as (
            select p.postcode_norm,
                   {_HAVERSINE_M.format(lat1='p.latitude', lon1='p.longitude', lat2='c.lat_c', lon2='c.lon_c')} as dist_m
            from pts p join centroids c using (postcode_norm)
        )
        select c.postcode_norm, c.lat_c, c.lon_c, c.n_points,
               avg(d.dist_m) as mean_dist_m,
               median(d.dist_m) as median_dist_m,
               max(d.dist_m) as max_dist_m
        from centroids c join dists d using (postcode_norm)
        group by c.postcode_norm, c.lat_c, c.lon_c, c.n_points
        """
    )
    [(n,)] = connection.execute(
        f"select count(*) from {POSTCODE_STATS_TABLE}"
    ).fetchall()
    _log(f"postcode centroids derived: {int(n)} distinct postcodes")


def build_city_stats_osm(connection: Any) -> None:
    connection.execute(
        f"""
        create or replace table {CITY_STATS_OSM_TABLE} as
        with pts as (
            select upper(trim(city)) as city_norm, latitude, longitude
            from {_QUALIFIED_ADDRESS_POINTS}
            where latitude is not null and longitude is not null
              and city is not null and trim(city) != ''
        ),
        centroids as (
            select city_norm, avg(latitude) as lat_c, avg(longitude) as lon_c,
                   count(*) as n_points
            from pts group by city_norm
        ),
        dists as (
            select p.city_norm,
                   {_HAVERSINE_M.format(lat1='p.latitude', lon1='p.longitude', lat2='c.lat_c', lon2='c.lon_c')} as dist_m
            from pts p join centroids c using (city_norm)
        )
        select c.city_norm, c.lat_c, c.lon_c, c.n_points,
               avg(d.dist_m) as mean_dist_m,
               median(d.dist_m) as median_dist_m,
               max(d.dist_m) as max_dist_m
        from centroids c join dists d using (city_norm)
        group by c.city_norm, c.lat_c, c.lon_c, c.n_points
        """
    )
    [(n,)] = connection.execute(
        f"select count(*) from {CITY_STATS_OSM_TABLE}"
    ).fetchall()
    _log(f"city centroids derived (OSM source): {int(n)} distinct cities")


def build_city_stats_alt(connection: Any) -> None:
    """Alternate city-centroid source: already-matched addresses' served coordinates,
    grouped by ``upper(trim(post_town))`` -- the same locally-computed join key used for
    the unmatched pool and the OSM city stats (NOT ``normalized_post_town``; see
    ``_matched_alt_sql`` for why that production column is unsafe to join on).
    """
    connection.execute(
        f"""
        create or replace table {CITY_STATS_ALT_TABLE} as
        with pts as (
            select upper(trim(post_town)) as city_norm, latitude, longitude
            from {MATCHED_ALT_TABLE}
            where post_town is not null and trim(post_town) != ''
        ),
        centroids as (
            select city_norm, avg(latitude) as lat_c, avg(longitude) as lon_c,
                   count(*) as n_points
            from pts group by city_norm
        ),
        dists as (
            select p.city_norm,
                   {_HAVERSINE_M.format(lat1='p.latitude', lon1='p.longitude', lat2='c.lat_c', lon2='c.lon_c')} as dist_m
            from pts p join centroids c using (city_norm)
        )
        select c.city_norm, c.lat_c, c.lon_c, c.n_points,
               avg(d.dist_m) as mean_dist_m,
               median(d.dist_m) as median_dist_m,
               max(d.dist_m) as max_dist_m
        from centroids c join dists d using (city_norm)
        group by c.city_norm, c.lat_c, c.lon_c, c.n_points
        """
    )
    [(n,)] = connection.execute(f"select count(*) from {CITY_STATS_ALT_TABLE}").fetchall()
    _log(f"city centroids derived (CH matched-alt source): {int(n)} distinct cities")


# --------------------------------------------------------------------------- #
# Reporting helpers                                                           #
# --------------------------------------------------------------------------- #


def _print_table(title: str, header: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    widths = [len(str(h)) for h in header]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(str(cell)))
    print(f"\n{title}")
    print("  " + "  ".join(str(h).ljust(widths[index]) for index, h in enumerate(header)))
    print("  " + "  ".join("-" * widths[index] for index in range(len(header))))
    for row in rows:
        print(
            "  "
            + "  ".join(str(cell).ljust(widths[index]) for index, cell in enumerate(row))
        )


def report_pool_bounds(connection: Any) -> dict[str, int]:
    [row] = connection.execute(
        f"""
        select
            count(*) as total,
            count(*) filter (where normalized_postal_code is not null
                              and normalized_postal_code != '') as has_postcode,
            count(*) filter (where normalized_post_town is not null
                              and normalized_post_town != '') as has_town,
            count(*) filter (where normalized_postal_code is not null
                              and normalized_postal_code != ''
                              and normalized_post_town is not null
                              and normalized_post_town != '') as has_both,
            count(*) filter (where (normalized_postal_code is null
                              or normalized_postal_code = '')
                              and (normalized_post_town is null
                              or normalized_post_town = '')) as has_neither
        from {UNMATCHED_POOL_TABLE}
        """
    ).fetchall()
    total, has_postcode, has_town, has_both, has_neither = row
    _print_table(
        "1. UNMATCHED POOL bounds (postal_code / post_town presence)",
        ["metric", "count", "% of pool"],
        [
            ["total unmatched (NOT in GEOCODED_STATUSES)", total, "100.0%"],
            ["has postal_code", has_postcode, f"{100.0*has_postcode/total:.1f}%"],
            ["has post_town", has_town, f"{100.0*has_town/total:.1f}%"],
            ["has both", has_both, f"{100.0*has_both/total:.1f}%"],
            ["has neither", has_neither, f"{100.0*has_neither/total:.1f}%"],
        ],
    )
    status_rows = connection.execute(
        f"""
        select match_status, count(*)
        from {UNMATCHED_POOL_TABLE}
        group by match_status order by count(*) desc
        """
    ).fetchall()
    _print_table(
        "1b. Unmatched pool by match_status",
        ["match_status", "count"],
        [[s, int(c)] for s, c in status_rows],
    )
    return {
        "total": int(total),
        "has_postcode": int(has_postcode),
        "has_town": int(has_town),
        "has_both": int(has_both),
        "has_neither": int(has_neither),
    }


def _spread_bucket_label(dist_m: float) -> str:
    for edge in SPREAD_BUCKETS_M:
        if dist_m < edge:
            return f"<{edge}m"
    return f">={SPREAD_BUCKETS_M[-1]}m"


def report_spread_distribution(connection: Any, *, stats_table: str, title: str) -> None:
    rows = connection.execute(
        f"select median_dist_m, n_points from {stats_table}"
    ).fetchall()
    from collections import Counter

    buckets_by_group: Counter[str] = Counter()
    for median_dist, _n in rows:
        buckets_by_group[_spread_bucket_label(float(median_dist))] += 1
    ordered = [f"<{e}m" for e in SPREAD_BUCKETS_M] + [f">={SPREAD_BUCKETS_M[-1]}m"]
    _print_table(
        title,
        ["median-distance-to-centroid bucket", "groups (n)"],
        [[label, buckets_by_group.get(label, 0)] for label in ordered],
    )


def report_centroid_coverage(
    connection: Any,
    *,
    stats_table: str,
    join_key_expr: str,
    label: str,
) -> None:
    rows = []
    for n in N_THRESHOLDS:
        [(addr_count,)] = connection.execute(
            f"""
            select count(*)
            from {UNMATCHED_POOL_TABLE} pool
            where exists (
                select 1 from {stats_table} s
                where s.{'postcode_norm' if 'postcode' in stats_table else 'city_norm'}
                      = ({join_key_expr})
                  and s.n_points >= {n}
            )
            """
        ).fetchall()
        [(group_count,)] = connection.execute(
            f"select count(*) from {stats_table} where n_points >= {n}"
        ).fetchall()
        rows.append([n, int(group_count), int(addr_count)])
    _print_table(
        f"{label}: coverage by minimum-point threshold N",
        ["N (min OSM/matched points per group)", "groups clearing N", "unmatched addresses reachable"],
        rows,
    )


def report_ladder(connection: Any) -> dict[str, int]:
    """Item 4: finest available coarse coordinate per unmatched address, at HEADLINE_N."""
    connection.execute(
        f"""
        create or replace table {LADDER_TABLE} as
        select
            pool.address_id,
            pool.normalized_postal_code,
            pool.town_key,
            pc.lat_c as postcode_lat, pc.lon_c as postcode_lon,
            pc.n_points as postcode_n, pc.median_dist_m as postcode_spread_m,
            city_osm.lat_c as city_osm_lat, city_osm.lon_c as city_osm_lon,
            city_osm.n_points as city_osm_n, city_osm.median_dist_m as city_osm_spread_m,
            city_alt.lat_c as city_alt_lat, city_alt.lon_c as city_alt_lon,
            city_alt.n_points as city_alt_n, city_alt.median_dist_m as city_alt_spread_m,
            case
                when pc.n_points >= {HEADLINE_N} then 'postcode'
                when city_osm.n_points >= {HEADLINE_N} or city_alt.n_points >= {HEADLINE_N}
                    then 'city'
                else 'none'
            end as rescue_precision,
            case
                when pc.n_points >= {HEADLINE_N} then pc.lat_c
                when city_osm.n_points >= {HEADLINE_N} then city_osm.lat_c
                when city_alt.n_points >= {HEADLINE_N} then city_alt.lat_c
                else null
            end as rescue_lat,
            case
                when pc.n_points >= {HEADLINE_N} then pc.lon_c
                when city_osm.n_points >= {HEADLINE_N} then city_osm.lon_c
                when city_alt.n_points >= {HEADLINE_N} then city_alt.lon_c
                else null
            end as rescue_lon,
            case
                when pc.n_points >= {HEADLINE_N} then 'osm_postcode'
                when city_osm.n_points >= {HEADLINE_N} then 'osm_city'
                when city_alt.n_points >= {HEADLINE_N} then 'ch_matched_city'
                else null
            end as rescue_source
        from {UNMATCHED_POOL_TABLE} pool
        left join {POSTCODE_STATS_TABLE} pc
            on pc.postcode_norm = pool.normalized_postal_code
        left join {CITY_STATS_OSM_TABLE} city_osm
            on city_osm.city_norm = pool.town_key
        left join {CITY_STATS_ALT_TABLE} city_alt
            on city_alt.city_norm = pool.town_key
        """
    )
    counts = connection.execute(
        f"""
        select rescue_precision, count(*)
        from {LADDER_TABLE}
        group by rescue_precision order by count(*) desc
        """
    ).fetchall()
    [(total,)] = connection.execute(f"select count(*) from {LADDER_TABLE}").fetchall()
    by_precision = {str(p): int(c) for p, c in counts}
    rescued = total - by_precision.get("none", 0)
    print("\n" + "=" * 78)
    print(f"4. LADDER HEADLINE (N>={HEADLINE_N} minimum points, finest-available)")
    print("=" * 78)
    _print_table(
        "Rescue precision breakdown",
        ["precision", "count", "% of unmatched pool"],
        [
            [p, c, f"{100.0*c/total:.2f}%"]
            for p, c in sorted(by_precision.items(), key=lambda kv: -kv[1])
        ]
        + [["TOTAL rescued (postcode+city)", rescued, f"{100.0*rescued/total:.2f}%"]],
    )
    # Sensitivity at N=1 and N=10 for the same ladder logic.
    sens_rows = []
    for n in N_THRESHOLDS:
        [(none_n,)] = connection.execute(
            f"""
            select count(*) from {LADDER_TABLE}
            where (postcode_n is null or postcode_n < {n})
              and (city_osm_n is null or city_osm_n < {n})
              and (city_alt_n is null or city_alt_n < {n})
            """
        ).fetchall()
        [(pc_n,)] = connection.execute(
            f"select count(*) from {LADDER_TABLE} where postcode_n >= {n}"
        ).fetchall()
        [(city_only_n,)] = connection.execute(
            f"""
            select count(*) from {LADDER_TABLE}
            where (postcode_n is null or postcode_n < {n})
              and (city_osm_n >= {n} or city_alt_n >= {n})
            """
        ).fetchall()
        rescued_n = total - int(none_n)
        sens_rows.append(
            [
                n,
                int(pc_n),
                int(city_only_n),
                rescued_n,
                f"{100.0*rescued_n/total:.2f}%",
            ]
        )
    _print_table(
        "Ladder sensitivity across N",
        ["N", "postcode-rescued", "city-only-rescued", "total rescued", "% of pool"],
        sens_rows,
    )
    return {"total": total, **by_precision, "rescued": rescued}


# --------------------------------------------------------------------------- #
# Correctness sanity (item 5)                                                 #
# --------------------------------------------------------------------------- #


def report_ambiguity(connection: Any) -> None:
    # (a) city-name collision risk: a city group whose points are geographically
    # scattered far apart may be one sprawling municipality (expected -- large spread is
    # labeled coarse) OR two distinct places sharing a name. Surface the worst offenders
    # for a human look; large true cities (Stockholm, Göteborg) are expected here too.
    worst = connection.execute(
        f"""
        select city_norm, n_points, round(median_dist_m) as median_m,
               round(max_dist_m) as max_m
        from {CITY_STATS_OSM_TABLE}
        where n_points >= {HEADLINE_N}
        order by max_dist_m desc
        limit 15
        """
    ).fetchall()
    _print_table(
        "5a. Widest-spread city groups (OSM source) -- eyeball for name collisions",
        ["post_town (normalized)", "n_points", "median_dist_m", "max_dist_m"],
        [[r[0], int(r[1]), r[2], r[3]] for r in worst],
    )

    # (b) coverage gap: post_town strings present in the unmatched pool that have NO
    # OSM city match at all (abbreviation / spelling mismatches, or genuinely foreign
    # "post towns" like UTLANDET). Uses town_key (locally computed upper(trim(post_town)),
    # diacritics intact) -- NOT normalized_post_town, see (c) below for why.
    gap = connection.execute(
        f"""
        with pool_towns as (
            select town_key as t, count(*) as n
            from {UNMATCHED_POOL_TABLE}
            where town_key is not null and town_key != ''
            group by 1
        )
        select p.t, p.n
        from pool_towns p
        where not exists (
            select 1 from {CITY_STATS_OSM_TABLE} c where c.city_norm = p.t
        )
        order by p.n desc
        limit 20
        """
    ).fetchall()
    _print_table(
        "5b. post_town strings unmatched pool has that OSM city has NOT (top by volume)",
        ["town_key", "unmatched addresses with this town"],
        [[r[0], int(r[1])] for r in gap],
    )
    [(distinct_towns,)] = connection.execute(
        f"""
        select count(distinct town_key) from {UNMATCHED_POOL_TABLE}
        where town_key is not null and town_key != ''
        """
    ).fetchall()
    [(matched_towns,)] = connection.execute(
        f"""
        select count(distinct pool.town_key)
        from {UNMATCHED_POOL_TABLE} pool
        where pool.town_key is not null and pool.town_key != ''
          and exists (
              select 1 from {CITY_STATS_OSM_TABLE} c
              where c.city_norm = pool.town_key
          )
        """
    ).fetchall()
    print(
        f"\n  distinct post_town strings in pool: {int(distinct_towns)}; "
        f"with an exact OSM city-name match: {int(matched_towns)} "
        f"({100.0*matched_towns/distinct_towns:.1f}% of DISTINCT names, "
        "volume-weighted coverage reported in section 3 above)"
    )

    # (c) the normalization bug, quantified: production's normalized_post_town strips
    # Swedish diacritics as "non-alnum" under ClickHouse's ASCII regex engine
    # ("GÖTEBORG" -> "g teborg"). Joining against OSM's diacritic-preserving city names
    # on THAT column -- which is what the brief's data description names -- would match
    # essentially nothing for any town with å/ä/ö. Quantify the delta directly.
    [(broken_matched,)] = connection.execute(
        f"""
        select count(distinct pool.normalized_post_town)
        from {UNMATCHED_POOL_TABLE} pool
        where pool.normalized_post_town is not null and pool.normalized_post_town != ''
          and exists (
              select 1 from {CITY_STATS_OSM_TABLE} c
              where c.city_norm = upper(pool.normalized_post_town)
          )
        """
    ).fetchall()
    print(
        "\n  BUG: joining on CH's own normalized_post_town (ASCII-only regex, strips "
        "diacritics) against OSM city names matches only "
        f"{int(broken_matched)} of {int(distinct_towns)} distinct town strings -- e.g. "
        "'GÖTEBORG' normalizes to 'g teborg' in production. This script routes around it "
        "by computing town_key = upper(trim(post_town)) locally (DuckDB's upper/trim ARE "
        "Unicode-aware); a real fallback tier must do the same, or fix the production "
        "regex ('[^[:alnum:]]+' -> a Unicode-letter-aware class) before relying on "
        "normalized_post_town for city matching."
    )


def report_samples(connection: Any) -> None:
    samples = connection.execute(
        f"""
        select
            substr(l.address_id, 1, 12) as address_id,
            l.normalized_postal_code, l.town_key,
            l.rescue_precision, l.rescue_source,
            round(l.rescue_lat, 5) as lat, round(l.rescue_lon, 5) as lon,
            l.postcode_n, l.city_osm_n, l.city_alt_n
        from {LADDER_TABLE} l
        where l.rescue_precision != 'none'
        order by hash(l.address_id)
        limit 15
        """
    ).fetchall()
    _print_table(
        "5c. Sample of 15 rescued addresses (eyeball plausibility)",
        [
            "address_id",
            "postcode",
            "post_town",
            "precision",
            "source",
            "lat",
            "lon",
            "pc_n",
            "city_osm_n",
            "city_alt_n",
        ],
        [[str(c) for c in row] for row in samples],
    )
    # The brief's motivating case, explicitly.
    stav = connection.execute(
        f"""
        select
            substr(l.address_id, 1, 12), l.normalized_postal_code, l.town_key,
            l.rescue_precision, l.rescue_source, round(l.rescue_lat, 5), round(l.rescue_lon, 5),
            l.postcode_n, l.city_osm_n
        from {LADDER_TABLE} l
        inner join {UNMATCHED_POOL_TABLE} pool on pool.address_id = l.address_id
        where pool.normalized_postal_code = '23100'
        order by l.address_id
        limit 5
        """
    ).fetchall()
    _print_table(
        "5d. STAVSTENSV / postcode 23100 Trelleborg -- the brief's motivating case",
        ["address_id", "postcode", "post_town", "precision", "source", "lat", "lon", "pc_n", "city_n"],
        [[str(c) for c in row] for row in stav],
    )


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure coarse-centroid (postcode/city) fallback coverage on the "
            "currently-unmatched Sweden address pool. Measurement only -- no writes."
        )
    )
    parser.add_argument(
        "--refresh-pool",
        action="store_true",
        help="Re-pull the unmatched + matched-alt pools from ClickHouse even if cached.",
    )
    parser.add_argument(
        "--workbench",
        type=Path,
        default=WORKBENCH_PATH,
        help=f"Path to the local DuckDB workbench (default {WORKBENCH_PATH}).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    load_dotenv(_SERVICE_ROOT / ".env")
    load_dotenv(_SERVICE_ROOT.parent / "backoffice" / ".env")

    args.workbench.parent.mkdir(parents=True, exist_ok=True)
    _log(f"workbench: {args.workbench}")
    connection = duckdb.connect(str(args.workbench))
    try:
        refresh_pool(connection, refresh=args.refresh_pool)

        [(osm_points,)] = connection.execute(
            f"select count(*) from {_QUALIFIED_ADDRESS_POINTS}"
        ).fetchall()
        _log(f"local OSM address_points available: {int(osm_points)}")

        report_pool_bounds(connection)

        build_postcode_stats(connection)
        build_city_stats_osm(connection)
        build_city_stats_alt(connection)

        report_centroid_coverage(
            connection,
            stats_table=POSTCODE_STATS_TABLE,
            join_key_expr="pool.normalized_postal_code",
            label="2. POSTCODE-CENTROID (OSM source)",
        )
        report_spread_distribution(
            connection,
            stats_table=POSTCODE_STATS_TABLE,
            title="2b. Postcode-centroid spread distribution (median dist to centroid)",
        )

        report_centroid_coverage(
            connection,
            stats_table=CITY_STATS_OSM_TABLE,
            join_key_expr="pool.town_key",
            label="3. CITY-CENTROID (OSM source)",
        )
        report_spread_distribution(
            connection,
            stats_table=CITY_STATS_OSM_TABLE,
            title="3b. City-centroid spread distribution, OSM source (median dist to centroid)",
        )
        report_centroid_coverage(
            connection,
            stats_table=CITY_STATS_ALT_TABLE,
            join_key_expr="pool.town_key",
            label="3c. CITY-CENTROID (CH matched-alt source)",
        )
        report_spread_distribution(
            connection,
            stats_table=CITY_STATS_ALT_TABLE,
            title="3d. City-centroid spread distribution, CH matched-alt source",
        )

        report_ladder(connection)
        report_ambiguity(connection)
        report_samples(connection)
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
