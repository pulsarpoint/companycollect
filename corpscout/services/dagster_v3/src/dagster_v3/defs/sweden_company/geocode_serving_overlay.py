"""The SE geocode SERVING read: precise outcomes, coarse centroids filling the gaps.

The geocode store (`se_address_geocodes`, see geocode_store.py) holds ONLY precise
matcher outcomes. This module is the read-time overlay that sits in front of it: for an
identity the precise matcher left `unmatched`/`ambiguous`, it serves a coarse
postcode-or-city CENTROID coordinate -- honestly labeled by precision, and ALWAYS ranked
below any precise match.

THE THREE RULES THIS SQL ENFORCES, and nothing downstream may re-express:

1. A PRECISE MATCH ALWAYS WINS. The overlay fires ONLY for an identity whose current
   precise outcome (build_current_geocodes_sql -- the same read rule every other consumer
   uses) has `match_status IN ('unmatched','ambiguous')`. A geocoded row
   (`matched_exact`/`matched_area`/...), a `postal_box`, an `invalid_address`, a
   `foreign_address`, a `property_identifier` -- all pass through UNCHANGED. A precise
   coordinate is never overwritten by a centroid, and a precise match that later arrives
   takes the identity back the moment the store carries it (the overlay is a read-time
   fill; it stores nothing).

2. FINEST AVAILABLE. For an eligible identity, the ladder is: the POSTCODE centroid if the
   address's postcode is in `se_postcode_centroids` AND that centroid's `spread_meters` is
   within `POSTCODE_SPREAD_MAX_METERS` (a loose postcode centroid is worse than the city
   one); else the CITY centroid if the address's post_town is in `se_city_centroids`; else
   the original unmatched row, unchanged. The two centroid tables are joined on the
   accent-preserving keys from centroid_keys.py, computed VERBATIM the same way the
   derivation side computes them, so the keys line up.

3. HONEST LABELS. An overlaid row carries `geocode_precision IN ('postcode','city')`,
   `geocode_provider = 'centroid_fallback'` and `match_status = GEOCODE_FALLBACK_STATUS`.
   The status stays inside the store's GEOCODED vocabulary (`matched_area`) so no consumer
   that filters on it is surprised; the two `postcode`/`city` precision values -- new to
   the serving read, never written to the store -- are what actually tell a coarse row
   apart from a building-precise one. `coordinate_supporting_point_count`,
   `coordinate_spread_meters` and `coordinate_locality` carry the centroid's own quality
   metadata so a poor centroid can still be spotted downstream.

WHY A NESTED SELECT AND NOT ALIAS REUSE. `_tier` (which rung of the ladder an identity
lands on) is computed once in the inner SELECT and read several times by the outer
projection. Nesting keeps the tier a plain column the outer query reads, rather than
relying on same-SELECT alias resolution, and keeps every LEFT JOIN NULL guarded by
`ifNull` so the fragment answers identically under both `join_use_nulls` settings.
"""

from collections.abc import Sequence

from dagster_v3.defs.sweden_company import shared_addresses
from dagster_v3.defs.sweden_company.centroid_keys import (
    city_key_sql,
    postcode_key_sql,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    GEOCODED_STATUSES,
    SERVING_COLUMNS,
    build_current_geocodes_sql,
)

# The centroid reference tables (migrations 000323 / 000324). Spelled here rather than
# imported to keep this serving module independent of the derivation/publish asset.
CLICKHOUSE_DATABASE = "corpscout"
POSTCODE_CENTROIDS_TABLE = f"{CLICKHOUSE_DATABASE}.se_postcode_centroids"
CITY_CENTROIDS_TABLE = f"{CLICKHOUSE_DATABASE}.se_city_centroids"
ADDRESSES_TABLE = shared_addresses.QUALIFIED_CLICKHOUSE_SHARED_ADDRESSES_TABLE

# The precise outcomes the overlay is allowed to fill. Everything else -- geocoded,
# postal_box, invalid_address, foreign_address, property_identifier -- passes through.
FALLBACK_ELIGIBLE_STATUSES = ("unmatched", "ambiguous")

# A postcode centroid looser than this is demoted to the city centroid: past a few km a
# "postcode" centroid no longer means the postcode.
POSTCODE_SPREAD_MAX_METERS = 3000.0

GEOCODE_FALLBACK_PROVIDER = "centroid_fallback"
GEOCODE_FALLBACK_COORDINATE_METHOD = "centroid_median"
POSTCODE_PRECISION = "postcode"
CITY_PRECISION = "city"

# The status an overlaid row reports. `matched_area` is the store's coarsest GEOCODED
# status, so a consumer filtering on GEOCODED_STATUSES still counts a rescued identity;
# `geocode_precision` ('postcode'/'city') is what marks it as coarse. Asserted a member of
# the store vocabulary here so a status rename cannot silently make the overlay serve a
# status no consumer recognizes.
GEOCODE_FALLBACK_STATUS = "matched_area"
assert GEOCODE_FALLBACK_STATUS in GEOCODED_STATUSES

# Which serving columns an overlaid row overrides, and the expression to override them
# with, read over the inner subquery (base serving columns as plain names, plus the
# `_tier`/`_pc_*`/`_cc_*`/`_pc_key`/`_city_key` helpers the inner SELECT adds). A column
# absent here passes through unchanged (`SELECT column`).
_POSTCODE = f"_tier = '{POSTCODE_PRECISION}'"
_CITY = f"_tier = '{CITY_PRECISION}'"
_OVERLAY = f"_tier IN ('{POSTCODE_PRECISION}', '{CITY_PRECISION}')"
_COLUMN_OVERRIDES = {
    "match_status": (
        f"if({_OVERLAY}, '{GEOCODE_FALLBACK_STATUS}', match_status)"
    ),
    "latitude": f"multiIf({_POSTCODE}, _pc_lat, {_CITY}, _cc_lat, latitude)",
    "longitude": f"multiIf({_POSTCODE}, _pc_lon, {_CITY}, _cc_lon, longitude)",
    "geocode_provider": (
        f"if({_OVERLAY}, '{GEOCODE_FALLBACK_PROVIDER}', geocode_provider)"
    ),
    "geocode_precision": (
        f"multiIf({_POSTCODE}, '{POSTCODE_PRECISION}',"
        f" {_CITY}, '{CITY_PRECISION}', geocode_precision)"
    ),
    "coordinate_method": (
        f"if({_OVERLAY}, '{GEOCODE_FALLBACK_COORDINATE_METHOD}', coordinate_method)"
    ),
    "coordinate_locality": (
        f"multiIf({_POSTCODE}, _pc_key, {_CITY}, _city_key, coordinate_locality)"
    ),
    "coordinate_supporting_point_count": (
        f"multiIf({_POSTCODE}, _pc_n, {_CITY}, _cc_n,"
        " coordinate_supporting_point_count)"
    ),
    "coordinate_spread_meters": (
        f"multiIf({_POSTCODE}, _pc_spread, {_CITY}, _cc_spread,"
        " coordinate_spread_meters)"
    ),
}


def build_served_geocodes_sql(
    *,
    columns: Sequence[str] = SERVING_COLUMNS,
    address_filter_sql: str = "",
) -> str:
    """One served row per address_id: precise outcome, or the finest centroid filling it.

    ``columns`` selects the output projection (a subset of ``SERVING_COLUMNS``); the
    default is the full serving shape. ``address_filter_sql`` is threaded down to the
    precise base read (build_current_geocodes_sql), where it constrains ``address_id``
    against the store's sorting key so a page-sized read touches a few parts -- it never
    goes on the outer overlay query.

    The result is deterministic and NULL-safe under both ``join_use_nulls`` settings: every
    LEFT JOIN column is read through ``ifNull`` when it gates the tier, and a missing
    centroid can never be selected because its tier requires ``point_count > 0``.
    """
    eligible = ", ".join(f"'{status}'" for status in FALLBACK_ELIGIBLE_STATUSES)
    precise = build_current_geocodes_sql(
        columns=SERVING_COLUMNS, address_filter_sql=address_filter_sql
    )
    # The serving columns are carried through each layer by explicit name rather than `*`:
    # the two centroid joins below both expose latitude/longitude/point_count/spread_meters,
    # and a bare `base.*`/`keyed.*` collides with them under ClickHouse's analyzer.
    base_columns = ",\n    ".join(
        f"base.{column} AS {column}" for column in SERVING_COLUMNS
    )
    keyed_columns = ",\n    ".join(
        f"keyed.{column} AS {column}" for column in SERVING_COLUMNS
    )
    # Base serving row + the two accent-preserving join keys off the address's own
    # postcode/post_town.
    keyed = (
        "SELECT\n"
        f"    {base_columns},\n"
        f"    {postcode_key_sql('address.postal_code')} AS _pc_key,\n"
        f"    {city_key_sql('address.post_town')} AS _city_key\n"
        f"FROM (\n{_indent(precise)}\n) AS base\n"
        f"LEFT JOIN {ADDRESSES_TABLE} AS address"
        " ON address.address_id = base.address_id"
    )
    # + the centroid columns and the tier: precise (untouched), postcode, or city.
    tiered = (
        "SELECT\n"
        f"    {keyed_columns},\n"
        "    keyed._pc_key AS _pc_key,\n"
        "    keyed._city_key AS _city_key,\n"
        "    pc.latitude AS _pc_lat,\n"
        "    pc.longitude AS _pc_lon,\n"
        "    pc.point_count AS _pc_n,\n"
        "    pc.spread_meters AS _pc_spread,\n"
        "    cc.latitude AS _cc_lat,\n"
        "    cc.longitude AS _cc_lon,\n"
        "    cc.point_count AS _cc_n,\n"
        "    cc.spread_meters AS _cc_spread,\n"
        "    multiIf(\n"
        f"        keyed.match_status NOT IN ({eligible}), 'precise',\n"
        "        ifNull(pc.point_count, 0) > 0"
        f" AND ifNull(pc.spread_meters, 1e18) <= {POSTCODE_SPREAD_MAX_METERS},"
        f" '{POSTCODE_PRECISION}',\n"
        f"        ifNull(cc.point_count, 0) > 0, '{CITY_PRECISION}',\n"
        "        'precise'\n"
        "    ) AS _tier\n"
        f"FROM (\n{_indent(keyed)}\n) AS keyed\n"
        f"LEFT JOIN {POSTCODE_CENTROIDS_TABLE} AS pc ON pc.key = keyed._pc_key\n"
        f"LEFT JOIN {CITY_CENTROIDS_TABLE} AS cc ON cc.key = keyed._city_key"
    )
    projection = ",\n    ".join(
        f"{_COLUMN_OVERRIDES[column]} AS {column}"
        if column in _COLUMN_OVERRIDES
        else column
        for column in columns
    )
    return f"SELECT\n    {projection}\nFROM (\n{_indent(tiered)}\n) AS overlaid"


def _indent(sql: str) -> str:
    return "\n".join(f"    {line}" for line in sql.splitlines())
