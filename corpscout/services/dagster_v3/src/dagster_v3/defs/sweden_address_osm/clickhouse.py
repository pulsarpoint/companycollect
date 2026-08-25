"""Publish the Sweden OSM gazetteer from the build DuckDB into ClickHouse.

The address-point and named-road indexes are built each weekly run into a host-local DuckDB
file (see normalize.py). This module republishes both tables into ClickHouse with the STAGED
atomic EXCHANGE TABLES pattern (export_duckdb_connection_table_to_clickhouse, truncate=True),
so live readers never observe a partial or empty table.

The one column that is not a straight mirror of the DuckDB build is ``normalized_match_key``.
It is recomputed with the RESOLVER's normalization (address_resolution/search_documents.py
``_compact_text_sql``) rather than the build's own ``normalized_*`` columns, so it lines up
byte for byte with corpscout.se_address_geocodes.normalized_match_key and a matched geocode
outcome can find its OSM point by key.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.address_resolution.search_documents import _compact_text_sql
from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.sweden_address_osm import tables

CLICKHOUSE_DATABASE = RESOLVED_DATABASE
ADDRESS_POINTS_TABLE_CH = "se_osm_address_points"
STREET_SEGMENTS_TABLE_CH = "se_osm_street_segments"

# Columns are listed in the ClickHouse table's own order (migrations 000322 / 000323). The
# batch helper names every column in both the INSERT and the DuckDB SELECT from these tuples,
# so the two stay aligned; the ClickHouse table order need only agree with the tuple order.
ADDRESS_POINT_COLUMNS: tuple[str, ...] = (
    "source_record_id",
    "osm_type",
    "osm_id",
    "country_code",
    "street",
    "house_number",
    "unit",
    "postcode",
    "city",
    "place",
    "full_address",
    "normalized_street",
    "normalized_house_number",
    "normalized_postcode",
    "normalized_city",
    "address_match_key",
    "normalized_match_key",
    "longitude",
    "latitude",
    "coordinate_method",
    "source_record_url",
    "source_tags_json",
    "source_url",
    "source_object_key",
    "source_md5",
    "source_snapshot_at",
    "source_retrieved_at",
    "published_at",
)

STREET_SEGMENT_COLUMNS: tuple[str, ...] = (
    "source_record_id",
    "osm_id",
    "street",
    "normalized_street",
    "normalized_match_key",
    "highway",
    "longitude",
    "latitude",
    "coordinate_method",
    "source_record_url",
    "source_tags_json",
    "source_url",
    "source_object_key",
    "source_md5",
    "source_snapshot_at",
    "source_retrieved_at",
    "published_at",
)


def address_point_match_key_sql() -> str:
    """The store-compatible match key for an OSM address point.

    Mirrors corpscout.se_address_geocodes.normalized_match_key, whose query side is
    ``concat_ws('|', normalized_postal_code, concat(normalized_street, normalized_house_number))``
    with each part produced by ``_compact_text_sql``. The reference (OSM) side feeds
    ``coalesce(nullif(street, ''), place, '')`` as the street name (see
    address_resolution_shadow._replace_building_reference_documents), so the same coalesce is
    used here.
    """
    postcode = _compact_text_sql("postcode")
    street = _compact_text_sql("coalesce(nullif(street, ''), place, '')")
    house = _compact_text_sql("house_number")
    return f"concat_ws('|', {postcode}, concat({street}, {house}))"


def street_segment_match_key_sql() -> str:
    """The postcode-less street key: the resolver's compact normalization of the street name."""
    return _compact_text_sql("street")


# The matched-row join sanity check (brief step 5 / step 2). A sample of matched_exact geocode
# outcomes computed against the SAME OSM snapshot as the freshly published gazetteer must find
# their OSM address point again -- robustly by OSM id, and by normalized_match_key wherever the
# OSM point itself carries a postcode. Points that carry no addr:postcode cannot reproduce the
# store key (which encodes the QUERY postcode), so key agreement is measured on the postcode-
# bearing subset and the overall key rate is only logged.
GAZETTEER_MATCH_JOIN_SQL = f"""
WITH
    (SELECT any(source_md5) FROM {CLICKHOUSE_DATABASE}.{ADDRESS_POINTS_TABLE_CH}) AS gazetteer_md5,
    sample AS (
        SELECT
            normalized_match_key AS store_key,
            candidate_record_ids[1] AS candidate_id
        FROM {CLICKHOUSE_DATABASE}.se_address_geocodes
        WHERE match_status = 'matched_exact'
          AND reference_md5 = gazetteer_md5
          AND normalized_match_key != ''
          AND length(candidate_record_ids) = 1
          -- The doubled wildcard escapes it: clickhouse-driver substitutes named params with
          -- Python percent-formatting, so a literal percent in the query must be doubled.
          AND (candidate_record_ids[1] LIKE 'way/%%'
               OR candidate_record_ids[1] LIKE 'node/%%'
               OR candidate_record_ids[1] LIKE 'relation/%%')
        LIMIT %(sample_limit)s
    )
SELECT
    count() AS sample_size,
    countIf(point.source_record_id != '') AS osm_id_present,
    countIf(point.source_record_id != '' AND point.normalized_match_key = sample.store_key)
        AS key_matches,
    countIf(point.source_record_id != '' AND point.normalized_postcode != '')
        AS postcode_bearing,
    countIf(
        point.source_record_id != ''
        AND point.normalized_postcode != ''
        AND point.normalized_match_key = sample.store_key
    ) AS key_matches_postcode_bearing
FROM sample
LEFT JOIN {CLICKHOUSE_DATABASE}.{ADDRESS_POINTS_TABLE_CH} AS point
    ON point.source_record_id = sample.candidate_id
"""


def row_count_is_within_band(
    *,
    clickhouse_count: int,
    duckdb_count: int,
    min_ratio: float = 0.99,
    max_ratio: float = 1.01,
) -> bool:
    """The published table must be non-empty and mirror the DuckDB source row count.

    The publish is a full copy, so the counts are expected to be equal; the band tolerates
    only incidental skew, never an empty or half-loaded table.
    """
    if clickhouse_count <= 0 or duckdb_count <= 0:
        return False
    ratio = clickhouse_count / duckdb_count
    return min_ratio <= ratio <= max_ratio


def gazetteer_match_join_is_healthy(
    *,
    sample_size: int,
    osm_id_present: int,
    postcode_bearing: int,
    key_matches_postcode_bearing: int,
    min_osm_id_rate: float = 0.80,
    min_key_rate: float = 0.70,
) -> bool:
    """Gate the matched-row join.

    An empty sample means no geocode outcome was computed against this gazetteer's snapshot yet
    (e.g. the first publish, before the store is appended) -- there is nothing to contradict, so
    the check passes. Otherwise the OSM candidate ids must resolve back into the gazetteer, and
    where the OSM point carries a postcode its normalized_match_key must reproduce the store key.
    A normalization regression collapses the postcode-bearing agreement to near zero and trips
    this gate.
    """
    if sample_size <= 0:
        return True
    if osm_id_present / sample_size < min_osm_id_rate:
        return False
    if postcode_bearing == 0:
        return True
    return key_matches_postcode_bearing / postcode_bearing >= min_key_rate


@dataclass(frozen=True, slots=True)
class SwedenOsmGazetteerPublishResult:
    address_points: int
    street_segments: int


def publish_sweden_osm_gazetteer(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    published_at: datetime,
    log: Callable[..., object] | None = None,
) -> SwedenOsmGazetteerPublishResult:
    """Replace both Sweden OSM gazetteer ClickHouse tables from the build DuckDB.

    Each table is staged into a ``_tmp`` copy and swapped in with EXCHANGE TABLES, so the swap
    is atomic per table and readers never see a partial load.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=CLICKHOUSE_DATABASE,
        tables=(ADDRESS_POINTS_TABLE_CH, STREET_SEGMENTS_TABLE_CH),
    )
    published_at_literal = f"TIMESTAMPTZ '{published_at.isoformat()}'"
    with clickhouse.get_connection() as client:
        if log is not None:
            log(
                "Publishing Sweden OSM gazetteer to ClickHouse: %s.%s, %s.%s",
                CLICKHOUSE_DATABASE,
                ADDRESS_POINTS_TABLE_CH,
                CLICKHOUSE_DATABASE,
                STREET_SEGMENTS_TABLE_CH,
            )
        address_points = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.DUCKDB_SCHEMA,
            duckdb_table=tables.ADDRESS_TABLE,
            clickhouse_database=CLICKHOUSE_DATABASE,
            clickhouse_table=ADDRESS_POINTS_TABLE_CH,
            columns=ADDRESS_POINT_COLUMNS,
            truncate=True,
            column_expressions={
                "street": "coalesce(street, '')",
                "house_number": "coalesce(house_number, '')",
                "unit": "coalesce(unit, '')",
                "postcode": "coalesce(postcode, '')",
                "city": "coalesce(city, '')",
                "place": "coalesce(place, '')",
                "full_address": "coalesce(full_address, '')",
                "normalized_match_key": address_point_match_key_sql(),
                "published_at": published_at_literal,
            },
            log=log,
        )
        street_segments = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.DUCKDB_SCHEMA,
            duckdb_table=tables.STREET_SEGMENT_TABLE,
            clickhouse_database=CLICKHOUSE_DATABASE,
            clickhouse_table=STREET_SEGMENTS_TABLE_CH,
            columns=STREET_SEGMENT_COLUMNS,
            truncate=True,
            column_expressions={
                "street": "coalesce(street, '')",
                "highway": "coalesce(highway, '')",
                "normalized_match_key": street_segment_match_key_sql(),
                "published_at": published_at_literal,
            },
            log=log,
        )
    if log is not None:
        log(
            "Finished Sweden OSM gazetteer publish: address_points=%d street_segments=%d",
            address_points,
            street_segments,
        )
    return SwedenOsmGazetteerPublishResult(
        address_points=address_points,
        street_segments=street_segments,
    )
