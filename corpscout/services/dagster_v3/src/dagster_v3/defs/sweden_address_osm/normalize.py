from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from dagster_v3.defs.sweden_address_osm import tables


def replace_osm_address_points(
    *,
    connection: Any,
    pbf_path: Path,
    source_url: str,
    source_object_key: str,
    source_md5: str,
    source_snapshot_at: datetime,
    source_retrieved_at: datetime,
) -> dict[str, int]:
    connection.execute("INSTALL spatial")
    connection.execute("LOAD spatial")
    return replace_osm_address_points_from_relation(
        connection=connection,
        source_relation_sql="select * from st_readosm(?)",
        source_relation_parameters=(str(pbf_path),),
        source_url=source_url,
        source_object_key=source_object_key,
        source_md5=source_md5,
        source_snapshot_at=source_snapshot_at,
        source_retrieved_at=source_retrieved_at,
    )


def replace_osm_address_points_from_relation(
    *,
    connection: Any,
    source_relation_sql: str,
    source_relation_parameters: Sequence[object],
    source_url: str,
    source_object_key: str,
    source_md5: str,
    source_snapshot_at: datetime,
    source_retrieved_at: datetime,
) -> dict[str, int]:
    """Build address-point and named-road indexes from one OSM snapshot."""
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(f"create schema if not exists {tables.DUCKDB_SCHEMA}")
        connection.execute(
            f"""
            create or replace temporary table _osm_index_objects as
            select
                cast(kind as varchar) as osm_type,
                id as osm_id,
                tags,
                refs,
                lat,
                lon
            from ({source_relation_sql}) source
            where (
                    tags['addr:housenumber'] is not null
                    and coalesce(upper(tags['addr:country']), 'SE') = 'SE'
                )
               or (
                    cast(kind as varchar) = 'way'
                    and nullif(trim(tags['name']), '') is not null
                    and nullif(trim(tags['highway']), '') is not null
                )
            """,
            source_relation_parameters,
        )
        connection.execute(
            """
            create or replace temporary table _osm_address_objects as
            select *
            from _osm_index_objects
            where tags['addr:housenumber'] is not null
              and coalesce(upper(tags['addr:country']), 'SE') = 'SE'
            """
        )
        connection.execute(
            """
            create or replace temporary table _osm_street_objects as
            select *
            from _osm_index_objects
            where osm_type = 'way'
              and nullif(trim(tags['name']), '') is not null
              and nullif(trim(tags['highway']), '') is not null
            """
        )
        connection.execute(
            """
            create or replace temporary table _osm_index_way_refs as
            select
                'address' as index_kind,
                address.osm_id,
                ref.node_id,
                ref.ref_order,
                array_length(address.refs) as expected_ref_count
            from _osm_address_objects address
            cross join unnest(address.refs) with ordinality
                as ref(node_id, ref_order)
            where address.osm_type = 'way'

            union all

            select
                'street' as index_kind,
                street.osm_id,
                ref.node_id,
                ref.ref_order,
                array_length(street.refs) as expected_ref_count
            from _osm_street_objects street
            cross join unnest(street.refs) with ordinality
                as ref(node_id, ref_order)
            """
        )
        connection.execute(
            f"""
            create or replace temporary table _osm_index_way_nodes as
            select
                ref.index_kind,
                ref.osm_id,
                ref.node_id,
                ref.ref_order,
                ref.expected_ref_count,
                node.lon,
                node.lat
            from ({source_relation_sql}) node
            inner join _osm_index_way_refs ref
                on ref.node_id = node.id
            where cast(node.kind as varchar) = 'node'
              and node.lon between -180 and 180
              and node.lat between -90 and 90
            """,
            source_relation_parameters,
        )
        connection.execute(
            """
            create or replace temporary table _osm_index_way_points as
            with way_geometry as (
                select
                    index_kind,
                    osm_id,
                    max(expected_ref_count) as expected_ref_count,
                    count(*) as matched_ref_count,
                    first(node_id order by ref_order) as first_node_id,
                    last(node_id order by ref_order) as last_node_id,
                    st_makeline(
                        list(st_point(lon, lat) order by ref_order)
                    ) as line_geometry
                from _osm_index_way_nodes
                group by index_kind, osm_id
            ), points as (
                select
                    index_kind,
                    osm_id,
                    case
                        when index_kind = 'address'
                         and expected_ref_count >= 4
                         and first_node_id = last_node_id
                        then st_pointonsurface(st_makepolygon(line_geometry))
                        when index_kind = 'address'
                        then st_centroid(line_geometry)
                        else st_lineinterpolatepoint(line_geometry, 0.5)
                    end as point_geometry,
                    case
                        when index_kind = 'address'
                         and expected_ref_count >= 4
                         and first_node_id = last_node_id
                        then 'osm_way_point_on_surface'
                        when index_kind = 'address' then 'osm_way_centroid'
                        else 'osm_road_segment_midpoint'
                    end as coordinate_method
                from way_geometry
                where matched_ref_count = expected_ref_count
                  and expected_ref_count >= 2
            )
            select
                index_kind,
                osm_id,
                st_x(point_geometry) as longitude,
                st_y(point_geometry) as latitude,
                coordinate_method
            from points
            """
        )
        connection.execute(
            """
            create or replace temporary table _osm_snapshot_metadata as
            select
                ?::varchar as source_url,
                ?::varchar as source_object_key,
                ?::varchar as source_md5,
                ?::timestamptz as source_snapshot_at,
                ?::timestamptz as source_retrieved_at
            """,
            [
                source_url,
                source_object_key,
                source_md5,
                source_snapshot_at,
                source_retrieved_at,
            ],
        )
        connection.execute(
            """
            create or replace temporary table _osm_address_points_next as
            with candidates as (
                select
                    address.osm_type,
                    address.osm_id,
                    address.tags,
                    address.lon as longitude,
                    address.lat as latitude,
                    'osm_node' as coordinate_method
                from _osm_address_objects address
                where address.osm_type = 'node'
                  and address.lon between -180 and 180
                  and address.lat between -90 and 90

                union all

                select
                    address.osm_type,
                    address.osm_id,
                    address.tags,
                    point.longitude,
                    point.latitude,
                    point.coordinate_method
                from _osm_address_objects address
                inner join _osm_index_way_points point
                    on point.index_kind = 'address'
                   and point.osm_id = address.osm_id
                where address.osm_type = 'way'
            ), address_fields as (
                select
                    concat(osm_type, '/', osm_id::varchar) as source_record_id,
                    osm_type,
                    osm_id,
                    coalesce(nullif(upper(trim(tags['addr:country'])), ''), 'SE')
                        as country_code,
                    nullif(trim(tags['addr:street']), '') as street,
                    nullif(trim(tags['addr:housenumber']), '') as house_number,
                    nullif(trim(tags['addr:unit']), '') as unit,
                    nullif(trim(tags['addr:postcode']), '') as postcode,
                    nullif(trim(tags['addr:city']), '') as city,
                    nullif(trim(tags['addr:place']), '') as place,
                    nullif(trim(tags['addr:full']), '') as full_address,
                    longitude,
                    latitude,
                    coordinate_method,
                    cast(to_json(tags) as varchar) as source_tags_json
                from candidates
            ), normalized as (
                select
                    *,
                    trim(regexp_replace(
                        lower(coalesce(street, place, '')),
                        '[^[:alnum:]]+',
                        ' ',
                        'g'
                    )) as normalized_street,
                    lower(regexp_replace(
                        coalesce(house_number, ''),
                        '[^[:alnum:]]+',
                        '',
                        'g'
                    )) as normalized_house_number,
                    lower(regexp_replace(
                        coalesce(postcode, ''),
                        '[^[:alnum:]]+',
                        '',
                        'g'
                    )) as normalized_postcode,
                    trim(regexp_replace(
                        lower(coalesce(city, '')),
                        '[^[:alnum:]]+',
                        ' ',
                        'g'
                    )) as normalized_city
                from address_fields
            )
            select
                normalized.source_record_id,
                normalized.osm_type,
                normalized.osm_id,
                normalized.country_code,
                normalized.street,
                normalized.house_number,
                normalized.unit,
                normalized.postcode,
                normalized.city,
                normalized.place,
                normalized.full_address,
                normalized.normalized_street,
                normalized.normalized_house_number,
                normalized.normalized_postcode,
                normalized.normalized_city,
                concat_ws(
                    '|',
                    normalized.normalized_postcode,
                    normalized.normalized_street,
                    normalized.normalized_house_number
                ) as address_match_key,
                normalized.longitude,
                normalized.latitude,
                normalized.coordinate_method,
                concat(
                    'https://www.openstreetmap.org/',
                    normalized.osm_type,
                    '/',
                    normalized.osm_id::varchar
                ) as source_record_url,
                normalized.source_tags_json,
                metadata.source_url,
                metadata.source_object_key,
                metadata.source_md5,
                metadata.source_snapshot_at,
                metadata.source_retrieved_at
            from normalized
            cross join _osm_snapshot_metadata metadata
            """
        )
        connection.execute(
            """
            create or replace temporary table _osm_street_segments_next as
            with street_fields as (
                select
                    concat('way/', street.osm_id::varchar) as source_record_id,
                    street.osm_id,
                    nullif(trim(street.tags['name']), '') as street,
                    nullif(trim(street.tags['highway']), '') as highway,
                    point.longitude,
                    point.latitude,
                    point.coordinate_method,
                    cast(to_json(street.tags) as varchar) as source_tags_json
                from _osm_street_objects street
                inner join _osm_index_way_points point
                    on point.index_kind = 'street'
                   and point.osm_id = street.osm_id
            ), normalized as (
                select
                    *,
                    trim(regexp_replace(
                        lower(street),
                        '[^[:alnum:]]+',
                        ' ',
                        'g'
                    )) as normalized_street
                from street_fields
            )
            select
                normalized.source_record_id,
                normalized.osm_id,
                normalized.street,
                normalized.normalized_street,
                normalized.highway,
                normalized.longitude,
                normalized.latitude,
                normalized.coordinate_method,
                concat(
                    'https://www.openstreetmap.org/way/',
                    normalized.osm_id::varchar
                ) as source_record_url,
                normalized.source_tags_json,
                metadata.source_url,
                metadata.source_object_key,
                metadata.source_md5,
                metadata.source_snapshot_at,
                metadata.source_retrieved_at
            from normalized
            cross join _osm_snapshot_metadata metadata
            where normalized.normalized_street != ''
            """
        )

        raw_address_objects = _scalar_count(
            connection, "select count(*) from _osm_address_objects"
        )
        node_address_points = _scalar_count(
            connection,
            "select count(*) from _osm_address_points_next where osm_type = 'node'",
        )
        way_address_points = _scalar_count(
            connection,
            "select count(*) from _osm_address_points_next where osm_type = 'way'",
        )
        relation_address_objects = _scalar_count(
            connection,
            "select count(*) from _osm_address_objects where osm_type = 'relation'",
        )
        way_address_objects = _scalar_count(
            connection,
            "select count(*) from _osm_address_objects where osm_type = 'way'",
        )
        raw_street_objects = _scalar_count(
            connection, "select count(*) from _osm_street_objects"
        )
        street_segments = _scalar_count(
            connection, "select count(*) from _osm_street_segments_next"
        )
        address_points = node_address_points + way_address_points
        if address_points == 0:
            raise ValueError("Sweden OSM extraction produced zero address points")

        connection.execute(
            f"""
            create or replace table {tables.QUALIFIED_ADDRESS_TABLE} as
            select * from _osm_address_points_next
            """
        )
        connection.execute(
            f"""
            create or replace table {tables.QUALIFIED_STREET_SEGMENT_TABLE} as
            select * from _osm_street_segments_next
            """
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise

    return {
        "raw_address_objects": raw_address_objects,
        "node_address_points": node_address_points,
        "way_address_points": way_address_points,
        "relation_address_objects_omitted": relation_address_objects,
        "incomplete_way_address_objects_omitted": (
            way_address_objects - way_address_points
        ),
        "address_points": address_points,
        "raw_street_objects": raw_street_objects,
        "incomplete_street_objects_omitted": raw_street_objects - street_segments,
        "street_segments": street_segments,
    }


def _scalar_count(connection: Any, query: str) -> int:
    row = connection.execute(query).fetchone()
    if row is None:
        raise ValueError(f"Count query returned no row: {query}")
    return int(row[0])
