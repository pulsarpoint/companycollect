from typing import Any

from dagster_v3.defs.sweden_address_osm import tables

OSM_ADDRESS_MATCH_COMPONENTS_TABLE = "_sweden_osm_address_match_components"
SITE_MAX_SPREAD_METERS = 100.0
AREA_MAX_SPREAD_METERS = 1_000.0
STREET_FALLBACK_CONFIDENCE = 0.4
STREET_FALLBACK_METHOD = "postal_code_street_address_point_median"
STREET_FALLBACK_COORDINATE_METHOD = "osm_street_address_point_median"
ROAD_STREET_FALLBACK_CONFIDENCE = 0.3
ROAD_STREET_FALLBACK_METHOD = "nearby_postcode_street_road_segment_median"
ROAD_STREET_FALLBACK_COORDINATE_METHOD = "osm_road_segment_midpoint_median"
ROAD_POSTCODE_CONTEXT_MAX_DISTANCE_METERS = 5_000.0
ROAD_LATITUDE_GRID_DEGREES = 0.05
ROAD_LONGITUDE_GRID_DEGREES = 0.1
ROAD_LATITUDE_NEIGHBOR_CELLS = 1
ROAD_LONGITUDE_NEIGHBOR_CELLS = 2


def replace_osm_address_match_components(connection: Any) -> None:
    """Build one exact-match candidate per OSM house-number component."""
    connection.execute(
        f"""
        create or replace temporary table {OSM_ADDRESS_MATCH_COMPONENTS_TABLE} as
        with expanded as (
            select
                address.*,
                house_number.value as house_number_component
            from {tables.QUALIFIED_ADDRESS_TABLE} address
            cross join unnest(
                regexp_split_to_array(coalesce(address.house_number, ''), '[,;]')
            ) as house_number(value)
        ), normalized as (
            select
                *,
                lower(regexp_replace(
                    trim(house_number_component),
                    '[^[:alnum:]]+',
                    '',
                    'g'
                )) as normalized_house_number_component
            from expanded
        )
        select distinct
            source_record_id,
            normalized_postcode,
            normalized_city,
            regexp_replace(
                concat(normalized_street, normalized_house_number_component),
                '[^[:alnum:]]+',
                '',
                'g'
            ) as normalized_street_house,
            latitude,
            longitude,
            coordinate_method,
            source_record_url,
            source_url,
            source_object_key,
            source_md5,
            source_snapshot_at,
            source_retrieved_at
        from normalized
        where normalized_street != ''
          and normalized_house_number_component != ''
        """
    )


def replace_postcode_address_match_candidates(
    connection: Any,
    *,
    table_name: str,
) -> None:
    """Group OSM candidates by postal code, street, and house number."""
    _replace_address_match_candidates(
        connection,
        table_name=table_name,
        match_key_sql="concat_ws('|', normalized_postcode, normalized_street_house)",
        where_sql="normalized_postcode != ''",
    )


def replace_city_address_match_candidates(
    connection: Any,
    *,
    table_name: str,
) -> None:
    """Group postcode-less OSM candidates by city, street, and house number."""
    _replace_address_match_candidates(
        connection,
        table_name=table_name,
        match_key_sql="concat_ws('|', normalized_city, normalized_street_house)",
        where_sql="normalized_postcode = '' and normalized_city != ''",
    )


def replace_country_address_match_candidates(
    connection: Any,
    *,
    table_name: str,
) -> None:
    """Group context-free OSM candidates by street and house number in Sweden."""
    _replace_address_match_candidates(
        connection,
        table_name=table_name,
        match_key_sql="normalized_street_house",
        where_sql="true",
    )


def replace_postcode_street_location_candidates(
    connection: Any,
    *,
    table_name: str,
) -> None:
    """Build conservative street locations from address points, then road geometry."""
    connection.execute(
        f"""
        create or replace temporary table {table_name} as
        with address_point_grouped as (
            select
                concat_ws(
                    '|',
                    normalized_postcode,
                    regexp_replace(normalized_street, '[^[:alnum:]]+', '', 'g')
                ) as normalized_street_location_key,
                count(*)::uinteger as supporting_point_count,
                median(latitude)::double as latitude,
                median(longitude)::double as longitude,
                min(latitude)::double as min_latitude,
                max(latitude)::double as max_latitude,
                min(longitude)::double as min_longitude,
                max(longitude)::double as max_longitude
            from {tables.QUALIFIED_ADDRESS_TABLE}
            where normalized_postcode != ''
              and normalized_street != ''
            group by normalized_street_location_key
        ), address_point_locations as (
            select
                normalized_street_location_key,
                supporting_point_count,
                latitude,
                longitude,
                2 * 6371000 * asin(least(1.0, sqrt(
                    pow(sin(radians(max_latitude - min_latitude) / 2), 2)
                    + cos(radians(min_latitude)) * cos(radians(max_latitude))
                      * pow(sin(radians(max_longitude - min_longitude) / 2), 2)
                )))::double as coordinate_spread_meters,
                '{STREET_FALLBACK_METHOD}'::varchar as match_method,
                {STREET_FALLBACK_CONFIDENCE}::float as match_confidence,
                '{STREET_FALLBACK_COORDINATE_METHOD}'::varchar
                    as coordinate_method
            from address_point_grouped
        ), postcode_centroids as (
            select
                normalized_postcode,
                median(latitude)::double as latitude,
                median(longitude)::double as longitude
            from {tables.QUALIFIED_ADDRESS_TABLE}
            where normalized_postcode != ''
            group by normalized_postcode
        ), postcode_cells as (
            select
                postcode.normalized_postcode,
                postcode.latitude,
                postcode.longitude,
                floor(
                    postcode.latitude / {ROAD_LATITUDE_GRID_DEGREES}
                )::integer + latitude_offset.value as latitude_cell,
                floor(
                    postcode.longitude / {ROAD_LONGITUDE_GRID_DEGREES}
                )::integer + longitude_offset.value as longitude_cell
            from postcode_centroids postcode
            cross join range(
                -{ROAD_LATITUDE_NEIGHBOR_CELLS},
                {ROAD_LATITUDE_NEIGHBOR_CELLS + 1}
            ) latitude_offset(value)
            cross join range(
                -{ROAD_LONGITUDE_NEIGHBOR_CELLS},
                {ROAD_LONGITUDE_NEIGHBOR_CELLS + 1}
            ) longitude_offset(value)
        ), road_cells as (
            select
                *,
                floor(latitude / {ROAD_LATITUDE_GRID_DEGREES})::integer
                    as latitude_cell,
                floor(longitude / {ROAD_LONGITUDE_GRID_DEGREES})::integer
                    as longitude_cell
            from {tables.QUALIFIED_STREET_SEGMENT_TABLE}
            where normalized_street != ''
        ), nearby_road_segments as (
            select
                concat_ws(
                    '|',
                    postcode.normalized_postcode,
                    regexp_replace(
                        road.normalized_street,
                        '[^[:alnum:]]+',
                        '',
                        'g'
                    )
                ) as normalized_street_location_key,
                road.source_record_id,
                road.latitude,
                road.longitude,
                2 * 6371000 * asin(least(1.0, sqrt(
                    pow(
                        sin(radians(road.latitude - postcode.latitude) / 2),
                        2
                    )
                    + cos(radians(postcode.latitude))
                      * cos(radians(road.latitude))
                      * pow(
                            sin(
                                radians(road.longitude - postcode.longitude) / 2
                            ),
                            2
                        )
                )))::double as postcode_distance_meters
            from road_cells road
            inner join postcode_cells postcode
                using (latitude_cell, longitude_cell)
        ), road_grouped as (
            select
                normalized_street_location_key,
                count(distinct source_record_id)::uinteger as supporting_point_count,
                median(latitude)::double as latitude,
                median(longitude)::double as longitude,
                min(latitude)::double as min_latitude,
                max(latitude)::double as max_latitude,
                min(longitude)::double as min_longitude,
                max(longitude)::double as max_longitude
            from nearby_road_segments
            where postcode_distance_meters
                <= {ROAD_POSTCODE_CONTEXT_MAX_DISTANCE_METERS}
            group by normalized_street_location_key
        ), road_locations as (
            select
                normalized_street_location_key,
                supporting_point_count,
                latitude,
                longitude,
                2 * 6371000 * asin(least(1.0, sqrt(
                    pow(sin(radians(max_latitude - min_latitude) / 2), 2)
                    + cos(radians(min_latitude)) * cos(radians(max_latitude))
                      * pow(sin(radians(max_longitude - min_longitude) / 2), 2)
                )))::double as coordinate_spread_meters,
                '{ROAD_STREET_FALLBACK_METHOD}'::varchar as match_method,
                {ROAD_STREET_FALLBACK_CONFIDENCE}::float as match_confidence,
                '{ROAD_STREET_FALLBACK_COORDINATE_METHOD}'::varchar
                    as coordinate_method
            from road_grouped
        ), all_locations as (
            select * from address_point_locations
            union all
            select * from road_locations
        )
        select
            normalized_street_location_key,
            supporting_point_count,
            latitude,
            longitude,
            coordinate_spread_meters,
            match_method,
            match_confidence,
            coordinate_method
        from all_locations
        qualify row_number() over (
            partition by normalized_street_location_key
            order by
                case
                    when coordinate_spread_meters <= {AREA_MAX_SPREAD_METERS}
                     and match_method = '{STREET_FALLBACK_METHOD}' then 0
                    when coordinate_spread_meters <= {AREA_MAX_SPREAD_METERS}
                        then 1
                    when match_method = '{STREET_FALLBACK_METHOD}' then 2
                    else 3
                end,
                supporting_point_count desc
        ) = 1
        """
    )


def normalized_street_location_key_sql(
    *,
    street_name_sql: str,
    street_address_sql: str,
    normalized_postcode_sql: str,
) -> str:
    """Return SQL for a parsed street/postcode key without house number."""
    street_without_detail = f"""
trim(regexp_replace(
    regexp_replace(
        split_part(coalesce({street_address_sql}, ''), ',', 1),
        '[[:space:]]*\\([^)]*\\)[[:space:]]*$',
        ''
    ),
    '(?i)[[:space:]]+[0-9]+[[:space:]]*(tr|trappor?|v[aå]n(ing)?|plan)\\.?$',
    ''
))
""".strip()
    street_name = f"""
regexp_extract(
    {street_without_detail},
    '^(.*)[[:space:]]+[0-9]+([[:space:]]*[[:alpha:]])?([[:space:]]*[-/:][[:space:]]*[0-9]+([[:space:]]*[[:alpha:]])?)?[[:space:]]*$',
    1
)
""".strip()
    return f"""
concat_ws(
    '|',
    {normalized_postcode_sql},
    coalesce(
        nullif(
            lower(regexp_replace(
                coalesce({street_name_sql}, ''),
                '[^[:alnum:]]+',
                '',
                'g'
            )),
            ''
        ),
        lower(regexp_replace({street_name}, '[^[:alnum:]]+', '', 'g'))
    )
)
""".strip()


def normalized_street_house_key_sql(
    *,
    street_name_sql: str,
    house_number_sql: str,
    fallback_normalized_street_sql: str,
) -> str:
    """Return SQL for an OSM key that excludes apartment and unit details."""
    return f"""
case
    when trim(coalesce({street_name_sql}, '')) != ''
     and trim(coalesce({house_number_sql}, '')) != ''
        then lower(regexp_replace(
            concat({street_name_sql}, {house_number_sql}),
            '[^[:alnum:]]+',
            '',
            'g'
        ))
    else {fallback_normalized_street_sql}
end
""".strip()


def _replace_address_match_candidates(
    connection: Any,
    *,
    table_name: str,
    match_key_sql: str,
    where_sql: str,
) -> None:
    connection.execute(
        f"""
        create or replace temporary table {table_name} as
        with keyed as (
            select
                {match_key_sql} as normalized_match_key,
                *
            from {OSM_ADDRESS_MATCH_COMPONENTS_TABLE}
            where {where_sql}
        ), grouped as (
            select
                normalized_match_key,
                count(*)::usmallint as candidate_count,
                median(latitude)::double as latitude,
                median(longitude)::double as longitude,
                min(latitude)::double as min_latitude,
                max(latitude)::double as max_latitude,
                min(longitude)::double as min_longitude,
                max(longitude)::double as max_longitude,
                first(coordinate_method order by source_record_id)
                    as first_coordinate_method,
                first(source_record_id order by source_record_id)
                    as source_record_id,
                first(source_record_url order by source_record_id)
                    as source_record_url,
                list(source_record_id order by source_record_id)
                    as candidate_record_ids,
                list(source_record_url order by source_record_id)
                    as candidate_record_urls
            from keyed
            group by normalized_match_key
        )
        select
            normalized_match_key,
            candidate_count,
            latitude,
            longitude,
            case
                when candidate_count = 1 then first_coordinate_method
                else 'osm_address_candidate_median'
            end as coordinate_method,
            source_record_id,
            source_record_url,
            candidate_record_ids,
            candidate_record_urls,
            2 * 6371000 * asin(least(1.0, sqrt(
                pow(sin(radians(max_latitude - min_latitude) / 2), 2)
                + cos(radians(min_latitude)) * cos(radians(max_latitude))
                  * pow(sin(radians(max_longitude - min_longitude) / 2), 2)
            )))::double as coordinate_spread_meters
        from grouped
        """
    )
