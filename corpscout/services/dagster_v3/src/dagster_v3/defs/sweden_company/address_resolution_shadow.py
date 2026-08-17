from collections.abc import Callable
from datetime import datetime
from typing import Any

from dagster_v3.defs.address_resolution.resolution import (
    replace_address_resolution_candidates,
    replace_address_resolution_results,
)
from dagster_v3.defs.address_resolution.search_documents import (
    replace_address_search_documents,
)
from dagster_v3.defs.sweden_address_osm import address_matching
from dagster_v3.defs.sweden_address_osm import tables as osm_tables
from dagster_v3.defs.sweden_company import (
    address_canonicalization,
    shared_address_geocoding,
    shared_addresses,
)
from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
)

SHADOW_QUERY_DOCUMENTS_TABLE = "se_address_resolution_query_index_shadow"
SHADOW_REFERENCE_DOCUMENTS_TABLE = "se_address_resolution_reference_index_shadow"
SHADOW_CANDIDATES_TABLE = "se_address_resolution_candidates_shadow"
SHADOW_RESULTS_TABLE = "se_address_resolution_results_shadow"
SHADOW_COMPARISON_TABLE = "se_address_resolution_comparison_shadow"

QUALIFIED_SHADOW_QUERY_DOCUMENTS_TABLE = (
    f"{address_canonicalization.ENRICHMENT_SCHEMA}.{SHADOW_QUERY_DOCUMENTS_TABLE}"
)
QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE = (
    f"{address_canonicalization.ENRICHMENT_SCHEMA}.{SHADOW_REFERENCE_DOCUMENTS_TABLE}"
)
QUALIFIED_SHADOW_CANDIDATES_TABLE = (
    f"{address_canonicalization.ENRICHMENT_SCHEMA}.{SHADOW_CANDIDATES_TABLE}"
)
QUALIFIED_SHADOW_RESULTS_TABLE = (
    f"{address_canonicalization.ENRICHMENT_SCHEMA}.{SHADOW_RESULTS_TABLE}"
)
QUALIFIED_SHADOW_COMPARISON_TABLE = (
    f"{address_canonicalization.ENRICHMENT_SCHEMA}.{SHADOW_COMPARISON_TABLE}"
)

INDEX_SCOPE = "SE-address-resolution-shadow-v1"


def replace_sweden_address_resolution_shadow(
    *,
    connection: Any,
    evaluation_run_id: str,
    evaluated_at: datetime,
    log: Callable[..., object] | None,
) -> dict[str, object]:
    """Build a non-serving Sweden resolution index, result, and comparison."""
    connection.execute(
        f"create schema if not exists {address_canonicalization.ENRICHMENT_SCHEMA}"
    )
    _log(log, "Building Sweden address-resolution query search documents")
    _replace_query_documents(connection)
    _log(log, "Building Sweden OSM building search documents")
    _replace_building_reference_documents(connection)
    _log(log, "Building Sweden OSM contextual street search documents")
    _replace_street_reference_documents(connection)
    connection.execute(
        f"""
        create or replace table {QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE} as
        select * from _sweden_shadow_building_reference_documents
        union all
        select * from _sweden_shadow_street_reference_documents
        """
    )

    _log(log, "Generating Sweden address-resolution shadow candidates")
    replace_address_resolution_candidates(
        connection,
        query_table=QUALIFIED_SHADOW_QUERY_DOCUMENTS_TABLE,
        reference_table=QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE,
        candidate_table=QUALIFIED_SHADOW_CANDIDATES_TABLE,
        policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
    )
    _log(log, "Ranking Sweden address-resolution shadow candidates")
    replace_address_resolution_results(
        connection,
        query_table=QUALIFIED_SHADOW_QUERY_DOCUMENTS_TABLE,
        candidate_table=QUALIFIED_SHADOW_CANDIDATES_TABLE,
        result_table="_sweden_address_resolution_results_next",
        policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
    )
    connection.execute(
        f"""
        create or replace table {QUALIFIED_SHADOW_RESULTS_TABLE} as
        select
            *,
            ?::varchar as evaluation_run_id,
            ?::timestamptz as evaluated_at
        from _sweden_address_resolution_results_next
        """,
        [evaluation_run_id, evaluated_at],
    )
    _replace_comparison(connection)
    _assert_shadow_invariants(connection)
    return _shadow_counts(connection)


def _replace_query_documents(connection: Any) -> None:
    replace_address_search_documents(
        connection,
        table_name=QUALIFIED_SHADOW_QUERY_DOCUMENTS_TABLE,
        source_sql=f"""
            select
                '{INDEX_SCOPE}'::varchar as index_scope,
                cast(address_id as varchar) as document_id,
                country_code,
                canonical_display_address as raw_address,
                canonical_display_address as search_text,
                street_name,
                house_number,
                unit,
                postal_code,
                post_town as locality,
                case
                    when address_kind = 'physical'
                     and regexp_matches(
                        street_address,
                        '(?i)(^|[[:space:]])[0-9]+:[0-9]+($|[[:space:],])'
                     ) then 'property_identifier'
                    else address_kind
                end as address_kind,
                ''::varchar as reference_precision,
                null::double as latitude,
                null::double as longitude,
                null::double as coordinate_spread_meters,
                0::uinteger as supporting_record_count,
                cast(address_id as varchar) as source_record_id,
                ''::varchar as source_record_url
            from {shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE}
        """,
    )


def _replace_building_reference_documents(connection: Any) -> None:
    replace_address_search_documents(
        connection,
        table_name="_sweden_shadow_building_reference_documents",
        source_sql=f"""
            select
                '{INDEX_SCOPE}'::varchar as index_scope,
                source_record_id as document_id,
                country_code,
                coalesce(full_address, '') as raw_address,
                concat_ws(
                    ', ',
                    concat_ws(
                        ' ',
                        coalesce(nullif(street, ''), place, ''),
                        coalesce(house_number, '')
                    ),
                    concat_ws(
                        ' ',
                        coalesce(postcode, ''),
                        coalesce(city, '')
                    )
                ) as search_text,
                coalesce(nullif(street, ''), place, '') as street_name,
                coalesce(house_number, '') as house_number,
                coalesce(unit, '') as unit,
                coalesce(postcode, '') as postal_code,
                coalesce(city, '') as locality,
                'physical'::varchar as address_kind,
                'building'::varchar as reference_precision,
                latitude,
                longitude,
                0::double as coordinate_spread_meters,
                1::uinteger as supporting_record_count,
                source_record_id,
                source_record_url
            from {osm_tables.QUALIFIED_ADDRESS_TABLE}
            where coalesce(nullif(street, ''), place, '') != ''
              and coalesce(house_number, '') != ''
        """,
    )


def _replace_street_reference_documents(connection: Any) -> None:
    connection.execute(
        """
        create or replace temporary table _sweden_shadow_postcode_centroids as
        select
            country_code,
            normalized_postal_code,
            first(postal_code order by document_id) as postal_code,
            first(locality order by document_id) as locality,
            median(latitude)::double as latitude,
            median(longitude)::double as longitude
        from _sweden_shadow_building_reference_documents
        where normalized_postal_code != ''
        group by country_code, normalized_postal_code
        """
    )
    _replace_address_point_street_inputs(connection)
    _replace_road_search_documents(connection)
    _replace_road_street_inputs(connection)
    connection.execute(
        f"""
        create or replace temporary table _sweden_shadow_street_reference_input as
        with candidates as (
            select * from _sweden_shadow_address_point_street_input
            union all
            select * from _sweden_shadow_road_street_input
        )
        select * exclude (normalized_street, source_priority)
        from candidates
        qualify row_number() over (
            partition by
                country_code,
                normalized_street,
                regexp_replace(
                    strip_accents(lower(nfc_normalize(postal_code))),
                    '[^[:alnum:]]+',
                    '',
                    'g'
                )
            order by
                case
                    when coordinate_spread_meters
                        <= {SWEDEN_ADDRESS_RESOLUTION_POLICY.area_maximum_spread_meters}
                     and source_priority = 0 then 0
                    when coordinate_spread_meters
                        <= {SWEDEN_ADDRESS_RESOLUTION_POLICY.area_maximum_spread_meters}
                        then 1
                    when source_priority = 0 then 2
                    else 3
                end,
                supporting_record_count desc,
                document_id
        ) = 1
        """
    )
    replace_address_search_documents(
        connection,
        source_sql="select * from _sweden_shadow_street_reference_input",
        table_name="_sweden_shadow_street_reference_documents",
    )


def _replace_address_point_street_inputs(connection: Any) -> None:
    connection.execute(
        f"""
        create or replace temporary table
            _sweden_shadow_address_point_street_input as
        select
            '{INDEX_SCOPE}'::varchar as index_scope,
            concat(
                'address-point-street/',
                md5(concat_ws(
                    '|',
                    country_code,
                    normalized_street,
                    normalized_postal_code,
                    normalized_locality
                ))
            ) as document_id,
            country_code,
            ''::varchar as raw_address,
            concat_ws(
                ', ',
                first(street_name order by document_id),
                concat_ws(
                    ' ',
                    first(postal_code order by document_id),
                    first(locality order by document_id)
                )
            ) as search_text,
            first(street_name order by document_id) as street_name,
            ''::varchar as house_number,
            ''::varchar as unit,
            first(postal_code order by document_id) as postal_code,
            first(locality order by document_id) as locality,
            'physical'::varchar as address_kind,
            'street'::varchar as reference_precision,
            median(latitude)::double as latitude,
            median(longitude)::double as longitude,
            {_spread_sql()} as coordinate_spread_meters,
            count(*)::uinteger as supporting_record_count,
            concat(
                'osm-address-point-street/',
                md5(concat_ws(
                    '|',
                    country_code,
                    normalized_street,
                    normalized_postal_code,
                    normalized_locality
                ))
            ) as source_record_id,
            ''::varchar as source_record_url,
            normalized_street,
            0::utinyint as source_priority
        from _sweden_shadow_building_reference_documents
        where normalized_street != ''
          and (normalized_postal_code != '' or normalized_locality != '')
        group by
            country_code,
            normalized_street,
            normalized_postal_code,
            normalized_locality
        """
    )


def _replace_road_search_documents(connection: Any) -> None:
    replace_address_search_documents(
        connection,
        source_sql=f"""
            select
                '{INDEX_SCOPE}'::varchar as index_scope,
                concat('road/', source_record_id) as document_id,
                'SE'::varchar as country_code,
                street as raw_address,
                street as search_text,
                street as street_name,
                ''::varchar as house_number,
                ''::varchar as unit,
                ''::varchar as postal_code,
                ''::varchar as locality,
                'physical'::varchar as address_kind,
                'street'::varchar as reference_precision,
                latitude,
                longitude,
                0::double as coordinate_spread_meters,
                1::uinteger as supporting_record_count,
                source_record_id,
                source_record_url
            from {osm_tables.QUALIFIED_STREET_SEGMENT_TABLE}
            where coalesce(street, '') != ''
        """,
        table_name="_sweden_shadow_road_documents",
    )


def _replace_road_street_inputs(connection: Any) -> None:
    connection.execute(
        f"""
        create or replace temporary table _sweden_shadow_road_street_input as
        with postcode_cells as (
            select
                postcode.*,
                floor(
                    latitude / {address_matching.ROAD_LATITUDE_GRID_DEGREES}
                )::integer + latitude_offset.value as latitude_cell,
                floor(
                    longitude / {address_matching.ROAD_LONGITUDE_GRID_DEGREES}
                )::integer + longitude_offset.value as longitude_cell
            from _sweden_shadow_postcode_centroids postcode
            cross join range(
                -{address_matching.ROAD_LATITUDE_NEIGHBOR_CELLS},
                {address_matching.ROAD_LATITUDE_NEIGHBOR_CELLS + 1}
            ) latitude_offset(value)
            cross join range(
                -{address_matching.ROAD_LONGITUDE_NEIGHBOR_CELLS},
                {address_matching.ROAD_LONGITUDE_NEIGHBOR_CELLS + 1}
            ) longitude_offset(value)
        ), road_cells as (
            select
                *,
                floor(
                    latitude / {address_matching.ROAD_LATITUDE_GRID_DEGREES}
                )::integer as latitude_cell,
                floor(
                    longitude / {address_matching.ROAD_LONGITUDE_GRID_DEGREES}
                )::integer as longitude_cell
            from _sweden_shadow_road_documents
            where normalized_street != ''
        ), nearby as (
            select
                road.*,
                postcode.postal_code,
                postcode.locality,
                postcode.normalized_postal_code,
                2 * 6371000 * asin(least(1.0, sqrt(
                    pow(
                        sin(radians(road.latitude - postcode.latitude) / 2),
                        2
                    )
                    + cos(radians(postcode.latitude))
                      * cos(radians(road.latitude))
                      * pow(
                          sin(
                              radians(
                                  road.longitude - postcode.longitude
                              ) / 2
                          ),
                          2
                      )
                )))::double as postcode_distance_meters
            from road_cells road
            inner join postcode_cells postcode
                using (latitude_cell, longitude_cell)
        )
        select
            '{INDEX_SCOPE}'::varchar as index_scope,
            concat(
                'road-street/',
                md5(concat_ws(
                    '|',
                    country_code,
                    normalized_street,
                    normalized_postal_code
                ))
            ) as document_id,
            country_code,
            ''::varchar as raw_address,
            concat_ws(
                ', ',
                first(street_name order by document_id),
                concat_ws(
                    ' ',
                    first(postal_code order by document_id),
                    first(locality order by document_id)
                )
            ) as search_text,
            first(street_name order by document_id) as street_name,
            ''::varchar as house_number,
            ''::varchar as unit,
            first(postal_code order by document_id) as postal_code,
            first(locality order by document_id) as locality,
            'physical'::varchar as address_kind,
            'street'::varchar as reference_precision,
            median(latitude)::double as latitude,
            median(longitude)::double as longitude,
            {_spread_sql()} as coordinate_spread_meters,
            count(distinct source_record_id)::uinteger
                as supporting_record_count,
            concat(
                'osm-road-street/',
                md5(concat_ws(
                    '|',
                    country_code,
                    normalized_street,
                    normalized_postal_code
                ))
            ) as source_record_id,
            ''::varchar as source_record_url,
            normalized_street,
            1::utinyint as source_priority
        from nearby
        where postcode_distance_meters
            <= {address_matching.ROAD_POSTCODE_CONTEXT_MAX_DISTANCE_METERS}
        group by country_code, normalized_street, normalized_postal_code
        """
    )


def _replace_comparison(connection: Any) -> None:
    connection.execute(
        f"""
        create or replace table {QUALIFIED_SHADOW_COMPARISON_TABLE} as
        select
            shadow.query_document_id as address_id,
            current.match_status as current_status,
            shadow.resolution_status as shadow_status,
            shadow.geocode_precision as shadow_precision,
            shadow.match_confidence as shadow_confidence,
            shadow.match_strategy as shadow_strategy,
            shadow.corrections,
            shadow.matched_street_name,
            shadow.matched_house_number,
            shadow.matched_postal_code,
            shadow.matched_locality,
            shadow.candidate_record_count,
            shadow.runner_up_score_margin,
            shadow.policy_version,
            shadow.evaluation_run_id,
            shadow.evaluated_at
        from {QUALIFIED_SHADOW_RESULTS_TABLE} shadow
        inner join {
            shared_address_geocoding.QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE
        } current
            on cast(current.address_id as varchar) = shadow.query_document_id
        """
    )


def _assert_shadow_invariants(connection: Any) -> None:
    [(queries, distinct_queries)] = connection.execute(
        f"""
        select count(*), count(distinct document_id)
        from {QUALIFIED_SHADOW_QUERY_DOCUMENTS_TABLE}
        """
    ).fetchall()
    [(results, distinct_results)] = connection.execute(
        f"""
        select count(*), count(distinct query_document_id)
        from {QUALIFIED_SHADOW_RESULTS_TABLE}
        """
    ).fetchall()
    [(comparisons,)] = connection.execute(
        f"select count(*) from {QUALIFIED_SHADOW_COMPARISON_TABLE}"
    ).fetchall()
    if int(queries) != int(distinct_queries):
        raise ValueError("Shadow query search-document IDs must be unique")
    if int(results) != int(distinct_results) or int(results) != int(queries):
        raise ValueError("Every shadow query must have one resolution result")
    if int(comparisons) != int(results):
        raise ValueError("Every shadow result must compare with the current matcher")


def _shadow_counts(connection: Any) -> dict[str, object]:
    status_rows = connection.execute(
        f"""
        select shadow_status, count(*)
        from {QUALIFIED_SHADOW_COMPARISON_TABLE}
        group by shadow_status
        order by shadow_status
        """
    ).fetchall()
    transition_rows = connection.execute(
        f"""
        select current_status, shadow_status, count(*) as address_count
        from {QUALIFIED_SHADOW_COMPARISON_TABLE}
        where current_status != shadow_status
        group by current_status, shadow_status
        order by address_count desc, current_status, shadow_status
        limit 20
        """
    ).fetchall()
    [(queries, references, candidates, results, changed)] = connection.execute(
        f"""
        select
            (select count(*) from {QUALIFIED_SHADOW_QUERY_DOCUMENTS_TABLE}),
            (select count(*) from {QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE}),
            (select count(*) from {QUALIFIED_SHADOW_CANDIDATES_TABLE}),
            (select count(*) from {QUALIFIED_SHADOW_RESULTS_TABLE}),
            (
                select count(*)
                from {QUALIFIED_SHADOW_COMPARISON_TABLE}
                where current_status != shadow_status
            )
        """
    ).fetchall()
    return {
        "query_documents": int(queries),
        "reference_documents": int(references),
        "candidates": int(candidates),
        "results": int(results),
        "changed_results": int(changed),
        "shadow_status_counts": {
            str(status): int(count) for status, count in status_rows
        },
        "largest_transitions": [
            {
                "current_status": str(current),
                "shadow_status": str(shadow),
                "address_count": int(count),
            }
            for current, shadow, count in transition_rows
        ],
        "query_table": QUALIFIED_SHADOW_QUERY_DOCUMENTS_TABLE,
        "reference_table": QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE,
        "candidate_table": QUALIFIED_SHADOW_CANDIDATES_TABLE,
        "result_table": QUALIFIED_SHADOW_RESULTS_TABLE,
        "comparison_table": QUALIFIED_SHADOW_COMPARISON_TABLE,
    }


def _spread_sql() -> str:
    return """
2 * 6371000 * asin(least(1.0, sqrt(
    pow(sin(radians(max(latitude) - min(latitude)) / 2), 2)
    + cos(radians(min(latitude))) * cos(radians(max(latitude)))
      * pow(sin(radians(max(longitude) - min(longitude)) / 2), 2)
)))::double
""".strip()


def _log(
    log: Callable[..., object] | None,
    message: str,
) -> None:
    if log is not None:
        log(message)
