import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

from dagster_v3.defs.sweden_address_osm import address_matching
from dagster_v3.defs.sweden_address_osm import tables as osm_tables
from dagster_v3.defs.sweden_company import address_canonicalization, shared_addresses

ADDRESS_GEOCODES_TABLE = "se_address_geocodes_current"
QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE = (
    f"{address_canonicalization.ENRICHMENT_SCHEMA}.{ADDRESS_GEOCODES_TABLE}"
)
QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE = (
    f"{address_canonicalization.CLICKHOUSE_DATABASE}.{ADDRESS_GEOCODES_TABLE}"
)

ADDRESS_GEOCODE_COLUMNS = (
    "address_id",
    "address_identity_run_id",
    "normalized_match_key",
    "match_status",
    "candidate_count",
    "candidate_record_ids",
    "candidate_record_urls",
    "match_method",
    "match_confidence",
    "latitude",
    "longitude",
    "geocode_provider",
    "geocode_precision",
    "coordinate_method",
    "coordinate_locality",
    "coordinate_supporting_point_count",
    "coordinate_spread_meters",
    "source_record_id",
    "source_record_url",
    "source_url",
    "source_object_key",
    "source_md5",
    "source_snapshot_at",
    "source_retrieved_at",
    "geocode_run_id",
    "matched_at",
)


def replace_sweden_shared_address_osm_matches(
    *,
    connection: Any,
    geocode_run_id: str,
    matched_at: datetime,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Classify shared addresses with exact and flagged street-level OSM matches."""
    _assert_shared_addresses_available(connection)
    connection.execute("begin transaction")
    try:
        stage_started_at = time.monotonic()
        _log(log, "Building unique Sweden OSM address match keys")
        _create_osm_match_reference_tables(connection)
        _log(
            log,
            "Built Sweden OSM address reference tables: postcode_candidates=%d "
            "city_candidates=%d country_candidates=%d street_locations=%d "
            "elapsed_seconds=%.1f",
            _count(connection, "_sweden_shared_osm_match_candidates"),
            _count(connection, "_sweden_shared_osm_city_address_match_candidates"),
            _count(connection, "_sweden_shared_osm_street_address_match_candidates"),
            _count(connection, "_sweden_shared_osm_postcode_street_locations"),
            time.monotonic() - stage_started_at,
        )

        stage_started_at = time.monotonic()
        _log(log, "Matching shared Sweden addresses to OSM")
        _create_shared_address_geocode_results(
            connection=connection,
            geocode_run_id=geocode_run_id,
            matched_at=matched_at,
        )
        _assert_shared_address_geocode_invariants(connection)
        connection.execute("commit")
        _log(
            log,
            "Matched shared Sweden addresses to OSM: rows=%d elapsed_seconds=%.1f",
            _count(connection, QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE),
            time.monotonic() - stage_started_at,
        )
    except Exception:
        connection.execute("rollback")
        raise

    status_counts = {
        str(status): int(count)
        for status, count in connection.execute(
            f"""
            select match_status, count(*)
            from {QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE}
            group by match_status
            """
        ).fetchall()
    }
    geolocated = connection.execute(
        f"""
        select count(*)
        from {QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE}
        where latitude is not null and longitude is not null
        """
    ).fetchone()
    return {
        "addresses": _count(connection, QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE),
        "geolocated": int(geolocated[0]) if geolocated is not None else 0,
        "matched_exact": status_counts.get("matched_exact", 0),
        "matched_site": status_counts.get("matched_site", 0),
        "matched_area": status_counts.get("matched_area", 0),
        "matched_street": status_counts.get("matched_street", 0),
        "ambiguous": status_counts.get("ambiguous", 0),
        "unmatched": status_counts.get("unmatched", 0),
        "invalid_address": status_counts.get("invalid_address", 0),
        "foreign_address": status_counts.get("foreign_address", 0),
        "postal_box": status_counts.get("postal_box", 0),
    }


def _create_osm_match_reference_tables(connection: Any) -> None:
    address_matching.replace_osm_address_match_components(connection)
    address_matching.replace_postcode_address_match_candidates(
        connection,
        table_name="_sweden_shared_osm_match_candidates",
    )
    address_matching.replace_city_address_match_candidates(
        connection,
        table_name="_sweden_shared_osm_city_address_match_candidates",
    )
    address_matching.replace_country_address_match_candidates(
        connection,
        table_name="_sweden_shared_osm_street_address_match_candidates",
    )
    address_matching.replace_postcode_street_location_candidates(
        connection,
        table_name="_sweden_shared_osm_postcode_street_locations",
    )
    connection.execute(
        f"""
        create or replace temporary table _sweden_shared_osm_snapshot_provenance as
        select
            first(source_url order by source_record_id) as source_url,
            first(source_object_key order by source_record_id) as source_object_key,
            first(source_md5 order by source_record_id) as source_md5,
            first(source_snapshot_at order by source_record_id) as source_snapshot_at,
            first(source_retrieved_at order by source_record_id) as source_retrieved_at
        from {osm_tables.QUALIFIED_ADDRESS_TABLE}
        """
    )
    connection.execute(
        f"""
        create or replace temporary table _sweden_shared_osm_city_centroids as
        select
            normalized_city,
            first(city order by source_record_id) as locality,
            count(*)::uinteger as supporting_point_count,
            median(latitude)::double as latitude,
            median(longitude)::double as longitude
        from {osm_tables.QUALIFIED_ADDRESS_TABLE}
        where normalized_city != ''
        group by normalized_city
        """
    )


def _create_shared_address_geocode_results(
    *,
    connection: Any,
    geocode_run_id: str,
    matched_at: datetime,
) -> None:
    street_location_key_sql = address_matching.normalized_street_location_key_sql(
        street_name_sql="street_name",
        street_address_sql="street_address",
        normalized_postcode_sql="normalized_postal_code",
    )
    street_house_key_sql = address_matching.normalized_street_house_key_sql(
        street_name_sql="street_name",
        house_number_sql="house_number",
        fallback_normalized_street_sql="normalized_street",
    )
    connection.execute(
        f"""
        create or replace table {QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE} as
        with address_keys as materialized (
            select
                *,
                concat_ws(
                    '|',
                    normalized_postal_code,
                    {street_house_key_sql}
                )
                    as normalized_match_key,
                concat_ws(
                    '|',
                    normalized_post_town,
                    {street_house_key_sql}
                )
                    as normalized_city_address_match_key,
                {street_house_key_sql} as normalized_street_address_match_key,
                {street_location_key_sql} as normalized_street_location_key,
                case
                    when address_kind = 'foreign' then 'foreign_address'
                    when address_kind = 'postal_box' then 'postal_box'
                    when address_kind = 'incomplete' then 'invalid_address'
                    else 'eligible'
                end as eligibility
            from {shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE}
        ), joined as (
            select
                address.*,
                coalesce(
                    postcode_osm.candidate_count,
                    city_osm.candidate_count,
                    street_osm.candidate_count,
                    0
                )::usmallint as candidate_count,
                coalesce(
                    postcode_osm.latitude,
                    city_osm.latitude,
                    street_osm.latitude
                ) as latitude,
                coalesce(
                    postcode_osm.longitude,
                    city_osm.longitude,
                    street_osm.longitude
                ) as longitude,
                coalesce(
                    postcode_osm.coordinate_method,
                    city_osm.coordinate_method,
                    street_osm.coordinate_method
                ) as coordinate_method,
                coalesce(
                    postcode_osm.coordinate_spread_meters,
                    city_osm.coordinate_spread_meters,
                    street_osm.coordinate_spread_meters
                ) as coordinate_spread_meters,
                coalesce(
                    postcode_osm.source_record_id,
                    city_osm.source_record_id,
                    street_osm.source_record_id
                ) as source_record_id,
                coalesce(
                    postcode_osm.source_record_url,
                    city_osm.source_record_url,
                    street_osm.source_record_url
                ) as source_record_url,
                coalesce(
                    postcode_osm.candidate_record_ids,
                    city_osm.candidate_record_ids,
                    street_osm.candidate_record_ids,
                    []::varchar[]
                )
                    as candidate_record_ids,
                coalesce(
                    postcode_osm.candidate_record_urls,
                    city_osm.candidate_record_urls,
                    street_osm.candidate_record_urls,
                    []::varchar[]
                )
                    as candidate_record_urls,
                case
                    when postcode_osm.candidate_count is not null then 'postal_code'
                    when city_osm.candidate_count is not null then 'city'
                    when street_osm.candidate_count is not null
                        then 'country_street_house'
                    else ''
                end as match_basis,
                city.locality as coordinate_locality,
                coalesce(city.supporting_point_count, 0)::uinteger
                    as coordinate_supporting_point_count,
                city.latitude as city_latitude,
                city.longitude as city_longitude,
                street_location.latitude as street_location_latitude,
                street_location.longitude as street_location_longitude,
                street_location.supporting_point_count
                    as street_location_supporting_point_count,
                street_location.coordinate_spread_meters
                    as street_location_spread_meters,
                street_location.match_method as street_location_match_method,
                street_location.match_confidence
                    as street_location_match_confidence,
                street_location.coordinate_method
                    as street_location_coordinate_method,
                snapshot.source_url,
                snapshot.source_object_key,
                snapshot.source_md5,
                snapshot.source_snapshot_at,
                snapshot.source_retrieved_at
            from address_keys address
            left join _sweden_shared_osm_match_candidates postcode_osm
                on postcode_osm.normalized_match_key = case
                    when address.eligibility = 'eligible'
                        then address.normalized_match_key
                    else null
                end
            left join _sweden_shared_osm_city_address_match_candidates city_osm
                on city_osm.normalized_match_key = case
                    when address.eligibility = 'eligible'
                        then address.normalized_city_address_match_key
                    else null
                end
            left join _sweden_shared_osm_street_address_match_candidates street_osm
                on street_osm.normalized_match_key = case
                    when address.eligibility = 'eligible'
                        then address.normalized_street_address_match_key
                    else null
                end
            left join _sweden_shared_osm_city_centroids city
                on city.normalized_city = case
                    when address.eligibility = 'postal_box'
                        then address.normalized_post_town
                    else null
                end
            left join _sweden_shared_osm_postcode_street_locations street_location
                on street_location.normalized_street_location_key = case
                    when address.eligibility = 'eligible'
                        then address.normalized_street_location_key
                    else null
                end
            cross join _sweden_shared_osm_snapshot_provenance snapshot
        )
        select
            address_id,
            address_identity_run_id,
            normalized_match_key,
            case
                when eligibility != 'eligible' then eligibility
                when candidate_count = 0
                 and street_location_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then 'matched_street'
                when candidate_count = 0 then 'unmatched'
                when candidate_count = 1 then 'matched_exact'
                when coordinate_spread_meters
                    <= {address_matching.SITE_MAX_SPREAD_METERS}
                    then 'matched_site'
                when coordinate_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then 'matched_area'
                else 'ambiguous'
            end as match_status,
            candidate_count,
            candidate_record_ids,
            candidate_record_urls,
            case
                when candidate_count = 1 and match_basis = 'postal_code'
                    then 'postal_code_street_house_exact_unique'
                when candidate_count = 1 and match_basis = 'city'
                    then 'city_street_house_exact_unique'
                when candidate_count = 1 and match_basis = 'country_street_house'
                    then 'country_street_house_exact_unique'
                when candidate_count > 1
                 and coordinate_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                 and match_basis = 'postal_code'
                    then 'postal_code_street_house_candidate_median'
                when candidate_count > 1
                 and coordinate_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                 and match_basis = 'city'
                    then 'city_street_house_candidate_median'
                when candidate_count > 1
                 and coordinate_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                 and match_basis = 'country_street_house'
                    then 'country_street_house_candidate_median'
                when eligibility = 'postal_box' and city_latitude is not null
                    then 'post_town_osm_address_point_median'
                when candidate_count = 0
                 and street_location_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then street_location_match_method
                else ''
            end as match_method,
            case
                when candidate_count = 1 then 1.0::float
                when coordinate_spread_meters
                    <= {address_matching.SITE_MAX_SPREAD_METERS}
                    then 0.8::float
                when coordinate_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then 0.6::float
                when eligibility = 'postal_box' and city_latitude is not null
                    then 0.5::float
                when candidate_count = 0
                 and street_location_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then street_location_match_confidence
                else 0.0::float
            end as match_confidence,
            case
                when candidate_count = 1
                  or coordinate_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then latitude
                when eligibility = 'postal_box' then city_latitude
                when candidate_count = 0
                 and street_location_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then street_location_latitude
                else null
            end as latitude,
            case
                when candidate_count = 1
                  or coordinate_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then longitude
                when eligibility = 'postal_box' then city_longitude
                when candidate_count = 0
                 and street_location_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then street_location_longitude
                else null
            end as longitude,
            case
                when candidate_count = 1
                  or coordinate_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                  or eligibility = 'postal_box' and city_latitude is not null
                  or candidate_count = 0
                   and street_location_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then 'openstreetmap'
                else ''
            end as geocode_provider,
            case
                when candidate_count = 1 then 'building'
                when coordinate_spread_meters
                    <= {address_matching.SITE_MAX_SPREAD_METERS}
                    then 'site'
                when coordinate_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then 'area'
                when eligibility = 'postal_box' and city_latitude is not null
                    then 'city'
                when candidate_count = 0
                 and street_location_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then 'street'
                else ''
            end as geocode_precision,
            case
                when candidate_count = 1
                  or coordinate_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then coordinate_method
                when eligibility = 'postal_box' and city_latitude is not null
                    then 'osm_city_address_point_median'
                when candidate_count = 0
                 and street_location_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then street_location_coordinate_method
                else null
            end as coordinate_method,
            case
                when eligibility = 'postal_box' and city_latitude is not null
                    then coordinate_locality
                else null
            end as coordinate_locality,
            case
                when candidate_count >= 1
                 and coordinate_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then candidate_count::uinteger
                when eligibility = 'postal_box' and city_latitude is not null
                    then coordinate_supporting_point_count
                when candidate_count = 0
                 and street_location_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then street_location_supporting_point_count
                else 0
            end as coordinate_supporting_point_count,
            case
                when candidate_count >= 1 then coordinate_spread_meters
                when candidate_count = 0
                 and street_location_spread_meters
                    <= {address_matching.AREA_MAX_SPREAD_METERS}
                    then street_location_spread_meters
                else null
            end as coordinate_spread_meters,
            case when candidate_count = 1 then source_record_id else null end
                as source_record_id,
            case when candidate_count = 1 then source_record_url else null end
                as source_record_url,
            source_url,
            source_object_key,
            source_md5,
            source_snapshot_at,
            source_retrieved_at,
            ?::varchar as geocode_run_id,
            ?::timestamptz as matched_at
        from joined
        """,
        [geocode_run_id, matched_at],
    )


def _assert_shared_address_geocode_invariants(connection: Any) -> None:
    [(address_rows, unique_addresses)] = connection.execute(
        f"""
        select count(*), count(distinct address_id)
        from {shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE}
        """
    ).fetchall()
    [
        (
            result_rows,
            unique_results,
            invalid_coordinates,
            invalid_spatial_matches,
        )
    ] = connection.execute(
        f"""
        select
            count(*),
            count(distinct address_id),
            count(*) filter (
                where (latitude is null) != (longitude is null)
                   or latitude not between -90 and 90
                   or longitude not between -180 and 180
            ),
            count(*) filter (
                where match_status = 'matched_site' and (
                    candidate_count <= 1
                    or latitude is null
                    or longitude is null
                    or geocode_precision != 'site'
                    or coordinate_supporting_point_count != candidate_count
                    or coordinate_spread_meters is null
                    or coordinate_spread_meters
                        > {address_matching.SITE_MAX_SPREAD_METERS}
                )
                or match_status = 'matched_area' and (
                    candidate_count <= 1
                    or latitude is null
                    or longitude is null
                    or geocode_precision != 'area'
                    or coordinate_supporting_point_count != candidate_count
                    or coordinate_spread_meters is null
                    or coordinate_spread_meters
                        <= {address_matching.SITE_MAX_SPREAD_METERS}
                    or coordinate_spread_meters
                        > {address_matching.AREA_MAX_SPREAD_METERS}
                )
                or match_status = 'matched_street' and (
                    candidate_count != 0
                    or len(candidate_record_ids) != 0
                    or len(candidate_record_urls) != 0
                    or latitude is null
                    or longitude is null
                    or geocode_provider != 'openstreetmap'
                    or geocode_precision != 'street'
                    or not (
                        match_method = '{address_matching.STREET_FALLBACK_METHOD}'
                        and abs(
                            match_confidence
                            - {address_matching.STREET_FALLBACK_CONFIDENCE}
                        ) <= 0.001
                        and coordinate_method
                            = '{address_matching.STREET_FALLBACK_COORDINATE_METHOD}'
                        or match_method
                            = '{address_matching.ROAD_STREET_FALLBACK_METHOD}'
                        and abs(
                            match_confidence
                            - {address_matching.ROAD_STREET_FALLBACK_CONFIDENCE}
                        ) <= 0.001
                        and coordinate_method = '{
            address_matching.ROAD_STREET_FALLBACK_COORDINATE_METHOD
        }'
                    )
                    or coordinate_supporting_point_count = 0
                    or coordinate_spread_meters is null
                    or coordinate_spread_meters < 0
                    or coordinate_spread_meters
                        > {address_matching.AREA_MAX_SPREAD_METERS}
                    or source_record_id is not null
                    or source_record_url is not null
                )
                or match_status = 'ambiguous' and (
                    candidate_count <= 1
                    or latitude is not null
                    or longitude is not null
                    or coordinate_spread_meters is null
                    or coordinate_spread_meters
                        <= {address_matching.AREA_MAX_SPREAD_METERS}
                )
            )
        from {QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE}
        """
    ).fetchall()
    if int(address_rows) != int(result_rows) or int(result_rows) != int(unique_results):
        raise ValueError("Every shared Sweden address must have one geocoding outcome")
    if int(address_rows) != int(unique_addresses):
        raise ValueError("Shared Sweden address IDs must be unique before geocoding")
    if int(invalid_coordinates) != 0:
        raise ValueError("Shared Sweden address coordinates must be valid WGS84 pairs")
    if int(invalid_spatial_matches) != 0:
        raise ValueError(
            "Shared Sweden address spatial match classifications are invalid"
        )


def _assert_shared_addresses_available(connection: Any) -> None:
    if _count(connection, shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE) == 0:
        raise ValueError(
            "Shared Sweden addresses must be materialized before OSM matching"
        )


def _count(connection: Any, qualified_table: str) -> int:
    row = connection.execute(f"select count(*) from {qualified_table}").fetchone()
    return int(row[0]) if row is not None else 0


def _log(
    log: Callable[..., object] | None,
    message: str,
    *args: object,
) -> None:
    if log is not None:
        log(message, *args)
