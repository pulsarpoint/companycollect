import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

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
    """Classify every shared address with exact postcode or city OSM matches."""
    _assert_shared_addresses_available(connection)
    connection.execute("begin transaction")
    try:
        stage_started_at = time.monotonic()
        _log(log, "Building unique Sweden OSM address match keys")
        _create_osm_match_reference_tables(connection)
        _log(
            log,
            "Built Sweden OSM address reference tables: postcode_candidates=%d "
            "city_candidates=%d elapsed_seconds=%.1f",
            _count(connection, "_sweden_shared_osm_match_candidates"),
            _count(connection, "_sweden_shared_osm_city_address_match_candidates"),
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
        "ambiguous": status_counts.get("ambiguous", 0),
        "unmatched": status_counts.get("unmatched", 0),
        "invalid_address": status_counts.get("invalid_address", 0),
        "foreign_address": status_counts.get("foreign_address", 0),
        "postal_box": status_counts.get("postal_box", 0),
    }


def _create_osm_match_reference_tables(connection: Any) -> None:
    connection.execute(
        f"""
        create or replace temporary table _sweden_shared_osm_match_candidates as
        with normalized as (
            select
                concat_ws(
                    '|',
                    normalized_postcode,
                    regexp_replace(
                        concat(normalized_street, normalized_house_number),
                        '[^[:alnum:]]+',
                        '',
                        'g'
                    )
                ) as normalized_match_key,
                *
            from {osm_tables.QUALIFIED_ADDRESS_TABLE}
            where normalized_postcode != ''
              and normalized_street != ''
              and normalized_house_number != ''
        )
        select
            normalized_match_key,
            count(*)::usmallint as candidate_count,
            first(latitude order by source_record_id) as latitude,
            first(longitude order by source_record_id) as longitude,
            first(coordinate_method order by source_record_id) as coordinate_method,
            first(source_record_id order by source_record_id) as source_record_id,
            first(source_record_url order by source_record_id) as source_record_url,
            list(source_record_id order by source_record_id) as candidate_record_ids,
            list(source_record_url order by source_record_id) as candidate_record_urls,
            first(source_url order by source_record_id) as source_url,
            first(source_object_key order by source_record_id) as source_object_key,
            first(source_md5 order by source_record_id) as source_md5,
            first(source_snapshot_at order by source_record_id) as source_snapshot_at,
            first(source_retrieved_at order by source_record_id) as source_retrieved_at
        from normalized
        group by normalized_match_key
        """
    )
    connection.execute(
        f"""
        create or replace temporary table
            _sweden_shared_osm_city_address_match_candidates as
        with normalized as (
            select
                concat_ws(
                    '|',
                    normalized_city,
                    regexp_replace(
                        concat(normalized_street, normalized_house_number),
                        '[^[:alnum:]]+',
                        '',
                        'g'
                    )
                ) as normalized_match_key,
                *
            from {osm_tables.QUALIFIED_ADDRESS_TABLE}
            where normalized_postcode = ''
              and normalized_city != ''
              and normalized_street != ''
              and normalized_house_number != ''
        )
        select
            normalized_match_key,
            count(*)::usmallint as candidate_count,
            first(latitude order by source_record_id) as latitude,
            first(longitude order by source_record_id) as longitude,
            first(coordinate_method order by source_record_id) as coordinate_method,
            first(source_record_id order by source_record_id) as source_record_id,
            first(source_record_url order by source_record_id) as source_record_url,
            list(source_record_id order by source_record_id) as candidate_record_ids,
            list(source_record_url order by source_record_id) as candidate_record_urls
        from normalized
        group by normalized_match_key
        """
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
    connection.execute(
        f"""
        create or replace table {QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE} as
        with address_keys as materialized (
            select
                *,
                concat_ws('|', normalized_postal_code, normalized_street)
                    as normalized_match_key,
                concat_ws('|', normalized_post_town, normalized_street)
                    as normalized_city_address_match_key,
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
                    0
                )::usmallint as candidate_count,
                coalesce(postcode_osm.latitude, city_osm.latitude) as latitude,
                coalesce(postcode_osm.longitude, city_osm.longitude) as longitude,
                coalesce(
                    postcode_osm.coordinate_method,
                    city_osm.coordinate_method
                ) as coordinate_method,
                coalesce(
                    postcode_osm.source_record_id,
                    city_osm.source_record_id
                ) as source_record_id,
                coalesce(
                    postcode_osm.source_record_url,
                    city_osm.source_record_url
                ) as source_record_url,
                coalesce(
                    postcode_osm.candidate_record_ids,
                    city_osm.candidate_record_ids,
                    []::varchar[]
                )
                    as candidate_record_ids,
                coalesce(
                    postcode_osm.candidate_record_urls,
                    city_osm.candidate_record_urls,
                    []::varchar[]
                )
                    as candidate_record_urls,
                case
                    when postcode_osm.candidate_count is not null then 'postal_code'
                    when city_osm.candidate_count is not null then 'city'
                    else ''
                end as match_basis,
                city.locality as coordinate_locality,
                coalesce(city.supporting_point_count, 0)::uinteger
                    as coordinate_supporting_point_count,
                city.latitude as city_latitude,
                city.longitude as city_longitude,
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
            left join _sweden_shared_osm_city_centroids city
                on city.normalized_city = case
                    when address.eligibility = 'postal_box'
                        then address.normalized_post_town
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
                when candidate_count = 0 then 'unmatched'
                when candidate_count = 1 then 'matched_exact'
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
                when eligibility = 'postal_box' and city_latitude is not null
                    then 'post_town_osm_address_point_median'
                else ''
            end as match_method,
            case
                when candidate_count = 1 then 1.0::float
                when eligibility = 'postal_box' and city_latitude is not null
                    then 0.5::float
                else 0.0::float
            end as match_confidence,
            case
                when candidate_count = 1 then latitude
                when eligibility = 'postal_box' then city_latitude
                else null
            end as latitude,
            case
                when candidate_count = 1 then longitude
                when eligibility = 'postal_box' then city_longitude
                else null
            end as longitude,
            case
                when candidate_count = 1
                  or eligibility = 'postal_box' and city_latitude is not null
                    then 'openstreetmap'
                else ''
            end as geocode_provider,
            case
                when candidate_count = 1 then 'building'
                when eligibility = 'postal_box' and city_latitude is not null
                    then 'city'
                else ''
            end as geocode_precision,
            case
                when candidate_count = 1 then coordinate_method
                when eligibility = 'postal_box' and city_latitude is not null
                    then 'osm_city_address_point_median'
                else null
            end as coordinate_method,
            case
                when eligibility = 'postal_box' and city_latitude is not null
                    then coordinate_locality
                else null
            end as coordinate_locality,
            case
                when eligibility = 'postal_box' and city_latitude is not null
                    then coordinate_supporting_point_count
                else 0
            end as coordinate_supporting_point_count,
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
    [(result_rows, unique_results, invalid_coordinates)] = connection.execute(
        f"""
        select
            count(*),
            count(distinct address_id),
            count(*) filter (
                where (latitude is null) != (longitude is null)
                   or latitude not between -90 and 90
                   or longitude not between -180 and 180
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
