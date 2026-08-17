from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

from dagster_v3.defs.sweden_address_osm import address_matching
from dagster_v3.defs.sweden_address_osm import tables as osm_tables
from dagster_v3.defs.sweden_company import address_canonicalization

ENRICHMENT_SCHEMA = "sweden_company_enrichment"
MATCH_RESULTS_TABLE = "address_osm_match_results"
GEOCODES_TABLE = "address_geocodes"
QUALIFIED_MATCH_RESULTS_TABLE = f"{ENRICHMENT_SCHEMA}.{MATCH_RESULTS_TABLE}"
QUALIFIED_GEOCODES_TABLE = f"{ENRICHMENT_SCHEMA}.{GEOCODES_TABLE}"

CLICKHOUSE_DATABASE = "corpscout"
CLICKHOUSE_TABLE = "se_company_address_geocodes"
QUALIFIED_CLICKHOUSE_TABLE = f"{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}"
CLICKHOUSE_RESULTS_TABLE = "se_company_address_geocode_results"
QUALIFIED_CLICKHOUSE_RESULTS_TABLE = f"{CLICKHOUSE_DATABASE}.{CLICKHOUSE_RESULTS_TABLE}"

ELIGIBLE_OSM_MATCH_KEY_SQL = """
case
    when company.eligibility = 'eligible' then company.normalized_match_key
    else null
end
""".strip()

CLICKHOUSE_EXPORT_COLUMNS = (
    "company_id",
    "address_key",
    "address_type",
    "address_source",
    "registry_source_record_uid",
    "country_code",
    "latitude",
    "longitude",
    "geocode_status",
    "geocode_provider",
    "geocode_precision",
    "match_method",
    "match_confidence",
    "candidate_count",
    "coordinate_method",
    "source_record_id",
    "source_record_url",
    "source_url",
    "source_object_key",
    "source_md5",
    "source_snapshot_at",
    "source_retrieved_at",
    "source_run_id",
    "matched_at",
)

CLICKHOUSE_RESULTS_EXPORT_COLUMNS = (
    "company_id",
    "address_key",
    "address_type",
    "address_source",
    "registry_source_record_uid",
    "street_address",
    "postal_code",
    "post_town",
    "country_code",
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
    "source_run_id",
    "matched_at",
)


def replace_sweden_company_address_osm_matches(
    *,
    connection: Any,
    source_run_id: str,
    matched_at: datetime,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Replace exact and flagged street-level OSM matches; retain every outcome."""
    _create_canonical_company_address_input(connection)
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(f"create schema if not exists {ENRICHMENT_SCHEMA}")
        stage_started_at = time.monotonic()
        _log(log, "Building unique Sweden OSM address match keys")
        address_matching.replace_osm_address_match_components(connection)
        address_matching.replace_postcode_address_match_candidates(
            connection,
            table_name="_sweden_osm_match_candidates",
        )
        address_matching.replace_city_address_match_candidates(
            connection,
            table_name="_sweden_osm_city_address_match_candidates",
        )
        address_matching.replace_country_address_match_candidates(
            connection,
            table_name="_sweden_osm_street_address_match_candidates",
        )
        address_matching.replace_postcode_street_location_candidates(
            connection,
            table_name="_sweden_osm_postcode_street_locations",
        )
        _log(
            log,
            "Built Sweden OSM address match keys: postcode_candidates=%d "
            "city_candidates=%d country_candidates=%d street_locations=%d "
            "elapsed_seconds=%.1f",
            _count(connection, "_sweden_osm_match_candidates"),
            _count(connection, "_sweden_osm_city_address_match_candidates"),
            _count(connection, "_sweden_osm_street_address_match_candidates"),
            _count(connection, "_sweden_osm_postcode_street_locations"),
            time.monotonic() - stage_started_at,
        )
        connection.execute(
            f"""
            create or replace temporary table _sweden_osm_snapshot_provenance as
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
            create or replace temporary table _sweden_osm_city_centroids as
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

        stage_started_at = time.monotonic()
        _log(log, "Normalizing Sweden company address match keys")
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
            create or replace temporary table _sweden_company_address_keys as
            with normalized as materialized (
                select
                    *,
                    upper(trim(country_code)) as normalized_country_code,
                    {street_house_key_sql} as normalized_street_house,
                    normalized_postal_code as normalized_postcode,
                    concat_ws(
                        '|',
                        normalized_post_town,
                        {street_house_key_sql}
                    )
                        as normalized_city_address_match_key,
                    {street_house_key_sql}
                        as normalized_street_address_match_key,
                    {street_location_key_sql} as normalized_street_location_key
                from _sweden_company_addresses
            )
            select
                *,
                concat_ws('|', normalized_postcode, normalized_street_house)
                    as normalized_match_key,
                case
                    when address_kind = 'foreign'
                        then 'foreign_address'
                    when address_kind = 'postal_box' then 'postal_box'
                    when address_kind = 'incomplete'
                        then 'invalid_address'
                    else 'eligible'
                end as eligibility
            from normalized
            """
        )
        _log(
            log,
            "Normalized Sweden company address match keys: rows=%d elapsed_seconds=%.1f",
            _count(connection, "_sweden_company_address_keys"),
            time.monotonic() - stage_started_at,
        )

        stage_started_at = time.monotonic()
        _log(log, "Joining Sweden company addresses to unique OSM address keys")
        connection.execute(
            f"""
            create or replace table {QUALIFIED_MATCH_RESULTS_TABLE} as
            with joined as (
                select
                    company.*,
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
                        when postcode_osm.candidate_count is not null
                            then 'postal_code'
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
                from _sweden_company_address_keys company
                left join _sweden_osm_match_candidates postcode_osm
                    on postcode_osm.normalized_match_key = {ELIGIBLE_OSM_MATCH_KEY_SQL}
                left join _sweden_osm_city_address_match_candidates city_osm
                    on city_osm.normalized_match_key = case
                        when company.eligibility = 'eligible'
                            then company.normalized_city_address_match_key
                        else null
                    end
                left join _sweden_osm_street_address_match_candidates street_osm
                    on street_osm.normalized_match_key = case
                        when company.eligibility = 'eligible'
                            then company.normalized_street_address_match_key
                        else null
                    end
                left join _sweden_osm_city_centroids city
                    on city.normalized_city = case
                        when company.eligibility = 'postal_box'
                            then company.normalized_post_town
                        else null
                    end
                left join _sweden_osm_postcode_street_locations street_location
                    on street_location.normalized_street_location_key = case
                        when company.eligibility = 'eligible'
                            then company.normalized_street_location_key
                        else null
                    end
                cross join _sweden_osm_snapshot_provenance snapshot
            )
            select
                company_id,
                address_key,
                address_type,
                address_source,
                registry_source_record_uid,
                street_address,
                postal_code,
                post_town,
                case
                    when normalized_country_code = '' then 'SE'
                    else normalized_country_code
                end as country_code,
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
                    when candidate_count = 1
                     and match_basis = 'country_street_house'
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
                ?::varchar as source_run_id,
                ?::timestamptz as matched_at
            from joined
            """,
            [source_run_id, matched_at],
        )
        _log(
            log,
            "Joined Sweden company addresses to OSM: rows=%d elapsed_seconds=%.1f",
            _count(connection, QUALIFIED_MATCH_RESULTS_TABLE),
            time.monotonic() - stage_started_at,
        )
        connection.execute(
            f"""
            create or replace table {QUALIFIED_GEOCODES_TABLE} as
            select
                company_id,
                address_key,
                address_type,
                address_source,
                registry_source_record_uid,
                country_code,
                latitude,
                longitude,
                match_status as geocode_status,
                geocode_provider,
                geocode_precision,
                match_method,
                match_confidence,
                candidate_count,
                coordinate_method,
                source_record_id,
                source_record_url,
                source_url,
                source_object_key,
                source_md5,
                source_snapshot_at,
                source_retrieved_at,
                source_run_id,
                matched_at
            from {QUALIFIED_MATCH_RESULTS_TABLE}
            where match_status = 'matched_exact'
            """
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise

    status_counts = {
        str(status): int(count)
        for status, count in connection.execute(
            f"""
            select match_status, count(*)
            from {QUALIFIED_MATCH_RESULTS_TABLE}
            group by match_status
            """
        ).fetchall()
    }
    counts = {
        "addresses": _count(connection, QUALIFIED_MATCH_RESULTS_TABLE),
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
    _log(log, "Completed Sweden company OSM address matching: counts=%s", counts)
    return counts


def _create_canonical_company_address_input(connection: Any) -> None:
    canonical_table = address_canonicalization.QUALIFIED_CANONICAL_ADDRESSES_TABLE
    if _count(connection, canonical_table) == 0:
        raise ValueError(
            "Sweden canonical company addresses must be materialized before OSM matching"
        )
    connection.execute(
        f"""
        create or replace temporary view _sweden_company_addresses as
        select
            company_id,
            canonical_address_key as address_key,
            representative_address_type as address_type,
            representative_address_source as address_source,
            street_address,
            street_name,
            house_number,
            unit,
            postal_code,
            post_town,
            country_code,
            address_kind,
            normalized_street,
            normalized_postal_code,
            normalized_post_town,
            representative_source_record_uid as registry_source_record_uid
        from {canonical_table}
        """
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
