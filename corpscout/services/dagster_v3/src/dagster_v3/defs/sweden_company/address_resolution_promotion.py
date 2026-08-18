from collections.abc import Callable
from datetime import datetime
from typing import Any

from dagster_v3.defs.sweden_address_osm import tables as osm_tables
from dagster_v3.defs.sweden_company import (
    shared_address_geocoding,
    shared_addresses,
)
from dagster_v3.defs.sweden_company.address_resolution_shadow import (
    QUALIFIED_SHADOW_QUERY_DOCUMENTS_TABLE,
    QUALIFIED_SHADOW_RESULTS_TABLE,
)

PROMOTION_STAGE_TABLE = "_sweden_address_resolution_geocodes_next"
GEOCODED_STATUSES = (
    "matched_exact",
    "matched_corrected",
    "matched_site",
    "matched_area",
    "matched_street",
)
VALID_STATUSES = (
    *GEOCODED_STATUSES,
    "ambiguous",
    "unmatched",
    "invalid_address",
    "foreign_address",
    "postal_box",
    "property_identifier",
)
POSTCODE_CONFLICT_STREET_STRATEGIES = (
    "street_without_house_postcode_conflict",
    "street_requested_house_missing_postcode_conflict",
)


def replace_current_geocodes_from_address_resolution_shadow(
    *,
    connection: Any,
    geocode_run_id: str,
    matched_at: datetime,
    expected_policy_version: str,
    log: Callable[[str], object] | None = None,
) -> dict[str, object]:
    """Promote one complete, policy-compatible shadow run to the serving table."""
    _assert_shadow_is_promotable(
        connection,
        expected_policy_version=expected_policy_version,
    )
    _log(log, "Promoting Sweden policy resolver results to the live DuckDB table")
    connection.execute("begin transaction")
    try:
        _replace_promotion_stage(
            connection,
            geocode_run_id=geocode_run_id,
            matched_at=matched_at,
        )
        _assert_promoted_geocode_invariants(connection, PROMOTION_STAGE_TABLE)
        connection.execute(
            f"""
            create or replace table {
                shared_address_geocoding.QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE
            } as
            select * from {PROMOTION_STAGE_TABLE}
            """
        )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise

    status_counts = {
        str(status): int(count)
        for status, count in connection.execute(
            f"""
            select match_status, count(*)
            from {shared_address_geocoding.QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE}
            group by match_status
            order by match_status
            """
        ).fetchall()
    }
    [(rows, geolocated, evaluation_run_id)] = connection.execute(
        f"""
        select
            count(*),
            count(*) filter (where latitude is not null),
            first(evaluation_run_id)
        from {QUALIFIED_SHADOW_RESULTS_TABLE}
        """
    ).fetchall()
    return {
        "rows": int(rows),
        "geolocated": int(geolocated),
        "evaluation_run_id": str(evaluation_run_id),
        "policy_version": expected_policy_version,
        "status_counts": status_counts,
        "table": shared_address_geocoding.QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE,
    }


def _assert_shadow_is_promotable(
    connection: Any,
    *,
    expected_policy_version: str,
) -> None:
    [(result_rows, unique_results, policy_versions, evaluation_runs)] = (
        connection.execute(
            f"""
            select
                count(*),
                count(distinct query_document_id),
                count(distinct policy_version),
                count(distinct evaluation_run_id)
            from {QUALIFIED_SHADOW_RESULTS_TABLE}
            """
        ).fetchall()
    )
    [(address_rows, unique_addresses, address_identity_runs)] = connection.execute(
        f"""
        select
            count(*),
            count(distinct address_id),
            count(distinct address_identity_run_id)
        from {shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE}
        """
    ).fetchall()
    [(set_mismatches,)] = connection.execute(
        f"""
        select count(*)
        from (
            select address.address_id, shadow.query_document_id
            from (
                select cast(address_id as varchar) as address_id
                from {shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE}
            ) address
            full outer join (
                select query_document_id
                from {QUALIFIED_SHADOW_RESULTS_TABLE}
            ) shadow on shadow.query_document_id = address.address_id
            where address.address_id is null or shadow.query_document_id is null
        ) mismatches
        """
    ).fetchall()
    [(unexpected_policy_rows, invalid_statuses)] = connection.execute(
        f"""
        select
            count(*) filter (where policy_version != ?),
            count(*) filter (where resolution_status not in ({_quoted(VALID_STATUSES)}))
        from {QUALIFIED_SHADOW_RESULTS_TABLE}
        """,
        [expected_policy_version],
    ).fetchall()
    [(postcode_conflict_overrides,)] = connection.execute(
        f"""
        select count(*)
        from {QUALIFIED_SHADOW_RESULTS_TABLE} shadow
        inner join {
            shared_address_geocoding.QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE
        } current
            on cast(current.address_id as varchar) = shadow.query_document_id
        where shadow.match_strategy in (
                {_quoted(POSTCODE_CONFLICT_STREET_STRATEGIES)}
              )
          and current.match_status != 'unmatched'
          and current.match_method not in (
                {_quoted(POSTCODE_CONFLICT_STREET_STRATEGIES)}
              )
        """
    ).fetchall()
    if int(address_rows) == 0:
        raise ValueError("Cannot promote an empty Sweden address snapshot")
    if int(address_rows) != int(unique_addresses):
        raise ValueError("Current Sweden address IDs must be unique before promotion")
    if int(address_identity_runs) != 1:
        raise ValueError("Current Sweden addresses must belong to one identity run")
    if int(result_rows) != int(unique_results) or int(result_rows) != int(address_rows):
        raise ValueError("Shadow results must contain one row per current address")
    if int(set_mismatches) != 0:
        raise ValueError(
            "Shadow results and current Sweden addresses must have equal IDs"
        )
    if int(policy_versions) != 1 or int(unexpected_policy_rows) != 0:
        raise ValueError(
            "Shadow results do not use the expected address-resolution policy "
            f"{expected_policy_version}"
        )
    if int(evaluation_runs) != 1:
        raise ValueError("Shadow results must belong to one evaluation run")
    if int(invalid_statuses) != 0:
        raise ValueError("Shadow results contain unsupported resolution statuses")
    if int(postcode_conflict_overrides) != 0:
        raise ValueError(
            "Postcode-conflict street fallbacks may only replace unmatched results "
            "or refresh an existing postcode-conflict fallback"
        )


def _replace_promotion_stage(
    connection: Any,
    *,
    geocode_run_id: str,
    matched_at: datetime,
) -> None:
    connection.execute(
        f"""
        create or replace temporary table _sweden_address_resolution_osm_provenance as
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
        create or replace temporary table {PROMOTION_STAGE_TABLE} as
        select
            cast(address.address_id as varchar) as address_id,
            address.address_identity_run_id,
            concat_ws(
                '|',
                query.normalized_postal_code,
                concat(query.normalized_street, query.normalized_house_number)
            ) as normalized_match_key,
            result.resolution_status as match_status,
            least(
                65535,
                case
                    when result.resolution_status in ({_quoted(GEOCODED_STATUSES)})
                        then result.candidate_record_count
                    when result.resolution_status = 'ambiguous'
                        then greatest(
                            result.candidate_record_count,
                            result.top_target_count
                        )
                    else 0
                end
            )::usmallint as candidate_count,
            result.candidate_record_ids,
            result.candidate_record_urls,
            result.match_strategy as match_method,
            result.match_confidence::float as match_confidence,
            result.latitude,
            result.longitude,
            'openstreetmap'::varchar as geocode_provider,
            result.geocode_precision,
            case
                when result.geocode_precision = 'building'
                 and result.candidate_record_count = 1
                 and list_extract(result.candidate_record_ids, 1) like 'node/%'
                    then 'osm_node'
                when result.geocode_precision = 'building'
                 and result.candidate_record_count = 1
                 and list_extract(result.candidate_record_ids, 1) like 'way/%'
                    then 'osm_way'
                when result.geocode_precision = 'building'
                 and result.candidate_record_count = 1
                    then 'osm_record'
                when result.geocode_precision = 'site'
                    then 'osm_candidate_site_median'
                when result.geocode_precision = 'area'
                    then 'osm_candidate_area_median'
                when result.geocode_precision = 'street'
                    then 'osm_street_evidence_median'
                else null
            end::varchar as coordinate_method,
            nullif(result.matched_locality, '') as coordinate_locality,
            result.supporting_record_count::uinteger
                as coordinate_supporting_point_count,
            result.coordinate_spread_meters,
            case
                when result.geocode_precision = 'building'
                 and result.candidate_record_count = 1
                    then nullif(list_extract(result.candidate_record_ids, 1), '')
                else null
            end as source_record_id,
            case
                when result.geocode_precision = 'building'
                 and result.candidate_record_count = 1
                    then nullif(list_extract(result.candidate_record_urls, 1), '')
                else null
            end as source_record_url,
            provenance.source_url,
            provenance.source_object_key,
            provenance.source_md5,
            provenance.source_snapshot_at,
            provenance.source_retrieved_at,
            ?::varchar as geocode_run_id,
            ?::timestamptz as matched_at
        from {shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE} address
        inner join {QUALIFIED_SHADOW_QUERY_DOCUMENTS_TABLE} query
            on query.document_id = cast(address.address_id as varchar)
        inner join {QUALIFIED_SHADOW_RESULTS_TABLE} result
            on result.query_document_id = query.document_id
        cross join _sweden_address_resolution_osm_provenance provenance
        """,
        [geocode_run_id, matched_at],
    )


def _assert_promoted_geocode_invariants(connection: Any, table_name: str) -> None:
    [
        (
            result_rows,
            unique_results,
            identity_runs,
            geocode_runs,
            invalid_coordinates,
            missing_geocoded_coordinates,
            unexpected_coordinates,
            invalid_precision,
            missing_provenance,
        )
    ] = connection.execute(
        f"""
        select
            count(*),
            count(distinct address_id),
            count(distinct address_identity_run_id),
            count(distinct geocode_run_id),
            count(*) filter (
                where (latitude is null) != (longitude is null)
                   or latitude not between -90 and 90
                   or longitude not between -180 and 180
            ),
            count(*) filter (
                where match_status in ({_quoted(GEOCODED_STATUSES)})
                  and latitude is null
            ),
            count(*) filter (
                where match_status not in ({_quoted(GEOCODED_STATUSES)})
                  and latitude is not null
            ),
            count(*) filter (
                where match_status in ('matched_exact', 'matched_corrected')
                      and geocode_precision != 'building'
                   or match_status = 'matched_site' and geocode_precision != 'site'
                   or match_status = 'matched_area' and geocode_precision != 'area'
                   or match_status = 'matched_street' and geocode_precision != 'street'
                   or match_status not in ({_quoted(GEOCODED_STATUSES)})
                      and geocode_precision != ''
            ),
            count(*) filter (
                where source_url is null
                   or source_object_key is null
                   or source_md5 is null
                   or source_snapshot_at is null
                   or source_retrieved_at is null
            )
        from {table_name}
        """
    ).fetchall()
    [(address_rows,)] = connection.execute(
        f"select count(*) from {shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE}"
    ).fetchall()
    if int(result_rows) != int(unique_results) or int(result_rows) != int(address_rows):
        raise ValueError("Promoted geocodes must contain one row per current address")
    if int(identity_runs) != 1 or int(geocode_runs) != 1:
        raise ValueError("Promoted geocodes must contain one identity and geocode run")
    if int(invalid_coordinates) != 0:
        raise ValueError("Promoted geocodes contain invalid coordinates")
    if int(missing_geocoded_coordinates) != 0 or int(unexpected_coordinates) != 0:
        raise ValueError("Promoted coordinates disagree with resolution status")
    if int(invalid_precision) != 0:
        raise ValueError("Promoted precision disagrees with resolution status")
    if int(missing_provenance) != 0:
        raise ValueError("Promoted geocodes are missing OSM snapshot provenance")


def _quoted(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _log(log: Callable[[str], object] | None, message: str) -> None:
    if log is not None:
        log(message)
