from datetime import datetime
from typing import Any

from dagster_v3.defs.address_resolution.model import AddressResolutionPolicy


def replace_unmatched_address_resolution_diagnostics(
    connection: Any,
    *,
    query_table: str,
    query_street_variant_table: str,
    reference_table: str,
    result_table: str,
    diagnostic_table: str,
    policy: AddressResolutionPolicy,
    diagnosed_at: datetime,
) -> dict[str, object]:
    """Explain every unmatched query with indexed, reviewable evidence."""
    _replace_reference_lookups(connection, reference_table=reference_table)
    _replace_nearest_context_house_candidates(
        connection,
        query_table=query_table,
        query_street_variant_table=query_street_variant_table,
        reference_table=reference_table,
        result_table=result_table,
    )
    connection.execute(
        f"""
        create or replace table {diagnostic_table} as
        with unmatched as materialized (
            select
                query.*,
                result.policy_version,
                result.evaluation_run_id,
                result.evaluated_at
            from {query_table} query
            inner join {result_table} result
                on result.query_document_id = query.document_id
            where result.resolution_status = 'unmatched'
        )
        select
            query.document_id as query_document_id,
            query.raw_address,
            query.street_name,
            query.house_number,
            query.unit,
            query.postal_code,
            query.locality,
            query.normalized_street,
            query.normalized_house_number,
            query.normalized_unit,
            query.normalized_postal_code,
            query.normalized_locality,
            case
                when query.normalized_street = ''
                    then 'missing_street_component'
                when query.normalized_house_number = ''
                    then 'missing_house_number'
                when nearest.street_edit_distance
                        <= {policy.maximum_street_edit_distance}
                 and (
                        nearest.query_street_variant_length
                            < {policy.minimum_fuzzy_street_length}
                     or nearest.reference_street_length
                            < {policy.minimum_fuzzy_street_length}
                 )
                    then 'street_too_short_for_typo_policy'
                when nearest.street_edit_distance
                        <= {policy.maximum_street_edit_distance}
                    then 'candidate_retrieval_gap'
                when nearest.street_edit_distance
                        <= {policy.maximum_street_edit_distance + 2}
                    then 'street_typo_outside_policy'
                when postcode_street.normalized_street is not null
                    then 'house_number_not_indexed'
                when locality_street.normalized_street is not null
                    then 'postal_code_conflict'
                when country_street.normalized_street is not null
                    then 'street_not_found_in_context'
                when nearest.query_document_id is not null
                    then 'street_candidate_too_distant'
                else 'no_osm_street_candidate'
            end::varchar as reason_code,
            country_street.normalized_street is not null
                as exact_street_exists_in_country,
            postcode_street.normalized_street is not null
                as exact_street_exists_in_postcode,
            locality_street.normalized_street is not null
                as exact_street_exists_in_locality,
            nearest.reference_street_name as nearest_reference_street_name,
            nearest.reference_house_number as nearest_reference_house_number,
            nearest.reference_postal_code as nearest_reference_postal_code,
            nearest.reference_locality as nearest_reference_locality,
            nearest.street_variant_kind as nearest_street_variant_kind,
            nearest.normalized_street_variant as nearest_query_street_variant,
            nearest.reference_normalized_street,
            nearest.street_edit_distance,
            nearest.candidate_count as nearest_candidate_count,
            nearest.query_street_variant_length,
            nearest.reference_street_length,
            {policy.minimum_fuzzy_street_length}::usmallint
                as minimum_fuzzy_street_length,
            {policy.maximum_street_edit_distance}::usmallint
                as maximum_allowed_street_edit_distance,
            query.policy_version,
            query.evaluation_run_id,
            query.evaluated_at,
            ?::timestamptz as diagnosed_at
        from unmatched query
        left join _address_diagnostic_country_streets country_street
            on country_street.country_code = query.country_code
           and country_street.normalized_street = query.normalized_street
        left join _address_diagnostic_postcode_streets postcode_street
            on postcode_street.country_code = query.country_code
           and postcode_street.normalized_postal_code
                = query.normalized_postal_code
           and postcode_street.normalized_street = query.normalized_street
        left join _address_diagnostic_locality_streets locality_street
            on locality_street.country_code = query.country_code
           and locality_street.normalized_locality = query.normalized_locality
           and locality_street.normalized_street = query.normalized_street
        left join _address_diagnostic_nearest_context_house_candidate nearest
            on nearest.query_document_id = query.document_id
        """,
        [diagnosed_at],
    )
    _assert_diagnostic_invariants(
        connection,
        result_table=result_table,
        diagnostic_table=diagnostic_table,
    )
    reason_counts = {
        str(reason): int(count)
        for reason, count in connection.execute(
            f"""
            select reason_code, count(*)
            from {diagnostic_table}
            group by reason_code
            order by count(*) desc, reason_code
            """
        ).fetchall()
    }
    [(rows, with_nearest_candidate, evaluation_run_id)] = connection.execute(
        f"""
        select
            count(*),
            count(*) filter (where nearest_reference_street_name is not null),
            first(evaluation_run_id)
        from {diagnostic_table}
        """
    ).fetchall()
    return {
        "rows": int(rows),
        "with_nearest_candidate": int(with_nearest_candidate),
        "evaluation_run_id": str(evaluation_run_id),
        "policy_version": policy.version,
        "reason_counts": reason_counts,
        "table": diagnostic_table,
    }


def _replace_reference_lookups(connection: Any, *, reference_table: str) -> None:
    connection.execute(
        f"""
        create or replace temporary table _address_diagnostic_country_streets as
        select distinct country_code, normalized_street
        from {reference_table}
        where normalized_street != ''
        """
    )
    connection.execute(
        f"""
        create or replace temporary table _address_diagnostic_postcode_streets as
        select distinct country_code, normalized_postal_code, normalized_street
        from {reference_table}
        where normalized_postal_code != '' and normalized_street != ''
        """
    )
    connection.execute(
        f"""
        create or replace temporary table _address_diagnostic_locality_streets as
        select distinct country_code, normalized_locality, normalized_street
        from {reference_table}
        where normalized_locality != '' and normalized_street != ''
        """
    )


def _replace_nearest_context_house_candidates(
    connection: Any,
    *,
    query_table: str,
    query_street_variant_table: str,
    reference_table: str,
    result_table: str,
) -> None:
    connection.execute(
        f"""
        create or replace temporary table _address_diagnostic_building_keys as
        select
            country_code,
            normalized_postal_code,
            normalized_locality,
            normalized_street,
            normalized_house_number,
            first(street_name order by document_id) as street_name,
            first(house_number order by document_id) as house_number,
            first(postal_code order by document_id) as postal_code,
            first(locality order by document_id) as locality
        from {reference_table}
        where reference_precision = 'building'
          and normalized_street != ''
          and normalized_house_number != ''
        group by
            country_code,
            normalized_postal_code,
            normalized_locality,
            normalized_street,
            normalized_house_number
        """
    )
    connection.execute(
        f"""
        create or replace temporary table
            _address_diagnostic_nearest_context_house_candidate as
        with unmatched_variants as materialized (
            select
                query.document_id as query_document_id,
                query.country_code,
                query.normalized_house_number,
                query.normalized_postal_code,
                query.normalized_locality,
                variant.normalized_street_variant,
                variant.variant_kind as street_variant_kind,
                variant.variant_rank
            from {query_table} query
            inner join {result_table} result
                on result.query_document_id = query.document_id
            inner join {query_street_variant_table} variant
                on variant.document_id = query.document_id
               and variant.index_scope = query.index_scope
               and variant.country_code = query.country_code
            where result.resolution_status = 'unmatched'
              and query.normalized_street != ''
              and query.normalized_house_number != ''
        ), candidates as materialized (
            select
                query.query_document_id,
                reference.street_name as reference_street_name,
                reference.house_number as reference_house_number,
                reference.postal_code as reference_postal_code,
                reference.locality as reference_locality,
                query.street_variant_kind,
                query.variant_rank,
                query.normalized_street_variant,
                reference.normalized_street as reference_normalized_street,
                length(query.normalized_street_variant)::usmallint
                    as query_street_variant_length,
                damerau_levenshtein(
                    query.normalized_street_variant,
                    reference.normalized_street
                )::usmallint as street_edit_distance,
                length(reference.normalized_street)::usmallint
                    as reference_street_length
            from unmatched_variants query
            inner join _address_diagnostic_building_keys reference
                on reference.country_code = query.country_code
               and reference.normalized_house_number
                    = query.normalized_house_number
               and (
                    query.normalized_postal_code != ''
                and query.normalized_postal_code
                    = reference.normalized_postal_code
                    or (
                        query.normalized_postal_code = ''
                     or reference.normalized_postal_code = ''
                    )
                and query.normalized_locality != ''
                and query.normalized_locality
                    = reference.normalized_locality
               )
        ), ranked as (
            select
                *,
                count(*) over (partition by query_document_id)::uinteger
                    as candidate_count
            from candidates
        )
        select * exclude (variant_rank)
        from ranked
        qualify row_number() over (
            partition by query_document_id
            order by
                street_edit_distance,
                variant_rank,
                reference_street_name,
                reference_postal_code,
                reference_locality
        ) = 1
        """
    )


def _assert_diagnostic_invariants(
    connection: Any,
    *,
    result_table: str,
    diagnostic_table: str,
) -> None:
    [(unmatched_rows,)] = connection.execute(
        f"""
        select count(*)
        from {result_table}
        where resolution_status = 'unmatched'
        """
    ).fetchall()
    [(rows, unique_rows, missing_reasons, policy_versions, evaluation_runs)] = (
        connection.execute(
            f"""
            select
                count(*),
                count(distinct query_document_id),
                count(*) filter (where reason_code = '' or reason_code is null),
                count(distinct policy_version),
                count(distinct evaluation_run_id)
            from {diagnostic_table}
            """
        ).fetchall()
    )
    if int(rows) != int(unique_rows) or int(rows) != int(unmatched_rows):
        raise ValueError("Diagnostics must contain one row per unmatched query")
    if int(missing_reasons) != 0:
        raise ValueError("Every unmatched diagnostic must have a reason code")
    if int(policy_versions) != 1 or int(evaluation_runs) != 1:
        raise ValueError("Diagnostics must describe one resolver policy and run")
