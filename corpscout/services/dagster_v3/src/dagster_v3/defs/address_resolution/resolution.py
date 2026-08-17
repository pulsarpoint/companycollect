from typing import Any

from dagster_v3.defs.address_resolution.model import AddressResolutionPolicy


def replace_address_resolution_candidates(
    connection: Any,
    *,
    query_table: str,
    query_street_variant_table: str,
    reference_table: str,
    candidate_table: str,
    policy: AddressResolutionPolicy,
) -> None:
    """Generate auditable candidates through explicit retrieval strategies."""
    query_variant_documents_table = "_address_resolution_query_street_variant_documents"
    connection.execute(
        f"""
        create or replace temporary table {query_variant_documents_table} as
        select
            query.* exclude (normalized_street, street_deletion_signatures),
            variant.street_variant,
            variant.normalized_street_variant as normalized_street,
            variant.variant_kind as street_variant_kind,
            variant.variant_rank as street_variant_rank,
            variant.street_deletion_signatures
        from {query_table} query
        inner join {query_street_variant_table} variant
            using (document_id, index_scope, country_code)
        """
    )
    query_postings_table = "_address_resolution_query_street_postings"
    reference_postings_table = "_address_resolution_reference_street_postings"
    _replace_fuzzy_street_postings(
        connection,
        source_table=query_variant_documents_table,
        postings_table=query_postings_table,
        policy=policy,
        reference_documents=False,
    )
    _replace_fuzzy_street_postings(
        connection,
        source_table=reference_table,
        postings_table=reference_postings_table,
        policy=policy,
        reference_documents=True,
    )
    connection.execute(
        f"""
        create or replace table {candidate_table} as
        with fuzzy_pairs as (
            select distinct
                query.document_id as query_document_id,
                reference.document_id as reference_document_id,
                query.normalized_street as query_street_variant,
                query.street_variant_kind,
                reference.normalized_street as reference_street
            from {query_postings_table} query
            inner join {reference_postings_table} reference
                on query.index_scope = reference.index_scope
               and query.country_code = reference.country_code
               and query.normalized_house_number
                    = reference.normalized_house_number
               and query.street_signature = reference.street_signature
               and query.normalized_street != reference.normalized_street
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
        ), expanded_building_pairs as (
            select
                query.document_id as query_document_id,
                reference.document_id as reference_document_id,
                'postcode'::varchar as context_basis
            from {query_variant_documents_table} query
            inner join {reference_table} reference
                on query.index_scope = reference.index_scope
               and query.country_code = reference.country_code
               and query.normalized_street = reference.normalized_street
               and query.normalized_house_number
                    = reference.normalized_house_number
               and query.normalized_postal_code
                    = reference.normalized_postal_code
            where query.street_variant_kind != 'parsed'
              and query.address_kind = 'physical'
              and reference.address_kind = 'physical'
              and reference.reference_precision = 'building'
              and query.normalized_street != ''
              and query.normalized_house_number != ''
              and query.normalized_postal_code != ''

            union all

            select
                query.document_id,
                reference.document_id,
                'locality'::varchar as context_basis
            from {query_variant_documents_table} query
            inner join {reference_table} reference
                on query.index_scope = reference.index_scope
               and query.country_code = reference.country_code
               and query.normalized_street = reference.normalized_street
               and query.normalized_house_number
                    = reference.normalized_house_number
               and query.normalized_locality = reference.normalized_locality
            where query.street_variant_kind != 'parsed'
              and query.address_kind = 'physical'
              and reference.address_kind = 'physical'
              and reference.reference_precision = 'building'
              and query.normalized_street != ''
              and query.normalized_house_number != ''
              and query.normalized_locality != ''
              and (
                    query.normalized_postal_code = ''
                 or reference.normalized_postal_code = ''
              )
        ), street_pairs as (
            select
                query.document_id as query_document_id,
                reference.document_id as reference_document_id,
                'postcode'::varchar as context_basis
            from {query_table} query
            inner join {reference_table} reference
                on query.index_scope = reference.index_scope
               and query.country_code = reference.country_code
               and query.normalized_street = reference.normalized_street
               and query.normalized_postal_code
                    = reference.normalized_postal_code
            where query.address_kind = 'physical'
              and reference.address_kind = 'physical'
              and reference.reference_precision = 'street'
              and query.normalized_street != ''
              and query.normalized_postal_code != ''

            union all

            select
                query.document_id,
                reference.document_id,
                'locality'::varchar as context_basis
            from {query_table} query
            inner join {reference_table} reference
                on query.index_scope = reference.index_scope
               and query.country_code = reference.country_code
               and query.normalized_street = reference.normalized_street
               and query.normalized_locality = reference.normalized_locality
            where query.address_kind = 'physical'
              and reference.address_kind = 'physical'
              and reference.reference_precision = 'street'
              and query.normalized_street != ''
              and query.normalized_locality != ''
              and (
                    query.normalized_postal_code = ''
                 or reference.normalized_postal_code = ''
              )

        ), base_candidate_pairs as materialized (
            select
                query.document_id as query_document_id,
                reference.document_id as reference_document_id,
                'raw_full_exact'::varchar as strategy,
                {policy.exact_score}::double as score,
                damerau_levenshtein(
                    query.normalized_street,
                    reference.normalized_street
                )::usmallint as street_edit_distance,
                []::varchar[] as corrections
            from {query_table} query
            inner join {reference_table} reference
                on query.index_scope = reference.index_scope
               and query.country_code = reference.country_code
               and query.normalized_raw_address
                    = reference.normalized_raw_address
            where query.address_kind = 'physical'
              and reference.address_kind = 'physical'
              and query.normalized_raw_address != ''

            union all

            select
                query.document_id,
                reference.document_id,
                'raw_full_exact'::varchar as strategy,
                {policy.exact_score}::double as score,
                damerau_levenshtein(
                    query.normalized_street,
                    reference.normalized_street
                )::usmallint as street_edit_distance,
                []::varchar[] as corrections
            from {query_table} query
            inner join {reference_table} reference
                on query.index_scope = reference.index_scope
               and query.country_code = reference.country_code
               and query.normalized_search_text
                    = reference.normalized_search_text
            where query.address_kind = 'physical'
              and reference.address_kind = 'physical'
              and query.normalized_search_text != ''

            union all

            select
                query.document_id,
                reference.document_id,
                case
                    when query.normalized_postal_code != ''
                     and query.normalized_postal_code
                        = reference.normalized_postal_code
                        then 'parsed_full_exact'
                    when query.normalized_postal_code != ''
                     and reference.normalized_postal_code != ''
                     and query.normalized_postal_code
                        != reference.normalized_postal_code
                        then 'postcode_mismatch_unique'
                    else 'parsed_locality_exact'
                end as strategy,
                case
                    when query.normalized_postal_code != ''
                     and query.normalized_postal_code
                        = reference.normalized_postal_code
                        then {policy.exact_score}
                    when query.normalized_postal_code != ''
                     and reference.normalized_postal_code != ''
                     and query.normalized_postal_code
                        != reference.normalized_postal_code
                        then {policy.postcode_mismatch_score}
                    else {policy.locality_fallback_score}
                end::double as score,
                0::usmallint as street_edit_distance,
                case
                    when query.normalized_postal_code != ''
                     and reference.normalized_postal_code != ''
                     and query.normalized_postal_code
                        != reference.normalized_postal_code
                        then ['postal_code']::varchar[]
                    else []::varchar[]
                end as corrections
            from {query_table} query
            inner join {reference_table} reference
                on query.index_scope = reference.index_scope
               and query.country_code = reference.country_code
               and query.normalized_street = reference.normalized_street
               and query.normalized_house_number
                    = reference.normalized_house_number
               and (
                    query.normalized_postal_code != ''
                and query.normalized_postal_code
                    = reference.normalized_postal_code
                    or query.normalized_locality != ''
                and query.normalized_locality
                    = reference.normalized_locality
               )
            where query.address_kind = 'physical'
              and reference.address_kind = 'physical'
              and reference.reference_precision = 'building'
              and query.normalized_street != ''
              and query.normalized_house_number != ''

            union all

            select
                query.document_id,
                reference.document_id,
                'country_street_house'::varchar as strategy,
                {policy.country_fallback_score}::double as score,
                0::usmallint as street_edit_distance,
                case
                    when query.normalized_postal_code
                            != reference.normalized_postal_code
                     and query.normalized_locality
                            != reference.normalized_locality
                        then ['postal_code', 'locality']::varchar[]
                    when query.normalized_postal_code
                            != reference.normalized_postal_code
                        then ['postal_code']::varchar[]
                    when query.normalized_locality
                            != reference.normalized_locality
                        then ['locality']::varchar[]
                    else ['address_context']::varchar[]
                end as corrections
            from {query_table} query
            inner join {reference_table} reference
                on query.index_scope = reference.index_scope
               and query.country_code = reference.country_code
               and query.normalized_street = reference.normalized_street
               and query.normalized_house_number
                    = reference.normalized_house_number
            where query.address_kind = 'physical'
              and reference.address_kind = 'physical'
              and reference.reference_precision = 'building'
              and query.normalized_street != ''
              and query.normalized_house_number != ''

            union all

            select
                query.document_id,
                reference.document_id,
                case
                    when pair.context_basis = 'postcode'
                        then 'expanded_street_postcode_house'
                    else 'expanded_street_locality_house'
                end as strategy,
                case
                    when pair.context_basis = 'postcode'
                        then {policy.fuzzy_postcode_score}
                    else {policy.fuzzy_locality_score}
                end::double as score,
                0::usmallint as street_edit_distance,
                ['street_abbreviation_expanded']::varchar[] as corrections
            from expanded_building_pairs pair
            inner join {query_table} query
                on query.document_id = pair.query_document_id
            inner join {reference_table} reference
                on reference.document_id = pair.reference_document_id

            union all

            select
                query.document_id,
                reference.document_id,
                case
                    when pair.street_variant_kind != 'parsed'
                     and query.normalized_postal_code != ''
                     and query.normalized_postal_code
                        = reference.normalized_postal_code
                        then 'expanded_street_fuzzy_postcode_house'
                    when pair.street_variant_kind != 'parsed'
                        then 'expanded_street_fuzzy_locality_house'
                    when query.normalized_postal_code != ''
                     and query.normalized_postal_code
                        = reference.normalized_postal_code
                        then 'fuzzy_street_postcode_house'
                    else 'fuzzy_street_locality_house'
                end as strategy,
                case
                    when query.normalized_postal_code != ''
                     and query.normalized_postal_code
                        = reference.normalized_postal_code
                        then {policy.fuzzy_postcode_score}
                    else {policy.fuzzy_locality_score}
                end::double as score,
                damerau_levenshtein(
                    pair.query_street_variant,
                    pair.reference_street
                )::usmallint as street_edit_distance,
                case
                    when pair.street_variant_kind != 'parsed'
                        then ['street_abbreviation_expanded']::varchar[]
                    else ['street_name']::varchar[]
                end as corrections
            from fuzzy_pairs pair
            inner join {query_table} query
                on query.document_id = pair.query_document_id
            inner join {reference_table} reference
                on reference.document_id = pair.reference_document_id
            where query.address_kind = 'physical'
              and reference.address_kind = 'physical'
              and reference.reference_precision = 'building'
              and pair.query_street_variant != pair.reference_street
              and query.normalized_house_number != ''
              and length(pair.query_street_variant)
                    >= {policy.minimum_fuzzy_street_length}
              and length(pair.reference_street)
                    >= {policy.minimum_fuzzy_street_length}
              and damerau_levenshtein(
                    pair.query_street_variant,
                    pair.reference_street
                  ) between 1 and {policy.maximum_street_edit_distance}

            union all

            select
                query.document_id,
                reference.document_id,
                case
                    when query.normalized_house_number = ''
                     and pair.context_basis = 'postcode'
                        then 'street_without_house'
                    when query.normalized_house_number = ''
                        then 'street_without_house_locality'
                    when pair.context_basis = 'postcode'
                        then 'street_requested_house_missing'
                    else 'street_requested_house_missing_locality'
                end as strategy,
                case
                    when query.normalized_house_number = ''
                     and pair.context_basis = 'postcode'
                        then {policy.street_without_house_score}
                    when query.normalized_house_number = ''
                        then {policy.street_without_house_locality_score}
                    when pair.context_basis = 'postcode'
                        then {policy.street_missing_requested_house_score}
                    else {policy.street_missing_requested_house_locality_score}
                end::double as score,
                0::usmallint as street_edit_distance,
                case
                    when query.normalized_house_number = ''
                        then ['house_number_missing']::varchar[]
                    else ['house_number_unavailable']::varchar[]
                end as corrections
            from street_pairs pair
            inner join {query_table} query
                on query.document_id = pair.query_document_id
            inner join {reference_table} reference
                on reference.document_id = pair.reference_document_id
            where query.address_kind = 'physical'
              and reference.address_kind = 'physical'
              and reference.reference_precision = 'street'
              and query.normalized_street != ''
        ), base_candidate_queries as materialized (
            select distinct query_document_id
            from base_candidate_pairs
        ), postcode_conflict_queries as materialized (
            select query.*
            from {query_table} query
            left join base_candidate_queries existing
                on existing.query_document_id = query.document_id
            where existing.query_document_id is null
              and query.address_kind = 'physical'
              and query.normalized_street != ''
              and query.normalized_locality != ''
              and query.normalized_postal_code != ''
        ), postcode_conflict_street_pairs as (
            select
                query.document_id as query_document_id,
                reference.document_id as reference_document_id
            from postcode_conflict_queries query
            inner join {reference_table} reference
                on query.index_scope = reference.index_scope
               and query.country_code = reference.country_code
               and query.normalized_street = reference.normalized_street
               and query.normalized_locality = reference.normalized_locality
            where reference.address_kind = 'physical'
              and reference.reference_precision = 'street'
              and reference.normalized_postal_code != ''
              and query.normalized_postal_code
                    != reference.normalized_postal_code
        ), postcode_conflict_candidate_pairs as (
            select
                query.document_id as query_document_id,
                reference.document_id as reference_document_id,
                case
                    when query.normalized_house_number = ''
                        then 'street_without_house_postcode_conflict'
                    else 'street_requested_house_missing_postcode_conflict'
                end::varchar as strategy,
                case
                    when query.normalized_house_number = ''
                        then {policy.street_without_house_postcode_conflict_score}
                    else {
            policy.street_missing_requested_house_postcode_conflict_score
        }
                end::double as score,
                0::usmallint as street_edit_distance,
                case
                    when query.normalized_house_number = ''
                        then ['house_number_missing', 'postal_code']::varchar[]
                    else ['house_number_unavailable', 'postal_code']::varchar[]
                end as corrections
            from postcode_conflict_street_pairs pair
            inner join {query_table} query
                on query.document_id = pair.query_document_id
            inner join {reference_table} reference
                on reference.document_id = pair.reference_document_id
        ), candidate_pairs as (
            select * from base_candidate_pairs
            union all
            select * from postcode_conflict_candidate_pairs
        ), deduplicated_pairs as (
            select *
            from candidate_pairs
            qualify row_number() over (
                partition by query_document_id, reference_document_id
                order by score desc, strategy
            ) = 1
        )
        select
            candidate.query_document_id,
            candidate.reference_document_id,
            case
                when candidate.strategy in (
                    'street_without_house_postcode_conflict',
                    'street_requested_house_missing_postcode_conflict'
                ) then concat_ws(
                    '|',
                    'street_locality_postcode_conflict',
                    reference.country_code,
                    reference.normalized_street,
                    reference.normalized_locality
                )
                else reference.search_document_key
            end as reference_address_key,
            candidate.strategy,
            candidate.score,
            candidate.street_edit_distance,
            query.normalized_raw_address
                = reference.normalized_raw_address
                and query.normalized_raw_address != '' as raw_address_exact,
            query.normalized_search_text
                = reference.normalized_search_text
                and query.normalized_search_text != '' as search_text_exact,
            query.normalized_street = reference.normalized_street
                and query.normalized_street != '' as street_exact,
            case
                when query.normalized_house_number = '' then 'query_missing'
                when reference.normalized_house_number = ''
                    then 'reference_missing'
                when query.normalized_house_number
                    = reference.normalized_house_number then 'exact'
                else 'mismatch'
            end as house_number_agreement,
            case
                when query.normalized_postal_code = '' then 'query_missing'
                when reference.normalized_postal_code = ''
                    then 'reference_missing'
                when query.normalized_postal_code
                    = reference.normalized_postal_code then 'exact'
                else 'mismatch'
            end as postal_code_agreement,
            case
                when query.normalized_locality = '' then 'query_missing'
                when reference.normalized_locality = ''
                    then 'reference_missing'
                when query.normalized_locality
                    = reference.normalized_locality then 'exact'
                else 'mismatch'
            end as locality_agreement,
            candidate.corrections,
            reference.reference_precision,
            reference.latitude,
            reference.longitude,
            reference.coordinate_spread_meters,
            reference.supporting_record_count,
            reference.street_name as matched_street_name,
            reference.house_number as matched_house_number,
            reference.postal_code as matched_postal_code,
            reference.locality as matched_locality,
            reference.source_record_id,
            reference.source_record_url
        from deduplicated_pairs candidate
        inner join {query_table} query
            on query.document_id = candidate.query_document_id
        inner join {reference_table} reference
            on reference.document_id = candidate.reference_document_id
        """
    )


def _replace_fuzzy_street_postings(
    connection: Any,
    *,
    source_table: str,
    postings_table: str,
    policy: AddressResolutionPolicy,
    reference_documents: bool,
) -> None:
    reference_filter = (
        "and reference_precision = 'building'" if reference_documents else ""
    )
    street_variant_kind = (
        "'parsed'::varchar" if reference_documents else "street_variant_kind"
    )
    connection.execute(
        f"""
        create or replace temporary table {postings_table} as
        select distinct
            document_id,
            index_scope,
            country_code,
            normalized_street,
            normalized_house_number,
            normalized_postal_code,
            normalized_locality,
            {street_variant_kind} as street_variant_kind,
            signature.value as street_signature
        from {source_table}
        cross join unnest(
            list_concat([normalized_street], street_deletion_signatures)
        ) signature(value)
        where address_kind = 'physical'
          and normalized_house_number != ''
          and length(normalized_street)
                >= {policy.minimum_fuzzy_street_length}
          and signature.value != ''
          {reference_filter}
        """
    )


def replace_address_resolution_results(
    connection: Any,
    *,
    query_table: str,
    candidate_table: str,
    result_table: str,
    policy: AddressResolutionPolicy,
) -> None:
    """Rank candidate evidence and enforce precision and ambiguity rules."""
    connection.execute(
        f"""
        create or replace table {result_table} as
        with target_candidates as (
            select
                query_document_id,
                reference_address_key,
                max(score)::double as score,
                first(strategy order by score desc, strategy)
                    as strategy,
                first(street_edit_distance order by score desc, strategy)
                    as street_edit_distance,
                bool_or(raw_address_exact) as raw_address_exact,
                bool_or(search_text_exact) as search_text_exact,
                bool_or(street_exact) as street_exact,
                first(house_number_agreement order by score desc, strategy)
                    as house_number_agreement,
                first(postal_code_agreement order by score desc, strategy)
                    as postal_code_agreement,
                first(locality_agreement order by score desc, strategy)
                    as locality_agreement,
                first(corrections order by score desc, strategy) as corrections,
                first(reference_precision order by score desc, strategy)
                    as reference_precision,
                median(latitude)::double as latitude,
                median(longitude)::double as longitude,
                greatest(
                    coalesce(max(coordinate_spread_meters), 0),
                    2 * 6371000 * asin(least(1.0, sqrt(
                        pow(sin(radians(max(latitude) - min(latitude)) / 2), 2)
                        + cos(radians(min(latitude)))
                          * cos(radians(max(latitude)))
                          * pow(
                              sin(
                                  radians(max(longitude) - min(longitude)) / 2
                              ),
                              2
                          )
                    )))
                )::double as coordinate_spread_meters,
                sum(supporting_record_count)::uinteger
                    as supporting_record_count,
                count(*)::uinteger as candidate_record_count,
                first(matched_street_name order by score desc, strategy)
                    as matched_street_name,
                first(matched_house_number order by score desc, strategy)
                    as matched_house_number,
                first(matched_postal_code order by score desc, strategy)
                    as matched_postal_code,
                first(matched_locality order by score desc, strategy)
                    as matched_locality,
                list_sort(list_distinct(list(source_record_id)))
                    as candidate_record_ids,
                list_sort(list_distinct(list(source_record_url)))
                    as candidate_record_urls
            from {candidate_table}
            group by query_document_id, reference_address_key
        ), scored_targets as (
            select
                *,
                max(score) over (partition by query_document_id) as top_score
            from target_candidates
        ), target_summary as (
            select
                query_document_id,
                top_score,
                count(*) filter (where score = top_score)::uinteger
                    as top_target_count,
                max(score) filter (where score < top_score)
                    as runner_up_score
            from scored_targets
            group by query_document_id, top_score
        ), selected_target as (
            select * exclude (top_score)
            from scored_targets
            qualify row_number() over (
                partition by query_document_id
                order by score desc, reference_address_key
            ) = 1
        ), joined as (
            select
                query.document_id,
                query.address_kind,
                selected.* exclude (query_document_id),
                summary.top_target_count,
                summary.runner_up_score,
                case
                    when summary.runner_up_score is null then null
                    else selected.score - summary.runner_up_score
                end as runner_up_score_margin,
                case
                    when summary.top_target_count > 1 then true
                    when summary.runner_up_score is not null
                     and selected.score - summary.runner_up_score
                        < {policy.minimum_decisive_score_margin}
                        then true
                    else false
                end as textually_ambiguous
            from {query_table} query
            left join selected_target selected
                on selected.query_document_id = query.document_id
            left join target_summary summary
                on summary.query_document_id = query.document_id
        )
        select
            document_id as query_document_id,
            case
                when address_kind = 'foreign' then 'foreign_address'
                when address_kind = 'postal_box' then 'postal_box'
                when address_kind in ('invalid', 'incomplete')
                    then 'invalid_address'
                when address_kind = 'property_identifier'
                    then 'property_identifier'
                when reference_address_key is null then 'unmatched'
                when textually_ambiguous then 'ambiguous'
                when reference_precision = 'street'
                 and coordinate_spread_meters
                    <= {policy.area_maximum_spread_meters}
                    then 'matched_street'
                when reference_precision = 'street' then 'ambiguous'
                when candidate_record_count = 1
                 and len(corrections) = 0 then 'matched_exact'
                when candidate_record_count = 1 then 'matched_corrected'
                when coordinate_spread_meters
                    <= {policy.site_maximum_spread_meters}
                    then 'matched_site'
                when coordinate_spread_meters
                    <= {policy.area_maximum_spread_meters}
                    then 'matched_area'
                else 'ambiguous'
            end as resolution_status,
            case
                when address_kind != 'physical' then ''
                when reference_address_key is null or textually_ambiguous then ''
                when reference_precision = 'street'
                 and coordinate_spread_meters
                    <= {policy.area_maximum_spread_meters}
                    then 'street'
                when reference_precision = 'street' then ''
                when candidate_record_count = 1 then 'building'
                when coordinate_spread_meters
                    <= {policy.site_maximum_spread_meters}
                    then 'site'
                when coordinate_spread_meters
                    <= {policy.area_maximum_spread_meters}
                    then 'area'
                else ''
            end as geocode_precision,
            coalesce(score, 0)::double as match_confidence,
            coalesce(strategy, '') as match_strategy,
            coalesce(street_edit_distance, 0)::usmallint
                as street_edit_distance,
            coalesce(raw_address_exact, false) as raw_address_exact,
            coalesce(search_text_exact, false) as search_text_exact,
            coalesce(street_exact, false) as street_exact,
            coalesce(house_number_agreement, '') as house_number_agreement,
            coalesce(postal_code_agreement, '') as postal_code_agreement,
            coalesce(locality_agreement, '') as locality_agreement,
            coalesce(corrections, []::varchar[]) as corrections,
            coalesce(top_target_count, 0)::uinteger as top_target_count,
            runner_up_score::double as runner_up_score,
            runner_up_score_margin::double as runner_up_score_margin,
            coalesce(candidate_record_count, 0)::uinteger
                as candidate_record_count,
            coalesce(supporting_record_count, 0)::uinteger
                as supporting_record_count,
            case
                when address_kind != 'physical'
                  or reference_address_key is null
                  or textually_ambiguous
                  or reference_precision = 'street'
                   and coordinate_spread_meters
                        > {policy.area_maximum_spread_meters}
                  or reference_precision != 'street'
                   and candidate_record_count > 1
                   and coordinate_spread_meters
                        > {policy.area_maximum_spread_meters}
                    then null
                else latitude
            end as latitude,
            case
                when address_kind != 'physical'
                  or reference_address_key is null
                  or textually_ambiguous
                  or reference_precision = 'street'
                   and coordinate_spread_meters
                        > {policy.area_maximum_spread_meters}
                  or reference_precision != 'street'
                   and candidate_record_count > 1
                   and coordinate_spread_meters
                        > {policy.area_maximum_spread_meters}
                    then null
                else longitude
            end as longitude,
            coordinate_spread_meters,
            coalesce(matched_street_name, '') as matched_street_name,
            coalesce(matched_house_number, '') as matched_house_number,
            coalesce(matched_postal_code, '') as matched_postal_code,
            coalesce(matched_locality, '') as matched_locality,
            coalesce(candidate_record_ids, []::varchar[])
                as candidate_record_ids,
            coalesce(candidate_record_urls, []::varchar[])
                as candidate_record_urls,
            '{policy.version}'::varchar as policy_version
        from joined
        """
    )
