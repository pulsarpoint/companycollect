{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['country_iso2', 'discovery_run_id', 'chunk_id'],
        engine='MergeTree()',
        partition_by=['country_iso2', 'toYYYYMM(matched_at)'],
        order_by=[
            'country_iso2',
            'discovery_run_id',
            'chunk_id',
            'company_id',
            'root_domain',
            'identifier_type'
        ],
        tags=['company_domain_suggestions_dbt_output']
    )
}}

SELECT
    matches.country_iso2,
    matches.chunk_id,
    matches.company_id,
    matches.company_name,
    matches.root_domain,
    matches.identifier_type,
    matches.normalized_identifier,
    matches.company_value,
    matches.domain_value,
    matches.source_url,
    matches.crawl_id,
    matches.identifiers_on_domain,
    matches.domains_for_identifier,
    candidates.candidate_domain_count,
    candidates.match_status,
    'se-domain-suggestions-dbt-v3' AS scoring_version,
    matches.discovery_run_id,
    toDateTime64('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] }}', 3, 'UTC')
        AS matched_at
FROM {{ ref('int_company_domain_identifier_match_classification') }} AS matches
INNER JOIN {{ ref('int_company_domain_identifier_candidates') }} AS candidates USING (
    country_iso2,
    discovery_run_id,
    chunk_id,
    company_id,
    root_domain
)
