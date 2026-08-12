{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['country_iso2', 'discovery_run_id', 'chunk_id'],
        engine='MergeTree()',
        partition_by=['country_iso2', 'toYYYYMM(suggested_at)'],
        order_by=['country_iso2', 'discovery_run_id', 'chunk_id', 'company_id', 'rank', 'root_domain'],
        tags=['company_domain_suggestions_dbt_output']
    )
}}

SELECT
    country_iso2,
    chunk_id,
    company_id,
    root_domain,
    toUInt16(1) AS rank,
    company_name,
    candidate_sources,
    identifier_score,
    toFloat32(0.0) AS website_name_score,
    toFloat32(0.0) AS domain_name_score,
    toFloat32(0.0) AS people_score,
    address_score,
    industry_score,
    toFloat32(0.0) AS country_score,
    toFloat32(0.0) AS web_presence_score,
    toFloat32(0.0) AS conflict_penalty,
    total_score,
    'se-domain-suggestions-dbt-v5' AS scoring_version,
    discovery_run_id,
    toDateTime64('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] }}', 3, 'UTC')
        AS suggested_at
FROM {{ ref('int_company_domain_candidates') }}
