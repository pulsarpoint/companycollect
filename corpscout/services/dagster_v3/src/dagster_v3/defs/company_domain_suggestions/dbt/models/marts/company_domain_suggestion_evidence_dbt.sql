{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['country_iso2', 'discovery_run_id', 'chunk_id'],
        engine='MergeTree()',
        partition_by=['country_iso2', 'toYYYYMM(suggested_at)'],
        order_by=[
            'country_iso2',
            'discovery_run_id',
            'chunk_id',
            'company_id',
            'root_domain',
            'signal_type',
            'source_field'
        ],
        tags=['company_domain_suggestions_dbt_output']
    )
}}

WITH resolved_candidates AS (
    SELECT *
    FROM {{ ref('int_company_domain_candidates') }}
),

identifier_evidence AS (
    SELECT
        matches.country_iso2 AS country_iso2,
        matches.chunk_id AS chunk_id,
        matches.company_id AS company_id,
        matches.root_domain AS root_domain,
        'identifier' AS signal_type,
        matches.identifier_type AS source_field,
        matches.company_value,
        matches.domain_value,
        toFloat32(70.0) AS score_contribution,
        matches.source_url,
        matches.crawl_id,
        matches.discovery_run_id AS discovery_run_id
    FROM {{ ref('int_company_domain_identifier_match_classification') }} AS matches
    INNER JOIN {{ ref('int_company_domain_identifier_candidates') }} AS identifier_candidates
        ON identifier_candidates.country_iso2 = matches.country_iso2
       AND identifier_candidates.discovery_run_id = matches.discovery_run_id
       AND identifier_candidates.chunk_id = matches.chunk_id
       AND identifier_candidates.company_id = matches.company_id
       AND identifier_candidates.root_domain = matches.root_domain
    INNER JOIN resolved_candidates
        ON resolved_candidates.country_iso2 = matches.country_iso2
       AND resolved_candidates.discovery_run_id = matches.discovery_run_id
       AND resolved_candidates.chunk_id = matches.chunk_id
       AND resolved_candidates.company_id = matches.company_id
       AND resolved_candidates.root_domain = matches.root_domain
    WHERE identifier_candidates.match_status = 'unique'
),

address_evidence_ranked AS (
    SELECT
        matches.country_iso2 AS country_iso2,
        matches.discovery_run_id AS discovery_run_id,
        matches.chunk_id AS chunk_id,
        matches.company_id AS company_id,
        matches.root_domain AS root_domain,
        matches.company_address_value AS company_address_value,
        matches.domain_address_value AS domain_address_value,
        matches.domain_address_source_url AS domain_address_source_url,
        matches.crawl_id AS crawl_id,
        matches.address_domain_count AS address_domain_count,
        matches.domain_address_observed_at AS domain_address_observed_at,
        row_number() OVER (
            PARTITION BY
                matches.country_iso2,
                matches.discovery_run_id,
                matches.chunk_id,
                matches.company_id,
                matches.root_domain
            ORDER BY
                matches.address_domain_count,
                matches.domain_address_observed_at DESC,
                matches.domain_address_source_url
        ) AS evidence_rank
    FROM {{ ref('int_company_domain_address_nace_matches') }} AS matches
    INNER JOIN resolved_candidates
        ON resolved_candidates.country_iso2 = matches.country_iso2
       AND resolved_candidates.discovery_run_id = matches.discovery_run_id
       AND resolved_candidates.chunk_id = matches.chunk_id
       AND resolved_candidates.company_id = matches.company_id
       AND resolved_candidates.root_domain = matches.root_domain
    WHERE matches.domain_eligible = 1
      AND has(resolved_candidates.candidate_sources, 'address')
),

address_evidence AS (
    SELECT
        country_iso2,
        chunk_id,
        company_id,
        root_domain,
        'address' AS signal_type,
        'normalized_postal_address' AS source_field,
        company_address_value AS company_value,
        domain_address_value AS domain_value,
        toFloat32(35.0) AS score_contribution,
        domain_address_source_url AS source_url,
        crawl_id,
        discovery_run_id
    FROM address_evidence_ranked
    WHERE evidence_rank = 1
),

industry_evidence_ranked AS (
    SELECT
        matches.country_iso2 AS country_iso2,
        matches.discovery_run_id AS discovery_run_id,
        matches.chunk_id AS chunk_id,
        matches.company_id AS company_id,
        matches.root_domain AS root_domain,
        matches.company_industry_value AS company_industry_value,
        matches.domain_industry_value AS domain_industry_value,
        matches.domain_industry_source_url AS domain_industry_source_url,
        matches.crawl_id AS crawl_id,
        matches.domain_industry_score AS domain_industry_score,
        matches.domain_industry_observed_at AS domain_industry_observed_at,
        row_number() OVER (
            PARTITION BY
                matches.country_iso2,
                matches.discovery_run_id,
                matches.chunk_id,
                matches.company_id,
                matches.root_domain
            ORDER BY
                matches.domain_industry_score DESC,
                matches.domain_industry_observed_at DESC,
                matches.domain_industry_source_url
        ) AS evidence_rank
    FROM {{ ref('int_company_domain_address_nace_matches') }} AS matches
    INNER JOIN resolved_candidates
        ON resolved_candidates.country_iso2 = matches.country_iso2
       AND resolved_candidates.discovery_run_id = matches.discovery_run_id
       AND resolved_candidates.chunk_id = matches.chunk_id
       AND resolved_candidates.company_id = matches.company_id
       AND resolved_candidates.root_domain = matches.root_domain
    WHERE matches.domain_eligible = 1
      AND has(resolved_candidates.candidate_sources, 'industry')
),

industry_evidence AS (
    SELECT
        country_iso2,
        chunk_id,
        company_id,
        root_domain,
        'industry' AS signal_type,
        'nace_exact' AS source_field,
        company_industry_value AS company_value,
        domain_industry_value AS domain_value,
        toFloat32(25.0) AS score_contribution,
        domain_industry_source_url AS source_url,
        crawl_id,
        discovery_run_id
    FROM industry_evidence_ranked
    WHERE evidence_rank = 1
),

all_evidence AS (
    SELECT * FROM identifier_evidence
    UNION ALL
    SELECT * FROM address_evidence
    UNION ALL
    SELECT * FROM industry_evidence
)

SELECT
    *,
    toDateTime64('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] }}', 3, 'UTC')
        AS suggested_at
FROM all_evidence
