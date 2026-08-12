{{
    config(
        order_by=[
            'country_iso2',
            'discovery_run_id',
            'chunk_id',
            'company_id',
            'root_domain'
        ]
    )
}}

WITH identifier_unique AS (
    SELECT *
    FROM {{ ref('int_company_domain_identifier_candidates') }}
    WHERE match_status = 'unique'
),

identifier_unique_companies AS (
    SELECT country_iso2, discovery_run_id, chunk_id, company_id
    FROM identifier_unique
    GROUP BY country_iso2, discovery_run_id, chunk_id, company_id
),

address_unique AS (
    SELECT *
    FROM {{ ref('int_company_domain_address_nace_candidates') }}
    WHERE match_status = 'unique'
),

address_candidates_without_identifier_conflicts AS (
    SELECT
        address_unique.country_iso2 AS country_iso2,
        address_unique.discovery_run_id AS discovery_run_id,
        address_unique.chunk_id AS chunk_id,
        address_unique.company_id AS company_id,
        address_unique.company_name AS company_name,
        address_unique.root_domain AS root_domain,
        address_unique.address_domain_count AS address_domain_count,
        address_unique.matched_address_count AS matched_address_count,
        address_unique.matched_nace_count AS matched_nace_count,
        address_unique.candidate_domain_count AS candidate_domain_count,
        address_unique.match_status AS match_status
    FROM address_unique
    LEFT JOIN identifier_unique_companies
        ON identifier_unique_companies.country_iso2
            = address_unique.country_iso2
       AND identifier_unique_companies.discovery_run_id
            = address_unique.discovery_run_id
       AND identifier_unique_companies.chunk_id = address_unique.chunk_id
       AND identifier_unique_companies.company_id = address_unique.company_id
    LEFT JOIN identifier_unique
        ON identifier_unique.country_iso2 = address_unique.country_iso2
       AND identifier_unique.discovery_run_id = address_unique.discovery_run_id
       AND identifier_unique.chunk_id = address_unique.chunk_id
       AND identifier_unique.company_id = address_unique.company_id
       AND identifier_unique.root_domain = address_unique.root_domain
    WHERE identifier_unique_companies.company_id = ''
       OR identifier_unique.root_domain = address_unique.root_domain
),

candidate_signals AS (
    SELECT
        country_iso2,
        discovery_run_id,
        chunk_id,
        company_id,
        company_name,
        root_domain,
        candidate_sources,
        identifier_score,
        toFloat32(0.0) AS address_score,
        toFloat32(0.0) AS industry_score
    FROM identifier_unique

    UNION ALL

    SELECT
        country_iso2,
        discovery_run_id,
        chunk_id,
        company_id,
        company_name,
        root_domain,
        ['address', 'industry'] AS candidate_sources,
        toFloat32(0.0) AS identifier_score,
        toFloat32(35.0) AS address_score,
        toFloat32(25.0) AS industry_score
    FROM address_candidates_without_identifier_conflicts
),

aggregated_candidates AS (
    SELECT
        country_iso2,
        discovery_run_id,
        chunk_id,
        company_id,
        any(company_name) AS company_name,
        root_domain,
        arraySort(arrayDistinct(arrayFlatten(groupArray(candidate_sources))))
            AS candidate_sources,
        max(identifier_score) AS identifier_score,
        max(address_score) AS address_score,
        max(industry_score) AS industry_score
    FROM candidate_signals
    GROUP BY country_iso2, discovery_run_id, chunk_id, company_id, root_domain
)

SELECT
    country_iso2,
    discovery_run_id,
    chunk_id,
    company_id,
    company_name,
    root_domain,
    candidate_sources,
    identifier_score,
    address_score,
    industry_score,
    least(toFloat32(100.0),
        identifier_score + address_score + industry_score
    ) AS total_score
FROM aggregated_candidates
