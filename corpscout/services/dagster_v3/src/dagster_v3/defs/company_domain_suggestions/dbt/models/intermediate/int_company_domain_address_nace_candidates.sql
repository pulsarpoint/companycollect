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

WITH candidate_pairs AS (
    SELECT
        country_iso2,
        discovery_run_id,
        chunk_id,
        company_id,
        any(company_name) AS company_name,
        root_domain,
        toUInt32(min(address_domain_count)) AS address_domain_count,
        max(domain_eligible) AS domain_eligible,
        toUInt16(countDistinct(normalized_address)) AS matched_address_count,
        toUInt16(countDistinct(company_nace)) AS matched_nace_count
    FROM {{ ref('int_company_domain_address_nace_matches') }}
    GROUP BY country_iso2, discovery_run_id, chunk_id, company_id, root_domain
),

company_fanout AS (
    SELECT
        country_iso2,
        discovery_run_id,
        chunk_id,
        company_id,
        toUInt32(countIf(domain_eligible = 1)) AS candidate_domain_count
    FROM candidate_pairs
    GROUP BY country_iso2, discovery_run_id, chunk_id, company_id
)

SELECT
    candidates.country_iso2,
    candidates.discovery_run_id,
    candidates.chunk_id,
    candidates.company_id,
    candidates.company_name,
    candidates.root_domain,
    candidates.address_domain_count,
    candidates.matched_address_count,
    candidates.matched_nace_count,
    fanout.candidate_domain_count,
    multiIf(
        candidates.domain_eligible = 0, 'directory',
        fanout.candidate_domain_count > 1, 'ambiguous',
        'unique'
    ) AS match_status
FROM candidate_pairs AS candidates
INNER JOIN company_fanout AS fanout USING (
    country_iso2,
    discovery_run_id,
    chunk_id,
    company_id
)
