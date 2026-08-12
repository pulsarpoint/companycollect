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
        arraySort(groupUniqArray(identifier_type)) AS candidate_sources,
        toUInt16(countDistinct(identifier_type)) AS identifier_type_count,
        max(identifiers_on_domain) AS identifiers_on_domain,
        max(domains_for_identifier) AS domains_for_identifier,
        max(domain_eligible) AS domain_eligible
    FROM {{ ref('int_company_domain_identifier_match_classification') }}
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
    candidates.candidate_sources,
    toFloat32(if(candidates.identifier_type_count > 1, 100.0, 70.0)) AS identifier_score,
    candidates.identifiers_on_domain,
    candidates.domains_for_identifier,
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
