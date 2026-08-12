SELECT
    country_iso2,
    discovery_run_id,
    chunk_id,
    company_id,
    root_domain
FROM {{ ref('company_domain_suggestions_dbt') }}
WHERE scoring_version = 'se-domain-suggestions-dbt-v5'
  AND (
      empty(candidate_sources)
      OR (
          NOT has(candidate_sources, 'vat')
          AND NOT has(candidate_sources, 'lei')
          AND NOT (
              has(candidate_sources, 'address')
              AND has(candidate_sources, 'industry')
          )
      )
      OR has(candidate_sources, 'address') != has(candidate_sources, 'industry')
      OR (has(candidate_sources, 'address') AND address_score <= 0)
      OR (has(candidate_sources, 'industry') AND industry_score <= 0)
  )
