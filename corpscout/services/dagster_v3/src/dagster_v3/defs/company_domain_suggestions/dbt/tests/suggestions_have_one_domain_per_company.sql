SELECT
    country_iso2,
    discovery_run_id,
    chunk_id,
    company_id,
    count() AS suggestion_count
FROM {{ ref('company_domain_suggestions_dbt') }}
WHERE scoring_version = 'se-domain-suggestions-dbt-v5'
GROUP BY country_iso2, discovery_run_id, chunk_id, company_id
HAVING suggestion_count > 1
