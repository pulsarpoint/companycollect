{{
    config(
        meta={
            'dagster': {
                'ref': {
                    'name': 'company_domain_suggestions_dbt',
                    'package': 'company_domain_suggestions',
                },
            },
        }
    )
}}

SELECT
    suggestions.country_iso2,
    suggestions.discovery_run_id,
    suggestions.chunk_id,
    suggestions.company_id,
    suggestions.root_domain
FROM {{ ref('company_domain_suggestions_dbt') }} AS suggestions
LEFT JOIN {{ ref('int_company_domain_candidates') }} AS candidates
    ON candidates.country_iso2 = suggestions.country_iso2
   AND candidates.discovery_run_id = suggestions.discovery_run_id
   AND candidates.chunk_id = suggestions.chunk_id
   AND candidates.company_id = suggestions.company_id
   AND candidates.root_domain = suggestions.root_domain
WHERE suggestions.scoring_version = 'se-domain-suggestions-dbt-v5'
  AND suggestions.discovery_run_id IN (
      SELECT discovery_run_id
      FROM {{ ref('int_company_domain_candidates') }}
      GROUP BY discovery_run_id
  )
  AND candidates.company_id = ''
