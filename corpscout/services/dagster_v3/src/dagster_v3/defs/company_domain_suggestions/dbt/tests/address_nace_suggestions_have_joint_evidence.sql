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

WITH evidence_signals AS (
    SELECT
        country_iso2,
        discovery_run_id,
        chunk_id,
        company_id,
        root_domain,
        groupUniqArray(signal_type) AS signal_types
    FROM {{ ref('company_domain_suggestion_evidence_dbt') }}
    GROUP BY country_iso2, discovery_run_id, chunk_id, company_id, root_domain
)

SELECT
    suggestions.country_iso2,
    suggestions.discovery_run_id,
    suggestions.chunk_id,
    suggestions.company_id,
    suggestions.root_domain
FROM {{ ref('company_domain_suggestions_dbt') }} AS suggestions
LEFT JOIN evidence_signals USING (
    country_iso2,
    discovery_run_id,
    chunk_id,
    company_id,
    root_domain
)
WHERE suggestions.scoring_version = 'se-domain-suggestions-dbt-v5'
  AND has(suggestions.candidate_sources, 'address')
  AND (
      NOT has(evidence_signals.signal_types, 'address')
      OR NOT has(evidence_signals.signal_types, 'industry')
  )
