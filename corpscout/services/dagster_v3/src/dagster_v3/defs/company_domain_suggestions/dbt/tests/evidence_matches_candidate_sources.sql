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
    evidence.country_iso2,
    evidence.discovery_run_id,
    evidence.chunk_id,
    evidence.company_id,
    evidence.root_domain,
    evidence.signal_type,
    evidence.source_field
FROM {{ ref('company_domain_suggestion_evidence_dbt') }} AS evidence
INNER JOIN {{ ref('company_domain_suggestions_dbt') }} AS suggestions
    ON suggestions.country_iso2 = evidence.country_iso2
   AND suggestions.discovery_run_id = evidence.discovery_run_id
   AND suggestions.chunk_id = evidence.chunk_id
   AND suggestions.company_id = evidence.company_id
   AND suggestions.root_domain = evidence.root_domain
WHERE suggestions.scoring_version = 'se-domain-suggestions-dbt-v5'
  AND suggestions.discovery_run_id IN (
      SELECT discovery_run_id
      FROM {{ ref('int_company_domain_candidates') }}
      GROUP BY discovery_run_id
  )
  AND (
      (evidence.signal_type = 'identifier'
          AND NOT has(suggestions.candidate_sources, evidence.source_field))
      OR (evidence.signal_type = 'address'
          AND NOT has(suggestions.candidate_sources, 'address'))
      OR (evidence.signal_type = 'industry'
          AND NOT has(suggestions.candidate_sources, 'industry'))
  )
