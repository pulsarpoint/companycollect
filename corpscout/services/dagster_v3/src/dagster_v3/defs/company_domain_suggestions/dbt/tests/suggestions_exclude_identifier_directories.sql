{{
    config(
        meta={
            'dagster': {
                'ref': {
                    'name': 'company_domain_suggestions_dbt',
                    'package': 'company_domain_suggestions',
                },
            }
        }
    )
}}

WITH domain_fanout AS (
    SELECT
        features.root_domain,
        countDistinct(tuple(
            lowerUTF8(features.source_field),
            features.normalized_value
        )) AS identifiers_on_domain
    FROM {{ source('corpscout', 'web_domain_identity_features') }} AS features FINAL
    WHERE features.feature_type = 'identifier'
    GROUP BY features.root_domain
)

SELECT
    suggestions.country_iso2,
    suggestions.discovery_run_id,
    suggestions.chunk_id,
    suggestions.company_id,
    suggestions.root_domain,
    domain_fanout.identifiers_on_domain
FROM {{ ref('company_domain_suggestions_dbt') }} AS suggestions
INNER JOIN domain_fanout USING (root_domain)
WHERE suggestions.scoring_version = 'se-domain-suggestions-dbt-v5'
  AND domain_fanout.identifiers_on_domain
      > toUInt32({{ var('max_identifiers_per_domain') }})
