{{
    config(
        order_by=[
            'country_iso2',
            'discovery_run_id',
            'chunk_id',
            'company_id',
            'root_domain',
            'identifier_type'
        ]
    )
}}

WITH matched_domains AS (
    SELECT DISTINCT root_domain
    FROM {{ ref('int_company_domain_identifier_matches') }}
),

domain_fanout AS (
    SELECT
        features.root_domain,
        toUInt32(countDistinct(tuple(
            lowerUTF8(features.source_field),
            features.normalized_value
        )))
            AS identifiers_on_domain
    FROM {{ source('corpscout', 'web_domain_identity_features') }} AS features FINAL
    INNER JOIN matched_domains USING (root_domain)
    WHERE features.feature_type = 'identifier'
    GROUP BY features.root_domain
),

matches_with_domain_fanout AS (
    SELECT
        matches.*,
        fanout.identifiers_on_domain,
        toUInt8(
            fanout.identifiers_on_domain
                <= toUInt32({{ var('max_identifiers_per_domain') }})
        ) AS domain_eligible
    FROM {{ ref('int_company_domain_identifier_matches') }} AS matches
    INNER JOIN domain_fanout AS fanout USING (root_domain)
),

identifier_fanout AS (
    SELECT
        identifier_type,
        normalized_identifier,
        toUInt32(countDistinct(root_domain)) AS domains_for_identifier
    FROM matches_with_domain_fanout
    WHERE domain_eligible = 1
    GROUP BY identifier_type, normalized_identifier
)

SELECT
    matches.country_iso2,
    matches.discovery_run_id,
    matches.chunk_id,
    matches.company_id,
    matches.company_name,
    matches.root_domain,
    matches.identifier_type,
    matches.normalized_identifier,
    matches.company_value,
    matches.domain_value,
    matches.source_url,
    matches.crawl_id,
    matches.source_resolved_at,
    matches.identifiers_on_domain,
    toUInt32(ifNull(fanout.domains_for_identifier, 0)) AS domains_for_identifier,
    matches.domain_eligible
FROM matches_with_domain_fanout AS matches
LEFT JOIN identifier_fanout AS fanout USING (identifier_type, normalized_identifier)
