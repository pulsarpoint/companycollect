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

WITH matched AS (
    SELECT
        features.country_iso2 AS country_iso2,
        '{{ var("discovery_run_id", "manual") }}' AS discovery_run_id,
        toUInt16({{ var('chunk_id', 0) }}) AS chunk_id,
        features.company_id AS company_id,
        features.company_name AS company_name,
        domains.root_domain AS root_domain,
        features.identifier_type AS identifier_type,
        features.normalized_identifier AS normalized_identifier,
        features.company_value AS company_value,
        domains.raw_value AS domain_value,
        domains.source_url AS source_url,
        domains.crawl_id AS crawl_id,
        domains.source_resolved_at AS source_resolved_at,
        row_number() OVER (
            PARTITION BY
                features.country_iso2,
                features.company_id,
                domains.root_domain,
                features.identifier_type,
                features.normalized_identifier
            ORDER BY domains.source_resolved_at DESC, domains.source_url
        ) AS evidence_rank
    FROM {{ ref('stg_se_company_domain_identifier_features') }} AS features
    INNER JOIN {{ source('corpscout', 'web_domain_identity_features') }} AS domains FINAL
        ON domains.feature_type = 'identifier'
       AND domains.normalized_value = features.normalized_identifier
       AND lowerUTF8(domains.source_field) = features.identifier_type
    WHERE features.country_iso2 = upper('{{ var("country_iso2", "SE") }}')
      AND cityHash64(features.company_id)
          % greatest(toUInt16({{ var('chunk_count', 1) }}), 1)
          = toUInt16({{ var('chunk_id', 0) }})
)

SELECT
    country_iso2,
    discovery_run_id,
    chunk_id,
    company_id,
    company_name,
    root_domain,
    identifier_type,
    normalized_identifier,
    company_value,
    domain_value,
    source_url,
    crawl_id,
    source_resolved_at
FROM matched
WHERE evidence_rank = 1
