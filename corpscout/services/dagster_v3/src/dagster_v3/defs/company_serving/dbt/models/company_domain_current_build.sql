{{ config(materialized='table', order_by=['country_code', 'company_id', 'is_primary', 'root_domain']) }}

WITH eligible AS (
    SELECT
        country_code,
        company_id,
        root_domain,
        website_url,
        website_host,
        source_names,
        suggested_confidence,
        suggested_primary,
        review_status,
        first_seen_at,
        last_seen_at,
        resolved_at
    FROM {{ ref('company_domains_build') }}
    WHERE is_active = 1
      AND review_status != 'rejected'
),

ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY country_code, company_id
            ORDER BY
                review_status = 'confirmed_primary' DESC,
                suggested_primary DESC,
                suggested_confidence DESC,
                root_domain
        ) AS domain_rank
    FROM eligible
)

SELECT
    country_code,
    company_id,
    root_domain,
    website_url,
    website_host,
    toUInt8(domain_rank = 1) AS is_primary,
    arrayStringConcat(source_names, '+') AS match_method,
    suggested_confidence AS confidence,
    first_seen_at,
    last_seen_at,
    resolved_at
FROM ranked
