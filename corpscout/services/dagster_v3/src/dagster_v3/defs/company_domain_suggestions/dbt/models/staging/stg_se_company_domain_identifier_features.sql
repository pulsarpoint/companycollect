{{
    config(
        order_by=['country_iso2', 'company_id', 'identifier_type', 'normalized_identifier']
    )
}}

WITH vat_features AS (
    SELECT
        country_iso2,
        company_id,
        company_name,
        'vat' AS identifier_type,
        normalized_value AS normalized_identifier,
        raw_value AS company_value
    FROM {{ ref('stg_se_company_match_features') }}
    WHERE feature_type = 'identifier'
      AND feature_subtype = 'vat'
),

lei_features AS (
    SELECT
        country_iso2,
        company_id,
        company_name,
        'lei' AS identifier_type,
        normalized_value AS normalized_identifier,
        raw_value AS company_value
    FROM {{ ref('stg_se_company_match_features') }}
    WHERE feature_type = 'identifier'
      AND feature_subtype = 'lei'
)

SELECT * FROM vat_features
UNION ALL
SELECT * FROM lei_features
