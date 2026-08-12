{{ config(materialized='table', order_by=['country_code', 'company_id', 'identifier_scheme', 'is_primary', 'identifier_value']) }}

WITH companies AS (
    SELECT company_id
    FROM {{ source('corpscout', 'se_companies') }} FINAL
),
leis AS (
    SELECT
        identifiers.company_id,
        upperUTF8(identifiers.issuer_id) AS identifier_value,
        argMax(identifiers.match_method, identifiers.resolved_at) AS source_match_method,
        argMax(identifiers.match_confidence, identifiers.resolved_at) AS source_match_confidence,
        min(identifiers.first_seen_date) AS first_seen_date,
        max(identifiers.last_seen_date) AS last_seen_date
    FROM {{ source('corpscout', 'company_identifier') }} AS identifiers
    WHERE identifiers.country_code = '{{ var("country_code") }}'
      AND identifiers.issuer_scheme = 'lei'
      AND identifiers.is_current = 1
    GROUP BY identifiers.company_id, identifier_value
),
vats AS (
    SELECT
        identifiers.company_id,
        upperUTF8(identifiers.issuer_id) AS identifier_value,
        argMax(identifiers.match_method, identifiers.resolved_at) AS source_match_method,
        argMax(identifiers.match_confidence, identifiers.resolved_at) AS source_match_confidence,
        min(identifiers.first_seen_date) AS first_seen_date,
        max(identifiers.last_seen_date) AS last_seen_date
    FROM {{ source('corpscout', 'company_identifier') }} AS identifiers
    WHERE identifiers.country_code = '{{ var("country_code") }}'
      AND identifiers.issuer_scheme = 'vat'
      AND identifiers.is_current = 1
    GROUP BY identifiers.company_id, identifier_value
),
direct_wikidata AS (
    SELECT
        companies.company_id,
        ids.wikidata_id,
        'wikidata_registry_identifier' AS match_method
    FROM {{ source('corpscout', 'wikidata_company_identifiers') }} AS ids FINAL
    INNER JOIN companies
        ON companies.company_id = replaceRegexpAll(ids.identifier_value, '[^0-9]', '')
    WHERE ids.identifier_type = 'se_orgnr'
),
lei_wikidata AS (
    SELECT
        leis.company_id,
        ids.wikidata_id,
        'wikidata_verified_lei' AS match_method
    FROM {{ source('corpscout', 'wikidata_company_identifiers') }} AS ids FINAL
    INNER JOIN leis ON leis.identifier_value = upperUTF8(ids.identifier_value)
    WHERE ids.identifier_type = 'lei'
),
wikidata AS (
    SELECT
        company_id,
        wikidata_id,
        argMax(match_method, match_method = 'wikidata_registry_identifier') AS match_method,
        row_number() OVER (
            PARTITION BY company_id
            ORDER BY match_method = 'wikidata_registry_identifier' DESC, wikidata_id
        ) = 1 AS is_primary
    FROM (
        SELECT * FROM direct_wikidata
        UNION ALL
        SELECT * FROM lei_wikidata
    )
    GROUP BY company_id, wikidata_id
)
SELECT
    '{{ var("country_code") }}' AS country_code,
    company_id,
    'national_registry' AS identifier_scheme,
    company_id AS identifier_value,
    toUInt8(1) AS is_primary,
    'registry_anchor' AS match_method,
    toFloat32(1) AS match_confidence,
    CAST(NULL, 'Nullable(Date)') AS first_seen_date,
    CAST(NULL, 'Nullable(Date)') AS last_seen_date,
    now64(3, 'UTC') AS resolved_at
FROM companies

UNION ALL

SELECT
    '{{ var("country_code") }}', company_id, 'vat', identifier_value,
    toUInt8(row_number() OVER (PARTITION BY company_id ORDER BY last_seen_date DESC, identifier_value) = 1),
    source_match_method,
    toFloat32(multiIf(lowerUTF8(CAST(source_match_confidence, 'String')) IN ('exact', 'high', 'verified'), 1, lowerUTF8(CAST(source_match_confidence, 'String')) = 'medium', 0.75, 0.5)),
    first_seen_date, last_seen_date, now64(3, 'UTC')
FROM vats

UNION ALL

SELECT
    '{{ var("country_code") }}', company_id, 'lei', identifier_value,
    toUInt8(row_number() OVER (PARTITION BY company_id ORDER BY last_seen_date DESC, identifier_value) = 1),
    source_match_method,
    toFloat32(multiIf(lowerUTF8(CAST(source_match_confidence, 'String')) IN ('exact', 'high', 'verified'), 1, lowerUTF8(CAST(source_match_confidence, 'String')) = 'medium', 0.75, 0.5)),
    first_seen_date, last_seen_date, now64(3, 'UTC')
FROM leis

UNION ALL

SELECT
    '{{ var("country_code") }}', company_id, 'wikidata', wikidata_id,
    toUInt8(is_primary), match_method, toFloat32(1), NULL, NULL, now64(3, 'UTC')
FROM wikidata
