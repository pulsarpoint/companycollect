{{
    config(
        materialized='view',
        tags=['company_domain_company_features']
    )
}}

WITH companies AS (
    SELECT
        company_id,
        ifNull(legal_name, '') AS company_name
    FROM {{ source('corpscout', 'se_companies') }} FINAL
    WHERE match(company_id, '^[0-9]{10}$')
),

-- The two register source tables of the 2026-09-03 SE basic-info design replace the
-- retired registry projection. Each is ReplacingMergeTree(observed_at) ORDER BY
-- company_id, so FINAL gives the last row written per company. The publisher appends a
-- has_company = 0 tombstone row when a source stops delivering a company and inserts the
-- company again when it returns, so has_company = 1 on the FINAL row is the delivered set,
-- the same contract the retired table's has_company flag gave this model.
-- Bolagsverket has no alternate name, exactly as before.
register_names AS (
    SELECT
        company_id,
        'scb' AS source,
        legal_name,
        alternate_name
    FROM {{ source('corpscout', 'se_scb_companies') }} FINAL
    WHERE has_company = 1

    UNION ALL

    SELECT
        company_id,
        'bolagsverket' AS source,
        legal_name,
        CAST(NULL AS Nullable(String)) AS alternate_name
    FROM {{ source('corpscout', 'se_bolagsverket_companies') }} FINAL
    WHERE has_company = 1
),

company_name_values AS (
    SELECT
        companies.company_id,
        companies.company_name,
        companies.company_name AS raw_value,
        'se_companies.legal_name' AS source_field
    FROM companies
    WHERE companies.company_name != ''

    UNION ALL

    SELECT
        companies.company_id,
        companies.company_name,
        ifNull(registry.legal_name, '') AS raw_value,
        concat(registry.source, '.legal_name') AS source_field
    FROM register_names AS registry
    INNER JOIN companies USING (company_id)
    WHERE ifNull(registry.legal_name, '') != ''

    UNION ALL

    SELECT
        companies.company_id,
        companies.company_name,
        ifNull(registry.alternate_name, '') AS raw_value,
        concat(registry.source, '.alternate_name') AS source_field
    FROM register_names AS registry
    INNER JOIN companies USING (company_id)
    WHERE ifNull(registry.alternate_name, '') != ''
),

name_features AS (
    SELECT DISTINCT
        'SE' AS country_iso2,
        company_id,
        company_name,
        'name' AS feature_type,
        'legal_name' AS feature_subtype,
        {{ normalize_identity_value('raw_value') }} AS normalized_value,
        raw_value,
        source_field
    FROM company_name_values
    WHERE length({{ normalize_identity_value('raw_value') }}) >= 3
),

vat_features AS (
    SELECT
        'SE' AS country_iso2,
        company_id,
        company_name,
        'identifier' AS feature_type,
        'vat' AS feature_subtype,
        concat('se', company_id, '01') AS normalized_value,
        concat('SE', company_id, '01') AS raw_value,
        'derived.expected_vat_identifier' AS source_field
    FROM companies
),

lei_features AS (
    SELECT DISTINCT
        'SE' AS country_iso2,
        companies.company_id,
        companies.company_name,
        'identifier' AS feature_type,
        'lei' AS feature_subtype,
        {{ normalize_identity_value('lei.lei') }} AS normalized_value,
        upperUTF8(lei.lei) AS raw_value,
        'gleif.lei' AS source_field
    FROM {{ source('corpscout', 'gleif_lei_records') }} AS lei FINAL
    INNER JOIN companies
        ON companies.company_id = {{ normalize_identity_value('lei.registered_as') }}
    WHERE match({{ normalize_identity_value('lei.lei') }}, '^[a-z0-9]{20}$')
      AND (
          upperUTF8(ifNull(lei.primary_country_iso2, '')) = 'SE'
          OR startsWith(upperUTF8(ifNull(lei.jurisdiction, '')), 'SE')
      )
),

address_features AS (
    SELECT DISTINCT
        'SE' AS country_iso2,
        companies.company_id,
        companies.company_name,
        'address' AS feature_type,
        'postal' AS feature_subtype,
        addresses.normalized_address AS normalized_value,
        ifNull(
            nullIf(trim(ifNull(addresses.raw_address, '')), ''),
            arrayStringConcat(arrayFilter(component -> component != '', [
                ifNull(addresses.street_address, ''),
                trim(concat(
                    ifNull(addresses.postal_code, ''),
                    ' ',
                    ifNull(addresses.post_town, '')
                )),
                ifNull(addresses.country_code, '')
            ]), ', ')
        ) AS raw_value,
        concat(addresses.source, '.', addresses.address_type) AS source_field
    FROM {{ source('corpscout', 'se_company_addresses_current') }} AS addresses
    INNER JOIN companies USING (company_id)
    WHERE addresses.has_address = 1
      AND addresses.normalized_address != ''
),

industry_features AS (
    SELECT DISTINCT
        'SE' AS country_iso2,
        companies.company_id,
        companies.company_name,
        'industry' AS feature_type,
        'nace' AS feature_subtype,
        replaceRegexpAll(industries.nace_rev2_class_code, '[^0-9]', '')
            AS normalized_value,
        industries.nace_rev2_class_code AS raw_value,
        concat('scb.', industries.source_field) AS source_field
    FROM {{ source('corpscout', 'se_industries') }} AS industries FINAL
    INNER JOIN companies USING (company_id)
    WHERE match(
        replaceRegexpAll(industries.nace_rev2_class_code, '[^0-9]', ''),
        '^[0-9]{4}$'
    )
)

SELECT * FROM name_features
UNION ALL
SELECT * FROM vat_features
UNION ALL
SELECT * FROM lei_features
UNION ALL
SELECT * FROM address_features
UNION ALL
SELECT * FROM industry_features
