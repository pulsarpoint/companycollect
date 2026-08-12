{{ config(materialized='table', order_by=['country_code', 'company_id', 'is_primary', 'lei']) }}

WITH leis AS (
    SELECT company_id, identifier_value AS lei, is_primary
    FROM {{ ref('company_external_identifier_current_build') }}
    WHERE identifier_scheme = 'lei'
),
exceptions AS (
    SELECT lei, arraySort(groupUniqArray(ifNull(exception_reason, exception_category))) AS reasons
    FROM {{ source('corpscout', 'gleif_lei_reporting_exceptions') }} FINAL
    GROUP BY lei
)
SELECT
    '{{ var("country_code") }}' AS country_code,
    leis.company_id,
    records.lei AS lei,
    leis.is_primary,
    records.legal_name,
    records.entity_status,
    records.registration_status,
    ifNull(records.category, '') AS category,
    ifNull(records.legal_form_id, '') AS legal_form_id,
    ifNull(records.jurisdiction, '') AS jurisdiction,
    ifNull(records.legal_address_country, '') AS legal_address_country,
    ifNull(records.headquarters_address_country, '') AS headquarters_country,
    toUInt8(materialize(ifNull(records.headquarters_address_country, '')) != ''
        AND materialize(ifNull(records.headquarters_address_country, '')) != '{{ var("country_code") }}') AS headquarters_abroad,
    ifNull(exceptions.reasons, CAST([], 'Array(String)')) AS ownership_exception_reasons,
    records.initial_registration_date,
    records.last_update_date,
    records.next_renewal_date,
    now64(3, 'UTC') AS resolved_at
FROM leis
INNER JOIN {{ source('corpscout', 'gleif_lei_records') }} AS records FINAL ON records.lei = leis.lei
LEFT JOIN exceptions ON exceptions.lei = leis.lei
