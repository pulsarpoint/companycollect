{{ config(materialized='table', order_by=['country_code', 'company_id', 'contact_type', 'contact_id']) }}

SELECT
    country_iso2 AS country_code,
    company_id,
    candidate_id AS contact_id,
    candidate_kind AS contact_type,
    normalized_value AS contact_value,
    registrable_domain,
    fiscal_year,
    toFloat32(least(1, greatest(0, evidence_count / 3))) AS confidence,
    now64(3, 'UTC') AS resolved_at
FROM {{ source('corpscout', 'esef_document_contact_candidates') }}
WHERE country_iso2 = '{{ var("country_code") }}'
QUALIFY row_number() OVER (
    PARTITION BY company_id, candidate_kind, normalized_value
    ORDER BY fiscal_year DESC, resolved_at DESC, candidate_id
) = 1
