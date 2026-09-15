{{ config(materialized='table', order_by=['country_code', 'company_id', 'root_domain']) }}

-- Entity processing owns source resolution, verification, withdrawals and history.
-- The serving publisher overlays the latest reviewer decisions before publication.
SELECT
    '{{ var("country_code") }}' AS country_code,
    domains.company_id AS company_id,
    domains.root_domain AS root_domain,
    domains.website_url AS website_url,
    domains.website_host AS website_host,
    domains.sources AS source_names,
    arrayMap(value -> toFloat32(value), domains.source_confidences) AS source_confidences,
    domains.source_record_ids AS source_record_ids,
    domains.source_urls AS source_urls,
    domains.confidence_bases AS confidence_bases,
    toFloat32(domains.confidence) AS suggested_confidence,
    domains.is_primary AS suggested_primary,
    domains.evidence_hash AS evidence_fingerprint,
    domains.review_status AS review_status,
    domains.review_note AS review_note,
    domains.reviewed_by AS reviewed_by,
    domains.reviewed_at AS reviewed_at,
    domains.reviewed_evidence_hash AS reviewed_evidence_fingerprint,
    domains.active AS is_active,
    domains.first_seen_at AS first_seen_at,
    domains.last_seen_at AS last_seen_at,
    domains.folded_at AS resolved_at
FROM {{ source('corpscout', 'se_company_domain') }} AS domains FINAL
INNER JOIN (SELECT company_id FROM {{ source('corpscout', 'se_company_basic_info') }} FINAL) AS companies
    ON companies.company_id = domains.company_id
WHERE '{{ var("country_code") }}' = 'SE'
