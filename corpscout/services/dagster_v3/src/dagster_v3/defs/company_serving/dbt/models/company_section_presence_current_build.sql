{{ config(materialized='table', order_by=['country_code', 'company_id', 'section']) }}

WITH company_anchors AS (
    SELECT company_id
    FROM {{ source('corpscout', 'se_companies') }} FINAL
),
section_rows AS (
    SELECT country_code, company_id, 'gleif' AS section, concat('entity:', lei) AS item_key, resolved_at AS observed_at
    FROM {{ ref('company_gleif_current_build') }}
    UNION ALL
    SELECT country_code, company_id, 'gleif', concat('relationship:', relationship_id), resolved_at
    FROM {{ ref('company_gleif_relationship_current_build') }}
    UNION ALL
    SELECT country_code, company_id, 'wikidata', wikidata_id, resolved_at FROM {{ ref('company_wikidata_current_build') }}
    UNION ALL
    SELECT country_code, company_id, 'management', management_id, resolved_at FROM {{ ref('company_management_current_build') }}
    UNION ALL
    SELECT country_code, company_id, 'descriptions', description_id, extracted_at FROM {{ ref('company_description_current_build') }}
    UNION ALL
    SELECT country_code, company_id, 'domains', concat('domain:', root_domain), resolved_at FROM {{ ref('company_domain_current_build') }}
    UNION ALL
    SELECT country_code, company_id, 'domains', concat('contact:', contact_id), resolved_at FROM {{ ref('company_contact_current_build') }}
    UNION ALL
    SELECT country_code, company_id, 'contracts', contract_ref, resolved_at FROM {{ ref('company_contract_current_build') }}
    UNION ALL
    SELECT '{{ var("country_code") }}', financials.company_id, 'financials', financials.company_id, financials.resolved_at
    FROM {{ source('corpscout', 'se_company_financials_latest') }} AS financials
    INNER JOIN company_anchors AS anchors ON anchors.company_id = financials.company_id
    UNION ALL
    SELECT '{{ var("country_code") }}', company_id, 'industries', classification_code, resolved_at
    FROM {{ ref('se_company_industry_display_current_build') }}
    UNION ALL
    SELECT '{{ var("country_code") }}', company_id, 'addresses', address_key, resolved_at
    FROM {{ ref('se_company_address_display_current_build') }}
    UNION ALL
    SELECT links.country_code, links.company_id, 'sources', toString(links.source_record_uid), links.linked_at
    FROM {{ source('corpscout', 'company_source_record_links') }} AS links
    INNER JOIN company_anchors AS anchors ON anchors.company_id = links.company_id
    WHERE links.country_code = '{{ var("country_code") }}'
    UNION ALL
    SELECT country_code, company_id, 'sources', source_record_uid, linked_at
    FROM {{ ref('company_serving_source_links_build') }}
    UNION ALL
    SELECT country_code, company_id, 'technology', root_domain, resolved_at
    FROM {{ ref('company_domain_current_build') }}
)
SELECT
    rows.country_code,
    rows.company_id,
    rows.section,
    toUInt32(countDistinct(rows.item_key)) AS item_count,
    max(rows.observed_at) AS latest_observed_at,
    now64(3, 'UTC') AS resolved_at
FROM section_rows AS rows
INNER JOIN company_anchors AS anchors
    ON anchors.company_id = rows.company_id
GROUP BY rows.country_code, rows.company_id, rows.section
