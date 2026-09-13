{{ config(materialized='table', order_by=['country_code', 'company_id', 'section']) }}

WITH company_anchors AS (
    SELECT company_id
    FROM {{ source('corpscout', 'se_company_basic_info') }} FINAL
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
    SELECT country_code, company_id, 'descriptions', description_id, extracted_at FROM {{ ref('company_description_current_build') }}
    UNION ALL
    SELECT country_code, company_id, 'domains', concat('domain:', root_domain), resolved_at FROM {{ ref('company_domain_current_build') }}
    UNION ALL
    SELECT country_code, company_id, 'domains', concat('contact:', contact_id), resolved_at FROM {{ ref('company_contact_current_build') }}
    UNION ALL
    SELECT country_code, company_id, 'contracts', contract_ref, resolved_at FROM {{ ref('company_contract_current_build') }}
    UNION ALL
    -- The financial entity (spec 2026-09-11 section 10, slice 4a): one presence row per
    -- company with an ACTIVE folded period, keyed by company as before; folded_at is the
    -- observation instant. se_company_financials_latest is a projection of the same table
    -- since that slice, so the presence reads the table it is derived from.
    SELECT '{{ var("country_code") }}', financials.company_id, 'financials', financials.company_id, financials.folded_at
    FROM {{ source('corpscout', 'se_company_financial') }} AS financials FINAL
    INNER JOIN company_anchors AS anchors ON anchors.company_id = financials.company_id
    WHERE financials.active = 1
    UNION ALL
    SELECT '{{ var("country_code") }}', company_id, 'industries', classification_code, resolved_at
    FROM {{ ref('se_company_industry_display_current_build') }}
    UNION ALL
    -- se_company_addresses_current (itself standing in for the retired
    -- se_company_address_display_current_build, migration 000314) is retired in favor of
    -- the address entity table (slice 4a, 2026-09-08): one row per company and published
    -- address, keyed by address_key, folded_at as the observation instant.
    SELECT '{{ var("country_code") }}', company_id, 'addresses',
           toString(address_key), folded_at
    FROM {{ source('corpscout', 'se_company_address') }} FINAL
    WHERE active = 1
    UNION ALL
    SELECT country_code, company_id, 'sources', source_record_uid, linked_at
    FROM {{ ref('company_section_item_source_links_build') }}
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
