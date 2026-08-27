{{ config(materialized='table', order_by=['country_code', 'company_id', 'section', 'item_key', 'source_record_uid']) }}

WITH company_anchors AS (
    SELECT company_id
    FROM {{ source('corpscout', 'se_companies') }} FINAL
),
source_records_raw AS (
    SELECT
        bolagsverket_source_record_uid AS source_record_uid,
        'registry_company' AS record_kind,
        lowerUTF8(ifNull(bolagsverket_source_payload_hash, '')) AS content_sha256,
        updated_from_raw_at AS first_seen_at,
        updated_from_raw_at AS last_seen_at,
        'sweden_bolagsverket' AS source_slug,
        ifNull(bolagsverket_source_record_id, '') AS source_record_key,
        '' AS source_url,
        '' AS source_object_key,
        lowerUTF8(ifNull(bolagsverket_source_payload_hash, '')) AS payload_sha256,
        updated_from_raw_at AS retrieved_at,
        source_run_id
    FROM {{ source('corpscout', 'se_companies') }} FINAL
    WHERE bolagsverket_source_record_uid != ''
    UNION ALL
    SELECT
        scb_source_record_uid, 'registry_company', lowerUTF8(ifNull(scb_source_payload_hash, '')),
        updated_from_raw_at, updated_from_raw_at, 'sweden_scb',
        ifNull(scb_source_record_id, ''), '', '', lowerUTF8(ifNull(scb_source_payload_hash, '')),
        updated_from_raw_at, source_run_id
    FROM {{ source('corpscout', 'se_companies') }} FINAL
    WHERE scb_source_record_uid != ''
    UNION ALL
    SELECT
        source_record_uid, 'annual_report_xhtml', lowerUTF8(toString(source_payload_hash)),
        resolved_at, resolved_at, 'sweden_financial', statement_key, '', xhtml_object_key,
        lowerUTF8(toString(source_payload_hash)), resolved_at, source_run_id
    FROM {{ source('corpscout', 'se_financial_reports') }} FINAL
    WHERE source_record_uid != ''
    UNION ALL
    SELECT
        lower(hex(SHA256(concat(
            'company-source-record-v1\nfile\nesef_report_package\n', lowerUTF8(package_sha256)
        )))),
        'esef_report_package', lowerUTF8(package_sha256),
        toDateTime64(resolved_at, 3, 'UTC'), toDateTime64(resolved_at, 3, 'UTC'),
        'esef_filings', fxo_id, package_url, '', lowerUTF8(package_sha256),
        toDateTime64(resolved_at, 3, 'UTC'), source_run_id
    FROM {{ source('corpscout', 'esef_filings') }} FINAL
    WHERE package_sha256 != ''
    UNION ALL
    SELECT
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\nwikidata\nwikidata_company_item\n',
            wikidata_id, '\n', lowerUTF8(toString(source_payload_hash))
        )))),
        'wikidata_company_item', lowerUTF8(toString(source_payload_hash)),
        retrieved_at, retrieved_at, 'wikidata', wikidata_id, wikidata_url, '',
        lowerUTF8(toString(source_payload_hash)), retrieved_at, source_run_id
    FROM {{ source('corpscout', 'wikidata_companies') }} FINAL
    WHERE wikidata_id != '' AND source_payload_hash != ''
    UNION ALL
    SELECT
        source_record_uid, 'wikidata_person_item', lowerUTF8(toString(source_payload_hash)),
        retrieved_at, retrieved_at, 'wikidata', person_wikidata_id,
        ifNull(wikidata_url, ''), '', lowerUTF8(toString(source_payload_hash)),
        retrieved_at, source_run_id
    FROM {{ source('corpscout', 'wikidata_persons') }} FINAL
    WHERE source_record_uid != '' AND source_payload_hash != ''
    UNION ALL
    SELECT
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\ngleif\ngleif_lei_record\n',
            lei, '\n', lower(hex(SHA256(concat(
                legal_name, '|', entity_status, '|', registration_status, '|',
                ifNull(jurisdiction, ''), '|', toString(last_update_date)
            ))))
        )))),
        'gleif_lei_record', lower(hex(SHA256(concat(
            legal_name, '|', entity_status, '|', registration_status, '|',
            ifNull(jurisdiction, ''), '|', toString(last_update_date)
        )))),
        retrieved_at, retrieved_at, 'gleif', lei,
        concat('https://lei.bloomberg.com/leis/view/', lei), '',
        lower(hex(SHA256(concat(
            legal_name, '|', entity_status, '|', registration_status, '|',
            ifNull(jurisdiction, ''), '|', toString(last_update_date)
        )))), retrieved_at, source_run_id
    FROM {{ source('corpscout', 'gleif_lei_records') }} FINAL
    UNION ALL
    SELECT
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\ngleif\ngleif_relationship_record\n',
            relationship_record_id, '\n', lower(hex(SHA256(concat(
                start_node_lei, '|', end_node_lei, '|', relationship_type, '|',
                relationship_status, '|', toString(last_update_date)
            ))))
        )))),
        'gleif_relationship_record', lower(hex(SHA256(concat(
            start_node_lei, '|', end_node_lei, '|', relationship_type, '|',
            relationship_status, '|', toString(last_update_date)
        )))),
        retrieved_at, retrieved_at, 'gleif', relationship_record_id, '', '',
        lower(hex(SHA256(concat(
            start_node_lei, '|', end_node_lei, '|', relationship_type, '|',
            relationship_status, '|', toString(last_update_date)
        )))), retrieved_at, source_run_id
    FROM {{ source('corpscout', 'gleif_lei_relationships') }} FINAL
    UNION ALL
    SELECT
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\n', source_slug,
            '\ncontract\n', contract_ref, '\n', lower(hex(SHA256(concat(
                company_id, '|', source_notice_id, '|', source_lot_id, '|', title, '|',
                ifNull(toString(value_amount_original), '<null>'), '|', value_currency
            ))))
        )))),
        'contract', lower(hex(SHA256(concat(
            company_id, '|', source_notice_id, '|', source_lot_id, '|', title, '|',
            ifNull(toString(value_amount_original), '<null>'), '|', value_currency
        )))),
        toDateTime64(resolved_at, 3, 'UTC'), toDateTime64(resolved_at, 3, 'UTC'),
        source_slug, contract_ref, source_url, '',
        lower(hex(SHA256(concat(
            company_id, '|', source_notice_id, '|', source_lot_id, '|', title, '|',
            ifNull(toString(value_amount_original), '<null>'), '|', value_currency
        )))), toDateTime64(resolved_at, 3, 'UTC'), '{{ var("source_run_id") }}'
    FROM {{ source('corpscout', 'company_contract_facts') }}
    WHERE country_code = '{{ var("country_code") }}'
    QUALIFY row_number() OVER (
        PARTITION BY company_id, contract_ref
        ORDER BY resolved_at DESC, source_notice_id, source_lot_id, source_winner_ordinal
    ) = 1
),
source_records AS (
    SELECT *
    FROM source_records_raw
    WHERE source_record_uid != ''
    QUALIFY row_number() OVER (
        PARTITION BY source_record_uid
        ORDER BY retrieved_at DESC, source_slug, source_record_key
    ) = 1
),
registry_source_uids AS (
    SELECT company_id, bolagsverket_source_record_uid AS source_record_uid
    FROM {{ source('corpscout', 'se_companies') }} FINAL
    WHERE bolagsverket_source_record_uid != ''
    UNION ALL
    SELECT company_id, scb_source_record_uid
    FROM {{ source('corpscout', 'se_companies') }} FINAL
    WHERE scb_source_record_uid != ''
),
registry_sources AS (
    SELECT
        '{{ var("country_code") }}' AS country_code,
        companies.company_id AS company_id,
        'sources' AS section,
        companies.source_record_uid AS item_key,
        companies.source_record_uid AS source_record_uid,
        'registry_subject' AS relationship_kind,
        'national_registry_identifier' AS match_method,
        toFloat32(1) AS match_confidence,
        '{{ var("source_run_id") }}' AS source_run_id,
        now64(3, 'UTC') AS linked_at
    FROM registry_source_uids AS companies
),
financial_sources AS (
    SELECT
        reports.country_iso2 AS country_code,
        reports.company_id,
        'sources' AS section,
        reports.source_record_uid AS item_key,
        reports.source_record_uid,
        'annual_report_subject' AS relationship_kind,
        'registry_company_identifier' AS match_method,
        toFloat32(1) AS match_confidence,
        reports.source_run_id,
        reports.resolved_at AS linked_at
    FROM {{ source('corpscout', 'se_financial_reports') }} AS reports FINAL
    WHERE reports.country_iso2 = '{{ var("country_code") }}'
      AND reports.source_record_uid != ''
),
esef_sources AS (
    SELECT
        mappings.country_iso2 AS country_code,
        companies.company_id,
        'sources' AS section,
        lower(hex(SHA256(concat(
            'company-source-record-v1\nfile\nesef_report_package\n',
            lowerUTF8(filings.package_sha256)
        )))) AS item_key,
        lower(hex(SHA256(concat(
            'company-source-record-v1\nfile\nesef_report_package\n',
            lowerUTF8(filings.package_sha256)
        )))) AS source_record_uid,
        'annual_report_subject' AS relationship_kind,
        mappings.match_source AS match_method,
        toFloat32(1) AS match_confidence,
        filings.source_run_id,
        toDateTime64(filings.resolved_at, 3, 'UTC') AS linked_at
    FROM {{ source('corpscout', 'esef_entity_registry_map') }} AS mappings FINAL
    INNER JOIN {{ source('corpscout', 'se_companies') }} AS companies FINAL
        ON companies.registration_number = mappings.registry_id
    INNER JOIN {{ source('corpscout', 'esef_filings') }} AS filings FINAL
        ON filings.lei = mappings.lei
    WHERE mappings.country_iso2 = '{{ var("country_code") }}'
      AND filings.package_sha256 != ''
),
gleif AS (
    SELECT
        current.country_code, current.company_id, 'gleif' AS section,
        concat('entity:', current.lei) AS item_key,
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\ngleif\ngleif_lei_record\n',
            records.lei, '\n', lower(hex(SHA256(concat(
                records.legal_name, '|', records.entity_status, '|',
                records.registration_status, '|', ifNull(records.jurisdiction, ''), '|',
                toString(records.last_update_date)
            ))))
        )))) AS source_record_uid,
        'gleif_identifier' AS relationship_kind,
        'company_identifier' AS match_method, toFloat32(1) AS match_confidence,
        records.source_run_id, records.retrieved_at AS linked_at
    FROM {{ ref('company_gleif_current_build') }} AS current
    INNER JOIN {{ source('corpscout', 'gleif_lei_records') }} AS records FINAL
        ON records.lei = current.lei
),
gleif_relationships AS (
    SELECT
        current.country_code, current.company_id, 'gleif' AS section,
        concat('relationship:', current.relationship_id) AS item_key,
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\ngleif\ngleif_relationship_record\n',
            records.relationship_record_id, '\n', lower(hex(SHA256(concat(
                records.start_node_lei, '|', records.end_node_lei, '|',
                records.relationship_type, '|', records.relationship_status, '|',
                toString(records.last_update_date)
            ))))
        )))) AS source_record_uid,
        'gleif_relationship' AS relationship_kind,
        'company_lei_endpoint' AS match_method, toFloat32(1) AS match_confidence,
        records.source_run_id, records.retrieved_at AS linked_at
    FROM {{ ref('company_gleif_relationship_current_build') }} AS current
    INNER JOIN {{ source('corpscout', 'gleif_lei_relationships') }} AS records FINAL
        ON records.relationship_record_id = current.relationship_id
),
contracts AS (
    SELECT
        facts.country_code, facts.company_id, 'contracts' AS section,
        facts.contract_ref AS item_key,
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\n', facts.source_slug,
            '\ncontract\n', facts.contract_ref, '\n', lower(hex(SHA256(concat(
                facts.company_id, '|', facts.source_notice_id, '|', facts.source_lot_id, '|',
                facts.title, '|', ifNull(toString(facts.value_amount_original), '<null>'), '|',
                facts.value_currency
            ))))
        )))) AS source_record_uid,
        'contract_supplier' AS relationship_kind,
        'resolved_supplier_identifier' AS match_method,
        toFloat32(1) AS match_confidence,
        '{{ var("source_run_id") }}' AS source_run_id,
        toDateTime64(facts.resolved_at, 3, 'UTC') AS linked_at
    FROM {{ source('corpscout', 'company_contract_facts') }} AS facts
    WHERE facts.country_code = '{{ var("country_code") }}'
    QUALIFY row_number() OVER (
        PARTITION BY facts.company_id, facts.contract_ref
        ORDER BY facts.resolved_at DESC, facts.source_notice_id,
                 facts.source_lot_id, facts.source_winner_ordinal
    ) = 1
),
wikidata AS (
    SELECT
        current.country_code, current.company_id, 'wikidata' AS section,
        current.wikidata_id AS item_key,
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\nwikidata\nwikidata_company_item\n',
            records.wikidata_id, '\n', lowerUTF8(toString(records.source_payload_hash))
        )))) AS source_record_uid,
        'wikidata_entity' AS relationship_kind,
        'resolved_wikidata_identifier' AS match_method,
        toFloat32(1) AS match_confidence, records.source_run_id,
        records.retrieved_at AS linked_at
    FROM {{ ref('company_wikidata_current_build') }} AS current
    INNER JOIN {{ source('corpscout', 'wikidata_companies') }} AS records FINAL
        ON records.wikidata_id = current.wikidata_id
),
wikidata_domains AS (
    SELECT
        current.country_code, current.company_id, 'domains' AS section,
        current.root_domain AS item_key,
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\nwikidata\nwikidata_company_item\n',
            records.wikidata_id, '\n', lowerUTF8(toString(records.source_payload_hash))
        )))) AS source_record_uid,
        'wikidata_website' AS relationship_kind,
        'official_website_claim' AS match_method,
        arrayElement(current.source_confidences, indexOf(current.source_names, 'wikidata')) AS match_confidence,
        records.source_run_id, records.retrieved_at AS linked_at
    FROM {{ ref('company_domains_build') }} AS current
    INNER JOIN {{ ref('company_external_identifier_current_build') }} AS ids
        ON ids.country_code = current.country_code AND ids.company_id = current.company_id
       AND ids.identifier_scheme = 'wikidata'
    INNER JOIN {{ source('corpscout', 'wikidata_companies') }} AS records FINAL
        ON records.wikidata_id = ids.identifier_value
    WHERE has(current.source_names, 'wikidata')
),
esef_domains AS (
    SELECT
        current.country_code, current.company_id, 'domains' AS section,
        current.root_domain AS item_key, candidates.source_record_uid,
        'annual_report_website' AS relationship_kind,
        'annual_report_extraction' AS match_method,
        arrayElement(current.source_confidences, indexOf(current.source_names, 'esef_filing')) AS match_confidence,
        candidates.source_run_id, candidates.resolved_at AS linked_at
    FROM {{ ref('company_domains_build') }} AS current
    INNER JOIN {{ source('corpscout', 'esef_document_contact_candidates') }} AS candidates
        ON candidates.country_iso2 = current.country_code
       AND candidates.company_id = current.company_id
       AND candidates.registrable_domain = current.root_domain
       AND candidates.candidate_kind = 'website'
    WHERE has(current.source_names, 'esef_filing')
      AND candidates.source_record_uid != ''
),
contacts AS (
    SELECT
        current.country_code, current.company_id, 'domains' AS section,
        current.contact_id AS item_key, candidates.source_record_uid,
        'contact_evidence' AS relationship_kind,
        'annual_report_extraction' AS match_method,
        current.confidence AS match_confidence, candidates.source_run_id,
        candidates.resolved_at AS linked_at
    FROM {{ ref('company_contact_current_build') }} AS current
    INNER JOIN {{ source('corpscout', 'esef_document_contact_candidates') }} AS candidates
        ON candidates.candidate_id = current.contact_id
    WHERE candidates.source_record_uid != ''
),
registry_observations AS (
    SELECT
        '{{ var("country_code") }}' AS country_code,
        company_id,
        reinterpretAsUUID(unhex(substring(hex(SHA256(concat(
            'country_person_observation|SE|se_xbrl_signatures|',
            statement_key, '|', signatory_kind, '|', toString(person_seq)
        ))), 1, 32))) AS observation_id,
        source_record_uid
    FROM {{ source('corpscout', 'se_financial_report_signatories') }}
),
registry_management AS (
    -- Mirrors company_management_current_build's management_id hash. No
    -- country_person identity resolution remains (country_people group
    -- retired), so the observation id -- the same one that model now uses
    -- directly as its grouping key -- is hashed straight through with no
    -- join required to keep this evidence link's item_key aligned with
    -- that model's management_id.
    SELECT
        observations.country_code,
        observations.company_id,
        lower(hex(SHA256(concat(
            'registry-person|', observations.country_code, '|', observations.company_id, '|',
            toString(observations.observation_id)
        )))) AS item_key,
        observations.source_record_uid
    FROM registry_observations AS observations
    GROUP BY observations.country_code, observations.company_id, item_key, observations.source_record_uid
),
management_candidates AS (
    SELECT
        current.country_code AS country_code,
        current.company_id AS company_id,
        'management' AS section,
        current.management_id AS item_key,
        if(
            has(current.source_systems, 'se_xbrl_signatures'), registry_rows.source_record_uid,
            if(has(current.source_systems, 'wikidata'), people.source_record_uid, esef.source_record_uid)
        ) AS selected_source_record_uid,
        multiIf(
            has(current.source_systems, 'se_xbrl_signatures'), 'annual_report_signature',
            has(current.source_systems, 'wikidata'), 'public_knowledge_graph_company_role',
            'annual_report_narrative_role'
        ) AS relationship_kind,
        multiIf(
            has(current.source_systems, 'se_xbrl_signatures'), 'annual_report_signatory_observation',
            has(current.source_systems, 'wikidata'), 'wikidata_company_claim',
            'annual_report_extraction'
        ) AS match_method,
        current.confidence AS match_confidence,
        '{{ var("source_run_id") }}' AS source_run_id,
        now64(3, 'UTC') AS linked_at
    FROM {{ ref('company_management_current_build') }} AS current
    LEFT JOIN registry_management AS registry_rows
        ON registry_rows.country_code = current.country_code
       AND registry_rows.company_id = current.company_id
       AND registry_rows.item_key = current.management_id
    LEFT JOIN {{ source('corpscout', 'wikidata_persons') }} AS people FINAL
        ON people.person_wikidata_id = current.external_person_value
       AND current.external_person_scheme = 'wikidata'
    LEFT JOIN {{ source('corpscout', 'esef_document_people') }} AS esef FINAL
        ON esef.candidate_uid = current.management_id
),
management AS (
    SELECT
        country_code, company_id, section, item_key,
        selected_source_record_uid AS source_record_uid,
        relationship_kind, match_method, match_confidence, source_run_id, linked_at
    FROM management_candidates
    WHERE selected_source_record_uid != ''
),
descriptions AS (
    SELECT
        current.country_code AS country_code,
        current.company_id AS company_id,
        'descriptions' AS section,
        current.description_id AS item_key,
        current.source_record_uid AS source_record_uid,
        'description_evidence' AS relationship_kind,
        current.extraction_method AS match_method,
        current.confidence AS match_confidence,
        '{{ var("source_run_id") }}' AS source_run_id,
        current.extracted_at AS linked_at
    FROM {{ ref('company_description_current_build') }} AS current
    WHERE current.source_record_uid != ''
),
addresses AS (
    SELECT
        '{{ var("country_code") }}' AS country_code, company_id, 'addresses' AS section,
        address_fingerprint AS item_key, source_record_uid,
        'registry_address' AS relationship_kind,
        'registry_company_record' AS match_method,
        toFloat32(1) AS match_confidence, source_run_id, observed_at AS linked_at
    FROM {{ source('corpscout', 'se_company_addresses_current') }}
    WHERE has_address = 1 AND has_observation = 1 AND source_record_uid != ''
),
industries AS (
    SELECT
        '{{ var("country_code") }}' AS country_code, company_id, 'industries' AS section,
        nace_rev2_class_code AS item_key, source_record_uid,
        'registry_industry' AS relationship_kind,
        'registry_company_record' AS match_method,
        toFloat32(1) AS match_confidence, source_run_id,
        updated_from_raw_at AS linked_at
    FROM {{ source('corpscout', 'se_industries') }} FINAL
    WHERE nace_rev2_class_code != '' AND source_record_uid != ''
),
financials AS (
    SELECT
        metrics.country_iso2 AS country_code, metrics.company_id,
        'financials' AS section,
        concat('standalone:', toString(metrics.fiscal_year)) AS item_key,
        metrics.source_record_uid, 'financial_statement' AS relationship_kind,
        'registry_company_identifier' AS match_method,
        toFloat32(1) AS match_confidence, metrics.source_run_id,
        metrics.resolved_at AS linked_at
    FROM {{ source('corpscout', 'se_bolagsverket_financial_metrics') }} AS metrics FINAL
    INNER JOIN company_anchors AS anchors ON anchors.company_id = metrics.company_id
    WHERE metrics.country_iso2 = '{{ var("country_code") }}'
      AND metrics.source_record_uid != ''
),
evidence_links AS (
    SELECT * FROM registry_sources
    UNION ALL SELECT * FROM financial_sources
    UNION ALL SELECT * FROM esef_sources
    UNION ALL SELECT * FROM gleif
    UNION ALL SELECT * FROM gleif_relationships
    UNION ALL SELECT * FROM contracts
    UNION ALL SELECT * FROM wikidata
    UNION ALL SELECT * FROM wikidata_domains
    UNION ALL SELECT * FROM esef_domains
    UNION ALL SELECT * FROM contacts
    UNION ALL SELECT * FROM management
    UNION ALL SELECT * FROM descriptions
    UNION ALL SELECT * FROM addresses
    UNION ALL SELECT * FROM industries
    UNION ALL SELECT * FROM financials
)
SELECT
    country_code,
    company_id,
    section,
    CAST(linked_item_key AS String) AS item_key,
    CAST(linked_source_record_uid AS String) AS source_record_uid,
    relationship_kind,
    match_method,
    match_confidence,
    source_run_id,
    linked_at,
    record_kind,
    content_sha256,
    first_seen_at,
    last_seen_at,
    source_slug,
    source_record_key,
    source_url,
    source_object_key,
    payload_sha256,
    retrieved_at
FROM (
    SELECT DISTINCT
        links.country_code AS country_code,
        links.company_id AS company_id,
        links.section AS section,
        links.item_key AS linked_item_key,
        links.source_record_uid AS linked_source_record_uid,
        links.relationship_kind AS relationship_kind,
        links.match_method AS match_method,
        links.match_confidence AS match_confidence,
        source_metadata.source_run_id AS source_run_id,
        links.linked_at AS linked_at,
        source_metadata.record_kind AS record_kind,
        source_metadata.content_sha256 AS content_sha256,
        source_metadata.first_seen_at AS first_seen_at,
        source_metadata.last_seen_at AS last_seen_at,
        source_metadata.source_slug AS source_slug,
        source_metadata.source_record_key AS source_record_key,
        source_metadata.source_url AS source_url,
        source_metadata.source_object_key AS source_object_key,
        source_metadata.payload_sha256 AS payload_sha256,
        source_metadata.retrieved_at AS retrieved_at
    FROM evidence_links AS links
    INNER JOIN company_anchors AS anchors
        ON anchors.company_id = links.company_id
    INNER JOIN source_records AS source_metadata
        ON source_metadata.source_record_uid = links.source_record_uid
)
