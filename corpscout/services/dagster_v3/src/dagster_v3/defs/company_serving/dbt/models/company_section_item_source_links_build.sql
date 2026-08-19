{{ config(materialized='table', order_by=['country_code', 'company_id', 'section', 'item_key', 'source_record_uid']) }}

WITH company_anchors AS (
    SELECT company_id
    FROM {{ source('corpscout', 'se_companies') }} FINAL
),
synthetic_links AS (
    SELECT
        source_links.country_code,
        source_links.company_id,
        multiIf(
            source_links.relationship_kind = 'gleif_identifier', 'gleif',
            source_links.relationship_kind = 'gleif_relationship', 'gleif',
            'contracts'
        ) AS section,
        origins.source_record_key AS item_key,
        source_links.source_record_uid,
        source_links.relationship_kind,
        source_links.match_method,
        source_links.match_confidence,
        source_links.source_run_id,
        source_links.linked_at
    FROM {{ ref('company_serving_source_links_build') }} AS source_links
    INNER JOIN {{ ref('company_serving_source_origins_build') }} AS origins
        ON origins.source_record_uid = source_links.source_record_uid
),
wikidata_snapshots AS (
    SELECT wikidata_id, argMax(source_record_uid, retrieved_at) AS source_record_uid
    FROM {{ source('corpscout', 'wikidata_company_source_snapshots') }} FINAL
    GROUP BY wikidata_id
),
wikidata AS (
    SELECT
        current.country_code, current.company_id, 'wikidata' AS section,
        current.wikidata_id AS item_key, snapshots.source_record_uid,
        'wikidata_entity' AS relationship_kind, 'resolved_wikidata_identifier' AS match_method,
        toFloat32(1) AS match_confidence, '{{ var("source_run_id") }}' AS source_run_id,
        now64(3, 'UTC') AS linked_at
    FROM {{ ref('company_wikidata_current_build') }} AS current
    INNER JOIN wikidata_snapshots AS snapshots ON snapshots.wikidata_id = current.wikidata_id
),
wikidata_domains AS (
    SELECT
        current.country_code, current.company_id, 'domains' AS section,
        current.root_domain AS item_key, snapshots.source_record_uid,
        'wikidata_website' AS relationship_kind, 'official_website_claim' AS match_method,
        arrayElement(current.source_confidences, indexOf(current.source_names, 'wikidata')) AS match_confidence,
        '{{ var("source_run_id") }}' AS source_run_id,
        now64(3, 'UTC') AS linked_at
    FROM {{ ref('company_domains_build') }} AS current
    INNER JOIN {{ ref('company_external_identifier_current_build') }} AS ids
        ON ids.country_code = current.country_code AND ids.company_id = current.company_id
       AND ids.identifier_scheme = 'wikidata'
    INNER JOIN wikidata_snapshots AS snapshots ON snapshots.wikidata_id = ids.identifier_value
    WHERE has(current.source_names, 'wikidata')
),
esef_domains AS (
    SELECT
        current.country_code, current.company_id, 'domains' AS section,
        current.root_domain AS item_key, candidates.source_record_uid,
        'annual_report_website' AS relationship_kind, 'annual_report_extraction' AS match_method,
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
        'contact_evidence' AS relationship_kind, 'annual_report_extraction' AS match_method,
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
registry_person_matches AS (
    SELECT
        source_matches.country_iso2,
        source_matches.observation_id,
        argMax(source_matches.person_id, (
            source_matches.decided_at,
            source_matches.resolver_version,
            toString(source_matches.person_id)
        )) AS person_id
    FROM {{ source('corpscout', 'country_person_match') }} AS source_matches
    WHERE source_matches.country_iso2 = '{{ var("country_code") }}'
    GROUP BY source_matches.country_iso2, source_matches.observation_id
),
registry_management AS (
    SELECT
        observations.country_code,
        observations.company_id,
        lower(hex(SHA256(concat(
            'registry-person|', observations.country_code, '|', observations.company_id, '|',
            toString(ifNull(matches.person_id, observations.observation_id))
        )))) AS item_key,
        observations.source_record_uid
    FROM registry_observations AS observations
    LEFT JOIN registry_person_matches AS matches
        ON matches.country_iso2 = observations.country_code
       AND matches.observation_id = observations.observation_id
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
            has(current.source_systems, 'se_xbrl_signatures'), 'country_person_match',
            has(current.source_systems, 'wikidata'), 'wikidata_company_claim',
            'annual_report_extraction'
        ) AS match_method,
        current.confidence AS match_confidence,
        '{{ var("source_run_id") }}' AS source_run_id,
        now64(3, 'UTC') AS linked_at
    FROM {{ ref('company_management_current_build') }} AS current
    LEFT JOIN registry_management AS registry_rows
        ON registry_rows.country_code = current.country_code AND registry_rows.company_id = current.company_id
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
        observations.country_code, observations.company_id, 'descriptions' AS section,
        lower(hex(SHA256(concat(observations.description_kind, '|', observations.source_record_uid)))) AS item_key,
        observations.source_record_uid, 'description_evidence' AS relationship_kind,
        observations.extraction_method AS match_method, observations.confidence AS match_confidence,
        observations.source_run_id, observations.extracted_at AS linked_at
    FROM {{ source('corpscout', 'company_description_observations') }} AS observations FINAL
    WHERE observations.country_code = '{{ var("country_code") }}'
    QUALIFY row_number() OVER (
        PARTITION BY observations.company_id, observations.description_kind, observations.source_record_uid
        ORDER BY observations.extracted_at DESC
    ) = 1
),
addresses AS (
    SELECT
        '{{ var("country_code") }}' AS country_code, company_id, 'addresses' AS section,
        address_fingerprint AS item_key, source_record_uid, 'registry_address' AS relationship_kind,
        'registry_company_record' AS match_method, toFloat32(1) AS match_confidence,
        source_run_id, observed_at AS linked_at
    FROM {{ source('corpscout', 'se_company_addresses_current') }}
    WHERE has_address = 1 AND has_observation = 1 AND source_record_uid != ''
),
industries AS (
    SELECT
        '{{ var("country_code") }}' AS country_code, company_id, 'industries' AS section,
        nace_rev2_class_code AS item_key, source_record_uid, 'registry_industry' AS relationship_kind,
        'registry_company_record' AS match_method, toFloat32(1) AS match_confidence,
        source_run_id, updated_from_raw_at AS linked_at
    FROM {{ source('corpscout', 'se_industries') }} FINAL
    WHERE nace_rev2_class_code != '' AND source_record_uid != ''
),
financials AS (
    SELECT
        metrics.country_iso2 AS country_code, metrics.company_id, 'financials' AS section,
        concat('standalone:', toString(metrics.fiscal_year)) AS item_key, metrics.source_record_uid,
        'financial_statement' AS relationship_kind, 'registry_company_identifier' AS match_method,
        toFloat32(1) AS match_confidence, metrics.source_run_id, metrics.resolved_at AS linked_at
    FROM {{ source('corpscout', 'se_financial_metrics') }} AS metrics FINAL
    INNER JOIN company_anchors AS anchors ON anchors.company_id = metrics.company_id
    WHERE metrics.country_iso2 = '{{ var("country_code") }}' AND metrics.source_record_uid != ''
),
evidence_links AS (
    SELECT * FROM synthetic_links WHERE item_key != ''
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
SELECT DISTINCT
    country_code,
    company_id,
    section,
    CAST(item_key AS String) AS item_key,
    CAST(source_record_uid AS String) AS source_record_uid,
    relationship_kind,
    match_method,
    match_confidence,
    source_run_id,
    linked_at
FROM evidence_links
