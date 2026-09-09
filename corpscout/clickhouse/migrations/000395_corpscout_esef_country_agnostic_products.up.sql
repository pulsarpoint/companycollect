CREATE DATABASE IF NOT EXISTS corpscout;

-- THE ESEF PRODUCTS STOP CARRYING A COUNTRY OR A COMPANY ID (spec 2026-09-08 revised
-- 2026-09-09, section 1). One link table, register-verified, and per-country views.

-- 1. The link's verification status: register_verified when the jurisdiction has a rule in
--    COUNTRY_IDENTITY_RULES and the id exists in that register, unverified when it does not,
--    gleif when the jurisdiction has no register here. The builder fills it on the next
--    rebuild, existing rows read the default until then.
ALTER TABLE corpscout.esef_entity_registry_map
    ADD COLUMN IF NOT EXISTS link_status LowCardinality(String) DEFAULT 'gleif' AFTER match_source;

-- 2. The three model-output tables carried the stamps as the head of their sorting key.
--    They are pure projections of esef_document_company_information, refilled by their
--    assets on the next run, so each is recreated under the document-keyed shape and the
--    old one parks as _legacy until the owner drops it (operations/esef_stamps_retire.md).
RENAME TABLE corpscout.esef_document_people TO corpscout.esef_document_people_legacy;
CREATE TABLE IF NOT EXISTS corpscout.esef_document_people
(
    candidate_uid FixedString(64),
    source_record_uid FixedString(64),
    source_document_id String,
    lei String,
    fiscal_year UInt16,
    name String,
    role String,
    role_category LowCardinality(String),
    organization String,
    status LowCardinality(String),
    effective_from Nullable(Date32),
    effective_to Nullable(Date32),
    confidence Float32,
    evidence_ids Array(String),
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    source_run_id String,
    extracted_at DateTime64(3, 'UTC'),
    person_profile_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'company-person-profile-v1\n',
            toString(length(lowerUTF8(trim(name)))), ':', lowerUTF8(trim(name)), '\n',
            '0:'
        )))),
    person_role_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'company-person-role-v1\n',
            toString(length(lowerUTF8(trim(role)))), ':', lowerUTF8(trim(role)), '\n',
            toString(length(lowerUTF8(trim(role_category)))), ':',
            lowerUTF8(trim(role_category)), '\n',
            toString(length(lowerUTF8(trim(organization)))), ':',
            lowerUTF8(trim(organization)), '\n',
            toString(length(lowerUTF8(trim(status)))), ':', lowerUTF8(trim(status)), '\n',
            ifNull(toString(effective_from), ''), '\n',
            ifNull(toString(effective_to), ''), '\n',
            toString(fiscal_year)
        ))))
)
ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (lei, fiscal_year, source_record_uid, candidate_uid);

RENAME TABLE corpscout.esef_document_business_items TO corpscout.esef_document_business_items_legacy;
CREATE TABLE IF NOT EXISTS corpscout.esef_document_business_items
(
    candidate_uid FixedString(64),
    source_record_uid FixedString(64),
    source_document_id String,
    lei String,
    fiscal_year UInt16,
    item_kind LowCardinality(String),
    name String,
    geography_type LowCardinality(String),
    confidence Float32,
    evidence_ids Array(String),
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    source_run_id String,
    extracted_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (lei, item_kind, fiscal_year, source_record_uid, candidate_uid);

RENAME TABLE corpscout.esef_document_group_relationships TO corpscout.esef_document_group_relationships_legacy;
CREATE TABLE IF NOT EXISTS corpscout.esef_document_group_relationships
(
    candidate_uid FixedString(64),
    source_record_uid FixedString(64),
    source_document_id String,
    lei String,
    fiscal_year UInt16,
    related_company_name String,
    relationship_type LowCardinality(String),
    ownership_percentage Nullable(Float32),
    jurisdiction String,
    confidence Float32,
    evidence_ids Array(String),
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    source_run_id String,
    extracted_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (lei, relationship_type, fiscal_year, source_record_uid, candidate_uid);

-- 3. One Swedish view per ESEF product (dagster_v3.defs.esef_filings.country_views), joining
--    the product to the register-verified Swedish link and putting the registry id first as
--    company_id. THE SELECTS BELOW ARE NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- each
--    is the exact rendering of build_se_esef_view_sql for its entry in tables.SE_ESEF_VIEWS.
--    Editing this file without editing that module, or the module without adding a
--    migration, trips the drift pin in tests/test_esef_country_views.py.
CREATE OR REPLACE VIEW corpscout.se_esef_filings AS
SELECT
    m.registry_id AS company_id,
    t.lei,
    t.entity_name,
    t.fxo_id,
    t.country,
    t.period_end,
    t.date_added,
    t.processed_at,
    t.json_url,
    t.package_url,
    t.report_url,
    t.viewer_url,
    t.package_sha256,
    t.error_count,
    t.warning_count,
    t.inconsistency_count,
    t.has_json_facts,
    t.source_url,
    t.source_run_id,
    t.resolved_at
FROM corpscout.esef_filings AS t FINAL
INNER JOIN (SELECT lei, registry_id FROM corpscout.esef_entity_registry_map FINAL WHERE country_iso2 = 'SE' AND link_status = 'register_verified') AS m ON m.lei = t.lei;

CREATE OR REPLACE VIEW corpscout.se_esef_facts AS
SELECT
    m.registry_id AS company_id,
    t.lei,
    t.fxo_id,
    t.period_end,
    t.fact_id,
    t.concept_qname,
    t.concept_namespace,
    t.concept_local_name,
    t.period_start,
    t.period_instant,
    t.period_duration_end,
    t.unit,
    t.currency,
    t.value_kind,
    t.raw_value,
    t.amount_original,
    t.decimals,
    t.dimensions,
    t.language,
    t.source_run_id,
    t.processed_week,
    t.resolved_at
FROM corpscout.esef_facts AS t FINAL
INNER JOIN (SELECT lei, registry_id FROM corpscout.esef_entity_registry_map FINAL WHERE country_iso2 = 'SE' AND link_status = 'register_verified') AS m ON m.lei = t.lei;

CREATE OR REPLACE VIEW corpscout.se_esef_disclosures AS
SELECT
    m.registry_id AS company_id,
    t.disclosure_id,
    t.disclosure_kind,
    t.source_document_id,
    t.source_record_uid,
    t.source_fact_id,
    t.source_fact_key,
    t.package_sha256,
    t.artifact_schema_version,
    t.lei,
    t.period_end,
    t.fiscal_year,
    t.concept_qname,
    t.concept_local_name,
    t.language,
    t.segment,
    t.selection_reason,
    t.report_member,
    t.period_json,
    t.section_type,
    t.page_id,
    t.printed_page_number,
    t.anchor_xpath,
    t.anchor_visual_order,
    t.extraction_method,
    t.text_sha256,
    t.parser_name,
    t.parser_version,
    t.blocks_json,
    t.plain_text,
    t.original_character_count,
    t.block_count,
    t.table_count,
    t.source_run_id,
    t.extracted_at,
    t.processed_week,
    t.resolved_at
FROM corpscout.esef_disclosures AS t
INNER JOIN (SELECT lei, registry_id FROM corpscout.esef_entity_registry_map FINAL WHERE country_iso2 = 'SE' AND link_status = 'register_verified') AS m ON m.lei = t.lei;

CREATE OR REPLACE VIEW corpscout.se_esef_document_contact_candidates AS
SELECT
    m.registry_id AS company_id,
    t.candidate_id,
    t.source_document_id,
    t.source_record_uid,
    t.package_sha256,
    t.lei,
    t.period_end,
    t.fiscal_year,
    t.candidate_kind,
    t.normalized_value,
    t.country_code,
    t.registrable_domain,
    t.hosts_json,
    t.normalized_urls_json,
    t.suggested_roles_json,
    t.evidence_json,
    t.evidence_count,
    t.extractor_versions_json,
    t.source_run_id,
    t.extracted_at,
    t.processed_week,
    t.resolved_at
FROM corpscout.esef_document_contact_candidates AS t
INNER JOIN (SELECT lei, registry_id FROM corpscout.esef_entity_registry_map FINAL WHERE country_iso2 = 'SE' AND link_status = 'register_verified') AS m ON m.lei = t.lei;

CREATE OR REPLACE VIEW corpscout.se_esef_document_company_information AS
SELECT
    m.registry_id AS company_id,
    t.source_document_id,
    t.source_record_uid,
    t.package_sha256,
    t.lei,
    t.period_end,
    t.fiscal_year,
    t.extraction_status,
    t.company_description,
    t.description_language,
    t.description_confidence,
    t.description_evidence_ids_json,
    t.people_json,
    t.products_and_services_json,
    t.customer_markets_json,
    t.operating_geographies_json,
    t.business_segments_json,
    t.material_group_relationships_json,
    t.enrichment_artifact_object_key,
    t.input_artifact_object_key,
    t.llm_request_object_key,
    t.llm_request_sha256,
    t.llm_response_text,
    t.llm_response_sha256,
    t.model_provider,
    t.model_name,
    t.prompt_version,
    t.prompt_tokens,
    t.completion_tokens,
    t.input_character_count,
    t.source_run_id,
    t.extracted_at,
    t.resolved_at
FROM corpscout.esef_document_company_information AS t
INNER JOIN (SELECT lei, registry_id FROM corpscout.esef_entity_registry_map FINAL WHERE country_iso2 = 'SE' AND link_status = 'register_verified') AS m ON m.lei = t.lei;

CREATE OR REPLACE VIEW corpscout.se_esef_document_people AS
SELECT
    m.registry_id AS company_id,
    t.candidate_uid,
    t.source_record_uid,
    t.source_document_id,
    t.lei,
    t.fiscal_year,
    t.name,
    t.role,
    t.role_category,
    t.organization,
    t.status,
    t.effective_from,
    t.effective_to,
    t.confidence,
    t.evidence_ids,
    t.model_provider,
    t.model_name,
    t.prompt_version,
    t.source_run_id,
    t.extracted_at,
    t.person_profile_hash,
    t.person_role_hash
FROM corpscout.esef_document_people AS t FINAL
INNER JOIN (SELECT lei, registry_id FROM corpscout.esef_entity_registry_map FINAL WHERE country_iso2 = 'SE' AND link_status = 'register_verified') AS m ON m.lei = t.lei;

CREATE OR REPLACE VIEW corpscout.se_esef_document_business_items AS
SELECT
    m.registry_id AS company_id,
    t.candidate_uid,
    t.source_record_uid,
    t.source_document_id,
    t.lei,
    t.fiscal_year,
    t.item_kind,
    t.name,
    t.geography_type,
    t.confidence,
    t.evidence_ids,
    t.model_provider,
    t.model_name,
    t.prompt_version,
    t.source_run_id,
    t.extracted_at
FROM corpscout.esef_document_business_items AS t FINAL
INNER JOIN (SELECT lei, registry_id FROM corpscout.esef_entity_registry_map FINAL WHERE country_iso2 = 'SE' AND link_status = 'register_verified') AS m ON m.lei = t.lei;

-- The se_company_person_esef read view was dropped by hand in SE person slice 0
-- (2026-09-09) and its DDL left this file per the dev-phase ledger policy. This migration's
-- ESEF products -- esef_document_people_legacy and the country-scoped se_esef_document_people
-- view -- are untouched.

CREATE OR REPLACE VIEW corpscout.se_esef_document_group_relationships AS
SELECT
    m.registry_id AS company_id,
    t.candidate_uid,
    t.source_record_uid,
    t.source_document_id,
    t.lei,
    t.fiscal_year,
    t.related_company_name,
    t.relationship_type,
    t.ownership_percentage,
    t.jurisdiction,
    t.confidence,
    t.evidence_ids,
    t.model_provider,
    t.model_name,
    t.prompt_version,
    t.source_run_id,
    t.extracted_at
FROM corpscout.esef_document_group_relationships AS t FINAL
INNER JOIN (SELECT lei, registry_id FROM corpscout.esef_entity_registry_map FINAL WHERE country_iso2 = 'SE' AND link_status = 'register_verified') AS m ON m.lei = t.lei;

