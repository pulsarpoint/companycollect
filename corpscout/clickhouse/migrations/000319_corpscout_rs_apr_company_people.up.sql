CREATE DATABASE IF NOT EXISTS corpscout;

-- Serbia APR paid-source readiness.
--
-- The public company UI was inspected manually for company 21141666 on
-- 2026-08-25. A legal-representative record exposes a person name, function,
-- a masked JMBG control, and whether the person represents independently.
-- SP3/SP4 paid transport fields are not public, so source_record_id,
-- source_person_key, and source_record_uid remain mapper-owned envelope fields.
--
-- Raw JMBG, passport, foreigner, and refugee identifiers MUST NOT be written to
-- ClickHouse. personal_identifier_hmac is a keyed HMAC-SHA256 produced before
-- loading. A plain SHA256 is insufficient for low-entropy identifiers.

CREATE TABLE IF NOT EXISTS corpscout.rs_apr_company_representative_observations
(
    company_id String,
    relationship_uid FixedString(64),
    source_person_key String,
    party_kind LowCardinality(String),
    name String,
    source_relationship_kind LowCardinality(String),
    role_code String,
    function_title Nullable(String),
    represents_independently Nullable(Bool),
    representation_method_raw Nullable(String),
    personal_identifier_kind LowCardinality(Nullable(String)),
    personal_identifier_hmac Nullable(FixedString(64)),
    is_present Bool,

    source_run_id String,
    source_record_id String,
    source_record_uid String,
    state_fingerprint FixedString(64),
    source_effective_from Nullable(Date32),
    source_effective_to Nullable(Date32),
    observed_at DateTime64(3, 'UTC'),

    CONSTRAINT rs_apr_representative_company_id CHECK match(company_id, '^[0-9]{8}$'),
    CONSTRAINT rs_apr_representative_relationship_uid CHECK notEmpty(relationship_uid),
    CONSTRAINT rs_apr_representative_name CHECK trim(name) != '',
    CONSTRAINT rs_apr_representative_source_kind CHECK trim(source_relationship_kind) != '',
    CONSTRAINT rs_apr_representative_role CHECK trim(role_code) != '',
    CONSTRAINT rs_apr_representative_source_record CHECK trim(source_record_uid) != ''
)
ENGINE = MergeTree
PARTITION BY toYear(observed_at)
ORDER BY (company_id, relationship_uid, observed_at, state_fingerprint);

-- Incremental APR web-service deliveries need explicit tombstones. Readers use
-- FINAL and filter is_current. A full backfill may instead rebuild this table
-- through a staging table and EXCHANGE TABLES.
CREATE TABLE IF NOT EXISTS corpscout.rs_apr_company_representatives_current
(
    company_id String,
    relationship_uid FixedString(64),
    source_person_key String,
    party_kind LowCardinality(String),
    name String,
    source_relationship_kind LowCardinality(String),
    role_code String,
    function_title Nullable(String),
    represents_independently Nullable(Bool),
    representation_method_raw Nullable(String),
    personal_identifier_kind LowCardinality(Nullable(String)),
    personal_identifier_hmac Nullable(FixedString(64)),
    is_current Bool,

    source_run_id String,
    source_record_id String,
    source_record_uid String,
    state_fingerprint FixedString(64),
    source_effective_from Nullable(Date32),
    source_effective_to Nullable(Date32),
    observed_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC'),

    CONSTRAINT rs_apr_representative_current_company_id CHECK match(company_id, '^[0-9]{8}$'),
    CONSTRAINT rs_apr_representative_current_name CHECK trim(name) != '',
    CONSTRAINT rs_apr_representative_current_source_record CHECK trim(source_record_uid) != ''
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (company_id, relationship_uid);

-- Beneficial ownership is a separate APR Central Register source, not SP3/SP4.
-- The field set follows the 2025 Act and APR's current public documentation.
-- No real beneficial-owner record was copied: the CEV portal requires eID login.
CREATE TABLE IF NOT EXISTS corpscout.rs_apr_company_beneficial_owner_observations
(
    company_id String,
    owner_uid FixedString(64),
    source_person_key String,
    person_kind LowCardinality(String),
    name String,
    personal_identifier_kind LowCardinality(Nullable(String)),
    personal_identifier_hmac Nullable(FixedString(64)),
    personal_identifier_issuing_country_code LowCardinality(Nullable(String)),
    birth_date Nullable(Date32),
    birth_place Nullable(String),
    birth_country_code LowCardinality(Nullable(String)),
    residence_country_code LowCardinality(Nullable(String)),
    stay_country_code LowCardinality(Nullable(String)),
    citizenship_country_codes Array(String),

    basis_code LowCardinality(String),
    basis_label_raw Nullable(String),
    ownership_percentage Nullable(Decimal(5, 2)),
    voting_rights_percentage Nullable(Decimal(5, 2)),
    acquired_on Nullable(Date32),
    registered_on Nullable(Date32),
    documents_registered_on Nullable(Date32),
    has_supporting_documents Bool,
    supporting_document_count UInt16,
    has_discrepancy Bool,
    discrepancy_note Nullable(String),

    trust_legal_form Nullable(String),
    trust_name Nullable(String),
    trust_registered_address Nullable(String),
    trust_identifier_kind LowCardinality(Nullable(String)),
    trust_identifier_value Nullable(String),
    trust_origin_country_code LowCardinality(Nullable(String)),
    trust_relationship_kind LowCardinality(Nullable(String)),
    is_present Bool,

    source_run_id String,
    source_record_id String,
    source_record_uid String,
    state_fingerprint FixedString(64),
    observed_at DateTime64(3, 'UTC'),

    CONSTRAINT rs_apr_beneficial_owner_company_id CHECK match(company_id, '^[0-9]{8}$'),
    CONSTRAINT rs_apr_beneficial_owner_uid CHECK notEmpty(owner_uid),
    CONSTRAINT rs_apr_beneficial_owner_name CHECK trim(name) != '',
    CONSTRAINT rs_apr_beneficial_owner_basis CHECK trim(basis_code) != '',
    CONSTRAINT rs_apr_beneficial_owner_percentage CHECK ownership_percentage IS NULL OR (ownership_percentage >= 0 AND ownership_percentage <= 100),
    CONSTRAINT rs_apr_beneficial_owner_voting CHECK voting_rights_percentage IS NULL OR (voting_rights_percentage >= 0 AND voting_rights_percentage <= 100),
    CONSTRAINT rs_apr_beneficial_owner_source_record CHECK trim(source_record_uid) != ''
)
ENGINE = MergeTree
PARTITION BY toYear(observed_at)
ORDER BY (company_id, owner_uid, observed_at, state_fingerprint);

CREATE TABLE IF NOT EXISTS corpscout.rs_apr_company_beneficial_owners_current
(
    company_id String,
    owner_uid FixedString(64),
    source_person_key String,
    person_kind LowCardinality(String),
    name String,
    personal_identifier_kind LowCardinality(Nullable(String)),
    personal_identifier_hmac Nullable(FixedString(64)),
    personal_identifier_issuing_country_code LowCardinality(Nullable(String)),
    birth_date Nullable(Date32),
    birth_place Nullable(String),
    birth_country_code LowCardinality(Nullable(String)),
    residence_country_code LowCardinality(Nullable(String)),
    stay_country_code LowCardinality(Nullable(String)),
    citizenship_country_codes Array(String),

    basis_code LowCardinality(String),
    basis_label_raw Nullable(String),
    ownership_percentage Nullable(Decimal(5, 2)),
    voting_rights_percentage Nullable(Decimal(5, 2)),
    acquired_on Nullable(Date32),
    registered_on Nullable(Date32),
    documents_registered_on Nullable(Date32),
    has_supporting_documents Bool,
    supporting_document_count UInt16,
    has_discrepancy Bool,
    discrepancy_note Nullable(String),

    trust_legal_form Nullable(String),
    trust_name Nullable(String),
    trust_registered_address Nullable(String),
    trust_identifier_kind LowCardinality(Nullable(String)),
    trust_identifier_value Nullable(String),
    trust_origin_country_code LowCardinality(Nullable(String)),
    trust_relationship_kind LowCardinality(Nullable(String)),
    is_current Bool,

    source_run_id String,
    source_record_id String,
    source_record_uid String,
    state_fingerprint FixedString(64),
    observed_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC'),

    CONSTRAINT rs_apr_beneficial_owner_current_company_id CHECK match(company_id, '^[0-9]{8}$'),
    CONSTRAINT rs_apr_beneficial_owner_current_name CHECK trim(name) != '',
    CONSTRAINT rs_apr_beneficial_owner_current_basis CHECK trim(basis_code) != '',
    CONSTRAINT rs_apr_beneficial_owner_current_percentage CHECK ownership_percentage IS NULL OR (ownership_percentage >= 0 AND ownership_percentage <= 100),
    CONSTRAINT rs_apr_beneficial_owner_current_voting CHECK voting_rights_percentage IS NULL OR (voting_rights_percentage >= 0 AND voting_rights_percentage <= 100),
    CONSTRAINT rs_apr_beneficial_owner_current_source_record CHECK trim(source_record_uid) != ''
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (company_id, owner_uid);

-- Canonical roles needed when the country-specific source rows are later fed
-- through the shared company-person normalization pipeline.
-- INSERT ... SELECT is required by the ClickHouse golang-migrate driver. It
-- rejects multi-row INSERT ... VALUES statements when applying migrations.
INSERT INTO corpscout.company_person_role_type
(
    role_code,
    display_name,
    role_group,
    description,
    is_active,
    created_at,
    updated_at
)
SELECT
    role_code,
    display_name,
    role_group,
    description,
    is_active,
    toDateTime64(seed_at, 3, 'UTC') AS created_at,
    toDateTime64(seed_at, 3, 'UTC') AS updated_at
FROM VALUES(
    'role_code String, display_name String, role_group String, description String, is_active UInt8, seed_at String',
    ('legal_representative', 'Legal representative', 'representation', 'Person or entity legally registered to represent a company.', 1, '2026-08-25 00:00:00'),
    ('other_representative', 'Other representative', 'representation', 'Other registered company representative.', 1, '2026-08-25 00:00:00'),
    ('director', 'Director', 'governance', 'Person registered as a director.', 1, '2026-08-25 00:00:00'),
    ('supervisory_board_member', 'Supervisory board member', 'governance', 'Member of a company supervisory board.', 1, '2026-08-25 00:00:00'),
    ('executive_board_member', 'Executive board member', 'governance', 'Member of a company executive board.', 1, '2026-08-25 00:00:00'),
    ('management_board_member', 'Management board member', 'governance', 'Member of a company management board.', 1, '2026-08-25 00:00:00'),
    ('procurist', 'Procurist', 'representation', 'Holder of an individual commercial procuration.', 1, '2026-08-25 00:00:00'),
    ('group_procurist', 'Group procurist', 'representation', 'Holder of a joint or group commercial procuration.', 1, '2026-08-25 00:00:00'),
    ('beneficial_owner', 'Beneficial owner', 'ownership', 'Natural person registered as a beneficial owner.', 1, '2026-08-25 00:00:00')
);
