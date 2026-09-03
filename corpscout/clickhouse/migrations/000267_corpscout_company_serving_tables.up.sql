CREATE DATABASE IF NOT EXISTS corpscout;

-- company_domain_observations removed on 2026-09-03: unused, dropped by hand (development-phase ledger policy).

-- Application-serving snapshots. Source tables remain source-oriented. Every
-- table below is ordered by the company key used by /company/:country/:id.
-- Current partitions are replaced atomically by the company_serving publisher,
-- so request queries never need FINAL.

CREATE TABLE IF NOT EXISTS corpscout.company_external_identifier_current
(
    country_code LowCardinality(String),
    company_id String,
    identifier_scheme LowCardinality(String),
    identifier_value String,
    is_primary UInt8,
    match_method LowCardinality(String),
    match_confidence Float32,
    first_seen_date Nullable(Date),
    last_seen_date Nullable(Date),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (
    country_code,
    company_id,
    identifier_scheme,
    is_primary,
    identifier_value
);

CREATE TABLE IF NOT EXISTS corpscout.company_gleif_current
(
    country_code LowCardinality(String),
    company_id String,
    lei String,
    is_primary UInt8,
    legal_name String,
    entity_status LowCardinality(String),
    registration_status LowCardinality(String),
    category LowCardinality(String),
    legal_form_id String,
    jurisdiction String,
    legal_address_country LowCardinality(String),
    headquarters_country LowCardinality(String),
    headquarters_abroad UInt8,
    ownership_exception_reasons Array(String),
    initial_registration_date Nullable(DateTime64(3, 'UTC')),
    last_update_date Nullable(DateTime64(3, 'UTC')),
    next_renewal_date Nullable(DateTime64(3, 'UTC')),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (country_code, company_id, is_primary, lei);

CREATE TABLE IF NOT EXISTS corpscout.company_gleif_relationship_current
(
    country_code LowCardinality(String),
    company_id String,
    relationship_id String,
    direction LowCardinality(String),
    relationship_type LowCardinality(String),
    other_lei String,
    other_country_code LowCardinality(Nullable(String)),
    other_company_id Nullable(String),
    other_name String,
    relationship_status LowCardinality(String),
    valid_from Nullable(DateTime64(3, 'UTC')),
    valid_to Nullable(DateTime64(3, 'UTC')),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (
    country_code,
    company_id,
    direction,
    relationship_type,
    relationship_id
);

CREATE TABLE IF NOT EXISTS corpscout.company_wikidata_current
(
    country_code LowCardinality(String),
    company_id String,
    wikidata_id String,
    is_primary UInt8,
    wikidata_url String,
    description String,
    official_name String,
    inception_date Nullable(Date),
    employee_count Nullable(UInt64),
    employee_count_as_of Nullable(Date),
    industry_label String,
    legal_form_label String,
    headquarters String,
    headquarters_country String,
    logo_url String,
    has_current_listing UInt8,
    listings Array(String),
    websites Array(String),
    linkedin_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (country_code, company_id, is_primary, wikidata_id);

CREATE TABLE IF NOT EXISTS corpscout.company_management_current
(
    country_code LowCardinality(String),
    company_id String,
    management_id FixedString(64),
    person_id Nullable(UUID),
    external_person_scheme LowCardinality(String),
    external_person_value String,
    display_name String,
    first_name String,
    last_name String,
    person_description String,
    birth_year Nullable(UInt16),
    image_url String,
    external_url String,
    role_kind LowCardinality(String),
    role_label String,
    signatory_kind LowCardinality(String),
    start_date Nullable(Date),
    end_date Nullable(Date),
    latest_fiscal_year Nullable(UInt16),
    is_current UInt8,
    confidence Float32,
    source_systems Array(String),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (country_code, company_id, is_current, role_kind, management_id);

CREATE TABLE IF NOT EXISTS corpscout.company_description_current
(
    country_code LowCardinality(String),
    company_id String,
    description_id FixedString(64),
    description_kind LowCardinality(String),
    text_original String,
    language_original LowCardinality(String),
    text_en Nullable(String),
    source_date Nullable(Date),
    extraction_method LowCardinality(String),
    confidence Float32,
    extracted_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (
    country_code,
    company_id,
    description_kind,
    description_id
);

CREATE TABLE IF NOT EXISTS corpscout.company_contact_current
(
    country_code LowCardinality(String),
    company_id String,
    contact_id String,
    contact_type LowCardinality(String),
    contact_value String,
    registrable_domain String,
    fiscal_year Nullable(UInt16),
    confidence Float32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (country_code, company_id, contact_type, contact_id);

CREATE TABLE IF NOT EXISTS corpscout.company_domain_current
(
    country_code LowCardinality(String),
    company_id String,
    root_domain String,
    website_url String,
    website_host String,
    is_primary UInt8,
    match_method LowCardinality(String),
    confidence Float32,
    first_seen_at DateTime64(3, 'UTC'),
    last_seen_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (country_code, company_id, is_primary, root_domain);

CREATE TABLE IF NOT EXISTS corpscout.company_contract_current
(
    country_code LowCardinality(String),
    company_id String,
    contract_ref String,
    source LowCardinality(String),
    notice_ref String,
    contract_date Nullable(Date),
    buyer_name String,
    title String,
    agreement_type String,
    cpv_code String,
    supplier_count UInt32,
    amount_original Nullable(Float64),
    amount_usd Nullable(Float64),
    currency String,
    notice_amount_original Nullable(Float64),
    notice_amount_usd Nullable(Float64),
    notice_currency String,
    source_url String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (
    country_code,
    company_id,
    ifNull(contract_date, toDate('1970-01-01')),
    contract_ref
);

CREATE TABLE IF NOT EXISTS corpscout.company_contract_summary_current
(
    country_code LowCardinality(String),
    company_id String,
    contract_count UInt32,
    last_contract_date Nullable(Date),
    total_attributable_value_usd Nullable(Float64),
    valued_contract_count UInt32,
    source_systems Array(String),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (country_code, company_id);

CREATE TABLE IF NOT EXISTS corpscout.se_company_industry_display_current
(
    company_id String,
    classification_system LowCardinality(String),
    classification_code String,
    classification_level UInt8,
    label_sv String,
    label_en String,
    is_primary UInt8,
    source LowCardinality(String),
    source_record_uid String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (
    company_id,
    is_primary,
    classification_system,
    classification_code
);

CREATE TABLE IF NOT EXISTS corpscout.se_company_address_display_current
(
    company_id String,
    address_key FixedString(64),
    address_type LowCardinality(String),
    source LowCardinality(String),
    raw_address String,
    display_address String,
    normalized_address String,
    street_address String,
    care_of String,
    postal_code String,
    post_town String,
    resolved_country_code LowCardinality(String),
    is_foreign UInt8,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    geocode_status LowCardinality(String),
    geocode_provider LowCardinality(String),
    geocode_precision LowCardinality(String),
    geocoded_at Nullable(DateTime64(3, 'UTC')),
    source_record_uid String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, address_type, source, address_key);

CREATE TABLE IF NOT EXISTS corpscout.company_section_item_source_links
(
    country_code LowCardinality(String),
    company_id String,
    section LowCardinality(String),
    item_key String,
    source_record_uid FixedString(64),
    relationship_kind LowCardinality(String),
    match_method LowCardinality(String),
    match_confidence Float32,
    source_run_id String,
    linked_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (
    country_code,
    company_id,
    section,
    item_key,
    source_record_uid
);

CREATE TABLE IF NOT EXISTS corpscout.company_section_presence_current
(
    country_code LowCardinality(String),
    company_id String,
    section LowCardinality(String),
    item_count UInt32,
    latest_observed_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (country_code, company_id, section);

-- Typed history exists only for source domains whose upstream ClickHouse table
-- otherwise replaces snapshots. Existing address, industry, description,
-- financial, and contract observation/fact tables remain the history source.

CREATE TABLE IF NOT EXISTS corpscout.company_external_identifier_observations
(
    country_code LowCardinality(String),
    company_id String,
    identifier_scheme LowCardinality(String),
    identifier_value String,
    is_primary UInt8,
    match_method LowCardinality(String),
    match_confidence Float32,
    first_seen_date Nullable(Date),
    last_seen_date Nullable(Date),
    resolved_at DateTime64(3, 'UTC'),
    state_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    has_observation UInt8,
    source_run_id String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY (country_code, toYear(observed_at))
ORDER BY (
    country_code,
    company_id,
    identifier_scheme,
    identifier_value,
    observed_at,
    observation_fingerprint
);

CREATE TABLE IF NOT EXISTS corpscout.company_gleif_observations
(
    country_code LowCardinality(String),
    company_id String,
    lei String,
    is_primary UInt8,
    legal_name String,
    entity_status LowCardinality(String),
    registration_status LowCardinality(String),
    category LowCardinality(String),
    legal_form_id String,
    jurisdiction String,
    legal_address_country LowCardinality(String),
    headquarters_country LowCardinality(String),
    headquarters_abroad UInt8,
    ownership_exception_reasons Array(String),
    initial_registration_date Nullable(DateTime64(3, 'UTC')),
    last_update_date Nullable(DateTime64(3, 'UTC')),
    next_renewal_date Nullable(DateTime64(3, 'UTC')),
    resolved_at DateTime64(3, 'UTC'),
    state_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    has_observation UInt8,
    source_run_id String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY (country_code, toYear(observed_at))
ORDER BY (country_code, company_id, lei, observed_at, observation_fingerprint);

CREATE TABLE IF NOT EXISTS corpscout.company_gleif_relationship_observations
(
    country_code LowCardinality(String),
    company_id String,
    relationship_id String,
    direction LowCardinality(String),
    relationship_type LowCardinality(String),
    other_lei String,
    other_country_code LowCardinality(Nullable(String)),
    other_company_id Nullable(String),
    other_name String,
    relationship_status LowCardinality(String),
    valid_from Nullable(DateTime64(3, 'UTC')),
    valid_to Nullable(DateTime64(3, 'UTC')),
    resolved_at DateTime64(3, 'UTC'),
    state_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    has_observation UInt8,
    source_run_id String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY (country_code, toYear(observed_at))
ORDER BY (
    country_code,
    company_id,
    relationship_id,
    observed_at,
    observation_fingerprint
);

CREATE TABLE IF NOT EXISTS corpscout.company_wikidata_observations
(
    country_code LowCardinality(String),
    company_id String,
    wikidata_id String,
    is_primary UInt8,
    wikidata_url String,
    description String,
    official_name String,
    inception_date Nullable(Date),
    employee_count Nullable(UInt64),
    employee_count_as_of Nullable(Date),
    industry_label String,
    legal_form_label String,
    headquarters String,
    headquarters_country String,
    logo_url String,
    has_current_listing UInt8,
    listings Array(String),
    websites Array(String),
    linkedin_id String,
    resolved_at DateTime64(3, 'UTC'),
    state_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    has_observation UInt8,
    source_run_id String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY (country_code, toYear(observed_at))
ORDER BY (
    country_code,
    company_id,
    wikidata_id,
    observed_at,
    observation_fingerprint
);

CREATE TABLE IF NOT EXISTS corpscout.company_management_observations
(
    country_code LowCardinality(String),
    company_id String,
    management_id FixedString(64),
    person_id Nullable(UUID),
    external_person_scheme LowCardinality(String),
    external_person_value String,
    display_name String,
    first_name String,
    last_name String,
    person_description String,
    birth_year Nullable(UInt16),
    image_url String,
    external_url String,
    role_kind LowCardinality(String),
    role_label String,
    signatory_kind LowCardinality(String),
    start_date Nullable(Date),
    end_date Nullable(Date),
    latest_fiscal_year Nullable(UInt16),
    is_current UInt8,
    confidence Float32,
    source_systems Array(String),
    resolved_at DateTime64(3, 'UTC'),
    state_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    has_observation UInt8,
    source_run_id String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY (country_code, toYear(observed_at))
ORDER BY (
    country_code,
    company_id,
    management_id,
    observed_at,
    observation_fingerprint
);
