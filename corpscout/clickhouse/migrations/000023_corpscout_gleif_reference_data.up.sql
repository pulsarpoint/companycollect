CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.gleif_lei_records
(
    lei String,
    legal_name String,
    legal_name_language Nullable(String),
    entity_status LowCardinality(String),
    registration_status LowCardinality(String),
    jurisdiction Nullable(String),
    category Nullable(String),
    subcategory Nullable(String),
    legal_form_id Nullable(String),
    legal_form_other Nullable(String),
    registered_at_id Nullable(String),
    registered_at_other Nullable(String),
    registered_as Nullable(String),
    associated_entity_lei Nullable(String),
    associated_entity_name Nullable(String),
    successor_entity_lei Nullable(String),
    successor_entity_name Nullable(String),
    creation_date Nullable(DateTime64(3, 'UTC')),
    expiration_date Nullable(DateTime64(3, 'UTC')),
    expiration_reason Nullable(String),
    initial_registration_date Nullable(DateTime64(3, 'UTC')),
    last_update_date Nullable(DateTime64(3, 'UTC')),
    next_renewal_date Nullable(DateTime64(3, 'UTC')),
    managing_lou Nullable(String),
    corroboration_level Nullable(String),
    validated_at_id Nullable(String),
    validated_at_other Nullable(String),
    validated_as Nullable(String),
    conformity_flag Nullable(String),
    legal_address_country Nullable(String),
    headquarters_address_country Nullable(String),
    primary_country_iso2 Nullable(String),
    golden_copy_publish_date Nullable(DateTime64(3, 'UTC')),
    source_system LowCardinality(String),
    source_run_id String,
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei);

CREATE TABLE IF NOT EXISTS corpscout.gleif_lei_names
(
    lei String,
    name_type LowCardinality(String),
    name String,
    name_normalized String,
    language Nullable(String),
    cdf_type Nullable(String),
    sequence UInt32,
    source_system LowCardinality(String),
    source_run_id String,
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei, name_type, name_normalized, sequence);

CREATE TABLE IF NOT EXISTS corpscout.gleif_lei_addresses
(
    lei String,
    address_role LowCardinality(String),
    language Nullable(String),
    address_lines Array(String),
    address_number Nullable(String),
    address_number_within_building Nullable(String),
    mail_routing Nullable(String),
    city Nullable(String),
    region Nullable(String),
    country Nullable(String),
    postal_code Nullable(String),
    normalized_address Nullable(String),
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    source_system LowCardinality(String),
    source_run_id String,
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei, address_role);

CREATE TABLE IF NOT EXISTS corpscout.gleif_lei_identifiers
(
    lei String,
    identifier_type LowCardinality(String),
    identifier_value String,
    identifier_scope Nullable(String),
    mapping_source LowCardinality(String),
    is_primary UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (identifier_type, identifier_value, lei);

CREATE TABLE IF NOT EXISTS corpscout.gleif_lei_relationships
(
    relationship_record_id String,
    start_node_lei String,
    start_node_type Nullable(String),
    end_node_lei String,
    end_node_type Nullable(String),
    relationship_type LowCardinality(String),
    relationship_status LowCardinality(String),
    valid_from Nullable(DateTime64(3, 'UTC')),
    valid_to Nullable(DateTime64(3, 'UTC')),
    initial_registration_date Nullable(DateTime64(3, 'UTC')),
    last_update_date Nullable(DateTime64(3, 'UTC')),
    registration_status Nullable(String),
    next_renewal_date Nullable(DateTime64(3, 'UTC')),
    managing_lou Nullable(String),
    corroboration_level Nullable(String),
    corroboration_documents Nullable(String),
    corroboration_reference Nullable(String),
    deleted_at Nullable(DateTime64(3, 'UTC')),
    source_system LowCardinality(String),
    source_run_id String,
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (start_node_lei, relationship_type, end_node_lei, relationship_record_id);

CREATE TABLE IF NOT EXISTS corpscout.gleif_lei_relationship_periods
(
    relationship_record_id String,
    period_type LowCardinality(String),
    start_date Nullable(Date),
    end_date Nullable(Date),
    source_system LowCardinality(String),
    source_run_id String,
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (relationship_record_id, period_type, ifNull(start_date, toDate('1970-01-01')));

CREATE TABLE IF NOT EXISTS corpscout.gleif_lei_reporting_exceptions
(
    exception_record_id String,
    lei String,
    parent_relationship_type LowCardinality(String),
    exception_category LowCardinality(String),
    exception_reason Nullable(String),
    exception_reference Nullable(String),
    initial_registration_date Nullable(DateTime64(3, 'UTC')),
    last_update_date Nullable(DateTime64(3, 'UTC')),
    registration_status Nullable(String),
    next_renewal_date Nullable(DateTime64(3, 'UTC')),
    managing_lou Nullable(String),
    source_system LowCardinality(String),
    source_run_id String,
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei, parent_relationship_type, exception_category, exception_record_id);

CREATE TABLE IF NOT EXISTS corpscout.gleif_lei_issuers
(
    lei String,
    name String,
    marketing_name Nullable(String),
    website Nullable(String),
    accreditation_date Nullable(DateTime64(3, 'UTC')),
    jurisdictions Array(String),
    fund_jurisdictions Array(String),
    source_system LowCardinality(String),
    source_run_id String,
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei);

CREATE TABLE IF NOT EXISTS corpscout.gleif_code_list_entries
(
    code_list LowCardinality(String),
    code String,
    label String,
    description Nullable(String),
    country_iso2 Nullable(String),
    valid_from Nullable(Date),
    valid_to Nullable(Date),
    source_system LowCardinality(String),
    source_run_id String,
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (code_list, code);
