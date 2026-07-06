CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.br_cgu_ceis_company_sanctions
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    snapshot_date Date32,
    source_dataset LowCardinality(String),
    source_url String,
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    registry LowCardinality(String),
    sanction_id String,
    cnpj String,
    cnpj_basico String,
    person_type LowCardinality(String),
    sanctioned_name String,
    sanctioning_agency_reported_name String,
    receita_legal_name String,
    receita_trade_name String,
    process_number String,
    sanction_category String,
    sanction_start_date Nullable(Date32),
    sanction_end_date Nullable(Date32),
    publication_date Nullable(Date32),
    publication String,
    publication_detail String,
    final_judgment_date Nullable(Date32),
    sanction_scope String,
    sanctioning_agency String,
    sanctioning_agency_state LowCardinality(String),
    sanctioning_agency_sphere LowCardinality(String),
    legal_basis String,
    source_information_date Nullable(Date32),
    information_origin String,
    notes String,
    source_payload_hash String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (snapshot_date, cnpj, sanction_id, process_number);

CREATE TABLE IF NOT EXISTS corpscout.br_cgu_cnep_company_sanctions
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    snapshot_date Date32,
    source_dataset LowCardinality(String),
    source_url String,
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    registry LowCardinality(String),
    sanction_id String,
    cnpj String,
    cnpj_basico String,
    person_type LowCardinality(String),
    sanctioned_name String,
    sanctioning_agency_reported_name String,
    receita_legal_name String,
    receita_trade_name String,
    process_number String,
    sanction_category String,
    fine_amount_brl Nullable(Decimal(38, 6)),
    sanction_start_date Nullable(Date32),
    sanction_end_date Nullable(Date32),
    publication_date Nullable(Date32),
    publication String,
    publication_detail String,
    final_judgment_date Nullable(Date32),
    sanction_scope String,
    sanctioning_agency String,
    sanctioning_agency_state LowCardinality(String),
    sanctioning_agency_sphere LowCardinality(String),
    legal_basis String,
    source_information_date Nullable(Date32),
    information_origin String,
    notes String,
    source_payload_hash String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (snapshot_date, cnpj, sanction_id, process_number);

CREATE TABLE IF NOT EXISTS corpscout.br_cgu_cepim_blocked_entities
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    snapshot_date Date32,
    source_dataset LowCardinality(String),
    source_url String,
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    cnpj String,
    cnpj_basico String,
    entity_name String,
    agreement_number String,
    granting_agency String,
    impediment_reason String,
    source_payload_hash String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (snapshot_date, cnpj, agreement_number);

CREATE TABLE IF NOT EXISTS corpscout.br_cgu_leniency_agreements
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    snapshot_date Date32,
    source_dataset LowCardinality(String),
    source_url String,
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    agreement_id String,
    sanctioned_document_raw String,
    cnpj String,
    cnpj_basico String,
    legal_name String,
    trade_name String,
    agreement_start_date Nullable(Date32),
    agreement_end_date Nullable(Date32),
    agreement_status LowCardinality(String),
    information_date Nullable(Date32),
    process_number String,
    agreement_terms String,
    sanctioning_agency String,
    source_payload_hash String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (snapshot_date, cnpj, agreement_id);

CREATE TABLE IF NOT EXISTS corpscout.br_cgu_leniency_agreement_effects
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    snapshot_date Date32,
    source_dataset LowCardinality(String),
    source_url String,
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    agreement_id String,
    agreement_effect String,
    effect_complement String,
    source_payload_hash String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (snapshot_date, agreement_id, agreement_effect);
