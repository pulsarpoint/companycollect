CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.br_pgfn_company_debts
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    snapshot_year UInt16,
    snapshot_quarter UInt8,
    snapshot_month LowCardinality(String),
    snapshot_reference_date Date32,
    source_system LowCardinality(String),
    source_url String,
    source_archive_key String,
    source_file_name String,
    source_row_number UInt64,
    cnpj String,
    cnpj_basico String,
    person_type LowCardinality(String),
    debtor_role String,
    debtor_name String,
    debtor_state LowCardinality(String),
    responsible_unit String,
    responsible_entity String,
    inscription_unit String,
    inscription_number String,
    inscription_situation_type String,
    inscription_situation String,
    main_revenue String,
    inscription_date Nullable(Date32),
    is_lawsuit Nullable(Bool),
    consolidated_amount_brl Nullable(Decimal(38, 6)),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (
    snapshot_year,
    snapshot_quarter,
    source_system,
    cnpj,
    inscription_number,
    debtor_role
);
