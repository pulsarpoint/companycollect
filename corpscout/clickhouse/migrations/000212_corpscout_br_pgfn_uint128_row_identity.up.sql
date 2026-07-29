CREATE DATABASE IF NOT EXISTS corpscout;

-- PGFN publishes quarterly snapshots. The previous sort key treated
-- (snapshot, source system, CNPJ, inscription, debtor role) as one row, but
-- 221,696 observed rows in 2021-Q2/Q4 share that key while belonging to
-- different responsible units and carrying different amounts. ReplacingMergeTree
-- could therefore discard valid source rows during background merges.
--
-- source_record_id now identifies the physical source row from its immutable
-- snapshot/file position. UInt128 keeps collision risk negligible without
-- storing a 64-character SHA-256 string. responsible_unit defines the observed
-- business grain, while source_record_id preserves any remaining distinct
-- physical rows at that grain.
DROP TABLE IF EXISTS corpscout.br_pgfn_company_debts__uint128_row_identity;

CREATE TABLE IF NOT EXISTS corpscout.br_pgfn_company_debts__uint128_row_identity
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id UInt128,
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
    debtor_role,
    responsible_unit,
    source_record_id
);

INSERT INTO corpscout.br_pgfn_company_debts__uint128_row_identity
(
    country_iso2,
    source_slug,
    source_run_id,
    source_record_id,
    snapshot_year,
    snapshot_quarter,
    snapshot_month,
    snapshot_reference_date,
    source_system,
    source_url,
    source_archive_key,
    source_file_name,
    source_row_number,
    cnpj,
    person_type,
    debtor_role,
    debtor_name,
    debtor_state,
    responsible_unit,
    responsible_entity,
    inscription_unit,
    inscription_number,
    inscription_situation_type,
    inscription_situation,
    main_revenue,
    inscription_date,
    is_lawsuit,
    consolidated_amount_brl,
    resolved_at
)
SELECT
    country_iso2,
    source_slug,
    source_run_id,
    reinterpretAsUInt128(MD5(concat(
        toString(snapshot_year),
        '-Q',
        toString(snapshot_quarter),
        '|',
        source_system,
        '|',
        source_file_name,
        '|',
        toString(source_row_number)
    ))) AS source_record_id,
    snapshot_year,
    snapshot_quarter,
    snapshot_month,
    snapshot_reference_date,
    source_system,
    source_url,
    source_archive_key,
    source_file_name,
    source_row_number,
    cnpj,
    person_type,
    debtor_role,
    debtor_name,
    debtor_state,
    responsible_unit,
    responsible_entity,
    inscription_unit,
    inscription_number,
    inscription_situation_type,
    inscription_situation,
    main_revenue,
    inscription_date,
    is_lawsuit,
    consolidated_amount_brl,
    resolved_at
FROM corpscout.br_pgfn_company_debts;

EXCHANGE TABLES corpscout.br_pgfn_company_debts__uint128_row_identity AND corpscout.br_pgfn_company_debts;

DROP TABLE IF EXISTS corpscout.br_pgfn_company_debts__uint128_row_identity;
