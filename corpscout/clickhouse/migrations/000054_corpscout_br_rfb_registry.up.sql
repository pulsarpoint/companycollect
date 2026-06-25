CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.br_companies
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    cnpj_basico String,
    headquarters_cnpj String,
    legal_name String,
    trade_name String,
    legal_nature_code LowCardinality(String),
    legal_nature_description_pt String,
    company_size_code LowCardinality(String),
    company_size_en LowCardinality(String),
    share_capital_amount_original Nullable(Decimal(18, 2)),
    status_code LowCardinality(String),
    status_en LowCardinality(String),
    is_active UInt8,
    status_date Nullable(Date),
    activity_start_date Nullable(Date),
    street_type String,
    street_name String,
    street_number String,
    address_complement String,
    district String,
    postal_code LowCardinality(String),
    state LowCardinality(String),
    municipality_code LowCardinality(String),
    municipality_name String,
    is_simples UInt8,
    is_mei UInt8,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj_basico);

CREATE TABLE IF NOT EXISTS corpscout.br_establishments
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    cnpj String,
    cnpj_basico String,
    cnpj_ordem LowCardinality(String),
    cnpj_dv LowCardinality(String),
    is_headquarters UInt8,
    trade_name String,
    status_code LowCardinality(String),
    status_en LowCardinality(String),
    status_date Nullable(Date),
    status_reason_code LowCardinality(String),
    activity_start_date Nullable(Date),
    primary_cnae_code LowCardinality(String),
    secondary_cnae_codes String,
    street_type String,
    street_name String,
    street_number String,
    address_complement String,
    district String,
    postal_code LowCardinality(String),
    state LowCardinality(String),
    municipality_code LowCardinality(String),
    municipality_name String,
    ddd_1 LowCardinality(String),
    telefone_1 String,
    ddd_2 LowCardinality(String),
    telefone_2 String,
    ddd_fax LowCardinality(String),
    fax String,
    correio_eletronico String,
    situacao_especial String,
    data_situacao_especial String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj_basico, cnpj);
