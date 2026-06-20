CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.fi_websites
    ADD COLUMN IF NOT EXISTS root_domain Nullable(String) AFTER website_host;

CREATE TABLE IF NOT EXISTS corpscout.no_companies
(
    org_number String,
    country_iso2 LowCardinality(String),
    name String,
    name_normalized String,
    registration_date Nullable(Date),
    incorporation_date Nullable(Date),
    lifecycle_status LowCardinality(String),
    is_active UInt8,
    legal_form_code Nullable(String),
    legal_form_description_original Nullable(String),
    legal_form_description_language Nullable(String),
    legal_form_description_en Nullable(String),
    legal_form_description_translated_at Nullable(DateTime64(3, 'UTC')),
    legal_form_description_translation_provider Nullable(String),
    legal_form_description_translation_model Nullable(String),
    primary_website_url Nullable(String),
    primary_website_host Nullable(String),
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (org_number);

CREATE TABLE IF NOT EXISTS corpscout.no_websites
(
    org_number String,
    website_url String,
    website_normalized_url String,
    website_host String,
    root_domain String,
    website_path Nullable(String),
    registered_on Nullable(Date),
    ended_on Nullable(Date),
    is_current UInt8,
    is_primary UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (org_number, website_normalized_url);

CREATE TABLE IF NOT EXISTS corpscout.no_industries
(
    org_number String,
    source_industry_code String,
    source_industry_code_set LowCardinality(String),
    description_original Nullable(String),
    description_language LowCardinality(String),
    description_en Nullable(String),
    description_translated_at Nullable(DateTime64(3, 'UTC')),
    description_translation_provider Nullable(String),
    description_translation_model Nullable(String),
    nace_revision LowCardinality(String),
    nace_code String,
    nace_normalized_code String,
    nace_mapping_method LowCardinality(String),
    nace_mapping_status LowCardinality(String),
    is_primary UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (org_number, source_industry_code);

CREATE TABLE IF NOT EXISTS corpscout.no_financial_statements
(
    country_iso2 LowCardinality(String),
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    org_number String,
    legal_name String,
    last_submitted_accounts_year Nullable(String),
    filing_id Nullable(Int64),
    journal_number Nullable(String),
    accounts_type LowCardinality(String),
    legal_form_code Nullable(String),
    is_parent_company UInt8,
    period_start_date Nullable(Date),
    period_end_date Nullable(Date),
    fiscal_year Nullable(Int64),
    currency LowCardinality(String),
    liquidation_accounts UInt8,
    statement_layout Nullable(String),
    is_not_audited UInt8,
    opted_out_audit UInt8,
    is_small_enterprise UInt8,
    accounting_rules Nullable(String),
    operating_revenue_amount_original Nullable(Decimal(38, 6)),
    operating_revenue_amount_usd Nullable(Decimal(38, 6)),
    operating_costs_amount_original Nullable(Decimal(38, 6)),
    operating_costs_amount_usd Nullable(Decimal(38, 6)),
    operating_result_amount_original Nullable(Decimal(38, 6)),
    operating_result_amount_usd Nullable(Decimal(38, 6)),
    net_financial_items_amount_original Nullable(Decimal(38, 6)),
    net_financial_items_amount_usd Nullable(Decimal(38, 6)),
    pretax_result_amount_original Nullable(Decimal(38, 6)),
    pretax_result_amount_usd Nullable(Decimal(38, 6)),
    net_result_amount_original Nullable(Decimal(38, 6)),
    net_result_amount_usd Nullable(Decimal(38, 6)),
    total_assets_amount_original Nullable(Decimal(38, 6)),
    total_assets_amount_usd Nullable(Decimal(38, 6)),
    current_assets_amount_original Nullable(Decimal(38, 6)),
    current_assets_amount_usd Nullable(Decimal(38, 6)),
    fixed_assets_amount_original Nullable(Decimal(38, 6)),
    fixed_assets_amount_usd Nullable(Decimal(38, 6)),
    equity_amount_original Nullable(Decimal(38, 6)),
    equity_amount_usd Nullable(Decimal(38, 6)),
    total_debt_amount_original Nullable(Decimal(38, 6)),
    total_debt_amount_usd Nullable(Decimal(38, 6)),
    current_liabilities_amount_original Nullable(Decimal(38, 6)),
    current_liabilities_amount_usd Nullable(Decimal(38, 6)),
    long_term_liabilities_amount_original Nullable(Decimal(38, 6)),
    long_term_liabilities_amount_usd Nullable(Decimal(38, 6)),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_source Nullable(String),
    source_url String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (org_number, ifNull(fiscal_year, 0), accounts_type, source_record_id);

CREATE TABLE IF NOT EXISTS corpscout.company_website_domains
(
    source_website_table LowCardinality(String),
    source_website_id String,
    country_iso2 Nullable(String),
    source_slug LowCardinality(String),
    company_id_type LowCardinality(String),
    company_id String,
    website_url String,
    website_normalized_url String,
    website_host String,
    root_domain String,
    is_current UInt8,
    is_primary UInt8,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, source_slug, company_id_type, company_id, source_website_table, source_website_id);

CREATE TABLE IF NOT EXISTS corpscout.domains
(
    root_domain String,
    company_count UInt64,
    website_count UInt64,
    source_slug_count UInt64,
    country_count UInt64,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY root_domain;
