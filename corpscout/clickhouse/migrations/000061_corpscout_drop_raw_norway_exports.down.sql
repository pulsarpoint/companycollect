CREATE DATABASE IF NOT EXISTS corpscout;

-- Recreate corpscout.companies in its post-000058 schema (free-text _en columns dropped).
DROP TABLE IF EXISTS corpscout.companies;
CREATE TABLE IF NOT EXISTS corpscout.companies
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_line_number UInt64,
    source_record_id String,
    source_payload_hash FixedString(64),
    org_number String,
    vat_id String,
    legal_name String,
    legal_form_code String,
    legal_form_description_original String,
    legal_form_description_en String,
    registration_date String,
    incorporation_date String,
    website String,
    phone String,
    nace1_code String,
    nace1_description_original String,
    nace1_description_en String,
    nace2_code String,
    nace2_description_original String,
    nace2_description_en String,
    nace3_code String,
    nace3_description_original String,
    nace3_description_en String,
    articles_purpose_original String,
    activity_text_original String,
    company_description_original String,
    employee_count Nullable(Int64),
    has_registered_employee_count UInt8,
    business_address_lines String,
    business_postal_code String,
    business_city String,
    business_municipality String,
    business_municipality_code String,
    business_country_code String,
    is_vat_registered UInt8,
    is_enterprise_register_registered UInt8,
    is_group_member UInt8,
    parent_org_number String,
    last_submitted_accounts_year String,
    status LowCardinality(String),
    is_active UInt8,
    source_url String,
    raw_entity String
)
ENGINE = ReplacingMergeTree
ORDER BY (org_number);

-- Recreate corpscout.financial_statements (mirror of 000004).
DROP TABLE IF EXISTS corpscout.financial_statements;
CREATE TABLE IF NOT EXISTS corpscout.financial_statements
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_line_number UInt64,
    source_record_id String,
    source_payload_hash FixedString(64),
    org_number String,
    legal_name String,
    website String,
    last_submitted_accounts_year String,
    filing_id Nullable(Int64),
    journal_number String,
    accounts_type LowCardinality(String),
    legal_form_code String,
    is_parent_company UInt8,
    period_start_date Nullable(Date),
    period_end_date Nullable(Date),
    fiscal_year Nullable(Int64),
    currency LowCardinality(String),
    liquidation_accounts UInt8,
    statement_layout String,
    is_not_audited UInt8,
    opted_out_audit UInt8,
    is_small_enterprise UInt8,
    accounting_rules String,
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
    fx_source String,
    source_url String,
    raw_financial_record String
)
ENGINE = ReplacingMergeTree
ORDER BY (org_number, accounts_type, source_record_id);

-- Recreate the translated view in its post-000058 form (c.* without EXCEPT).
CREATE OR REPLACE VIEW corpscout.norway_companies_translated AS
SELECT
    c.*,
    ifNull(ap.translated_text, '') AS articles_purpose_en,
    ifNull(act.translated_text, '') AS activity_text_en,
    ifNull(cd.translated_text, '') AS company_description_en
FROM corpscout.companies AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'articles_purpose'
    GROUP BY source_text_hash
) AS ap ON ap.source_text_hash = cityHash64(c.articles_purpose_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'activity_text'
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(c.activity_text_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'company_description'
    GROUP BY source_text_hash
) AS cd ON cd.source_text_hash = cityHash64(c.company_description_original);
