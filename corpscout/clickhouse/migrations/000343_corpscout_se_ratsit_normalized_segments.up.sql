CREATE DATABASE IF NOT EXISTS corpscout;

-- Content-addressed normalized Ratsit report JSON. Scan observations remain in
-- se_company_ratsit. These tables store one copy of each successful report per
-- normalizer version, even when later scans reuse the same S3 object.
--
-- The company row is written after every child segment and acts as the report's
-- completion marker. Its counts must match the inserted child rows.
CREATE TABLE IF NOT EXISTS corpscout.se_ratsit_company
(
    company_id String CODEC(ZSTD(3)),
    result_sha256 FixedString(64),
    normalizer_version LowCardinality(String),
    schema_version UInt16,
    parser_version LowCardinality(String),
    requested_url String CODEC(ZSTD(3)),
    source_url String CODEC(ZSTD(3)),
    result_bucket LowCardinality(String),
    result_object_key String CODEC(ZSTD(3)),
    name String CODEC(ZSTD(3)),
    organization_number String CODEC(ZSTD(3)),
    legal_form Nullable(String) CODEC(ZSTD(3)),
    status Nullable(String) CODEC(ZSTD(3)),
    address_street Nullable(String) CODEC(ZSTD(3)),
    address_postal_code Nullable(String) CODEC(ZSTD(3)),
    address_locality Nullable(String) CODEC(ZSTD(3)),
    address_county Nullable(String) CODEC(ZSTD(3)),
    business_description Nullable(String) CODEC(ZSTD(3)),
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    source_date_modified Nullable(Date32),
    industry_code_count UInt16,
    summary_count UInt16,
    responsible_people_count UInt16,
    establishment_count UInt16,
    financial_report_count UInt16,
    financial_period_count UInt16,
    people_at_address_count UInt16,
    normalized_at DateTime64(6, 'UTC'),

    CONSTRAINT se_ratsit_company_company_id CHECK
        match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT se_ratsit_company_result_hash CHECK
        match(toString(result_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_ratsit_company_normalizer CHECK normalizer_version != '',
    CONSTRAINT se_ratsit_company_versions CHECK
        schema_version > 0 AND parser_version != '',
    CONSTRAINT se_ratsit_company_name CHECK trim(name) != '',
    CONSTRAINT se_ratsit_company_organization_number CHECK
        organization_number = right(company_id, 10),
    CONSTRAINT se_ratsit_company_requested_url CHECK
        requested_url = concat('https://www.ratsit.se/', right(company_id, 10)),
    CONSTRAINT se_ratsit_company_source_url CHECK
        startsWith(source_url, 'https://www.ratsit.se/'),
    CONSTRAINT se_ratsit_company_result_location CHECK
        result_bucket != ''
        AND startsWith(
            result_object_key,
            concat('sweden_ratsit/pilot/company_id=', company_id, '/')
        ),
    CONSTRAINT se_ratsit_company_coordinates CHECK
        (latitude IS NULL OR (latitude >= -90 AND latitude <= 90))
        AND (longitude IS NULL OR (longitude >= -180 AND longitude <= 180))
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version);

CREATE TABLE IF NOT EXISTS corpscout.se_ratsit_company_industry_codes
(
    company_id String CODEC(ZSTD(3)),
    result_sha256 FixedString(64),
    normalizer_version LowCardinality(String),
    industry_index UInt16,
    industry_code Nullable(String) CODEC(ZSTD(3)),
    industry_description Nullable(String) CODEC(ZSTD(3)),
    normalized_at DateTime64(6, 'UTC'),

    CONSTRAINT se_ratsit_industry_company_id CHECK
        match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT se_ratsit_industry_result_hash CHECK
        match(toString(result_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_ratsit_industry_normalizer CHECK normalizer_version != '',
    CONSTRAINT se_ratsit_industry_value CHECK
        ifNull(trim(industry_code), '') != ''
        OR ifNull(trim(industry_description), '') != ''
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (
    company_id,
    result_sha256,
    normalizer_version,
    industry_index
);

CREATE TABLE IF NOT EXISTS corpscout.se_ratsit_company_summaries
(
    company_id String CODEC(ZSTD(3)),
    result_sha256 FixedString(64),
    normalizer_version LowCardinality(String),
    summary_index UInt16,
    summary_text String CODEC(ZSTD(3)),
    normalized_at DateTime64(6, 'UTC'),

    CONSTRAINT se_ratsit_summary_company_id CHECK
        match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT se_ratsit_summary_result_hash CHECK
        match(toString(result_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_ratsit_summary_normalizer CHECK normalizer_version != '',
    CONSTRAINT se_ratsit_summary_text CHECK trim(summary_text) != ''
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version, summary_index);

CREATE TABLE IF NOT EXISTS corpscout.se_ratsit_responsible_people
(
    company_id String CODEC(ZSTD(3)),
    result_sha256 FixedString(64),
    normalizer_version LowCardinality(String),
    person_index UInt16,
    display_name Nullable(String) CODEC(ZSTD(3)),
    role Nullable(String) CODEC(ZSTD(3)),
    profile_url Nullable(String) CODEC(ZSTD(3)),
    normalized_at DateTime64(6, 'UTC'),

    CONSTRAINT se_ratsit_responsible_company_id CHECK
        match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT se_ratsit_responsible_result_hash CHECK
        match(toString(result_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_ratsit_responsible_normalizer CHECK normalizer_version != '',
    CONSTRAINT se_ratsit_responsible_value CHECK
        ifNull(trim(display_name), '') != ''
        OR ifNull(trim(role), '') != ''
        OR ifNull(trim(profile_url), '') != '',
    CONSTRAINT se_ratsit_responsible_profile_url CHECK
        profile_url IS NULL
        OR startsWith(profile_url, 'https://www.ratsit.se/')
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version, person_index);

-- Ratsit's workplaces[] segment is modeled as establishments: physical local
-- operating units, not people or canonical company addresses.
CREATE TABLE IF NOT EXISTS corpscout.se_ratsit_establishments
(
    company_id String CODEC(ZSTD(3)),
    result_sha256 FixedString(64),
    normalizer_version LowCardinality(String),
    establishment_index UInt16,
    name Nullable(String) CODEC(ZSTD(3)),
    identifier Nullable(String) CODEC(ZSTD(3)),
    industry_code Nullable(String) CODEC(ZSTD(3)),
    industry_description Nullable(String) CODEC(ZSTD(3)),
    address_street Nullable(String) CODEC(ZSTD(3)),
    address_postal_code Nullable(String) CODEC(ZSTD(3)),
    address_locality Nullable(String) CODEC(ZSTD(3)),
    address_county Nullable(String) CODEC(ZSTD(3)),
    number_of_employees_raw Nullable(String) CODEC(ZSTD(3)),
    number_of_employees Nullable(UInt32),
    normalized_at DateTime64(6, 'UTC'),

    CONSTRAINT se_ratsit_establishment_company_id CHECK
        match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT se_ratsit_establishment_result_hash CHECK
        match(toString(result_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_ratsit_establishment_normalizer CHECK normalizer_version != '',
    CONSTRAINT se_ratsit_establishment_value CHECK
        ifNull(trim(name), '') != ''
        OR ifNull(trim(identifier), '') != ''
        OR ifNull(trim(industry_code), '') != ''
        OR ifNull(trim(industry_description), '') != ''
        OR ifNull(trim(address_street), '') != ''
        OR ifNull(trim(address_postal_code), '') != ''
        OR ifNull(trim(address_locality), '') != ''
        OR ifNull(trim(address_county), '') != ''
        OR ifNull(trim(number_of_employees_raw), '') != ''
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (
    company_id,
    result_sha256,
    normalizer_version,
    establishment_index
);

CREATE TABLE IF NOT EXISTS corpscout.se_ratsit_financial_reports
(
    company_id String CODEC(ZSTD(3)),
    result_sha256 FixedString(64),
    normalizer_version LowCardinality(String),
    financial_report_index UInt16,
    scope LowCardinality(String),
    monetary_unit LowCardinality(Nullable(String)),
    period_count UInt16,
    normalized_at DateTime64(6, 'UTC'),

    CONSTRAINT se_ratsit_financial_report_company_id CHECK
        match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT se_ratsit_financial_report_result_hash CHECK
        match(toString(result_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_ratsit_financial_report_normalizer CHECK
        normalizer_version != '',
    CONSTRAINT se_ratsit_financial_report_scope CHECK trim(scope) != '',
    CONSTRAINT se_ratsit_financial_report_unit CHECK
        monetary_unit IS NULL OR monetary_unit IN ('SEK', 'TSEK', 'MSEK')
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (
    company_id,
    result_sha256,
    normalizer_version,
    financial_report_index
);

-- Income statement, balance sheet, and key-ratio objects are one-to-one with a
-- financial period, so they remain one wide row rather than three join tables.
-- Monetary values retain the source report's scale in monetary_unit.
CREATE TABLE IF NOT EXISTS corpscout.se_ratsit_financial_periods
(
    company_id String CODEC(ZSTD(3)),
    result_sha256 FixedString(64),
    normalizer_version LowCardinality(String),
    financial_report_index UInt16,
    period_index UInt16,
    scope LowCardinality(String),
    monetary_unit LowCardinality(Nullable(String)),
    fiscal_year UInt16,
    period_start Nullable(Date32),
    period_end Nullable(Date32),
    period_months Nullable(UInt16),
    revenue_amount Nullable(Decimal(38, 6)),
    operating_costs_amount Nullable(Decimal(38, 6)),
    operating_profit_amount Nullable(Decimal(38, 6)),
    profit_after_financial_items_amount Nullable(Decimal(38, 6)),
    net_income_amount Nullable(Decimal(38, 6)),
    current_assets_amount Nullable(Decimal(38, 6)),
    fixed_assets_amount Nullable(Decimal(38, 6)),
    share_capital_amount Nullable(Decimal(38, 6)),
    equity_amount Nullable(Decimal(38, 6)),
    untaxed_reserves_amount Nullable(Decimal(38, 6)),
    provisions_amount Nullable(Decimal(38, 6)),
    long_term_liabilities_amount Nullable(Decimal(38, 6)),
    current_liabilities_amount Nullable(Decimal(38, 6)),
    liabilities_amount Nullable(Decimal(38, 6)),
    total_assets_amount Nullable(Decimal(38, 6)),
    balance_sheet_total_amount Nullable(Decimal(38, 6)),
    cash_liquidity_percent Nullable(Decimal(18, 6)),
    equity_ratio_percent Nullable(Decimal(18, 6)),
    net_profit_margin_percent Nullable(Decimal(18, 6)),
    ebitda_amount Nullable(Decimal(38, 6)),
    personnel_cost_per_employee_msek Nullable(Decimal(38, 6)),
    revenue_per_employee_msek Nullable(Decimal(38, 6)),
    revenue_change_percent Nullable(Decimal(18, 6)),
    average_salary Nullable(Decimal(38, 6)),
    dividend_amount Nullable(Decimal(38, 6)),
    employee_count Nullable(UInt32),
    normalized_at DateTime64(6, 'UTC'),

    CONSTRAINT se_ratsit_financial_period_company_id CHECK
        match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT se_ratsit_financial_period_result_hash CHECK
        match(toString(result_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_ratsit_financial_period_normalizer CHECK
        normalizer_version != '',
    CONSTRAINT se_ratsit_financial_period_scope CHECK trim(scope) != '',
    CONSTRAINT se_ratsit_financial_period_unit CHECK
        monetary_unit IS NULL OR monetary_unit IN ('SEK', 'TSEK', 'MSEK'),
    CONSTRAINT se_ratsit_financial_period_year CHECK
        fiscal_year >= 1800 AND fiscal_year <= 2200,
    CONSTRAINT se_ratsit_financial_period_dates CHECK
        period_start IS NULL
        OR period_end IS NULL
        OR period_start <= period_end,
    CONSTRAINT se_ratsit_financial_period_months CHECK
        period_months IS NULL OR period_months > 0
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (
    company_id,
    result_sha256,
    normalizer_version,
    financial_report_index,
    period_index
);

-- Personal data about residents at the company's address. Keep this source
-- observation restricted and separate from company-person relationships.
CREATE TABLE IF NOT EXISTS corpscout.se_ratsit_people_at_address
(
    company_id String CODEC(ZSTD(3)),
    result_sha256 FixedString(64),
    normalizer_version LowCardinality(String),
    person_index UInt16,
    name String CODEC(ZSTD(3)),
    age Nullable(UInt16),
    profile_url Nullable(String) CODEC(ZSTD(3)),
    normalized_at DateTime64(6, 'UTC'),

    CONSTRAINT se_ratsit_address_person_company_id CHECK
        match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT se_ratsit_address_person_result_hash CHECK
        match(toString(result_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_ratsit_address_person_normalizer CHECK
        normalizer_version != '',
    CONSTRAINT se_ratsit_address_person_name CHECK trim(name) != '',
    CONSTRAINT se_ratsit_address_person_profile_url CHECK
        profile_url IS NULL
        OR startsWith(profile_url, 'https://www.ratsit.se/')
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version, person_index);
