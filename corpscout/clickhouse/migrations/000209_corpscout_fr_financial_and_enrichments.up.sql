CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fr_financial_metrics
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    siren String,
    period_end_date Nullable(Date),
    fiscal_year Nullable(Int32),
    balance_type_code LowCardinality(String),
    currency LowCardinality(String),
    revenue_amount_original Nullable(Decimal(38, 2)),
    revenue_amount_usd Nullable(Decimal(38, 2)),
    gross_margin_amount_original Nullable(Decimal(38, 2)),
    gross_margin_amount_usd Nullable(Decimal(38, 2)),
    ebitda_amount_original Nullable(Decimal(38, 2)),
    ebitda_amount_usd Nullable(Decimal(38, 2)),
    ebit_amount_original Nullable(Decimal(38, 2)),
    ebit_amount_usd Nullable(Decimal(38, 2)),
    net_income_amount_original Nullable(Decimal(38, 2)),
    net_income_amount_usd Nullable(Decimal(38, 2)),
    debt_ratio_percent Nullable(Decimal(38, 6)),
    liquidity_ratio_percent Nullable(Decimal(38, 6)),
    asset_age_ratio_percent Nullable(Decimal(38, 6)),
    financial_autonomy_percent Nullable(Decimal(38, 6)),
    operating_working_capital_to_revenue_percent Nullable(Decimal(38, 6)),
    interest_coverage_percent Nullable(Decimal(38, 6)),
    cash_flow_to_revenue_percent Nullable(Decimal(38, 6)),
    repayment_capacity_ratio Nullable(Decimal(38, 6)),
    ebitda_margin_percent Nullable(Decimal(38, 6)),
    current_income_before_tax_to_revenue_percent Nullable(Decimal(38, 6)),
    operating_working_capital_days Nullable(Decimal(38, 6)),
    inventory_turnover_days Nullable(Decimal(38, 6)),
    customer_payment_days Nullable(Decimal(38, 6)),
    supplier_payment_days Nullable(Decimal(38, 6)),
    confidentiality_status LowCardinality(String),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_source String,
    source_url String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (siren, source_record_id);

CREATE TABLE IF NOT EXISTS corpscout.fr_company_enrichments
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    siren String,
    head_office_siret String,
    insee_updated_at Nullable(DateTime64(3, 'UTC')),
    rne_updated_at Nullable(DateTime64(3, 'UTC')),
    has_gender_equality_index Nullable(UInt8),
    has_responsible_purchasing_commitment Nullable(UInt8),
    has_alim_confiance_listing Nullable(UInt8),
    is_association Nullable(UInt8),
    is_individual_entrepreneur Nullable(UInt8),
    has_entertainment_entrepreneur_license Nullable(UInt8),
    is_living_heritage_company Nullable(UInt8),
    entertainment_entrepreneur_status LowCardinality(String),
    is_social_solidarity_economy Nullable(UInt8),
    is_training_organization Nullable(UInt8),
    is_qualiopi_certified Nullable(UInt8),
    is_administration Nullable(UInt8),
    mission_company_status_code LowCardinality(String),
    training_organization_ids Array(String),
    collective_agreement_ids Array(String),
    is_inclusion_structure Nullable(UInt8),
    inclusion_structure_type LowCardinality(String),
    legal_finess_ids Array(String),
    has_ademe_aid Nullable(UInt8),
    is_lawyer Nullable(UInt8),
    source_url String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY siren;

CREATE TABLE IF NOT EXISTS corpscout.fr_company_financials_latest
(
    company_id String,
    fiscal_year Nullable(Int32),
    period_end_date Nullable(Date),
    currency LowCardinality(String),
    revenue_amount_original Nullable(Float64),
    revenue_amount_usd Nullable(Float64),
    net_result_amount_original Nullable(Float64),
    net_result_amount_usd Nullable(Float64),
    total_assets_amount_original Nullable(Float64),
    total_assets_amount_usd Nullable(Float64),
    equity_amount_original Nullable(Float64),
    equity_amount_usd Nullable(Float64),
    employees Nullable(Float64),
    years_count UInt32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY company_id;
