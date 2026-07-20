CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.esef_filings
(
    lei                 String,
    entity_name         String,
    fxo_id              String,
    country             LowCardinality(String),
    period_end          Date32,
    date_added          Date32,
    processed_at        Nullable(DateTime64(6)),
    json_url            String,
    package_url         String,
    report_url          String,
    viewer_url          String,
    package_sha256      String,
    error_count         UInt32,
    warning_count       UInt32,
    inconsistency_count UInt32,
    has_json_facts      UInt8,
    source_url          String,
    source_run_id       String,
    resolved_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei, period_end, fxo_id);

CREATE TABLE IF NOT EXISTS corpscout.esef_facts
(
    lei                 String,
    fxo_id              String,
    period_end          Date32,
    fact_id             String,
    concept_qname       String,
    concept_namespace   LowCardinality(String),
    concept_local_name  String,
    period_start        Nullable(Date32),
    period_instant      Nullable(Date32),
    period_duration_end Nullable(Date32),
    unit                LowCardinality(String),
    currency            LowCardinality(String),
    value_kind          LowCardinality(String),
    raw_value           String,
    amount_original     Nullable(Decimal128(2)),
    decimals            Nullable(Int32),
    dimensions          String,
    language            LowCardinality(String),
    source_run_id       String,
    resolved_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei, period_end, fxo_id, fact_id);

CREATE TABLE IF NOT EXISTS corpscout.esef_financial_metrics
(
    lei                              String,
    entity_name                      String,
    fxo_id                           String,
    country                          LowCardinality(String),
    scope                            LowCardinality(String),
    fiscal_year                      Int32,
    period_start                     Nullable(Date32),
    period_end                       Date32,
    currency                         LowCardinality(String),
    revenue_amount_original          Nullable(Decimal128(2)),
    revenue_amount_usd               Nullable(Decimal128(2)),
    operating_profit_amount_original Nullable(Decimal128(2)),
    operating_profit_amount_usd      Nullable(Decimal128(2)),
    profit_loss_amount_original      Nullable(Decimal128(2)),
    profit_loss_amount_usd           Nullable(Decimal128(2)),
    total_assets_amount_original     Nullable(Decimal128(2)),
    total_assets_amount_usd          Nullable(Decimal128(2)),
    equity_amount_original           Nullable(Decimal128(2)),
    equity_amount_usd                Nullable(Decimal128(2)),
    liabilities_amount_original      Nullable(Decimal128(2)),
    liabilities_amount_usd           Nullable(Decimal128(2)),
    cash_amount_original             Nullable(Decimal128(2)),
    cash_amount_usd                  Nullable(Decimal128(2)),
    employees                        Nullable(Int64),
    mapped_fact_count                UInt32,
    source_fact_count                UInt32,
    mapping_version                  LowCardinality(String),
    fx_rate_to_usd                   Nullable(Float64),
    fx_rate_date                     Nullable(Date32),
    fx_source                        LowCardinality(String),
    viewer_url                       String,
    source_run_id                    String,
    resolved_at                      DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei, period_end, fxo_id);

CREATE TABLE IF NOT EXISTS corpscout.esef_entity_registry_map
(
    lei                    String,
    country_iso2           LowCardinality(String),
    registry_id_raw        String,
    registry_id            String,
    match_source           LowCardinality(String),
    source_run_id          String,
    resolved_at            DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (country_iso2, registry_id, lei);
