-- Production SHOW CREATE TABLE snapshot (2026-09-12), CODECs and SETTINGS stripped.
-- Harness fixture only -- not a migration, never apply to a real ClickHouse.
CREATE TABLE corpscout.se_bolagsverket_financial_metrics
(
    `country_iso2` LowCardinality(String),
    `source_slug` LowCardinality(String),
    `source_run_id` String,
    `source_record_id` String,
    `statement_key` String,
    `source_record_uid` String DEFAULT lower(hex(SHA256(concat('company-source-record-v1\nstructured\nsweden_financial\nannual_report_xhtml\n', statement_key, '\n', statement_key)))),
    `company_id` String,
    `report_period_start` Nullable(Date32),
    `report_period_end` Nullable(Date32),
    `fiscal_year` Nullable(UInt16),
    `observation_kind` LowCardinality(String) DEFAULT 'reported',
    `source_fiscal_year` Nullable(UInt16) DEFAULT fiscal_year,
    `reported_company_name` Nullable(String),
    `source_archive_url` String,
    `source_archive_key` String,
    `source_archive_name` String,
    `nested_zip_name` String,
    `xhtml_object_key` String,
    `xhtml_source_uri` String,
    `taxonomy_entrypoint` Nullable(String),
    `currency` LowCardinality(String),
    `revenue_amount_original` Nullable(Decimal(38, 6)),
    `revenue_amount_usd` Nullable(Decimal(38, 6)),
    `operating_profit_loss_amount_original` Nullable(Decimal(38, 6)),
    `operating_profit_loss_amount_usd` Nullable(Decimal(38, 6)),
    `profit_loss_amount_original` Nullable(Decimal(38, 6)),
    `profit_loss_amount_usd` Nullable(Decimal(38, 6)),
    `total_assets_amount_original` Nullable(Decimal(38, 6)),
    `total_assets_amount_usd` Nullable(Decimal(38, 6)),
    `equity_amount_original` Nullable(Decimal(38, 6)),
    `equity_amount_usd` Nullable(Decimal(38, 6)),
    `liabilities_amount_original` Nullable(Decimal(38, 6)),
    `liabilities_amount_usd` Nullable(Decimal(38, 6)),
    `cash_and_bank_amount_original` Nullable(Decimal(38, 6)),
    `cash_and_bank_amount_usd` Nullable(Decimal(38, 6)),
    `current_assets_amount_original` Nullable(Decimal(38, 6)),
    `current_assets_amount_usd` Nullable(Decimal(38, 6)),
    `current_receivables_amount_original` Nullable(Decimal(38, 6)),
    `current_receivables_amount_usd` Nullable(Decimal(38, 6)),
    `current_liabilities_amount_original` Nullable(Decimal(38, 6)),
    `current_liabilities_amount_usd` Nullable(Decimal(38, 6)),
    `personnel_expenses_amount_original` Nullable(Decimal(38, 6)),
    `personnel_expenses_amount_usd` Nullable(Decimal(38, 6)),
    `wages_and_salaries_amount_original` Nullable(Decimal(38, 6)),
    `wages_and_salaries_amount_usd` Nullable(Decimal(38, 6)),
    `employees` Nullable(UInt64),
    `source_fact_count` UInt64,
    `mapped_fact_count` UInt64,
    `unmapped_numeric_fact_count` UInt64,
    `metric_warnings` String,
    `mapping_version` LowCardinality(String),
    `fx_rate_to_usd` Nullable(Decimal(38, 12)),
    `fx_rate_date` Nullable(Date32),
    `fx_source` String,
    `source_payload_hash` FixedString(64),
    `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (company_id, ifNull(report_period_end, toDate32('1970-01-01')), statement_key)

;
CREATE TABLE corpscout.esef_financial_metrics
(
    `lei` String,
    `entity_name` String,
    `fxo_id` String,
    `country` LowCardinality(String),
    `scope` LowCardinality(String),
    `fiscal_year` Int32,
    `period_start` Nullable(Date32),
    `period_end` Date32,
    `currency` LowCardinality(String),
    `revenue_amount_original` Nullable(Decimal(38, 2)),
    `revenue_amount_usd` Nullable(Decimal(38, 2)),
    `operating_profit_amount_original` Nullable(Decimal(38, 2)),
    `operating_profit_amount_usd` Nullable(Decimal(38, 2)),
    `profit_loss_amount_original` Nullable(Decimal(38, 2)),
    `profit_loss_amount_usd` Nullable(Decimal(38, 2)),
    `total_assets_amount_original` Nullable(Decimal(38, 2)),
    `total_assets_amount_usd` Nullable(Decimal(38, 2)),
    `equity_amount_original` Nullable(Decimal(38, 2)),
    `equity_amount_usd` Nullable(Decimal(38, 2)),
    `liabilities_amount_original` Nullable(Decimal(38, 2)),
    `liabilities_amount_usd` Nullable(Decimal(38, 2)),
    `cash_amount_original` Nullable(Decimal(38, 2)),
    `cash_amount_usd` Nullable(Decimal(38, 2)),
    `personnel_expenses_amount_original` Nullable(Decimal(38, 2)),
    `personnel_expenses_amount_usd` Nullable(Decimal(38, 2)),
    `employees` Nullable(Int64),
    `mapped_fact_count` UInt32,
    `source_fact_count` UInt32,
    `mapping_version` LowCardinality(String),
    `fx_rate_to_usd` Nullable(Float64),
    `fx_rate_date` Nullable(Date32),
    `fx_source` LowCardinality(String),
    `viewer_url` String,
    `source_run_id` String,
    `resolved_at` DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei, period_end, fxo_id)

;
CREATE TABLE corpscout.esef_filings
(
    `lei` String,
    `entity_name` String,
    `fxo_id` String,
    `country` LowCardinality(String),
    `period_end` Date32,
    `date_added` Date32,
    `processed_at` Nullable(DateTime64(6)),
    `json_url` String,
    `package_url` String,
    `report_url` String,
    `viewer_url` String,
    `package_sha256` String,
    `error_count` UInt32,
    `warning_count` UInt32,
    `inconsistency_count` UInt32,
    `has_json_facts` UInt8,
    `source_url` String,
    `source_run_id` String,
    `resolved_at` DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei, period_end, fxo_id)

;
CREATE TABLE corpscout.esef_entity_registry_map
(
    `lei` String,
    `country_iso2` LowCardinality(String),
    `registry_id_raw` String,
    `registry_id` String,
    `match_source` LowCardinality(String),
    `link_status` LowCardinality(String) DEFAULT 'gleif',
    `source_run_id` String,
    `resolved_at` DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (country_iso2, registry_id, lei)

;
CREATE TABLE corpscout.se_ratsit_financial_reports
(
    `company_id` String,
    `result_sha256` FixedString(64),
    `normalizer_version` LowCardinality(String),
    `financial_report_index` UInt16,
    `scope` LowCardinality(String),
    `monetary_unit` LowCardinality(Nullable(String)),
    `period_count` UInt16,
    `normalized_at` DateTime64(6, 'UTC'),
    CONSTRAINT se_ratsit_financial_report_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT se_ratsit_financial_report_result_hash CHECK match(toString(result_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_ratsit_financial_report_normalizer CHECK normalizer_version != '',
    CONSTRAINT se_ratsit_financial_report_scope CHECK trimBoth(scope) != '',
    CONSTRAINT se_ratsit_financial_report_unit CHECK (monetary_unit IS NULL) OR (monetary_unit IN ('SEK', 'TSEK', 'MSEK'))
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version, financial_report_index)

;
CREATE TABLE corpscout.se_ratsit_financial_periods
(
    `company_id` String,
    `result_sha256` FixedString(64),
    `normalizer_version` LowCardinality(String),
    `financial_report_index` UInt16,
    `period_index` UInt16,
    `period_kind` LowCardinality(String) DEFAULT '',
    `scope` LowCardinality(String),
    `monetary_unit` LowCardinality(Nullable(String)),
    `fiscal_year` UInt16,
    `period_start` Nullable(Date32),
    `period_end` Nullable(Date32),
    `period_months` Nullable(UInt16),
    `revenue_amount` Nullable(Decimal(38, 6)),
    `revenue_amount_usd` Nullable(Decimal(38, 6)),
    `operating_costs_amount` Nullable(Decimal(38, 6)),
    `operating_costs_amount_usd` Nullable(Decimal(38, 6)),
    `operating_profit_amount` Nullable(Decimal(38, 6)),
    `operating_profit_amount_usd` Nullable(Decimal(38, 6)),
    `profit_after_financial_items_amount` Nullable(Decimal(38, 6)),
    `profit_after_financial_items_amount_usd` Nullable(Decimal(38, 6)),
    `net_income_amount` Nullable(Decimal(38, 6)),
    `net_income_amount_usd` Nullable(Decimal(38, 6)),
    `current_assets_amount` Nullable(Decimal(38, 6)),
    `current_assets_amount_usd` Nullable(Decimal(38, 6)),
    `fixed_assets_amount` Nullable(Decimal(38, 6)),
    `fixed_assets_amount_usd` Nullable(Decimal(38, 6)),
    `share_capital_amount` Nullable(Decimal(38, 6)),
    `share_capital_amount_usd` Nullable(Decimal(38, 6)),
    `equity_amount` Nullable(Decimal(38, 6)),
    `equity_amount_usd` Nullable(Decimal(38, 6)),
    `untaxed_reserves_amount` Nullable(Decimal(38, 6)),
    `untaxed_reserves_amount_usd` Nullable(Decimal(38, 6)),
    `provisions_amount` Nullable(Decimal(38, 6)),
    `provisions_amount_usd` Nullable(Decimal(38, 6)),
    `long_term_liabilities_amount` Nullable(Decimal(38, 6)),
    `long_term_liabilities_amount_usd` Nullable(Decimal(38, 6)),
    `current_liabilities_amount` Nullable(Decimal(38, 6)),
    `current_liabilities_amount_usd` Nullable(Decimal(38, 6)),
    `liabilities_amount` Nullable(Decimal(38, 6)),
    `liabilities_amount_usd` Nullable(Decimal(38, 6)),
    `total_assets_amount` Nullable(Decimal(38, 6)),
    `total_assets_amount_usd` Nullable(Decimal(38, 6)),
    `balance_sheet_total_amount` Nullable(Decimal(38, 6)),
    `balance_sheet_total_amount_usd` Nullable(Decimal(38, 6)),
    `cash_liquidity_percent` Nullable(Decimal(18, 6)),
    `equity_ratio_percent` Nullable(Decimal(18, 6)),
    `net_profit_margin_percent` Nullable(Decimal(18, 6)),
    `ebitda_amount` Nullable(Decimal(38, 6)),
    `ebitda_amount_usd` Nullable(Decimal(38, 6)),
    `personnel_cost_per_employee_msek` Nullable(Decimal(38, 6)),
    `personnel_cost_per_employee_usd` Nullable(Decimal(38, 6)),
    `revenue_per_employee_msek` Nullable(Decimal(38, 6)),
    `revenue_per_employee_usd` Nullable(Decimal(38, 6)),
    `revenue_change_percent` Nullable(Decimal(18, 6)),
    `average_salary` Nullable(Decimal(38, 6)),
    `dividend_amount` Nullable(Decimal(38, 6)),
    `dividend_amount_usd` Nullable(Decimal(38, 6)),
    `employee_count` Nullable(UInt32),
    `fx_rate_to_usd` Nullable(Decimal(38, 12)),
    `fx_rate_date` Nullable(Date32),
    `fx_source` LowCardinality(String) DEFAULT '',
    `normalized_at` DateTime64(6, 'UTC'),
    CONSTRAINT se_ratsit_financial_period_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT se_ratsit_financial_period_result_hash CHECK match(toString(result_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_ratsit_financial_period_normalizer CHECK normalizer_version != '',
    CONSTRAINT se_ratsit_financial_period_scope CHECK trimBoth(scope) != '',
    CONSTRAINT se_ratsit_financial_period_unit CHECK (monetary_unit IS NULL) OR (monetary_unit IN ('SEK', 'TSEK', 'MSEK')),
    CONSTRAINT se_ratsit_financial_period_year CHECK (fiscal_year >= 1800) AND (fiscal_year <= 2200),
    CONSTRAINT se_ratsit_financial_period_dates CHECK (period_start IS NULL) OR (period_end IS NULL) OR (period_start <= period_end),
    CONSTRAINT se_ratsit_financial_period_months CHECK (period_months IS NULL) OR (period_months > 0),
    CONSTRAINT se_ratsit_financial_period_kind CHECK period_kind IN ('', 'employment_only', 'financial_only', 'financial_and_employment')
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version, financial_report_index, period_index)

;
