CREATE DATABASE IF NOT EXISTS corpscout;

-- THE SE COMPANY FINANCIAL ENTITY (spec 2026-09-11 section 4, slice 1). Five tables on the
-- basic-info shape keyed by PERIOD: per-source suggestions, the folded main table with its
-- history, and the reviewer's precedence rules and hide rules. No normalized layer: the
-- sources deliver typed numbers, so unit scaling and field mapping happen in the extractors
-- (slice 2). Nothing reads these tables until the fold (slice 3) and the backoffice (slice 4).
--
-- ONE ROW PER COMPANY, ACCOUNTING SCOPE AND PERIOD END. scope is standalone (legal-entity
-- accounts: Bolagsverket, Ratsit's company reports) or consolidated (group accounts: ESEF,
-- Ratsit's consolidated reports) -- a group figure never merges with a legal-entity figure.
-- period_key is the text '<scope>:<period_end>' and is what suggestions, rules and the
-- backoffice key on -- the CHECK pins it to the two columns it is derived from.
--
-- TWENTY MONETARY FIELDS, EACH A PAIR. <field>_amount_original is the figure in the
-- source's currency at FULL units (Ratsit's 57.1 MSEK lands as 57100000 with amount_scale
-- 1000000 saying it was published in millions) -- <field>_amount_usd is the source's own
-- conversion, copied, never recomputed here. NULL means "no opinion", never zero. The main
-- row carries a _source beside every value and no row-level fx columns: each figure's USD
-- twin is its own winner's conversion, so one rate per row would be wrong the moment two
-- sources share a row.
--
-- A TOMBSTONE is a suggestion row whose twenty figures and employees are all NULL: an
-- extractor writes one per period a source stops delivering. The reviewer sources use the
-- same period keys as the pipeline sources.

-- Per-source suggestions (spec 4.1): one current row per company, source and period.
CREATE TABLE IF NOT EXISTS corpscout.se_company_financial_suggestion
(
    company_id String,
    source LowCardinality(String),
    period_key String,
    suggestion_id FixedString(64),
    suggested_at DateTime64(3, 'UTC'),
    source_record_uid String,
    scope LowCardinality(String),
    period_end Date32,
    period_end_derived UInt8 DEFAULT 0,
    period_start Nullable(Date32),
    fiscal_year Nullable(UInt16),
    period_months Nullable(UInt16),
    filing_fiscal_year Nullable(UInt16),
    currency LowCardinality(Nullable(String)),
    amount_scale UInt32 DEFAULT 1,
    revenue_amount_original Nullable(Decimal(38, 6)),
    revenue_amount_usd Nullable(Decimal(38, 6)),
    operating_costs_amount_original Nullable(Decimal(38, 6)),
    operating_costs_amount_usd Nullable(Decimal(38, 6)),
    operating_result_amount_original Nullable(Decimal(38, 6)),
    operating_result_amount_usd Nullable(Decimal(38, 6)),
    result_after_financial_items_amount_original Nullable(Decimal(38, 6)),
    result_after_financial_items_amount_usd Nullable(Decimal(38, 6)),
    net_result_amount_original Nullable(Decimal(38, 6)),
    net_result_amount_usd Nullable(Decimal(38, 6)),
    ebitda_amount_original Nullable(Decimal(38, 6)),
    ebitda_amount_usd Nullable(Decimal(38, 6)),
    total_assets_amount_original Nullable(Decimal(38, 6)),
    total_assets_amount_usd Nullable(Decimal(38, 6)),
    fixed_assets_amount_original Nullable(Decimal(38, 6)),
    fixed_assets_amount_usd Nullable(Decimal(38, 6)),
    current_assets_amount_original Nullable(Decimal(38, 6)),
    current_assets_amount_usd Nullable(Decimal(38, 6)),
    cash_and_bank_amount_original Nullable(Decimal(38, 6)),
    cash_and_bank_amount_usd Nullable(Decimal(38, 6)),
    equity_amount_original Nullable(Decimal(38, 6)),
    equity_amount_usd Nullable(Decimal(38, 6)),
    share_capital_amount_original Nullable(Decimal(38, 6)),
    share_capital_amount_usd Nullable(Decimal(38, 6)),
    untaxed_reserves_amount_original Nullable(Decimal(38, 6)),
    untaxed_reserves_amount_usd Nullable(Decimal(38, 6)),
    provisions_amount_original Nullable(Decimal(38, 6)),
    provisions_amount_usd Nullable(Decimal(38, 6)),
    liabilities_amount_original Nullable(Decimal(38, 6)),
    liabilities_amount_usd Nullable(Decimal(38, 6)),
    long_term_liabilities_amount_original Nullable(Decimal(38, 6)),
    long_term_liabilities_amount_usd Nullable(Decimal(38, 6)),
    current_liabilities_amount_original Nullable(Decimal(38, 6)),
    current_liabilities_amount_usd Nullable(Decimal(38, 6)),
    personnel_expenses_amount_original Nullable(Decimal(38, 6)),
    personnel_expenses_amount_usd Nullable(Decimal(38, 6)),
    wages_and_salaries_amount_original Nullable(Decimal(38, 6)),
    wages_and_salaries_amount_usd Nullable(Decimal(38, 6)),
    dividend_amount_original Nullable(Decimal(38, 6)),
    dividend_amount_usd Nullable(Decimal(38, 6)),
    employees Nullable(UInt64),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date32),
    fx_source LowCardinality(String) DEFAULT '',
    decided_by LowCardinality(String) DEFAULT '',
    note String DEFAULT '',
    source_run_id String,
    extractor_version LowCardinality(String),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_scope CHECK scope IN ('standalone', 'consolidated'),
    CONSTRAINT valid_period_key CHECK period_key = concat(scope, ':', toString(period_end)),
    CONSTRAINT valid_amount_scale CHECK amount_scale IN (1, 1000, 1000000)
)
ENGINE = ReplacingMergeTree(suggested_at)
ORDER BY (company_id, source, period_key);

-- Published periods (spec 4.2): one row per company, scope and period end, written by the
-- fold. currency is decided first and gates which rows may supply money (spec 6) -- a
-- _source column is '' when the field has no value.
CREATE TABLE IF NOT EXISTS corpscout.se_company_financial
(
    company_id String,
    scope LowCardinality(String),
    period_end Date32,
    period_key String,
    period_start Nullable(Date32),
    period_start_source LowCardinality(String),
    fiscal_year Nullable(UInt16),
    fiscal_year_source LowCardinality(String),
    period_months Nullable(UInt16),
    period_months_source LowCardinality(String),
    currency LowCardinality(String),
    currency_source LowCardinality(String),
    revenue_amount_original Nullable(Decimal(38, 6)),
    revenue_amount_usd Nullable(Decimal(38, 6)),
    revenue_source LowCardinality(String),
    operating_costs_amount_original Nullable(Decimal(38, 6)),
    operating_costs_amount_usd Nullable(Decimal(38, 6)),
    operating_costs_source LowCardinality(String),
    operating_result_amount_original Nullable(Decimal(38, 6)),
    operating_result_amount_usd Nullable(Decimal(38, 6)),
    operating_result_source LowCardinality(String),
    result_after_financial_items_amount_original Nullable(Decimal(38, 6)),
    result_after_financial_items_amount_usd Nullable(Decimal(38, 6)),
    result_after_financial_items_source LowCardinality(String),
    net_result_amount_original Nullable(Decimal(38, 6)),
    net_result_amount_usd Nullable(Decimal(38, 6)),
    net_result_source LowCardinality(String),
    ebitda_amount_original Nullable(Decimal(38, 6)),
    ebitda_amount_usd Nullable(Decimal(38, 6)),
    ebitda_source LowCardinality(String),
    total_assets_amount_original Nullable(Decimal(38, 6)),
    total_assets_amount_usd Nullable(Decimal(38, 6)),
    total_assets_source LowCardinality(String),
    fixed_assets_amount_original Nullable(Decimal(38, 6)),
    fixed_assets_amount_usd Nullable(Decimal(38, 6)),
    fixed_assets_source LowCardinality(String),
    current_assets_amount_original Nullable(Decimal(38, 6)),
    current_assets_amount_usd Nullable(Decimal(38, 6)),
    current_assets_source LowCardinality(String),
    cash_and_bank_amount_original Nullable(Decimal(38, 6)),
    cash_and_bank_amount_usd Nullable(Decimal(38, 6)),
    cash_and_bank_source LowCardinality(String),
    equity_amount_original Nullable(Decimal(38, 6)),
    equity_amount_usd Nullable(Decimal(38, 6)),
    equity_source LowCardinality(String),
    share_capital_amount_original Nullable(Decimal(38, 6)),
    share_capital_amount_usd Nullable(Decimal(38, 6)),
    share_capital_source LowCardinality(String),
    untaxed_reserves_amount_original Nullable(Decimal(38, 6)),
    untaxed_reserves_amount_usd Nullable(Decimal(38, 6)),
    untaxed_reserves_source LowCardinality(String),
    provisions_amount_original Nullable(Decimal(38, 6)),
    provisions_amount_usd Nullable(Decimal(38, 6)),
    provisions_source LowCardinality(String),
    liabilities_amount_original Nullable(Decimal(38, 6)),
    liabilities_amount_usd Nullable(Decimal(38, 6)),
    liabilities_source LowCardinality(String),
    long_term_liabilities_amount_original Nullable(Decimal(38, 6)),
    long_term_liabilities_amount_usd Nullable(Decimal(38, 6)),
    long_term_liabilities_source LowCardinality(String),
    current_liabilities_amount_original Nullable(Decimal(38, 6)),
    current_liabilities_amount_usd Nullable(Decimal(38, 6)),
    current_liabilities_source LowCardinality(String),
    personnel_expenses_amount_original Nullable(Decimal(38, 6)),
    personnel_expenses_amount_usd Nullable(Decimal(38, 6)),
    personnel_expenses_source LowCardinality(String),
    wages_and_salaries_amount_original Nullable(Decimal(38, 6)),
    wages_and_salaries_amount_usd Nullable(Decimal(38, 6)),
    wages_and_salaries_source LowCardinality(String),
    dividend_amount_original Nullable(Decimal(38, 6)),
    dividend_amount_usd Nullable(Decimal(38, 6)),
    dividend_source LowCardinality(String),
    employees Nullable(UInt64),
    employees_source LowCardinality(String),
    sources Array(LowCardinality(String)),
    active UInt8,
    inactive_reason LowCardinality(String),
    folded_at DateTime64(3, 'UTC'),
    fold_version LowCardinality(String),
    source_run_id String,
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_scope CHECK scope IN ('standalone', 'consolidated'),
    CONSTRAINT valid_period_key CHECK period_key = concat(scope, ':', toString(period_end))
)
ENGINE = ReplacingMergeTree(folded_at)
ORDER BY (company_id, scope, period_end);

-- Period history (spec 4.3): every main column plus the change block, appended by the fold
-- only when a period's values, sources or activity changed, the first publish included.
-- Append-only, no constraints (the main table validated the row).
CREATE TABLE IF NOT EXISTS corpscout.se_company_financial_history
(
    company_id String,
    scope LowCardinality(String),
    period_end Date32,
    period_key String,
    period_start Nullable(Date32),
    period_start_source LowCardinality(String),
    fiscal_year Nullable(UInt16),
    fiscal_year_source LowCardinality(String),
    period_months Nullable(UInt16),
    period_months_source LowCardinality(String),
    currency LowCardinality(String),
    currency_source LowCardinality(String),
    revenue_amount_original Nullable(Decimal(38, 6)),
    revenue_amount_usd Nullable(Decimal(38, 6)),
    revenue_source LowCardinality(String),
    operating_costs_amount_original Nullable(Decimal(38, 6)),
    operating_costs_amount_usd Nullable(Decimal(38, 6)),
    operating_costs_source LowCardinality(String),
    operating_result_amount_original Nullable(Decimal(38, 6)),
    operating_result_amount_usd Nullable(Decimal(38, 6)),
    operating_result_source LowCardinality(String),
    result_after_financial_items_amount_original Nullable(Decimal(38, 6)),
    result_after_financial_items_amount_usd Nullable(Decimal(38, 6)),
    result_after_financial_items_source LowCardinality(String),
    net_result_amount_original Nullable(Decimal(38, 6)),
    net_result_amount_usd Nullable(Decimal(38, 6)),
    net_result_source LowCardinality(String),
    ebitda_amount_original Nullable(Decimal(38, 6)),
    ebitda_amount_usd Nullable(Decimal(38, 6)),
    ebitda_source LowCardinality(String),
    total_assets_amount_original Nullable(Decimal(38, 6)),
    total_assets_amount_usd Nullable(Decimal(38, 6)),
    total_assets_source LowCardinality(String),
    fixed_assets_amount_original Nullable(Decimal(38, 6)),
    fixed_assets_amount_usd Nullable(Decimal(38, 6)),
    fixed_assets_source LowCardinality(String),
    current_assets_amount_original Nullable(Decimal(38, 6)),
    current_assets_amount_usd Nullable(Decimal(38, 6)),
    current_assets_source LowCardinality(String),
    cash_and_bank_amount_original Nullable(Decimal(38, 6)),
    cash_and_bank_amount_usd Nullable(Decimal(38, 6)),
    cash_and_bank_source LowCardinality(String),
    equity_amount_original Nullable(Decimal(38, 6)),
    equity_amount_usd Nullable(Decimal(38, 6)),
    equity_source LowCardinality(String),
    share_capital_amount_original Nullable(Decimal(38, 6)),
    share_capital_amount_usd Nullable(Decimal(38, 6)),
    share_capital_source LowCardinality(String),
    untaxed_reserves_amount_original Nullable(Decimal(38, 6)),
    untaxed_reserves_amount_usd Nullable(Decimal(38, 6)),
    untaxed_reserves_source LowCardinality(String),
    provisions_amount_original Nullable(Decimal(38, 6)),
    provisions_amount_usd Nullable(Decimal(38, 6)),
    provisions_source LowCardinality(String),
    liabilities_amount_original Nullable(Decimal(38, 6)),
    liabilities_amount_usd Nullable(Decimal(38, 6)),
    liabilities_source LowCardinality(String),
    long_term_liabilities_amount_original Nullable(Decimal(38, 6)),
    long_term_liabilities_amount_usd Nullable(Decimal(38, 6)),
    long_term_liabilities_source LowCardinality(String),
    current_liabilities_amount_original Nullable(Decimal(38, 6)),
    current_liabilities_amount_usd Nullable(Decimal(38, 6)),
    current_liabilities_source LowCardinality(String),
    personnel_expenses_amount_original Nullable(Decimal(38, 6)),
    personnel_expenses_amount_usd Nullable(Decimal(38, 6)),
    personnel_expenses_source LowCardinality(String),
    wages_and_salaries_amount_original Nullable(Decimal(38, 6)),
    wages_and_salaries_amount_usd Nullable(Decimal(38, 6)),
    wages_and_salaries_source LowCardinality(String),
    dividend_amount_original Nullable(Decimal(38, 6)),
    dividend_amount_usd Nullable(Decimal(38, 6)),
    dividend_source LowCardinality(String),
    employees Nullable(UInt64),
    employees_source LowCardinality(String),
    sources Array(LowCardinality(String)),
    active UInt8,
    inactive_reason LowCardinality(String),
    folded_at DateTime64(3, 'UTC'),
    fold_version LowCardinality(String),
    source_run_id String,
    changed_fields Array(String),
    changed_at DateTime64(3, 'UTC'),
    change_kind LowCardinality(String),
    fold_run_id String
)
ENGINE = MergeTree
ORDER BY (company_id, scope, period_end, changed_at);

-- Precedence (spec 4.4): the basic-info shape with a period scope. company_id '' rows are
-- the global order exported from precedence.py -- a company row is a reviewer decision for
-- one period (period_key = the suggestion key) or for every period of the company
-- (period_key ''). The export never touches a company row -- a release is a new version with
-- removed = 1, never a delete.
CREATE TABLE IF NOT EXISTS corpscout.se_company_financial_precedence
(
    company_id String,
    period_key String,
    field LowCardinality(String),
    source LowCardinality(String),
    precedence UInt32,
    removed UInt8 DEFAULT 0,
    decided_by LowCardinality(String) DEFAULT '',
    note String DEFAULT '',
    decided_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_global_scope CHECK company_id != '' OR period_key = ''
)
ENGINE = ReplacingMergeTree(decided_at)
ORDER BY (company_id, period_key, field, source);

-- Reviewer rules (spec 4.5): the address shape, one action -- hide a period. removed = 1
-- releases it (the tab's Unhide) -- a rule is never edited in place.
CREATE TABLE IF NOT EXISTS corpscout.se_company_financial_rule
(
    company_id String,
    period_key String,
    action LowCardinality(String),
    removed UInt8 DEFAULT 0,
    decided_by LowCardinality(String) DEFAULT '',
    note String DEFAULT '',
    decided_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_action CHECK action IN ('hide')
)
ENGINE = ReplacingMergeTree(decided_at)
ORDER BY (company_id, period_key, action);
