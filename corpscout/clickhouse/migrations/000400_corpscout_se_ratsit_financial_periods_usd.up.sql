CREATE DATABASE IF NOT EXISTS corpscout;

-- USD TWINS FOR RATSIT'S FINANCIAL PERIODS (financial entity spec 2026-09-11 section 3, slice 0).
--
-- The currency standard (data-source-guidelines section 7) stores every monetary figure a
-- source publishes WITH its USD twin in the source table, converted as a separate re-runnable
-- step. se_ratsit_financial_periods carried twenty monetary columns and no USD, so the figures
-- could only ever answer a single-country question. Each monetary column gets a twin right
-- after it: the eighteen *_amount lines (balance_sheet_total_amount included, for fidelity)
-- and the two per-employee figures Ratsit publishes in MSEK. One rate covers every figure of a
-- row, so fx_rate_to_usd / fx_rate_date / fx_source stay singular, after employee_count.
--
-- No twin for average_salary: Ratsit never states its unit (v2 proposal, field semantics), so
-- a conversion would guess. No twin for the *_percent ratios: they are not money.
--
-- The native columns keep their names and values: `revenue_amount` stays the published figure
-- in the row's monetary_unit (MSEK for every row today) the twin is the FULL-UNIT dollar value,
-- scale applied before FX by the asset se_ratsit_financial_periods_usd, which fills these
-- columns in place with one mutation over the rows whose fx_rate_to_usd is still NULL. The
-- normalizer never writes them (its INSERT lists its own columns), so they default to NULL on
-- every new report version and the asset converts them on its next run.

ALTER TABLE corpscout.se_ratsit_financial_periods
    ADD COLUMN IF NOT EXISTS revenue_amount_usd Nullable(Decimal(38, 6)) AFTER revenue_amount,
    ADD COLUMN IF NOT EXISTS operating_costs_amount_usd Nullable(Decimal(38, 6)) AFTER operating_costs_amount,
    ADD COLUMN IF NOT EXISTS operating_profit_amount_usd Nullable(Decimal(38, 6)) AFTER operating_profit_amount,
    ADD COLUMN IF NOT EXISTS profit_after_financial_items_amount_usd Nullable(Decimal(38, 6)) AFTER profit_after_financial_items_amount,
    ADD COLUMN IF NOT EXISTS net_income_amount_usd Nullable(Decimal(38, 6)) AFTER net_income_amount,
    ADD COLUMN IF NOT EXISTS current_assets_amount_usd Nullable(Decimal(38, 6)) AFTER current_assets_amount,
    ADD COLUMN IF NOT EXISTS fixed_assets_amount_usd Nullable(Decimal(38, 6)) AFTER fixed_assets_amount,
    ADD COLUMN IF NOT EXISTS share_capital_amount_usd Nullable(Decimal(38, 6)) AFTER share_capital_amount,
    ADD COLUMN IF NOT EXISTS equity_amount_usd Nullable(Decimal(38, 6)) AFTER equity_amount,
    ADD COLUMN IF NOT EXISTS untaxed_reserves_amount_usd Nullable(Decimal(38, 6)) AFTER untaxed_reserves_amount,
    ADD COLUMN IF NOT EXISTS provisions_amount_usd Nullable(Decimal(38, 6)) AFTER provisions_amount,
    ADD COLUMN IF NOT EXISTS long_term_liabilities_amount_usd Nullable(Decimal(38, 6)) AFTER long_term_liabilities_amount,
    ADD COLUMN IF NOT EXISTS current_liabilities_amount_usd Nullable(Decimal(38, 6)) AFTER current_liabilities_amount,
    ADD COLUMN IF NOT EXISTS liabilities_amount_usd Nullable(Decimal(38, 6)) AFTER liabilities_amount,
    ADD COLUMN IF NOT EXISTS total_assets_amount_usd Nullable(Decimal(38, 6)) AFTER total_assets_amount,
    ADD COLUMN IF NOT EXISTS balance_sheet_total_amount_usd Nullable(Decimal(38, 6)) AFTER balance_sheet_total_amount,
    ADD COLUMN IF NOT EXISTS ebitda_amount_usd Nullable(Decimal(38, 6)) AFTER ebitda_amount,
    ADD COLUMN IF NOT EXISTS personnel_cost_per_employee_usd Nullable(Decimal(38, 6)) AFTER personnel_cost_per_employee_msek,
    ADD COLUMN IF NOT EXISTS revenue_per_employee_usd Nullable(Decimal(38, 6)) AFTER revenue_per_employee_msek,
    ADD COLUMN IF NOT EXISTS dividend_amount_usd Nullable(Decimal(38, 6)) AFTER dividend_amount,
    ADD COLUMN IF NOT EXISTS fx_rate_to_usd Nullable(Decimal(38, 12)) AFTER employee_count,
    ADD COLUMN IF NOT EXISTS fx_rate_date Nullable(Date32) AFTER fx_rate_to_usd,
    ADD COLUMN IF NOT EXISTS fx_source LowCardinality(String) DEFAULT '' AFTER fx_rate_date;
