CREATE DATABASE IF NOT EXISTS corpscout;

-- Additive: banks and most industrials tag ifrs-full:EmployeeBenefitsExpense
-- (2,792 of 6,620 parsed filings, audit 2026-08-31) -- the serving view's
-- personnel_expenses outputs were hardcoded NULL until now.
ALTER TABLE corpscout.esef_financial_metrics
    ADD COLUMN IF NOT EXISTS personnel_expenses_amount_original Nullable(Decimal128(2)) AFTER cash_amount_usd,
    ADD COLUMN IF NOT EXISTS personnel_expenses_amount_usd Nullable(Decimal128(2)) AFTER personnel_expenses_amount_original;

CREATE OR REPLACE VIEW corpscout.se_financials_esef_current AS
WITH versions AS (
    SELECT
        identifiers.company_id AS company_id,
        metrics.fxo_id AS fxo_id,
        metrics.scope AS accounting_scope,
        metrics.fiscal_year AS fiscal_year,
        metrics.period_start AS report_period_start,
        metrics.period_end AS report_period_end,
        metrics.currency AS currency,
        metrics.revenue_amount_original AS revenue_amount_original,
        metrics.revenue_amount_usd AS revenue_amount_usd,
        metrics.operating_profit_amount_original AS operating_result_amount_original,
        metrics.operating_profit_amount_usd AS operating_result_amount_usd,
        metrics.profit_loss_amount_original AS net_result_amount_original,
        metrics.profit_loss_amount_usd AS net_result_amount_usd,
        metrics.total_assets_amount_original AS total_assets_amount_original,
        metrics.total_assets_amount_usd AS total_assets_amount_usd,
        metrics.equity_amount_original AS equity_amount_original,
        metrics.equity_amount_usd AS equity_amount_usd,
        metrics.liabilities_amount_original AS liabilities_amount_original,
        metrics.liabilities_amount_usd AS liabilities_amount_usd,
        metrics.cash_amount_original AS cash_and_bank_amount_original,
        metrics.cash_amount_usd AS cash_and_bank_amount_usd,
        metrics.personnel_expenses_amount_original AS personnel_expenses_amount_original,
        metrics.personnel_expenses_amount_usd AS personnel_expenses_amount_usd,
        metrics.employees AS employees,
        metrics.source_fact_count AS source_fact_count,
        metrics.mapped_fact_count AS mapped_fact_count,
        metrics.mapping_version AS mapping_version,
        metrics.fx_rate_to_usd AS fx_rate_to_usd,
        metrics.fx_rate_date AS fx_rate_date,
        metrics.fx_source AS fx_source,
        filings.source_url AS source_url,
        filings.viewer_url AS viewer_url,
        lower(hex(SHA256(concat(
            'company-source-record-v1\nfile\nesef_report_package\n',
            lowerUTF8(filings.package_sha256)
        )))) AS source_record_uid,
        toUInt32(extract(metrics.fxo_id, '-([0-9]+)$')) AS version
    FROM corpscout.esef_financial_metrics AS metrics FINAL
    INNER JOIN corpscout.esef_filings AS filings FINAL
        ON filings.lei = metrics.lei
       AND filings.period_end = metrics.period_end
       AND filings.fxo_id = metrics.fxo_id
    INNER JOIN corpscout.company_identifier AS identifiers
        ON identifiers.issuer_scheme = 'lei'
       AND identifiers.issuer_id = upperUTF8(trimBoth(metrics.lei))
    WHERE identifiers.country_code = 'SE'
      AND identifiers.is_current = 1
)
SELECT
    'esef' AS source_id,
    argMax(toString(v.accounting_scope), v.version) AS accounting_scope,
    v.company_id,
    argMax(v.fxo_id, v.version) AS source_document_id,
    toInt32(v.fiscal_year) AS fiscal_year,
    argMaxIf(
        v.report_period_start,
        v.version,
        v.report_period_start IS NOT NULL
    ) AS report_period_start,
    toNullable(v.report_period_end) AS report_period_end,
    argMaxIf(toString(v.currency), v.version, v.currency != '') AS currency,
    CAST(argMaxIf(v.revenue_amount_original, v.version, v.revenue_amount_original IS NOT NULL) AS Nullable(Decimal(38, 6))) AS revenue_amount_original,
    CAST(argMaxIf(v.revenue_amount_usd, v.version, v.revenue_amount_usd IS NOT NULL) AS Nullable(Decimal(38, 6))) AS revenue_amount_usd,
    CAST(argMaxIf(v.operating_result_amount_original, v.version, v.operating_result_amount_original IS NOT NULL) AS Nullable(Decimal(38, 6))) AS operating_result_amount_original,
    CAST(argMaxIf(v.operating_result_amount_usd, v.version, v.operating_result_amount_usd IS NOT NULL) AS Nullable(Decimal(38, 6))) AS operating_result_amount_usd,
    CAST(argMaxIf(v.net_result_amount_original, v.version, v.net_result_amount_original IS NOT NULL) AS Nullable(Decimal(38, 6))) AS net_result_amount_original,
    CAST(argMaxIf(v.net_result_amount_usd, v.version, v.net_result_amount_usd IS NOT NULL) AS Nullable(Decimal(38, 6))) AS net_result_amount_usd,
    CAST(argMaxIf(v.total_assets_amount_original, v.version, v.total_assets_amount_original IS NOT NULL) AS Nullable(Decimal(38, 6))) AS total_assets_amount_original,
    CAST(argMaxIf(v.total_assets_amount_usd, v.version, v.total_assets_amount_usd IS NOT NULL) AS Nullable(Decimal(38, 6))) AS total_assets_amount_usd,
    CAST(argMaxIf(v.equity_amount_original, v.version, v.equity_amount_original IS NOT NULL) AS Nullable(Decimal(38, 6))) AS equity_amount_original,
    CAST(argMaxIf(v.equity_amount_usd, v.version, v.equity_amount_usd IS NOT NULL) AS Nullable(Decimal(38, 6))) AS equity_amount_usd,
    CAST(argMaxIf(v.liabilities_amount_original, v.version, v.liabilities_amount_original IS NOT NULL) AS Nullable(Decimal(38, 6))) AS liabilities_amount_original,
    CAST(argMaxIf(v.liabilities_amount_usd, v.version, v.liabilities_amount_usd IS NOT NULL) AS Nullable(Decimal(38, 6))) AS liabilities_amount_usd,
    CAST(argMaxIf(v.cash_and_bank_amount_original, v.version, v.cash_and_bank_amount_original IS NOT NULL) AS Nullable(Decimal(38, 6))) AS cash_and_bank_amount_original,
    CAST(argMaxIf(v.cash_and_bank_amount_usd, v.version, v.cash_and_bank_amount_usd IS NOT NULL) AS Nullable(Decimal(38, 6))) AS cash_and_bank_amount_usd,
    CAST(NULL AS Nullable(Decimal(38, 6))) AS current_assets_amount_original,
    CAST(NULL AS Nullable(Decimal(38, 6))) AS current_assets_amount_usd,
    CAST(NULL AS Nullable(Decimal(38, 6))) AS current_liabilities_amount_original,
    CAST(NULL AS Nullable(Decimal(38, 6))) AS current_liabilities_amount_usd,
    CAST(argMaxIf(v.personnel_expenses_amount_original, v.version, v.personnel_expenses_amount_original IS NOT NULL) AS Nullable(Decimal(38, 6))) AS personnel_expenses_amount_original,
    CAST(argMaxIf(v.personnel_expenses_amount_usd, v.version, v.personnel_expenses_amount_usd IS NOT NULL) AS Nullable(Decimal(38, 6))) AS personnel_expenses_amount_usd,
    CAST(NULL AS Nullable(Decimal(38, 6))) AS wages_and_salaries_amount_original,
    CAST(NULL AS Nullable(Decimal(38, 6))) AS wages_and_salaries_amount_usd,
    CAST(argMaxIf(v.employees, v.version, v.employees IS NOT NULL) AS Nullable(Int64)) AS employees,
    toUInt64(argMax(v.source_fact_count, v.version)) AS source_fact_count,
    toUInt64(argMax(v.mapped_fact_count, v.version)) AS mapped_fact_count,
    argMax(toString(v.mapping_version), v.version) AS mapping_version,
    argMaxIf(v.fx_rate_to_usd, v.version, v.fx_rate_to_usd IS NOT NULL) AS fx_rate_to_usd,
    argMaxIf(v.fx_rate_date, v.version, v.fx_rate_date IS NOT NULL) AS fx_rate_date,
    argMaxIf(toString(v.fx_source), v.version, v.fx_source != '') AS fx_source,
    'filed' AS observation,
    toNullable(toInt32(v.fiscal_year)) AS source_fiscal_year,
    arrayDistinct(arrayFilter(value -> value != '', [
        argMaxIf(v.source_record_uid, v.version, v.revenue_amount_usd IS NOT NULL),
        argMaxIf(v.source_record_uid, v.version, v.operating_result_amount_usd IS NOT NULL),
        argMaxIf(v.source_record_uid, v.version, v.net_result_amount_usd IS NOT NULL),
        argMaxIf(v.source_record_uid, v.version, v.total_assets_amount_usd IS NOT NULL),
        argMaxIf(v.source_record_uid, v.version, v.equity_amount_usd IS NOT NULL),
        argMaxIf(v.source_record_uid, v.version, v.liabilities_amount_usd IS NOT NULL),
        argMaxIf(v.source_record_uid, v.version, v.cash_and_bank_amount_usd IS NOT NULL),
        argMaxIf(v.source_record_uid, v.version, v.personnel_expenses_amount_usd IS NOT NULL),
        argMaxIf(v.source_record_uid, v.version, v.employees IS NOT NULL),
        argMaxIf(v.source_record_uid, v.version, v.viewer_url != '')
    ])) AS source_record_uids,
    argMaxIf(v.source_url, v.version, v.source_url != '') AS source_url,
    argMaxIf(v.viewer_url, v.version, v.viewer_url != '') AS viewer_url
FROM versions AS v
GROUP BY v.company_id, v.fiscal_year, v.report_period_end;
