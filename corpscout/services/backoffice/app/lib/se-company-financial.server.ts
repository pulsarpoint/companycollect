import { chQuery } from "~/lib/clickhouse.server";

/**
 * The serving summary the company list and the public company page read:
 * one row per company, the newest year that resolved. Every amount is
 * projected as text -- ClickHouse hands Decimal back as a string anyway, and
 * a uniform type keeps `tabular-nums` formatting in one place.
 */
export interface SeCompanyFinancialsLatestRow {
  fiscal_year: string;
  period_end_date: string;
  currency: string;
  revenue_amount_original: string;
  revenue_amount_usd: string;
  net_result_amount_original: string;
  net_result_amount_usd: string;
  total_assets_amount_original: string;
  total_assets_amount_usd: string;
  equity_amount_original: string;
  equity_amount_usd: string;
  employees: string;
  years_count: string;
  resolved_at: string;
}

/** One fiscal year as one financial source resolved it. */
export interface SeCompanyFinancialYearRow {
  source_id: string;
  accounting_scope: string;
  source_document_id: string;
  fiscal_year: string;
  report_period_start: string;
  report_period_end: string;
  currency: string;
  revenue_amount_original: string;
  operating_result_amount_original: string;
  net_result_amount_original: string;
  total_assets_amount_original: string;
  equity_amount_original: string;
  liabilities_amount_original: string;
  cash_and_bank_amount_original: string;
  current_assets_amount_original: string;
  current_liabilities_amount_original: string;
  personnel_expenses_amount_original: string;
  wages_and_salaries_amount_original: string;
  employees: string;
  revenue_amount_usd: string;
  net_result_amount_usd: string;
  /** "filed" or "comparative" -- a comparative year is a prior-year column
   * lifted out of a later filing, not its own report. */
  observation: string;
  source_fact_count: string;
  mapped_fact_count: string;
  mapping_version: string;
  fx_rate_to_usd: string;
  fx_rate_date: string;
  fx_source: string;
  source_url: string;
  viewer_url: string;
}

/** One filed XBRL/iXBRL report as the parser recorded it. */
export interface SeCompanyFinancialReportRow {
  source_slug: string;
  statement_key: string;
  source_record_uid: string;
  fiscal_year: string;
  report_period_start: string;
  report_period_end: string;
  reported_company_name: string;
  report_language: string;
  taxonomy_entrypoint: string;
  source_archive_name: string;
  nested_zip_name: string;
  xhtml_object_key: string;
  xhtml_sha256: string;
  facts_count: string;
  contexts_count: string;
  units_count: string;
  parser_version: string;
  resolved_at: string;
}

export interface SeCompanyFinancialDetail {
  latest: SeCompanyFinancialsLatestRow | null;
  /** Per-source year rows, in the order SOURCE_VIEWS declares. */
  sources: Array<{
    source_id: string;
    view: string;
    years: SeCompanyFinancialYearRow[];
  }>;
  reports: SeCompanyFinancialReportRow[];
}

/**
 * se_company_financials_latest is a plain MergeTree rebuilt whole per run, so
 * no FINAL. Nullable amounts are collapsed to '' rather than rendered as
 * "null": a company that reports no equity and one whose equity is zero must
 * not look alike.
 */
export const FINANCIALS_LATEST_SQL = `SELECT
  ifNull(toString(f.fiscal_year), '') AS fiscal_year,
  ifNull(toString(f.period_end_date), '') AS period_end_date,
  toString(f.currency) AS currency,
  ifNull(toString(f.revenue_amount_original), '') AS revenue_amount_original,
  ifNull(toString(f.revenue_amount_usd), '') AS revenue_amount_usd,
  ifNull(toString(f.net_result_amount_original), '') AS net_result_amount_original,
  ifNull(toString(f.net_result_amount_usd), '') AS net_result_amount_usd,
  ifNull(toString(f.total_assets_amount_original), '') AS total_assets_amount_original,
  ifNull(toString(f.total_assets_amount_usd), '') AS total_assets_amount_usd,
  ifNull(toString(f.equity_amount_original), '') AS equity_amount_original,
  ifNull(toString(f.equity_amount_usd), '') AS equity_amount_usd,
  ifNull(toString(f.employees), '') AS employees,
  toString(f.years_count) AS years_count,
  toString(f.resolved_at) AS resolved_at
FROM corpscout.se_company_financials_latest AS f
WHERE f.company_id = {companyId:String}
LIMIT 1`;

/**
 * The two serving views the public company page reads (see
 * `SWEDEN_FINANCIAL_SOURCE_VIEWS` in queries.server.ts): standalone accounts
 * filed with Bolagsverket, and consolidated IFRS figures from ESEF. They are
 * views over already-deduplicated aggregates -- the FINAL each one needs sits
 * inside its own definition -- so this query adds none.
 */
export const SOURCE_VIEWS = [
  { source_id: "bolagsverket-annual-accounts", view: "se_financials_bolagsverket_current" },
  { source_id: "esef", view: "se_financials_esef_current" },
] as const;

/** Built from the SOURCE_VIEWS allowlist only; the company id is always a
 * named parameter, never interpolated. */
export function financialSourceYearsSql(view: string): string {
  return `SELECT
  source_id AS source_id,
  accounting_scope AS accounting_scope,
  source_document_id AS source_document_id,
  ifNull(toString(fiscal_year), '') AS fiscal_year,
  ifNull(toString(report_period_start), '') AS report_period_start,
  ifNull(toString(report_period_end), '') AS report_period_end,
  currency AS currency,
  ifNull(toString(revenue_amount_original), '') AS revenue_amount_original,
  ifNull(toString(operating_result_amount_original), '') AS operating_result_amount_original,
  ifNull(toString(net_result_amount_original), '') AS net_result_amount_original,
  ifNull(toString(total_assets_amount_original), '') AS total_assets_amount_original,
  ifNull(toString(equity_amount_original), '') AS equity_amount_original,
  ifNull(toString(liabilities_amount_original), '') AS liabilities_amount_original,
  ifNull(toString(cash_and_bank_amount_original), '') AS cash_and_bank_amount_original,
  ifNull(toString(current_assets_amount_original), '') AS current_assets_amount_original,
  ifNull(toString(current_liabilities_amount_original), '') AS current_liabilities_amount_original,
  ifNull(toString(personnel_expenses_amount_original), '') AS personnel_expenses_amount_original,
  ifNull(toString(wages_and_salaries_amount_original), '') AS wages_and_salaries_amount_original,
  ifNull(toString(employees), '') AS employees,
  ifNull(toString(revenue_amount_usd), '') AS revenue_amount_usd,
  ifNull(toString(net_result_amount_usd), '') AS net_result_amount_usd,
  observation AS observation,
  toString(source_fact_count) AS source_fact_count,
  toString(mapped_fact_count) AS mapped_fact_count,
  mapping_version AS mapping_version,
  ifNull(toString(fx_rate_to_usd), '') AS fx_rate_to_usd,
  ifNull(toString(fx_rate_date), '') AS fx_rate_date,
  fx_source AS fx_source,
  source_url AS source_url,
  viewer_url AS viewer_url
FROM corpscout.${view}
WHERE company_id = {companyId:String}
ORDER BY fiscal_year DESC
LIMIT 25`;
}

/**
 * Every filed report the XBRL parser recorded for this company, newest first.
 * se_financial_reports is a ReplacingMergeTree(resolved_at), so FINAL: a
 * re-parsed filing must show once, as its newest version. A company files a
 * handful of reports a year at most, so 200 is far above the real ceiling
 * while still bounding a pathological id.
 */
export const FINANCIAL_REPORTS_SQL = `SELECT
  toString(r.source_slug) AS source_slug,
  r.statement_key AS statement_key,
  r.source_record_uid AS source_record_uid,
  ifNull(toString(r.fiscal_year), '') AS fiscal_year,
  ifNull(toString(r.report_period_start), '') AS report_period_start,
  ifNull(toString(r.report_period_end), '') AS report_period_end,
  ifNull(r.reported_company_name, '') AS reported_company_name,
  ifNull(toString(r.report_language), '') AS report_language,
  ifNull(r.taxonomy_entrypoint, '') AS taxonomy_entrypoint,
  r.source_archive_name AS source_archive_name,
  r.nested_zip_name AS nested_zip_name,
  r.xhtml_object_key AS xhtml_object_key,
  toString(r.xhtml_sha256) AS xhtml_sha256,
  toString(r.facts_count) AS facts_count,
  toString(r.contexts_count) AS contexts_count,
  toString(r.units_count) AS units_count,
  toString(r.parser_version) AS parser_version,
  toString(r.resolved_at) AS resolved_at
FROM corpscout.se_financial_reports AS r FINAL
WHERE r.company_id = {companyId:String}
ORDER BY r.report_period_end DESC NULLS LAST, r.statement_key
LIMIT 200`;

/** The whole Financial tab in one round of parallel reads. */
export async function loadSeCompanyFinancialDetail(
  companyId: string,
): Promise<SeCompanyFinancialDetail> {
  const [latestRows, reports, ...sourceRows] = await Promise.all([
    chQuery<SeCompanyFinancialsLatestRow>(FINANCIALS_LATEST_SQL, { companyId }),
    chQuery<SeCompanyFinancialReportRow>(FINANCIAL_REPORTS_SQL, { companyId }),
    ...SOURCE_VIEWS.map((source) =>
      chQuery<SeCompanyFinancialYearRow>(
        financialSourceYearsSql(source.view),
        { companyId },
      ),
    ),
  ]);
  return {
    latest: latestRows[0] ?? null,
    sources: SOURCE_VIEWS.map((source, index) => ({
      source_id: source.source_id,
      view: source.view,
      years: sourceRows[index] ?? [],
    })),
    reports,
  };
}
