import { isIP } from "node:net";
import { chQuery } from "~/lib/clickhouse.server";
import { getUnifiedCompanyDomains } from "~/lib/company-domains.server";
import {
  COMPANY_FLAG_SOURCES,
  availableCompanyFlags,
  flagFilterKey,
  type CompanyFlagId,
} from "~/lib/company-flags";
import {
  getSortColumn,
  PAGE_SIZES,
  type CountryConfig,
  type FinancialSourceDefinition,
  type SortDir,
} from "~/lib/countries";
import type { CompanyFilters } from "~/lib/filters";
import type { FinancialReportDocumentSummary } from "~/lib/norway-financial-reports";
import { getNorwayFinancialReports } from "~/lib/norway-financial-reports.server";
import {
  buildWebTechnologyHistory,
  type CompanyWebTechnologyHistory,
  type WebTechnologyCrawlCoverage,
  type WebTechnologyCrawlDetection,
} from "~/lib/web-technology-history";

export interface CountryStats {
  total: number;
  active: number;
}

function clampInt(
  value: number | undefined,
  min: number,
  max: number,
  fallback: number,
): number {
  if (value === undefined || !Number.isFinite(value)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(value)));
}

export async function getCountryStats(
  country: CountryConfig,
): Promise<CountryStats> {
  // Table/column identifiers come from the static registry, never from users.
  const rows = await chQuery<{ total: string; active: string }>(
    `SELECT count() AS total, countIf(${country.activeExpr}) AS active
     FROM ${country.companiesTable}`,
  );
  const row = rows[0];
  return { total: Number(row.total), active: Number(row.active) };
}

// Re-exported for callers that historically imported PAGE_SIZES from here
// (e.g. Task 2's tests). Source of truth moved to ~/lib/countries so
// client-bundled code (data-table/pagination.tsx) doesn't import a .server
// module.
export { PAGE_SIZES };

export type CompanyListRow = Record<
  string,
  string | number | null | CompanyFlagId[]
> & {
  active: 0 | 1;
  /** Which kinds of data we hold for this company. See lib/company-flags. */
  flags?: CompanyFlagId[];
};

export interface CompanySearchResult {
  rows: CompanyListRow[];
  total: number;
  page: number;
  pageSize: number;
  sort: string;
  dir: SortDir;
}

export interface CompanyShell {
  company: CompanyListRow;
  record: Record<string, unknown>;
  industryKey: string;
}

interface IndustryRow {
  company_id: string;
  industry_code: string | null;
  industry_label: string | null;
}

/**
 * Flags a register keeps ON the company row, computed in the main select.
 *
 * The rest are existence checks against companion tables and are fetched
 * separately -- see attachCompanyFlags. Splitting them this way means a
 * register that stores its address inline costs no extra query at all.
 */

/**
 * A WHERE conjunct restricting to companies that do (or do not) hold a flag.
 *
 * Unlike the per-page flag lookup, this cannot be scoped to the visible ids --
 * it decides which rows exist at all, so each check is a semi-join against the
 * whole companion table. They are the same tables the flags are read from, so
 * the filter and the glyph can never disagree.
 *
 * Returns null when the country has no source for the flag, so an unknown
 * combination filters nothing rather than excluding everything.
 */
function flagFilterCondition(
  country: CountryConfig,
  flagId: CompanyFlagId,
  want: boolean,
): string | null {
  const source = COMPANY_FLAG_SOURCES[country.code.toLowerCase()]?.[flagId];
  if (!source) return null;

  if ("expr" in source) {
    const value = `coalesce(toString(${source.expr}), '')`;
    return want ? `${value} != ''` : `${value} = ''`;
  }

  const id = `toString(${country.idColumn})`;
  const subquery =
    "idQuery" in source
      ? source.idQuery
      : "market" in source
        ? `SELECT toString(company_id) FROM company_market_summary
         WHERE country_code = '${country.code.toUpperCase()}'`
        : `SELECT toString(${source.idColumn}) FROM ${source.table}`;
  return want ? `${id} IN (${subquery})` : `${id} NOT IN (${subquery})`;
}

export async function companyHasFlag(
  country: CountryConfig,
  id: string,
  flagId: CompanyFlagId,
): Promise<boolean> {
  const source = COMPANY_FLAG_SOURCES[country.code.toLowerCase()]?.[flagId];
  if (!source) return false;

  let query: string;
  if ("expr" in source) {
    query = `SELECT 1 AS found
      FROM ${country.companiesTable}
      WHERE ${country.idColumn} = {id:String}
        AND coalesce(toString(${source.expr}), '') != ''
      LIMIT 1`;
  } else if ("idQuery" in source) {
    query = `SELECT 1 AS found
      FROM (${source.idQuery})
      WHERE toString(company_id) = {id:String}
      LIMIT 1`;
  } else if ("market" in source) {
    query = `SELECT 1 AS found
      FROM company_market_summary
      WHERE country_code = '${country.code.toUpperCase()}'
        AND toString(company_id) = {id:String}
      LIMIT 1`;
  } else {
    query = `SELECT 1 AS found
      FROM ${source.table}
      WHERE toString(${source.idColumn}) = {id:String}
      LIMIT 1`;
  }

  const rows = await chQuery<{ found: number }>(query, { id });
  return rows.length > 0;
}

function flagSelectList(country: CountryConfig): string[] {
  const sources = COMPANY_FLAG_SOURCES[country.code.toLowerCase()] ?? {};
  return Object.entries(sources)
    .filter(([, source]) => "expr" in source)
    .map(
      ([id, source]) =>
        `toUInt8(coalesce(toString(${(source as { expr: string }).expr}), '') != '') AS __flag_${id}`,
    );
}

/**
 * Which of the flags each visible company can light, in one round trip.
 *
 * A UNION of existence checks rather than one query per flag: four kinds of
 * data would otherwise be four round trips per page, and each branch is an
 * indexed lookup of the ~50 ids actually on screen.
 */
async function attachCompanyFlags(
  country: CountryConfig,
  rows: CompanyListRow[],
): Promise<void> {
  const code = country.code.toLowerCase();
  const sources = COMPANY_FLAG_SOURCES[code] ?? {};
  const ids = rows.map((r) => String(r.id ?? "")).filter((v) => v !== "");

  // Inline flags are already in the row from the select above.
  for (const row of rows) {
    const held: CompanyFlagId[] = [];
    for (const flag of availableCompanyFlags(code)) {
      const key = `__flag_${flag.id}` as keyof CompanyListRow;
      const inline = row[key];
      if (
        typeof inline !== "object" &&
        inline !== undefined &&
        Number(inline) === 1
      ) {
        held.push(flag.id);
      }
      delete (row as Record<string, unknown>)[key as string];
    }
    row.flags = held;
  }
  if (ids.length === 0) return;

  const branches = Object.entries(sources).flatMap(([id, source]) => {
    if ("table" in source) {
      return [
        `SELECT toString(${source.idColumn}) AS company_id, '${id}' AS flag
         FROM ${source.table}
         WHERE toString(${source.idColumn}) IN {ids:Array(String)}`,
      ];
    }
    if ("idQuery" in source) {
      return [
        `SELECT toString(company_id) AS company_id, '${id}' AS flag
         FROM (${source.idQuery})
         WHERE toString(company_id) IN {ids:Array(String)}`,
      ];
    }
    if ("market" in source) {
      return [
        `SELECT toString(company_id) AS company_id, '${id}' AS flag
         FROM company_market_summary
         WHERE country_code = '${country.code.toUpperCase()}'
           AND toString(company_id) IN {ids:Array(String)}`,
      ];
    }
    return [];
  });
  if (branches.length === 0) return;

  const found = await chQuery<{ company_id: string; flag: CompanyFlagId }>(
    branches.join("\nUNION ALL\n"),
    { ids },
  );
  const byId = new Map<string, Set<CompanyFlagId>>();
  for (const hit of found) {
    let set = byId.get(hit.company_id);
    if (!set) byId.set(hit.company_id, (set = new Set()));
    set.add(hit.flag);
  }
  for (const row of rows) {
    const extra = byId.get(String(row.id ?? ""));
    if (extra) row.flags = [...(row.flags ?? []), ...extra];
  }
}

function buildCompanySelectList(country: CountryConfig): string {
  const joinKeyExpr = country.industryJoinKeyExpr ?? country.idColumn;
  return [
    ...country.columns.map((c) => `${c.expr} AS ${c.key}`),
    `toUInt8(${country.activeExpr}) AS active`,
    ...(country.industryQuery
      ? [`toString(${joinKeyExpr}) AS __industry_key`]
      : []),
    ...(country.placeQuery
      ? [`toString(${country.idColumn}) AS __place_key`]
      : []),
    ...flagSelectList(country),
  ].join(",\n       ");
}

/** Header data shared by every company sub-route. This deliberately avoids
 * the detail page's many section queries, so opening a report does not also
 * fetch contracts, domains, officers, ESEF filings, and other overview data. */
export async function getCompanyShell(
  country: CountryConfig,
  id: string,
): Promise<CompanyShell | null> {
  const companyShellQuery = country.detail?.companyShellQuery;
  if (companyShellQuery) {
    const [shellRecord] = await chQuery<Record<string, unknown>>(
      companyShellQuery,
      { id },
    );
    if (!shellRecord) return null;

    const company: CompanyListRow = {
      active: Number(shellRecord.__shell_active) === 1 ? 1 : 0,
    };
    for (const column of country.columns) {
      company[column.key] = shellRecord[`__shell_${column.key}`] as
        string | number | null;
    }
    const industryKey = String(shellRecord.__shell_industry_key ?? "");
    for (const key of Object.keys(shellRecord)) {
      if (key.startsWith("__shell_")) delete shellRecord[key];
    }
    return { company, record: shellRecord, industryKey };
  }

  const selectList = buildCompanySelectList(country);
  const [companies, records] = await Promise.all([
    chQuery<CompanyListRow & { __industry_key?: string }>(
      `SELECT ${selectList}
       FROM ${country.companiesTable}
       WHERE ${country.idColumn} = {id:String}
       LIMIT 1`,
      { id },
    ),
    chQuery<Record<string, unknown>>(
      country.detail?.recordQuery ??
        `SELECT * FROM ${country.companiesTable}
         WHERE ${country.idColumn} = {id:String}
         LIMIT 1`,
      { id },
    ),
  ]);
  const company = companies[0];
  if (!company) return null;
  const industryKey = String(company.__industry_key ?? "");
  delete company.__industry_key;
  return { company, record: records[0] ?? {}, industryKey };
}

export async function searchCompanies(
  country: CountryConfig,
  opts: {
    q?: string;
    page?: number;
    pageSize?: number;
    sort?: string | null;
    dir?: string | null;
    filters?: CompanyFilters;
  },
): Promise<CompanySearchResult> {
  const pageSize = PAGE_SIZES.includes(opts.pageSize as 25 | 50 | 100)
    ? (opts.pageSize as number)
    : 50;
  const requestedPage = clampInt(opts.page, 1, Number.MAX_SAFE_INTEGER, 1);
  const q = (opts.q ?? "").trim();
  const sortColumn = getSortColumn(country, opts.sort ?? null);
  const dir: SortDir = opts.dir === "desc" ? "desc" : "asc";

  const conds: string[] = [];
  const params: Record<string, unknown> = {};
  if (q) {
    conds.push(
      `${country.searchColumnExpr ?? country.nameColumn} ILIKE {pattern:String}`,
    );
    params.pattern = `%${q}%`;
  }
  for (const column of country.columns) {
    if (!column.filterable) continue;
    const values = opts.filters?.[column.key];
    if (!values || values.length === 0) continue;
    // Column expr from registry; values bound as an Array(String) param.
    conds.push(`${column.expr} IN {f_${column.key}:Array(String)}`);
    params[`f_${column.key}`] = values;
  }
  for (const flag of availableCompanyFlags(country.code)) {
    const values = opts.filters?.[flagFilterKey(flag.id)];
    if (!values || values.length === 0) continue;
    const yes = values.includes("yes");
    const no = values.includes("no");
    // Both ticked is every company, which is the same as no filter at all.
    if (yes === no) continue;
    const condition = flagFilterCondition(country, flag.id, yes);
    if (condition) conds.push(condition);
  }

  const industryValues = opts.filters?.industry;
  if (industryValues?.length && country.industryFilterExpr) {
    conds.push(country.industryFilterExpr);
    params.f_industry = industryValues;
  }
  const where = conds.length > 0 ? `WHERE ${conds.join(" AND ")}` : "";

  const countRows = await chQuery<{ total: string }>(
    `SELECT count() AS total FROM ${country.companiesTable} ${where}`,
    params,
  );
  const total = Number(countRows[0].total);

  // Clamp the requested page to the real page range (count runs first on purpose).
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  const page = Math.min(requestedPage, lastPage);

  const selectList = buildCompanySelectList(country);

  const rows = await chQuery<CompanyListRow & { __industry_key?: string }>(
    `SELECT ${selectList}
     FROM ${country.companiesTable}
     ${where}
     ORDER BY coalesce(toString(${sortColumn.expr}), '') = '' ASC, ${sortColumn.expr} ${dir === "desc" ? "DESC" : "ASC"}, ${country.idColumn}
     LIMIT ${pageSize} OFFSET ${(page - 1) * pageSize}`,
    params,
  );

  if (country.industryQuery) {
    const ids = rows.map((r) => r.__industry_key ?? "").filter((v) => v !== "");
    const industries = ids.length
      ? await chQuery<IndustryRow>(country.industryQuery, { ids })
      : [];
    const byId = new Map(industries.map((i) => [i.company_id, i]));
    for (const row of rows) {
      const hit = byId.get(row.__industry_key ?? "");
      row.industry_code = hit?.industry_code ?? null;
      row.industry_label = hit?.industry_label ?? null;
      delete row.__industry_key;
    }
  }

  if (country.placeQuery) {
    const ids = rows
      .map((r) => String(r.__place_key ?? ""))
      .filter((v) => v !== "");
    const places = ids.length
      ? await chQuery<{ company_id: string; place: string }>(
          country.placeQuery,
          { ids },
        )
      : [];
    const byId = new Map(places.map((p) => [p.company_id, p.place]));
    for (const row of rows) {
      row.place = byId.get(String(row.__place_key ?? "")) ?? null;
      delete row.__place_key;
    }
  }

  await attachCompanyFlags(country, rows);

  return { rows, total, page, pageSize, sort: sortColumn.key, dir };
}

export interface FinancialYearRow {
  fiscal_year: string;
  currency: string;
  revenue_amount_original: number | null;
  revenue_amount_usd: number | null;
  net_result_amount_original: number | null;
  net_result_amount_usd: number | null;
  total_assets_amount_original: number | null;
  total_assets_amount_usd: number | null;
  equity_amount_original: number | null;
  equity_amount_usd: number | null;
  employees: number | null;
  /** "filed" (actual filing) vs "comparative" (recovered from a later
   * filing's multi-year overview). Absent for countries without history. */
  observation?: "filed" | "comparative";
  /** For comparative rows: fiscal year of the filing that carried the figures. */
  source_fiscal_year?: string;
  source_record_uid?: string;
  evidence?: EvidenceRef[];
}

export async function getCompanyFinancials(
  country: CountryConfig,
  id: string,
): Promise<FinancialYearRow[]> {
  if (!country.detail?.financialsQuery) return [];
  return chQuery<FinancialYearRow>(country.detail.financialsQuery, { id });
}

/** Public corporate income tax data (tax base + assessed taxes), one row per
 * tax year. Semantically distinct from FinancialYearRow: taxable income is a
 * tax-base figure, not accounting profit — never merge the two sections. */
/** One public procurement contract win, in the CANONICAL cross-country shape
 * produced by every publicContractsQuery (source portal + TED union). */
export interface PublicContractRow {
  source: string;
  notice_ref: string;
  contract_date: string;
  buyer_name: string;
  title: string;
  /** Attributable to this winner. Null where the source publishes no figure
   * that can be attributed to one -- Sweden's UHM publishes no value at all,
   * and any source may simply omit it on a given notice. Hilma's comes from
   * its lot-grain value, which is one company's amount wherever a lot has a
   * single winner; on such a notice it legitimately equals the notice total. */
  amount_original: number | null;
  amount_usd: number | null;
  currency: string;
  /** The whole notice's value, repeated across its winners. Shown as a
   * fallback and labelled as such; summing it across rows is meaningless. */
  notice_amount_original: number | null;
  notice_amount_usd: number | null;
  notice_currency: string;
  /** The source document this row came from. Empty when the source publishes
   * no per-contract address (Sweden's UHM is a bulk CSV). */
  source_url: string;
}

/**
 * A company's award history in one row, for the header above the contracts
 * table.
 *
 * total_value_usd and valued_count are null/0 wherever a register publishes no
 * per-winner figure -- which is every French company. The header shows a value
 * only when one exists: printing "total value: 0" would say the company won
 * nothing.
 */
export interface ContractSummaryRow {
  award_count: number | string;
  valued_count: number | string;
  total_value_usd: number | null;
  last_award_date: string;
  sources: string;
}

/**
 * One fiscal year of a French filing, with the ratio suite INPI publishes.
 *
 * Distinct from FinancialYearRow: France carries gross margin, EBITDA, EBIT
 * and fourteen ratio and working-capital-day columns that the canonical shape
 * has no room for, and carries neither equity nor total assets, which it has.
 *
 * A null figure means WITHHELD, not zero -- 23.5% of French filings are
 * partially confidential and may legally omit lines.
 */
export interface FrFinancialRow {
  fiscal_year: string;
  /** 'C' complete, 'S' simplified, 'K' consolidated. */
  balance_type: string;
  /** 'Public' | 'Partiellement confidentiel' | 'Partiellement confidentiel (RAPCAC)' | 'Publication simplifiee'. */
  confidentiality: string;
  currency: string;
  revenue_original: number | null;
  revenue_usd: number | null;
  gross_margin_original: number | null;
  gross_margin_usd: number | null;
  ebitda_original: number | null;
  ebitda_usd: number | null;
  ebit_original: number | null;
  ebit_usd: number | null;
  net_income_original: number | null;
  net_income_usd: number | null;
  ebitda_margin_percent: number | null;
  debt_ratio_percent: number | null;
  financial_autonomy_percent: number | null;
  liquidity_ratio_percent: number | null;
  interest_coverage_percent: number | null;
  customer_payment_days: number | null;
  supplier_payment_days: number | null;
  inventory_turnover_days: number | null;
  period_end_date: string;
}

export interface TaxRecordRow {
  tax_year: string;
  currency: string;
  municipality_name: string;
  taxable_income_amount_original: number | null;
  taxable_income_amount_usd: number | null;
  taxes_total_amount_original: number | null;
  taxes_total_amount_usd: number | null;
  prepayments_total_amount_original: number | null;
  prepayments_total_amount_usd: number | null;
  tax_refund_amount_original: number | null;
  tax_refund_amount_usd: number | null;
  residual_tax_amount_original: number | null;
  residual_tax_amount_usd: number | null;
}

export interface FactRow {
  concept: string;
  concept_local_name?: string | null;
  /** Taxonomy concept metadata is currently returned by Sweden's factsQuery;
   * other countries may omit it. */
  concept_label_en?: string | null;
  concept_label_sv?: string | null;
  concept_description_en?: string | null;
  concept_description_sv?: string | null;
  concept_label_en_source?: string | null;
  concept_label_translation_provider?: string | null;
  concept_label_translation_model?: string | null;
  concept_label_translation_version?: number | null;
  concept_description_en_source?: string | null;
  concept_description_translation_provider?: string | null;
  concept_description_translation_model?: string | null;
  concept_description_translation_version?: number | null;
  concept_taxonomy_entrypoint?: string | null;
  concept_source_url?: string | null;
  value_kind: string;
  raw_value: string;
  amount_original: number | null;
  amount_usd: number | null;
  fx_rate_date?: string | null;
  fx_source?: string | null;
  unit_id?: string | null;
  decimals?: string | null;
  currency: string | null;
  date_value: string | null;
  text_value: string | null;
  dimensions: string;
  context_id: string;
}

/** Raw source facts for one fiscal year's filing; empty when the country has
 * no factsQuery or the year's facts aren't loaded. */
export async function getCompanyFacts(
  country: CountryConfig,
  id: string,
  year: number,
): Promise<FactRow[]> {
  if (!country.detail?.factsQuery) return [];
  return chQuery<FactRow>(country.detail.factsQuery, { id, year });
}

export interface FactsDocument {
  object_key: string;
  source_uri: string;
  archive_url: string;
  archive_name: string;
  nested_zip_name: string;
}

/** Locates the original source document for one fiscal year's filing. */
export async function getFactsDocument(
  country: CountryConfig,
  id: string,
  year: number,
): Promise<FactsDocument | null> {
  if (!country.detail?.factsDocumentQuery) return null;
  const rows = await chQuery<FactsDocument>(country.detail.factsDocumentQuery, {
    id,
    year,
  });
  return rows[0] ?? null;
}

export interface ContactRow {
  contact_type: string;
  contact_value: string;
  evidence?: EvidenceRef[];
}

export interface DomainRow {
  domain: string;
  website_url: string | null;
  domain_source: string;
  confidence: number | null;
  is_primary: 0 | 1;
  source_names?: string[];
  review_status?: string;
  evidence_changed?: boolean;
  evidence?: EvidenceRef[];
}

export interface CompanyTechnologyDetail {
  domains: DomainRow[];
  selectedDomain: string;
  webTechnologyHistory: CompanyWebTechnologyHistory | null;
}

export async function getCompanyDomains(
  country: CountryConfig,
  id: string,
): Promise<DomainRow[]> {
  if (country.code === "se") {
    const domains = (await getUnifiedCompanyDomains(country.code, id)).filter(
      (domain) => domain.active && domain.reviewStatus !== "rejected",
    );
    const hasConfirmedPrimary = domains.some(
      (domain) => domain.reviewStatus === "confirmed_primary",
    );
    return domains.map((domain) => ({
      domain: domain.rootDomain,
      website_url: domain.websiteUrl || null,
      domain_source: domain.sources.map((source) => source.name).join(" + "),
      confidence: domain.suggestedConfidence,
      is_primary:
        domain.reviewStatus === "confirmed_primary" ||
        (!hasConfirmedPrimary && domain.suggestedPrimary)
          ? 1
          : 0,
      source_names: domain.sources.map((source) => source.name),
      review_status: domain.reviewStatus,
      evidence_changed: domain.evidenceChanged,
    }));
  }
  return country.detail?.domainsQuery
    ? chQuery<DomainRow>(country.detail.domainsQuery, { id })
    : [];
}

function selectedCompanyDomain(
  domains: DomainRow[],
  requestedDomain?: string,
): DomainRow | undefined {
  const normalized = requestedDomain?.trim().toLowerCase().replace(/\.$/, "");
  return (
    domains.find((domain) => domain.domain === normalized) ??
    domains.find((domain) => domain.is_primary === 1) ??
    domains[0]
  );
}

interface WebTechnologyCoverageRow {
  crawl_id: string;
  observed_pages: string;
  processed_at: string;
}

interface WebTechnologyDetectionRow {
  crawl_id: string;
  technology: string;
  categories: string[];
  detected_versions: string[];
  confidence: number;
  detected_pages: string;
  sample_urls: string[];
}

export async function getCompanyWebTechnologyHistory(
  domain: string,
): Promise<CompanyWebTechnologyHistory | null> {
  const [coverageRows, detectionRows] = await Promise.all([
    chQuery<WebTechnologyCoverageRow>(
      `SELECT
        crawl_id,
        toString(uniqExact(source_url)) AS observed_pages,
        toString(max(resolved_at)) AS processed_at
      FROM commoncrawl_domains
      WHERE root_domain = {domain:String}
      GROUP BY crawl_id
      ORDER BY crawl_id`,
      { domain },
    ),
    chQuery<WebTechnologyDetectionRow>(
      `SELECT
        crawl_id,
        toString(technology) AS technology,
        arraySort(groupUniqArray(toString(category))) AS categories,
        arraySort(arrayFilter(value -> notEmpty(value),
          groupUniqArray(toString(version)))) AS detected_versions,
        toUInt8(max(confidence)) AS confidence,
        toString(uniqExact(page_url)) AS detected_pages,
        arraySlice(arraySort(groupUniqArray(page_url)), 1, 5) AS sample_urls
      FROM commoncrawl_page_technologies
      WHERE root_domain = {domain:String}
      GROUP BY crawl_id, technology
      ORDER BY crawl_id, technology`,
      { domain },
    ),
  ]);
  if (coverageRows.length === 0 && detectionRows.length === 0) return null;

  const coverage = coverageRows.map<WebTechnologyCrawlCoverage>((row) => ({
    crawlId: row.crawl_id,
    observedPages: Number(row.observed_pages),
    processedAt: row.processed_at,
  }));
  const detections = detectionRows.map<WebTechnologyCrawlDetection>((row) => ({
    crawlId: row.crawl_id,
    name: row.technology,
    categories: row.categories,
    versions: row.detected_versions,
    confidence: row.confidence,
    detectedPages: Number(row.detected_pages),
    sampleUrls: row.sample_urls,
  }));
  return buildWebTechnologyHistory(domain, coverage, detections);
}

export async function getCompanyTechnologyDetail(
  country: CountryConfig,
  id: string,
  requestedDomain?: string,
): Promise<CompanyTechnologyDetail> {
  const domains = await getCompanyDomains(country, id);
  const selectedDomain = selectedCompanyDomain(domains, requestedDomain);
  const webTechnologyHistory = selectedDomain
    ? await getCompanyWebTechnologyHistory(selectedDomain.domain)
    : null;
  return {
    domains,
    selectedDomain: selectedDomain?.domain ?? "",
    webTechnologyHistory,
  };
}

export type TechnologyHostnameEvidence = "certificate" | "dns";

export interface TechnologyDnsRecord {
  type: string;
  value: string;
  priority: number;
  sources: string[];
  discoveries: string[];
  seenDates: string[];
  firstSeen: string;
  lastSeen: string;
}

export interface TechnologyIpAddress {
  ip: string;
  version: 4 | 6;
  networkSegment: string;
  firstSeen: string;
  lastSeen: string;
  countryCode: string | null;
  countryName: string | null;
  cityName: string | null;
  asn: number | null;
  asnOrganization: string | null;
  rdapRegistration: TechnologyIpRdapRegistration | null;
}

export interface TechnologyIpRdapRegistration {
  networkKey: string;
  matchedCidr: string;
  rir: string;
  handle: string;
  name: string | null;
  registrationType: string | null;
  countryCode: string | null;
  statuses: string[];
  registrantNames: string[];
  startAddress: string;
  endAddress: string;
  registrationDate: string | null;
  lastChangedAt: string | null;
  fetchedAt: string;
  sourceUrl: string | null;
}

export interface TechnologyHostname {
  hostname: string;
  label: string;
  isApex: boolean;
  isWildcard: boolean;
  evidence: TechnologyHostnameEvidence[];
  certificateFirstSeen: string | null;
  certificateLastSeen: string | null;
  certificateExpiresAt: string | null;
  certificateSourceLogs: string[];
  dnsFirstSeen: string | null;
  dnsLastSeen: string | null;
  dnsDiscoverySource: string | null;
  hasIpv4: boolean;
  hasIpv6: boolean;
  hasCname: boolean;
  records: TechnologyDnsRecord[];
  ipAddresses: TechnologyIpAddress[];
}

export interface TechnologyDnsScan {
  status: string;
  resolvedAt: string;
  nameservers: string[];
  nameserverIps: string[];
  queriesTotal: number;
  queriesOk: number;
  dnssecSigned: boolean;
  dsOutcome: string;
  dnskeyOutcome: string;
  zoneTransferOpen: boolean;
}

export interface CompanyTechnologyInfrastructure {
  domain: string;
  page: number;
  pageSize: number;
  summary: {
    totalHostnames: number;
    certificateHostnames: number;
    dnsHostnames: number;
    resolvedIpAddressesOnPage: number;
    rdapRegisteredIpAddressesOnPage: number;
  };
  scan: TechnologyDnsScan | null;
  hostnames: TechnologyHostname[];
}

export interface CompanyTechnologyIpInventoryAddress extends TechnologyIpAddress {
  hostnames: string[];
}

export interface CompanyTechnologyIpInventory {
  domain: string;
  page: number;
  pageSize: number;
  summary: {
    totalAddresses: number;
    ipv4Addresses: number;
    ipv6Addresses: number;
    rdapRegisteredAddressesOnPage: number;
  };
  addresses: CompanyTechnologyIpInventoryAddress[];
}

export interface TechnologyIpDomainConnection {
  ip: string;
  version: 4 | 6;
  domain: string;
  hostnames: string[];
  sources: string[];
  discoveries: string[];
  firstSeen: string;
  lastSeen: string;
}

export interface TechnologyIpDomainConnectionPage {
  page: number;
  pageSize: number;
  total: number | null;
  hasMore: boolean;
  connections: TechnologyIpDomainConnection[];
}

export interface TechnologyIpDetail {
  address: TechnologyIpAddress;
  historyIndexCoverage: {
    completedPartitions: number;
    totalPartitions: number;
  };
  exactConnections: TechnologyIpDomainConnectionPage;
  segmentConnections: TechnologyIpDomainConnectionPage;
}

export interface CompanyTechnologyIpDetail extends TechnologyIpDetail {
  companyDomain: string;
  companyHostnames: string[];
}

interface TechnologyHostnameRow {
  hostname: string;
  label: string;
  has_certificate: 0 | 1;
  has_dns: 0 | 1;
  is_wildcard: 0 | 1;
  certificate_first_seen: string;
  certificate_last_seen: string;
  certificate_expires_at: string;
  certificate_source_logs: string[];
  dns_first_seen: string;
  dns_last_seen: string;
  dns_discovery_source: string;
  has_ipv4: 0 | 1;
  has_ipv6: 0 | 1;
  has_cname: 0 | 1;
  total_hostnames: string;
  certificate_hostnames: string;
  dns_hostnames: string;
}

interface TechnologyDnsRecordRow {
  hostname: string;
  type: string;
  value: string;
  priority: number;
  sources: string[];
  discoveries: string[];
  seen_dates: string[];
  first_seen: string;
  last_seen: string;
}

interface TechnologyIpEnrichmentRow {
  ip: string;
  country_code: string | null;
  country_name: string | null;
  city_name: string | null;
  asn: number | null;
  asn_organization: string | null;
}

interface TechnologyIpRdapRow {
  ip: string;
  network_key: string;
  matched_cidr: string;
  rir: string;
  handle: string;
  name: string | null;
  registration_type: string | null;
  country_code: string | null;
  statuses: string[];
  registrant_names: string[];
  start_address: string;
  end_address: string;
  registration_date: string | null;
  last_changed_at: string | null;
  fetched_at: string;
  source_url: string | null;
}

interface TechnologyIpMetadata {
  networkSegment: string;
  countryCode: string | null;
  countryName: string | null;
  cityName: string | null;
  asn: number | null;
  asnOrganization: string | null;
  rdapRegistration: TechnologyIpRdapRegistration | null;
}

interface TechnologyIpInventoryRow {
  ip: string;
  version: 4 | 6;
  hostnames: string[];
  first_seen: string;
  last_seen: string;
  total_addresses: string;
  ipv4_addresses: string;
  ipv6_addresses: string;
}

interface TechnologyIpDomainConnectionRow {
  ip: string;
  version: 4 | 6;
  domain: string;
  hostnames: string[];
  sources: string[];
  discoveries: string[];
  first_seen: string;
  last_seen: string;
  address_first_seen?: string;
  address_last_seen?: string;
  total_connections?: string;
}

interface TechnologyDnsScanRow {
  status: string;
  resolved_at: string;
  nameservers: string[];
  nameserver_ips: string[];
  queries_total: number;
  queries_ok: number;
  dnssec_signed: 0 | 1;
  ds_outcome: string;
  dnskey_outcome: string;
  zone_transfer_open: 0 | 1;
}

const technologyHostnameInventorySql = `WITH evidence AS (
  SELECT
    fqdn AS hostname,
    if(fqdn = {domain:String}, '',
      substring(fqdn, 1, length(fqdn) - length({domain:String}) - 1)
    ) AS label,
    toUInt8(1) AS has_certificate,
    toUInt8(0) AS has_dns,
    toUInt8(max(is_wildcard)) AS is_wildcard,
    toString(min(first_seen)) AS certificate_first_seen,
    toString(max(last_seen)) AS certificate_last_seen,
    toString(max(last_not_after)) AS certificate_expires_at,
    arraySort(arrayDistinct(arrayFlatten(groupArray(source_logs)))) AS certificate_source_logs,
    '' AS dns_first_seen,
    '' AS dns_last_seen,
    '' AS dns_discovery_source,
    toUInt8(0) AS has_ipv4,
    toUInt8(0) AS has_ipv6,
    toUInt8(0) AS has_cname
  FROM ctlogs.hostnames
  WHERE registered_domain = {domain:String}
  GROUP BY fqdn
  UNION ALL
  SELECT
    hostname,
    substring(
      hostname,
      1,
      greatest(
        toInt64(length(hostname)) - toInt64(length({domain:String})) - 1,
        0
      )
    ),
    toUInt8(0),
    toUInt8(1),
    toUInt8(0),
    '',
    '',
    '',
    CAST([], 'Array(String)'),
    toString(min(first_seen)),
    toString(max(last_seen)),
    multiIf(
      max(discovery_rank) = 3, 'axfr',
      max(discovery_rank) = 2, 'ct',
      max(discovery_rank) = 1, 'static',
      'unknown'
    ),
    toUInt8(max(has_ipv4)),
    toUInt8(max(has_ipv6)),
    toUInt8(max(has_cname))
  FROM domain_hostnames_state
  WHERE root_domain = {domain:String}
  GROUP BY hostname
), inventory AS (
  SELECT
    hostname,
    any(label) AS label,
    max(evidence.has_certificate) AS has_certificate,
    max(evidence.has_dns) AS has_dns,
    max(evidence.is_wildcard) AS is_wildcard,
    anyIf(evidence.certificate_first_seen, evidence.has_certificate = 1) AS certificate_first_seen,
    anyIf(evidence.certificate_last_seen, evidence.has_certificate = 1) AS certificate_last_seen,
    anyIf(evidence.certificate_expires_at, evidence.has_certificate = 1) AS certificate_expires_at,
    anyIf(evidence.certificate_source_logs, evidence.has_certificate = 1) AS certificate_source_logs,
    anyIf(evidence.dns_first_seen, evidence.has_dns = 1) AS dns_first_seen,
    anyIf(evidence.dns_last_seen, evidence.has_dns = 1) AS dns_last_seen,
    anyIf(evidence.dns_discovery_source, evidence.has_dns = 1) AS dns_discovery_source,
    max(evidence.has_ipv4) AS has_ipv4,
    max(evidence.has_ipv6) AS has_ipv6,
    max(evidence.has_cname) AS has_cname
  FROM evidence
  GROUP BY hostname
)
SELECT
  inventory.*,
  toString(count() OVER ()) AS total_hostnames,
  toString(countIf(has_certificate = 1) OVER ()) AS certificate_hostnames,
  toString(countIf(has_dns = 1) OVER ()) AS dns_hostnames
FROM inventory
ORDER BY hostname != {domain:String}, hostname
LIMIT {limit:UInt32} OFFSET {offset:UInt64}`;

const technologyDnsRecordsSql = `SELECT
  name AS hostname,
  toString(record_type) AS type,
  toString(value) AS value,
  toUInt16(priority) AS priority,
  arraySort(arrayDistinct(arrayFlatten(groupArray(sources)))) AS sources,
  arraySort(arrayDistinct(arrayFlatten(groupArray(discoveries)))) AS discoveries,
  arrayMap(date -> toString(date),
    arraySort(arrayDistinct(arrayFlatten(groupArray(seen_dates))))) AS seen_dates,
  toString(min(first_seen)) AS first_seen,
  toString(max(last_seen)) AS last_seen
FROM commoncrawl_domain_dns_records
WHERE root_domain = {domain:String}
  AND name IN {hostnames:Array(String)}
  AND record_type != 'RRSIG'
GROUP BY name, record_type, value, priority
ORDER BY hostname, type, priority, value`;

const technologyIpEnrichmentSql = `SELECT
  ip,
  argMax(country_iso_code, enriched_at) AS country_code,
  argMax(country_name, enriched_at) AS country_name,
  argMax(city_name, enriched_at) AS city_name,
  argMax(asn, enriched_at) AS asn,
  argMax(asn_organization, enriched_at) AS asn_organization
FROM commoncrawl_ip_geoip
PREWHERE (bucket, ip) IN arrayZip(
  {buckets:Array(UInt16)},
  {ips:Array(String)}
)
GROUP BY ip`;

const technologyIpRdapSql = `WITH matched AS (
  SELECT
    ip,
    dictGetOrDefault(
      'corpscout.rdap_network_trie',
      'network_key',
      tuple(toIPv4(ip)),
      ''
    ) AS network_key,
    dictGetOrDefault(
      'corpscout.rdap_network_trie',
      'matched_cidr',
      tuple(toIPv4(ip)),
      ''
    ) AS matched_cidr
  FROM (SELECT arrayJoin({ipv4s:Array(String)}) AS ip)
  UNION ALL
  SELECT
    ip,
    dictGetOrDefault(
      'corpscout.rdap_network_trie',
      'network_key',
      tuple(toIPv6(ip)),
      ''
    ) AS network_key,
    dictGetOrDefault(
      'corpscout.rdap_network_trie',
      'matched_cidr',
      tuple(toIPv6(ip)),
      ''
    ) AS matched_cidr
  FROM (SELECT arrayJoin({ipv6s:Array(String)}) AS ip)
)
SELECT
  matched.ip,
  registration.network_key,
  matched.matched_cidr,
  toString(registration.rir) AS rir,
  registration.handle,
  registration.name,
  registration.registration_type,
  registration.country_code,
  registration.status AS statuses,
  registration.registrant_names,
  registration.start_address,
  registration.end_address,
  toString(registration.registration_date) AS registration_date,
  toString(registration.last_changed_at) AS last_changed_at,
  toString(registration.fetched_at) AS fetched_at,
  registration.self_url AS source_url
FROM matched
INNER JOIN rdap_networks_current AS registration
  ON matched.network_key = registration.network_key
WHERE matched.network_key != ''
  AND matched.matched_cidr NOT IN ('0.0.0.0/0', '::/0')`;

function mapTechnologyIpRdapRegistration(
  row: TechnologyIpRdapRow | undefined,
): TechnologyIpRdapRegistration | null {
  if (!row) return null;
  return {
    networkKey: row.network_key,
    matchedCidr: row.matched_cidr,
    rir: row.rir,
    handle: row.handle,
    name: row.name,
    registrationType: row.registration_type,
    countryCode: row.country_code,
    statuses: row.statuses,
    registrantNames: row.registrant_names,
    startAddress: row.start_address,
    endAddress: row.end_address,
    registrationDate: row.registration_date,
    lastChangedAt: row.last_changed_at,
    fetchedAt: row.fetched_at,
    sourceUrl: row.source_url,
  };
}

async function getTechnologyIpMetadata(
  addresses: Array<{ ip: string; version: 4 | 6 }>,
): Promise<Map<string, TechnologyIpMetadata>> {
  const validAddresses = Array.from(
    new Map(
      addresses
        .filter((address) => isIP(address.ip) === address.version)
        .map((address) => [address.ip, address]),
    ).values(),
  );
  if (validAddresses.length === 0) return new Map();

  const ips = validAddresses.map((address) => address.ip);
  const [addressRows, rdapRows] = await Promise.all([
    chQuery<{ ip: string; bucket: number; network_segment: string }>(
      `SELECT
         ip,
         toUInt16(cityHash64(ip) % 256) AS bucket,
         if(
           isIPv4String(ip),
           concat(toString(tupleElement(IPv4CIDRToRange(toIPv4(ip), 24), 1)), '/24'),
           concat(toString(tupleElement(IPv6CIDRToRange(toIPv6(ip), 48), 1)), '/48')
         ) AS network_segment
       FROM (SELECT arrayJoin({ips:Array(String)}) AS ip)`,
      { ips },
    ),
    chQuery<TechnologyIpRdapRow>(technologyIpRdapSql, {
      ipv4s: validAddresses
        .filter((address) => address.version === 4)
        .map((address) => address.ip),
      ipv6s: validAddresses
        .filter((address) => address.version === 6)
        .map((address) => address.ip),
    }),
  ]);
  const enrichmentRows = await chQuery<TechnologyIpEnrichmentRow>(
    technologyIpEnrichmentSql,
    {
      ips: addressRows.map((row) => row.ip),
      buckets: addressRows.map((row) => row.bucket),
    },
  );
  const enrichmentByIp = new Map(
    enrichmentRows.map((enrichment) => [enrichment.ip, enrichment]),
  );
  const rdapByIp = new Map(rdapRows.map((row) => [row.ip, row]));
  const addressByIp = new Map(addressRows.map((row) => [row.ip, row]));

  return new Map(
    validAddresses.map((address) => {
      const enrichment = enrichmentByIp.get(address.ip);
      return [
        address.ip,
        {
          networkSegment:
            addressByIp.get(address.ip)?.network_segment ?? address.ip,
          countryCode: enrichment?.country_code ?? null,
          countryName: enrichment?.country_name ?? null,
          cityName: enrichment?.city_name ?? null,
          asn: enrichment?.asn ?? null,
          asnOrganization: enrichment?.asn_organization ?? null,
          rdapRegistration: mapTechnologyIpRdapRegistration(
            rdapByIp.get(address.ip),
          ),
        },
      ];
    }),
  );
}

function groupTechnologyRows<T extends { hostname: string }>(
  rows: T[],
): Map<string, T[]> {
  const grouped = new Map<string, T[]>();
  for (const row of rows) {
    const values = grouped.get(row.hostname) ?? [];
    values.push(row);
    grouped.set(row.hostname, values);
  }
  return grouped;
}

/** Combines CT observations with DNS-confirmed names for the selected domain. */
export async function getCompanyTechnologyInfrastructure(
  country: CountryConfig,
  id: string,
  opts: { domain?: string; page?: number; pageSize?: number } = {},
): Promise<CompanyTechnologyInfrastructure | null> {
  const domains = await getCompanyDomains(country, id);
  const selectedDomain = selectedCompanyDomain(domains, opts.domain);
  if (!selectedDomain) return null;

  const domain = selectedDomain.domain;
  const pageSize = PAGE_SIZES.includes(opts.pageSize as 25 | 50 | 100)
    ? (opts.pageSize as number)
    : 50;
  const requestedPage = clampInt(opts.page, 1, Number.MAX_SAFE_INTEGER, 1);
  const loadHostnamePage = (page: number) =>
    chQuery<TechnologyHostnameRow>(technologyHostnameInventorySql, {
      domain,
      limit: pageSize,
      offset: (page - 1) * pageSize,
    });
  let page = requestedPage;
  let [hostnameRows, scanRows] = await Promise.all([
    loadHostnamePage(page),
    chQuery<TechnologyDnsScanRow>(
      `SELECT
        toString(status) AS status,
        toString(resolved_at) AS resolved_at,
        nameservers,
        ns_ips AS nameserver_ips,
        queries_total,
        queries_ok,
        dnssec_signed,
        toString(ds_outcome) AS ds_outcome,
        toString(dnskey_outcome) AS dnskey_outcome,
        axfr_open AS zone_transfer_open
      FROM commoncrawl_domain_dns_scan FINAL
      WHERE root_domain = {domain:String}
      LIMIT 1`,
      { domain },
    ),
  ]);
  if (hostnameRows.length === 0 && requestedPage > 1) {
    const firstPageRows = await loadHostnamePage(1);
    const total = Number(firstPageRows[0]?.total_hostnames ?? 0);
    page = Math.max(1, Math.ceil(total / pageSize));
    hostnameRows = page === 1 ? firstPageRows : await loadHostnamePage(page);
  }
  const summaryRow = hostnameRows[0];
  const totalHostnames = Number(summaryRow?.total_hostnames ?? 0);
  const hostnameNames = hostnameRows.map((row) => row.hostname);
  const recordRows = hostnameNames.length
    ? await chQuery<TechnologyDnsRecordRow>(technologyDnsRecordsSql, {
        domain,
        hostnames: hostnameNames,
      })
    : [];
  const resolvedIpRecords = recordRows.filter(
    (record) => record.type === "A" || record.type === "AAAA",
  );
  const resolvedIps = Array.from(
    new Set(resolvedIpRecords.map((record) => record.value)),
  );
  const metadataByIp = await getTechnologyIpMetadata(
    resolvedIpRecords.map((record) => ({
      ip: record.value,
      version: record.type === "A" ? 4 : 6,
    })),
  );
  const ipRows = resolvedIpRecords.map((record) => {
    const metadata = metadataByIp.get(record.value);
    return {
      hostname: record.hostname,
      ip: record.value,
      version: (record.type === "A" ? 4 : 6) as 4 | 6,
      first_seen: record.first_seen,
      last_seen: record.last_seen,
      metadata,
    };
  });
  const recordsByHostname = groupTechnologyRows(recordRows);
  const ipsByHostname = groupTechnologyRows(ipRows);

  const hostnames = hostnameRows.map<TechnologyHostname>((row) => ({
    hostname: row.hostname,
    label: row.label,
    isApex: row.hostname === domain,
    isWildcard: row.is_wildcard === 1,
    evidence: [
      ...(row.has_certificate === 1
        ? (["certificate"] as TechnologyHostnameEvidence[])
        : []),
      ...(row.has_dns === 1 ? (["dns"] as TechnologyHostnameEvidence[]) : []),
    ],
    certificateFirstSeen: row.certificate_first_seen || null,
    certificateLastSeen: row.certificate_last_seen || null,
    certificateExpiresAt: row.certificate_expires_at || null,
    certificateSourceLogs: row.certificate_source_logs,
    dnsFirstSeen: row.dns_first_seen || null,
    dnsLastSeen: row.dns_last_seen || null,
    dnsDiscoverySource: row.dns_discovery_source || null,
    hasIpv4: row.has_ipv4 === 1,
    hasIpv6: row.has_ipv6 === 1,
    hasCname: row.has_cname === 1,
    records: (recordsByHostname.get(row.hostname) ?? []).map((record) => ({
      type: record.type,
      value: record.value,
      priority: record.priority,
      sources: record.sources,
      discoveries: record.discoveries,
      seenDates: record.seen_dates,
      firstSeen: record.first_seen,
      lastSeen: record.last_seen,
    })),
    ipAddresses: (ipsByHostname.get(row.hostname) ?? []).map((ip) => ({
      ip: ip.ip,
      version: ip.version,
      firstSeen: ip.first_seen,
      lastSeen: ip.last_seen,
      networkSegment: ip.metadata?.networkSegment ?? ip.ip,
      countryCode: ip.metadata?.countryCode ?? null,
      countryName: ip.metadata?.countryName ?? null,
      cityName: ip.metadata?.cityName ?? null,
      asn: ip.metadata?.asn ?? null,
      asnOrganization: ip.metadata?.asnOrganization ?? null,
      rdapRegistration: ip.metadata?.rdapRegistration ?? null,
    })),
  }));
  const scanRow = scanRows[0];

  return {
    domain,
    page,
    pageSize,
    summary: {
      totalHostnames,
      certificateHostnames: Number(summaryRow?.certificate_hostnames ?? 0),
      dnsHostnames: Number(summaryRow?.dns_hostnames ?? 0),
      resolvedIpAddressesOnPage: resolvedIps.length,
      rdapRegisteredIpAddressesOnPage: Array.from(metadataByIp.values()).filter(
        (metadata) => metadata.rdapRegistration,
      ).length,
    },
    scan: scanRow
      ? {
          status: scanRow.status,
          resolvedAt: scanRow.resolved_at,
          nameservers: scanRow.nameservers,
          nameserverIps: scanRow.nameserver_ips,
          queriesTotal: scanRow.queries_total,
          queriesOk: scanRow.queries_ok,
          dnssecSigned: scanRow.dnssec_signed === 1,
          dsOutcome: scanRow.ds_outcome,
          dnskeyOutcome: scanRow.dnskey_outcome,
          zoneTransferOpen: scanRow.zone_transfer_open === 1,
        }
      : null,
    hostnames,
  };
}

const technologyIpInventorySql = `WITH inventory AS (
  SELECT
    toString(value) AS ip,
    toUInt8(if(record_type = 'A', 4, 6)) AS version,
    arraySort(groupUniqArray(name)) AS hostnames,
    toString(min(first_seen)) AS first_seen,
    toString(max(last_seen)) AS last_seen
  FROM commoncrawl_domain_dns_records
  WHERE root_domain = {domain:String}
    AND record_type IN ('A', 'AAAA')
  GROUP BY record_type, value
)
SELECT
  inventory.*,
  toString(count() OVER ()) AS total_addresses,
  toString(countIf(version = 4) OVER ()) AS ipv4_addresses,
  toString(countIf(version = 6) OVER ()) AS ipv6_addresses
FROM inventory
ORDER BY version, ip
LIMIT {limit:UInt32} OFFSET {offset:UInt64}`;

/** Lists every distinct address resolved by the selected company domain. */
export async function getCompanyTechnologyIpInventory(
  country: CountryConfig,
  id: string,
  opts: { domain?: string; page?: number; pageSize?: number } = {},
): Promise<CompanyTechnologyIpInventory | null> {
  const domains = await getCompanyDomains(country, id);
  const selectedDomain = selectedCompanyDomain(domains, opts.domain);
  if (!selectedDomain) return null;

  const domain = selectedDomain.domain;
  const pageSize = PAGE_SIZES.includes(opts.pageSize as 25 | 50 | 100)
    ? (opts.pageSize as number)
    : 25;
  const requestedPage = clampInt(opts.page, 1, Number.MAX_SAFE_INTEGER, 1);
  const loadPage = (page: number) =>
    chQuery<TechnologyIpInventoryRow>(technologyIpInventorySql, {
      domain,
      limit: pageSize,
      offset: (page - 1) * pageSize,
    });
  let page = requestedPage;
  let rows = await loadPage(page);
  if (rows.length === 0 && requestedPage > 1) {
    const firstPageRows = await loadPage(1);
    const total = Number(firstPageRows[0]?.total_addresses ?? 0);
    page = Math.max(1, Math.ceil(total / pageSize));
    rows = page === 1 ? firstPageRows : await loadPage(page);
  }

  const metadataByIp = await getTechnologyIpMetadata(rows);
  const summaryRow = rows[0];
  return {
    domain,
    page,
    pageSize,
    summary: {
      totalAddresses: Number(summaryRow?.total_addresses ?? 0),
      ipv4Addresses: Number(summaryRow?.ipv4_addresses ?? 0),
      ipv6Addresses: Number(summaryRow?.ipv6_addresses ?? 0),
      rdapRegisteredAddressesOnPage: Array.from(metadataByIp.values()).filter(
        (metadata) => metadata.rdapRegistration,
      ).length,
    },
    addresses: rows.map((row) => {
      const metadata = metadataByIp.get(row.ip);
      return {
        ip: row.ip,
        version: row.version,
        networkSegment: metadata?.networkSegment ?? row.ip,
        hostnames: row.hostnames,
        firstSeen: row.first_seen,
        lastSeen: row.last_seen,
        countryCode: metadata?.countryCode ?? null,
        countryName: metadata?.countryName ?? null,
        cityName: metadata?.cityName ?? null,
        asn: metadata?.asn ?? null,
        asnOrganization: metadata?.asnOrganization ?? null,
        rdapRegistration: metadata?.rdapRegistration ?? null,
      };
    }),
  };
}

const technologyExactIpConnectionsSql = `WITH connections AS (
  SELECT
    ip,
    toUInt8(ip_version) AS version,
    root_domain AS domain,
    hostnames,
    sources,
    discoveries,
    toString(first_seen) AS first_seen,
    toString(last_seen) AS last_seen
  FROM commoncrawl_domain_ip_connections FINAL
  PREWHERE segment_bucket = toUInt8(cityHash64({networkSegment:String}) % 64)
    AND segment_cidr = {networkSegment:String}
    AND ip_version = {version:UInt8}
    AND address = toIPv6({ip:String})
)
SELECT
  connections.*,
  toString(min(first_seen) OVER ()) AS address_first_seen,
  toString(max(last_seen) OVER ()) AS address_last_seen,
  toString(count() OVER ()) AS total_connections
FROM connections
ORDER BY domain != {companyDomain:String}, domain
LIMIT {limit:UInt32} OFFSET {offset:UInt64}`;

const technologySegmentIpConnectionsSql = `SELECT
  ip,
  toUInt8(ip_version) AS version,
  root_domain AS domain,
  hostnames,
  sources,
  discoveries,
  toString(first_seen) AS first_seen,
  toString(last_seen) AS last_seen
FROM commoncrawl_domain_ip_connections FINAL
PREWHERE segment_bucket = toUInt8(cityHash64({networkSegment:String}) % 64)
  AND segment_cidr = {networkSegment:String}
  AND ip_version = {version:UInt8}
WHERE address != toIPv6({ip:String})
ORDER BY address, domain
LIMIT {limit:UInt32} OFFSET {offset:UInt64}`;

const technologyIpHistoryCoverageSql = `SELECT
  toString(count()) AS completed_partitions
FROM commoncrawl_domain_ip_backfill_status FINAL
WHERE bucket < 16`;

function mapTechnologyIpDomainConnection(
  row: TechnologyIpDomainConnectionRow,
): TechnologyIpDomainConnection {
  return {
    ip: row.ip,
    version: row.version,
    domain: row.domain,
    hostnames: row.hostnames,
    sources: row.sources,
    discoveries: row.discoveries,
    firstSeen: row.first_seen,
    lastSeen: row.last_seen,
  };
}

/** Loads the canonical technical view of an address across every observed domain. */
export async function getTechnologyIpDetail(
  address: string,
  opts: { exactPage?: number; segmentPage?: number } = {},
): Promise<TechnologyIpDetail | null> {
  const version = isIP(address);
  if (version !== 4 && version !== 6) return null;

  const [metadata, coverageRows] = await Promise.all([
    getTechnologyIpMetadata([{ ip: address, version }]).then((rows) =>
      rows.get(address),
    ),
    chQuery<{ completed_partitions: string }>(technologyIpHistoryCoverageSql),
  ]);
  const networkSegment = metadata?.networkSegment ?? address;
  const pageSize = 25;
  const requestedExactPage = clampInt(
    opts.exactPage,
    1,
    Number.MAX_SAFE_INTEGER,
    1,
  );
  const loadExactPage = (page: number) =>
    chQuery<TechnologyIpDomainConnectionRow>(technologyExactIpConnectionsSql, {
      ip: address,
      version,
      networkSegment,
      companyDomain: "",
      limit: pageSize,
      offset: (page - 1) * pageSize,
    });
  let exactPage = requestedExactPage;
  let exactRows = await loadExactPage(exactPage);
  if (exactRows.length === 0 && requestedExactPage > 1) {
    const firstPageRows = await loadExactPage(1);
    const total = Number(firstPageRows[0]?.total_connections ?? 0);
    exactPage = Math.max(1, Math.ceil(total / pageSize));
    exactRows =
      exactPage === 1 ? firstPageRows : await loadExactPage(exactPage);
  }
  const observation = exactRows[0];
  if (!observation) return null;

  const requestedSegmentPage = clampInt(
    opts.segmentPage,
    1,
    Number.MAX_SAFE_INTEGER,
    1,
  );
  const segmentRows = await chQuery<TechnologyIpDomainConnectionRow>(
    technologySegmentIpConnectionsSql,
    {
      ip: address,
      version,
      networkSegment,
      limit: pageSize + 1,
      offset: (requestedSegmentPage - 1) * pageSize,
    },
  );
  const exactTotal = Number(observation.total_connections ?? 0);

  return {
    historyIndexCoverage: {
      completedPartitions: Number(coverageRows[0]?.completed_partitions ?? 0),
      totalPartitions: 16,
    },
    address: {
      ip: address,
      version,
      networkSegment,
      firstSeen: observation.address_first_seen ?? observation.first_seen,
      lastSeen: observation.address_last_seen ?? observation.last_seen,
      countryCode: metadata?.countryCode ?? null,
      countryName: metadata?.countryName ?? null,
      cityName: metadata?.cityName ?? null,
      asn: metadata?.asn ?? null,
      asnOrganization: metadata?.asnOrganization ?? null,
      rdapRegistration: metadata?.rdapRegistration ?? null,
    },
    exactConnections: {
      page: exactPage,
      pageSize,
      total: exactTotal,
      hasMore: exactPage * pageSize < exactTotal,
      connections: exactRows.map(mapTechnologyIpDomainConnection),
    },
    segmentConnections: {
      page: requestedSegmentPage,
      pageSize,
      total: null,
      hasMore: segmentRows.length > pageSize,
      connections: segmentRows
        .slice(0, pageSize)
        .map(mapTechnologyIpDomainConnection),
    },
  };
}

/**
 * Loads reverse DNS evidence for one address already associated with the company.
 * Exact-IP sharing is kept separate from the broader and weaker network-segment association.
 */
export async function getCompanyTechnologyIpDetail(
  country: CountryConfig,
  id: string,
  address: string,
  opts: { domain?: string; exactPage?: number; segmentPage?: number } = {},
): Promise<CompanyTechnologyIpDetail | null> {
  const version = isIP(address);
  if (version !== 4 && version !== 6) return null;

  const domains = await getCompanyDomains(country, id);
  const selectedDomain = selectedCompanyDomain(domains, opts.domain);
  if (!selectedDomain) return null;

  const [companyRows, coverageRows] = await Promise.all([
    chQuery<{
      hostnames: string[];
      first_seen: string;
      last_seen: string;
    }>(
      `SELECT
        arraySort(groupUniqArray(name)) AS hostnames,
        toString(min(first_seen)) AS first_seen,
        toString(max(last_seen)) AS last_seen
      FROM commoncrawl_domain_dns_records
      WHERE root_domain = {domain:String}
        AND record_type = {recordType:String}
        AND if(
          record_type = 'A',
          toIPv6(toString(assumeNotNull(toIPv4OrNull(value)))),
          assumeNotNull(toIPv6OrNull(value))
        ) = toIPv6({ip:String})
      HAVING length(hostnames) > 0`,
      {
        domain: selectedDomain.domain,
        recordType: version === 4 ? "A" : "AAAA",
        ip: address,
      },
    ),
    chQuery<{ completed_partitions: string }>(technologyIpHistoryCoverageSql),
  ]);
  const companyObservation = companyRows[0];
  if (!companyObservation) return null;

  const metadata = (
    await getTechnologyIpMetadata([{ ip: address, version }])
  ).get(address);
  const pageSize = 25;
  const requestedExactPage = clampInt(
    opts.exactPage,
    1,
    Number.MAX_SAFE_INTEGER,
    1,
  );
  const loadExactPage = (page: number) =>
    chQuery<TechnologyIpDomainConnectionRow>(technologyExactIpConnectionsSql, {
      ip: address,
      version,
      networkSegment: metadata?.networkSegment ?? address,
      companyDomain: selectedDomain.domain,
      limit: pageSize,
      offset: (page - 1) * pageSize,
    });
  let exactPage = requestedExactPage;
  let exactRows = await loadExactPage(exactPage);
  if (exactRows.length === 0 && requestedExactPage > 1) {
    const firstPageRows = await loadExactPage(1);
    const total = Number(firstPageRows[0]?.total_connections ?? 0);
    exactPage = Math.max(1, Math.ceil(total / pageSize));
    exactRows =
      exactPage === 1 ? firstPageRows : await loadExactPage(exactPage);
  }
  let exactTotal = Number(exactRows[0]?.total_connections ?? 0);
  if (
    exactPage === 1 &&
    !exactRows.some((row) => row.domain === selectedDomain.domain)
  ) {
    exactRows.unshift({
      ip: address,
      version,
      domain: selectedDomain.domain,
      hostnames: companyObservation.hostnames,
      sources: [],
      discoveries: [],
      first_seen: companyObservation.first_seen,
      last_seen: companyObservation.last_seen,
    });
    exactTotal += 1;
  }

  const registration = metadata?.rdapRegistration ?? null;
  const networkSegment = metadata?.networkSegment ?? address;
  const requestedSegmentPage = clampInt(
    opts.segmentPage,
    1,
    Number.MAX_SAFE_INTEGER,
    1,
  );
  const segmentRows = await chQuery<TechnologyIpDomainConnectionRow>(
    technologySegmentIpConnectionsSql,
    {
      ip: address,
      version,
      networkSegment,
      limit: pageSize + 1,
      offset: (requestedSegmentPage - 1) * pageSize,
    },
  );
  const hasMoreSegmentConnections = segmentRows.length > pageSize;
  const visibleSegmentRows = segmentRows.slice(0, pageSize);

  return {
    companyDomain: selectedDomain.domain,
    companyHostnames: companyObservation.hostnames,
    historyIndexCoverage: {
      completedPartitions: Number(coverageRows[0]?.completed_partitions ?? 0),
      totalPartitions: 16,
    },
    address: {
      ip: address,
      version,
      networkSegment,
      firstSeen: companyObservation.first_seen,
      lastSeen: companyObservation.last_seen,
      countryCode: metadata?.countryCode ?? null,
      countryName: metadata?.countryName ?? null,
      cityName: metadata?.cityName ?? null,
      asn: metadata?.asn ?? null,
      asnOrganization: metadata?.asnOrganization ?? null,
      rdapRegistration: registration,
    },
    exactConnections: {
      page: exactPage,
      pageSize,
      total: exactTotal,
      hasMore: exactPage * pageSize < exactTotal,
      connections: exactRows.map(mapTechnologyIpDomainConnection),
    },
    segmentConnections: {
      page: requestedSegmentPage,
      pageSize,
      total: null,
      hasMore: hasMoreSegmentConnections,
      connections: visibleSegmentRows.map(mapTechnologyIpDomainConnection),
    },
  };
}

export interface EvidenceOrigin {
  sourceSlug: string;
  sourceRecordKey: string;
  sourceUrl: string;
  sourceObjectKey: string;
  payloadSha256: string;
  retrievedAt: string;
  sourceRunId: string;
}

export interface EvidenceRef {
  sourceRecordUid: string;
  recordKind: string;
  contentSha256: string;
  firstSeenAt: string;
  lastSeenAt: string;
  origins: EvidenceOrigin[];
  connectionKind?: string;
  sourceDate?: string;
  extractionMethod?: string;
  confidence?: number;
  evidenceIds?: string[];
  evidenceLocator?: string;
  modelProvider?: string;
  modelName?: string;
  promptVersion?: string;
}

export interface CompanySourceRecord {
  sourceRecordUid: string;
  recordKind: string;
  firstSeenAt: string;
  lastSeenAt: string;
  evidence: EvidenceRef[];
}

export interface CompanyDescriptionObservation {
  observationUid: string;
  sourceRecordUid: string;
  descriptionKind: string;
  textOriginal: string;
  languageOriginal: string;
  textEn: string | null;
  extractedAt: string;
  evidence: EvidenceRef[];
}

export interface EsefPersonObservation {
  candidateUid: string;
  sourceRecordUid: string;
  sourceDocumentId: string;
  fiscalYear: number;
  name: string;
  role: string;
  roleCategory: string;
  organization: string;
  status: string;
  effectiveFrom: string;
  effectiveTo: string;
  evidence: EvidenceRef[];
}

export interface CompanyBusinessItemObservation {
  candidateUid: string;
  sourceRecordUid: string;
  fiscalYear: number;
  itemKind: string;
  name: string;
  geographyType: string;
  evidence: EvidenceRef[];
}

export interface CompanyRelationshipObservation {
  candidateUid: string;
  sourceRecordUid: string;
  fiscalYear: number;
  relatedCompanyName: string;
  relationshipType: string;
  ownershipPercentage: number | null;
  jurisdiction: string;
  evidence: EvidenceRef[];
}

export interface SourceContactObservation {
  candidateId: string;
  sourceRecordUid: string;
  fiscalYear: number;
  candidateKind: string;
  normalizedValue: string;
  registrableDomain: string;
  evidence: EvidenceRef[];
}

export interface IndustryDetailRow {
  industry_code: string;
  description_original: string;
  industry_label: string;
  is_primary: 0 | 1;
  source_record_uid?: string;
  evidence?: EvidenceRef[];
}

export interface AddressRow {
  address_id?: string;
  canonical_address_key?: string;
  address_type: string;
  address_types?: string[];
  address_sources?: string[];
  address_member_count?: number;
  source_members?: AddressSourceMember[];
  full_address: string;
  /** ISO 3166-1 alpha-2 country when the source identifies the address country. */
  address_country_code?: string;
  /** Separate from country code because SCB marks foreign addresses without always naming a country. */
  address_is_foreign?: boolean | 0 | 1;
  /**
   * A geocoder-friendly rendering of the same address, where the display form is
   * too noisy to resolve. Optional: most registers publish an address Nominatim
   * handles as-is, and those omit it.
   */
  geocode_address?: string;
  /** Structured fallback fields used when the exact address is absent from the map provider. */
  geocode_street?: string;
  geocode_postal_code?: string;
  /** libpostal components retained separately from the original display address. */
  street_name?: string;
  house_number?: string;
  address_unit?: string;
  /** Offline geocode from the address-owned geocoding table. */
  latitude?: number | null;
  longitude?: number | null;
  geocode_status?: string;
  geocode_provider?: string;
  geocode_precision?: string;
  geocode_match_method?: string;
  geocode_match_confidence?: number;
  geocode_candidate_count?: number;
  geocode_candidate_record_urls?: string[];
  geocode_coordinate_locality?: string;
  geocode_coordinate_supporting_point_count?: number;
  geocode_coordinate_spread_meters?: number | null;
  geocode_source_record_id?: string;
  geocode_source_record_url?: string;
  geocode_source_url?: string;
  geocode_source_object_key?: string;
  geocode_source_md5?: string;
  geocode_source_snapshot_at?: string;
  geocode_source_retrieved_at?: string;
  geocode_source_run_id?: string;
  geocode_matched_at?: string;
}

export interface AddressSourceMember {
  address_key: string;
  address_type: string;
  address_source: string;
  raw_address: string;
  display_address: string;
  street_name?: string;
  house_number?: string;
  address_unit?: string;
  registry_source_record_uid: string;
  registry_source_run_id: string;
  source_observed_at: string;
}

export interface SecondaryNameRow {
  name: string;
  name_kind: "secondary" | "foreign";
  registered: string;
  scope: string;
}

export interface OfficerRow {
  country_iso2: string;
  person_id: string;
  person_profile_available?: boolean;
  first_name: string;
  last_name: string;
  role_original: string;
  role_kind: string;
  signatory_kind: string;
  fiscal_year: number;
  source_record_uid?: string;
  evidence?: EvidenceRef[];
}

export interface AuditRow {
  audit_firm: string;
  opinion_kind: "standard" | "modified" | "unknown";
  opinion_date: string;
  fiscal_year: number;
}

export interface PeopleMatchRow {
  full_name_normalized: string;
  country_iso2: string;
  person_id: string;
  company_id: string;
  company_name: string;
  role_kind: string;
  fiscal_year: number;
}

export interface GleifRelationshipRow {
  direction: "parent" | "subsidiary";
  /** GLEIF vocabulary, e.g. IS_DIRECTLY_CONSOLIDATED_BY. */
  relationship_type: string;
  other_lei: string;
  name: string;
  jurisdiction: string;
  /** The other entity's registry id when it is in this country, else ''. */
  local_id: string;
}

export interface GleifEntityRow {
  lei: string;
  /** GLEIF LEI registration status: ISSUED | LAPSED | RETIRED | ... */
  lei_status: string;
  /** GLEIF entity category: GENERAL | FUND | SOLE_PROPRIETOR | ... */
  category: string;
  hq_country: string;
  /** 0/1: headquarters country differs from the registration country. */
  hq_abroad: number;
  /** Comma-joined GLEIF no-parent exception reasons (NATURAL_PERSONS, ...). */
  ownership_exceptions: string;
}

export interface WikidataCompanyRow {
  wikidata_id: string;
  wikidata_url: string;
  description: string;
  official_name: string;
  inception_date: string;
  employee_count: number | null;
  employee_count_as_of: string;
  industry_label: string;
  legal_form_label: string;
  headquarters: string;
  headquarters_country: string;
  logo_url: string;
  has_current_listing: number;
  /** "Exchange: TICKER | Exchange: TICKER" for current listings, or ''. */
  listings: string;
  /** Space-separated official website URLs, or ''. */
  websites: string;
  linkedin_id: string;
  evidence?: EvidenceRef[];
}

export interface WikidataPersonRow {
  person_wikidata_id: string;
  name: string;
  description: string;
  birth_year: number | null;
  image_url: string;
  wikidata_url: string;
  /** Human role label from the company-anchored link: founder, chief
   * executive officer, chairperson, board member, owned by. */
  role_label: string;
  is_current: number;
  start_date: string;
  end_date: string;
  evidence?: EvidenceRef[];
}

export interface CompanyDetail {
  company: CompanyListRow;
  record: Record<string, unknown>;
  financials: FinancialYearRow[];
  /** France's extended per-year metrics; empty for every other country. */
  frFinancials: FrFinancialRow[];
  contacts: ContactRow[];
  domains: DomainRow[];
  statements: Record<string, unknown>[];
  industries: IndustryDetailRow[];
  addresses: AddressRow[];
  taxRecords: TaxRecordRow[];
  publicContracts: PublicContractRow[];
  /** Award-history summary; null when the country declares no summary query
   * or the company has never won one. */
  contractSummary: ContractSummaryRow | null;
  secondaryNames: SecondaryNameRow[];
  officers: OfficerRow[];
  /** Same-name matches for the officers above, in OTHER companies. Fetched
   * after officers resolves — the query needs their names. */
  peopleMatches: PeopleMatchRow[];
  /** Latest filing's audit firm + opinion form; null when unavailable. */
  audit: AuditRow | null;
  /** GLEIF corporate-group links; empty when no LEI or no query. */
  gleifRelationships: GleifRelationshipRow[];
  /** GLEIF entity facts for the company's LEI; null when no LEI. */
  gleifEntity: GleifEntityRow | null;
  /** Wikidata enrichment matched via LEI; null when unmatched. */
  wikidata: WikidataCompanyRow | null;
  /** Company-anchored Wikidata person links; empty when unmatched. */
  wikidataPeople: WikidataPersonRow[];
  /** Consolidated IFRS figures from the company's ESEF annual reports,
   * newest fiscal year first; empty when the company files none. */
  esefFilings: EsefFilingRow[];
  /** Source-preserving cards for countries with a dedicated Financials tab. */
  financialSources: CompanyFinancialSource[];
  sourceRecords: CompanySourceRecord[];
  descriptions: CompanyDescriptionObservation[];
  esefPeople: EsefPersonObservation[];
  businessItems: CompanyBusinessItemObservation[];
  sourceRelationships: CompanyRelationshipObservation[];
  sourceContacts: SourceContactObservation[];
}

/** One fiscal year of consolidated IFRS figures extracted from a company's
 * ESEF annual report, with a link back to the source document. */
export interface EsefFilingRow {
  primary_fxo_id: string;
  fiscal_year: number;
  period_end: string;
  currency: string;
  revenue_amount_original: number | null;
  revenue_amount_usd: number | null;
  operating_profit_amount_original: number | null;
  operating_profit_amount_usd: number | null;
  profit_loss_amount_original: number | null;
  profit_loss_amount_usd: number | null;
  total_assets_amount_original: number | null;
  total_assets_amount_usd: number | null;
  equity_amount_original: number | null;
  equity_amount_usd: number | null;
  liabilities_amount_original: number | null;
  liabilities_amount_usd: number | null;
  cash_amount_original: number | null;
  cash_amount_usd: number | null;
  employees: number | null;
  mapped_fact_count: number;
  source_fact_count: number;
  viewer_url: string;
  source_url: string;
  package_url: string;
  filing_versions: number;
  composed_from_amendment: number;
  source_record_uids: string[];
  evidence?: EvidenceRef[];
}

export type CompanyFinancialSource =
  | (Extract<FinancialSourceDefinition, { kind: "registry" }> & {
      financials: FinancialYearRow[];
      documents: FinancialReportDocumentSummary[];
    })
  | (Extract<FinancialSourceDefinition, { kind: "esef" }> & {
      filings: EsefFilingRow[];
    });

interface CompanySourceRecordQueryRow {
  source_record_uid: string;
  record_kind: string;
  content_sha256: string;
  first_seen_at: string;
  last_seen_at: string;
  source_slug: string;
  source_record_key: string;
  source_url: string;
  source_object_key: string;
  payload_sha256: string;
  retrieved_at: string;
  source_run_id: string;
}

interface CompanySourceRecordUidRow {
  source_record_uid: string;
}

interface CompanySourceRecordMetadataRow {
  source_record_uid: string;
  record_kind: string;
  content_sha256: string;
  first_seen_at: string;
  last_seen_at: string;
}

interface CompanySourceRecordOriginRow {
  source_record_uid: string;
  source_slug: string;
  source_record_key: string;
  source_url: string;
  source_object_key: string;
  payload_sha256: string;
  retrieved_at: string;
  source_run_id: string;
}

interface DescriptionObservationQueryRow {
  observation_uid: string;
  source_record_uid: string;
  description_kind: string;
  text_original: string;
  language_original: string;
  text_en: string | null;
  extraction_method: string;
  confidence: number;
  evidence_ids: string[];
  source_field: string;
  source_date: string;
  model_provider: string;
  model_name: string;
  prompt_version: string;
  extracted_at: string;
}

interface EsefPersonQueryRow {
  candidate_uid: string;
  source_record_uid: string;
  source_document_id: string;
  fiscal_year: number;
  name: string;
  role: string;
  role_category: string;
  organization: string;
  status: string;
  effective_from: string;
  effective_to: string;
  confidence: number;
  evidence_ids: string[];
  model_provider: string;
  model_name: string;
  prompt_version: string;
  extracted_at: string;
}

interface BusinessItemQueryRow {
  candidate_uid: string;
  source_record_uid: string;
  fiscal_year: number;
  item_kind: string;
  name: string;
  geography_type: string;
  confidence: number;
  evidence_ids: string[];
  model_provider: string;
  model_name: string;
  prompt_version: string;
  extracted_at: string;
}

interface SourceRelationshipQueryRow {
  candidate_uid: string;
  source_record_uid: string;
  fiscal_year: number;
  related_company_name: string;
  relationship_type: string;
  ownership_percentage: number | null;
  jurisdiction: string;
  confidence: number;
  evidence_ids: string[];
  model_provider: string;
  model_name: string;
  prompt_version: string;
  extracted_at: string;
}

interface SourceContactQueryRow {
  candidate_id: string;
  source_record_uid: string;
  fiscal_year: number;
  candidate_kind: string;
  normalized_value: string;
  registrable_domain: string;
  extracted_at: string;
}

export const COMPANY_SOURCE_RECORD_UIDS_QUERY = `
SELECT DISTINCT toString(source_record_uid) AS source_record_uid
FROM corpscout.company_source_record_links
PREWHERE country_code = {country:String} AND company_id = {id:String}`;

export const COMPANY_SOURCE_RECORD_METADATA_QUERY = `
SELECT
  toString(source_record_uid) AS source_record_uid,
  argMax(record_kind, company_source_records.last_seen_at) AS record_kind,
  argMax(content_sha256, company_source_records.last_seen_at) AS content_sha256,
  toString(min(first_seen_at)) AS first_seen_at,
  toString(max(company_source_records.last_seen_at)) AS last_seen_at
FROM corpscout.company_source_records
PREWHERE source_record_uid IN {sourceRecordUids:Array(String)}
GROUP BY source_record_uid`;

export const COMPANY_SOURCE_RECORD_ORIGINS_QUERY = `
SELECT
  toString(source_record_uid) AS source_record_uid,
  source_slug,
  source_record_key,
  source_url,
  source_object_key,
  argMax(payload_sha256, company_source_record_origins.retrieved_at) AS payload_sha256,
  toString(max(company_source_record_origins.retrieved_at)) AS retrieved_at,
  argMax(source_run_id, company_source_record_origins.retrieved_at) AS source_run_id
FROM corpscout.company_source_record_origins
PREWHERE source_record_uid IN {sourceRecordUids:Array(String)}
GROUP BY
  source_record_uid,
  source_slug,
  source_record_key,
  source_url,
  source_object_key`;

async function getCompanySourceRecordRows(params: {
  country: string;
  id: string;
}): Promise<CompanySourceRecordQueryRow[]> {
  const linkedRows = await chQuery<CompanySourceRecordUidRow>(
    COMPANY_SOURCE_RECORD_UIDS_QUERY,
    params,
  );
  const sourceRecordUids = linkedRows.map((row) => row.source_record_uid);
  if (sourceRecordUids.length === 0) return [];

  const [records, origins] = await Promise.all([
    chQuery<CompanySourceRecordMetadataRow>(
      COMPANY_SOURCE_RECORD_METADATA_QUERY,
      { sourceRecordUids },
    ),
    chQuery<CompanySourceRecordOriginRow>(COMPANY_SOURCE_RECORD_ORIGINS_QUERY, {
      sourceRecordUids,
    }),
  ]);
  const originsByUid = new Map<string, CompanySourceRecordOriginRow[]>();
  for (const origin of origins) {
    const existing = originsByUid.get(origin.source_record_uid) ?? [];
    existing.push(origin);
    originsByUid.set(origin.source_record_uid, existing);
  }

  return records
    .flatMap((record) => {
      const recordOrigins = originsByUid.get(record.source_record_uid);
      if (!recordOrigins || recordOrigins.length === 0) {
        return [
          {
            ...record,
            source_slug: "",
            source_record_key: "",
            source_url: "",
            source_object_key: "",
            payload_sha256: "",
            retrieved_at: "",
            source_run_id: "",
          },
        ];
      }
      return recordOrigins.map((origin) => ({ ...record, ...origin }));
    })
    .sort(
      (left, right) =>
        right.last_seen_at.localeCompare(left.last_seen_at) ||
        left.source_slug.localeCompare(right.source_slug) ||
        left.source_record_key.localeCompare(right.source_record_key),
    );
}

export const COMPANY_DESCRIPTION_OBSERVATIONS_QUERY = `
SELECT
  toString(observation_uid) AS observation_uid,
  toString(source_record_uid) AS source_record_uid,
  description_kind,
  text_original,
  language_original,
  text_en,
  extraction_method,
  toFloat64(confidence) AS confidence,
  evidence_ids,
  source_field,
  coalesce(toString(source_date), '') AS source_date,
  model_provider,
  model_name,
  prompt_version,
  toString(extracted_at) AS extracted_at
FROM corpscout.company_description_observations FINAL
WHERE country_code = {country:String} AND company_id = {id:String}
ORDER BY extracted_at DESC, observation_uid`;

export const ESEF_DOCUMENT_PEOPLE_QUERY = `
SELECT
  toString(candidate_uid) AS candidate_uid,
  toString(source_record_uid) AS source_record_uid,
  source_document_id,
  fiscal_year,
  name,
  role,
  role_category,
  organization,
  status,
  coalesce(toString(effective_from), '') AS effective_from,
  coalesce(toString(effective_to), '') AS effective_to,
  toFloat64(confidence) AS confidence,
  evidence_ids,
  model_provider,
  model_name,
  prompt_version,
  toString(extracted_at) AS extracted_at
FROM corpscout.esef_document_people FINAL
WHERE country_code = {country:String} AND company_id = {id:String}
ORDER BY fiscal_year DESC, status = 'current' DESC, role_category, name`;

export const ESEF_DOCUMENT_BUSINESS_ITEMS_QUERY = `
SELECT
  toString(candidate_uid) AS candidate_uid,
  toString(source_record_uid) AS source_record_uid,
  fiscal_year,
  item_kind,
  name,
  geography_type,
  toFloat64(confidence) AS confidence,
  evidence_ids,
  model_provider,
  model_name,
  prompt_version,
  toString(extracted_at) AS extracted_at
FROM corpscout.esef_document_business_items FINAL
WHERE country_code = {country:String} AND company_id = {id:String}
ORDER BY fiscal_year DESC, item_kind, name`;

export const ESEF_DOCUMENT_RELATIONSHIPS_QUERY = `
SELECT
  toString(candidate_uid) AS candidate_uid,
  toString(source_record_uid) AS source_record_uid,
  fiscal_year,
  related_company_name,
  relationship_type,
  ownership_percentage,
  jurisdiction,
  toFloat64(confidence) AS confidence,
  evidence_ids,
  model_provider,
  model_name,
  prompt_version,
  toString(extracted_at) AS extracted_at
FROM corpscout.esef_document_group_relationships FINAL
WHERE country_code = {country:String} AND company_id = {id:String}
ORDER BY fiscal_year DESC, relationship_type, related_company_name`;

export const ESEF_DOCUMENT_CONTACTS_QUERY = `
SELECT
  toString(candidate_id) AS candidate_id,
  source_record_uid,
  fiscal_year,
  candidate_kind,
  normalized_value,
  registrable_domain,
  extracted_at
FROM corpscout.esef_document_contact_candidates
WHERE country_iso2 = {country:String} AND company_id = {id:String}
ORDER BY fiscal_year DESC, candidate_kind, normalized_value`;

function buildEvidenceRefs(rows: CompanySourceRecordQueryRow[]): {
  sourceRecords: CompanySourceRecord[];
  byUid: Map<string, EvidenceRef>;
} {
  const byUid = new Map<string, EvidenceRef>();
  for (const row of rows) {
    const existing = byUid.get(row.source_record_uid);
    const evidence = existing ?? {
      sourceRecordUid: row.source_record_uid,
      recordKind: row.record_kind,
      contentSha256: row.content_sha256,
      firstSeenAt: row.first_seen_at,
      lastSeenAt: row.last_seen_at,
      origins: [],
    };
    if (row.source_slug !== "") {
      const origin: EvidenceOrigin = {
        sourceSlug: row.source_slug,
        sourceRecordKey: row.source_record_key,
        sourceUrl: row.source_url,
        sourceObjectKey: row.source_object_key,
        payloadSha256: row.payload_sha256,
        retrievedAt: row.retrieved_at,
        sourceRunId: row.source_run_id,
      };
      if (
        !evidence.origins.some(
          (value) => JSON.stringify(value) === JSON.stringify(origin),
        )
      ) {
        evidence.origins.push(origin);
      }
    }
    byUid.set(row.source_record_uid, evidence);
  }
  return {
    byUid,
    sourceRecords: [...byUid.values()].map((evidence) => ({
      sourceRecordUid: evidence.sourceRecordUid,
      recordKind: evidence.recordKind,
      firstSeenAt: evidence.firstSeenAt,
      lastSeenAt: evidence.lastSeenAt,
      evidence: [evidence],
    })),
  };
}

function observationEvidence(
  byUid: Map<string, EvidenceRef>,
  sourceRecordUid: string,
  details: Omit<
    EvidenceRef,
    | "sourceRecordUid"
    | "recordKind"
    | "contentSha256"
    | "firstSeenAt"
    | "lastSeenAt"
    | "origins"
  >,
): EvidenceRef[] {
  const source = byUid.get(sourceRecordUid);
  return source ? [{ ...source, ...details }] : [];
}

/**
 * ESEF figures for one company, resolved through corpscout.company_identifier
 * rather than re-deriving the LEI from registered_as — so this one query serves
 * every country instead of needing a per-country variant.
 *
 * Amendments are handled per field, not per row. filings.xbrl.org keeps each
 * version of a filing under its own fxo_id, and an amendment does not always
 * re-tag everything the original did: measured 2026-07-25, taking only the
 * newest version would drop revenue for 17 company-periods, total assets for
 * 18, equity for 16. Each metric therefore comes from the newest version that
 * actually reports it, which can compose a row from two documents —
 * composed_from_amendment flags that so the document link is not read as the
 * sole source of every figure on the row.
 */
const ESEF_FILINGS_QUERY = `
WITH versions AS (
  SELECT
    m.fxo_id AS fxo_id,
    m.fiscal_year AS fiscal_year,
    m.period_end AS period_end,
    m.currency AS currency,
    m.revenue_amount_original AS revenue_amount_original,
    m.revenue_amount_usd AS revenue_amount_usd,
    m.operating_profit_amount_original AS operating_profit_amount_original,
    m.operating_profit_amount_usd AS operating_profit_amount_usd,
    m.profit_loss_amount_original AS profit_loss_amount_original,
    m.profit_loss_amount_usd AS profit_loss_amount_usd,
    m.total_assets_amount_original AS total_assets_amount_original,
    m.total_assets_amount_usd AS total_assets_amount_usd,
    m.equity_amount_original AS equity_amount_original,
    m.equity_amount_usd AS equity_amount_usd,
    m.liabilities_amount_original AS liabilities_amount_original,
    m.liabilities_amount_usd AS liabilities_amount_usd,
    m.cash_amount_original AS cash_amount_original,
    m.cash_amount_usd AS cash_amount_usd,
    m.employees AS employees,
    m.mapped_fact_count AS mapped_fact_count,
    m.source_fact_count AS source_fact_count,
    m.viewer_url AS viewer_url,
    f.source_url AS source_url,
    f.package_url AS package_url,
    lower(hex(SHA256(concat(
      'company-source-record-v1\\nfile\\nesef_report_package\\n',
      lowerUTF8(f.package_sha256)
    )))) AS source_record_uid,
    toUInt32(extract(m.fxo_id, '-([0-9]+)$')) AS version
  FROM corpscout.esef_financial_metrics AS m
  INNER JOIN corpscout.esef_filings AS f
    ON f.lei = m.lei
   AND f.period_end = m.period_end
   AND f.fxo_id = m.fxo_id
  INNER JOIN corpscout.company_identifier AS c
    ON c.issuer_scheme = 'lei'
   AND c.issuer_id = upperUTF8(trimBoth(m.lei))
  WHERE c.country_code = {country:String}
    AND c.company_id = {id:String}
)
SELECT
  argMax(v.fxo_id, v.version) AS primary_fxo_id,
  v.fiscal_year AS fiscal_year,
  toString(v.period_end) AS period_end,
  argMaxIf(v.currency, v.version, v.currency != '') AS currency,
  argMaxIf(v.revenue_amount_original, v.version, v.revenue_amount_original IS NOT NULL) AS revenue_amount_original,
  argMaxIf(v.revenue_amount_usd, v.version, v.revenue_amount_usd IS NOT NULL) AS revenue_amount_usd,
  argMaxIf(v.operating_profit_amount_original, v.version, v.operating_profit_amount_original IS NOT NULL) AS operating_profit_amount_original,
  argMaxIf(v.operating_profit_amount_usd, v.version, v.operating_profit_amount_usd IS NOT NULL) AS operating_profit_amount_usd,
  argMaxIf(v.profit_loss_amount_original, v.version, v.profit_loss_amount_original IS NOT NULL) AS profit_loss_amount_original,
  argMaxIf(v.profit_loss_amount_usd, v.version, v.profit_loss_amount_usd IS NOT NULL) AS profit_loss_amount_usd,
  argMaxIf(v.total_assets_amount_original, v.version, v.total_assets_amount_original IS NOT NULL) AS total_assets_amount_original,
  argMaxIf(v.total_assets_amount_usd, v.version, v.total_assets_amount_usd IS NOT NULL) AS total_assets_amount_usd,
  argMaxIf(v.equity_amount_original, v.version, v.equity_amount_original IS NOT NULL) AS equity_amount_original,
  argMaxIf(v.equity_amount_usd, v.version, v.equity_amount_usd IS NOT NULL) AS equity_amount_usd,
  argMaxIf(v.liabilities_amount_original, v.version, v.liabilities_amount_original IS NOT NULL) AS liabilities_amount_original,
  argMaxIf(v.liabilities_amount_usd, v.version, v.liabilities_amount_usd IS NOT NULL) AS liabilities_amount_usd,
  argMaxIf(v.cash_amount_original, v.version, v.cash_amount_original IS NOT NULL) AS cash_amount_original,
  argMaxIf(v.cash_amount_usd, v.version, v.cash_amount_usd IS NOT NULL) AS cash_amount_usd,
  argMaxIf(v.employees, v.version, v.employees IS NOT NULL) AS employees,
  argMax(v.mapped_fact_count, v.version) AS mapped_fact_count,
  argMax(v.source_fact_count, v.version) AS source_fact_count,
  -- Newest version that actually carries a link, not simply the newest: an
  -- amendment sometimes lands without a viewer_url and the row would then
  -- offer no way back to the source document.
  argMaxIf(v.viewer_url, v.version, v.viewer_url != '') AS viewer_url,
  argMaxIf(v.source_url, v.version, v.source_url != '') AS source_url,
  argMaxIf(v.package_url, v.version, v.package_url != '') AS package_url,
  arrayDistinct(arrayFilter(value -> value != '', [
    argMaxIf(v.source_record_uid, v.version, v.revenue_amount_usd IS NOT NULL),
    argMaxIf(v.source_record_uid, v.version, v.operating_profit_amount_usd IS NOT NULL),
    argMaxIf(v.source_record_uid, v.version, v.profit_loss_amount_usd IS NOT NULL),
    argMaxIf(v.source_record_uid, v.version, v.total_assets_amount_usd IS NOT NULL),
    argMaxIf(v.source_record_uid, v.version, v.equity_amount_usd IS NOT NULL),
    argMaxIf(v.source_record_uid, v.version, v.liabilities_amount_usd IS NOT NULL),
    argMaxIf(v.source_record_uid, v.version, v.cash_amount_usd IS NOT NULL),
    argMaxIf(v.source_record_uid, v.version, v.employees IS NOT NULL),
    argMaxIf(v.source_record_uid, v.version, v.viewer_url != '')
  ])) AS source_record_uids,
  toUInt32(count()) AS filing_versions,
  toUInt8(
    count() > 1
    -- maxIf over an empty set returns 0 rather than NULL, so a company that
    -- reports no revenue in any version would otherwise look "composed".
    AND countIf(v.revenue_amount_usd IS NOT NULL) > 0
    AND maxIf(v.version, v.revenue_amount_usd IS NOT NULL) != max(v.version)
  ) AS composed_from_amendment
FROM versions AS v
GROUP BY v.fiscal_year, v.period_end
ORDER BY v.fiscal_year DESC
LIMIT 12`;

const COMPANY_EVIDENCE_SCHEMA_QUERY = `
SELECT toUInt8(
  countIf(name IN (
    'company_source_records',
    'company_source_record_origins',
    'company_source_record_links',
    'company_description_observations',
    'esef_document_people',
    'esef_document_business_items',
    'esef_document_group_relationships'
  )) = 7
  AND (
    SELECT count()
    FROM system.columns
    WHERE database = 'corpscout'
      AND table = 'esef_document_contact_candidates'
      AND name = 'source_record_uid'
  ) = 1
) AS ready
FROM system.tables
WHERE database = 'corpscout'`;

function companyEvidenceSchemaReady(): Promise<boolean> {
  return chQuery<{ ready: number }>(COMPANY_EVIDENCE_SCHEMA_QUERY).then(
    (rows) => Number(rows[0]?.ready ?? 0) === 1,
  );
}

function evidenceQueryWhenReady<T>(
  ready: Promise<boolean>,
  query: () => Promise<T[]>,
): Promise<T[]> {
  return ready.then((isReady) => (isReady ? query() : []));
}

function attachFinancialEvidence(
  financials: FinancialYearRow[],
  esefFilings: EsefFilingRow[],
  evidenceByUid: Map<string, EvidenceRef>,
): void {
  for (const row of financials) {
    row.evidence = row.source_record_uid
      ? observationEvidence(evidenceByUid, row.source_record_uid, {
          extractionMethod: "ixbrl_fact_mapping",
        })
      : [];
  }
  for (const row of esefFilings) {
    row.evidence = row.source_record_uids.flatMap((uid) =>
      observationEvidence(evidenceByUid, uid, {
        extractionMethod: "ixbrl_fact_mapping",
        sourceDate: row.period_end,
      }),
    );
  }
}

function buildCompanyFinancialSources(
  definitions: readonly FinancialSourceDefinition[],
  financials: FinancialYearRow[],
  esefFilings: EsefFilingRow[],
  documents: FinancialReportDocumentSummary[],
): CompanyFinancialSource[] {
  const sources: CompanyFinancialSource[] = [];
  for (const definition of definitions) {
    if (definition.kind === "registry") {
      if (financials.length > 0 || documents.length > 0) {
        sources.push({ ...definition, financials, documents });
      }
      continue;
    }
    if (esefFilings.length > 0) {
      sources.push({ ...definition, filings: esefFilings });
    }
  }
  return sources;
}

export interface CompanyFinancialDetail {
  financialSources: CompanyFinancialSource[];
}

/** Financial-tab data only. Unlike getCompanyDetail, this does not fetch
 * contacts, people, contracts, or other overview sections. */
export async function getCompanyFinancialDetail(
  country: CountryConfig,
  id: string,
): Promise<CompanyFinancialDetail> {
  const definitions = country.detail?.financialSources ?? [];
  const includesRegistry = definitions.some(
    (source) => source.kind === "registry",
  );
  const includesEsef = definitions.some((source) => source.kind === "esef");
  const documentProvider = definitions.find(
    (source) => source.kind === "registry" && source.documentProvider,
  );
  const evidenceParams = { id, country: country.code.toUpperCase() };
  const evidenceSchemaReadyPromise = companyEvidenceSchemaReady();
  const [financials, esefFilings, documents, sourceRecordRows] =
    await Promise.all([
      includesRegistry
        ? getCompanyFinancials(country, id)
        : Promise.resolve([]),
      includesEsef
        ? chQuery<EsefFilingRow>(ESEF_FILINGS_QUERY, evidenceParams)
        : Promise.resolve([]),
      documentProvider?.kind === "registry" &&
      documentProvider.documentProvider === "norway_annual_reports"
        ? getNorwayFinancialReports(id)
        : Promise.resolve([]),
      evidenceQueryWhenReady(evidenceSchemaReadyPromise, () =>
        getCompanySourceRecordRows(evidenceParams),
      ),
    ]);
  const { byUid } = buildEvidenceRefs(sourceRecordRows);
  attachFinancialEvidence(financials, esefFilings, byUid);
  return {
    financialSources: buildCompanyFinancialSources(
      definitions,
      financials,
      esefFilings,
      documents,
    ),
  };
}

export async function getCompanyDetail(
  country: CountryConfig,
  id: string,
  existingShell?: CompanyShell | null,
): Promise<CompanyDetail | null> {
  const shell =
    existingShell === undefined
      ? await getCompanyShell(country, id)
      : existingShell;
  if (!shell) return null;
  const company = { ...shell.company };

  const sectionsPromise = Promise.all([
    country.detail?.financialsQuery
      ? chQuery<FinancialYearRow>(country.detail.financialsQuery, { id })
      : Promise.resolve([]),
    country.detail?.contactsQuery
      ? chQuery<ContactRow>(country.detail.contactsQuery, { id })
      : Promise.resolve([]),
    country.detail?.domainsQuery
      ? chQuery<DomainRow>(country.detail.domainsQuery, { id })
      : Promise.resolve([]),
  ]);
  const statementsPromise = country.detail?.statementsQuery
    ? chQuery<Record<string, unknown>>(country.detail.statementsQuery, { id })
    : Promise.resolve([]);
  const industriesPromise = country.detail?.industriesQuery
    ? chQuery<IndustryDetailRow>(country.detail.industriesQuery, { id })
    : Promise.resolve([]);
  const addressesPromise = country.detail?.addressQuery
    ? chQuery<AddressRow>(country.detail.addressQuery, { id })
    : Promise.resolve([]);
  const taxRecordsPromise = country.detail?.taxRecordsQuery
    ? chQuery<TaxRecordRow>(country.detail.taxRecordsQuery, { id })
    : Promise.resolve([]);
  const frFinancialsPromise = country.detail?.financialMetricsQuery
    ? chQuery<FrFinancialRow>(country.detail.financialMetricsQuery, { id })
    : Promise.resolve([]);
  const publicContractsPromise = country.detail?.publicContractsQuery
    ? chQuery<PublicContractRow>(country.detail.publicContractsQuery, { id })
    : Promise.resolve([]);
  const contractSummaryPromise = country.detail?.contractSummaryQuery
    ? chQuery<ContractSummaryRow>(country.detail.contractSummaryQuery, { id })
    : Promise.resolve([]);
  const secondaryNamesPromise = country.detail?.secondaryNamesQuery
    ? chQuery<SecondaryNameRow>(country.detail.secondaryNamesQuery, { id })
    : Promise.resolve([]);
  const officersPromise = country.detail?.officersQuery
    ? chQuery<OfficerRow>(country.detail.officersQuery, { id })
    : Promise.resolve([]);
  const auditPromise = country.detail?.auditQuery
    ? chQuery<AuditRow>(country.detail.auditQuery, { id })
    : Promise.resolve([]);
  const gleifRelationshipsPromise = country.detail?.gleifRelationshipsQuery
    ? chQuery<GleifRelationshipRow>(country.detail.gleifRelationshipsQuery, {
        id,
      })
    : Promise.resolve([]);
  const gleifEntityPromise = country.detail?.gleifEntityQuery
    ? chQuery<GleifEntityRow>(country.detail.gleifEntityQuery, { id })
    : Promise.resolve([]);
  const wikidataPromise = country.detail?.wikidataQuery
    ? chQuery<WikidataCompanyRow>(country.detail.wikidataQuery, { id })
    : Promise.resolve([]);
  const wikidataPeoplePromise = country.detail?.wikidataPeopleQuery
    ? chQuery<WikidataPersonRow>(country.detail.wikidataPeopleQuery, { id })
    : Promise.resolve([]);
  // Country-agnostic: resolved through company_identifier, so no per-country
  // config entry is needed and every country gets it at once.
  const esefFilingsPromise = chQuery<EsefFilingRow>(ESEF_FILINGS_QUERY, {
    id,
    country: country.code.toUpperCase(),
  });
  const financialDocumentsPromise = country.detail?.financialSources?.some(
    (source) =>
      source.kind === "registry" &&
      source.documentProvider === "norway_annual_reports",
  )
    ? getNorwayFinancialReports(id)
    : Promise.resolve([]);
  const evidenceParams = { id, country: country.code.toUpperCase() };
  const evidenceSchemaReadyPromise = companyEvidenceSchemaReady();
  const sourceRecordRowsPromise = evidenceQueryWhenReady(
    evidenceSchemaReadyPromise,
    () => getCompanySourceRecordRows(evidenceParams),
  );
  const descriptionRowsPromise = evidenceQueryWhenReady(
    evidenceSchemaReadyPromise,
    () =>
      chQuery<DescriptionObservationQueryRow>(
        COMPANY_DESCRIPTION_OBSERVATIONS_QUERY,
        evidenceParams,
      ),
  );
  const esefPeopleRowsPromise = evidenceQueryWhenReady(
    evidenceSchemaReadyPromise,
    () =>
      chQuery<EsefPersonQueryRow>(ESEF_DOCUMENT_PEOPLE_QUERY, evidenceParams),
  );
  const businessItemRowsPromise = evidenceQueryWhenReady(
    evidenceSchemaReadyPromise,
    () =>
      chQuery<BusinessItemQueryRow>(
        ESEF_DOCUMENT_BUSINESS_ITEMS_QUERY,
        evidenceParams,
      ),
  );
  const sourceRelationshipRowsPromise = evidenceQueryWhenReady(
    evidenceSchemaReadyPromise,
    () =>
      chQuery<SourceRelationshipQueryRow>(
        ESEF_DOCUMENT_RELATIONSHIPS_QUERY,
        evidenceParams,
      ),
  );
  const sourceContactRowsPromise = evidenceQueryWhenReady(
    evidenceSchemaReadyPromise,
    () =>
      chQuery<SourceContactQueryRow>(
        ESEF_DOCUMENT_CONTACTS_QUERY,
        evidenceParams,
      ),
  );
  // No-op guards close the unhandled-rejection window between promise
  // construction and the `await` below — the await still surfaces real errors.
  sectionsPromise.catch(() => {});
  statementsPromise.catch(() => {});
  industriesPromise.catch(() => {});
  addressesPromise.catch(() => {});
  taxRecordsPromise.catch(() => {});
  publicContractsPromise.catch(() => {});
  secondaryNamesPromise.catch(() => {});
  officersPromise.catch(() => {});
  auditPromise.catch(() => {});
  gleifRelationshipsPromise.catch(() => {});
  gleifEntityPromise.catch(() => {});
  wikidataPromise.catch(() => {});
  wikidataPeoplePromise.catch(() => {});
  esefFilingsPromise.catch(() => {});
  financialDocumentsPromise.catch(() => {});
  frFinancialsPromise.catch(() => {});
  contractSummaryPromise.catch(() => {});
  sourceRecordRowsPromise.catch(() => {});
  descriptionRowsPromise.catch(() => {});
  esefPeopleRowsPromise.catch(() => {});
  businessItemRowsPromise.catch(() => {});
  sourceRelationshipRowsPromise.catch(() => {});
  sourceContactRowsPromise.catch(() => {});

  if (country.industryQuery) {
    const key = shell.industryKey;
    const industries = key
      ? await chQuery<IndustryRow>(country.industryQuery, { ids: [key] })
      : [];
    company.industry_code = industries[0]?.industry_code ?? null;
    company.industry_label = industries[0]?.industry_label ?? null;
  }

  const [
    [financials, contacts, domains],
    statements,
    industries,
    addresses,
    taxRecords,
    publicContracts,
    secondaryNames,
    officers,
    auditRows,
    gleifRelationships,
    gleifEntityRows,
    wikidataRows,
    wikidataPeople,
    esefFilings,
    financialDocuments,
    frFinancials,
    contractSummaryRows,
    sourceRecordRows,
    descriptionRows,
    esefPeopleRows,
    businessItemRows,
    sourceRelationshipRows,
    sourceContactRows,
  ] = await Promise.all([
    sectionsPromise,
    statementsPromise,
    industriesPromise,
    addressesPromise,
    taxRecordsPromise,
    publicContractsPromise,
    secondaryNamesPromise,
    officersPromise,
    auditPromise,
    gleifRelationshipsPromise,
    gleifEntityPromise,
    wikidataPromise,
    wikidataPeoplePromise,
    esefFilingsPromise,
    financialDocumentsPromise,
    frFinancialsPromise,
    contractSummaryPromise,
    sourceRecordRowsPromise,
    descriptionRowsPromise,
    esefPeopleRowsPromise,
    businessItemRowsPromise,
    sourceRelationshipRowsPromise,
    sourceContactRowsPromise,
  ]);

  const { sourceRecords, byUid: evidenceByUid } =
    buildEvidenceRefs(sourceRecordRows);
  const descriptions: CompanyDescriptionObservation[] = descriptionRows.map(
    (row) => ({
      observationUid: row.observation_uid,
      sourceRecordUid: row.source_record_uid,
      descriptionKind: row.description_kind,
      textOriginal: row.text_original,
      languageOriginal: row.language_original,
      textEn: row.text_en,
      extractedAt: row.extracted_at,
      evidence: observationEvidence(evidenceByUid, row.source_record_uid, {
        sourceDate: row.source_date,
        extractionMethod: row.extraction_method,
        confidence: row.confidence,
        evidenceIds: row.evidence_ids,
        evidenceLocator: row.source_field,
        modelProvider: row.model_provider,
        modelName: row.model_name,
        promptVersion: row.prompt_version,
      }),
    }),
  );
  const esefPeople: EsefPersonObservation[] = esefPeopleRows.map((row) => ({
    candidateUid: row.candidate_uid,
    sourceRecordUid: row.source_record_uid,
    sourceDocumentId: row.source_document_id,
    fiscalYear: row.fiscal_year,
    name: row.name,
    role: row.role,
    roleCategory: row.role_category,
    organization: row.organization,
    status: row.status,
    effectiveFrom: row.effective_from,
    effectiveTo: row.effective_to,
    evidence: observationEvidence(evidenceByUid, row.source_record_uid, {
      extractionMethod: "llm_extraction",
      confidence: row.confidence,
      evidenceIds: row.evidence_ids,
      evidenceLocator: `corpscout.esef_document_people:${row.candidate_uid}`,
      modelProvider: row.model_provider,
      modelName: row.model_name,
      promptVersion: row.prompt_version,
      sourceDate: row.extracted_at,
    }),
  }));
  const businessItems: CompanyBusinessItemObservation[] = businessItemRows.map(
    (row) => ({
      candidateUid: row.candidate_uid,
      sourceRecordUid: row.source_record_uid,
      fiscalYear: row.fiscal_year,
      itemKind: row.item_kind,
      name: row.name,
      geographyType: row.geography_type,
      evidence: observationEvidence(evidenceByUid, row.source_record_uid, {
        extractionMethod: "llm_extraction",
        confidence: row.confidence,
        evidenceIds: row.evidence_ids,
        evidenceLocator: `corpscout.esef_document_business_items:${row.candidate_uid}`,
        modelProvider: row.model_provider,
        modelName: row.model_name,
        promptVersion: row.prompt_version,
        sourceDate: row.extracted_at,
      }),
    }),
  );
  const sourceRelationships: CompanyRelationshipObservation[] =
    sourceRelationshipRows.map((row) => ({
      candidateUid: row.candidate_uid,
      sourceRecordUid: row.source_record_uid,
      fiscalYear: row.fiscal_year,
      relatedCompanyName: row.related_company_name,
      relationshipType: row.relationship_type,
      ownershipPercentage: row.ownership_percentage,
      jurisdiction: row.jurisdiction,
      evidence: observationEvidence(evidenceByUid, row.source_record_uid, {
        extractionMethod: "llm_extraction",
        confidence: row.confidence,
        evidenceIds: row.evidence_ids,
        evidenceLocator: `corpscout.esef_document_group_relationships:${row.candidate_uid}`,
        modelProvider: row.model_provider,
        modelName: row.model_name,
        promptVersion: row.prompt_version,
        sourceDate: row.extracted_at,
      }),
    }));
  const sourceContacts: SourceContactObservation[] = sourceContactRows.map(
    (row) => ({
      candidateId: row.candidate_id,
      sourceRecordUid: row.source_record_uid,
      fiscalYear: row.fiscal_year,
      candidateKind: row.candidate_kind,
      normalizedValue: row.normalized_value,
      registrableDomain: row.registrable_domain,
      evidence: observationEvidence(evidenceByUid, row.source_record_uid, {
        extractionMethod: "deterministic_ixbrl",
        evidenceLocator: `corpscout.esef_document_contact_candidates:${row.candidate_id}`,
        sourceDate: row.extracted_at,
      }),
    }),
  );

  attachFinancialEvidence(financials, esefFilings, evidenceByUid);
  const financialSources = buildCompanyFinancialSources(
    country.detail?.financialSources ?? [],
    financials,
    esefFilings,
    financialDocuments,
  );
  for (const row of industries) {
    row.evidence = row.source_record_uid
      ? observationEvidence(evidenceByUid, row.source_record_uid, {
          extractionMethod: "source_field",
        })
      : [];
  }
  for (const row of officers) {
    row.evidence = row.source_record_uid
      ? observationEvidence(evidenceByUid, row.source_record_uid, {
          extractionMethod: "ixbrl_signature",
        })
      : [];
  }
  const latestWikidataEvidence =
    sourceRecords.find((row) =>
      row.evidence[0]?.origins.some(
        (origin) => origin.sourceSlug === "wikidata",
      ),
    )?.evidence ?? [];
  if (wikidataRows[0]) wikidataRows[0].evidence = latestWikidataEvidence;
  for (const row of wikidataPeople) row.evidence = latestWikidataEvidence;

  // Same-name matches need the officers' names, so this can only start once
  // officersPromise has resolved — one batched query, not per-person fetches.
  let peopleMatches: PeopleMatchRow[] = [];
  if (country.detail?.peopleMatchesQuery && officers.length > 0) {
    const names = Array.from(
      new Set(
        officers
          .map((o) => `${o.first_name} ${o.last_name}`.trim().toLowerCase())
          .filter((name) => name !== ""),
      ),
    );
    if (names.length > 0) {
      peopleMatches = await chQuery<PeopleMatchRow>(
        country.detail.peopleMatchesQuery,
        { id, names },
      ).catch(() => [] as PeopleMatchRow[]);
    }
  }

  return {
    company,
    record: shell.record,
    financials,
    frFinancials,
    contacts,
    domains,
    statements,
    industries,
    addresses,
    taxRecords,
    publicContracts,
    contractSummary: contractSummaryRows[0] ?? null,
    secondaryNames,
    officers,
    peopleMatches,
    audit: auditRows[0] ?? null,
    gleifRelationships,
    gleifEntity: gleifEntityRows[0] ?? null,
    wikidata: wikidataRows[0] ?? null,
    wikidataPeople,
    esefFilings,
    financialSources,
    sourceRecords,
    descriptions,
    esefPeople,
    businessItems,
    sourceRelationships,
    sourceContacts,
  };
}
