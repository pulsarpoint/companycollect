import { chQuery } from "~/lib/clickhouse.server";
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
  type SortDir,
} from "~/lib/countries";
import type { CompanyFilters } from "~/lib/filters";

export interface CountryStats {
  total: number;
  active: number;
}

function clampInt(value: number | undefined, min: number, max: number, fallback: number): number {
  if (value === undefined || !Number.isFinite(value)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(value)));
}

export async function getCountryStats(country: CountryConfig): Promise<CountryStats> {
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

export type CompanyListRow = Record<string, string | number | null | CompanyFlagId[]> & {
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
    "market" in source
      ? `SELECT toString(company_id) FROM company_market_summary
         WHERE country_code = '${country.code.toUpperCase()}'`
      : `SELECT toString(${source.idColumn}) FROM ${source.table}`;
  return want ? `${id} IN (${subquery})` : `${id} NOT IN (${subquery})`;
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
      if (typeof inline !== "object" && inline !== undefined && Number(inline) === 1) {
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
    ...(country.industryQuery ? [`toString(${joinKeyExpr}) AS __industry_key`] : []),
    ...(country.placeQuery ? [`toString(${country.idColumn}) AS __place_key`] : []),
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
  delete company.__industry_key;
  return { company, record: records[0] ?? {} };
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
    conds.push(`${country.searchColumnExpr ?? country.nameColumn} ILIKE {pattern:String}`);
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
    const ids = rows.map((r) => String(r.__place_key ?? "")).filter((v) => v !== "");
    const places = ids.length
      ? await chQuery<{ company_id: string; place: string }>(country.placeQuery, { ids })
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
  ebitda_original: number | null;
  ebit_original: number | null;
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
  /** English concept label (SE joins se_financial_concept_labels; other
   * countries' factsQuery don't return it). Empty until translated. */
  concept_label_en?: string | null;
  value_kind: string;
  raw_value: string;
  amount_original: number | null;
  amount_usd: number | null;
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
  const rows = await chQuery<FactsDocument>(country.detail.factsDocumentQuery, { id, year });
  return rows[0] ?? null;
}

export interface ContactRow {
  contact_type: string;
  contact_value: string;
}

export interface DomainRow {
  domain: string;
  website_url: string | null;
  domain_source: string;
  confidence: number | null;
  is_primary: 0 | 1;
}

export interface IndustryDetailRow {
  industry_code: string;
  description_original: string;
  industry_label: string;
  is_primary: 0 | 1;
}

export interface AddressRow {
  address_type: string;
  full_address: string;
  /**
   * A geocoder-friendly rendering of the same address, where the display form is
   * too noisy to resolve. Optional: most registers publish an address Nominatim
   * handles as-is, and those omit it.
   */
  geocode_address?: string;
}

export interface SecondaryNameRow {
  name: string;
  name_kind: "secondary" | "foreign";
  registered: string;
  scope: string;
}

export interface OfficerRow {
  first_name: string;
  last_name: string;
  role_original: string;
  role_kind: string;
  signatory_kind: string;
  fiscal_year: number;
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
}

/** One fiscal year of consolidated IFRS figures extracted from a company's
 * ESEF annual report, with a link back to the source document. */
export interface EsefFilingRow {
  fiscal_year: number;
  period_end: string;
  currency: string;
  revenue_amount_original: number | null;
  revenue_amount_usd: number | null;
  operating_profit_amount_usd: number | null;
  profit_loss_amount_usd: number | null;
  total_assets_amount_usd: number | null;
  equity_amount_usd: number | null;
  viewer_url: string;
  filing_versions: number;
  composed_from_amendment: number;
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
    m.fiscal_year AS fiscal_year,
    m.period_end AS period_end,
    m.currency AS currency,
    m.revenue_amount_original AS revenue_amount_original,
    m.revenue_amount_usd AS revenue_amount_usd,
    m.operating_profit_amount_usd AS operating_profit_amount_usd,
    m.profit_loss_amount_usd AS profit_loss_amount_usd,
    m.total_assets_amount_usd AS total_assets_amount_usd,
    m.equity_amount_usd AS equity_amount_usd,
    m.viewer_url AS viewer_url,
    toUInt32(extract(m.fxo_id, '-([0-9]+)$')) AS version
  FROM corpscout.esef_financial_metrics AS m
  INNER JOIN corpscout.company_identifier AS c
    ON c.issuer_scheme = 'lei'
   AND c.issuer_id = upperUTF8(trimBoth(m.lei))
  WHERE c.country_code = {country:String}
    AND c.company_id = {id:String}
)
SELECT
  v.fiscal_year AS fiscal_year,
  toString(v.period_end) AS period_end,
  argMaxIf(v.currency, v.version, v.currency != '') AS currency,
  argMaxIf(v.revenue_amount_original, v.version, v.revenue_amount_original IS NOT NULL) AS revenue_amount_original,
  argMaxIf(v.revenue_amount_usd, v.version, v.revenue_amount_usd IS NOT NULL) AS revenue_amount_usd,
  argMaxIf(v.operating_profit_amount_usd, v.version, v.operating_profit_amount_usd IS NOT NULL) AS operating_profit_amount_usd,
  argMaxIf(v.profit_loss_amount_usd, v.version, v.profit_loss_amount_usd IS NOT NULL) AS profit_loss_amount_usd,
  argMaxIf(v.total_assets_amount_usd, v.version, v.total_assets_amount_usd IS NOT NULL) AS total_assets_amount_usd,
  argMaxIf(v.equity_amount_usd, v.version, v.equity_amount_usd IS NOT NULL) AS equity_amount_usd,
  -- Newest version that actually carries a link, not simply the newest: an
  -- amendment sometimes lands without a viewer_url and the row would then
  -- offer no way back to the source document.
  argMaxIf(v.viewer_url, v.version, v.viewer_url != '') AS viewer_url,
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

export async function getCompanyDetail(
  country: CountryConfig,
  id: string,
): Promise<CompanyDetail | null> {
  const selectList = buildCompanySelectList(country);

  const rows = await chQuery<CompanyListRow & { __industry_key?: string }>(
    `SELECT ${selectList}
     FROM ${country.companiesTable}
     WHERE ${country.idColumn} = {id:String}
     LIMIT 1`,
    { id },
  );
  if (rows.length === 0) return null;
  const company = rows[0];

  // Kick off the record + section queries immediately — they only depend on
  // `id` — so they run concurrently with the industry round-trip below
  // instead of after it.
  const recordPromise = chQuery<Record<string, unknown>>(
    country.detail?.recordQuery ??
      `SELECT * FROM ${country.companiesTable} WHERE ${country.idColumn} = {id:String} LIMIT 1`,
    { id },
  );
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
    ? chQuery<GleifRelationshipRow>(country.detail.gleifRelationshipsQuery, { id })
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
  // No-op guards close the unhandled-rejection window between promise
  // construction and the `await` below — the await still surfaces real errors.
  recordPromise.catch(() => {});
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
  frFinancialsPromise.catch(() => {});

  if (country.industryQuery) {
    const key = company.__industry_key ?? "";
    const industries = key
      ? await chQuery<IndustryRow>(country.industryQuery, { ids: [key] })
      : [];
    company.industry_code = industries[0]?.industry_code ?? null;
    company.industry_label = industries[0]?.industry_label ?? null;
    delete company.__industry_key;
  }

  const [records, [financials, contacts, domains], statements, industries, addresses, taxRecords, publicContracts, secondaryNames, officers, auditRows, gleifRelationships, gleifEntityRows, wikidataRows, wikidataPeople, esefFilings, frFinancials] = await Promise.all([
    recordPromise,
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
    frFinancialsPromise,
  ]);

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
      peopleMatches = await chQuery<PeopleMatchRow>(country.detail.peopleMatchesQuery, { id, names }).catch(() => [] as PeopleMatchRow[]);
    }
  }

  return {
    company,
    record: records[0] ?? {},
    financials,
    frFinancials,
    contacts,
    domains,
    statements,
    industries,
    addresses,
    taxRecords,
    publicContracts,
    secondaryNames,
    officers,
    peopleMatches,
    audit: auditRows[0] ?? null,
    gleifRelationships,
    gleifEntity: gleifEntityRows[0] ?? null,
    wikidata: wikidataRows[0] ?? null,
    wikidataPeople,
    esefFilings,
  };
}
