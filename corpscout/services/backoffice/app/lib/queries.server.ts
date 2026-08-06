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
  type FinancialSourceDefinition,
  type SortDir,
} from "~/lib/countries";
import type { CompanyFilters } from "~/lib/filters";
import type { FinancialReportDocumentSummary } from "~/lib/norway-financial-reports";
import { getNorwayFinancialReports } from "~/lib/norway-financial-reports.server";

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
  evidence?: EvidenceRef[];
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
  country_iso2: string;
  person_id: string;
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

export const COMPANY_SOURCE_RECORDS_QUERY = `
SELECT
  toString(links.source_record_uid) AS source_record_uid,
  records.record_kind AS record_kind,
  records.content_sha256 AS content_sha256,
  toString(records.first_seen_at) AS first_seen_at,
  toString(records.last_seen_at) AS last_seen_at,
  coalesce(origins.source_slug, '') AS source_slug,
  coalesce(origins.source_record_key, '') AS source_record_key,
  coalesce(origins.source_url, '') AS source_url,
  coalesce(origins.source_object_key, '') AS source_object_key,
  coalesce(origins.payload_sha256, '') AS payload_sha256,
  coalesce(toString(origins.retrieved_at), '') AS retrieved_at,
  coalesce(origins.source_run_id, '') AS source_run_id
FROM corpscout.company_source_record_links AS links FINAL
INNER JOIN corpscout.company_source_records AS records FINAL
  ON records.source_record_uid = links.source_record_uid
LEFT JOIN corpscout.company_source_record_origins AS origins FINAL
  ON origins.source_record_uid = links.source_record_uid
WHERE links.country_code = {country:String}
  AND links.company_id = {id:String}
ORDER BY records.last_seen_at DESC, source_slug, source_record_key`;

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
        chQuery<CompanySourceRecordQueryRow>(
          COMPANY_SOURCE_RECORDS_QUERY,
          evidenceParams,
        ),
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
    () =>
      chQuery<CompanySourceRecordQueryRow>(
        COMPANY_SOURCE_RECORDS_QUERY,
        evidenceParams,
      ),
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
    const key = company.__industry_key ?? "";
    const industries = key
      ? await chQuery<IndustryRow>(country.industryQuery, { ids: [key] })
      : [];
    company.industry_code = industries[0]?.industry_code ?? null;
    company.industry_label = industries[0]?.industry_label ?? null;
    delete company.__industry_key;
  }

  const [
    records,
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
