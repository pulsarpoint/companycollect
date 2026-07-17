import { chQuery } from "~/lib/clickhouse.server";
import { COUNTRIES, getCountry, type CountryConfig } from "~/lib/countries";

export const TOP_COMPANIES_LIMIT = 25;
export const TOP_DIVISIONS_LIMIT = 15;

export type CountryTotals = {
  country_code: string;
  companies: number;
  revenue_usd: number | null;
  latest_fiscal_year: number | null;
};

export type DivisionRevenue = {
  division: string;
  label: string;
  companies: number;
  revenue_usd: number | null;
};

export type TopCompany = {
  country_code: string;
  company_id: string;
  name: string;
  revenue_usd: number | null;
  fiscal_year: number | null;
  industry_label: string | null;
  excluded_from_sums: boolean;
};

const summaryCountries = () => COUNTRIES.filter((c) => c.financialsLatest);
const naceCountries = () => COUNTRIES.filter((c) => c.financialsLatest && c.financialsAggregates?.nace);
const exclusion = (c: CountryConfig) => c.financialsAggregates?.sumExclusionExpr;
// NOTE: no revenue-IS-NOT-NULL conjunct here — `sum()` skips NULLs on its own,
// and adding it would silently shrink the companies COUNT (NO has ~90k summary
// rows with fiscal data but no convertible revenue). Counts mean "companies
// with financial data (minus sum-exclusions)".
const sumWhere = (c: CountryConfig, extra?: string) => {
  const conds = [exclusion(c), extra].filter(Boolean);
  return conds.length > 0 ? `WHERE ${conds.join(" AND ")}` : "";
};

// ---------- Division labels (24h module-level cache) ----------

const DIVISION_LABELS_TTL_MS = 24 * 60 * 60 * 1000;
let divisionLabelsCache: { loadedAt: number; labels: Map<string, string> } | null = null;

/** Strips the leading "62 " style code that nace_categories bakes into description_en. */
function stripLeadingCode(label: string): string {
  return label.replace(/^\d+\s+/, "");
}

/**
 * Division-level (2-digit) NACE labels, current revision preferred, falling
 * back to a REV_2-only code when no current-revision row exists for it.
 * Cached in-memory for 24h, mirroring the facet-cache pattern in facets.server.ts.
 */
export async function getDivisionLabels(): Promise<Map<string, string>> {
  if (divisionLabelsCache && Date.now() - divisionLabelsCache.loadedAt < DIVISION_LABELS_TTL_MS) {
    return divisionLabelsCache.labels;
  }
  const rows = await chQuery<{ code: string; label: string }>(`
SELECT code, any(label) AS label FROM (
  SELECT normalized_code AS code, description_en AS label
  FROM nace_categories WHERE level = 'division'
  ORDER BY is_current DESC
) GROUP BY code`);
  const labels = new Map(rows.map((r) => [r.code, stripLeadingCode(r.label)]));
  divisionLabelsCache = { loadedAt: Date.now(), labels };
  return labels;
}

// ---------- Raw row shapes + parsers ----------

interface RawTotalsRow {
  country_code: string;
  companies: number | string;
  revenue_usd: number | string | null;
  latest_fiscal_year: number | string | null;
}

interface RawDivisionRow {
  division: string;
  companies: number | string;
  revenue_usd: number | string | null;
}

interface RawTopCompanyRow {
  country_code: string;
  company_id: string;
  revenue_usd: number | string | null;
  fiscal_year: number | string | null;
  excluded_from_sums: number | string;
}

function parseTotals(row: RawTotalsRow): CountryTotals {
  return {
    country_code: row.country_code,
    companies: Number(row.companies),
    revenue_usd: row.revenue_usd == null ? null : Number(row.revenue_usd),
    latest_fiscal_year: row.latest_fiscal_year == null ? null : Number(row.latest_fiscal_year),
  };
}

function toDivisionRevenue(row: RawDivisionRow, labels: Map<string, string>): DivisionRevenue {
  return {
    division: row.division,
    label: labels.get(row.division) ?? row.division,
    companies: Number(row.companies),
    revenue_usd: row.revenue_usd == null ? null : Number(row.revenue_usd),
  };
}

// ---------- SQL builders ----------

function countryTotalsSql(c: CountryConfig, extra?: string): string {
  return `SELECT '${c.code}' AS country_code, toUInt32(count()) AS companies,
  sum(f.revenue_amount_usd) AS revenue_usd, max(f.fiscal_year) AS latest_fiscal_year
FROM ${c.financialsLatest!.table} f
${sumWhere(c, extra)}`;
}

function countryDivisionsSql(c: CountryConfig): string {
  const nace = c.financialsAggregates!.nace!;
  return `SELECT substring(i.code, 1, 2) AS division, toUInt32(count()) AS companies,
  sum(f.revenue_amount_usd) AS revenue_usd
FROM ${c.financialsLatest!.table} f
INNER JOIN (
  SELECT ${nace.companyKeyExpr} AS cid, any(${nace.naceCodeExpr}) AS code
  FROM ${nace.industriesTable} WHERE ${nace.filterExpr} GROUP BY cid
) i ON i.cid = f.company_id
${sumWhere(c)}
GROUP BY division ORDER BY revenue_usd DESC`;
}

/** Named-param division scope: user-supplied `division` binds via {division:String}, never interpolated. */
function divisionScopeExpr(c: CountryConfig): string {
  const nace = c.financialsAggregates!.nace!;
  return `f.company_id IN (
  SELECT ${nace.companyKeyExpr} FROM ${nace.industriesTable}
  WHERE ${nace.filterExpr} AND substring(${nace.naceCodeExpr}, 1, 2) = {division:String}
)`;
}

/** Lists keep excluded rows — no sumWhere/exclusion here, only the `excluded_from_sums` flag. */
function topCompaniesSql(c: CountryConfig, extra?: string): string {
  const excl = exclusion(c);
  const excludedExpr = excl ? `toUInt8(NOT (${excl}))` : `toUInt8(0)`;
  const conds = ["f.revenue_amount_usd IS NOT NULL", extra].filter(Boolean);
  return `SELECT '${c.code}' AS country_code, f.company_id AS company_id,
  f.revenue_amount_usd AS revenue_usd, f.fiscal_year AS fiscal_year,
  ${excludedExpr} AS excluded_from_sums
FROM ${c.financialsLatest!.table} f
WHERE ${conds.join(" AND ")}
ORDER BY f.revenue_amount_usd DESC
LIMIT ${TOP_COMPANIES_LIMIT}`;
}

function nameLookupSql(c: CountryConfig): string {
  return `SELECT toString(${c.idColumn}) AS id, ${c.nameColumn} AS name
FROM ${c.companiesTable}
WHERE ${c.idColumn} IN {ids:Array(String)}`;
}

// ---------- Name + industry enrichment ----------

async function enrichTopCompanies(rows: RawTopCompanyRow[]): Promise<TopCompany[]> {
  if (rows.length === 0) return [];
  const byCountry = new Map<string, RawTopCompanyRow[]>();
  for (const row of rows) {
    const group = byCountry.get(row.country_code) ?? [];
    group.push(row);
    byCountry.set(row.country_code, group);
  }
  const groups = await Promise.all(
    [...byCountry.entries()].map(async ([code, group]) => {
      const country = getCountry(code);
      if (!country) return [];
      const ids = group.map((r) => r.company_id);
      const [nameRows, industryRows] = await Promise.all([
        chQuery<{ id: string; name: string }>(nameLookupSql(country), { ids }),
        country.industryQuery
          ? chQuery<{ company_id: string; industry_label: string | null }>(country.industryQuery, { ids })
          : Promise.resolve([]),
      ]);
      const nameById = new Map(nameRows.map((r) => [r.id, r.name]));
      const industryById = new Map(industryRows.map((r) => [r.company_id, r.industry_label]));
      return group.map(
        (r): TopCompany => ({
          country_code: r.country_code,
          company_id: r.company_id,
          name: nameById.get(r.company_id) ?? r.company_id,
          revenue_usd: r.revenue_usd == null ? null : Number(r.revenue_usd),
          fiscal_year: r.fiscal_year == null ? null : Number(r.fiscal_year),
          industry_label: industryById.get(r.company_id) ?? null,
          excluded_from_sums: Boolean(Number(r.excluded_from_sums)),
        }),
      );
    }),
  );
  return groups.flat().sort((a, b) => (b.revenue_usd ?? -Infinity) - (a.revenue_usd ?? -Infinity));
}

// ---------- Landing (global) overview ----------

function allCountryTotalsSql(): string {
  return summaryCountries()
    .map((c) => countryTotalsSql(c))
    .join(" UNION ALL ");
}

function landingTopDivisionsSql(): string {
  const branches = naceCountries().map((c) => countryDivisionsSql(c));
  return `SELECT division, toUInt32(sum(companies)) AS companies, sum(revenue_usd) AS revenue_usd
FROM (${branches.join(" UNION ALL ")})
GROUP BY division
ORDER BY revenue_usd DESC
LIMIT ${TOP_DIVISIONS_LIMIT}`;
}

function landingTopCompaniesSql(): string {
  const branches = summaryCountries().map((c) => topCompaniesSql(c));
  return `SELECT country_code, company_id, revenue_usd, fiscal_year, excluded_from_sums
FROM (${branches.join(" UNION ALL ")})
ORDER BY revenue_usd DESC
LIMIT ${TOP_COMPANIES_LIMIT}`;
}

export async function getGlobalFinancialOverview(): Promise<{
  countries: CountryTotals[];
  topDivisions: DivisionRevenue[];
  topCompanies: TopCompany[];
}> {
  const [labels, totalsRows, divisionRows, topRows] = await Promise.all([
    getDivisionLabels(),
    chQuery<RawTotalsRow>(allCountryTotalsSql()),
    chQuery<RawDivisionRow>(landingTopDivisionsSql()),
    chQuery<RawTopCompanyRow>(landingTopCompaniesSql()),
  ]);
  const topCompanies = await enrichTopCompanies(topRows);
  return {
    countries: totalsRows.map(parseTotals),
    topDivisions: divisionRows.map((r) => toDivisionRevenue(r, labels)),
    topCompanies,
  };
}

// ---------- Country page ----------

export async function getCountryFinancials(code: string): Promise<null | {
  totals: CountryTotals;
  divisions: DivisionRevenue[] | null;
  unmapped: DivisionRevenue | null;
  topCompanies: TopCompany[];
}> {
  const country = getCountry(code);
  if (!country?.financialsLatest) return null;
  const nace = country.financialsAggregates?.nace;

  const [labels, totalsRows, divisionRows, topRows] = await Promise.all([
    getDivisionLabels(),
    chQuery<RawTotalsRow>(countryTotalsSql(country)),
    nace ? chQuery<RawDivisionRow>(countryDivisionsSql(country)) : Promise.resolve(null),
    chQuery<RawTopCompanyRow>(topCompaniesSql(country)),
  ]);

  const totals = parseTotals(totalsRows[0]);
  let divisions: DivisionRevenue[] | null = null;
  let unmapped: DivisionRevenue | null = null;
  if (divisionRows) {
    divisions = divisionRows.map((r) => toDivisionRevenue(r, labels));
    const mappedCompanies = divisions.reduce((s, d) => s + d.companies, 0);
    const mappedRevenue = divisions.reduce((s, d) => s + (d.revenue_usd ?? 0), 0);
    unmapped = {
      division: "unmapped",
      label: "Unmapped",
      companies: totals.companies - mappedCompanies,
      revenue_usd: (totals.revenue_usd ?? 0) - mappedRevenue,
    };
  }

  const topCompanies = await enrichTopCompanies(topRows);
  return { totals, divisions, unmapped, topCompanies };
}

// ---------- Industry (division) page ----------

export async function getIndustryFinancials(division: string): Promise<null | {
  division: string;
  label: string;
  countries: CountryTotals[];
  topCompanies: TopCompany[];
}> {
  if (!/^\d{2}$/.test(division)) return null;
  const labels = await getDivisionLabels();
  const label = labels.get(division);
  if (!label) return null;

  const countries = naceCountries();
  if (countries.length === 0) return null;

  const params = { division };
  const totalsSql = countries.map((c) => countryTotalsSql(c, divisionScopeExpr(c))).join(" UNION ALL ");
  const topSql = countries.map((c) => topCompaniesSql(c, divisionScopeExpr(c))).join(" UNION ALL ");

  const [totalsRows, rawTop] = await Promise.all([
    chQuery<RawTotalsRow>(totalsSql, params),
    chQuery<RawTopCompanyRow>(
      `SELECT country_code, company_id, revenue_usd, fiscal_year, excluded_from_sums
       FROM (${topSql})
       ORDER BY revenue_usd DESC
       LIMIT ${TOP_COMPANIES_LIMIT}`,
      params,
    ),
  ]);

  const topCompanies = await enrichTopCompanies(rawTop);
  return { division, label, countries: totalsRows.map(parseTotals), topCompanies };
}
