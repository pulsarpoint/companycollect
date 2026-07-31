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
  // Reserved for a future industry column — no route renders it yet, and for
  // SE `industryQuery` needs a prefixed-id expr when wired (see review 2026-07-18).
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
  // argMax(description_en, is_current) is a contractual pick-the-current-row
  // reduction (unlike relying on ORDER BY-then-any() collapsing groups in
  // scan order); verified to return identical labels for all ~84 divisions,
  // including code '45' (REV_2-only, is_current always 0 — argMax still
  // deterministically picks a row when values tie).
  const rows = await chQuery<{ code: string; label: string }>(`
SELECT normalized_code AS code, argMax(description_en, is_current) AS label
FROM nace_categories WHERE level = 'division'
GROUP BY code`);
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

/**
 * Per-country UNION ALL results come back in whatever order ClickHouse
 * happens to schedule the branches — not user visible ordering. Sort by
 * revenue_usd desc (nulls last), with country_code asc as a stable tiebreak,
 * so "Revenue by country" charts/tables render identically on every load.
 */
function sortCountryTotals(rows: CountryTotals[]): CountryTotals[] {
  return [...rows].sort((a, b) => {
    if (a.revenue_usd == null && b.revenue_usd == null) return a.country_code.localeCompare(b.country_code);
    if (a.revenue_usd == null) return 1;
    if (b.revenue_usd == null) return -1;
    if (b.revenue_usd !== a.revenue_usd) return b.revenue_usd - a.revenue_usd;
    return a.country_code.localeCompare(b.country_code);
  });
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
  const table = c.financialsLatest!.table;
  // Pre-filter the industries table to companies present in the (much
  // smaller) summary table BEFORE the GROUP BY, instead of grouping the
  // entire industries table and only narrowing it down at the join. Cuts
  // GB (7.3M industries rows) from ~0.6s to ~0.2s for this branch.
  return `SELECT substring(i.code, 1, 2) AS division, toUInt32(count()) AS companies,
  sum(f.revenue_amount_usd) AS revenue_usd
FROM ${table} f
INNER JOIN (
  SELECT ${nace.companyKeyExpr} AS cid, any(${nace.naceCodeExpr}) AS code
  FROM ${nace.industriesTable}
  WHERE ${nace.filterExpr} AND ${nace.companyKeyExpr} IN (SELECT company_id FROM ${table})
  GROUP BY cid
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
      // industry_label enrichment removed: no route rendered it, and for SE
      // it silently matched zero rows anyway (id-prefix mismatch). See the
      // TopCompany.industry_label field comment.
      const nameRows = await chQuery<{ id: string; name: string }>(nameLookupSql(country), { ids });
      const nameById = new Map(nameRows.map((r) => [r.id, r.name]));
      return group.map(
        (r): TopCompany => ({
          country_code: r.country_code,
          company_id: r.company_id,
          name: nameById.get(r.company_id) ?? r.company_id,
          revenue_usd: r.revenue_usd == null ? null : Number(r.revenue_usd),
          fiscal_year: r.fiscal_year == null ? null : Number(r.fiscal_year),
          industry_label: null,
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

/**
 * Financial totals used by both the financials landing and the country
 * directory. Keeping this query here preserves each registry source's sum
 * rules, including Norway's foreign-branch exclusion.
 */
export async function getAllCountryFinancialTotals(): Promise<CountryTotals[]> {
  const rows = await chQuery<RawTotalsRow>(allCountryTotalsSql());
  return sortCountryTotals(rows.map(parseTotals));
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
    getAllCountryFinancialTotals(),
    chQuery<RawDivisionRow>(landingTopDivisionsSql()),
    chQuery<RawTopCompanyRow>(landingTopCompaniesSql()),
  ]);
  const topCompanies = await enrichTopCompanies(topRows);
  return {
    countries: totalsRows,
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

export async function getIndustryFinancials(
  division: string,
  countryCode?: string,
): Promise<null | {
  division: string;
  label: string;
  countryCode: string | null;
  countries: CountryTotals[];
  topCompanies: TopCompany[];
}> {
  if (!/^\d{2}$/.test(division)) return null;
  const labels = await getDivisionLabels();
  const label = labels.get(division);
  if (!label) return null;

  const normalizedCountryCode = countryCode?.toLowerCase();
  const countries = normalizedCountryCode
    ? naceCountries().filter((country) => country.code === normalizedCountryCode)
    : naceCountries();
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
  // Countries with zero companies in this division add nothing but noise
  // ("0 / —" rows) to the country table and chart — drop them here so every
  // consumer of getIndustryFinancials sees only countries actually present.
  const presentCountries = sortCountryTotals(totalsRows.map(parseTotals)).filter((c) => c.companies > 0);
  return {
    division,
    label,
    countryCode: normalizedCountryCode ?? null,
    countries: presentCountries,
    topCompanies,
  };
}

// ---------- Per-year country overview ----------

/**
 * Which fiscal years a country can actually show.
 *
 * Read from the data rather than assumed, so the year selector offers only
 * years that will render something. Bounded to a decade: the metrics tables
 * reach back to 2017 and the overview chart shows ten years.
 */
export async function getCountryFinancialYears(code: string): Promise<number[]> {
  const country = getCountry(code);
  if (!country?.financialsByYear) return [];
  const rows = await chQuery<{ fiscal_year: string; companies: string }>(
    `SELECT toString(fiscal_year) AS fiscal_year, toString(count()) AS companies
     FROM ${country.financialsByYear.table}
     WHERE revenue_amount_usd IS NOT NULL
     GROUP BY fiscal_year
     ORDER BY fiscal_year`,
  );
  // A year with a handful of early filings is not worth offering — it would
  // show a "leading industries" ranking built from a dozen companies.
  const meaningful = rows.filter((r) => Number(r.companies) >= 100);
  return meaningful.map((r) => Number(r.fiscal_year));
}

/**
 * The year to land on: the most recent one that is substantially filed.
 *
 * Not simply the latest. Sweden's newest fiscal year carries 10,789 filings
 * against the previous year's 386,371 — accounts arrive for months after a year
 * ends — so landing there would show a real year that reads as a collapse. A
 * quarter of the busiest year is the bar; thinner years stay selectable.
 */
export async function getCountryDefaultFinancialYear(
  code: string,
): Promise<number | null> {
  const country = getCountry(code);
  if (!country?.financialsByYear) return null;
  const rows = await chQuery<{ fiscal_year: string; companies: string }>(
    `SELECT toString(fiscal_year) AS fiscal_year, toString(count()) AS companies
     FROM ${country.financialsByYear.table}
     WHERE revenue_amount_usd IS NOT NULL
     GROUP BY fiscal_year
     ORDER BY fiscal_year`,
  );
  if (rows.length === 0) return null;
  const peak = Math.max(...rows.map((r) => Number(r.companies)));
  const substantial = rows.filter((r) => Number(r.companies) >= peak * 0.25);
  if (substantial.length === 0) return Number(rows[rows.length - 1].fiscal_year);
  return Number(substantial[substantial.length - 1].fiscal_year);
}

/**
 * Totals, leading divisions and top companies for ONE fiscal year.
 *
 * Deliberately a separate path from getCountryFinancials: that one reads
 * financialsLatest, which holds a single row per company — its most recent
 * filing — and so mixes fiscal years across companies. Asking it for a year is
 * not possible, and changing it would alter the landing page and every
 * industry page. This reads the metrics table, which keeps every year.
 */
export async function getCountryFinancialsForYear(
  code: string,
  year: number,
): Promise<null | {
  totals: CountryTotals;
  divisions: DivisionRevenue[] | null;
  topCompanies: TopCompany[];
}> {
  const country = getCountry(code);
  if (!country?.financialsByYear) return null;
  const { table, idColumn } = country.financialsByYear;
  const nace = country.financialsAggregates?.nace;
  const scope = `f.fiscal_year = {year:UInt16} AND f.revenue_amount_usd IS NOT NULL`;

  const [labels, totalsRows, divisionRows, topRows] = await Promise.all([
    getDivisionLabels(),
    chQuery<RawTotalsRow>(
      `SELECT '${country.code}' AS country_code,
              toUInt32(count()) AS companies,
              sum(f.revenue_amount_usd) AS revenue_usd,
              max(f.fiscal_year) AS latest_fiscal_year
       FROM ${table} f
       WHERE ${scope}`,
      { year },
    ),
    nace
      ? chQuery<RawDivisionRow>(
          `SELECT substring(i.code, 1, 2) AS division,
                  toUInt32(count()) AS companies,
                  sum(f.revenue_amount_usd) AS revenue_usd
           FROM ${table} f
           INNER JOIN (
             SELECT ${nace.companyKeyExpr} AS cid, any(${nace.naceCodeExpr}) AS code
             FROM ${nace.industriesTable}
             WHERE ${nace.filterExpr}
             GROUP BY cid
           ) i ON i.cid = toString(f.${idColumn})
           WHERE ${scope}
           GROUP BY division
           ORDER BY revenue_usd DESC
           LIMIT ${TOP_DIVISIONS_LIMIT}`,
          { year },
        )
      : Promise.resolve(null),
    chQuery<RawTopCompanyRow>(
      `SELECT '${country.code}' AS country_code,
              toString(f.${idColumn}) AS company_id,
              f.revenue_amount_usd AS revenue_usd,
              f.fiscal_year AS fiscal_year,
              toUInt8(0) AS excluded_from_sums
       FROM ${table} f
       WHERE ${scope}
       ORDER BY f.revenue_amount_usd DESC
       LIMIT ${TOP_COMPANIES_LIMIT}`,
      { year },
    ),
  ]);

  return {
    totals: parseTotals(totalsRows[0]),
    divisions: divisionRows ? divisionRows.map((r) => toDivisionRevenue(r, labels)) : null,
    topCompanies: await enrichTopCompanies(topRows),
  };
}
