import { chQuery } from "~/lib/clickhouse.server";
import { COUNTRIES, getCountry } from "~/lib/countries";
import { getAllCountryFinancialTotals } from "~/lib/financial-aggregates.server";

export type CountryDirectoryRow = {
  country_code: string;
  country_name: string;
  flag: string;
  total_companies: number;
  companies_with_financials: number;
  revenue_usd: number | null;
  latest_fiscal_year: number | null;
};

export type CountryIndustryGroup = {
  code: string;
  label: string;
  companies: number;
};

type RawDirectoryRow = {
  country_code: string;
  total_companies: number | string;
  companies_with_financials: number | string;
};

export async function getCountryDirectory(): Promise<CountryDirectoryRow[]> {
  // One UNION leg per country, against that country's own tables. Table names
  // and code literals come from the static registry only. A company "has
  // financials" when its country's latest-financials summary holds a row for
  // it — countries without a summary table report 0, which is the truth.
  const totalsSql = COUNTRIES.map(
    (c) =>
      `SELECT '${c.code}' AS country_code, toUInt64(count()) AS total_companies FROM ${c.companiesTable}`,
  ).join("\nUNION ALL\n");
  const financialsCountSql = COUNTRIES.filter((c) => c.financialsLatest)
    .map(
      (c) =>
        `SELECT '${c.code}' AS country_code, toUInt64(count()) AS companies_with_financials FROM ${c.financialsLatest!.table}`,
    )
    .join("\nUNION ALL\n");

  const [totalRows, financialCountRows, financialRows] = await Promise.all([
    chQuery<{ country_code: string; total_companies: number | string }>(totalsSql),
    chQuery<{ country_code: string; companies_with_financials: number | string }>(
      financialsCountSql,
    ),
    getAllCountryFinancialTotals(),
  ]);
  const registryRows: RawDirectoryRow[] = totalRows.map((row) => ({
    country_code: row.country_code,
    total_companies: row.total_companies,
    companies_with_financials:
      financialCountRows.find((f) => f.country_code === row.country_code)
        ?.companies_with_financials ?? 0,
  }));

  const registryByCode = new Map(registryRows.map((row) => [row.country_code, row]));
  const financialByCode = new Map(financialRows.map((row) => [row.country_code, row]));

  return COUNTRIES.map((country) => {
    const registry = registryByCode.get(country.code);
    const financials = financialByCode.get(country.code);
    return {
      country_code: country.code,
      country_name: country.name,
      flag: country.flag,
      total_companies: Number(registry?.total_companies ?? 0),
      companies_with_financials: Number(registry?.companies_with_financials ?? 0),
      revenue_usd: financials?.revenue_usd ?? null,
      latest_fiscal_year: financials?.latest_fiscal_year ?? null,
    };
  }).sort((a, b) => a.country_name.localeCompare(b.country_name));
}

export async function getCountryIndustryGroups(code: string): Promise<CountryIndustryGroup[]> {
  // The country's own facet query (value, label, cnt — count-sorted, no
  // params) is exactly this breakdown. A country that publishes no industry
  // axis has no query and honestly reports nothing.
  const country = getCountry(code);
  if (!country?.industryFacetQuery) return [];

  const rows = await chQuery<{ value: string; label: string; cnt: number | string }>(
    country.industryFacetQuery,
  );

  return rows.slice(0, 15).map((row) => ({
    code: row.value,
    label: row.label || row.value,
    companies: Number(row.cnt),
  }));
}
