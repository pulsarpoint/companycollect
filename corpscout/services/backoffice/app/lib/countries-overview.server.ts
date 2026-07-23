import { chQuery } from "~/lib/clickhouse.server";
import { COUNTRIES } from "~/lib/countries";
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
  const [registryRows, financialRows] = await Promise.all([
    chQuery<RawDirectoryRow>(`
SELECT country_code,
  toUInt64(count()) AS total_companies,
  toUInt64(countIf(has_financials = 1)) AS companies_with_financials
FROM companies_all
GROUP BY country_code`),
    getAllCountryFinancialTotals(),
  ]);

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
  const rows = await chQuery<{
    code: string;
    label: string;
    companies: number | string;
  }>(
    `SELECT industry_code AS code,
       any(industry_label) AS label,
       toUInt64(count()) AS companies
     FROM companies_all
     WHERE country_code = {code:String} AND industry_code != ''
     GROUP BY code
     ORDER BY companies DESC, code ASC
     LIMIT 15`,
    { code },
  );

  return rows.map((row) => ({
    code: row.code,
    label: row.label || row.code,
    companies: Number(row.companies),
  }));
}
