import { chQuery } from "~/lib/clickhouse.server";

export type MacroIndicatorValue = {
  value: number;
  year: number;
};

export type CountryMacroIndicators = {
  gdp: MacroIndicatorValue | null;
  exports: MacroIndicatorValue | null;
  imports: MacroIndicatorValue | null;
};

const INDICATORS = {
  gdp: "NY.GDP.MKTP.CD",
  exports: "NE.EXP.GNFS.CD",
  imports: "NE.IMP.GNFS.CD",
} as const;

type MacroIndicatorRow = {
  indicator_code: string;
  value: number;
  year: number;
};

type MacroQuery = (
  sql: string,
  params?: Record<string, unknown>,
) => Promise<MacroIndicatorRow[]>;

export async function getCountryMacroIndicators(
  countryCode: string,
  options: { queryImpl?: MacroQuery } = {},
): Promise<CountryMacroIndicators> {
  const queryImpl: MacroQuery =
    options.queryImpl ??
    ((sql, params) => chQuery<MacroIndicatorRow>(sql, params));
  const indicatorCodes = Object.values(INDICATORS);
  const rows = await queryImpl(
    `
      SELECT
        indicator_code,
        argMax(value, year) AS value,
        max(year) AS year
      FROM corpscout.world_bank_macro_observations
      WHERE country_code = {country:String}
        AND indicator_code IN {indicators:Array(String)}
      GROUP BY indicator_code
    `,
    {
      country: countryCode.toLowerCase(),
      indicators: indicatorCodes,
    },
  );

  const valuesByIndicator = new Map<string, MacroIndicatorValue>();
  for (const row of rows) {
    const value = Number(row.value);
    const year = Number(row.year);
    if (Number.isFinite(value) && Number.isInteger(year)) {
      valuesByIndicator.set(row.indicator_code, { value, year });
    }
  }

  return {
    gdp: valuesByIndicator.get(INDICATORS.gdp) ?? null,
    exports: valuesByIndicator.get(INDICATORS.exports) ?? null,
    imports: valuesByIndicator.get(INDICATORS.imports) ?? null,
  };
}
