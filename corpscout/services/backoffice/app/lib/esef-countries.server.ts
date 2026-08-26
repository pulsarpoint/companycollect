import { chQuery } from "~/lib/clickhouse.server";

interface EsefCountryRow {
  country_iso2: string;
}

/** Return the country codes present in the ESEF filing document index. */
export async function loadEsefCountryCodes(): Promise<string[]> {
  const rows = await chQuery<EsefCountryRow>(`
    SELECT DISTINCT upperUTF8(trim(country)) AS country_iso2
    FROM esef_filings
    WHERE country != ''
    ORDER BY country_iso2
  `);

  return [
    ...new Set(
      rows
        .map(({ country_iso2 }) => country_iso2.trim().toUpperCase())
        .filter((countryIso2) => /^[A-Z]{2}$/.test(countryIso2)),
    ),
  ].sort();
}
