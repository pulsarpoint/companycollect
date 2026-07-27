import { EU_EEA_COUNTRIES } from "./eu-countries";

export interface CompanyMatch {
  country_code: string; // register country, lowercase iso2 (e.g. "cz")
  company_id: string;
}

/** Registers outside the EU/EEA list that appear as buyer/winner countries. */
const EXTRA_ISO3: Record<string, string> = {
  BRA: "BR",
  GBR: "GB",
  USA: "US",
  CHE: "CH",
};

const ISO3_TO_ISO2: Record<string, string> = {
  ...Object.fromEntries(EU_EEA_COUNTRIES.map((c) => [c.iso3, c.iso2])),
  ...EXTRA_ISO3,
};

/** Normalizes a register/row country value ("CZE", "cz", "CZ", "") to
 * lowercase iso2, or null when unknown. */
export function toIso2(value: string | null | undefined): string | null {
  const raw = (value ?? "").trim().toUpperCase();
  if (raw.length === 2) return raw.toLowerCase();
  if (raw.length === 3) return ISO3_TO_ISO2[raw]?.toLowerCase() ?? null;
  return null;
}

/** Picks the register entry a buyer/winner cell may link to. National org
 * number formats collide across countries (a Czech ICO equalling a Brazilian
 * id was observed live), so a link is only produced when it is unambiguous:
 * either the row names a country and exactly that candidate exists, or there
 * is exactly one candidate and the row carries no country signal. */
export function pickCompanyMatch(
  candidates: CompanyMatch[] | undefined,
  rowCountry: string | null | undefined,
): CompanyMatch | null {
  if (!candidates || candidates.length === 0) return null;
  const iso2 = toIso2(rowCountry);
  if (iso2 !== null) {
    return candidates.find((c) => c.country_code.toLowerCase() === iso2) ?? null;
  }
  return candidates.length === 1 ? candidates[0] : null;
}
