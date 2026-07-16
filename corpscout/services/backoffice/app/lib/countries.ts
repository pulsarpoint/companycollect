export type CountryFeature = "financials" | "industries" | "contacts" | "domains";

export interface CountryConfig {
  /** Lowercase ISO2 code, used as the URL segment /:country. */
  code: string;
  name: string;
  flag: string;
  /** ClickHouse table holding the canonical company rows. */
  companiesTable: string;
  /** Column holding the national registry identifier. */
  idColumn: string;
  /** Column holding the display name. */
  nameColumn: string;
  /** SQL boolean expression selecting active companies. */
  activeExpr: string;
  /** Human-readable approximate row count, shown on the picker card. */
  approxCompanies: string;
  /** Which auxiliary data families exist for this country. */
  features: CountryFeature[];
}

export const COUNTRIES: CountryConfig[] = [
  { code: "no", name: "Norway", flag: "🇳🇴", companiesTable: "no_companies", idColumn: "org_number", nameColumn: "name", activeExpr: "is_active = 1", approxCompanies: "1.2M", features: ["financials", "industries", "contacts", "domains"] },
  { code: "fi", name: "Finland", flag: "🇫🇮", companiesTable: "fi_companies", idColumn: "business_id", nameColumn: "name", activeExpr: "is_active = 1", approxCompanies: "460k", features: ["financials", "industries", "contacts", "domains"] },
  { code: "se", name: "Sweden", flag: "🇸🇪", companiesTable: "se_companies", idColumn: "registration_number", nameColumn: "legal_name", activeExpr: "status = 'active'", approxCompanies: "4.1M", features: ["financials", "industries"] },
  { code: "ee", name: "Estonia", flag: "🇪🇪", companiesTable: "ee_companies", idColumn: "reg_code", nameColumn: "name", activeExpr: "is_active = 1", approxCompanies: "373k", features: ["financials", "industries", "contacts", "domains"] },
  { code: "lv", name: "Latvia", flag: "🇱🇻", companiesTable: "lv_companies", idColumn: "regcode", nameColumn: "legal_name", activeExpr: "is_active = 1", approxCompanies: "485k", features: ["financials", "contacts", "domains"] },
  { code: "gb", name: "United Kingdom", flag: "🇬🇧", companiesTable: "gb_companies", idColumn: "company_number", nameColumn: "name", activeExpr: "is_active = 1", approxCompanies: "5.7M", features: ["financials", "industries"] },
  { code: "fr", name: "France", flag: "🇫🇷", companiesTable: "fr_companies", idColumn: "siren", nameColumn: "name", activeExpr: "is_active = 1", approxCompanies: "29.7M", features: ["industries"] },
  { code: "br", name: "Brazil", flag: "🇧🇷", companiesTable: "br_companies", idColumn: "cnpj_basico", nameColumn: "legal_name", activeExpr: "is_active = 1", approxCompanies: "68.6M", features: ["financials", "contacts", "domains"] },
  { code: "cz", name: "Czechia", flag: "🇨🇿", companiesTable: "cz_companies", idColumn: "ico", nameColumn: "name", activeExpr: "is_active = 1", approxCompanies: "3.5M", features: ["industries", "contacts", "domains"] },
  { code: "sk", name: "Slovakia", flag: "🇸🇰", companiesTable: "sk_companies", idColumn: "ico", nameColumn: "name", activeExpr: "is_active = 1", approxCompanies: "2.2M", features: ["financials", "industries"] },
];

export function getCountry(code: string): CountryConfig | undefined {
  const normalized = code.toLowerCase();
  return COUNTRIES.find((c) => c.code === normalized);
}
