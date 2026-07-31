/**
 * What we actually hold about a company, as one glyph each.
 *
 * A company list answers "who exists". It does not answer "is this row worth
 * opening" — a Swedish AB with five years of filings and a Swedish AB we know
 * nothing about beyond its name look identical. These flags put that on the
 * row: one character per kind of data, lit when we have it.
 *
 * Availability is per COUNTRY as well as per company. Czechia has no financial
 * source at all, and only four registers reach a market, so a flag is offered
 * only where the country could fill it — otherwise every row would carry a
 * permanently dark glyph that says nothing about the company and everything
 * about our coverage.
 *
 * Client-safe: no `.server` imports, so the table and legend can use it.
 */

export type CompanyFlagId = "financials" | "contacts" | "address" | "trading";

export type CompanyFlag = {
  id: CompanyFlagId;
  /** The single character shown in the cell. */
  char: string;
  label: string;
  /** What being lit actually means, for the legend's title text. */
  meaning: string;
};

/** Order is the order they render in the cell, and in the legend. */
export const COMPANY_FLAGS: CompanyFlag[] = [
  {
    id: "financials",
    char: "F",
    label: "Financials",
    meaning: "At least one filed set of accounts",
  },
  {
    id: "contacts",
    char: "C",
    label: "Contacts",
    meaning: "An email, phone or website found for the company",
  },
  {
    id: "address",
    char: "A",
    label: "Address",
    meaning: "A registered address",
  },
  {
    id: "trading",
    char: "T",
    label: "Traded",
    meaning: "Listed on a market, with an instrument we can price",
  },
];

/**
 * Where each flag is read from, per country.
 *
 * Kept as one table rather than spread through the country blocks: the point
 * of the flags is that they mean the same thing everywhere, and that is much
 * easier to check when they are written down together.
 *
 * `table` is an existence check keyed on `idColumn`. `expr` is a column on the
 * companies table that is non-empty when the fact is held — used where a
 * register keeps the address on the company row rather than beside it.
 */
export type FlagSource =
  | { table: string; idColumn: string }
  | { expr: string }
  | { market: true };

export const COMPANY_FLAG_SOURCES: Record<
  string,
  Partial<Record<CompanyFlagId, FlagSource>>
> = {
  // Norway only for now, deliberately. The other nine follow the same shapes
  // -- a companion table for Estonia's and Brazil's contacts, an `address`
  // column on the company row for Latvia, Czechia, France and Slovakia, and
  // the market flag wherever company_market_summary reaches -- but the
  // measured spread is what makes the column worth having, and that is worth
  // seeing on one country before it lands on ten.
  //
  // Measured on 1,167,141 Norwegian companies: financials 425,365 (36%),
  // contacts 479,035 (41%), address 1,165,337 (99.8%), traded 249.
  no: {
    financials: { table: "no_company_financials_latest", idColumn: "company_id" },
    // Contacts are keyed on registry_id, not company_id, in every register
    // that has them.
    contacts: { table: "no_company_contacts", idColumn: "registry_id" },
    address: { table: "no_company_addresses", idColumn: "registry_id" },
    trading: { market: true },
  },
};

/** The flags this country can fill, in canonical order. */
export function availableCompanyFlags(countryCode: string): CompanyFlag[] {
  const sources = COMPANY_FLAG_SOURCES[countryCode.toLowerCase()] ?? {};
  return COMPANY_FLAGS.filter((flag) => sources[flag.id] !== undefined);
}
