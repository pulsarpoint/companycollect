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

export type CompanyFlagId =
  "financials" | "contacts" | "domain" | "domain_suggestion" | "trading";

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
    id: "domain",
    char: "D",
    label: "Domain",
    meaning: "A website matched to the company and validated",
  },
  {
    id: "domain_suggestion",
    char: "S",
    label: "Unreviewed domains",
    meaning: "At least one associated domain still needs human review",
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
 * companies table that is non-empty when the fact is held. `idQuery` handles
 * sources whose company match spans several shared datasets and must return a
 * single `company_id` column.
 */
export type FlagSource =
  | { table: string; idColumn: string }
  | { expr: string }
  | { idQuery: string }
  | { market: true };

function swedenSectionCompanyIds(section: string): string {
  return `SELECT company_id
    FROM company_section_presence_current
    PREWHERE country_code = 'SE' AND section = '${section}'`;
}

export const COMPANY_FLAG_SOURCES: Record<
  string,
  Partial<Record<CompanyFlagId, FlagSource>>
> = {
  // A flag is offered where the country HAS the source, not where the source
  // is well filled. A dark F is true either way, and it states plainly what a
  // features list claiming "financials" does not.
  //
  // Low coverage is not one condition though, and the glyph cannot tell them
  // apart -- so, for anyone reading a country's number below:
  //
  //   Brazil   1,218 companies of 68.6M, and CORRECT. The source is CVM, the
  //            securities regulator, so only listed companies file -- and
  //            Brazil has about 1,200. It sits on 63M raw statement rows and
  //            903,767 normalised metrics, so the pipeline is complete over
  //            the population it can reach. F there marks the publicly
  //            accountable companies, which is worth seeing.
  //   Slovakia 1 row, in sk_financial_metrics AND sk_company_financials_latest.
  //            That is a stalled pipeline, not a narrow source.
  //   Estonia  74.6%, because its register publishes annual reports for every
  //            company rather than only for listed ones.
  //
  // Trading is the exception, and deliberately. It reads one shared table that
  // the markets pipeline populates for four countries only. A dark T in France
  // would not mean "no listing found for this company", it would mean we do
  // not look -- a different claim, and a misleading one.
  //
  // Coverage when last measured, as a percentage of each register:
  //        F       C       D       T
  //  no   36.45   41.04    9.70   0.021
  //  fi    4.47   25.80   25.79   0.038
  //  se   16.44      --    0.05   0.023
  //  ee   74.57   97.85   17.91     --
  //  lv   52.32    0.53    0.32     --
  //  gb    0.42      --      --     --
  //  fr    5.34      --      --     --
  //  br    0.00   83.58    1.24   0.0005
  //  cz      --    0.19    0.13     --
  //  sk    0.00      --      --     --
  no: {
    financials: {
      table: "no_company_financials_latest",
      idColumn: "company_id",
    },
    // Contacts and domains are keyed on registry_id, not company_id, in every
    // register that has them.
    contacts: { table: "no_company_contacts", idColumn: "registry_id" },
    domain: { table: "no_company_domains", idColumn: "registry_id" },
    trading: { market: true },
  },
  fi: {
    financials: {
      table: "fi_company_financials_latest",
      idColumn: "company_id",
    },
    contacts: { table: "fi_company_contacts", idColumn: "registry_id" },
    domain: { table: "fi_company_domains", idColumn: "registry_id" },
    trading: { market: true },
  },
  se: {
    financials: { idQuery: swedenSectionCompanyIds("financials") },
    contacts: { idQuery: swedenSectionCompanyIds("domains") },
    domain: {
      idQuery: `SELECT company_id
        FROM company_domains FINAL
        WHERE country_code = 'SE'
          AND is_active = 1
          AND review_status != 'rejected'`,
    },
    domain_suggestion: {
      idQuery: `SELECT company_id
        FROM company_domains FINAL
        WHERE country_code = 'SE'
          AND is_active = 1
          AND review_status = 'unreviewed'`,
    },
    trading: { market: true },
  },
  ee: {
    financials: {
      table: "ee_company_financials_latest",
      idColumn: "company_id",
    },
    contacts: { table: "ee_company_contacts", idColumn: "registry_id" },
    domain: { table: "ee_company_domains", idColumn: "registry_id" },
  },
  lv: {
    financials: {
      table: "lv_company_financials_latest",
      idColumn: "company_id",
    },
    contacts: { table: "lv_company_contacts", idColumn: "registry_id" },
    domain: { table: "lv_company_domains", idColumn: "registry_id" },
  },
  gb: {
    financials: {
      table: "gb_company_financials_latest",
      idColumn: "company_id",
    },
  },
  fr: {
    financials: {
      table: "fr_company_financials_latest",
      idColumn: "company_id",
    },
  },
  br: {
    financials: {
      table: "br_company_financials_latest",
      idColumn: "company_id",
    },
    contacts: { table: "br_company_contacts", idColumn: "registry_id" },
    domain: { table: "br_company_domains", idColumn: "registry_id" },
    trading: { market: true },
  },
  cz: {
    contacts: { table: "cz_company_contacts", idColumn: "registry_id" },
    domain: { table: "cz_company_domains", idColumn: "registry_id" },
  },
  sk: {
    financials: {
      table: "sk_company_financials_latest",
      idColumn: "company_id",
    },
  },
};

/** The flags this country can fill, in canonical order. */
export function availableCompanyFlags(countryCode: string): CompanyFlag[] {
  const sources = COMPANY_FLAG_SOURCES[countryCode.toLowerCase()] ?? {};
  return COMPANY_FLAGS.filter((flag) => sources[flag.id] !== undefined);
}

/**
 * The filter key a flag uses in the URL, e.g. `f_flag_financials=yes`.
 *
 * Separate from the facet keys, which drive a searchable combobox over a
 * column's distinct values. A flag has exactly two states, so it gets a pair
 * of toggles instead -- and "no" is as useful as "yes" here: finding the
 * Norwegian companies we hold NO financials for is how a coverage gap gets
 * noticed.
 */
export function flagFilterKey(id: CompanyFlagId): string {
  return `flag_${id}`;
}

export const FLAG_FILTER_VALUES = ["yes", "no"] as const;
export type FlagFilterValue = (typeof FLAG_FILTER_VALUES)[number];

/** Filter keys this country offers, one per flag it can fill. */
export function flagFilterKeys(countryCode: string): string[] {
  return availableCompanyFlags(countryCode).map((flag) =>
    flagFilterKey(flag.id),
  );
}
