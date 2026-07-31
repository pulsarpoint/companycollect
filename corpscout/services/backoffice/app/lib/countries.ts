export type CountryFeature = "financials" | "industries" | "contacts" | "domains";

export type ColumnKind = "id" | "text" | "date" | "status";
export type SortDir = "asc" | "desc";

export const PAGE_SIZES = [25, 50, 100] as const;

export interface CompanyColumn {
  /** Stable key: row field name, ?sort= value, and SQL alias. [a-z_]+ only. */
  key: string;
  /** Column header text. */
  label: string;
  /** SQL select expression. Registry-only — never user input. */
  expr: string;
  /** Sortable columns become ORDER BY candidates. Industry is never sortable. */
  sortable: boolean;
  /** Rendering hint for the UI. */
  kind: ColumnKind;
  /** Filterable columns become facet candidates (categorical values only). */
  filterable?: boolean;
}

export interface CountryDetailConfig {
  /**
   * The company page exposes a dedicated Financials tab with source report
   * documents and extracted facts. The route owns the source-specific query;
   * this flag is the navigation capability, not a source identifier.
   */
  financialReports?: boolean;
  /** {id:String} → canonical financial rows (see FinancialYearRow in queries.server). */
  financialsQuery?: string;
  /** {id:String} → { contact_type, contact_value } rows. */
  contactsQuery?: string;
  /** {id:String} → { domain, website_url, domain_source, confidence, is_primary } rows. */
  domainsQuery?: string;
  /**
   * {id:String} → FULL country-shaped statement rows (SELECT *), newest
   * first, ALL filings (no per-year dedup). Rendered by a country-specific
   * component when one exists, else by the auto field-grid fallback.
   */
  statementsQuery?: string;
  /**
   * Optional override for the detail record fetch: {id:String} → ONE row.
   * MUST select the base table's full row (c.*) — used to join translated
   * columns without dropping base-only fields (fidelity rule).
   */
  recordQuery?: string;
  /** {id:String} → all industry rows: industry_code, description_original, industry_label (canonical NACE English), is_primary. */
  industriesQuery?: string;
  /** {id:String} → address rows: address_type, full_address (clean comma-joined). */
  addressQuery?: string;
  /**
   * {id:String} → public tax records (see TaxRecordRow in queries.server),
   * one row per tax year, newest first. Tax-base figures (taxable income,
   * assessed taxes) — NOT financial statements; rendered as a separate
   * section, never merged into Financials.
   */
  taxRecordsQuery?: string;
  /**
   * {id:String} → public procurement contract wins (see PublicContractRow in
   * queries.server), newest first, CANONICAL shape across countries:
   * source ('hilma' | 'ted' | …), notice_ref, contract_date, buyer_name,
   * title, amount_original/amount_usd/currency. Each country's SQL unions
   * whatever procurement sources it has into this shape; the generic
   * PublicContractsSection renders it for every country identically.
   */
  publicContractsQuery?: string;
  /**
   * {id:String} + {year:UInt16} → raw source facts for ONE fiscal year's
   * filing (see FactRow in queries.server), in source document order.
   * Presence of this query makes the Financials year column link to
   * /company/:country/:id/facts/:year.
   */
  factsQuery?: string;
  /**
   * {id:String} → registered alternative names: name, name_kind
   * ("secondary" scoped trade names / "foreign" foreign-language names),
   * registered (date string), scope (original-language clause, may be '').
   */
  secondaryNamesQuery?: string;
  /**
   * {id:String} → officer/signatory rows for the latest fiscal year that
   * has people: first_name, last_name, role_original, role_kind,
   * signatory_kind, fiscal_year. Board and certification signatures for
   * the same person are pre-merged per signatory_kind server-side; the UI
   * does a final merge across signatory kinds (see ManagementSection).
   */
  officersQuery?: string;
  /**
   * {names:Array(String)} + {id:String} → same-name matches for the
   * displayed officers, from the cross-country people table, excluding the
   * current company: full_name_normalized, country_iso2, company_id,
   * company_name, role_kind, fiscal_year. Names are lowercase trimmed
   * "first last" pairs matching full_name_normalized. Run once per page
   * load (batched), after officersQuery resolves.
   */
  peopleMatchesQuery?: string;
  /**
   * {id:String} → ONE latest-filing audit row: audit_firm, opinion_kind
   * ('standard' | 'modified' | 'unknown'), opinion_date, fiscal_year.
   * Rendered inside the Management card's auditor block.
   */
  auditQuery?: string;
  /**
   * {id:String} + {year:UInt16} → ONE row locating the original source
   * document behind that fiscal year's facts: object_key + source_uri
   * (s3://bucket/key in the corpscout object store, proxied to the browser)
   * and archive_url/archive_name/nested_zip_name (public upstream archive).
   */
  factsDocumentQuery?: string;
  /**
   * {id:String} → GLEIF corporate-group links for the company's LEI (see
   * GleifRelationshipRow in queries.server): direction ('parent' |
   * 'subsidiary'), relationship_type (GLEIF vocabulary, e.g.
   * IS_DIRECTLY_CONSOLIDATED_BY), other_lei, name, jurisdiction, and
   * local_id — the other entity's registry number when it belongs to THIS
   * country (enables an internal company link), else ''.
   */
  gleifRelationshipsQuery?: string;
  /**
   * {id:String} → ONE GLEIF entity row for the company's LEI (see
   * GleifEntityRow in queries.server): lei, lei_status (ISSUED | LAPSED |
   * RETIRED | …), category (GENERAL | FUND | SOLE_PROPRIETOR | …),
   * hq_country, hq_abroad (0/1), ownership_exceptions (comma-joined GLEIF
   * no-parent reasons, e.g. NATURAL_PERSONS). Empty result when the
   * company has no LEI.
   */
  gleifEntityQuery?: string;
  /**
   * {id:String} → ONE Wikidata company row (see WikidataCompanyRow in
   * queries.server), matched via the company's LEI. Description, official
   * name, inception, employees (+ as-of date), industry/legal-form labels,
   * HQ, logo URL, current stock listings ("Exchange: TICKER | …"),
   * websites, LinkedIn id, and the wikidata.org URL. Empty when unmatched.
   */
  wikidataQuery?: string;
  /**
   * {id:String} → Wikidata-linked PEOPLE for the matched company (see
   * WikidataPersonRow in queries.server): company-anchored role links
   * (founder / CEO / chairperson / board member / owner) joined with each
   * person's item data (name, description, birth year, image, wikidata
   * URL). Current roles first. Empty when the company is unmatched or has
   * no person links.
   */
  wikidataPeopleQuery?: string;
}

export interface CountryConfig {
  /** Lowercase ISO2 code, used as the URL segment /:country. */
  code: string;
  /** Uppercase ISO3 code used by IMF and UN Comtrade. */
  iso3: string;
  /** Eurostat geography code when the country is covered by Eurostat. */
  eurostatGeoCode?: string;
  name: string;
  flag: string;
  /** ClickHouse table holding the canonical company rows. */
  companiesTable: string;
  /** Column holding the national registry identifier. */
  idColumn: string;
  /** Column holding the display name. */
  nameColumn: string;
  /**
   * Optional SQL expression the list search matches against instead of
   * nameColumn — for countries whose registered names use formal legal-form
   * words users don't type (SE registers "Aktiebolag" where everyone
   * writes "AB"). The expression should contain the display name plus any
   * normalized variants, e.g. concat(name, ' ', abbreviated-name).
   */
  searchColumnExpr?: string;
  /** SQL boolean expression selecting active companies. */
  activeExpr: string;
  /** Human-readable approximate row count, shown on the picker card. */
  approxCompanies: string;
  /** Which auxiliary data families exist for this country. */
  features: CountryFeature[];
  /** Visible list columns, in display order. Must include keys "id" and "name". */
  columns: CompanyColumn[];
  /**
   * SQL returning one industry row per visible company:
   * SELECT ... AS company_id, ... AS industry_code, ... AS industry_label
   * with the page's join-key values bound as {ids:Array(String)}.
   */
  industryQuery?: string;
  /** Company-table expression producing the industry join key. Defaults to idColumn. */
  industryJoinKeyExpr?: string;
  /** Facet options SQL: value, label (canonical NACE English), cnt. No params. */
  industryFacetQuery?: string;
  /** Boolean WHERE expr filtering companies by industry; binds {f_industry:Array(String)}. */
  industryFilterExpr?: string;
  /** Company-detail page queries (financials/contacts/domains), where the data exists. */
  detail?: CountryDetailConfig;
  /** Latest-financials summary table (one row per company). companyKeyExpr is the expression on companiesTable matching summary.company_id. */
  financialsLatest?: { table: string; companyKeyExpr: string };
  /** NACE join + sum-exclusion rules for the financial aggregates layer. */
  financialsAggregates?: CountryFinancialsAggregates;
  /**
   * Per-FISCAL-YEAR financials, for the overview's year selector.
   *
   * financialsLatest holds one row per company — its most recent filing — so it
   * cannot answer "what did 2022 look like". The metrics table keeps every
   * year. Only declared where a country has one with revenue in USD; Norway's
   * exists but carries no revenue_amount_usd, so it is deliberately absent and
   * the year selector simply does not reach that country's revenue cards.
   */
  financialsByYear?: { table: string; idColumn: string };
}

export type CountryFinancialsAggregates = {
  /** Primary-NACE join for the summary table; omit when no usable mapping. */
  nace?: {
    industriesTable: string;
    /** Expression on industriesTable yielding summary.company_id values. */
    companyKeyExpr: string;
    /** Expression yielding normalized NACE digits (class level). */
    naceCodeExpr: string;
    /** WHERE conjunct scoping to usable primary rows. */
    filterExpr: string;
  };
  /** Conjunct on summary alias `f` excluding rows from SUMS (lists keep them). */
  sumExclusionExpr?: string;
};

export const COUNTRIES: CountryConfig[] = [
  {
    code: "no", iso3: "NOR", eurostatGeoCode: "NO", name: "Norway", flag: "🇳🇴", companiesTable: "no_companies",
    idColumn: "org_number", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "1.2M", features: ["financials", "industries", "contacts", "domains"],
    columns: [
      { key: "id", label: "Org number", expr: "org_number", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      // The CODE, so the shared decoder names it — English once translated,
      // the register's own term until then. Reading
      // legal_form_description_original directly showed Norwegian only, and
      // left the filter listing values no decoder could label.
      { key: "legal_form", label: "Legal form", expr: "legal_form_code", sortable: true, kind: "text", filterable: true },
      { key: "status", label: "Status", expr: "lifecycle_status", sortable: true, kind: "status", filterable: true },
      { key: "registered", label: "Registered", expr: "toString(registration_date)", sortable: true, kind: "date" },
      { key: "website", label: "Website", expr: "primary_website_host", sortable: false, kind: "text" },
    ],
    industryQuery: `SELECT i.org_number AS company_id,
  i.nace_normalized_code AS industry_code,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label
FROM no_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = substring(i.nace_normalized_code, 1, 4) AND n.is_current = 1
WHERE i.org_number IN {ids:Array(String)}
ORDER BY i.is_primary DESC
LIMIT 1 BY i.org_number`,
    industryFacetQuery: `SELECT i.nace_normalized_code AS value,
  coalesce(nullIf(any(n.description_en), ''), any(coalesce(i.description_en, i.description_original)), value) AS label,
  count() AS cnt
FROM no_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = substring(i.nace_normalized_code, 1, 4) AND n.is_current = 1
WHERE i.is_primary = 1 AND i.nace_normalized_code != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
    industryFilterExpr: `org_number IN (SELECT org_number FROM no_industries WHERE is_primary = 1 AND nace_normalized_code IN {f_industry:Array(String)})`,
    detail: {
      financialReports: true,
      financialsQuery: `SELECT toString(fiscal_year) AS fiscal_year, currency AS currency,
  toFloat64(operating_revenue_amount_original) AS revenue_amount_original,
  toFloat64(operating_revenue_amount_usd) AS revenue_amount_usd,
  toFloat64(net_result_amount_original) AS net_result_amount_original,
  toFloat64(net_result_amount_usd) AS net_result_amount_usd,
  toFloat64(total_assets_amount_original) AS total_assets_amount_original,
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
  toFloat64(equity_amount_original) AS equity_amount_original,
  toFloat64(equity_amount_usd) AS equity_amount_usd,
  NULL AS employees
FROM no_financial_statements
WHERE org_number = {id:String}
ORDER BY fiscal_year DESC, resolved_at DESC
LIMIT 1 BY fiscal_year
LIMIT 20`,
      contactsQuery: `SELECT contact_type AS contact_type, contact_value AS contact_value
FROM no_company_contacts
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY contact_type, contact_value
LIMIT 100`,
      domainsQuery: `SELECT domain AS domain, website_url AS website_url, domain_source AS domain_source,
  toFloat64(confidence) AS confidence, is_primary AS is_primary
FROM no_company_domains
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY is_primary DESC, confidence DESC
LIMIT 50`,
      statementsQuery: `SELECT * FROM no_financial_statements
WHERE org_number = {id:String}
ORDER BY fiscal_year DESC, resolved_at DESC
LIMIT 40`,
      recordQuery: `SELECT c.*, t.articles_purpose_en, t.activity_text_en, t.legal_form_description_en
FROM no_companies AS c
LEFT JOIN no_companies_translated AS t ON t.org_number = c.org_number
WHERE c.org_number = {id:String}
LIMIT 1`,
      industriesQuery: `SELECT i.nace_normalized_code AS industry_code,
  coalesce(i.description_original, '') AS description_original,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label,
  i.is_primary AS is_primary
FROM no_industries AS i
LEFT JOIN nace_categories AS n ON n.normalized_code = substring(i.nace_normalized_code, 1, 4) AND n.is_current = 1
WHERE i.org_number = {id:String}
ORDER BY i.is_primary DESC, industry_code
LIMIT 100`,
      addressQuery: `SELECT address_type AS address_type,
  arrayStringConcat(arrayFilter(x -> x != '', [
    coalesce(address_lines, ''),
    trim(concat(coalesce(postal_code, ''), ' ', coalesce(city, ''))),
    coalesce(country, '')
  ]), ', ') AS full_address
FROM no_company_addresses
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY address_type
LIMIT 10`,
    },
    financialsLatest: { table: "no_company_financials_latest", companyKeyExpr: "org_number" },
    financialsAggregates: {
      nace: {
        industriesTable: "no_industries",
        companyKeyExpr: "toString(org_number)",
        naceCodeExpr: "nace_normalized_code",
        filterExpr: "is_primary = 1 AND nace_normalized_code != ''",
      },
      // NUF branches file the foreign parent's full accounts (AWS EMEA €19.4bn);
      // real data, not Norway-earned — excluded from sums, kept in lists.
      sumExclusionExpr: "f.company_id NOT IN (SELECT toString(org_number) FROM no_companies WHERE legal_form_code = 'NUF')",
    },
  },
  {
    code: "fi", iso3: "FIN", eurostatGeoCode: "FI", name: "Finland", flag: "🇫🇮", companiesTable: "fi_companies",
    idColumn: "business_id", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "460k", features: ["financials", "industries", "contacts", "domains"],
    columns: [
      { key: "id", label: "Business ID", expr: "business_id", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "coalesce(legal_form_description_en, legal_form_description_original, legal_form_code)", sortable: true, kind: "text", filterable: true }, // populated from YTJ companyForms since 2026-07-17
      { key: "status", label: "Status", expr: "lifecycle_status", sortable: true, kind: "status", filterable: true },
      { key: "registered", label: "Registered", expr: "toString(registration_date)", sortable: true, kind: "date" },
      { key: "website", label: "Website", expr: "primary_website_url", sortable: false, kind: "text" },
    ],
    industryQuery: `SELECT i.business_id AS company_id,
  coalesce(i.source_industry_code, '') AS industry_code,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.source_industry_code, '') AS industry_label
FROM fi_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = substring(coalesce(i.source_industry_code, ''), 1, 4) AND n.is_current = 1
WHERE i.business_id IN {ids:Array(String)}
ORDER BY i.is_primary DESC
LIMIT 1 BY i.business_id`,
    industryFacetQuery: `SELECT coalesce(i.source_industry_code, '') AS value,
  coalesce(nullIf(any(n.description_en), ''), any(coalesce(i.description_en, i.description_original)), value) AS label,
  count() AS cnt
FROM fi_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = substring(coalesce(i.source_industry_code, ''), 1, 4) AND n.is_current = 1
WHERE i.is_primary = 1 AND coalesce(i.source_industry_code, '') != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
    industryFilterExpr: `business_id IN (SELECT business_id FROM fi_industries WHERE is_primary = 1 AND coalesce(source_industry_code, '') IN {f_industry:Array(String)})`,
    detail: {
      financialsQuery: `SELECT toString(toYear(period_end)) AS fiscal_year, currency_original AS currency,
  toFloat64(revenue_amount_original) AS revenue_amount_original,
  toFloat64(revenue_amount_usd) AS revenue_amount_usd,
  toFloat64(profit_loss_amount_original) AS net_result_amount_original,
  toFloat64(profit_loss_amount_usd) AS net_result_amount_usd,
  toFloat64(total_assets_amount_original) AS total_assets_amount_original,
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
  toFloat64(equity_amount_original) AS equity_amount_original,
  toFloat64(equity_amount_usd) AS equity_amount_usd,
  toFloat64(employees) AS employees
FROM fi_financial_metrics
WHERE business_id = {id:String}
ORDER BY fiscal_year DESC, resolved_at DESC
LIMIT 1 BY fiscal_year
LIMIT 20`,
      contactsQuery: `SELECT contact_type AS contact_type, contact_value AS contact_value
FROM fi_company_contacts
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY contact_type, contact_value
LIMIT 100`,
      domainsQuery: `SELECT domain AS domain, website_url AS website_url, domain_source AS domain_source,
  toFloat64(confidence) AS confidence, is_primary AS is_primary
FROM fi_company_domains
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY is_primary DESC, confidence DESC
LIMIT 50`,
      industriesQuery: `SELECT coalesce(i.source_industry_code, '') AS industry_code,
  coalesce(i.description_original, '') AS description_original,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.source_industry_code, '') AS industry_label,
  i.is_primary AS is_primary
FROM fi_industries AS i
LEFT JOIN nace_categories AS n ON n.normalized_code = substring(coalesce(i.source_industry_code, ''), 1, 4) AND n.is_current = 1
WHERE i.business_id = {id:String}
ORDER BY i.is_primary DESC, industry_code
LIMIT 100`,
      addressQuery: `SELECT address_type AS address_type,
  arrayStringConcat(arrayFilter(x -> x != '', [
    coalesce(address_lines, ''),
    trim(concat(coalesce(postal_code, ''), ' ', coalesce(city, ''))),
    coalesce(country, country_code, '')
  ]), ', ') AS full_address
FROM fi_company_addresses
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY address_type
LIMIT 10`,
      taxRecordsQuery: `SELECT toString(tax_year) AS tax_year, currency AS currency,
  municipality_name AS municipality_name,
  toFloat64(taxable_income_amount_original) AS taxable_income_amount_original,
  toFloat64(taxable_income_amount_usd) AS taxable_income_amount_usd,
  toFloat64(taxes_total_amount_original) AS taxes_total_amount_original,
  toFloat64(taxes_total_amount_usd) AS taxes_total_amount_usd,
  toFloat64(prepayments_total_amount_original) AS prepayments_total_amount_original,
  toFloat64(prepayments_total_amount_usd) AS prepayments_total_amount_usd,
  toFloat64(tax_refund_amount_original) AS tax_refund_amount_original,
  toFloat64(tax_refund_amount_usd) AS tax_refund_amount_usd,
  toFloat64(residual_tax_amount_original) AS residual_tax_amount_original,
  toFloat64(residual_tax_amount_usd) AS residual_tax_amount_usd
FROM fi_tax_records
WHERE business_id = {id:String}
ORDER BY tax_year DESC
LIMIT 1 BY tax_year
LIMIT 20`,
      publicContractsQuery: `SELECT
  source_slug AS source,
  concat(source_notice_id, if(source_lot_id = '', '', concat(':', source_lot_id))) AS notice_ref,
  coalesce(toString(publication_date), '') AS contract_date,
  buyer_name,
  title,
  toFloat64(value_amount_original) AS amount_original,
  toFloat64(value_amount_usd) AS amount_usd,
  value_currency AS currency,
  toFloat64(notice_value_amount_original) AS notice_amount_original,
  toFloat64(notice_value_amount_usd) AS notice_amount_usd,
  notice_value_currency AS notice_currency,
  source_url
FROM fi_government_contracts
WHERE company_id = {id:String}
ORDER BY publication_date DESC NULLS LAST, contract_id
LIMIT 100`,
    },
    financialsLatest: { table: "fi_company_financials_latest", companyKeyExpr: "business_id" },
  },
  {
    code: "se", iso3: "SWE", eurostatGeoCode: "SE", name: "Sweden", flag: "🇸🇪", companiesTable: "se_companies",
    idColumn: "registration_number", nameColumn: "legal_name", activeExpr: "status = 'active'",
    // Bolagsverket registers formal legal-form words; users type the
    // abbreviations ("Investor AB" vs registered "Investor Aktiebolag").
    searchColumnExpr:
      "concat(coalesce(legal_name, ''), ' ', replaceRegexpAll(replaceRegexpAll(coalesce(legal_name, ''), '(?i)aktiebolag', 'AB'), '(?i)handelsbolag', 'HB'))",
    approxCompanies: "3.4M", features: ["financials", "industries"],
    industryJoinKeyExpr: "company_id",
    columns: [
      { key: "id", label: "Reg. number", expr: "registration_number", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "legal_name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "legal_form_code", sortable: true, kind: "text", filterable: true },
      { key: "status", label: "Status", expr: "status", sortable: true, kind: "status", filterable: true },
      { key: "registered", label: "Incorporated", expr: "toString(incorporation_date)", sortable: true, kind: "date" },
    ],
    industryQuery: `SELECT i.company_id AS company_id,
  i.nace_rev2_class_code AS industry_code,
  coalesce(nullIf(n.description_en, ''), i.nace_rev2_class_code) AS industry_label
FROM se_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_rev2_class_code AND n.is_current = 1
WHERE i.company_id IN {ids:Array(String)}
ORDER BY i.is_primary DESC, i.sequence ASC
LIMIT 1 BY i.company_id`,
    industryFacetQuery: `SELECT i.nace_rev2_class_code AS value,
  coalesce(nullIf(any(n.description_en), ''), value) AS label,
  count() AS cnt
FROM se_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_rev2_class_code AND n.is_current = 1
WHERE i.is_primary = 1 AND i.nace_rev2_class_code != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
    industryFilterExpr: `company_id IN (SELECT company_id FROM se_industries WHERE is_primary = 1 AND nace_rev2_class_code IN {f_industry:Array(String)})`,
    detail: {
      // activity_description is aliased to _original so the language
      // toggle's pairing rule (needs BOTH <base>_en and <base>_original)
      // collapses it with activity_description_en from the translated
      // view; the code-label columns are deliberately unpaired singles.
      // vat_number is DERIVED (SE + orgnr + 01, the standard momsreg.nr
      // format) — the source carries no VAT registration flag, so this is
      // the number IF the company is VAT-registered, not proof that it is.
      recordQuery: `SELECT c.* EXCEPT (activity_description),
  c.activity_description AS activity_description_original,
  t.activity_description_en AS activity_description_en,
  t.legal_form_label_en AS legal_form_label_en,
  t.status_reason_label_en AS status_reason_label_en,
  g.lei AS lei,
  concat('SE', c.registration_number, '01') AS vat_number
FROM se_companies AS c
LEFT JOIN se_companies_translated AS t ON t.company_id = c.company_id
LEFT JOIN (
  SELECT replaceRegexpAll(registered_as, '[^0-9]', '') AS orgnr,
    argMax(lei, (registration_status = 'ISSUED', entity_status = 'ACTIVE')) AS lei
  FROM gleif_lei_records
  WHERE jurisdiction = 'SE' AND registered_as != ''
  GROUP BY orgnr
) AS g ON g.orgnr = c.registration_number
WHERE c.registration_number = {id:String}
LIMIT 1`,
      // se_financial_metrics is keyed on the normalized 10-digit orgnr
      // (= registration_number since the 2026-07-18 identity fix). Some
      // companies carry duplicate per-year rows where one is all-NULL —
      // the isNull tiebreak prefers the complete row (same rule as the
      // company_financials_latest summary build).
      // Filed years come from metrics (full column set). Years the company
      // never filed digitally are backfilled from the flerårsöversikt
      // comparatives in se_financial_history — revenue and total assets
      // only: result_after_financial_items is NOT net result (91% concept
      // agreement measured), and solidity's scale is ambiguous, so neither
      // is surfaced in these shared columns.
      financialsQuery: `SELECT * FROM (
  SELECT toString(fiscal_year) AS fiscal_year, currency AS currency,
    toFloat64(revenue_amount_original) AS revenue_amount_original,
    toFloat64(revenue_amount_usd) AS revenue_amount_usd,
    toFloat64(profit_loss_amount_original) AS net_result_amount_original,
    toFloat64(profit_loss_amount_usd) AS net_result_amount_usd,
    toFloat64(total_assets_amount_original) AS total_assets_amount_original,
    toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
    toFloat64(equity_amount_original) AS equity_amount_original,
    toFloat64(equity_amount_usd) AS equity_amount_usd,
    toFloat64(employees) AS employees,
    'filed' AS observation,
    '' AS source_fiscal_year
  FROM se_financial_metrics
  WHERE company_id = {id:String}
  ORDER BY fiscal_year DESC, isNull(revenue_amount_original) ASC, source_record_id DESC
  LIMIT 1 BY fiscal_year
  UNION ALL
  SELECT toString(fiscal_year), currency,
    toFloat64(revenue_amount_original),
    toFloat64(revenue_amount_usd),
    NULL, NULL,
    toFloat64(total_assets_amount_original),
    toFloat64(total_assets_amount_usd),
    NULL, NULL,
    NULL,
    'comparative',
    toString(source_fiscal_year)
  FROM se_financial_history
  WHERE company_id = {id:String} AND observation = 'comparative'
    AND fiscal_year NOT IN (
      SELECT fiscal_year FROM se_financial_metrics WHERE company_id = {id:String}
    )
)
ORDER BY fiscal_year DESC
LIMIT 25`,
      // Same filing the metrics row shows (metrics statement_key first);
      // falls back to the newest filing seen in facts for years whose
      // metrics row hasn't been rebuilt yet.
      factsQuery: `SELECT f.concept_local_name AS concept,
  l.label_en AS concept_label_en,
  f.value_kind AS value_kind, f.raw_value AS raw_value,
  toFloat64(f.amount_original) AS amount_original,
  toFloat64(f.amount_usd) AS amount_usd,
  f.currency AS currency,
  toString(f.date_value) AS date_value,
  f.text_value AS text_value,
  f.dimensions AS dimensions,
  f.context_id AS context_id
FROM se_financial_facts AS f
LEFT JOIN se_financial_concept_labels AS l
  ON l.concept_local_name = f.concept_local_name
WHERE f.company_id = {id:String} AND f.statement_key IN (
  SELECT statement_key FROM (
    SELECT statement_key, 0 AS pri
    FROM se_financial_metrics
    WHERE company_id = {id:String} AND fiscal_year = {year:UInt16}
    ORDER BY isNull(revenue_amount_original) ASC, source_record_id DESC
    LIMIT 1
    UNION ALL
    SELECT argMax(statement_key, resolved_at) AS statement_key, 1 AS pri
    FROM se_financial_facts
    WHERE company_id = {id:String} AND toYear(report_period_end) = {year:UInt16}
    HAVING count() > 0
  )
  ORDER BY pri
  LIMIT 1
)
ORDER BY fact_ordinal
LIMIT 3000`,
      // Bolagsverket packs all registered names into legal_name_raw as
      // Name$TYPE$Date[$Scope] entries joined by '|'. SARS_FORNAMN are
      // scoped secondary trade names, FORNAMN_FRSPRAK foreign-language
      // names. Parsed at query time; a handful of malformed source entries
      // fail the type filter and drop out harmlessly.
      secondaryNamesQuery: `SELECT entry[1] AS name,
  if(entry[2] = 'FORNAMN_FRSPRAK-ORGNAM', 'foreign', 'secondary') AS name_kind,
  entry[3] AS registered,
  entry[4] AS scope
FROM (
  SELECT splitByChar('\$', arrayJoin(splitByChar('|', assumeNotNull(legal_name_raw)))) AS entry
  FROM se_companies
  WHERE registration_number = {id:String} AND legal_name_raw IS NOT NULL
)
WHERE entry[2] IN ('SARS_FORNAMN-ORGNAM', 'FORNAMN_FRSPRAK-ORGNAM')
ORDER BY registered, name
LIMIT 200`,
      // Latest fiscal year's signatories. Note: the two argMax()/max()
      // aggregates below can't be aliased to their own input column name
      // (e.g. `argMax(role_kind, ...) AS role_kind`) — ClickHouse's alias
      // resolution treats that as a self-referential expansion and throws
      // ILLEGAL_AGGREGATION, so the inner subquery uses `_out` aliases and
      // the outer SELECT renames them back to the plan's field names.
      officersQuery: `SELECT first_name, last_name, role_original, role_kind_out AS role_kind, signatory_kind, fiscal_year
FROM (
  SELECT first_name, last_name,
    argMax(role_original, role_kind != 'unknown') AS role_original,
    argMax(role_kind, role_kind != 'unknown') AS role_kind_out,
    signatory_kind, fiscal_year
  FROM se_company_officers
  WHERE company_id = {id:String}
    AND fiscal_year = (SELECT max(fiscal_year) FROM se_company_officers WHERE company_id = {id:String})
  GROUP BY first_name, last_name, signatory_kind, fiscal_year
)
ORDER BY signatory_kind = 'auditor', role_kind != 'chairman', role_kind != 'ceo', last_name
LIMIT 100`,
      // Same-name matches across all companies for the officers shown above
      // (batched: {names} carries every displayed person in one round trip).
      // Same self-alias pitfall as officersQuery — max(fiscal_year) can't be
      // AS fiscal_year — hence the inner _out alias.
      peopleMatchesQuery: `SELECT full_name_normalized, country_iso2, company_id, company_name, role_kind, fiscal_year_out AS fiscal_year
FROM (
  SELECT full_name_normalized, country_iso2, company_id,
    any(company_name) AS company_name,
    argMax(role_kind, fiscal_year) AS role_kind,
    max(fiscal_year) AS fiscal_year_out
  FROM company_people_all
  WHERE full_name_normalized IN {names:Array(String)}
    AND NOT (country_iso2 = 'SE' AND company_id = {id:String})
  GROUP BY full_name_normalized, country_iso2, company_id
)
ORDER BY full_name_normalized, fiscal_year DESC
LIMIT 10 BY full_name_normalized
LIMIT 200`,
      // LIMIT BY caps matches per name at 10 so common surnames don't starve others
      // A filing year can yield two audit rows (annual report + separate
      // audit-report document) — prefer the one carrying an opinion and date.
      auditQuery: `SELECT audit_firm AS audit_firm, opinion_kind AS opinion_kind,
  coalesce(toString(opinion_date), '') AS opinion_date, fiscal_year AS fiscal_year
FROM se_company_audits
WHERE company_id = {id:String}
ORDER BY fiscal_year DESC, opinion_kind = 'unknown' ASC, isNull(opinion_date) ASC, statement_key DESC
LIMIT 1`,
      factsDocumentQuery: `SELECT xhtml_object_key AS object_key,
  xhtml_source_uri AS source_uri,
  source_archive_url AS archive_url,
  source_archive_name AS archive_name,
  nested_zip_name AS nested_zip_name
FROM se_financial_metrics
WHERE company_id = {id:String} AND fiscal_year = {year:UInt16}
ORDER BY isNull(revenue_amount_original) ASC, source_record_id DESC
LIMIT 1`,
      // GLEIF consolidation links both ways: rows where our LEI is the
      // START node are our parents (X IS_CONSOLIDATED_BY parent), rows
      // where it is the END node are subsidiaries. local_id carries the
      // digits-only orgnr for SE-jurisdiction relatives so the UI can link
      // internally; ACTIVE relationship rows only.
      gleifRelationshipsQuery: `WITH (
  SELECT coalesce(argMax(lei, (registration_status = 'ISSUED', entity_status = 'ACTIVE')), '')
  FROM gleif_lei_records
  WHERE jurisdiction = 'SE'
    AND replaceRegexpAll(registered_as, '[^0-9]', '') = {id:String}
) AS my_lei
SELECT
  if(r.start_node_lei = my_lei, 'parent', 'subsidiary') AS direction,
  r.relationship_type AS relationship_type,
  if(r.start_node_lei = my_lei, r.end_node_lei, r.start_node_lei) AS other_lei,
  any(coalesce(g.legal_name, '')) AS name,
  any(coalesce(g.jurisdiction, '')) AS jurisdiction,
  any(if(g.jurisdiction = 'SE',
      replaceRegexpAll(coalesce(g.registered_as, ''), '[^0-9]', ''), '')) AS local_id
FROM gleif_lei_relationships AS r
LEFT JOIN gleif_lei_records AS g
  ON g.lei = if(r.start_node_lei = my_lei, r.end_node_lei, r.start_node_lei)
WHERE my_lei != ''
  AND (r.start_node_lei = my_lei OR r.end_node_lei = my_lei)
  AND r.relationship_status = 'ACTIVE'
GROUP BY direction, relationship_type, other_lei
ORDER BY direction = 'subsidiary', relationship_type, name
LIMIT 500`,
      gleifEntityQuery: `SELECT r.lei AS lei,
  r.registration_status AS lei_status,
  coalesce(r.category, '') AS category,
  coalesce(hq.country, '') AS hq_country,
  toUInt8(coalesce(hq.country, 'SE') != 'SE') AS hq_abroad,
  coalesce(exc.reasons, '') AS ownership_exceptions
FROM gleif_lei_records AS r
LEFT JOIN (
  SELECT lei, any(country) AS country
  FROM gleif_lei_addresses
  WHERE address_role = 'HEADQUARTERS_ADDRESS'
  GROUP BY lei
) AS hq ON hq.lei = r.lei
LEFT JOIN (
  SELECT lei, arrayStringConcat(arraySort(groupUniqArray(exception_reason)), ',') AS reasons
  FROM gleif_lei_reporting_exceptions
  GROUP BY lei
) AS exc ON exc.lei = r.lei
WHERE r.lei = (
  SELECT coalesce(argMax(lei, (registration_status = 'ISSUED', entity_status = 'ACTIVE')), '')
  FROM gleif_lei_records
  WHERE jurisdiction = 'SE'
    AND replaceRegexpAll(registered_as, '[^0-9]', '') = {id:String}
)
LIMIT 1`,
      // Wikidata enrichment: matched on the Swedish org number (P6460,
      // extracted at wikidata ingest since 2026-07-20) with LEI fallback.
      wikidataQuery: `WITH (
  SELECT coalesce(argMax(lei, (registration_status = 'ISSUED', entity_status = 'ACTIVE')), '')
  FROM gleif_lei_records
  WHERE jurisdiction = 'SE'
    AND replaceRegexpAll(registered_as, '[^0-9]', '') = {id:String}
) AS my_lei
SELECT w.wikidata_id AS wikidata_id,
  w.wikidata_url AS wikidata_url,
  coalesce(w.company_description, '') AS description,
  coalesce(w.official_name, '') AS official_name,
  coalesce(toString(w.inception_date), '') AS inception_date,
  w.employee_count AS employee_count,
  coalesce(toString(w.employee_count_point_in_time), '') AS employee_count_as_of,
  coalesce(w.industry_label, '') AS industry_label,
  coalesce(w.legal_form_label, '') AS legal_form_label,
  coalesce(w.headquarters_label, '') AS headquarters,
  coalesce(w.headquarters_country_label, '') AS headquarters_country,
  coalesce(w.logo_image_url, '') AS logo_url,
  toUInt8(w.has_current_listing) AS has_current_listing,
  coalesce(l.listings, '') AS listings,
  coalesce(s.websites, '') AS websites,
  coalesce(li.linkedin, '') AS linkedin_id
FROM wikidata_companies AS w
LEFT JOIN (
  SELECT wikidata_id,
    arrayStringConcat(groupUniqArray(concat(exchange_name, ': ', ticker)), ' | ') AS listings
  FROM wikidata_company_listings
  WHERE is_current AND ticker != ''
  GROUP BY wikidata_id
) AS l ON l.wikidata_id = w.wikidata_id
LEFT JOIN (
  SELECT wikidata_id,
    arrayStringConcat(groupUniqArray(website_url), ' ') AS websites
  FROM wikidata_company_websites
  GROUP BY wikidata_id
) AS s ON s.wikidata_id = w.wikidata_id
LEFT JOIN (
  SELECT wikidata_id, any(identifier_value) AS linkedin
  FROM wikidata_company_identifiers
  WHERE identifier_type = 'linkedin_company_id'
  GROUP BY wikidata_id
) AS li ON li.wikidata_id = w.wikidata_id
WHERE w.wikidata_id IN (
  SELECT wikidata_id FROM wikidata_company_identifiers
  WHERE (identifier_type = 'se_orgnr'
         AND replaceRegexpAll(identifier_value, '[^0-9]', '') = {id:String})
     OR (my_lei != '' AND identifier_type = 'lei' AND upper(identifier_value) = my_lei)
)
LIMIT 1`,
      wikidataPeopleQuery: `WITH (
  SELECT coalesce(argMax(lei, (registration_status = 'ISSUED', entity_status = 'ACTIVE')), '')
  FROM gleif_lei_records
  WHERE jurisdiction = 'SE'
    AND replaceRegexpAll(registered_as, '[^0-9]', '') = {id:String}
) AS my_lei
SELECT p.person_wikidata_id AS person_wikidata_id,
  per.name AS name,
  coalesce(per.description, '') AS description,
  per.birth_year AS birth_year,
  coalesce(per.image_url, '') AS image_url,
  coalesce(per.wikidata_url, '') AS wikidata_url,
  p.role_label AS role_label,
  toUInt8(p.is_current) AS is_current,
  coalesce(toString(p.start_date), '') AS start_date,
  coalesce(toString(p.end_date), '') AS end_date
FROM wikidata_company_people AS p
JOIN wikidata_persons AS per ON per.person_wikidata_id = p.person_wikidata_id
WHERE p.company_wikidata_id IN (
  SELECT wikidata_id FROM wikidata_company_identifiers
  WHERE (identifier_type = 'se_orgnr'
         AND replaceRegexpAll(identifier_value, '[^0-9]', '') = {id:String})
     OR (my_lei != '' AND identifier_type = 'lei' AND upper(identifier_value) = my_lei)
)
ORDER BY p.is_current DESC, p.role_label, per.name
LIMIT 100`,
      industriesQuery: `SELECT i.nace_rev2_class_code AS industry_code,
  '' AS description_original,
  coalesce(nullIf(n.description_en, ''), i.nace_rev2_class_code) AS industry_label,
  i.is_primary AS is_primary
FROM se_industries AS i
LEFT JOIN nace_categories AS n ON n.normalized_code = i.nace_rev2_class_code AND n.is_current = 1
WHERE i.company_id IN (SELECT company_id FROM se_companies WHERE registration_number = {id:String})
ORDER BY i.is_primary DESC, i.sequence
LIMIT 100`,
      addressQuery: `SELECT address_type AS address_type,
  arrayStringConcat(arrayFilter(x -> x != '', [
    coalesce(care_of, ''),
    coalesce(nullIf(street_address, ''), raw_address, ''),
    trim(concat(coalesce(postal_code, ''), ' ', coalesce(post_town, '')))
  ]), ', ') AS full_address
FROM se_company_addresses
WHERE company_id IN (SELECT company_id FROM se_companies WHERE registration_number = {id:String})
ORDER BY address_type
LIMIT 10`,
      publicContractsQuery: `SELECT
  source_slug AS source,
  concat(source_notice_id, if(source_lot_id = '', '', concat(':', source_lot_id))) AS notice_ref,
  coalesce(toString(publication_date), '') AS contract_date,
  buyer_name,
  title,
  toFloat64(value_amount_original) AS amount_original,
  toFloat64(value_amount_usd) AS amount_usd,
  value_currency AS currency,
  toFloat64(notice_value_amount_original) AS notice_amount_original,
  toFloat64(notice_value_amount_usd) AS notice_amount_usd,
  notice_value_currency AS notice_currency,
  source_url
FROM se_government_contracts
WHERE company_id = {id:String}
ORDER BY publication_date DESC NULLS LAST, contract_id
LIMIT 100`,
    },
    financialsLatest: { table: "se_company_financials_latest", companyKeyExpr: "company_id" },
    financialsByYear: { table: "se_financial_metrics", idColumn: "company_id" },
    financialsAggregates: {
      // Company ids normalized at the dagster layer since 2026-07-18 (16-prefix stripped); no workaround needed.
      nace: {
        industriesTable: "se_industries",
        companyKeyExpr: "toString(company_id)",
        naceCodeExpr: "nace_rev2_class_code",
        filterExpr: "is_primary = 1 AND nace_rev2_class_code != ''",
      },
    },
  },
  {
    code: "ee", iso3: "EST", eurostatGeoCode: "EE", name: "Estonia", flag: "🇪🇪", companiesTable: "ee_companies",
    idColumn: "reg_code", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "373k", features: ["financials", "industries", "contacts", "domains"],
    columns: [
      { key: "id", label: "Reg. code", expr: "reg_code", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "coalesce(nullIf(legal_form_en, ''), legal_form_original)", sortable: true, kind: "text", filterable: true },
      { key: "status", label: "Status", expr: "coalesce(nullIf(status_en, ''), status_original)", sortable: true, kind: "status", filterable: true },
      { key: "registered", label: "First entry", expr: "toString(first_entry_date)", sortable: true, kind: "date" },
      { key: "place", label: "Location", expr: "location", sortable: false, kind: "text", filterable: true },
    ],
    industryQuery: `SELECT i.reg_code AS company_id,
  i.nace_normalized_code AS industry_code,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label
FROM ee_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.reg_code IN {ids:Array(String)}
ORDER BY i.is_primary DESC
LIMIT 1 BY i.reg_code`,
    industryFacetQuery: `SELECT i.nace_normalized_code AS value,
  coalesce(nullIf(any(n.description_en), ''), any(coalesce(i.description_en, i.description_original)), value) AS label,
  count() AS cnt
FROM ee_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.is_primary = 1 AND i.nace_normalized_code != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
    industryFilterExpr: `reg_code IN (SELECT reg_code FROM ee_industries WHERE is_primary = 1 AND nace_normalized_code IN {f_industry:Array(String)})`,
    detail: {
      financialsQuery: `SELECT toString(fiscal_year) AS fiscal_year, currency AS currency,
  toFloat64(revenue_amount_original) AS revenue_amount_original,
  toFloat64(revenue_amount_usd) AS revenue_amount_usd,
  toFloat64(net_result_amount_original) AS net_result_amount_original,
  toFloat64(net_result_amount_usd) AS net_result_amount_usd,
  toFloat64(total_assets_amount_original) AS total_assets_amount_original,
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
  toFloat64(equity_amount_original) AS equity_amount_original,
  toFloat64(equity_amount_usd) AS equity_amount_usd,
  NULL AS employees
FROM ee_financial_metrics
WHERE reg_code = {id:String}
ORDER BY fiscal_year DESC, resolved_at DESC
LIMIT 1 BY fiscal_year
LIMIT 20`,
      contactsQuery: `SELECT contact_type AS contact_type, contact_value AS contact_value
FROM ee_company_contacts
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY contact_type, contact_value
LIMIT 100`,
      domainsQuery: `SELECT domain AS domain, website_url AS website_url, domain_source AS domain_source,
  toFloat64(confidence) AS confidence, is_primary AS is_primary
FROM ee_company_domains
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY is_primary DESC, confidence DESC
LIMIT 50`,
      industriesQuery: `SELECT i.nace_normalized_code AS industry_code,
  coalesce(i.description_original, '') AS description_original,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label,
  i.is_primary AS is_primary
FROM ee_industries AS i
LEFT JOIN nace_categories AS n ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.reg_code = {id:String}
ORDER BY i.is_primary DESC, industry_code
LIMIT 100`,
      addressQuery: `SELECT 'registered' AS address_type,
  arrayStringConcat(arrayFilter(x -> x != '', [
    coalesce(nullIf(address, ''), location, ''),
    coalesce(postal_code, '')
  ]), ', ') AS full_address
FROM ee_companies
WHERE reg_code = {id:String}
LIMIT 1`,
    },
    financialsLatest: { table: "ee_company_financials_latest", companyKeyExpr: "reg_code" },
    financialsAggregates: {
      nace: {
        industriesTable: "ee_industries",
        companyKeyExpr: "toString(reg_code)",
        naceCodeExpr: "nace_normalized_code",
        filterExpr: "is_primary = 1 AND nace_normalized_code != ''",
      },
    },
  },
  {
    code: "lv", iso3: "LVA", eurostatGeoCode: "LV", name: "Latvia", flag: "🇱🇻", companiesTable: "lv_companies",
    idColumn: "regcode", nameColumn: "legal_name", activeExpr: "is_active = 1",
    approxCompanies: "485k", features: ["financials", "contacts", "domains"],
    columns: [
      { key: "id", label: "Reg. code", expr: "regcode", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "legal_name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "coalesce(nullIf(legal_form_description_en, ''), legal_form_text)", sortable: true, kind: "text", filterable: true },
      { key: "status", label: "Status", expr: "status", sortable: true, kind: "status", filterable: true },
      { key: "registered", label: "Registered", expr: "registered_date", sortable: true, kind: "date" },
      { key: "place", label: "City", expr: "coalesce(address_city_name, '')", sortable: false, kind: "text", filterable: true },
    ],
    industryQuery: `SELECT regcode AS company_id,
  nace_code AS industry_code,
  coalesce(nullIf(nace_label, ''), nace_code) AS industry_label
FROM lv_companies_nace
WHERE regcode IN {ids:Array(String)}`,
    detail: {
      financialsQuery: `SELECT toString(fiscal_year) AS fiscal_year, currency AS currency,
  toFloat64(revenue_amount_original) AS revenue_amount_original,
  toFloat64(revenue_amount_usd) AS revenue_amount_usd,
  toFloat64(net_result_amount_original) AS net_result_amount_original,
  toFloat64(net_result_amount_usd) AS net_result_amount_usd,
  toFloat64(total_assets_amount_original) AS total_assets_amount_original,
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
  toFloat64(equity_amount_original) AS equity_amount_original,
  toFloat64(equity_amount_usd) AS equity_amount_usd,
  toFloat64(employees) AS employees
FROM lv_financial_metrics
WHERE regcode = {id:String}
ORDER BY fiscal_year DESC, resolved_at DESC
LIMIT 1 BY fiscal_year
LIMIT 20`,
      contactsQuery: `SELECT contact_type AS contact_type, contact_value AS contact_value
FROM lv_company_contacts
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY contact_type, contact_value
LIMIT 100`,
      domainsQuery: `SELECT domain AS domain, website_url AS website_url, domain_source AS domain_source,
  toFloat64(confidence) AS confidence, is_primary AS is_primary
FROM lv_company_domains
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY is_primary DESC, confidence DESC
LIMIT 50`,
      recordQuery: `SELECT c.*, t.activity_text_en
FROM lv_companies AS c
LEFT JOIN lv_companies_translated AS t ON t.regcode = c.regcode
WHERE c.regcode = {id:String}
LIMIT 1`,
      addressQuery: `SELECT 'registered' AS address_type,
  arrayStringConcat(arrayFilter(x -> x != '', [
    coalesce(address, ''),
    coalesce(postal_code, '')
  ]), ', ') AS full_address
FROM lv_companies
WHERE regcode = {id:String}
LIMIT 1`,
    },
    financialsLatest: { table: "lv_company_financials_latest", companyKeyExpr: "regcode" },
  },
  {
    code: "gb", iso3: "GBR", eurostatGeoCode: "UK", name: "United Kingdom", flag: "🇬🇧", companiesTable: "gb_companies",
    idColumn: "company_number", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "5.7M", features: ["financials", "industries"],
    columns: [
      { key: "id", label: "Company number", expr: "company_number", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Category", expr: "company_category", sortable: true, kind: "text", filterable: true },
      { key: "status", label: "Status", expr: "company_status", sortable: true, kind: "status", filterable: true },
      { key: "registered", label: "Incorporated", expr: "toString(incorporation_date)", sortable: true, kind: "date" },
      { key: "place", label: "City", expr: "city", sortable: false, kind: "text", filterable: true },
    ],
    industryQuery: `SELECT i.company_number AS company_id,
  i.nace_normalized_code AS industry_code,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label
FROM gb_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.company_number IN {ids:Array(String)}
ORDER BY i.is_primary DESC
LIMIT 1 BY i.company_number`,
    industryFacetQuery: `SELECT i.nace_normalized_code AS value,
  coalesce(nullIf(any(n.description_en), ''), any(coalesce(i.description_en, i.description_original)), value) AS label,
  count() AS cnt
FROM gb_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.is_primary = 1 AND i.nace_normalized_code != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
    industryFilterExpr: `company_number IN (SELECT company_number FROM gb_industries WHERE is_primary = 1 AND nace_normalized_code IN {f_industry:Array(String)})`,
    detail: {
      financialsQuery: `SELECT toString(fiscal_year) AS fiscal_year, currency AS currency,
  toFloat64(revenue_amount_original) AS revenue_amount_original,
  toFloat64(revenue_amount_usd) AS revenue_amount_usd,
  toFloat64(net_result_amount_original) AS net_result_amount_original,
  toFloat64(net_result_amount_usd) AS net_result_amount_usd,
  toFloat64(total_assets_amount_original) AS total_assets_amount_original,
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
  toFloat64(equity_amount_original) AS equity_amount_original,
  toFloat64(equity_amount_usd) AS equity_amount_usd,
  NULL AS employees
FROM gb_financial_metrics
WHERE company_number = {id:String}
ORDER BY fiscal_year DESC, resolved_at DESC
LIMIT 1 BY fiscal_year
LIMIT 20`,
      industriesQuery: `SELECT i.nace_normalized_code AS industry_code,
  coalesce(i.description_original, '') AS description_original,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label,
  i.is_primary AS is_primary
FROM gb_industries AS i
LEFT JOIN nace_categories AS n ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.company_number = {id:String}
ORDER BY i.is_primary DESC, industry_code
LIMIT 100`,
      addressQuery: `SELECT 'registered' AS address_type,
  arrayStringConcat(arrayFilter(x -> x != '', [
    coalesce(address, ''),
    coalesce(address_line_2, ''),
    trim(concat(coalesce(postal_code, ''), ' ', coalesce(city, ''))),
    coalesce(county, ''),
    coalesce(country, '')
  ]), ', ') AS full_address
FROM gb_companies
WHERE company_number = {id:String}
LIMIT 1`,
    },
    financialsLatest: { table: "gb_company_financials_latest", companyKeyExpr: "company_number" },
    financialsAggregates: {
      nace: {
        industriesTable: "gb_industries",
        companyKeyExpr: "toString(company_number)",
        naceCodeExpr: "nace_normalized_code",
        filterExpr: "is_primary = 1 AND nace_normalized_code != ''",
      },
    },
  },
  {
    code: "fr", iso3: "FRA", eurostatGeoCode: "FR", name: "France", flag: "🇫🇷", companiesTable: "fr_companies",
    idColumn: "siren", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "29.7M", features: ["industries"],
    columns: [
      { key: "id", label: "SIREN", expr: "siren", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "legal_form_en", sortable: true, kind: "text", filterable: true },
      { key: "status", label: "Status", expr: "status_en", sortable: true, kind: "status", filterable: true },
      { key: "registered", label: "Created", expr: "toString(creation_date)", sortable: true, kind: "date" },
      { key: "place", label: "City", expr: "city", sortable: false, kind: "text", filterable: true },
    ],
    industryQuery: `SELECT i.siren AS company_id,
  i.nace_normalized_code AS industry_code,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label
FROM fr_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.siren IN {ids:Array(String)}
ORDER BY i.is_primary DESC
LIMIT 1 BY i.siren`,
    industryFacetQuery: `SELECT i.nace_normalized_code AS value,
  coalesce(nullIf(any(n.description_en), ''), any(coalesce(i.description_en, i.description_original)), value) AS label,
  count() AS cnt
FROM fr_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.is_primary = 1 AND i.nace_normalized_code != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
    industryFilterExpr: `siren IN (SELECT siren FROM fr_industries WHERE is_primary = 1 AND nace_normalized_code IN {f_industry:Array(String)})`,
    detail: {
      industriesQuery: `SELECT i.nace_normalized_code AS industry_code,
  coalesce(i.description_original, '') AS description_original,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label,
  i.is_primary AS is_primary
FROM fr_industries AS i
LEFT JOIN nace_categories AS n ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.siren = {id:String}
ORDER BY i.is_primary DESC, industry_code
LIMIT 100`,
      addressQuery: `SELECT 'registered' AS address_type,
  arrayStringConcat(arrayFilter(x -> x != '', [
    coalesce(address, ''),
    coalesce(address_supplement, ''),
    trim(concat(coalesce(postal_code, ''), ' ', coalesce(city, '')))
  ]), ', ') AS full_address
FROM fr_companies
WHERE siren = {id:String}
LIMIT 1`,
    },
  },
  {
    code: "br", iso3: "BRA", name: "Brazil", flag: "🇧🇷", companiesTable: "br_companies",
    idColumn: "cnpj_basico", nameColumn: "legal_name", activeExpr: "is_active = 1",
    approxCompanies: "68.6M", features: ["financials", "contacts", "domains"],
    columns: [
      { key: "id", label: "CNPJ", expr: "cnpj_basico", sortable: true, kind: "id" },
      { key: "name", label: "Legal name", expr: "legal_name", sortable: true, kind: "text" },
      { key: "trade_name", label: "Trade name", expr: "trade_name", sortable: false, kind: "text" },
      { key: "size", label: "Size", expr: "company_size_en", sortable: true, kind: "text", filterable: true },
      { key: "status", label: "Status", expr: "status_en", sortable: true, kind: "status", filterable: true },
      { key: "registered", label: "Activity start", expr: "toString(activity_start_date)", sortable: true, kind: "date" },
      { key: "place", label: "Municipality", expr: "concat(municipality_name, ' / ', state)", sortable: false, kind: "text", filterable: true },
    ],
    // CNAE's OWN name first, the NACE division only as a fallback.
    //
    // Norway and Finland prefer their NACE join because it lands on the 4-digit
    // class, which is as specific as the register's own code. Brazil's bridge is
    // DIVISION level -- CNAE and NACE agree on the two-digit division and
    // nothing below it -- so preferring it here labelled a clothing shop
    // "Retail trade, except of motor vehicles and motorcycles". True, and far
    // coarser than what IBGE actually publishes for that code.
    //
    // description_en is empty until brazil_comp_cnae_translation_load runs, so
    // today this still falls through to the division and sharpens by itself
    // once the translator has been round -- no second change to make.
    industryQuery: `SELECT e.cnpj_basico AS company_id,
  e.primary_cnae_code AS industry_code,
  coalesce(nullIf(c.description_en, ''), nullIf(m.nace_description_en, ''), e.primary_cnae_code) AS industry_label
FROM br_establishments AS e
LEFT JOIN br_cnae_categories_translated AS c
  ON c.normalized_code = e.primary_cnae_code AND c.level = 'subclass'
LEFT JOIN br_cnae_to_nace AS m ON m.cnae_normalized_code = e.primary_cnae_code
WHERE e.cnpj_basico IN {ids:Array(String)} AND e.is_headquarters = 1
ORDER BY e.primary_cnae_code != '' DESC
LIMIT 1 BY e.cnpj_basico`,
    industryFacetQuery: `SELECT e.primary_cnae_code AS value,
  coalesce(nullIf(any(c.description_en), ''), nullIf(any(m.nace_description_en), ''), value) AS label,
  count() AS cnt
FROM br_establishments AS e
LEFT JOIN br_cnae_categories_translated AS c
  ON c.normalized_code = e.primary_cnae_code AND c.level = 'subclass'
LEFT JOIN br_cnae_to_nace AS m ON m.cnae_normalized_code = e.primary_cnae_code
WHERE e.is_headquarters = 1 AND e.primary_cnae_code != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
    industryFilterExpr: `cnpj_basico IN (SELECT cnpj_basico FROM br_establishments WHERE is_headquarters = 1 AND primary_cnae_code IN {f_industry:Array(String)})`,
    detail: {
      // The COMPANY-keyed view, like Sweden's and Finland's -- not
      // br_government_contract_awards. A company page asks "what did THIS company
      // win", and the awards view's extra rows are precisely the ones with no
      // company_id to attach to, so they could never appear here anyway.
      publicContractsQuery: `SELECT
  source_slug AS source,
  concat(source_notice_id, if(source_lot_id = '', '', concat(':', source_lot_id))) AS notice_ref,
  coalesce(toString(publication_date), '') AS contract_date,
  buyer_name,
  title,
  toFloat64(value_amount_original) AS amount_original,
  toFloat64(value_amount_usd) AS amount_usd,
  value_currency AS currency,
  toFloat64(notice_value_amount_original) AS notice_amount_original,
  toFloat64(notice_value_amount_usd) AS notice_amount_usd,
  notice_value_currency AS notice_currency,
  source_url
FROM br_government_contracts
WHERE company_id = {id:String}
ORDER BY publication_date DESC NULLS LAST, contract_id
LIMIT 100`,
      financialsQuery: `SELECT toString(fy) AS fiscal_year, any(cur) AS currency,
  anyIf(orig, metric = 'revenue') AS revenue_amount_original,
  anyIf(usd, metric = 'revenue') AS revenue_amount_usd,
  anyIf(orig, metric = 'net_income') AS net_result_amount_original,
  anyIf(usd, metric = 'net_income') AS net_result_amount_usd,
  anyIf(orig, metric = 'total_assets') AS total_assets_amount_original,
  anyIf(usd, metric = 'total_assets') AS total_assets_amount_usd,
  anyIf(orig, metric = 'equity') AS equity_amount_original,
  anyIf(usd, metric = 'equity') AS equity_amount_usd,
  NULL AS employees
FROM (
  SELECT toYear(period_end_date) AS fy, metric_name AS metric,
    toFloat64(amount_original) AS orig, toFloat64(amount_usd) AS usd, currency AS cur
  FROM br_cvm_financial_metrics
  WHERE cnpj_basico = {id:String} AND period_type = 'annual'
  ORDER BY consolidation_type = 'consolidated' DESC, reference_date DESC, version DESC
  LIMIT 1 BY fy, metric
)
GROUP BY fy
ORDER BY fy DESC
LIMIT 20`,
      contactsQuery: `SELECT contact_type AS contact_type, contact_value AS contact_value
FROM br_company_contacts
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY contact_type, contact_value
LIMIT 100`,
      domainsQuery: `SELECT domain AS domain, website_url AS website_url, domain_source AS domain_source,
  toFloat64(confidence) AS confidence, is_primary AS is_primary
FROM br_company_domains
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY is_primary DESC, confidence DESC
LIMIT 50`,
      // The code as Brazil writes it (4781-4/00, not 4781400) because this one
      // is display-only, and IBGE's Portuguese as the original beside the
      // English -- the pairing every other country's page already shows. The
      // section suppresses the original when it equals the label, so nothing
      // doubles up while description_en is still empty.
      industriesQuery: `SELECT coalesce(nullIf(c.code, ''), e.primary_cnae_code) AS industry_code,
  coalesce(c.description_pt, '') AS description_original,
  coalesce(nullIf(c.description_en, ''), nullIf(m.nace_description_en, ''), e.primary_cnae_code) AS industry_label,
  1 AS is_primary
FROM br_establishments AS e
LEFT JOIN br_cnae_categories_translated AS c
  ON c.normalized_code = e.primary_cnae_code AND c.level = 'subclass'
LEFT JOIN br_cnae_to_nace AS m ON m.cnae_normalized_code = e.primary_cnae_code
WHERE e.cnpj_basico = {id:String} AND e.is_headquarters = 1 AND e.primary_cnae_code != ''
LIMIT 100`,
      addressQuery: `SELECT 'headquarters' AS address_type,
  arrayStringConcat(arrayFilter(x -> x != '', [
    trim(concat(coalesce(street_type, ''), ' ', coalesce(street_name, ''), ' ', coalesce(street_number, ''))),
    coalesce(address_complement, ''),
    coalesce(district, ''),
    trim(concat(coalesce(postal_code, ''), ' ', coalesce(municipality_name, ''))),
    coalesce(state, '')
  ]), ', ') AS full_address,
  -- What the geocoder gets, built from the fields rather than by unpicking the
  -- display string. Verified against Nominatim on 2026-07-30: the display form
  -- resolves to NOTHING while this resolves to -12.9796986, -38.4568852.
  --   complement dropped   'EDIF METROPOLITANO ALFA LOJA 01' is noise to a geocoder
  --   district dropped     neighbourhood names add nothing once the street is there
  --   number un-padded     RFB zero-pads it, so 000999 has to become 999
  --   CEP hyphenated       Brazilian postal codes are written 41820-020
  arrayStringConcat(arrayFilter(x -> x != '', [
    trim(concat(coalesce(street_type, ''), ' ', coalesce(street_name, ''), ' ',
      if(match(coalesce(street_number, ''), '^0*[0-9]+$'),
         toString(toUInt64OrZero(street_number)), coalesce(street_number, '')))),
    coalesce(municipality_name, ''),
    coalesce(state, ''),
    if(length(coalesce(postal_code, '')) = 8,
       concat(substring(postal_code, 1, 5), '-', substring(postal_code, 6, 3)),
       coalesce(postal_code, ''))
  ]), ', ') AS geocode_address
FROM br_companies
WHERE cnpj_basico = {id:String}
LIMIT 1`,
    },
    financialsLatest: { table: "br_company_financials_latest", companyKeyExpr: "cnpj_basico" },
  },
  {
    code: "cz", iso3: "CZE", eurostatGeoCode: "CZ", name: "Czechia", flag: "🇨🇿", companiesTable: "cz_companies",
    idColumn: "ico", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "3.5M", features: ["industries", "contacts", "domains"],
    columns: [
      { key: "id", label: "IČO", expr: "ico", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "legal_form_en", sortable: true, kind: "text", filterable: true },
      { key: "status", label: "Status", expr: "if(is_active = 1, 'active', 'inactive')", sortable: true, kind: "status", filterable: true },
      { key: "registered", label: "Established", expr: "toString(established_date)", sortable: true, kind: "date" },
      { key: "place", label: "City", expr: "city", sortable: false, kind: "text", filterable: true },
    ],
    industryQuery: `SELECT i.ico AS company_id,
  i.nace_normalized_code AS industry_code,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label
FROM cz_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.ico IN {ids:Array(String)}
ORDER BY i.is_primary DESC
LIMIT 1 BY i.ico`,
    industryFacetQuery: `SELECT i.nace_normalized_code AS value,
  coalesce(nullIf(any(n.description_en), ''), any(coalesce(i.description_en, i.description_original)), value) AS label,
  count() AS cnt
FROM cz_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.is_primary = 1 AND i.nace_normalized_code != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
    industryFilterExpr: `ico IN (SELECT ico FROM cz_industries WHERE is_primary = 1 AND nace_normalized_code IN {f_industry:Array(String)})`,
    detail: {
      contactsQuery: `SELECT contact_type AS contact_type, contact_value AS contact_value
FROM cz_company_contacts
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY contact_type, contact_value
LIMIT 100`,
      domainsQuery: `SELECT domain AS domain, website_url AS website_url, domain_source AS domain_source,
  toFloat64(confidence) AS confidence, is_primary AS is_primary
FROM cz_company_domains
WHERE registry_id = {id:String} AND is_current = 1
ORDER BY is_primary DESC, confidence DESC
LIMIT 50`,
      industriesQuery: `SELECT i.nace_normalized_code AS industry_code,
  coalesce(i.description_original, '') AS description_original,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label,
  i.is_primary AS is_primary
FROM cz_industries AS i
LEFT JOIN nace_categories AS n ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.ico = {id:String}
ORDER BY i.is_primary DESC, industry_code
LIMIT 100`,
      addressQuery: `SELECT 'registered' AS address_type,
  arrayStringConcat(arrayFilter(x -> x != '', [
    coalesce(address, ''),
    trim(concat(coalesce(postal_code, ''), ' ', coalesce(city, '')))
  ]), ', ') AS full_address
FROM cz_companies
WHERE ico = {id:String}
LIMIT 1`,
    },
  },
  {
    code: "sk", iso3: "SVK", eurostatGeoCode: "SK", name: "Slovakia", flag: "🇸🇰", companiesTable: "sk_companies",
    idColumn: "ico", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "2.2M", features: ["financials", "industries"],
    columns: [
      { key: "id", label: "IČO", expr: "ico", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "coalesce(nullIf(legal_form_en, ''), legal_form_original)", sortable: true, kind: "text", filterable: true },
      { key: "status", label: "Status", expr: "if(is_active = 1, 'active', 'inactive')", sortable: true, kind: "status", filterable: true },
      { key: "registered", label: "Established", expr: "toString(established_date)", sortable: true, kind: "date" },
      { key: "place", label: "City", expr: "city", sortable: false, kind: "text", filterable: true },
    ],
    industryQuery: `SELECT i.ico AS company_id,
  i.nace_normalized_code AS industry_code,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label
FROM sk_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.ico IN {ids:Array(String)}
ORDER BY i.is_primary DESC
LIMIT 1 BY i.ico`,
    industryFacetQuery: `SELECT i.nace_normalized_code AS value,
  coalesce(nullIf(any(n.description_en), ''), any(coalesce(i.description_en, i.description_original)), value) AS label,
  count() AS cnt
FROM sk_industries AS i
LEFT JOIN nace_categories AS n
  ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.is_primary = 1 AND i.nace_normalized_code != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
    industryFilterExpr: `ico IN (SELECT ico FROM sk_industries WHERE is_primary = 1 AND nace_normalized_code IN {f_industry:Array(String)})`,
    detail: {
      industriesQuery: `SELECT i.nace_normalized_code AS industry_code,
  coalesce(i.description_original, '') AS description_original,
  coalesce(nullIf(n.description_en, ''), i.description_en, i.description_original, i.nace_normalized_code) AS industry_label,
  i.is_primary AS is_primary
FROM sk_industries AS i
LEFT JOIN nace_categories AS n ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1
WHERE i.ico = {id:String}
ORDER BY i.is_primary DESC, industry_code
LIMIT 100`,
      addressQuery: `SELECT 'registered' AS address_type,
  arrayStringConcat(arrayFilter(x -> x != '', [
    coalesce(address, ''),
    trim(concat(coalesce(postal_code, ''), ' ', coalesce(city, '')))
  ]), ', ') AS full_address
FROM sk_companies
WHERE ico = {id:String}
LIMIT 1`,
    },
    financialsLatest: { table: "sk_company_financials_latest", companyKeyExpr: "ico" },
    financialsAggregates: {
      nace: {
        industriesTable: "sk_industries",
        companyKeyExpr: "toString(ico)",
        naceCodeExpr: "nace_normalized_code",
        filterExpr: "is_primary = 1 AND nace_normalized_code != ''",
      },
    },
  },
];

export function getCountry(code: string): CountryConfig | undefined {
  const normalized = code.toLowerCase();
  return COUNTRIES.find((c) => c.code === normalized);
}

export function getSortColumn(
  country: CountryConfig,
  key: string | null,
): CompanyColumn {
  const match = country.columns.find((c) => c.sortable && c.key === key);
  return match ?? country.columns.find((c) => c.key === "name")!;
}
