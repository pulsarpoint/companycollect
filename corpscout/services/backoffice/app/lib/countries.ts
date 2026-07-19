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
   * {id:String} + {year:UInt16} → raw source facts for ONE fiscal year's
   * filing (see FactRow in queries.server), in source document order.
   * Presence of this query makes the Financials year column link to
   * /company/:country/:id/facts/:year.
   */
  factsQuery?: string;
}

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
    code: "no", name: "Norway", flag: "🇳🇴", companiesTable: "no_companies",
    idColumn: "org_number", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "1.2M", features: ["financials", "industries", "contacts", "domains"],
    columns: [
      { key: "id", label: "Org number", expr: "org_number", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "coalesce(legal_form_description_original, legal_form_code)", sortable: true, kind: "text", filterable: true },
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
    code: "fi", name: "Finland", flag: "🇫🇮", companiesTable: "fi_companies",
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
    },
    financialsLatest: { table: "fi_company_financials_latest", companyKeyExpr: "business_id" },
  },
  {
    code: "se", name: "Sweden", flag: "🇸🇪", companiesTable: "se_companies",
    idColumn: "registration_number", nameColumn: "legal_name", activeExpr: "status = 'active'",
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
      // se_financial_metrics is keyed on the normalized 10-digit orgnr
      // (= registration_number since the 2026-07-18 identity fix). Some
      // companies carry duplicate per-year rows where one is all-NULL —
      // the isNull tiebreak prefers the complete row (same rule as the
      // company_financials_latest summary build).
      financialsQuery: `SELECT toString(fiscal_year) AS fiscal_year, currency AS currency,
  toFloat64(revenue_amount_original) AS revenue_amount_original,
  toFloat64(revenue_amount_usd) AS revenue_amount_usd,
  toFloat64(profit_loss_amount_original) AS net_result_amount_original,
  toFloat64(profit_loss_amount_usd) AS net_result_amount_usd,
  toFloat64(total_assets_amount_original) AS total_assets_amount_original,
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
  toFloat64(equity_amount_original) AS equity_amount_original,
  toFloat64(equity_amount_usd) AS equity_amount_usd,
  toFloat64(employees) AS employees
FROM se_financial_metrics
WHERE company_id = {id:String}
ORDER BY fiscal_year DESC, isNull(revenue_amount_original) ASC, source_record_id DESC
LIMIT 1 BY fiscal_year
LIMIT 20`,
      // Same filing the metrics row shows (metrics statement_key first);
      // falls back to the newest filing seen in facts for years whose
      // metrics row hasn't been rebuilt yet.
      factsQuery: `SELECT concept_local_name AS concept, value_kind AS value_kind, raw_value AS raw_value,
  toFloat64(amount_original) AS amount_original,
  toFloat64(amount_usd) AS amount_usd,
  currency AS currency,
  toString(date_value) AS date_value,
  text_value AS text_value,
  dimensions AS dimensions
FROM se_financial_facts
WHERE company_id = {id:String} AND statement_key IN (
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
    },
    financialsLatest: { table: "se_company_financials_latest", companyKeyExpr: "company_id" },
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
    code: "ee", name: "Estonia", flag: "🇪🇪", companiesTable: "ee_companies",
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
    code: "lv", name: "Latvia", flag: "🇱🇻", companiesTable: "lv_companies",
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
    code: "gb", name: "United Kingdom", flag: "🇬🇧", companiesTable: "gb_companies",
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
    code: "fr", name: "France", flag: "🇫🇷", companiesTable: "fr_companies",
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
    code: "br", name: "Brazil", flag: "🇧🇷", companiesTable: "br_companies",
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
    industryQuery: `SELECT e.cnpj_basico AS company_id,
  e.primary_cnae_code AS industry_code,
  coalesce(nullIf(m.nace_description_en, ''), e.primary_cnae_code) AS industry_label
FROM br_establishments AS e
LEFT JOIN br_cnae_to_nace AS m ON m.cnae_normalized_code = e.primary_cnae_code
WHERE e.cnpj_basico IN {ids:Array(String)} AND e.is_headquarters = 1
ORDER BY e.primary_cnae_code != '' DESC
LIMIT 1 BY e.cnpj_basico`,
    industryFacetQuery: `SELECT e.primary_cnae_code AS value,
  coalesce(nullIf(any(m.nace_description_en), ''), value) AS label,
  count() AS cnt
FROM br_establishments AS e
LEFT JOIN br_cnae_to_nace AS m ON m.cnae_normalized_code = e.primary_cnae_code
WHERE e.is_headquarters = 1 AND e.primary_cnae_code != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`,
    industryFilterExpr: `cnpj_basico IN (SELECT cnpj_basico FROM br_establishments WHERE is_headquarters = 1 AND primary_cnae_code IN {f_industry:Array(String)})`,
    detail: {
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
      industriesQuery: `SELECT e.primary_cnae_code AS industry_code,
  '' AS description_original,
  coalesce(nullIf(m.nace_description_en, ''), e.primary_cnae_code) AS industry_label,
  1 AS is_primary
FROM br_establishments AS e
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
  ]), ', ') AS full_address
FROM br_companies
WHERE cnpj_basico = {id:String}
LIMIT 1`,
    },
    financialsLatest: { table: "br_company_financials_latest", companyKeyExpr: "cnpj_basico" },
  },
  {
    code: "cz", name: "Czechia", flag: "🇨🇿", companiesTable: "cz_companies",
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
    code: "sk", name: "Slovakia", flag: "🇸🇰", companiesTable: "sk_companies",
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
