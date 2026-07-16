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
}

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
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
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
    },
  },
  {
    code: "fi", name: "Finland", flag: "🇫🇮", companiesTable: "fi_companies",
    idColumn: "business_id", nameColumn: "name", activeExpr: "is_active = 1",
    approxCompanies: "460k", features: ["financials", "industries", "contacts", "domains"],
    columns: [
      { key: "id", label: "Business ID", expr: "business_id", sortable: true, kind: "id" },
      { key: "name", label: "Name", expr: "name", sortable: true, kind: "text" },
      { key: "legal_form", label: "Legal form", expr: "coalesce(legal_form_description_en, legal_form_description_original, legal_form_code)", sortable: true, kind: "text" }, // legal_form_* columns are NULL for all fi_companies rows (pipeline gap, 2026-07-16) — re-flag when populated
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
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
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
    },
  },
  {
    code: "se", name: "Sweden", flag: "🇸🇪", companiesTable: "se_companies",
    idColumn: "registration_number", nameColumn: "legal_name", activeExpr: "status = 'active'",
    approxCompanies: "4.1M", features: ["financials", "industries"],
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
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
  toFloat64(equity_amount_usd) AS equity_amount_usd,
  NULL AS employees
FROM ee_financial_metrics
WHERE reg_code = {id:String}
ORDER BY fiscal_year DESC
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
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
  toFloat64(equity_amount_usd) AS equity_amount_usd,
  toFloat64(employees) AS employees
FROM lv_financial_metrics
WHERE regcode = {id:String}
ORDER BY fiscal_year DESC
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
    },
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
  toFloat64(total_assets_amount_usd) AS total_assets_amount_usd,
  toFloat64(equity_amount_usd) AS equity_amount_usd,
  NULL AS employees
FROM gb_financial_metrics
WHERE company_number = {id:String}
ORDER BY fiscal_year DESC
LIMIT 1 BY fiscal_year
LIMIT 20`,
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
  anyIf(usd, metric = 'total_assets') AS total_assets_amount_usd,
  anyIf(usd, metric = 'equity') AS equity_amount_usd,
  NULL AS employees
FROM (
  SELECT toYear(period_end_date) AS fy, metric_name AS metric,
    toFloat64(amount_original) AS orig, toFloat64(amount_usd) AS usd, currency AS cur
  FROM br_cvm_financial_metrics
  WHERE cnpj_basico = {id:String} AND period_type = 'annual'
  ORDER BY consolidation_type = 'consolidated' DESC
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
    },
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
