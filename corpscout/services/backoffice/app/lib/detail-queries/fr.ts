/**
 * France's company-detail queries.
 *
 * Held here rather than inline in countries.ts because that file is 1,482
 * lines and already mostly SQL literals; France adds five more. The config
 * still lives beside every other country's -- only the strings moved.
 *
 * Plain string consts with no imports, so this module is safe on both sides of
 * the server boundary (countries.ts is imported by route components).
 */

/**
 * Contract wins, in the canonical PublicContractRow shape.
 *
 * DECP publishes no per-winner value: value_amount_original and
 * value_amount_usd are NULL for all 721,161 French rows, while the notice
 * totals are populated for all of them. Both are selected, and
 * PublicContractsSection renders the notice figure LABELLED as the whole
 * procurement rather than as this company's share. Never divide it by the
 * number of winners -- DECP does not say how it was split.
 */
export const FR_PUBLIC_CONTRACTS_QUERY = `SELECT
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
FROM fr_government_contracts
WHERE company_id = {id:String}
ORDER BY publication_date DESC NULLS LAST, contract_id
LIMIT 100`;

/**
 * Wikidata enrichment, matched on the SIREN (P1616) with an LEI fallback.
 *
 * Coverage is thin -- 169 companies carry an fr_siren identifier and 168 are
 * reachable through an FR-jurisdiction LEI, almost entirely the same set. The
 * section hides itself when unmatched, which is why wiring it at this coverage
 * still pays: it is right for the handful of large companies that do match
 * (FNAC DARTY, Q47088340) and invisible everywhere else.
 */
export const FR_WIKIDATA_QUERY = `WITH (
  SELECT coalesce(argMax(lei, (registration_status = 'ISSUED', entity_status = 'ACTIVE')), '')
  FROM gleif_lei_records
  WHERE jurisdiction = 'FR'
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
  WHERE (identifier_type = 'fr_siren'
         AND replaceRegexpAll(identifier_value, '[^0-9]', '') = {id:String})
     OR (my_lei != '' AND identifier_type = 'lei' AND upper(identifier_value) = my_lei)
)
LIMIT 1`;

/** Company-anchored Wikidata people, same match rule as FR_WIKIDATA_QUERY. */
export const FR_WIKIDATA_PEOPLE_QUERY = `WITH (
  SELECT coalesce(argMax(lei, (registration_status = 'ISSUED', entity_status = 'ACTIVE')), '')
  FROM gleif_lei_records
  WHERE jurisdiction = 'FR'
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
  WHERE (identifier_type = 'fr_siren'
         AND replaceRegexpAll(identifier_value, '[^0-9]', '') = {id:String})
     OR (my_lei != '' AND identifier_type = 'lei' AND upper(identifier_value) = my_lei)
)
ORDER BY p.is_current DESC, p.role_label, per.name
LIMIT 100`;

/**
 * Per-year filed accounts, with the ratio suite INPI publishes.
 *
 * ONE ROW PER FISCAL YEAR. 41,055 (siren, fiscal_year) pairs carry more than
 * one filing because a company can file under more than one balance type --
 * C complete (4,924,259 rows), S simplified (1,586,394), K consolidated
 * (31,579). LIMIT 1 BY takes one, ordered by an explicit priority: the
 * entity's own complete accounts first, then its simplified ones, and the
 * consolidated group last, because this is the entity's page and not the
 * group's.
 *
 * NOT argMin over the balance type. argMin on a tied key picks arbitrarily
 * between runs -- the defect that made Swedish contract counts flicker
 * between 301 and 299 from identical data.
 *
 * The balance-type priority alone is not enough to break every tie: 10,344
 * (siren, fiscal_year, balance_type_code) groups across 10,178 companies carry
 * more than one row, so a third ORDER BY term, m.period_end_date DESC, decides
 * those -- the latest close wins under a given year label. Without it, a
 * company that changed its year-end (e.g. two 'C' filings for 2020 with
 * different period ends and genuinely different figures) would show whichever
 * filing happened to sort first physically, which is exactly the arbitrary
 * pick this query exists to avoid.
 *
 * No equity and no total assets: fr_company_financials_latest carries those
 * columns for every country but France fills neither, NULL across all
 * 1,586,046 rows. Joining it would add two permanently empty columns.
 *
 * Currency is EUR for all 6,542,232 rows, so the *_usd twins are conversions
 * rather than a second reporting currency.
 */
export const FR_FINANCIAL_METRICS_QUERY = `SELECT
  toString(m.fiscal_year) AS fiscal_year,
  m.balance_type_code AS balance_type,
  m.confidentiality_status AS confidentiality,
  m.currency AS currency,
  toFloat64(m.revenue_amount_original) AS revenue_original,
  toFloat64(m.revenue_amount_usd) AS revenue_usd,
  toFloat64(m.gross_margin_amount_original) AS gross_margin_original,
  toFloat64(m.gross_margin_amount_usd) AS gross_margin_usd,
  toFloat64(m.ebitda_amount_original) AS ebitda_original,
  toFloat64(m.ebitda_amount_usd) AS ebitda_usd,
  toFloat64(m.ebit_amount_original) AS ebit_original,
  toFloat64(m.ebit_amount_usd) AS ebit_usd,
  toFloat64(m.net_income_amount_original) AS net_income_original,
  toFloat64(m.net_income_amount_usd) AS net_income_usd,
  toFloat64(m.ebitda_margin_percent) AS ebitda_margin_percent,
  toFloat64(m.debt_ratio_percent) AS debt_ratio_percent,
  toFloat64(m.financial_autonomy_percent) AS financial_autonomy_percent,
  toFloat64(m.liquidity_ratio_percent) AS liquidity_ratio_percent,
  toFloat64(m.interest_coverage_percent) AS interest_coverage_percent,
  toFloat64(m.customer_payment_days) AS customer_payment_days,
  toFloat64(m.supplier_payment_days) AS supplier_payment_days,
  toFloat64(m.inventory_turnover_days) AS inventory_turnover_days,
  coalesce(toString(m.period_end_date), '') AS period_end_date
FROM fr_financial_metrics AS m
WHERE m.siren = {id:String}
ORDER BY m.fiscal_year DESC,
  multiIf(m.balance_type_code = 'C', 0, m.balance_type_code = 'S', 1, 2),
  m.period_end_date DESC
LIMIT 1 BY m.fiscal_year
LIMIT 15`;

/**
 * One row summarising a company's award history.
 *
 * total_value_usd is NULL for every French company -- public_award_value_usd
 * and public_award_valued_count are derived from the per-winner figure DECP
 * does not publish, so they are NULL and 0 across all 99,287 rows. It is
 * selected anyway because the header renders a value only when one exists, and
 * the other eight contract countries have the same table with figures in it.
 *
 * This aggregates fr_government_contracts directly rather than reading
 * fr_government_contract_summary: that view is unfiltered and groups over the
 * entire French contract set plus an unfiltered CTE, so every page view pays
 * a 721,161-row scan even for the 29.6M French companies with no contracts at
 * all (measured: 0.39s / 721k rows vs 0.077s for this form). Pushing the
 * company predicate into both the CTE and the main query instead keeps the
 * cost proportional to one company's rows. The cross: expression is carried
 * over verbatim from the view so a contract seen through two sources still
 * counts once -- it makes no difference for single-source France but keeps
 * the semantics identical for the eight other countries that may adopt this
 * later.
 */
export const FR_CONTRACT_SUMMARY_QUERY = `WITH cross_source_keys AS (
  SELECT contract_key FROM fr_government_contracts
  WHERE company_id = {id:String} AND contract_key != ''
  GROUP BY contract_key HAVING uniqExact(source_slug) > 1
)
SELECT
  toUInt32(uniqExact(if(contract_key IN (SELECT contract_key FROM cross_source_keys), concat('cross:', contract_key), contract_id))) AS award_count,
  toUInt32(countIf(value_amount_usd IS NOT NULL)) AS valued_count,
  toFloat64(sum(value_amount_usd)) AS total_value_usd,
  coalesce(toString(max(publication_date)), '') AS last_award_date,
  arrayStringConcat(arraySort(groupUniqArray(source_slug)), ', ') AS sources
FROM fr_government_contracts
WHERE company_id = {id:String}
HAVING award_count > 0`;
