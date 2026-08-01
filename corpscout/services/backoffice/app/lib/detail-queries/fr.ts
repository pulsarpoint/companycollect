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
