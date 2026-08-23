import { chQuery } from "~/lib/clickhouse.server";

/**
 * One registered address observation: a (company, address type, source)
 * triple, joined to everything the address pipeline later derived from it.
 *
 * Every field is a string because ClickHouse's LEFT JOIN misses are collapsed
 * with ifNull in the query rather than mapped in TypeScript -- including
 * latitude/longitude, which stay text so "no coordinate" is one value ("")
 * rather than null, 0 and undefined.
 */
export interface SeCompanyAddressRow {
  address_type: string;
  source: string;
  raw_address: string;
  display_address: string;
  care_of: string;
  street_address: string;
  postal_code: string;
  post_town: string;
  /** As the register recorded it. */
  country_code: string;
  /** As the normalization resolved it (a foreign address keeps its own code). */
  resolved_country_code: string;
  is_foreign: number;
  normalized_address: string;
  has_address: number;
  /** sha256 of the normalized address; `address_fingerprint` in the source
   * table, `address_key` in the display and member tables. */
  address_key: string;
  observed_at: string;
  updated_from_raw_at: string;
  /** The canonical address identity this observation was folded into. */
  address_id: string;
  canonical_address_key: string;
  canonical_display_address: string;
  /** 1 when this source is the one the canonical address is displayed from. */
  is_canonical_source: number;
  link_review_status: string;
  geocode_status: string;
  geocode_precision: string;
  geocode_provider: string;
  geocode_match_method: string;
  geocode_match_confidence: string;
  latitude: string;
  longitude: string;
  geocoded_at: string;
}

/**
 * Registered addresses with their normalization and geocoding, keyed the way
 * the pipeline keys them.
 *
 * The join chain is the real one and cannot be shortened: the geocoder works
 * on *canonical* addresses (`se_addresses_current.address_id`, one row per
 * distinct address across all companies), not on a company's own observation.
 * Getting from one to the other goes through the member table (observation
 * `address_key` -> `canonical_address_key`) and then the link table
 * (`canonical_address_key` -> `address_id`), so a company row that has not
 * been through address identity yet simply carries empty geocode fields
 * instead of dropping out.
 *
 * Every joined table here is a plain MergeTree snapshot rebuilt per run
 * (checked against system.tables), so none of them takes FINAL -- adding it
 * would be a full-table dedup pass for no change in the result.
 */
export const ADDRESS_ROWS_SQL = `SELECT
  toString(a.address_type) AS address_type,
  toString(a.source) AS source,
  ifNull(a.raw_address, '') AS raw_address,
  ifNull(d.display_address, '') AS display_address,
  ifNull(a.care_of, '') AS care_of,
  ifNull(a.street_address, '') AS street_address,
  ifNull(a.postal_code, '') AS postal_code,
  ifNull(a.post_town, '') AS post_town,
  ifNull(toString(a.country_code), '') AS country_code,
  ifNull(toString(d.resolved_country_code), '') AS resolved_country_code,
  toUInt8(ifNull(d.is_foreign, 0)) AS is_foreign,
  a.normalized_address AS normalized_address,
  toUInt8(a.has_address) AS has_address,
  toString(a.address_fingerprint) AS address_key,
  toString(a.observed_at) AS observed_at,
  toString(a.updated_from_raw_at) AS updated_from_raw_at,
  ifNull(toString(l.address_id), '') AS address_id,
  ifNull(toString(l.canonical_address_key), '') AS canonical_address_key,
  ifNull(ca.canonical_display_address, '') AS canonical_display_address,
  toUInt8(
    ifNull(toString(ca.representative_address_source), '') = toString(a.source)
  ) AS is_canonical_source,
  ifNull(toString(l.review_status), '') AS link_review_status,
  ifNull(toString(g.match_status), '') AS geocode_status,
  ifNull(toString(g.geocode_precision), '') AS geocode_precision,
  ifNull(toString(g.geocode_provider), '') AS geocode_provider,
  ifNull(toString(g.match_method), '') AS geocode_match_method,
  ifNull(toString(g.match_confidence), '') AS geocode_match_confidence,
  ifNull(toString(g.latitude), '') AS latitude,
  ifNull(toString(g.longitude), '') AS longitude,
  ifNull(toString(g.matched_at), '') AS geocoded_at
FROM corpscout.se_company_addresses_current AS a
LEFT JOIN corpscout.se_company_address_display_current AS d
  ON d.company_id = a.company_id AND d.address_key = a.address_fingerprint
LEFT JOIN corpscout.se_company_address_members_current AS m
  ON m.company_id = a.company_id AND m.address_key = a.address_fingerprint
LEFT JOIN corpscout.se_company_address_links_current AS l
  ON l.company_id = a.company_id
 AND l.canonical_address_key = m.canonical_address_key
LEFT JOIN corpscout.se_addresses_current AS ca
  ON ca.address_id = l.address_id
LEFT JOIN corpscout.se_address_geocodes_current AS g
  ON g.address_id = l.address_id
WHERE a.company_id = {companyId:String}
ORDER BY a.address_type, a.source
LIMIT 100`;

/** Every registered address of one company, newest normalization attached. */
export async function loadSeCompanyAddresses(
  companyId: string,
): Promise<SeCompanyAddressRow[]> {
  return chQuery<SeCompanyAddressRow>(ADDRESS_ROWS_SQL, { companyId });
}
