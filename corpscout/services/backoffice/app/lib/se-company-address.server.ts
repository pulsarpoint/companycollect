import { chQuery } from "~/lib/clickhouse.server";

/**
 * One registered address observation: a (company, address type, source)
 * triple, joined to everything the address pipeline later derived from it.
 *
 * Every field is a string because the query collapses joined-side misses to
 * "" rather than mapping them in TypeScript -- including latitude/longitude,
 * so "no coordinate" is one value ("") rather than null, 0 and undefined.
 * The `has_*` flags say which joined side actually matched; see ADDRESS_ROWS_SQL.
 */
export interface SeCompanyAddressRow {
  address_type: string;
  source: string;
  /** 1 when the normalization has produced a display row for this observation. */
  has_display: number;
  /** 1 when address identity has folded this observation into a canonical address. */
  has_link: number;
  has_canonical: number;
  /** 1 when the canonical address has been through the geocoder. */
  has_geocode: number;
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
 * been through address identity yet simply carries empty derived fields
 * instead of dropping out.
 *
 * MISSES ARE GATED, NOT ifNull'd. ClickHouse fills a LEFT JOIN miss with each
 * column's *type default*, not NULL, so `ifNull` is blind to it: on a miss
 * `g.match_confidence` (Float32) reads 0 and `g.matched_at` (DateTime64) reads
 * 1970-01-01, and the page rendered both as if the geocoder had answered.
 * Each joined side therefore carries a `has_*` flag tested against a column
 * that is never empty on a real row -- the run id that stamped it, or the
 * display table's own `source` -- and every column from that side is wrapped
 * in `if(has_..., ..., '')`. `ifNull` stays only where the SOURCE column is
 * genuinely Nullable (a.care_of, g.latitude), which is a different question.
 *
 * Every joined table here is a plain MergeTree snapshot rebuilt per run
 * (checked against system.tables), so none of them takes FINAL -- adding it
 * would be a full-table dedup pass for no change in the result.
 */
export const ADDRESS_ROWS_SQL = `SELECT
  toString(a.address_type) AS address_type,
  toString(a.source) AS source,
  toUInt8(d.source != '') AS has_display,
  toUInt8(l.address_identity_run_id != '') AS has_link,
  toUInt8(ca.address_identity_run_id != '') AS has_canonical,
  toUInt8(g.geocode_run_id != '') AS has_geocode,
  ifNull(a.raw_address, '') AS raw_address,
  if(has_display, d.display_address, '') AS display_address,
  ifNull(a.care_of, '') AS care_of,
  ifNull(a.street_address, '') AS street_address,
  ifNull(a.postal_code, '') AS postal_code,
  ifNull(a.post_town, '') AS post_town,
  ifNull(toString(a.country_code), '') AS country_code,
  if(has_display, toString(d.resolved_country_code), '') AS resolved_country_code,
  toUInt8(has_display AND d.is_foreign) AS is_foreign,
  a.normalized_address AS normalized_address,
  toUInt8(a.has_address) AS has_address,
  toString(a.address_fingerprint) AS address_key,
  toString(a.observed_at) AS observed_at,
  toString(a.updated_from_raw_at) AS updated_from_raw_at,
  if(has_link, toString(l.address_id), '') AS address_id,
  if(has_link, toString(l.canonical_address_key), '') AS canonical_address_key,
  if(has_link, toString(l.review_status), '') AS link_review_status,
  if(has_canonical, ca.canonical_display_address, '') AS canonical_display_address,
  toUInt8(
    has_canonical
    AND toString(ca.representative_address_source) = toString(a.source)
  ) AS is_canonical_source,
  if(has_geocode, toString(g.match_status), '') AS geocode_status,
  if(has_geocode, toString(g.geocode_precision), '') AS geocode_precision,
  if(has_geocode, toString(g.geocode_provider), '') AS geocode_provider,
  if(has_geocode, toString(g.match_method), '') AS geocode_match_method,
  if(has_geocode, toString(g.match_confidence), '') AS geocode_match_confidence,
  ifNull(toString(g.latitude), '') AS latitude,
  ifNull(toString(g.longitude), '') AS longitude,
  if(has_geocode, toString(g.matched_at), '') AS geocoded_at
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
