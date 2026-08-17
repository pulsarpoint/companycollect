import { chQuery } from "~/lib/clickhouse.server";

export const ADDRESS_QUALITY_FILTERS = [
  "all",
  "ambiguous",
  "unmatched",
  "invalid",
  "street_fallback",
  "city_fallback",
  "low_confidence",
] as const;

export type AddressQualityFilter = (typeof ADDRESS_QUALITY_FILTERS)[number];

export interface AddressQualityStats {
  reviewable: number;
  ambiguous: number;
  unmatched: number;
  invalid: number;
  streetFallback: number;
  cityFallback: number;
  lowConfidence: number;
}

export interface AddressQualityCompany {
  companyId: string;
  companyName: string;
}

export interface AddressQualityRow {
  addressId: string;
  displayAddress: string;
  representativeSource: string;
  streetAddress: string;
  postalCode: string;
  postTown: string;
  addressKind: string;
  companyCount: number;
  evidenceCount: number;
  matchStatus: string;
  candidateCount: number;
  candidateRecordUrls: string[];
  matchMethod: string;
  matchConfidence: number;
  latitude: number | null;
  longitude: number | null;
  geocodePrecision: string;
  coordinateMethod: string;
  coordinateLocality: string;
  coordinateSupportingPointCount: number;
  sourceUrl: string;
  sourceSnapshotAt: string;
  matchedAt: string;
  companies: AddressQualityCompany[];
}

export interface AddressQualityQueueResult {
  rows: AddressQualityRow[];
  stats: AddressQualityStats;
  total: number;
  page: number;
  pageSize: number;
}

interface AddressQualityStatsRow {
  reviewable: number | string;
  ambiguous: number | string;
  unmatched: number | string;
  invalid: number | string;
  street_fallback: number | string;
  city_fallback: number | string;
  low_confidence: number | string;
}

interface AddressQualityDatabaseRow {
  address_id: string;
  display_address: string;
  representative_source: string;
  street_address: string;
  postal_code: string;
  post_town: string;
  address_kind: string;
  company_count: number | string;
  evidence_count: number | string;
  match_status: string;
  candidate_count: number | string;
  candidate_record_urls: string[];
  match_method: string;
  match_confidence: number | string;
  latitude: number | string | null;
  longitude: number | string | null;
  geocode_precision: string;
  coordinate_method: string;
  coordinate_locality: string;
  coordinate_supporting_point_count: number | string;
  source_url: string;
  source_snapshot_at: string;
  matched_at: string;
}

interface AddressQualityCompanyLinkRow {
  address_id: string;
  company_id: string;
}

interface AddressQualityCompanyNameRow {
  company_id: string;
  company_name: string;
}

const QUALITY_FILTER_SQL: Record<AddressQualityFilter, string> = {
  all: `(
    geocode.match_status IN ('ambiguous', 'unmatched', 'invalid_address')
    OR geocode.geocode_precision = 'street'
    OR geocode.geocode_precision = 'city'
    OR (
      geocode.match_status = 'matched_exact'
      AND geocode.match_confidence < 0.8
    )
  )`,
  ambiguous: "geocode.match_status = 'ambiguous'",
  unmatched: "geocode.match_status = 'unmatched'",
  invalid: "geocode.match_status = 'invalid_address'",
  street_fallback: "geocode.geocode_precision = 'street'",
  city_fallback: "geocode.geocode_precision = 'city'",
  low_confidence: `(
    geocode.match_status = 'matched_exact'
    AND geocode.match_confidence < 0.8
  )`,
};

const ADDRESS_QUALITY_STATS_QUERY = `SELECT
  countIf(
    match_status IN ('ambiguous', 'unmatched', 'invalid_address')
    OR geocode_precision = 'street'
    OR geocode_precision = 'city'
    OR (match_status = 'matched_exact' AND match_confidence < 0.8)
  ) AS reviewable,
  countIf(match_status = 'ambiguous') AS ambiguous,
  countIf(match_status = 'unmatched') AS unmatched,
  countIf(match_status = 'invalid_address') AS invalid,
  countIf(geocode_precision = 'street') AS street_fallback,
  countIf(geocode_precision = 'city') AS city_fallback,
  countIf(match_status = 'matched_exact' AND match_confidence < 0.8)
    AS low_confidence
FROM corpscout.se_address_geocodes_current`;

const ADDRESS_SEARCH_SQL = `(
  {query:String} = ''
  OR positionCaseInsensitiveUTF8(
    address.canonical_display_address,
    {query:String}
  ) > 0
  OR positionCaseInsensitiveUTF8(address.street_address, {query:String}) > 0
  OR positionCaseInsensitiveUTF8(address.postal_code, {query:String}) > 0
  OR positionCaseInsensitiveUTF8(address.post_town, {query:String}) > 0
  OR toString(address.address_id) = {query:String}
)`;

function normalizedPageSize(pageSize: number): number {
  return [25, 50, 100].includes(pageSize) ? pageSize : 50;
}

export function parseAddressQualityFilter(
  value: string | null,
): AddressQualityFilter {
  return ADDRESS_QUALITY_FILTERS.includes(value as AddressQualityFilter)
    ? (value as AddressQualityFilter)
    : "ambiguous";
}

function toNullableNumber(value: number | string | null): number | null {
  return value === null ? null : Number(value);
}

function qualityTotal(
  stats: AddressQualityStats,
  filter: AddressQualityFilter,
): number {
  const totals: Record<AddressQualityFilter, number> = {
    all: stats.reviewable,
    ambiguous: stats.ambiguous,
    unmatched: stats.unmatched,
    invalid: stats.invalid,
    street_fallback: stats.streetFallback,
    city_fallback: stats.cityFallback,
    low_confidence: stats.lowConfidence,
  };
  return totals[filter];
}

export async function searchAddressQualityQueue(options: {
  filter: AddressQualityFilter;
  query: string;
  page: number;
  pageSize: number;
}): Promise<AddressQualityQueueResult> {
  const page = Math.max(1, Math.floor(options.page));
  const pageSize = normalizedPageSize(options.pageSize);
  const query = options.query.trim().slice(0, 200);
  const qualityFilter = QUALITY_FILTER_SQL[options.filter];
  const params = {
    query,
    limit: pageSize,
    offset: (page - 1) * pageSize,
  };

  const [statsRows, searchedTotalRows, databaseRows] = await Promise.all([
    chQuery<AddressQualityStatsRow>(ADDRESS_QUALITY_STATS_QUERY),
    query
      ? chQuery<{ total: number | string }>(
          `SELECT count() AS total
           FROM corpscout.se_address_geocodes_current AS geocode
           INNER JOIN corpscout.se_addresses_current AS address USING (address_id)
           WHERE ${qualityFilter}
             AND ${ADDRESS_SEARCH_SQL}`,
          params,
        )
      : Promise.resolve([]),
    chQuery<AddressQualityDatabaseRow>(
      `SELECT
         toString(address.address_id) AS address_id,
         address.canonical_display_address AS display_address,
         address.representative_address_source AS representative_source,
         address.street_address,
         address.postal_code,
         address.post_town,
         address.address_kind,
         address.company_count,
         address.evidence_count,
         geocode.match_status,
         geocode.candidate_count,
         geocode.candidate_record_urls,
         geocode.match_method,
         geocode.match_confidence,
         geocode.latitude,
         geocode.longitude,
         geocode.geocode_precision,
         ifNull(geocode.coordinate_method, '') AS coordinate_method,
         ifNull(geocode.coordinate_locality, '') AS coordinate_locality,
         geocode.coordinate_supporting_point_count,
         ifNull(geocode.source_url, '') AS source_url,
         ifNull(toString(geocode.source_snapshot_at), '') AS source_snapshot_at,
         toString(geocode.matched_at) AS matched_at
       FROM corpscout.se_address_geocodes_current AS geocode
       INNER JOIN corpscout.se_addresses_current AS address USING (address_id)
       WHERE ${qualityFilter}
         AND ${ADDRESS_SEARCH_SQL}
       ORDER BY
         multiIf(
           geocode.match_status = 'ambiguous', 0,
           geocode.match_status = 'invalid_address', 1,
           geocode.geocode_precision = 'street', 2,
           geocode.geocode_precision = 'city', 3,
           geocode.match_status = 'unmatched', 4,
           5
         ),
         address.company_count DESC,
         address.address_id
       LIMIT {limit:UInt64}
       OFFSET {offset:UInt64}`,
      params,
    ),
  ]);

  const addressIds = databaseRows.map((row) => row.address_id);
  const companyLinkRows =
    addressIds.length === 0
      ? []
      : await chQuery<AddressQualityCompanyLinkRow>(
          `SELECT
             toString(link.address_id) AS address_id,
             link.company_id
           FROM corpscout.se_company_address_links_current AS link
           PREWHERE link.address_id IN {addressIds:Array(String)}
           ORDER BY link.address_id, link.company_id
           LIMIT 3 BY link.address_id`,
          { addressIds },
        );

  const companyIds = [...new Set(companyLinkRows.map((row) => row.company_id))];
  const companyNameRows =
    companyIds.length === 0
      ? []
      : await chQuery<AddressQualityCompanyNameRow>(
          `SELECT
             toString(registration_number) AS company_id,
             coalesce(legal_name, '') AS company_name
           FROM corpscout.se_companies
           PREWHERE registration_number IN {companyIds:Array(String)}`,
          { companyIds },
        );
  const companyNames = new Map(
    companyNameRows.map((row) => [row.company_id, row.company_name]),
  );

  const companiesByAddress = new Map<string, AddressQualityCompany[]>();
  for (const company of companyLinkRows) {
    const companies = companiesByAddress.get(company.address_id) ?? [];
    companies.push({
      companyId: company.company_id,
      companyName: companyNames.get(company.company_id) || company.company_id,
    });
    companiesByAddress.set(company.address_id, companies);
  }

  const statsRow = statsRows[0];
  const stats = {
    reviewable: Number(statsRow?.reviewable ?? 0),
    ambiguous: Number(statsRow?.ambiguous ?? 0),
    unmatched: Number(statsRow?.unmatched ?? 0),
    invalid: Number(statsRow?.invalid ?? 0),
    streetFallback: Number(statsRow?.street_fallback ?? 0),
    cityFallback: Number(statsRow?.city_fallback ?? 0),
    lowConfidence: Number(statsRow?.low_confidence ?? 0),
  };

  return {
    rows: databaseRows.map((row) => ({
      addressId: row.address_id,
      displayAddress: row.display_address,
      representativeSource: row.representative_source,
      streetAddress: row.street_address,
      postalCode: row.postal_code,
      postTown: row.post_town,
      addressKind: row.address_kind,
      companyCount: Number(row.company_count),
      evidenceCount: Number(row.evidence_count),
      matchStatus: row.match_status,
      candidateCount: Number(row.candidate_count),
      candidateRecordUrls: row.candidate_record_urls,
      matchMethod: row.match_method,
      matchConfidence: Number(row.match_confidence),
      latitude: toNullableNumber(row.latitude),
      longitude: toNullableNumber(row.longitude),
      geocodePrecision: row.geocode_precision,
      coordinateMethod: row.coordinate_method,
      coordinateLocality: row.coordinate_locality,
      coordinateSupportingPointCount: Number(
        row.coordinate_supporting_point_count,
      ),
      sourceUrl: row.source_url,
      sourceSnapshotAt: row.source_snapshot_at,
      matchedAt: row.matched_at,
      companies: companiesByAddress.get(row.address_id) ?? [],
    })),
    stats,
    total: query
      ? Number(searchedTotalRows[0]?.total ?? 0)
      : qualityTotal(stats, options.filter),
    page,
    pageSize,
  };
}
