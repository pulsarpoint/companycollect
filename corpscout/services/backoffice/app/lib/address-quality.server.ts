import { chQuery } from "~/lib/clickhouse.server";

/**
 * The published SE address entity (spec 2026-09-06, section 3.3): one row per
 * company and published address, geocode included. Slice 4b renames the table,
 * so the queue names it exactly once.
 */
const ADDRESS_TABLE = "corpscout.se_company_address_v2";

/**
 * The address line without its trailing postcode and town, so the queue can
 * show the street part the old chain stored in its own column, and `''` for a
 * line that is ONLY a postal part -- normalizer v3 publishes postcode-only
 * addresses (`100 11 Stockholm`), which have no comma for the strip to cut at,
 * so the strip alone would show the whole line as a street. Written with a
 * doubled backslash because the literal reaches ClickHouse as SQL text.
 */
const STREET_PART_SQL =
  "if(match(address.normalized_address, '^[0-9]{3} [0-9]{2}[^,]*$'), '', " +
  "replaceRegexpOne(address.normalized_address, ',\\\\s*[0-9]{3} [0-9]{2}[^,]*$', ''))";

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
  /** The company the published address belongs to: the entity is per company,
   * so a shared building is now one queue row per registration. */
  companyId: string;
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
  company_id: string;
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

interface AddressQualityCompanyNameRow {
  company_id: string;
  company_name: string;
}

const QUALITY_FILTER_SQL: Record<AddressQualityFilter, string> = {
  all: `(
    address.geocode_status IN ('ambiguous', 'unmatched', 'invalid_address')
    OR address.geocode_precision = 'street'
    OR address.geocode_precision = 'city'
    OR (
      address.geocode_status = 'matched_exact'
      AND address.geocode_confidence < 0.8
    )
  )`,
  ambiguous: "address.geocode_status = 'ambiguous'",
  unmatched: "address.geocode_status = 'unmatched'",
  invalid: "address.geocode_status = 'invalid_address'",
  street_fallback: "address.geocode_precision = 'street'",
  city_fallback: "address.geocode_precision = 'city'",
  low_confidence: `(
    address.geocode_status = 'matched_exact'
    AND address.geocode_confidence < 0.8
  )`,
};

/** Published rows only: a hidden or withdrawn address is not reviewable. */
const PUBLISHED_SQL = `FROM ${ADDRESS_TABLE} AS address FINAL
       WHERE address.active = 1`;

/** The counters and the filters are the same predicates, so the tiles can
 * never disagree with the queue they link to. */
const ADDRESS_QUALITY_STATS_QUERY = `SELECT
  countIf(${QUALITY_FILTER_SQL.all}) AS reviewable,
  countIf(${QUALITY_FILTER_SQL.ambiguous}) AS ambiguous,
  countIf(${QUALITY_FILTER_SQL.unmatched}) AS unmatched,
  countIf(${QUALITY_FILTER_SQL.invalid}) AS invalid,
  countIf(${QUALITY_FILTER_SQL.street_fallback}) AS street_fallback,
  countIf(${QUALITY_FILTER_SQL.city_fallback}) AS city_fallback,
  countIf(${QUALITY_FILTER_SQL.low_confidence}) AS low_confidence
${PUBLISHED_SQL}`;

const ADDRESS_SEARCH_SQL = `(
  {query:String} = ''
  OR positionCaseInsensitiveUTF8(
    address.normalized_address,
    {query:String}
  ) > 0
  OR positionCaseInsensitiveUTF8(ifNull(address.postal_code, ''), {query:String}) > 0
  OR positionCaseInsensitiveUTF8(ifNull(address.city, ''), {query:String}) > 0
  OR toString(address.address_key) = {query:String}
)`;

/**
 * The entity carries no OSM candidate or extract provenance, so the columns the
 * queue used to read from the geocode serving view are answered with empty
 * values rather than dropped: the table keeps its shape.
 */
const ADDRESS_QUALITY_COLUMNS_SQL = `address.company_id AS company_id,
         toString(address.address_key) AS address_id,
         address.normalized_address AS display_address,
         toString(address.text_source) AS representative_source,
         ${STREET_PART_SQL} AS street_address,
         ifNull(address.postal_code, '') AS postal_code,
         ifNull(address.city, '') AS post_town,
         if(ifNull(address.box, '') != '', 'postal_box', 'physical') AS address_kind,
         toUInt64(1) AS company_count,
         toUInt64(length(address.sources)) AS evidence_count,
         toString(address.geocode_status) AS match_status,
         toString(address.geocode_method) AS match_method,
         ifNull(address.geocode_confidence, 0) AS match_confidence,
         address.latitude AS latitude,
         address.longitude AS longitude,
         toString(address.geocode_precision) AS geocode_precision,
         toString(address.geocode_method) AS coordinate_method,
         ifNull(address.city, '') AS coordinate_locality,
         toUInt64(0) AS coordinate_supporting_point_count,
         toUInt64(0) AS candidate_count,
         CAST([], 'Array(String)') AS candidate_record_urls,
         '' AS source_url,
         '' AS source_snapshot_at,
         ifNull(toString(address.geocoded_at), '') AS matched_at`;

/** The old queue put the most-shared address first; a per-company row carries
 * its source observations instead, so the best-evidenced row leads. */
const ADDRESS_QUALITY_ORDER_SQL = `ORDER BY
         multiIf(
           address.geocode_status = 'ambiguous', 0,
           address.geocode_status = 'invalid_address', 1,
           address.geocode_precision = 'street', 2,
           address.geocode_precision = 'city', 3,
           address.geocode_status = 'unmatched', 4,
           5
         ),
         length(address.sources) DESC,
         address.address_key,
         address.company_id`;

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
           ${PUBLISHED_SQL}
             AND ${qualityFilter}
             AND ${ADDRESS_SEARCH_SQL}`,
          params,
        )
      : Promise.resolve([]),
    chQuery<AddressQualityDatabaseRow>(
      `SELECT
         ${ADDRESS_QUALITY_COLUMNS_SQL}
       ${PUBLISHED_SQL}
         AND ${qualityFilter}
         AND ${ADDRESS_SEARCH_SQL}
       ${ADDRESS_QUALITY_ORDER_SQL}
       LIMIT {limit:UInt64}
       OFFSET {offset:UInt64}`,
      params,
    ),
  ]);

  const companyIds = [...new Set(databaseRows.map((row) => row.company_id))];
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
      companyId: row.company_id,
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
      companies: [
        {
          companyId: row.company_id,
          companyName: companyNames.get(row.company_id) || row.company_id,
        },
      ],
    })),
    stats,
    total: query
      ? Number(searchedTotalRows[0]?.total ?? 0)
      : qualityTotal(stats, options.filter),
    page,
    pageSize,
  };
}
