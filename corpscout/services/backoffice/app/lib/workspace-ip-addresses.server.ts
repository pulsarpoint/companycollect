import { isIP } from "node:net";
import { chQuery } from "~/lib/clickhouse.server";
import {
  workspaceIpListOrder,
  type WorkspaceIpFilters,
} from "~/lib/workspace-ip-addresses";

/** One row per inventory IP with its current enrichment (migration 000470). A
 * refreshable view rebuilds it daily and after every IP enrichment task. */
export const IP_SEARCH_RELATION = "corpscout.ip_enrichment_search";

export interface WorkspaceIpAddress {
  bucket: number;
  ip_version: 4 | 6;
  ip: string;
  first_seen: string;
  last_seen: string;
  country_iso_code: string | null;
  city_name: string | null;
  asn: number | null;
  asn_organization: string | null;
  rdap_matched_cidr: string | null;
  rdap_name: string | null;
}

interface SearchRow {
  bucket: number;
  ip_version: 4 | 6;
  ip: string;
  first_seen: string;
  last_seen: string;
  asn: number;
  asn_organization: string;
  country_iso_code: string;
  subdivision_iso_code: string;
  city_name: string;
  rdap_matched_cidr: string;
  rdap_name: string;
}

const PAGE_SIZE = 50;
const QUERY_SETTINGS =
  "SETTINGS max_threads=4, max_execution_time=20, optimize_read_in_order=1";

export interface WorkspaceIpStatistics {
  total: number;
  ipv4: number;
  ipv6: number;
  countedAt: string;
  /** Last successful rebuild of the search table (UTC, "YYYY-MM-DD hh:mm:ss"). */
  refreshedAt: string | null;
  /** The last rebuild raised: the table still serves the previous snapshot. */
  refreshFailed: boolean;
}

let statisticsCache: WorkspaceIpStatistics | undefined;
let statisticsPending: Promise<WorkspaceIpStatistics> | undefined;
const STATISTICS_TTL_MS = 5 * 60 * 1000;

export async function getWorkspaceIpStatistics(): Promise<WorkspaceIpStatistics> {
  if (
    statisticsCache &&
    Date.now() - Date.parse(statisticsCache.countedAt) < STATISTICS_TTL_MS
  ) {
    return statisticsCache;
  }
  if (statisticsPending) return statisticsPending;
  statisticsPending = (async () => {
    // One row per IP: a count reads the one-byte ip_version column only.
    const [counts, refreshes] = await Promise.all([
      chQuery<{ ip_version: number; addresses: string }>(
        `SELECT ip_version, toString(count()) AS addresses FROM ${IP_SEARCH_RELATION}
         GROUP BY ip_version ${QUERY_SETTINGS}`,
      ),
      chQuery<{ last_success_time: string | null; failed: number }>(
        `SELECT toString(last_success_time) AS last_success_time,
           toUInt8(exception != '') AS failed
         FROM system.view_refreshes
         WHERE database = 'corpscout' AND view = 'ip_enrichment_search'`,
      ),
    ]);
    let ipv4 = 0;
    let ipv6 = 0;
    for (const row of counts) {
      if (row.ip_version === 4) ipv4 += Number(row.addresses);
      if (row.ip_version === 6) ipv6 += Number(row.addresses);
    }
    const refresh = refreshes[0];
    statisticsCache = {
      total: ipv4 + ipv6,
      ipv4,
      ipv6,
      countedAt: new Date().toISOString(),
      refreshedAt: refresh?.last_success_time || null,
      refreshFailed: Boolean(refresh?.failed),
    };
    return statisticsCache;
  })();
  try {
    return await statisticsPending;
  } finally {
    statisticsPending = undefined;
  }
}

type Cursor =
  | { order: "asn"; key: [number, number, string] }
  | { order: "location"; key: [string, string, string, number, string] };

function isBucket(value: unknown): value is number {
  return Number.isInteger(value) && Number(value) >= 0 && Number(value) < 256;
}

function isShortString(value: unknown): value is string {
  return typeof value === "string" && value.length <= 256;
}

function decodeCursor(after: string, order: Cursor["order"]): Cursor | null {
  if (!after || after.length > 1024) return null;
  try {
    const cursor = JSON.parse(Buffer.from(after, "base64url").toString());
    const key = cursor?.key;
    if (cursor?.order !== order || !Array.isArray(key)) return null;
    if (
      order === "asn" &&
      key.length === 3 &&
      Number.isInteger(key[0]) &&
      key[0] >= 0 &&
      key[0] <= 4294967295 &&
      isBucket(key[1]) &&
      typeof key[2] === "string" &&
      isIP(key[2])
    )
      return { order, key: [key[0], key[1], key[2]] };
    if (
      order === "location" &&
      key.length === 5 &&
      isShortString(key[0]) &&
      isShortString(key[1]) &&
      isShortString(key[2]) &&
      isBucket(key[3]) &&
      typeof key[4] === "string" &&
      isIP(key[4])
    )
      return { order, key: [key[0], key[1], key[2], key[3], key[4]] };
  } catch {
    // A stale or malformed cursor starts at the first page.
  }
  return null;
}

function encodeCursor(order: Cursor["order"], row: SearchRow): string {
  const key =
    order === "asn"
      ? [row.asn, row.bucket, row.ip]
      : [
          row.country_iso_code,
          row.subdivision_iso_code,
          row.city_name,
          row.bucket,
          row.ip,
        ];
  return Buffer.from(JSON.stringify({ order, key })).toString("base64url");
}

/** Filter predicates over the search table, every value a bound parameter. */
export function workspaceIpConditions(filters: WorkspaceIpFilters): {
  conditions: string[];
  params: Record<string, unknown>;
} {
  const conditions: string[] = [];
  const params: Record<string, unknown> = {};
  if (filters.version !== "any") {
    conditions.push("ip_version = {version:UInt8}");
    params.version = Number(filters.version);
  }
  if (filters.asn.length) {
    conditions.push("asn IN {asns:Array(UInt32)}");
    params.asns = filters.asn.map(Number);
  }
  if (filters.country.length) {
    conditions.push("country_iso_code IN {countries:Array(String)}");
    params.countries = filters.country;
  }
  if (filters.country.length === 1 && filters.region.length) {
    conditions.push("subdivision_iso_code IN {regions:Array(String)}");
    params.regions = filters.region;
  }
  if (filters.country.length === 1 && filters.city.length) {
    conditions.push("city_name IN {cities:Array(String)}");
    params.cities = filters.city;
  }
  if (filters.search) {
    params.search = filters.search;
    const version = isIP(filters.search);
    if (version) {
      // Canonicalize IPv6 (including mapped IPv4) exactly as DNS ingestion does.
      // The bucket narrows every key order's range, ip_bloom finds the granule.
      const canonical =
        version === 4
          ? "toString(toIPv4({search:String}))"
          : "toString(toIPv6({search:String}))";
      conditions.push(
        `bucket = toUInt16(modulo(cityHash64(${canonical}), 256))`,
        `ip = ${canonical}`,
      );
    } else {
      // Search text is ASCII; this exclusive upper bound describes precisely
      // the same prefix as a string interval.
      conditions.push("ip >= {search:String}", "ip < {prefixEnd:String}");
      params.prefixEnd =
        filters.search.slice(0, -1) +
        String.fromCharCode(
          filters.search.charCodeAt(filters.search.length - 1) + 1,
        );
    }
  }
  return { conditions, params };
}

/**
 * `(a, b, c) > (x, y, z)` spelled as nested comparisons: ClickHouse 26.5 does not
 * use a tuple comparison to narrow the primary key (every page would scan from
 * the start of the filtered range), but it does use this form.
 */
export function keysetAfter(keys: [column: string, value: string][]): string {
  const [[column, value], ...rest] = keys;
  return rest.length
    ? `(${column} > ${value} OR (${column} = ${value} AND ${keysetAfter(rest)}))`
    : `${column} > ${value}`;
}

const ORDER_KEYS = {
  asn: "asn, bucket, ip",
  location: "country_iso_code, subdivision_iso_code, city_name, bucket, ip",
} as const;

function emptyToNull(value: string): string | null {
  return value === "" ? null : value;
}

export async function listWorkspaceIpAddresses(
  filters: WorkspaceIpFilters,
  after = "",
) {
  if (
    filters.search &&
    (!/^[0-9a-f:.]+$/.test(filters.search) || filters.search.length > 45)
  ) {
    return {
      rows: [] as WorkspaceIpAddress[],
      hasMore: false,
      next: "",
      after: "",
    };
  }
  const order = workspaceIpListOrder(filters);
  const cursor = decodeCursor(after, order);
  const { conditions, params } = workspaceIpConditions(filters);
  params.limit = PAGE_SIZE + 1;
  if (cursor?.order === "asn") {
    conditions.push(
      keysetAfter([
        ["asn", "{afterAsn:UInt32}"],
        ["bucket", "{afterBucket:UInt16}"],
        ["ip", "{afterIp:String}"],
      ]),
    );
    [params.afterAsn, params.afterBucket, params.afterIp] = cursor.key;
  } else if (cursor?.order === "location") {
    conditions.push(
      keysetAfter([
        ["country_iso_code", "{afterCountry:String}"],
        ["subdivision_iso_code", "{afterRegion:String}"],
        ["city_name", "{afterCity:String}"],
        ["bucket", "{afterBucket:UInt16}"],
        ["ip", "{afterIp:String}"],
      ]),
    );
    [
      params.afterCountry,
      params.afterRegion,
      params.afterCity,
      params.afterBucket,
      params.afterIp,
    ] = cursor.key;
  }
  // Keyset over a key order ClickHouse reads in order: the sort key
  // (asn, bucket, ip), or the by_location projection for location filters.
  const rows = await chQuery<SearchRow>(
    `SELECT bucket, ip_version, ip, toString(first_seen) AS first_seen,
       toString(last_seen) AS last_seen, asn, asn_organization, country_iso_code,
       subdivision_iso_code, city_name, rdap_matched_cidr, rdap_name
     FROM ${IP_SEARCH_RELATION}
     ${conditions.length ? `WHERE ${conditions.join(" AND ")}` : ""}
     ORDER BY ${ORDER_KEYS[order]} LIMIT {limit:UInt32} ${QUERY_SETTINGS}`,
    params,
  );
  const page = rows.slice(0, PAGE_SIZE);
  return {
    rows: page.map(
      (row): WorkspaceIpAddress => ({
        bucket: row.bucket,
        ip_version: row.ip_version,
        ip: row.ip,
        first_seen: row.first_seen,
        last_seen: row.last_seen,
        country_iso_code: emptyToNull(row.country_iso_code),
        city_name: emptyToNull(row.city_name),
        asn: row.asn === 0 ? null : row.asn,
        asn_organization: emptyToNull(row.asn_organization),
        rdap_matched_cidr: emptyToNull(row.rdap_matched_cidr),
        rdap_name: emptyToNull(row.rdap_name),
      }),
    ),
    hasMore: rows.length > PAGE_SIZE,
    next: page.length ? encodeCursor(order, page[page.length - 1]) : "",
    after: cursor ? after : "",
  };
}
