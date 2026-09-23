import { isIP } from "node:net";
import { chQuery } from "~/lib/clickhouse.server";
import type { WorkspaceIpFilters } from "~/lib/workspace-ip-addresses";

interface IpKey {
  bucket: number;
  ip_version: 4 | 6;
  ip: string;
}

interface IpObservation extends IpKey {
  first_seen: string;
  last_seen: string;
}

interface Geoip {
  ip: string;
  country_iso_code: string | null;
  city_name: string | null;
  asn: number | null;
  asn_organization: string | null;
}

interface Enrichment extends Geoip {
  city_data_status: string;
  asn_data_status: string;
  rdap_matched_cidr: string | null;
  rdap_name: string | null;
}

export interface WorkspaceIpAddress extends IpObservation, Geoip {
  rdap_matched_cidr: string | null;
  rdap_name: string | null;
}

const PAGE_SIZE = 50;
const QUERY_SETTINGS = "SETTINGS max_threads=4, max_execution_time=20";

export interface WorkspaceIpStatistics {
  total: number;
  ipv4: number;
  ipv6: number;
  countedAt: string;
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
    let nextBucket = 0;
    let ipv4 = 0;
    let ipv6 = 0;
    // FINAL collapses repeated observations. Counting bounded, disjoint hash
    // buckets avoids one huge merge and caps database concurrency at four reads.
    const workers = await Promise.allSettled(
      Array.from({ length: 4 }, async () => {
        while (nextBucket < 256) {
          const bucket = nextBucket++;
          const counts = await chQuery<{
            ip_version: number;
            addresses: string;
          }>(
            `SELECT ip_version, toString(count()) AS addresses
           FROM corpscout.commoncrawl_ip_addresses FINAL WHERE bucket = {bucket:UInt16}
           GROUP BY ip_version SETTINGS max_threads=1, max_execution_time=20`,
            { bucket },
          ).catch((error) => {
            nextBucket = 256;
            throw error;
          });
          for (const row of counts) {
            if (row.ip_version === 4) ipv4 += Number(row.addresses);
            if (row.ip_version === 6) ipv6 += Number(row.addresses);
          }
        }
      }),
    );
    const failure = workers.find((worker) => worker.status === "rejected");
    if (failure?.status === "rejected") throw failure.reason;
    statisticsCache = {
      total: ipv4 + ipv6,
      ipv4,
      ipv6,
      countedAt: new Date().toISOString(),
    };
    return statisticsCache;
  })();
  try {
    return await statisticsPending;
  } finally {
    statisticsPending = undefined;
  }
}

function decodeCursor(after: string): IpKey | null {
  if (!after || after.length > 256) return null;
  try {
    const key = JSON.parse(Buffer.from(after, "base64url").toString());
    if (
      key &&
      Number.isInteger(key.bucket) &&
      key.bucket >= 0 &&
      key.bucket < 256 &&
      (key.ip_version === 4 || key.ip_version === 6) &&
      typeof key.ip === "string" &&
      isIP(key.ip) === key.ip_version
    )
      return { bucket: key.bucket, ip_version: key.ip_version, ip: key.ip };
  } catch {
    // A stale or malformed cursor starts at the first page.
  }
  return null;
}

export async function listWorkspaceIpAddresses(
  filters: WorkspaceIpFilters,
  after = "",
) {
  const cursor = decodeCursor(after);
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
  const conditions: string[] = [];
  const params: Record<string, unknown> = {
    limit: PAGE_SIZE + 1,
    search: filters.search,
  };
  if (cursor) {
    conditions.push(
      "(bucket, ip_version, ip) > ({bucket:UInt16}, {cursorVersion:UInt8}, {afterIp:String})",
    );
    Object.assign(params, {
      bucket: cursor.bucket,
      cursorVersion: cursor.ip_version,
      afterIp: cursor.ip,
    });
  }
  if (filters.version !== "any") {
    conditions.push("ip_version = {version:UInt8}");
    params.version = Number(filters.version);
  }
  if (filters.search) {
    const version = isIP(filters.search);
    if (version) {
      // Canonicalize IPv6 (including mapped IPv4) exactly as DNS ingestion does.
      const canonical =
        version === 4
          ? "toString(toIPv4({search:String}))"
          : "toString(toIPv6({search:String}))";
      conditions.push(
        `bucket = toUInt16(modulo(cityHash64(${canonical}), 256))`,
        `ip = ${canonical}`,
      );
    } else {
      conditions.push(
        "ip_version IN (4, 6)",
        "ip >= {search:String}",
        "ip < {prefixEnd:String}",
      );
      // Search text is ASCII; this exclusive upper bound describes precisely
      // the same prefix and gives ClickHouse an indexable string interval.
      params.prefixEnd =
        filters.search.slice(0, -1) +
        String.fromCharCode(
          filters.search.charCodeAt(filters.search.length - 1) + 1,
        );
    }
  }

  // Page over distinct keys first. FINAL or a global min/max aggregation would
  // merge tens of millions of observations before it could apply LIMIT.
  const selectKeys = (extraConditions: string[] = []) => {
    const where = [...conditions, ...extraConditions];
    return `SELECT DISTINCT bucket, ip_version, ip FROM corpscout.commoncrawl_ip_addresses
      ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
      ORDER BY bucket, ip_version, ip LIMIT {limit:UInt32}`;
  };
  const keySettings = `${QUERY_SETTINGS}, optimize_read_in_order=1, optimize_distinct_in_order=1`;
  let keys: IpKey[] = [];
  if (filters.search && !isIP(filters.search)) {
    // Prefixes cannot constrain the hash that leads the primary key. Explicit
    // bucket equality lets each branch prune its index; an unrestricted prefix
    // search otherwise reads the full inventory before producing the first page.
    for (
      let bucket = cursor?.bucket ?? 0;
      bucket < 256 && keys.length < PAGE_SIZE + 1;
      bucket += 8
    ) {
      const branches = Array.from(
        { length: Math.min(8, 256 - bucket) },
        (_, i) => `(${selectKeys([`bucket = ${bucket + i}`])})`,
      );
      const batch = await chQuery<IpKey>(
        `SELECT bucket, ip_version, ip FROM (${branches.join(" UNION ALL ")})
         ORDER BY bucket, ip_version, ip LIMIT {limit:UInt32} ${keySettings}`,
        { ...params, limit: PAGE_SIZE + 1 - keys.length },
      );
      keys.push(...batch);
    }
  } else {
    keys = await chQuery<IpKey>(`${selectKeys()} ${keySettings}`, params);
  }
  const page = keys.slice(0, PAGE_SIZE);
  if (!page.length)
    return {
      rows: [] as WorkspaceIpAddress[],
      hasMore: false,
      next: "",
      after: cursor ? after : "",
    };
  const pageParams = {
    buckets: page.map((row) => row.bucket),
    versions: page.map((row) => row.ip_version),
    ips: page.map((row) => row.ip),
  };
  const [observations, legacy, enrichment] = await Promise.all([
    chQuery<IpObservation>(
      `SELECT bucket, ip_version, ip, toString(min(first_seen)) AS first_seen,
       toString(max(last_seen)) AS last_seen FROM corpscout.commoncrawl_ip_addresses
       WHERE (bucket, ip_version, ip) IN arrayZip({buckets:Array(UInt16)}, {versions:Array(UInt8)}, {ips:Array(String)})
       GROUP BY bucket, ip_version, ip ${QUERY_SETTINGS}`,
      pageParams,
    ),
    chQuery<Geoip>(
      `SELECT ip, country_iso_code, city_name, asn, asn_organization
       FROM corpscout.commoncrawl_ip_geoip FINAL
       WHERE (bucket, ip) IN arrayZip({buckets:Array(UInt16)}, {ips:Array(String)})
       ${QUERY_SETTINGS}`,
      pageParams,
    ),
    chQuery<Enrichment>(
      `SELECT ip, country_iso_code, city_name, asn, asn_organization,
       city_data_status, asn_data_status, rdap_matched_cidr, rdap_name
       FROM corpscout.ip_enrichment_current
       WHERE (bucket, ip) IN arrayZip({buckets:Array(UInt16)}, {ips:Array(String)})
       ${QUERY_SETTINGS}`,
      pageParams,
    ),
  ]);
  const observationMap = new Map(observations.map((row) => [row.ip, row]));
  const legacyMap = new Map(legacy.map((row) => [row.ip, row]));
  const enrichmentMap = new Map(enrichment.map((row) => [row.ip, row]));
  const conclusive = new Set(["found", "not_found", "not_global"]);
  const rows = page.map((key): WorkspaceIpAddress => {
    const current = enrichmentMap.get(key.ip);
    const fallback = legacyMap.get(key.ip);
    // A conclusive negative clears older data; transient failures may retain it.
    const city =
      current && conclusive.has(current.city_data_status) ? current : fallback;
    const asn =
      current && conclusive.has(current.asn_data_status) ? current : fallback;
    return {
      ...key,
      first_seen: observationMap.get(key.ip)?.first_seen ?? "",
      last_seen: observationMap.get(key.ip)?.last_seen ?? "",
      country_iso_code: city?.country_iso_code ?? null,
      city_name: city?.city_name ?? null,
      asn: asn?.asn ?? null,
      asn_organization: asn?.asn_organization ?? null,
      rdap_matched_cidr: current?.rdap_matched_cidr ?? null,
      rdap_name: current?.rdap_name ?? null,
    };
  });
  return {
    rows,
    hasMore: keys.length > PAGE_SIZE,
    next: Buffer.from(JSON.stringify(page.at(-1))).toString("base64url"),
    after: cursor ? after : "",
  };
}
