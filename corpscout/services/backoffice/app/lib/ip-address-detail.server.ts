import { isIP } from "node:net";
import { chQuery } from "~/lib/clickhouse.server";
import {
  DNS_HISTORY_MAX_HOSTNAMES,
  IP_DETAIL_PAGE_SIZE,
  type IpDomainScope,
} from "~/lib/ip-address-detail";

/**
 * Server reads for the backoffice IP address detail page (`/admin/ip-addresses/:address`).
 * Every read of commoncrawl_ip_addresses, ip_enrichment_current and rdap_ip_lookup_results
 * filters on the 256-way address bucket, every read of commoncrawl_domain_ip_connections
 * PREWHEREs on the 64-way segment bucket and segment CIDR and pages by keyset (no OFFSET, no
 * count), and commoncrawl_domain_dns_records is read only on demand for ONE root domain
 * (`root_domain =`, its partition and sort key), never by value alone.
 */

export interface ResolvedIpAddress {
  ip: string;
  version: 4 | 6;
  /** toUInt16(cityHash64(ip) % 256), the key prefix of the per-address tables. */
  bucket: number;
  /** The stable /24 (IPv4) or /48 (IPv6) neighbourhood. */
  networkSegment: string;
}

export type LookupStatus =
  | "not_attempted"
  | "found"
  | "not_found"
  | "not_global"
  | "retryable_error"
  | "terminal_error";

export interface IpAddressHeader {
  address: ResolvedIpAddress;
  firstSeen: string | null;
  lastSeen: string | null;
  /** Null when the address has no enrichment result yet. */
  statuses: { city: LookupStatus; asn: LookupStatus; rdap: LookupStatus } | null;
}

export interface IpComponentStatus {
  status: LookupStatus;
  checkedAt: string | null;
  errorCode: string | null;
  retryAfter: string | null;
  /** Status and time of the lookup the displayed data comes from. */
  dataStatus: LookupStatus;
  dataAt: string | null;
}

export interface IpEnrichmentOverview {
  resultId: string;
  taskId: string;
  completedAt: string;
  processorVersion: string;
  ipScope: string;
  city: IpComponentStatus;
  asnStatus: IpComponentStatus;
  rdapStatus: IpComponentStatus;
  geo: {
    continentCode: string | null;
    continentName: string | null;
    countryIsoCode: string | null;
    countryName: string | null;
    registeredCountryIsoCode: string | null;
    registeredCountryName: string | null;
    subdivisionIsoCodes: string[];
    subdivisionNames: string[];
    cityName: string | null;
    latitude: number | null;
    longitude: number | null;
    accuracyRadiusKm: number | null;
    timezone: string | null;
    cityNetwork: string | null;
    cityDbBuildDate: string | null;
  };
  asn: {
    asn: number | null;
    organization: string | null;
    network: string | null;
    dbBuildDate: string | null;
  };
  rdap: {
    networkKey: string | null;
    rir: string | null;
    handle: string | null;
    name: string | null;
    registrantNames: string[];
    matchedCidr: string | null;
    registrationType: string | null;
    statuses: string[];
    countryCode: string | null;
    registrationDate: string | null;
    lastChangedAt: string | null;
    selfUrl: string | null;
  };
}

export interface RdapIpLookupMarker {
  status: string;
  networkKey: string | null;
  errorCode: string | null;
  retryAfter: string | null;
  queriedAt: string;
}

export interface IpAddressOverview {
  address: ResolvedIpAddress;
  enrichment: IpEnrichmentOverview | null;
  rdapMarker: RdapIpLookupMarker | null;
}

export interface RdapNetworkRecord {
  networkKey: string;
  rir: string;
  handle: string;
  ipVersion: number;
  startAddress: string;
  endAddress: string;
  name: string | null;
  registrationType: string | null;
  countryCode: string | null;
  statuses: string[];
  registrantHandles: string[];
  registrantNames: string[];
  parentNetworkKey: string | null;
  parentHandle: string | null;
  selfUrl: string | null;
  upUrl: string | null;
  registrationDate: string | null;
  lastChangedAt: string | null;
  fetchedAt: string;
  source: string;
}

export interface RdapRegistryClass {
  registryClass: string;
  coveredRirBlocks: number;
  ianaDesignation: string;
  ianaRir: string;
  ianaStatus: string;
  specialRegistry: string;
  specialStatus: string;
  classifiedAt: string;
}

export interface RdapNetworkSegment {
  cidr: string;
  prefixLength: number;
  segmentRole: string;
  derivedAt: string;
}

export interface IpAddressRegistration {
  address: ResolvedIpAddress;
  /** Where the network key came from: the enrichment result or the RDAP trie. */
  keySource: "enrichment" | "trie" | null;
  matchedCidr: string | null;
  network: (RdapNetworkRecord & { rawResponse: string }) | null;
  registryClass: RdapRegistryClass | null;
  parent: RdapNetworkRecord | null;
  segments: RdapNetworkSegment[];
}

export interface IpDomainConnection {
  ip: string;
  version: number;
  domain: string;
  hostnames: string[];
  sources: string[];
  discoveries: string[];
  firstSeen: string;
  lastSeen: string;
}

export interface IpHistoryCoverage {
  completedPartitions: number;
  totalPartitions: number;
}

/** Keyset paging state shared by the DNS and Domains tabs. */
export interface IpKeysetPage {
  /** The cursor this page starts after ("" on the first page). */
  after: string;
  /** The cursor of the next page, or null on the last page. */
  next: string | null;
  pageSize: number;
  /** Exact only when everything fits on the first page; otherwise unknown ("50+"). */
  total: number | null;
}

export interface IpAddressDomains extends IpKeysetPage {
  address: ResolvedIpAddress;
  scope: IpDomainScope;
  connections: IpDomainConnection[];
  coverage: IpHistoryCoverage;
}

/** One root domain with the hostnames whose A/AAAA record points at the address. */
export interface IpDnsDomainGroup {
  rootDomain: string;
  type: "A" | "AAAA";
  hostnames: string[];
  sources: string[];
  discoveries: string[];
  seenDates: number;
  firstSeen: string;
  lastSeen: string;
}

export interface IpAddressDnsRecords extends IpKeysetPage {
  address: ResolvedIpAddress;
  domains: IpDnsDomainGroup[];
  coverage: IpHistoryCoverage;
}

/** One record-level row from commoncrawl_domain_dns_records (on-demand history). */
export interface IpDnsRecord {
  hostname: string;
  type: string;
  value: string;
  sources: string[];
  discoveries: string[];
  seenDates: number;
  firstSeen: string;
  lastSeen: string;
}

// One statement per family so no conversion runs on the other family's text.
const RESOLVE_SQL = {
  4: `SELECT
  ip,
  toUInt16(cityHash64(ip) % 256) AS bucket,
  concat(toString(tupleElement(IPv4CIDRToRange(toIPv4(ip), 24), 1)), '/24') AS network_segment
FROM (SELECT toString(toIPv4OrNull({raw:String})) AS ip)
WHERE ip IS NOT NULL`,
  6: `SELECT
  ip,
  toUInt16(cityHash64(ip) % 256) AS bucket,
  concat(toString(tupleElement(IPv6CIDRToRange(toIPv6(ip), 48), 1)), '/48') AS network_segment
FROM (SELECT toString(toIPv6OrNull({raw:String})) AS ip)
WHERE ip IS NOT NULL`,
} as const;

/**
 * Canonicalises an address the way ClickHouse stores it (toString(toIPv4/toIPv6(...))) and
 * derives its bucket and network segment. Returns null for anything that is not an IP.
 */
export async function resolveIpAddress(
  raw: string,
): Promise<ResolvedIpAddress | null> {
  const candidate = raw.trim();
  if (!candidate || candidate.length > 45) return null;
  const version = isIP(candidate);
  if (version !== 4 && version !== 6) return null;
  const rows = await chQuery<{
    ip: string;
    bucket: number;
    network_segment: string;
  }>(RESOLVE_SQL[version], { raw: candidate });
  const row = rows[0];
  if (!row?.ip) return null;
  return {
    ip: row.ip,
    version,
    bucket: Number(row.bucket),
    networkSegment: row.network_segment,
  };
}

/**
 * For child loaders: 404 on an invalid address, null when the address is valid but not in its
 * canonical form (the parent loader redirects; the child skips its reads).
 */
export async function resolveCanonicalIpAddress(
  raw: string,
): Promise<ResolvedIpAddress | null> {
  const address = await resolveIpAddress(raw);
  if (!address) throw new Response("Not found", { status: 404 });
  return address.ip === raw ? address : null;
}

function addressParams(address: ResolvedIpAddress) {
  return { bucket: address.bucket, ip: address.ip, version: address.version };
}

// Point reads (one bucket + ip key) prefetch their few granules in parallel; the in-order page
// reads over commoncrawl_domain_ip_connections keep the default settings.
const POINT_READ_SETTINGS =
  "allow_prefetched_read_pool_for_local_filesystem=1, local_filesystem_read_prefetch=1, max_threads=16";

const HEADER_OBSERVATION_SQL = `SELECT
  toString(min(first_seen)) AS first_seen,
  toString(max(last_seen)) AS last_seen
FROM commoncrawl_ip_addresses
WHERE bucket = {bucket:UInt16}
  AND ip_version = {version:UInt8}
  AND ip = {ip:String}
HAVING count() > 0
SETTINGS ${POINT_READ_SETTINGS}`;

export async function getIpAddressHeader(
  address: ResolvedIpAddress,
): Promise<IpAddressHeader> {
  const [observations, enrichment] = await Promise.all([
    chQuery<{ first_seen: string; last_seen: string }>(
      HEADER_OBSERVATION_SQL,
      addressParams(address),
    ),
    loadEnrichmentRow(address),
  ]);
  return {
    address,
    firstSeen: observations[0]?.first_seen ?? null,
    lastSeen: observations[0]?.last_seen ?? null,
    statuses: enrichment
      ? {
          city: enrichment.city_lookup_status,
          asn: enrichment.asn_lookup_status,
          rdap: enrichment.rdap_lookup_status,
        }
      : null,
  };
}

const OVERVIEW_SQL = `SELECT
  toString(result_id) AS result_id,
  toString(task_id) AS task_id,
  toString(completed_at) AS completed_at,
  toString(processor_version) AS processor_version,
  toString(ip_scope) AS ip_scope,
  toString(city_lookup_status) AS city_lookup_status,
  toString(city_checked_at) AS city_checked_at,
  city_error_code,
  toString(city_retry_after) AS city_retry_after,
  toString(city_data_status) AS city_data_status,
  toString(city_data_at) AS city_data_at,
  toString(asn_lookup_status) AS asn_lookup_status,
  toString(asn_checked_at) AS asn_checked_at,
  asn_error_code,
  toString(asn_retry_after) AS asn_retry_after,
  toString(asn_data_status) AS asn_data_status,
  toString(asn_data_at) AS asn_data_at,
  toString(rdap_lookup_status) AS rdap_lookup_status,
  toString(rdap_checked_at) AS rdap_checked_at,
  rdap_error_code,
  toString(rdap_retry_after) AS rdap_retry_after,
  toString(rdap_data_status) AS rdap_data_status,
  toString(rdap_data_at) AS rdap_data_at,
  continent_code,
  continent_name,
  country_iso_code,
  country_name,
  registered_country_iso_code,
  registered_country_name,
  subdivision_iso_codes,
  subdivision_names,
  city_name,
  latitude,
  longitude,
  accuracy_radius_km,
  timezone,
  city_network,
  toString(toDate(city_db_build_epoch)) AS city_db_build_date,
  asn,
  asn_organization,
  asn_network,
  toString(toDate(asn_db_build_epoch)) AS asn_db_build_date,
  rdap_network_key,
  rdap_rir,
  rdap_handle,
  rdap_name,
  rdap_registrant_names,
  rdap_matched_cidr,
  rdap_registration_type,
  rdap_statuses,
  rdap_country_code,
  toString(rdap_registration_date) AS rdap_registration_date,
  toString(rdap_last_changed_at) AS rdap_last_changed_at,
  rdap_self_url,
  rdap_parent_network_key
FROM ip_enrichment_current
WHERE bucket = {bucket:UInt16}
  AND ip = {ip:String}
LIMIT 1
SETTINGS optimize_move_to_prewhere_if_final=1, ${POINT_READ_SETTINGS}`;

const RDAP_MARKER_SQL = `SELECT
  toString(lookup_status) AS lookup_status,
  network_key,
  error_code,
  toString(retry_after) AS retry_after,
  toString(queried_at) AS queried_at
FROM rdap_ip_lookup_results_current
WHERE bucket = {bucket:UInt16}
  AND ip_version = {version:UInt8}
  AND ip = {ip:String}
LIMIT 1
SETTINGS ${POINT_READ_SETTINGS}`;

type Nullable<T> = T | null;

export interface OverviewRow {
  result_id: string;
  task_id: string;
  completed_at: string;
  processor_version: string;
  ip_scope: string;
  city_lookup_status: LookupStatus;
  city_checked_at: Nullable<string>;
  city_error_code: Nullable<string>;
  city_retry_after: Nullable<string>;
  city_data_status: LookupStatus;
  city_data_at: Nullable<string>;
  asn_lookup_status: LookupStatus;
  asn_checked_at: Nullable<string>;
  asn_error_code: Nullable<string>;
  asn_retry_after: Nullable<string>;
  asn_data_status: LookupStatus;
  asn_data_at: Nullable<string>;
  rdap_lookup_status: LookupStatus;
  rdap_checked_at: Nullable<string>;
  rdap_error_code: Nullable<string>;
  rdap_retry_after: Nullable<string>;
  rdap_data_status: LookupStatus;
  rdap_data_at: Nullable<string>;
  continent_code: Nullable<string>;
  continent_name: Nullable<string>;
  country_iso_code: Nullable<string>;
  country_name: Nullable<string>;
  registered_country_iso_code: Nullable<string>;
  registered_country_name: Nullable<string>;
  subdivision_iso_codes: string[];
  subdivision_names: string[];
  city_name: Nullable<string>;
  latitude: Nullable<number>;
  longitude: Nullable<number>;
  accuracy_radius_km: Nullable<number>;
  timezone: Nullable<string>;
  city_network: Nullable<string>;
  city_db_build_date: Nullable<string>;
  asn: Nullable<number>;
  asn_organization: Nullable<string>;
  asn_network: Nullable<string>;
  asn_db_build_date: Nullable<string>;
  rdap_network_key: Nullable<string>;
  rdap_rir: Nullable<string>;
  rdap_handle: Nullable<string>;
  rdap_name: Nullable<string>;
  rdap_registrant_names: string[];
  rdap_matched_cidr: Nullable<string>;
  rdap_registration_type: Nullable<string>;
  rdap_statuses: string[];
  rdap_country_code: Nullable<string>;
  rdap_registration_date: Nullable<string>;
  rdap_last_changed_at: Nullable<string>;
  rdap_self_url: Nullable<string>;
  rdap_parent_network_key: Nullable<string>;
}

/** Enrichment rows served within this window reuse one read (header + tab of one request). */
const ENRICHMENT_TTL_MS = 30_000;
const enrichmentCache = new Map<
  string,
  { expires: number; row: Promise<OverviewRow | null> }
>();

/**
 * The one ip_enrichment_current read behind the header, Overview and Registration. Parent and
 * child loaders of a request run concurrently, so the pending promise is shared, and a short
 * TTL keeps tab switches from re-reading the view.
 */
export function loadEnrichmentRow(
  address: ResolvedIpAddress,
): Promise<OverviewRow | null> {
  const key = `${address.bucket}:${address.ip}`;
  const now = Date.now();
  const cached = enrichmentCache.get(key);
  if (cached && cached.expires > now) return cached.row;
  for (const [entryKey, entry] of enrichmentCache) {
    if (entry.expires <= now) enrichmentCache.delete(entryKey);
  }
  const row = chQuery<OverviewRow>(OVERVIEW_SQL, addressParams(address)).then(
    (rows) => rows[0] ?? null,
  );
  enrichmentCache.set(key, { expires: now + ENRICHMENT_TTL_MS, row });
  row.catch(() => enrichmentCache.delete(key));
  return row;
}

/** Test hook: forget cached enrichment rows. */
export function clearEnrichmentCache(): void {
  enrichmentCache.clear();
}

function componentStatus(
  row: OverviewRow,
  prefix: "city" | "asn" | "rdap",
): IpComponentStatus {
  return {
    status: row[`${prefix}_lookup_status`],
    checkedAt: row[`${prefix}_checked_at`],
    errorCode: row[`${prefix}_error_code`],
    retryAfter: row[`${prefix}_retry_after`],
    dataStatus: row[`${prefix}_data_status`],
    dataAt: row[`${prefix}_data_at`],
  };
}

function numberOrNull(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function mapOverview(row: OverviewRow): IpEnrichmentOverview {
  return {
    resultId: row.result_id,
    taskId: row.task_id,
    completedAt: row.completed_at,
    processorVersion: row.processor_version,
    ipScope: row.ip_scope,
    city: componentStatus(row, "city"),
    asnStatus: componentStatus(row, "asn"),
    rdapStatus: componentStatus(row, "rdap"),
    geo: {
      continentCode: row.continent_code,
      continentName: row.continent_name,
      countryIsoCode: row.country_iso_code,
      countryName: row.country_name,
      registeredCountryIsoCode: row.registered_country_iso_code,
      registeredCountryName: row.registered_country_name,
      subdivisionIsoCodes: row.subdivision_iso_codes ?? [],
      subdivisionNames: row.subdivision_names ?? [],
      cityName: row.city_name,
      latitude: numberOrNull(row.latitude),
      longitude: numberOrNull(row.longitude),
      accuracyRadiusKm: numberOrNull(row.accuracy_radius_km),
      timezone: row.timezone,
      cityNetwork: row.city_network,
      cityDbBuildDate: row.city_db_build_date,
    },
    asn: {
      asn: numberOrNull(row.asn),
      organization: row.asn_organization,
      network: row.asn_network,
      dbBuildDate: row.asn_db_build_date,
    },
    rdap: {
      networkKey: row.rdap_network_key,
      rir: row.rdap_rir,
      handle: row.rdap_handle,
      name: row.rdap_name,
      registrantNames: row.rdap_registrant_names ?? [],
      matchedCidr: row.rdap_matched_cidr,
      registrationType: row.rdap_registration_type,
      statuses: row.rdap_statuses ?? [],
      countryCode: row.rdap_country_code,
      registrationDate: row.rdap_registration_date,
      lastChangedAt: row.rdap_last_changed_at,
      selfUrl: row.rdap_self_url,
    },
  };
}

export async function getIpAddressOverview(
  address: ResolvedIpAddress,
): Promise<IpAddressOverview> {
  const params = addressParams(address);
  const [enrichment, markerRows] = await Promise.all([
    loadEnrichmentRow(address),
    chQuery<{
      lookup_status: string;
      network_key: string | null;
      error_code: string | null;
      retry_after: string | null;
      queried_at: string;
    }>(RDAP_MARKER_SQL, params),
  ]);
  const marker = markerRows[0];
  return {
    address,
    enrichment: enrichment ? mapOverview(enrichment) : null,
    rdapMarker: marker
      ? {
          status: marker.lookup_status,
          networkKey: marker.network_key,
          errorCode: marker.error_code,
          retryAfter: marker.retry_after,
          queriedAt: marker.queried_at,
        }
      : null,
  };
}

// Same longest-prefix read as technologyIpRdapSql in queries.server.ts, for one address.
const TRIE_KEY_SQL_V4 = `SELECT
  dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv4({ip:String})), '') AS network_key,
  dictGetOrDefault('corpscout.rdap_network_trie', 'matched_cidr', tuple(toIPv4({ip:String})), '') AS matched_cidr`;

const TRIE_KEY_SQL_V6 = `SELECT
  dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv6({ip:String})), '') AS network_key,
  dictGetOrDefault('corpscout.rdap_network_trie', 'matched_cidr', tuple(toIPv6({ip:String})), '') AS matched_cidr`;

const NETWORK_COLUMNS = `network_key,
  toString(rir) AS rir,
  handle,
  ip_version,
  start_address,
  end_address,
  name,
  registration_type,
  country_code,
  status AS statuses,
  registrant_handles,
  registrant_names,
  parent_network_key,
  parent_handle,
  self_url,
  up_url,
  toString(registration_date) AS registration_date,
  toString(last_changed_at) AS last_changed_at,
  toString(fetched_at) AS fetched_at`;

const NETWORK_SQL = `SELECT
  ${NETWORK_COLUMNS},
  JSONExtractString(raw_response, 'corpscout', 'source') AS source,
  raw_response
FROM rdap_networks_current
WHERE network_key = {networkKey:String}
LIMIT 1`;

const PARENT_NETWORK_SQL = `SELECT
  ${NETWORK_COLUMNS}
FROM rdap_networks_current
WHERE network_key = {networkKey:String}
LIMIT 1`;

const REGISTRY_CLASS_SQL = `SELECT
  toString(registry_class) AS registry_class,
  covered_rir_blocks,
  iana_designation,
  toString(iana_rir) AS iana_rir,
  toString(iana_status) AS iana_status,
  toString(special_registry) AS special_registry,
  toString(special_status) AS special_status,
  toString(classified_at) AS classified_at
FROM rdap_network_registry_class_current
WHERE network_key = {networkKey:String}
LIMIT 1`;

// rdap_network_segments_current aggregates every CIDR, so a per-network read goes to the
// table's (network_key, cidr) sort key instead and keeps all roles visible.
const SEGMENTS_SQL = `SELECT
  cidr,
  prefix_length,
  toString(segment_role) AS segment_role,
  toString(derived_at) AS derived_at
FROM rdap_network_segments FINAL
WHERE network_key = {networkKey:String}
ORDER BY segment_role, prefix_length, cidr
LIMIT 500`;

interface NetworkRow {
  network_key: string;
  rir: string;
  handle: string;
  ip_version: number;
  start_address: string;
  end_address: string;
  name: string | null;
  registration_type: string | null;
  country_code: string | null;
  statuses: string[];
  registrant_handles: string[];
  registrant_names: string[];
  parent_network_key: string | null;
  parent_handle: string | null;
  self_url: string | null;
  up_url: string | null;
  registration_date: string | null;
  last_changed_at: string | null;
  fetched_at: string;
  source?: string;
  raw_response?: string;
}

function mapNetwork(row: NetworkRow): RdapNetworkRecord {
  return {
    networkKey: row.network_key,
    rir: row.rir,
    handle: row.handle,
    ipVersion: Number(row.ip_version),
    startAddress: row.start_address,
    endAddress: row.end_address,
    name: row.name,
    registrationType: row.registration_type,
    countryCode: row.country_code,
    statuses: row.statuses ?? [],
    registrantHandles: row.registrant_handles ?? [],
    registrantNames: row.registrant_names ?? [],
    parentNetworkKey: row.parent_network_key,
    parentHandle: row.parent_handle,
    selfUrl: row.self_url,
    upUrl: row.up_url,
    registrationDate: row.registration_date,
    lastChangedAt: row.last_changed_at,
    fetchedAt: row.fetched_at,
    source: row.source ?? "",
  };
}

function prettyJson(raw: string): string {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}

type RegistryClassRow = {
  registry_class: string;
  covered_rir_blocks: number;
  iana_designation: string;
  iana_rir: string;
  iana_status: string;
  special_registry: string;
  special_status: string;
  classified_at: string;
};

type SegmentRow = {
  cidr: string;
  prefix_length: number;
  segment_role: string;
  derived_at: string;
};

export async function getIpAddressRegistration(
  address: ResolvedIpAddress,
): Promise<IpAddressRegistration> {
  // The network and parent keys come from the shared enrichment row (no extra read).
  const enrichment = await loadEnrichmentRow(address);
  let networkKey = enrichment?.rdap_network_key || null;
  let matchedCidr = enrichment?.rdap_matched_cidr || null;
  const enrichmentParentKey = networkKey
    ? enrichment?.rdap_parent_network_key || null
    : null;
  let keySource: IpAddressRegistration["keySource"] = networkKey
    ? "enrichment"
    : null;
  if (!networkKey) {
    const trieRows = await chQuery<{
      network_key: string;
      matched_cidr: string;
    }>(address.version === 4 ? TRIE_KEY_SQL_V4 : TRIE_KEY_SQL_V6, {
      ip: address.ip,
    });
    const trie = trieRows[0];
    if (trie?.network_key && !["0.0.0.0/0", "::/0"].includes(trie.matched_cidr)) {
      networkKey = trie.network_key;
      matchedCidr = trie.matched_cidr;
      keySource = "trie";
    }
  }
  if (!networkKey) {
    return {
      address,
      keySource: null,
      matchedCidr: null,
      network: null,
      registryClass: null,
      parent: null,
      segments: [],
    };
  }

  // One parallel batch: network, parent, registry class and segments.
  const [networkRows, parentBatchRows, classRows, segmentRows] =
    await Promise.all([
      chQuery<NetworkRow>(NETWORK_SQL, { networkKey }),
      enrichmentParentKey
        ? chQuery<NetworkRow>(PARENT_NETWORK_SQL, {
            networkKey: enrichmentParentKey,
          })
        : Promise.resolve([] as NetworkRow[]),
      chQuery<RegistryClassRow>(REGISTRY_CLASS_SQL, { networkKey }),
      chQuery<SegmentRow>(SEGMENTS_SQL, { networkKey }),
    ]);
  const networkRow = networkRows[0];
  // A trie match carries no parent key until its network row is read.
  const trieParentKey =
    keySource === "trie" ? networkRow?.parent_network_key || null : null;
  const parentRows = trieParentKey
    ? await chQuery<NetworkRow>(PARENT_NETWORK_SQL, {
        networkKey: trieParentKey,
      })
    : parentBatchRows;
  const registryClass = classRows[0];

  return {
    address,
    keySource,
    matchedCidr,
    network: networkRow
      ? {
          ...mapNetwork(networkRow),
          rawResponse: prettyJson(networkRow.raw_response ?? ""),
        }
      : null,
    registryClass: registryClass
      ? {
          registryClass: registryClass.registry_class,
          coveredRirBlocks: Number(registryClass.covered_rir_blocks),
          ianaDesignation: registryClass.iana_designation,
          ianaRir: registryClass.iana_rir,
          ianaStatus: registryClass.iana_status,
          specialRegistry: registryClass.special_registry,
          specialStatus: registryClass.special_status,
          classifiedAt: registryClass.classified_at,
        }
      : null,
    parent: parentRows[0] ? mapNetwork(parentRows[0]) : null,
    segments: segmentRows.map((row) => ({
      cidr: row.cidr,
      prefixLength: Number(row.prefix_length),
      segmentRole: row.segment_role,
      derivedAt: row.derived_at,
    })),
  };
}

const CONNECTION_PREWHERE = `segment_bucket = toUInt8(cityHash64({networkSegment:String}) % 64)
  AND segment_cidr = {networkSegment:String}
  AND ip_version = {version:UInt8}`;

// The exact-IP read of technologyExactIpConnectionsSql (queries.server.ts) with keyset paging
// on root_domain (the last sort-key column) instead of OFFSET and no count() OVER ().
const EXACT_CONNECTIONS_SQL = `SELECT
  ip,
  toUInt8(ip_version) AS version,
  root_domain AS domain,
  hostnames,
  sources,
  discoveries,
  toString(first_seen) AS first_seen,
  toString(last_seen) AS last_seen
FROM commoncrawl_domain_ip_connections FINAL
PREWHERE ${CONNECTION_PREWHERE}
  AND address = toIPv6({ip:String})
  AND root_domain > {after:String}
ORDER BY root_domain
LIMIT {limit:UInt32}`;

// The neighbourhood read of technologySegmentIpConnectionsSql, keyset on (address, root_domain).
const SEGMENT_CONNECTIONS_SQL = `SELECT
  ip,
  toUInt8(ip_version) AS version,
  root_domain AS domain,
  hostnames,
  sources,
  discoveries,
  toString(first_seen) AS first_seen,
  toString(last_seen) AS last_seen
FROM commoncrawl_domain_ip_connections FINAL
PREWHERE ${CONNECTION_PREWHERE}
  AND (address, root_domain) > (toIPv6({afterAddress:String}), {afterDomain:String})
WHERE address != toIPv6({ip:String})
ORDER BY address, root_domain
LIMIT {limit:UInt32}`;

// The DNS tab: hostnames pointing at the address, one row per hostname, read from the
// connections table only. Sources, discoveries, dates and the seen count are per root domain.
const DNS_HOSTNAMES_SQL = `SELECT
  root_domain,
  arrayJoin(arraySort(hostnames)) AS hostname,
  if(ip_version = 4, 'A', 'AAAA') AS type,
  arraySort(sources) AS sources,
  arraySort(discoveries) AS discoveries,
  length(seen_dates) AS seen_dates,
  toString(first_seen) AS first_seen,
  toString(last_seen) AS last_seen
FROM (
  SELECT ip_version, root_domain, hostnames, sources, discoveries, seen_dates, first_seen, last_seen
  FROM commoncrawl_domain_ip_connections FINAL
  PREWHERE ${CONNECTION_PREWHERE}
    AND address = toIPv6({ip:String})
    AND root_domain > {after:String}
  ORDER BY root_domain
  LIMIT {limit:UInt32}
)
ORDER BY root_domain, hostname`;

const COVERAGE_SQL = `SELECT
  toString(count()) AS completed_partitions
FROM commoncrawl_domain_ip_backfill_status FINAL
WHERE bucket < 16`;

// Record-level history for ONE root domain, loaded when its row is expanded. root_domain leads
// the sort and partition key of commoncrawl_domain_dns_records and is always an equality.
const DNS_RECORD_HISTORY_SQL = `SELECT
  name AS hostname,
  toString(record_type) AS type,
  toString(value) AS value,
  arraySort(arrayDistinct(arrayFlatten(groupArray(sources)))) AS sources,
  arraySort(arrayDistinct(arrayFlatten(groupArray(discoveries)))) AS discoveries,
  length(arrayDistinct(arrayFlatten(groupArray(seen_dates)))) AS seen_dates,
  toString(min(first_seen)) AS first_seen,
  toString(max(last_seen)) AS last_seen
FROM commoncrawl_domain_dns_records
WHERE root_domain = {rootDomain:String}
  AND name IN {hostnames:Array(String)}
  AND record_type_code = {code:UInt16}
  AND if(
    {code:UInt16} = 1,
    toIPv6(toString(assumeNotNull(toIPv4OrNull(value)))),
    assumeNotNull(toIPv6OrNull(value))
  ) = toIPv6({ip:String})
GROUP BY name, record_type, value
ORDER BY hostname, value`;

// Same, for a root domain whose hostname list is too long to pass (root_domain = only).
const DNS_RECORD_HISTORY_ALL_NAMES_SQL = DNS_RECORD_HISTORY_SQL.replace(
  "  AND name IN {hostnames:Array(String)}\n",
  "",
);

interface ConnectionRow {
  ip: string;
  version: number;
  domain: string;
  hostnames: string[];
  sources: string[];
  discoveries: string[];
  first_seen: string;
  last_seen: string;
}

function mapConnection(row: ConnectionRow): IpDomainConnection {
  return {
    ip: row.ip,
    version: Number(row.version),
    domain: row.domain,
    hostnames: [...(row.hostnames ?? [])].sort(),
    sources: [...(row.sources ?? [])].sort(),
    discoveries: [...(row.discoveries ?? [])].sort(),
    firstSeen: row.first_seen,
    lastSeen: row.last_seen,
  };
}

/**
 * Trims a limit+1 read to one page. hasMore only when the extra row exists, so a page of
 * exactly pageSize rows is the last one.
 */
export function keysetPage<T>(
  rows: T[],
  pageSize: number,
  after: string,
  cursorOf: (row: T) => string,
): { rows: T[] } & Omit<IpKeysetPage, "pageSize"> {
  const hasMore = rows.length > pageSize;
  const pageRows = rows.slice(0, pageSize);
  return {
    rows: pageRows,
    after,
    next: hasMore && pageRows.length ? cursorOf(pageRows[pageRows.length - 1]) : null,
    total: !after && !hasMore ? pageRows.length : null,
  };
}

/** Segment cursors are "<ip>|<root domain>"; anything malformed starts at the first page. */
function parseSegmentCursor(after: string): { address: string; domain: string } | null {
  const split = after.indexOf("|");
  if (split <= 0) return null;
  const address = after.slice(0, split);
  if (!isIP(address)) return null;
  return { address, domain: after.slice(split + 1) };
}

async function loadCoverage(): Promise<IpHistoryCoverage> {
  const rows = await chQuery<{ completed_partitions: string }>(COVERAGE_SQL);
  return {
    completedPartitions: Number(rows[0]?.completed_partitions ?? 0),
    totalPartitions: 16,
  };
}

function connectionParams(address: ResolvedIpAddress) {
  return {
    ip: address.ip,
    version: address.version,
    networkSegment: address.networkSegment,
  };
}

export async function getIpAddressDomains(
  address: ResolvedIpAddress,
  opts: { after?: string; scope?: IpDomainScope } = {},
): Promise<IpAddressDomains> {
  const scope: IpDomainScope = opts.scope === "segment" ? "segment" : "exact";
  let after = (opts.after ?? "").slice(0, 600);
  const segmentCursor = scope === "segment" ? parseSegmentCursor(after) : null;
  if (scope === "segment" && !segmentCursor) after = "";
  const read =
    scope === "segment"
      ? chQuery<ConnectionRow>(SEGMENT_CONNECTIONS_SQL, {
          ...connectionParams(address),
          afterAddress: segmentCursor?.address ?? "::",
          afterDomain: segmentCursor?.domain ?? "",
          limit: IP_DETAIL_PAGE_SIZE + 1,
        })
      : chQuery<ConnectionRow>(EXACT_CONNECTIONS_SQL, {
          ...connectionParams(address),
          after,
          limit: IP_DETAIL_PAGE_SIZE + 1,
        });
  const [rows, coverage] = await Promise.all([read, loadCoverage()]);
  const page = keysetPage(rows, IP_DETAIL_PAGE_SIZE, after, (row) =>
    scope === "segment" ? `${row.ip}|${row.domain}` : row.domain,
  );
  return {
    address,
    scope,
    after: page.after,
    next: page.next,
    total: page.total,
    pageSize: IP_DETAIL_PAGE_SIZE,
    connections: page.rows.map(mapConnection),
    coverage,
  };
}

interface DnsHostnameRow {
  root_domain: string;
  hostname: string;
  type: "A" | "AAAA";
  sources: string[];
  discoveries: string[];
  seen_dates: number | string;
  first_seen: string;
  last_seen: string;
}

export async function getIpAddressDnsRecords(
  address: ResolvedIpAddress,
  opts: { after?: string } = {},
): Promise<IpAddressDnsRecords> {
  const after = (opts.after ?? "").slice(0, 300);
  const [rows, coverage] = await Promise.all([
    chQuery<DnsHostnameRow>(DNS_HOSTNAMES_SQL, {
      ...connectionParams(address),
      after,
      limit: IP_DETAIL_PAGE_SIZE + 1,
    }),
    loadCoverage(),
  ]);
  const groups = new Map<string, IpDnsDomainGroup>();
  for (const row of rows) {
    const group = groups.get(row.root_domain);
    if (group) {
      group.hostnames.push(row.hostname);
      continue;
    }
    groups.set(row.root_domain, {
      rootDomain: row.root_domain,
      type: row.type,
      hostnames: [row.hostname],
      sources: row.sources ?? [],
      discoveries: row.discoveries ?? [],
      seenDates: Number(row.seen_dates ?? 0),
      firstSeen: row.first_seen,
      lastSeen: row.last_seen,
    });
  }
  const page = keysetPage(
    [...groups.values()],
    IP_DETAIL_PAGE_SIZE,
    after,
    (group) => group.rootDomain,
  );
  return {
    address,
    after: page.after,
    next: page.next,
    total: page.total,
    pageSize: IP_DETAIL_PAGE_SIZE,
    domains: page.rows,
    coverage,
  };
}

/** Record-level rows for one root domain's hostnames pointing at the address. */
export async function getIpDnsRecordHistory(
  address: ResolvedIpAddress,
  rootDomain: string,
  hostnames: string[],
): Promise<IpDnsRecord[]> {
  const names = [...new Set(hostnames.filter((name) => name && name.length <= 253))];
  const rows = await chQuery<{
    hostname: string;
    type: string;
    value: string;
    sources: string[];
    discoveries: string[];
    seen_dates: number | string;
    first_seen: string;
    last_seen: string;
  }>(
    names.length && names.length <= DNS_HISTORY_MAX_HOSTNAMES
      ? DNS_RECORD_HISTORY_SQL
      : DNS_RECORD_HISTORY_ALL_NAMES_SQL,
    {
      rootDomain,
      ...(names.length && names.length <= DNS_HISTORY_MAX_HOSTNAMES
        ? { hostnames: names }
        : {}),
      code: address.version === 4 ? 1 : 28,
      ip: address.ip,
    },
  );
  return rows.map((row) => ({
    hostname: row.hostname,
    type: row.type,
    value: row.value,
    sources: row.sources ?? [],
    discoveries: row.discoveries ?? [],
    seenDates: Number(row.seen_dates ?? 0),
    firstSeen: row.first_seen,
    lastSeen: row.last_seen,
  }));
}
