import { beforeEach, expect, it, vi } from "vitest";
import {
  EMPTY_WORKSPACE_IP_FILTERS,
  parseWorkspaceIpFilters,
  workspaceIpAddressesHref,
  type WorkspaceIpFilters,
} from "~/lib/workspace-ip-addresses";

const db = vi.hoisted(() => ({ chQuery: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => db);
const { listWorkspaceIpAddresses } =
  await import("~/lib/workspace-ip-addresses.server");
beforeEach(() => vi.resetAllMocks());

const filters = (overrides: Partial<WorkspaceIpFilters> = {}) => ({
  ...EMPTY_WORKSPACE_IP_FILTERS,
  ...overrides,
});
const row = (i: number, overrides: Record<string, unknown> = {}) => ({
  bucket: 3,
  ip_version: 4,
  ip: `192.0.2.${i}`,
  first_seen: "2026-01-01 00:00:00.000",
  last_seen: "2026-02-01 00:00:00.000",
  asn: 0,
  asn_organization: "",
  country_iso_code: "",
  subdivision_iso_code: "",
  city_name: "",
  rdap_matched_cidr: "",
  rdap_name: "",
  ...overrides,
});
const decode = (cursor: string) =>
  JSON.parse(Buffer.from(cursor, "base64url").toString());
const encode = (value: unknown) =>
  Buffer.from(JSON.stringify(value)).toString("base64url");

it("pages the whole search table by its sort key in one query, unknowns as nulls", async () => {
  db.chQuery.mockResolvedValueOnce(
    Array.from({ length: 51 }, (_, i) =>
      row(i + 1, i === 0 ? { asn: 15169, asn_organization: "Google LLC", country_iso_code: "US", city_name: "Mountain View", rdap_matched_cidr: "192.0.2.0/24", rdap_name: "NET" } : {}),
    ),
  );
  const result = await listWorkspaceIpAddresses(filters());
  expect(db.chQuery).toHaveBeenCalledTimes(1);
  const [sql, params] = db.chQuery.mock.calls[0];
  expect(sql).toContain("FROM corpscout.ip_enrichment_search");
  expect(sql).toContain("ORDER BY asn, bucket, ip LIMIT {limit:UInt32}");
  expect(sql).not.toContain("WHERE");
  expect(params).toEqual({ limit: 51 });
  expect(result.rows).toHaveLength(50);
  expect(result.rows[0]).toMatchObject({ asn: 15169, asn_organization: "Google LLC", country_iso_code: "US", city_name: "Mountain View", rdap_matched_cidr: "192.0.2.0/24" });
  expect(result.rows[1]).toMatchObject({ asn: null, asn_organization: null, country_iso_code: null, city_name: null, rdap_matched_cidr: null, rdap_name: null });
  expect(result.hasMore).toBe(true);
  expect(decode(result.next)).toEqual({ order: "asn", key: [0, 3, "192.0.2.50"] });
});

it("filters ASNs by the primary key and continues from the full cursor", async () => {
  db.chQuery.mockResolvedValue([]);
  const after = encode({ order: "asn", key: [15169, 7, "8.8.8.8"] });
  const result = await listWorkspaceIpAddresses(
    filters({ asn: ["15169", "3301"], version: "4", country: ["US"] }),
    after,
  );
  const [sql, params] = db.chQuery.mock.calls[0];
  expect(sql).toContain("asn IN {asns:Array(UInt32)}");
  expect(sql).toContain("country_iso_code IN {countries:Array(String)}");
  expect(sql).toContain("ip_version = {version:UInt8}");
  expect(sql).toContain(
    "(asn > {afterAsn:UInt32} OR (asn = {afterAsn:UInt32} AND (bucket > {afterBucket:UInt16} OR (bucket = {afterBucket:UInt16} AND ip > {afterIp:String}))))",
  );
  expect(sql).toContain("ORDER BY asn, bucket, ip");
  expect(params).toMatchObject({ asns: [15169, 3301], countries: ["US"], version: 4, afterAsn: 15169, afterBucket: 7, afterIp: "8.8.8.8" });
  expect(result.after).toBe(after);
});

it("pages location filters in the location projection's order, values bound", async () => {
  db.chQuery.mockResolvedValueOnce(
    Array.from({ length: 51 }, (_, i) => row(i + 1, { country_iso_code: "SE", subdivision_iso_code: "AB", city_name: "Stockholm" })),
  );
  const after = encode({ order: "location", key: ["SE", "AB", "Stockholm", 2, "192.0.2.9"] });
  const result = await listWorkspaceIpAddresses(
    parseWorkspaceIpFilters(new URLSearchParams("country=se&region=ab&city=Stockholm")),
    after,
  );
  const [sql, params] = db.chQuery.mock.calls[0];
  expect(sql).toContain("subdivision_iso_code IN {regions:Array(String)}");
  expect(sql).toContain("city_name IN {cities:Array(String)}");
  expect(sql).toContain("ORDER BY country_iso_code, subdivision_iso_code, city_name, bucket, ip");
  expect(sql).toContain("(country_iso_code > {afterCountry:String} OR (country_iso_code = {afterCountry:String} AND (subdivision_iso_code > {afterRegion:String}");
  expect(sql).not.toMatch(/'SE'|Stockholm|'AB'/);
  expect(params).toMatchObject({ countries: ["SE"], regions: ["AB"], cities: ["Stockholm"], afterCountry: "SE", afterRegion: "AB", afterCity: "Stockholm", afterBucket: 2, afterIp: "192.0.2.9" });
  expect(decode(result.next)).toEqual({ order: "location", key: ["SE", "AB", "Stockholm", 3, "192.0.2.50"] });
});

it("ignores a cursor of the other key order and starts at the first page", async () => {
  db.chQuery.mockResolvedValue([]);
  const after = encode({ order: "asn", key: [1, 2, "1.1.1.1"] });
  const result = await listWorkspaceIpAddresses(filters({ country: ["SE"] }), after);
  expect(result.after).toBe("");
  expect(db.chQuery.mock.calls[0][0]).not.toContain("{afterIp:String}");
});

it.each([
  "broken",
  encode(null),
  encode({ order: "asn", key: [1, 256, "1.1.1.1"] }),
  encode({ order: "asn", key: [1, 2, "not-an-ip"] }),
  encode({ order: "asn", key: [-1, 2, "1.1.1.1"] }),
])("ignores invalid cursors safely: %s", async (after) => {
  db.chQuery.mockResolvedValue([]);
  const result = await listWorkspaceIpAddresses(filters(), after);
  expect(result.after).toBe("");
  expect(db.chQuery.mock.calls[0][0]).not.toContain("{afterIp:String}");
});

it("canonicalizes exact IPv6 searches and narrows them to one bucket", async () => {
  db.chQuery.mockResolvedValue([]);
  const parsed = parseWorkspaceIpFilters(new URLSearchParams("search=2001:0DB8:0:0:0:0:0:2&version=6"));
  await listWorkspaceIpAddresses(parsed);
  const [sql, params] = db.chQuery.mock.calls[0];
  expect(sql).toContain("bucket = toUInt16(modulo(cityHash64(toString(toIPv6({search:String}))), 256))");
  expect(sql).toContain("ip = toString(toIPv6({search:String}))");
  expect(params).toMatchObject({ search: "2001:0db8:0:0:0:0:0:2", version: 6 });
});

it("keeps the prefix interval on the search table when an ASN or location filter narrows it", async () => {
  db.chQuery.mockResolvedValue([]);
  await listWorkspaceIpAddresses(filters({ search: "192.0.2.", asn: ["3301"] }));
  expect(db.chQuery).toHaveBeenCalledTimes(1);
  const [sql, params] = db.chQuery.mock.calls[0];
  expect(sql).toContain("FROM corpscout.ip_enrichment_search");
  expect(sql).toContain("ip >= {search:String} AND ip < {prefixEnd:String}");
  expect(sql).not.toContain("192.0.2.");
  expect(params).toMatchObject({ search: "192.0.2.", prefixEnd: "192.0.2/", asns: [3301] });
});

it("resolves a bare prefix on the inventory by bucket, then reads those rows by (bucket, ip)", async () => {
  const keys = Array.from({ length: 51 }, (_, i) => ({ bucket: 9, ip_version: 4, ip: `192.0.2.${i + 1}` }));
  db.chQuery
    .mockResolvedValueOnce([]) // buckets 0-7: nothing
    .mockResolvedValueOnce(keys) // buckets 8-15: a full page plus one
    // Snapshot rows: 192.0.2.2 is newer than the last rebuild and is left out.
    .mockResolvedValueOnce(keys.filter((key) => key.ip !== "192.0.2.2").map((key) => row(0, { ...key, asn: 3301 })));
  const result = await listWorkspaceIpAddresses(filters({ search: "192.0.2.", version: "4" }));
  expect(db.chQuery).toHaveBeenCalledTimes(3);
  const [first, firstParams] = db.chQuery.mock.calls[0];
  expect(first).toContain("FROM corpscout.commoncrawl_ip_addresses");
  expect(first).toContain("bucket = 0 AND");
  expect(first).toContain("bucket = 7 AND");
  expect(first).toContain("ip_version = {version:UInt8}");
  expect(first).not.toContain("192.0.2.");
  expect(firstParams).toMatchObject({ search: "192.0.2.", prefixEnd: "192.0.2/", limit: 51, version: 4 });
  expect(db.chQuery.mock.calls[1][0]).toContain("bucket = 8 AND");
  const [hydrate, hydrateParams] = db.chQuery.mock.calls[2];
  expect(hydrate).toContain("FROM corpscout.ip_enrichment_search");
  expect(hydrate).toContain("bucket IN {buckets:Array(UInt16)} AND ip IN {ips:Array(String)}");
  expect(hydrateParams.buckets).toEqual([9]);
  expect(hydrateParams.ips).toHaveLength(50);
  expect(hydrateParams.ips).not.toContain("192.0.2.51");
  expect(result.order).toBe("inventory");
  expect(result.rows.map((r) => r.ip)).not.toContain("192.0.2.2");
  expect(result.rows).toHaveLength(49);
  expect(result.rows[0]).toMatchObject({ ip: "192.0.2.1", asn: 3301 });
  expect(result.hasMore).toBe(true);
  expect(decode(result.next)).toEqual({ order: "inventory", key: [9, 4, "192.0.2.50"] });
});

it("continues a bare prefix from its inventory cursor bucket", async () => {
  db.chQuery.mockResolvedValue([]);
  const after = encode({ order: "inventory", key: [17, 4, "192.0.2.9"] });
  const result = await listWorkspaceIpAddresses(filters({ search: "192.0.2." }), after);
  const [sql, params] = db.chQuery.mock.calls[0];
  expect(sql).toContain("bucket = 17 AND");
  expect(sql).not.toContain("bucket = 16 AND");
  expect(sql).toContain(
    "(bucket > {afterBucket:UInt16} OR (bucket = {afterBucket:UInt16} AND (ip_version > {afterVersion:UInt8} OR (ip_version = {afterVersion:UInt8} AND ip > {afterIp:String}))))",
  );
  expect(params).toMatchObject({ afterBucket: 17, afterVersion: 4, afterIp: "192.0.2.9" });
  // Walked to the last bucket group, found nothing: no hydration read, no next page.
  expect(db.chQuery.mock.calls.at(-1)?.[0]).toContain("bucket = 255 AND");
  expect(result).toMatchObject({ rows: [], hasMore: false, next: "", after });
});

it("rejects invalid address text without querying", async () => {
  await listWorkspaceIpAddresses(filters({ search: "x'%_" }));
  expect(db.chQuery).not.toHaveBeenCalled();
});

it("keeps the cursor and the filters on the next-page link", () => {
  const parsed = parseWorkspaceIpFilters(new URLSearchParams("asn=AS15169&country=US&version=4"));
  const href = new URL(workspaceIpAddressesHref(parsed, "cursor"), "http://localhost");
  expect(href.searchParams.getAll("asn")).toEqual(["15169"]);
  expect(href.searchParams.get("country")).toBe("US");
  expect(href.searchParams.get("after")).toBe("cursor");
});
