import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  ipDetailTabFromPath,
  parseDomainScope,
  parsePage,
  rdapSourceLabel,
} from "~/lib/ip-address-detail";

const db = vi.hoisted(() => ({ chQuery: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => db);
const {
  getIpAddressDnsRecords,
  getIpAddressDomains,
  getIpAddressHeader,
  getIpAddressOverview,
  getIpAddressRegistration,
  resolveIpAddress,
} = await import("~/lib/ip-address-detail.server");

beforeEach(() => vi.resetAllMocks());

const v4 = {
  ip: "185.28.20.221",
  version: 4 as const,
  bucket: 17,
  networkSegment: "185.28.20.0/24",
};
const v6 = {
  ip: "2001:db8::1",
  version: 6 as const,
  bucket: 200,
  networkSegment: "2001:db8::/48",
};

function sqlCalls(): string[] {
  return db.chQuery.mock.calls.map(([sql]) => String(sql));
}

function connection(domain: string, ip = v4.ip) {
  return {
    ip,
    version: 4,
    domain,
    hostnames: [`www.${domain}`, domain],
    sources: ["scan"],
    discoveries: ["apex"],
    first_seen: "2026-01-01 00:00:00.000",
    last_seen: "2026-09-01 00:00:00.000",
  };
}

describe("resolveIpAddress", () => {
  it.each(["", "not-an-ip", "1.2.3", "256.1.1.1", "8.8.8.8/24", "x".repeat(60)])(
    "rejects %j without querying ClickHouse",
    async (raw) => {
      expect(await resolveIpAddress(raw)).toBeNull();
      expect(db.chQuery).not.toHaveBeenCalled();
    },
  );

  it("canonicalises IPv4 through toIPv4 and derives bucket and /24 segment", async () => {
    db.chQuery.mockResolvedValueOnce([
      { ip: "8.8.8.8", bucket: "12", network_segment: "8.8.8.0/24" },
    ]);
    expect(await resolveIpAddress(" 8.8.8.8 ")).toEqual({
      ip: "8.8.8.8",
      version: 4,
      bucket: 12,
      networkSegment: "8.8.8.0/24",
    });
    const [sql, params] = db.chQuery.mock.calls[0];
    expect(sql).toContain("toString(toIPv4OrNull({raw:String}))");
    expect(sql).toContain("toUInt16(cityHash64(ip) % 256) AS bucket");
    expect(sql).toContain("IPv4CIDRToRange(toIPv4(ip), 24)");
    expect(sql).not.toContain("toIPv6");
    expect(params).toEqual({ raw: "8.8.8.8" });
  });

  it("canonicalises IPv6 through toIPv6 with a /48 segment", async () => {
    db.chQuery.mockResolvedValueOnce([
      { ip: "2001:db8::1", bucket: 200, network_segment: "2001:db8::/48" },
    ]);
    expect(await resolveIpAddress("2001:DB8:0:0::1")).toEqual(v6);
    const [sql, params] = db.chQuery.mock.calls[0];
    expect(sql).toContain("toString(toIPv6OrNull({raw:String}))");
    expect(sql).toContain("IPv6CIDRToRange(toIPv6(ip), 48)");
    expect(sql).not.toContain("toIPv4");
    expect(params).toEqual({ raw: "2001:DB8:0:0::1" });
  });

  it("returns null when ClickHouse cannot convert the address", async () => {
    db.chQuery.mockResolvedValueOnce([]);
    expect(await resolveIpAddress("1.2.3.4")).toBeNull();
  });
});

describe("per-address reads use the bucket", () => {
  it("header reads the inventory and enrichment status by bucket and ip", async () => {
    db.chQuery
      .mockResolvedValueOnce([{ first_seen: "2026-01-01", last_seen: "2026-09-01" }])
      .mockResolvedValueOnce([]);
    const header = await getIpAddressHeader(v4);
    expect(header).toMatchObject({
      firstSeen: "2026-01-01",
      lastSeen: "2026-09-01",
      statuses: null,
    });
    for (const [sql, params] of db.chQuery.mock.calls) {
      expect(sql).toContain("bucket = {bucket:UInt16}");
      expect(sql).toContain("ip = {ip:String}");
      expect(params).toMatchObject({ bucket: 17, ip: v4.ip, version: 4 });
    }
    expect(sqlCalls()[0]).toContain("FROM commoncrawl_ip_addresses");
    expect(sqlCalls()[1]).toContain("FROM ip_enrichment_current");
  });

  it("overview reads ip_enrichment_current and the RDAP marker by bucket", async () => {
    db.chQuery
      .mockResolvedValueOnce([
        {
          result_id: "r",
          task_id: "t",
          completed_at: "2026-09-27 00:00:00",
          city_lookup_status: "found",
          asn_lookup_status: "found",
          rdap_lookup_status: "retryable_error",
          rdap_data_status: "found",
          latitude: 1.5,
          longitude: null,
          asn: "15169",
          subdivision_iso_codes: [],
          subdivision_names: [],
          rdap_registrant_names: [],
          rdap_statuses: [],
        },
      ])
      .mockResolvedValueOnce([
        { lookup_status: "found", network_key: "arin:NET-1", queried_at: "2026-09-27" },
      ]);
    const overview = await getIpAddressOverview(v4);
    expect(overview.enrichment).toMatchObject({
      taskId: "t",
      asn: { asn: 15169 },
      geo: { latitude: 1.5, longitude: null },
      rdapStatus: { status: "retryable_error", dataStatus: "found" },
    });
    expect(overview.rdapMarker).toMatchObject({ status: "found", networkKey: "arin:NET-1" });
    const [enrichmentSql, markerSql] = sqlCalls();
    expect(enrichmentSql).toContain("FROM ip_enrichment_current");
    expect(enrichmentSql).toContain("bucket = {bucket:UInt16}");
    expect(markerSql).toContain("FROM rdap_ip_lookup_results_current");
    expect(markerSql).toContain("bucket = {bucket:UInt16}");
    expect(markerSql).toContain("ip_version = {version:UInt8}");
  });

  it("overview renders a not-enriched address", async () => {
    db.chQuery.mockResolvedValue([]);
    expect(await getIpAddressOverview(v4)).toEqual({
      address: v4,
      enrichment: null,
      rdapMarker: null,
    });
  });
});

describe("getIpAddressRegistration", () => {
  const network = {
    network_key: "ripe:NET-1",
    rir: "ripe",
    handle: "NET-1",
    ip_version: 4,
    start_address: "185.28.20.0",
    end_address: "185.28.23.255",
    name: "EXAMPLE",
    statuses: ["active"],
    registrant_handles: [],
    registrant_names: ["Example"],
    parent_network_key: "ripe:PARENT",
    fetched_at: "2026-09-27",
    source: "ripe-rest",
    raw_response: '{"handle":"NET-1"}',
  };

  it("uses the enrichment network key and loads class, segments and parent", async () => {
    db.chQuery
      .mockResolvedValueOnce([{ rdap_network_key: "ripe:NET-1", rdap_matched_cidr: "185.28.20.0/22" }])
      .mockResolvedValueOnce([network])
      .mockResolvedValueOnce([{ registry_class: "reusable", covered_rir_blocks: "0" }])
      .mockResolvedValueOnce([{ cidr: "185.28.20.0/22", prefix_length: 22, segment_role: "lookup_result", derived_at: "x" }])
      .mockResolvedValueOnce([{ ...network, network_key: "ripe:PARENT", handle: "PARENT", parent_network_key: null }]);
    const result = await getIpAddressRegistration(v4);
    expect(result.keySource).toBe("enrichment");
    expect(result.network?.rawResponse).toBe('{\n  "handle": "NET-1"\n}');
    expect(result.network?.source).toBe("ripe-rest");
    expect(result.registryClass?.registryClass).toBe("reusable");
    expect(result.segments).toHaveLength(1);
    expect(result.parent?.handle).toBe("PARENT");
    const calls = sqlCalls();
    expect(calls[0]).toContain("bucket = {bucket:UInt16}");
    expect(calls.some((sql) => sql.includes("rdap_network_trie"))).toBe(false);
    expect(calls[1]).toContain("JSONExtractString(raw_response, 'corpscout', 'source')");
    expect(calls[3]).toContain("FROM rdap_network_segments FINAL");
    expect(calls[3]).toContain("network_key = {networkKey:String}");
    expect(db.chQuery.mock.calls[4][1]).toEqual({ networkKey: "ripe:PARENT" });
  });

  it("falls back to the RDAP trie and ignores the catch-all", async () => {
    db.chQuery
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([{ network_key: "iana:ALL", matched_cidr: "::/0" }]);
    const result = await getIpAddressRegistration(v6);
    expect(result).toMatchObject({ keySource: null, network: null, segments: [] });
    expect(sqlCalls()[1]).toContain("tuple(toIPv6({ip:String}))");
    expect(db.chQuery).toHaveBeenCalledTimes(2);
  });

  it("labels registration sources", () => {
    expect(rdapSourceLabel("ripe-rest")).toBe("RIPE REST");
    expect(rdapSourceLabel("apnic-whois")).toBe("APNIC whois");
    expect(rdapSourceLabel("")).toBe("RDAP");
  });
});

describe("connections and DNS records", () => {
  it("pages exact-IP domains with limit+1 and a capped count, no window count", async () => {
    const rows = Array.from({ length: 51 }, (_, i) => connection(`d${i}.example`));
    db.chQuery.mockImplementation(async (sql: string) => {
      if (sql.includes("toString(count()) AS total")) return [{ total: "10001" }];
      if (sql.includes("commoncrawl_domain_ip_backfill_status")) return [{ completed_partitions: "9" }];
      return rows;
    });
    const result = await getIpAddressDomains(v4, { page: 3 });
    expect(result).toMatchObject({
      scope: "exact",
      page: 3,
      hasMore: true,
      total: { count: 10000, capped: true },
      coverage: { completedPartitions: 9, totalPartitions: 16 },
    });
    expect(result.connections).toHaveLength(50);
    const pageCall = db.chQuery.mock.calls.find(([sql]) => String(sql).includes("LIMIT {limit:UInt32}"));
    expect(pageCall?.[1]).toMatchObject({
      ip: v4.ip,
      version: 4,
      networkSegment: "185.28.20.0/24",
      limit: 51,
      offset: 100,
    });
    for (const sql of sqlCalls().filter((sql) => sql.includes("commoncrawl_domain_ip_connections"))) {
      expect(sql).toContain("PREWHERE segment_bucket = toUInt8(cityHash64({networkSegment:String}) % 64)");
      expect(sql).toContain("segment_cidr = {networkSegment:String}");
      expect(sql).not.toContain("OVER ()");
    }
  });

  it("switches to the segment neighbourhood without a count", async () => {
    db.chQuery.mockImplementation(async (sql: string) =>
      sql.includes("backfill") ? [{ completed_partitions: "16" }] : [connection("n.example", "185.28.20.9")],
    );
    const result = await getIpAddressDomains(v4, { scope: "segment" });
    expect(result).toMatchObject({ scope: "segment", total: null, hasMore: false });
    expect(sqlCalls().some((sql) => sql.includes("address != toIPv6({ip:String})"))).toBe(true);
    expect(sqlCalls().some((sql) => sql.includes("count()") && !sql.includes("backfill"))).toBe(false);
  });

  it("reads dns records once per page root domain, keyed by root_domain and hostnames", async () => {
    db.chQuery.mockImplementation(async (sql: string, params: Record<string, unknown>) => {
      if (sql.includes("commoncrawl_domain_dns_records")) {
        return [
          {
            root_domain: params.rootDomain,
            hostname: `www.${params.rootDomain}`,
            type: "A",
            value: v4.ip,
            sources: ["scan"],
            discoveries: [],
            seen_dates: "4",
            first_seen: "x",
            last_seen: "y",
          },
        ];
      }
      if (sql.includes("AS total")) return [{ total: "2" }];
      if (sql.includes("backfill")) return [{ completed_partitions: "16" }];
      return [connection("a.example"), connection("b.example")];
    });
    const result = await getIpAddressDnsRecords(v4, { page: 2 });
    expect(result).toMatchObject({ page: 2, pageSize: 10, hasMore: false });
    expect(result.rootDomains).toEqual(["a.example", "b.example"]);
    expect(result.records.map((record) => record.rootDomain)).toEqual(["a.example", "b.example"]);
    expect(result.records[0]).toMatchObject({ seenDates: 4 });
    expect(result.total).toEqual({ count: 2, capped: false });
    const pageCall = db.chQuery.mock.calls.find(([sql]) => String(sql).includes("LIMIT {limit:UInt32}"));
    expect(pageCall?.[1]).toMatchObject({ limit: 11, offset: 10 });
    const dnsCalls = db.chQuery.mock.calls.filter(([sql]) =>
      String(sql).includes("commoncrawl_domain_dns_records"),
    );
    expect(dnsCalls.map(([, params]) => params)).toEqual([
      { rootDomain: "a.example", hostnames: ["www.a.example", "a.example"], recordType: "A", ip: v4.ip },
      { rootDomain: "b.example", hostnames: ["www.b.example", "b.example"], recordType: "A", ip: v4.ip },
    ]);
  });

  it("never reads commoncrawl_domain_dns_records when the page is empty", async () => {
    db.chQuery.mockImplementation(async (sql: string) =>
      sql.includes("AS total") ? [{ total: "0" }] : sql.includes("backfill") ? [{ completed_partitions: "16" }] : [],
    );
    const result = await getIpAddressDnsRecords(v6);
    expect(result.records).toEqual([]);
    expect(sqlCalls().some((sql) => sql.includes("commoncrawl_domain_dns_records"))).toBe(false);
  });

  it("never filters commoncrawl_domain_dns_records without a page root domain", async () => {
    const domains = Array.from({ length: 11 }, (_, i) => connection(`d${i}.example`));
    db.chQuery.mockImplementation(async (sql: string) =>
      sql.includes("AS total") ? [{ total: "11" }] : sql.includes("backfill") ? [] : sql.includes("dns_records") ? [] : domains,
    );
    const result = await getIpAddressDnsRecords(v6, { page: 1 });
    expect(result.hasMore).toBe(true);
    const dnsCalls = db.chQuery.mock.calls.filter(([sql]) => String(sql).includes("commoncrawl_domain_dns_records"));
    expect(dnsCalls).toHaveLength(10);
    for (const [sql, params] of dnsCalls) {
      expect(sql).toMatch(/WHERE root_domain = \{rootDomain:String\}\n\s+AND name IN \{hostnames:Array\(String\)\}/);
      expect(result.rootDomains).toContain(params.rootDomain);
      expect(params).toMatchObject({ recordType: "AAAA", ip: v6.ip });
    }
    expect(dnsCalls.map(([, params]) => params.rootDomain)).not.toContain("d10.example");
  });
});

describe("client helpers", () => {
  it("parses paging, scope and tab", () => {
    expect(parsePage(null)).toBe(1);
    expect(parsePage("0")).toBe(1);
    expect(parsePage("abc")).toBe(1);
    expect(parsePage("4")).toBe(4);
    expect(parseDomainScope("segment")).toBe("segment");
    expect(parseDomainScope("other")).toBe("exact");
    expect(ipDetailTabFromPath("/admin/ip-addresses/8.8.8.8/dns")).toBe("dns");
    expect(ipDetailTabFromPath("/admin/ip-addresses/8.8.8.8")).toBe("overview");
  });
});
