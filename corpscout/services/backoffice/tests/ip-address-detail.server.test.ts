import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  formatPageTotal,
  ipDetailTabFromPath,
  ipDnsHistoryHref,
  parseAfter,
  parseDomainScope,
  rdapSourceLabel,
} from "~/lib/ip-address-detail";

const db = vi.hoisted(() => ({ chQuery: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => db);
const {
  clearEnrichmentCache,
  getIpAddressDnsRecords,
  getIpAddressDomains,
  getIpAddressHeader,
  getIpAddressOverview,
  getIpAddressRegistration,
  getIpDnsRecordHistory,
  keysetPage,
  loadEnrichmentRow,
  resolveIpAddress,
} = await import("~/lib/ip-address-detail.server");

beforeEach(() => {
  vi.resetAllMocks();
  clearEnrichmentCache();
});

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

describe("one enrichment read per request", () => {
  it("header reads the inventory by bucket and shares the enrichment row", async () => {
    db.chQuery.mockImplementation(async (sql: string) =>
      sql.includes("commoncrawl_ip_addresses")
        ? [{ first_seen: "2026-01-01", last_seen: "2026-09-01" }]
        : [{ city_lookup_status: "found", asn_lookup_status: "not_found", rdap_lookup_status: "found", subdivision_iso_codes: [], subdivision_names: [], rdap_registrant_names: [], rdap_statuses: [] }],
    );
    const header = await getIpAddressHeader(v4);
    expect(header).toMatchObject({
      firstSeen: "2026-01-01",
      statuses: { city: "found", asn: "not_found", rdap: "found" },
    });
    await getIpAddressOverview(v4);
    const enrichmentReads = sqlCalls().filter((sql) => sql.includes("FROM ip_enrichment_current"));
    expect(enrichmentReads).toHaveLength(1);
    for (const [sql, params] of db.chQuery.mock.calls) {
      expect(sql).toContain("bucket = {bucket:UInt16}");
      expect(sql).toContain("ip = {ip:String}");
      expect(sql).toContain("local_filesystem_read_prefetch=1");
      expect(params).toMatchObject({ bucket: 17, ip: v4.ip, version: 4 });
    }
  });

  it("concurrent loaders share one pending read, and a failure is not cached", async () => {
    db.chQuery.mockRejectedValueOnce(new Error("timeout")).mockResolvedValue([]);
    await expect(loadEnrichmentRow(v6)).rejects.toThrow("timeout");
    const [a, b] = await Promise.all([loadEnrichmentRow(v6), loadEnrichmentRow(v6)]);
    expect(a).toBeNull();
    expect(b).toBeNull();
    expect(db.chQuery).toHaveBeenCalledTimes(2);
  });

  it("caps the memo at 1000 entries, evicting the oldest", async () => {
    db.chQuery.mockResolvedValue([]);
    const at = (i: number) => ({ ...v4, ip: `10.0.${Math.floor(i / 256)}.${i % 256}` });
    for (let i = 0; i < 1001; i++) await loadEnrichmentRow(at(i));
    expect(db.chQuery).toHaveBeenCalledTimes(1001);
    await loadEnrichmentRow(at(1000));
    expect(db.chQuery).toHaveBeenCalledTimes(1001);
    await loadEnrichmentRow(at(0));
    expect(db.chQuery).toHaveBeenCalledTimes(1002);
  });

  it("overview maps the enrichment row and reads the RDAP marker by bucket", async () => {
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
    expect(markerSql).toContain("FROM rdap_ip_lookup_results_current");
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

  function registrationMock(enrichment: object[]) {
    db.chQuery.mockImplementation(async (sql: string, params: { networkKey?: string }) => {
      if (sql.includes("FROM ip_enrichment_current")) return enrichment;
      if (sql.includes("rdap_network_trie")) return [{ network_key: "ripe:NET-1", matched_cidr: "185.28.20.0/22" }];
      if (sql.includes("rdap_network_registry_class_current")) return [{ registry_class: "reusable", covered_rir_blocks: "0" }];
      if (sql.includes("rdap_network_segments")) return [{ cidr: "185.28.20.0/22", prefix_length: 22, segment_role: "lookup_result", derived_at: "x" }];
      if (sql.includes("rdap_networks_current")) {
        return params.networkKey === "ripe:PARENT"
          ? [{ ...network, network_key: "ripe:PARENT", handle: "PARENT", parent_network_key: null, source: undefined, raw_response: undefined }]
          : [network];
      }
      return [];
    });
  }

  it("takes both keys from the enrichment row and reads everything in one batch", async () => {
    registrationMock([{ rdap_network_key: "ripe:NET-1", rdap_matched_cidr: "185.28.20.0/22", rdap_parent_network_key: "ripe:PARENT" }]);
    const result = await getIpAddressRegistration(v4);
    expect(result.keySource).toBe("enrichment");
    expect(result.network?.rawResponse).toBe('{\n  "handle": "NET-1"\n}');
    expect(result.network?.source).toBe("ripe-rest");
    expect(result.registryClass?.registryClass).toBe("reusable");
    expect(result.segments).toHaveLength(1);
    expect(result.parent?.handle).toBe("PARENT");
    const calls = sqlCalls();
    expect(calls).toHaveLength(5);
    expect(calls.some((sql) => sql.includes("rdap_network_trie"))).toBe(false);
    const networkReads = db.chQuery.mock.calls.filter(([sql]) => String(sql).includes("FROM rdap_networks_current"));
    const [networkSql] = networkReads.find(([, params]) => params.networkKey === "ripe:NET-1")!;
    const [parentSql] = networkReads.find(([, params]) => params.networkKey === "ripe:PARENT")!;
    expect(networkSql).toContain("JSONExtractString(raw_response, 'corpscout', 'source')");
    expect(parentSql).not.toContain("raw_response");
    expect(calls.find((sql) => sql.includes("rdap_network_segments"))).toContain("FROM rdap_network_segments FINAL");
  });

  it("falls back to the RDAP trie, then reads the parent from the network row", async () => {
    registrationMock([]);
    const result = await getIpAddressRegistration(v4);
    expect(result.keySource).toBe("trie");
    expect(result.parent?.handle).toBe("PARENT");
    expect(sqlCalls()[1]).toContain("tuple(toIPv4({ip:String}))");
  });

  it("ignores the catch-all trie match", async () => {
    db.chQuery
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([{ network_key: "iana:ALL", matched_cidr: "::/0" }]);
    const result = await getIpAddressRegistration(v6);
    expect(result).toMatchObject({ keySource: null, network: null, segments: [] });
    expect(db.chQuery).toHaveBeenCalledTimes(2);
  });

  it("labels registration sources", () => {
    expect(rdapSourceLabel("ripe-rest")).toBe("RIPE REST");
    expect(rdapSourceLabel("apnic-whois")).toBe("APNIC whois");
    expect(rdapSourceLabel("")).toBe("RDAP");
  });
});

describe("keyset pages", () => {
  it("a limit+1 read with exactly pageSize rows has no next page and an exact total", () => {
    const rows = Array.from({ length: 50 }, (_, i) => `d${i}`);
    expect(keysetPage(rows, 50, "", (row) => row)).toMatchObject({ next: null, total: 50 });
    expect(keysetPage(rows, 50, "c", (row) => row)).toMatchObject({ next: null, total: null });
    const more = keysetPage([...rows, "extra"], 50, "", (row) => row);
    expect(more).toMatchObject({ next: "d49", total: null });
    expect(more.rows).toHaveLength(50);
  });

  it("reads exact-IP domains in two phases without FINAL, keyset on root_domain", async () => {
    const domains = Array.from({ length: 51 }, (_, i) => `d${String(i).padStart(2, "0")}.example`);
    db.chQuery.mockImplementation(async (sql: string, params: { domains?: string[] }) => {
      if (sql.includes("backfill")) return [{ completed_partitions: "9" }];
      if (sql.includes("SELECT DISTINCT root_domain")) return domains.map((root_domain) => ({ root_domain }));
      return (params.domains ?? []).map((domain) => ({
        ip: v4.ip, version: 4, domain, all_hostnames: [`www.${domain}`, domain], all_sources: ["scan"],
        all_discoveries: ["apex"], seen_date_count: "3", first_seen_at: "x", last_seen_at: "y",
      }));
    });
    const result = await getIpAddressDomains(v4, { after: "c.example" });
    expect(result).toMatchObject({
      scope: "exact",
      after: "c.example",
      next: "d49.example",
      total: null,
      coverage: { completedPartitions: 9, totalPartitions: 16 },
    });
    expect(result.connections).toHaveLength(50);
    expect(result.connections[0]).toMatchObject({ domain: "d00.example", hostnames: ["d00.example", "www.d00.example"] });

    const [phase1Sql, phase1Params] = db.chQuery.mock.calls.find(([q]) => String(q).includes("SELECT DISTINCT root_domain"))!;
    const [phase2Sql, phase2Params] = db.chQuery.mock.calls.find(([q]) => String(q).includes("GROUP BY root_domain"))!;
    for (const sql of [phase1Sql, phase2Sql]) {
      expect(sql).not.toContain("FINAL");
      expect(sql).toContain("PREWHERE segment_bucket = toUInt8(cityHash64({networkSegment:String}) % 64)");
      expect(sql).toContain("address = toIPv6({ip:String})");
    }
    expect(phase1Sql).toMatch(/root_domain > \{after:String\}\nORDER BY root_domain\nLIMIT \{limit:UInt32\}\nSETTINGS optimize_read_in_order=1, optimize_distinct_in_order=1/);
    expect(phase1Params).toMatchObject({ ip: v4.ip, version: 4, networkSegment: "185.28.20.0/24", after: "c.example", limit: 51 });
    expect(phase2Sql).toContain("root_domain IN {domains:Array(String)}");
    expect(phase2Sql).toContain("groupUniqArrayArray(hostnames)");
    expect(phase2Sql).toContain("length(groupUniqArrayArray(seen_dates))");
    expect(phase2Params.domains).toEqual(domains.slice(0, 50));
    for (const q of sqlCalls()) {
      expect(q).not.toContain("OFFSET");
      expect(q).not.toContain("OVER ()");
    }
  });

  it("skips phase 2 when phase 1 finds no root domains", async () => {
    db.chQuery.mockResolvedValue([]);
    const result = await getIpAddressDomains(v4);
    expect(result).toMatchObject({ connections: [], total: 0, next: null });
    const dns = await getIpAddressDnsRecords(v4);
    expect(dns).toMatchObject({ domains: [], total: 0 });
    expect(sqlCalls().some((q) => q.includes("GROUP BY root_domain"))).toBe(false);
  });

  it("keysets the segment neighbourhood on (address, root_domain)", async () => {
    db.chQuery.mockImplementation(async (sql: string) =>
      sql.includes("backfill") ? [{ completed_partitions: "16" }] : [connection("n.example", "185.28.20.9")],
    );
    const result = await getIpAddressDomains(v4, { scope: "segment", after: "185.28.20.3|m.example" });
    expect(result).toMatchObject({ scope: "segment", next: null, total: null, after: "185.28.20.3|m.example" });
    const [sql, params] = db.chQuery.mock.calls.find(([q]) => String(q).includes("commoncrawl_domain_ip_connections"))!;
    expect(sql).toContain("AND address >= toIPv6({afterAddress:String})");
    expect(sql).toContain("(address, root_domain) > (toIPv6({afterAddress:String}), {afterDomain:String})");
    expect(sql).toContain("address != toIPv6({ip:String})");
    expect(params).toMatchObject({ afterAddress: "185.28.20.3", afterDomain: "m.example" });
  });

  it("a malformed segment cursor starts at the first page", async () => {
    db.chQuery.mockResolvedValue([]);
    const result = await getIpAddressDomains(v4, { scope: "segment", after: "junk" });
    expect(result).toMatchObject({ after: "", total: 0 });
    const [, params] = db.chQuery.mock.calls.find(([q]) => String(q).includes("commoncrawl_domain_ip_connections"))!;
    expect(params).toMatchObject({ afterAddress: "::", afterDomain: "" });
  });
});

describe("DNS tab", () => {
  it("builds hostname rows by arrayJoin over phase 2, grouped per root domain", async () => {
    const hostRows = (domain: string, hosts: string[]) =>
      hosts.map((hostname) => ({ root_domain: domain, hostname, type: "A", sources: ["scan"], discoveries: ["apex"], seen_dates: "4", first_seen: "x", last_seen: "y" }));
    db.chQuery.mockImplementation(async (sql: string) => {
      if (sql.includes("backfill")) return [{ completed_partitions: "16" }];
      if (sql.includes("SELECT DISTINCT root_domain")) return [{ root_domain: "a.example" }, { root_domain: "b.example" }];
      return [...hostRows("a.example", ["a.example", "www.a.example"]), ...hostRows("b.example", ["b.example"])];
    });
    const result = await getIpAddressDnsRecords(v4);
    expect(result).toMatchObject({ after: "", next: null, total: 2, pageSize: 50 });
    expect(result.domains).toEqual([
      expect.objectContaining({ rootDomain: "a.example", type: "A", hostnames: ["a.example", "www.a.example"], seenDates: 4 }),
      expect.objectContaining({ rootDomain: "b.example", hostnames: ["b.example"] }),
    ]);
    expect(sqlCalls().some((q) => q.includes("commoncrawl_domain_dns_records"))).toBe(false);
    const [sql, params] = db.chQuery.mock.calls.find(([q]) => String(q).includes("arrayJoin"))!;
    expect(sql).toContain("arrayJoin(arraySort(all_hostnames))");
    expect(sql).toContain("root_domain IN {domains:Array(String)}");
    expect(sql).not.toContain("FINAL");
    expect(params).toMatchObject({ domains: ["a.example", "b.example"], version: 4 });
  });

  it("the 51st root domain of phase 1 becomes the next page", async () => {
    const domains = Array.from({ length: 51 }, (_, i) => `d${String(i).padStart(2, "0")}.example`);
    db.chQuery.mockImplementation(async (sql: string, params: { domains?: string[] }) => {
      if (sql.includes("backfill")) return [];
      if (sql.includes("SELECT DISTINCT root_domain")) return domains.map((root_domain) => ({ root_domain }));
      return (params.domains ?? []).map((root_domain) => ({ root_domain, hostname: "h", type: "AAAA", sources: [], discoveries: [], seen_dates: 1, first_seen: "x", last_seen: "y" }));
    });
    const result = await getIpAddressDnsRecords(v6, { after: "a" });
    expect(result.domains).toHaveLength(50);
    expect(result).toMatchObject({ next: "d49.example", total: null });
  });

  it("record history reads one root domain by record_type_code and hostnames", async () => {
    db.chQuery.mockResolvedValue([{ hostname: "www.a.example", type: "AAAA", value: "2001:db8::1", sources: [], discoveries: [], seen_dates: "3", first_seen: "x", last_seen: "y" }]);
    const records = await getIpDnsRecordHistory(v6, "a.example", ["www.a.example", "www.a.example", ""]);
    expect(records[0]).toMatchObject({ hostname: "www.a.example", seenDates: 3 });
    const [sql, params] = db.chQuery.mock.calls[0];
    expect(sql).toMatch(/WHERE root_domain = \{rootDomain:String\}\n\s+AND name IN \{hostnames:Array\(String\)\}\n\s+AND record_type_code = \{code:UInt16\}/);
    expect(params).toEqual({ rootDomain: "a.example", hostnames: ["www.a.example"], code: 28, ip: v6.ip });
  });

  it("record history for a domain with too many hostnames keeps root_domain = only", async () => {
    db.chQuery.mockResolvedValue([]);
    const many = Array.from({ length: 201 }, (_, i) => `h${i}.a.example`);
    await getIpDnsRecordHistory(v4, "a.example", many);
    const [sql, params] = db.chQuery.mock.calls[0];
    expect(sql).toContain("WHERE root_domain = {rootDomain:String}");
    expect(sql).not.toContain("name IN");
    expect(params).toEqual({ rootDomain: "a.example", code: 1, ip: v4.ip });
  });
});

describe("client helpers", () => {
  it("parses cursor, scope and tab", () => {
    expect(parseAfter(null)).toBe("");
    expect(parseAfter("x".repeat(700))).toHaveLength(600);
    expect(parseDomainScope("segment")).toBe("segment");
    expect(parseDomainScope("other")).toBe("exact");
    expect(ipDetailTabFromPath("/admin/ip-addresses/8.8.8.8/dns")).toBe("dns");
    expect(ipDetailTabFromPath("/admin/ip-addresses/8.8.8.8")).toBe("overview");
    expect(formatPageTotal(null, 50)).toBe("50+");
    expect(formatPageTotal(12, 12)).toBe("12");
    expect(ipDnsHistoryHref("2001:db8::1", "a.example", ["x.a.example"])).toBe(
      "/admin/ip-addresses/2001%3Adb8%3A%3A1/dns/a.example?h=x.a.example",
    );
    // Under the count cap but over 4 KB: no hostnames, the server reads root_domain only.
    const long = Array.from({ length: 100 }, (_, i) => `${"x".repeat(60)}${i}.a.example`);
    expect(ipDnsHistoryHref("8.8.8.8", "a.example", long)).toBe("/admin/ip-addresses/8.8.8.8/dns/a.example");
    const many = Array.from({ length: 201 }, (_, i) => `h${i}.a.example`);
    expect(ipDnsHistoryHref("8.8.8.8", "a.example", many)).toBe("/admin/ip-addresses/8.8.8.8/dns/a.example");
  });
});
