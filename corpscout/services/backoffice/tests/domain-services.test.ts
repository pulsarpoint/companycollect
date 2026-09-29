import { beforeEach, describe, expect, it, vi } from "vitest";

const ch = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: ch.query }));

import { groupCurrent, parseProviderDomainsFilter, type ServiceInterval } from "~/lib/domain-services";
import { getDomainServices, getProviderDomains, getProviderSummaries, getUnmappedKeys } from "~/lib/domain-services.server";

beforeEach(() => {
  ch.query.mockReset();
});

const iv = (o: Partial<ServiceInterval>): ServiceInterval => ({
  serviceType: "dns", providerKey: "loopia", providerSlug: "loopia", serviceKeys: [], firstSeen: "2025-01-01",
  lastSeen: "2026-09-01", isCurrent: true, evidence: 1, recordTypes: ["NS"], confidence: 1, ...o,
});

describe("domain services helpers", () => {
  it("groups current intervals by service type in label order and drops ended ones", () => {
    const groups = groupCurrent([iv({ serviceType: "email" }), iv({}), iv({ isCurrent: false, providerKey: "old" })]);
    expect(groups.map((g) => g.serviceType)).toEqual(["dns", "email"]);
    expect(groups[0].intervals.map((i) => i.providerKey)).toEqual(["loopia"]);
  });

  it("parses provider domain filters defensively", () => {
    expect(parseProviderDomainsFilter(new URLSearchParams("type=bogus&service=a%20b&now=0&page=-4")))
      .toEqual({ serviceType: "", service: "", now: false, page: 1 });
    expect(parseProviderDomainsFilter(new URLSearchParams("type=dns&service=ionos.dns&page=3")))
      .toEqual({ serviceType: "dns", service: "ionos.dns", now: true, page: 3 });
  });
});

describe("domain services queries", () => {
  it("reads a domain's history, current flags and evidence with the domain as a parameter", async () => {
    ch.query
      .mockResolvedValueOnce([{ resolutions: 3 }])
      .mockResolvedValueOnce([{ service_type: "dns", provider_key: "loopia", provider_slug: "loopia", service_keys: ["loopia.dns"],
        first_seen: "2025-01-01", last_seen: "2026-09-01", evidence: "2", record_types: ["NS"], confidence: 1 }])
      .mockResolvedValueOnce([{ service_type: "dns", provider_key: "loopia", first_seen: "2025-01-01" }])
      .mockResolvedValueOnce([{ service_type: "dns", provider_key: "loopia", record_name: "a.se", record_type: "NS",
        subject: "ns1.loopia.se", rule_id: "r", valid_from: "2025-01-01", valid_to: "2026-09-01" }]);
    const out = await getDomainServices("a.se");
    expect(out.resolved).toBe(true);
    expect(out.intervals[0]).toMatchObject({ providerSlug: "loopia", isCurrent: true, evidence: 2 });
    expect(out.evidence[0].subject).toBe("ns1.loopia.se");
    for (const [sql, params] of ch.query.mock.calls) {
      expect(sql).not.toContain("a.se");
      expect(params).toMatchObject({ domain: "a.se" });
    }
  });

  it("reports an unresolved domain without querying its history", async () => {
    ch.query.mockResolvedValueOnce([{ resolutions: 0 }]);
    const out = await getDomainServices("never.se");
    expect(out).toEqual({ domain: "never.se", resolved: false, intervals: [], evidence: [] });
    expect(ch.query).toHaveBeenCalledTimes(1);
  });

  it("builds provider summaries with totals from the '' service type row", async () => {
    ch.query.mockResolvedValueOnce([
      { provider_slug: "ionos", name: "IONOS", category: "hosting", service_type: "", domains_now: "10", domains_ever: "12" },
      { provider_slug: "ionos", name: "IONOS", category: "hosting", service_type: "dns", domains_now: "9", domains_ever: "11" },
    ]);
    const [p] = await getProviderSummaries();
    expect(p).toEqual({ slug: "ionos", name: "IONOS", category: "hosting", domainsNow: 10, domainsEver: 12, byType: { dns: { now: 9, ever: 11 } } });
  });

  it("lists only unmapped keys", async () => {
    ch.query.mockResolvedValueOnce([{ provider_key: "ui-dns.xyz", domains_now: "5", domains_ever: "6" }]);
    await getUnmappedKeys();
    const [sql] = ch.query.mock.calls[0];
    expect(sql).toContain("provider_slug = ''");
    expect(sql).toContain("service_type = ''");
  });

  it("clamps a provider domains page past the end to the last page", async () => {
    ch.query.mockImplementation(async (sql: string) => {
      if (sql.includes("FROM corpscout.provider_service_counts")) return [{ service_type: "", now: "120", ever: "150" }];
      return [];
    });
    const out = await getProviderDomains("ionos", { serviceType: "", service: "", now: true, page: 99 });
    expect(out.page).toBe(3);
    const pageCall = ch.query.mock.calls.find(([sql]) => sql.includes("SELECT DISTINCT root_domain"));
    expect(pageCall?.[1]).toMatchObject({ slug: "ionos", offset: 100, limit: 50 });
  });
});

describe("review fixes", () => {
  function dispatch(handlers: [string, unknown[]][]) {
    ch.query.mockImplementation(async (sql: string) => {
      for (const [needle, rows] of handlers) if (sql.includes(needle)) return rows;
      throw new Error(`unexpected query: ${sql.slice(0, 80)}`);
    });
  }

  it("takes per-type counts and the unfiltered total from provider_service_counts and aggregates only the page's domains", async () => {
    dispatch([
      ["FROM corpscout.provider_service_counts", [
        { service_type: "", now: "120", ever: "150" },
        { service_type: "dns", now: "100", ever: "130" },
      ]],
      ["SELECT DISTINCT root_domain", [{ root_domain: "a.se" }, { root_domain: "b.se" }]],
      ["root_domain IN {domains:Array(String)}", [
        { root_domain: "a.se", service_types: ["dns"], service_keys: [], first_seen: "2025-01-01", last_seen: "2026-09-01", is_current: 1 },
      ]],
      ["ARRAY JOIN service_keys", []],
    ]);
    const out = await getProviderDomains("ionos", { serviceType: "", service: "", now: true, page: 1 });
    expect(out.total).toBe(120);
    expect(out.byType).toEqual({ dns: { now: 100, ever: 130 } });
    const pageCall = ch.query.mock.calls.find(([sql]) => sql.includes("root_domain IN {domains:Array(String)}"));
    expect(pageCall?.[1]).toMatchObject({ domains: ["a.se", "b.se"] });
    expect(ch.query.mock.calls.some(([sql]) => sql.includes("uniqExact(root_domain) AS total"))).toBe(false);
  });

  it("counts the total over intervals only when a service filter applies", async () => {
    dispatch([
      ["FROM corpscout.provider_service_counts", []],
      ["uniqExact(root_domain) AS total", [{ total: "7" }]],
      ["SELECT DISTINCT root_domain", []],
      ["ARRAY JOIN service_keys", []],
    ]);
    const out = await getProviderDomains("ionos", { serviceType: "", service: "ionos.dns", now: true, page: 1 });
    expect(out.total).toBe(7);
    expect(out.rows).toEqual([]);
  });

  it("leaves keys that a provider definition names out of the unmapped list", async () => {
    ch.query.mockResolvedValueOnce([]);
    await getUnmappedKeys();
    const [sql] = ch.query.mock.calls[0];
    expect(sql).toContain("provider_key NOT IN (SELECT arrayJoin(provider_keys) FROM corpscout.provider_services FINAL)");
  });
});
