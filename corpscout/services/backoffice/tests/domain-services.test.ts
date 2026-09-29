import { beforeEach, describe, expect, it, vi } from "vitest";

const ch = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: ch.query }));

import { groupCurrent, parseProviderDomainsFilter, type ServiceInterval } from "~/lib/domain-services";
import { getDomainServices, getProviderDomains, getProviderSummaries, getUnmappedKeys } from "~/lib/domain-services.server";

beforeEach(() => ch.query.mockReset());

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
    ch.query
      .mockResolvedValueOnce([{ total: "120" }])                       // count
      .mockResolvedValueOnce([{ root_domain: "x.se", service_types: ["dns"], service_keys: [], first_seen: "2025-01-01", last_seen: "2026-09-01", is_current: 1 }])
      .mockResolvedValueOnce([])                                       // by type
      .mockResolvedValueOnce([]);                                      // by service
    const out = await getProviderDomains("ionos", { serviceType: "", service: "", now: true, page: 99 });
    expect(out.page).toBe(3);
    expect(ch.query.mock.calls[1][1]).toMatchObject({ slug: "ionos", offset: 100, limit: 50 });
  });
});
