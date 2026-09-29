import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { TechnologySectionTabs } from "~/components/detail/technology-section-tabs";

const server = vi.hoisted(() => ({ getDomainServices: vi.fn() }));
vi.mock("~/lib/domain-services.server", () => server);
const route = await import("~/routes/admin-domain-services");

beforeEach(() => vi.clearAllMocks());

const interval = { serviceType: "dns", providerKey: "loopia", providerSlug: "loopia", serviceKeys: ["loopia.dns"], firstSeen: "2025-01-01",
  lastSeen: "2026-09-01", isCurrent: true, evidence: 2, recordTypes: ["NS"], confidence: 1 };

function view(loaderData: unknown) {
  const Page = route.default as (p: { loaderData: unknown }) => React.ReactElement;
  return renderToStaticMarkup(<MemoryRouter><Page loaderData={loaderData} /></MemoryRouter>);
}

describe("domain services tab", () => {
  it("lowercases the domain for the query", async () => {
    server.getDomainServices.mockResolvedValue({ domain: "example.se", resolved: false, intervals: [], evidence: [] });
    await route.loader({ params: { domain: "EXAMPLE.SE" } } as never);
    expect(server.getDomainServices).toHaveBeenCalledWith("example.se");
  });

  it("shows current providers linked to their provider pages, history and evidence", () => {
    const html = view({ domain: "a.se", resolved: true,
      intervals: [interval, { ...interval, providerKey: "ui-dns.xyz", providerSlug: "", isCurrent: false, lastSeen: "2024-01-01" }],
      evidence: [{ serviceType: "dns", providerKey: "loopia", recordName: "a.se", recordType: "NS", subject: "ns1.loopia.se", ruleId: "r",
        validFrom: "2025-01-01", validTo: "2026-09-01" }] });
    expect(html).toContain('href="/admin/provider-feeds/providers/loopia/domains"');
    expect(html).toContain("ui-dns.xyz");
    expect(html).toContain("unmapped");
    expect(html).toContain("ns1.loopia.se");
  });

  it("distinguishes not resolved from resolved without providers", () => {
    expect(view({ domain: "a.se", resolved: false, intervals: [], evidence: [] })).toContain("not been resolved");
    expect(view({ domain: "a.se", resolved: true, intervals: [], evidence: [] })).toContain("No providers found");
  });

  it("adds a Services tab to the domain tabs", () => {
    const html = renderToStaticMarkup(<MemoryRouter><TechnologySectionTabs basePath="/admin/domains/a.se" section="services" dnsRecords /></MemoryRouter>);
    expect(html).toContain('href="/admin/domains/a.se/services"');
  });
});

describe("domain services tab errors", () => {
  it("shows an error card instead of failing the page when ClickHouse is down", async () => {
    server.getDomainServices.mockRejectedValue(new Error("connect ECONNREFUSED"));
    const data = await route.loader({ params: { domain: "a.se" } } as never);
    expect(data).toMatchObject({ domain: "a.se", error: "connect ECONNREFUSED" });
    expect(view(data)).toContain("connect ECONNREFUSED");
  });
});
