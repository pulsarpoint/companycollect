import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";

const server = vi.hoisted(() => ({ getProviderSummaries: vi.fn(), getUnmappedKeys: vi.fn() }));
vi.mock("~/lib/domain-services.server", () => server);
const route = await import("~/routes/admin-providers");

const providers = [
  { slug: "ionos", name: "IONOS", category: "hosting", domainsNow: 10, domainsEver: 12, byType: { dns: { now: 9, ever: 11 } } },
  { slug: "zoho", name: "Zoho", category: "email", domainsNow: 3, domainsEver: 3, byType: {} },
];

function view(loaderData: unknown, url = "/admin/providers") {
  const Page = route.default as (p: { loaderData: unknown }) => React.ReactElement;
  return renderToStaticMarkup(<MemoryRouter initialEntries={[url]}><Page loaderData={loaderData} /></MemoryRouter>);
}

describe("providers list", () => {
  it("reports ClickHouse errors instead of crashing", async () => {
    server.getProviderSummaries.mockRejectedValue(new Error("boom"));
    server.getUnmappedKeys.mockResolvedValue([]);
    expect(await route.loader()).toEqual({ error: "boom", providers: [], unmapped: [] });
  });

  it("links providers to their domains and lists unmapped keys", () => {
    const html = view({ providers, unmapped: [{ providerKey: "ui-dns.xyz", domainsNow: 5, domainsEver: 6 }] });
    expect(html).toContain('href="/admin/provider-feeds/providers/ionos/domains"');
    expect(html).toContain("Unmapped provider domains");
    expect(html).toContain("ui-dns.xyz");
  });

  it("filters by the search parameter", () => {
    const html = view({ providers, unmapped: [] }, "/admin/providers?q=zoh");
    expect(html).toContain("Zoho");
    expect(html).not.toContain("IONOS");
  });
});
