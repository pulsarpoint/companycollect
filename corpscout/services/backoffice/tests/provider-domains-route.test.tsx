import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

const server = vi.hoisted(() => ({ getProviderDomains: vi.fn() }));
const recon = vi.hoisted(() => ({ loadProviderDocument: vi.fn() }));
vi.mock("~/lib/domain-services.server", () => server);
vi.mock("~/lib/provider-recon.server", () => recon);
const route = await import("~/routes/admin-provider-feeds-provider-domains");

beforeEach(() => vi.clearAllMocks());

const page = { rows: [{ domain: "x.se", serviceTypes: ["dns"], serviceKeys: ["ionos.dns"], firstSeen: "2025-01-01", lastSeen: "2026-09-01", isCurrent: true }],
  total: 120, page: 3, pageSize: 50, byType: { dns: { now: 100, ever: 120 } }, byService: [{ service: "ionos.dns", now: 100, ever: 120 }] };

describe("provider domains tab", () => {
  it("is a 404 for an unknown provider without querying ClickHouse", async () => {
    recon.loadProviderDocument.mockResolvedValue(null);
    await expect(route.loader({ params: { slug: "nope" }, request: new Request("http://x/admin/provider-feeds/providers/nope/domains") } as never))
      .rejects.toMatchObject({ init: { status: 404 } });
    expect(server.getProviderDomains).not.toHaveBeenCalled();
  });

  it("passes the decoded slug and parsed filters", async () => {
    recon.loadProviderDocument.mockResolvedValue({ slug: "one-com", display_name: "One.com" });
    server.getProviderDomains.mockResolvedValue(page);
    await route.loader({ params: { slug: "one-com" },
      request: new Request("http://x/admin/provider-feeds/providers/one-com/domains?type=dns&page=9&now=0") } as never);
    expect(server.getProviderDomains).toHaveBeenCalledWith("one-com", { serviceType: "dns", service: "", now: false, page: 9 });
  });

  it("renders domains linked to their Services tab with paging", () => {
    const Page = route.default as (p: { loaderData: unknown }) => React.ReactElement;
    const html = renderToStaticMarkup(<MemoryRouter initialEntries={["/admin/provider-feeds/providers/ionos/domains?page=3"]}>
      <Page loaderData={{ provider: { slug: "ionos", name: "IONOS" }, filter: { serviceType: "", service: "", now: true, page: 3 }, ...page }} />
    </MemoryRouter>);
    expect(html).toContain('href="/admin/domains/x.se/services"');
    expect(html).toContain("Page 3 of 3");
    expect(html).toContain("ionos.dns");
  });
});
