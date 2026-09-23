import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { TechnologySectionTabs } from "~/components/detail/technology-section-tabs";

const queries = vi.hoisted(() => ({
  getDomainTechnologyDetail: vi.fn(),
  getDomainTechnologyInfrastructure: vi.fn(),
  getDomainTechnologyIpInventory: vi.fn(),
  getDomainTechnologyIpDetail: vi.fn(),
}));
const web = vi.hoisted(() => ({ getDomainWebIntelligence: vi.fn() }));
const mail = vi.hoisted(() => ({ getDomainMailSecurity: vi.fn() }));
const webtech = vi.hoisted(() => ({ getDomainWebtech: vi.fn() }));
vi.mock("~/lib/queries.server", () => queries);
vi.mock("~/lib/web-intelligence.server", () => web);
vi.mock("~/lib/se-company-mail-security.server", () => mail);
vi.mock("~/lib/webtech.server", () => webtech);

const overview = await import("~/routes/admin-se-domain-technology");
const infrastructure = await import("~/routes/admin-se-domain-infrastructure");
const ips = await import("~/routes/admin-se-domain-ip-addresses");
const ip = await import("~/routes/admin-se-domain-ip-address");
const intelligence = await import("~/routes/admin-se-domain-web-intelligence");
const security = await import("~/routes/admin-se-domain-mail-security");
const technologies = await import("~/routes/admin-se-domain-web-technologies");
const commonCrawl = await import("~/routes/admin-common-crawl-domain");

beforeEach(() => vi.clearAllMocks());

function args() {
  return {
    params: { domain: "EXAMPLE.SE", address: "192.0.2.10" },
    request: new Request(
      "http://localhost/admin/se/companies/domains/EXAMPLE.SE?domain=unrelated.se&page=2&pageSize=25&exactPage=3&segmentPage=4",
    ),
  } as never;
}

describe("domain technology routes", () => {
  it("keeps Common Crawl evidence at the root and technology navigation in the same domain", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter initialEntries={["/admin/common-crawl/novelic.com"]}>
        <TechnologySectionTabs
          basePath="/admin/common-crawl/novelic.com"
          section="overview"
          websiteEvidenceOverview
          mailSecurity
        />
      </MemoryRouter>,
    );
    expect(html).toContain("Website evidence");
    expect(html).toContain("Archived technologies");
    for (const section of ["technologies", "web-technologies", "infrastructure", "ip-addresses", "mail-security"]) {
      expect(html).toContain(`href="/admin/common-crawl/novelic.com/${section}"`);
    }
    expect(html).not.toContain("/web-intelligence");
    expect(html).not.toContain("/admin/se/");
  });

  it("normalizes the Common Crawl domain for every child view while retaining its path and query", () => {
    expect.assertions(2);
    try {
      commonCrawl.loader({
        params: { domain: "WWW.NOVELIC.COM" },
        request: new Request("http://localhost/admin/common-crawl/WWW.NOVELIC.COM/ip-addresses/192.0.2.1?exactPage=2"),
      } as never);
    } catch (response) {
      expect((response as Response).status).toBe(302);
      expect((response as Response).headers.get("Location")).toBe(
        "/admin/common-crawl/novelic.com/ip-addresses/192.0.2.1?exactPage=2",
      );
    }
  });

  it("loads all sections by the path domain without needing a company or accepting a domain override", async () => {
    await overview.loader(args());
    await infrastructure.loader(args());
    await ips.loader(args());
    await ip.loader(args());
    await intelligence.loader(args());
    await security.loader(args());
    await technologies.loader(args());

    expect(queries.getDomainTechnologyDetail).toHaveBeenCalledWith(
      "example.se",
    );
    expect(queries.getDomainTechnologyInfrastructure).toHaveBeenCalledWith(
      "example.se",
      { page: 2, pageSize: 25 },
    );
    expect(queries.getDomainTechnologyIpInventory).toHaveBeenCalledWith(
      "example.se",
      { page: 2, pageSize: 25 },
    );
    expect(queries.getDomainTechnologyIpDetail).toHaveBeenCalledWith(
      "example.se",
      "192.0.2.10",
      { exactPage: 3, segmentPage: 4 },
    );
    expect(web.getDomainWebIntelligence).toHaveBeenCalledWith("example.se");
    expect(mail.getDomainMailSecurity).toHaveBeenCalledWith("example.se");
    expect(webtech.getDomainWebtech).toHaveBeenCalledWith("example.se");
  });

  it("keeps empty technology results explicit", () => {
    const Component = overview.default;
    const html = renderToStaticMarkup(
      <Component
        {...({
          loaderData: { webTechnologyHistory: null, technologyCatalog: {} },
          params: { domain: "example.se" },
        } as Parameters<typeof Component>[0])}
      />,
    );
    expect(html).toContain("No technologies discovered yet");
    expect(html).toContain("example.se");
  });

  it("keeps domain navigation in the domain area and company navigation on its selected domain", () => {
    const render = (basePath: string, search = "") =>
      renderToStaticMarkup(
        <MemoryRouter initialEntries={[basePath]}>
          <TechnologySectionTabs
            basePath={basePath}
            section="overview"
            search={search}
            mailSecurity
          />
        </MemoryRouter>,
      );
    const domain = render("/admin/se/companies/domains/example.se");
    expect(domain).toContain(
      'href="/admin/se/companies/domains/example.se/infrastructure"',
    );
    expect(domain).toContain(
      'href="/admin/se/companies/domains/example.se/ip-addresses"',
    );
    expect(domain).toContain(
      'href="/admin/se/companies/domains/example.se/mail-security"',
    );
    expect(domain).not.toContain("?domain=");
    expect(domain).toContain('href="/admin/se/companies/domains/example.se/web-technologies"');
    const workspace = render("/admin/domains/example.se");
    for (const section of ["web-technologies", "web-intelligence", "infrastructure", "ip-addresses", "mail-security"]) {
      expect(workspace).toContain(`href="/admin/domains/example.se/${section}"`);
    }
    const company = render(
      "/admin/se/company/123/technology",
      "?domain=example.se",
    );
    expect(company).toContain(
      'href="/admin/se/company/123/technology/infrastructure?domain=example.se"',
    );
    expect(company).toContain('href="/admin/se/company/123/technology/web-technologies?domain=example.se"');
  });
});
