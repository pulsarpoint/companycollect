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
vi.mock("~/lib/queries.server", () => queries);
vi.mock("~/lib/web-intelligence.server", () => web);
vi.mock("~/lib/se-company-mail-security.server", () => mail);

const overview = await import("~/routes/admin-se-domain-technology");
const infrastructure = await import("~/routes/admin-se-domain-infrastructure");
const ips = await import("~/routes/admin-se-domain-ip-addresses");
const ip = await import("~/routes/admin-se-domain-ip-address");
const intelligence = await import("~/routes/admin-se-domain-web-intelligence");
const security = await import("~/routes/admin-se-domain-mail-security");

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
  it("loads all sections by the path domain without needing a company or accepting a domain override", async () => {
    await overview.loader(args());
    await infrastructure.loader(args());
    await ips.loader(args());
    await ip.loader(args());
    await intelligence.loader(args());
    await security.loader(args());

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
    const company = render(
      "/admin/se/company/123/technology",
      "?domain=example.se",
    );
    expect(company).toContain(
      'href="/admin/se/company/123/technology/infrastructure?domain=example.se"',
    );
  });
});
