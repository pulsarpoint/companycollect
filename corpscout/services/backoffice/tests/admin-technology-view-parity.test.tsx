import type { ComponentProps, ReactElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it, vi } from "vitest";
import CompanyOverview from "~/routes/admin-se-company-technology";
import DomainOverview from "~/routes/admin-se-domain-technology";
import CompanyWebtech from "~/routes/admin-se-company-technology-web-technologies";
import DomainWebtech from "~/routes/admin-se-domain-web-technologies";
import CompanyIp from "~/routes/admin-se-company-technology-ip-address";
import DomainIp from "~/routes/admin-se-domain-ip-address";
import { buildWebTechnologyHistory } from "~/lib/web-technology-history";
import type { CompanyTechnologyIpDetail, DomainRow } from "~/lib/queries.server";

vi.mock("~/lib/clickhouse.server", () => ({ chQuery: vi.fn() }));

const domain = "related.se";
const companyBase = "/admin/se/company/123/technology";
const domainBase = `/admin/se/companies/domains/${domain}`;
const association: DomainRow = {
  domain, website_url: `https://${domain}`, domain_source: "registry",
  confidence: 100, is_primary: 0,
};
const params = { companyId: "123", domain, address: "192.0.2.10" };
const routeContext = { params, matches: [] as never };

function render(element: ReactElement, url: string) {
  const router = createMemoryRouter([{ path: "*", element }], { initialEntries: [url] });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("admin company and domain technology views", () => {
  it.each([false, true])("shows the same overview with detections=%s", (detections) => {
    const data = {
      webTechnologyHistory: detections ? buildWebTechnologyHistory(domain,
        [{ crawlId: "CC-MAIN-2026-26", observedPages: 1, processedAt: "2026-07-01 10:00:00.000" }],
        [{ crawlId: "CC-MAIN-2026-26", name: "WordPress", categories: ["CMS"], versions: ["6.5"], confidence: 100, detectedPages: 1, sampleUrls: [`https://${domain}`] }],
      ) : null,
      technologyCatalog: {},
    };
    const company = render(<CompanyOverview {...({ ...routeContext, loaderData: { ...data, domains: [association], selectedDomain: domain } } as ComponentProps<typeof CompanyOverview>)} />, companyBase);
    const standalone = render(<DomainOverview {...({ ...routeContext, loaderData: data } as ComponentProps<typeof DomainOverview>)} />, domainBase);
    expect(company).toBe(standalone);
    expect(company).toContain(detections ? "WordPress" : "No technologies discovered yet");
    expect(company).not.toContain("Web presence");
  });

  it("keeps companies without an associated domain explicit", () => {
    const html = render(<CompanyOverview {...({ ...routeContext, loaderData: { domains: [], selectedDomain: "", webTechnologyHistory: null, technologyCatalog: {} } } as ComponentProps<typeof CompanyOverview>)} />, companyBase);
    expect(html).toContain("No domains resolved for this company");
  });

  it("includes the same Webtech scan and queue controls on both entry points", () => {
    const data = { domain, scan: null, detections: [], catalog: {}, pages: [] };
    const company = render(<CompanyWebtech {...({ ...routeContext, loaderData: data } as ComponentProps<typeof CompanyWebtech>)} />, `${companyBase}/web-technologies?domain=${domain}`);
    const standalone = render(<DomainWebtech {...({ ...routeContext, loaderData: data } as ComponentProps<typeof DomainWebtech>)} />, `${domainBase}/web-technologies`);
    expect(company).toBe(standalone);
    expect(company).toContain("Add to Webtech queue");
    expect(company).toContain(`Queue the homepage of ${domain}`);
    expect(company).toContain("No Webtech scan available");
  });

  it("shows the same IP evidence and preserves each entry point's return link", () => {
    const detail: CompanyTechnologyIpDetail = {
      companyDomain: domain, companyHostnames: [`api.${domain}`],
      historyIndexCoverage: { completedPartitions: 16, totalPartitions: 16 },
      address: { ip: params.address, version: 4, networkSegment: "192.0.2.0/24", firstSeen: "2026-01-01 00:00:00.000", lastSeen: "2026-08-01 00:00:00.000", countryCode: "SE", countryName: "Sweden", cityName: null, asn: null, asnOrganization: null, rdapRegistration: null },
      exactConnections: { page: 1, pageSize: 25, total: 1, hasMore: false, connections: [{ ip: params.address, version: 4, domain, hostnames: [`api.${domain}`], sources: [], discoveries: [], firstSeen: "2026-01-01 00:00:00.000", lastSeen: "2026-08-01 00:00:00.000" }] },
      segmentConnections: { page: 1, pageSize: 25, total: null, hasMore: false, connections: [] },
    };
    const company = render(<CompanyIp {...({ ...routeContext, loaderData: detail } as ComponentProps<typeof CompanyIp>)} />, `${companyBase}/ip-addresses/${params.address}?domain=${domain}`);
    const standalone = render(<DomainIp {...({ ...routeContext, loaderData: detail } as ComponentProps<typeof DomainIp>)} />, `${domainBase}/ip-addresses/${params.address}`);
    const companyBack = `${companyBase}/ip-addresses?domain=${domain}`;
    const domainBack = `${domainBase}/ip-addresses`;
    expect(company).toContain(`href="${companyBack}"`);
    expect(standalone).toContain(`href="${domainBack}"`);
    expect(company.replace(companyBack, domainBack)).toBe(standalone);
    expect(company).toContain("This domain");
    expect(company).toContain("Domain observation");
    expect(company).not.toContain("This company");
  });

  it("shows the same explicit empty IP evidence state", () => {
    const company = render(<CompanyIp {...({ ...routeContext, loaderData: null } as ComponentProps<typeof CompanyIp>)} />, `${companyBase}/ip-addresses/${params.address}`);
    const standalone = render(<DomainIp {...({ ...routeContext, loaderData: null } as ComponentProps<typeof DomainIp>)} />, `${domainBase}/ip-addresses/${params.address}`);
    expect(company).toBe(standalone);
    expect(company).toContain("No DNS evidence for this IP address");
    expect(company).toContain(params.address);
  });
});
