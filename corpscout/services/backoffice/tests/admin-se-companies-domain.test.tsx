import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { DomainGraphRelease, DomainGraphResult } from "~/lib/domain-graph.server";
import type { SeDomainRow } from "~/lib/se-domains-list.server";

const graph = vi.hoisted(() => ({
  getDomainGraphReleases: vi.fn(),
  searchDomainGraph: vi.fn(),
}));
const domains = vi.hoisted(() => ({
  loadSeDomainCompanies: vi.fn(),
  loadSeDomainCompanyCounts: vi.fn(),
}));
vi.mock("~/lib/domain-graph.server", () => graph);
vi.mock("~/lib/se-domains-list.server", () => domains);
const crawls = vi.hoisted(() => ({loadDomainCrawls: vi.fn()}));
vi.mock("~/lib/domain-crawls.server", () => crawls);
const { default: AdminSeCompaniesDomain, loader } = await import(
  "~/routes/admin-se-companies-domain"
);

const release: DomainGraphRelease = {
  graph_release: "cc-main-2026-jun-jul-aug",
  node_count: "119722885",
  edge_count: "2450405793",
  published_at: "2026-09-16 12:00:00.000",
};
const result: DomainGraphResult = {
  found: true,
  total: 2,
  page: 1,
  pageSize: 50,
  counts: { all: 2, mutual: 1, outgoing: 1, incoming: 0 },
  rows: [
    { connected_domain: "mutual.se", outgoing: 1, incoming: 1, reciprocal: 1, n_hosts: 2 },
    { connected_domain: "outgoing.se", outgoing: 1, incoming: 0, reciprocal: 0, n_hosts: 1 },
  ],
};
const COMPANY: SeDomainRow = {
  company_id: "5560125220",
  legal_name: "Example AB",
  root_domain: "example.se",
  website_url: "https://www.example.se/",
  website_host: "www.example.se",
  association: "connected",
  is_primary: 1,
  confidence: 0.9,
  sources: ["wikidata"],
  supporting_sources: ["wikidata"],
  verification_status: "success",
  review_status: "unreviewed",
  active: 1,
  inactive_reason: "",
  company_count: 2,
};
const OTHER: SeDomainRow = {
  ...COMPANY,
  company_id: "5560125221",
  legal_name: "Other AB",
  is_primary: 0,
  confidence: 0.6,
  association: "uncertain",
};

function get(query = "", domain = "example.se") {
  return loader({
    request: new Request(`http://backoffice/admin/se/companies/domains/${domain}${query}`),
    params: { domain },
  } as never);
}

function renderPage(loaderData: Awaited<ReturnType<typeof loader>>, query = ""): string {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <AdminSeCompaniesDomain
            {...({ loaderData } as Parameters<typeof AdminSeCompaniesDomain>[0])}
          />
        ),
      },
    ],
    { initialEntries: [`/admin/se/companies/domains/example.se${query}`] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

beforeEach(() => {
  crawls.loadDomainCrawls.mockReset().mockResolvedValue([
    {type: "site_info", label: "Basic info", latest: null, saved: null},
    {type: "jobs", label: "Jobs", latest: null, saved: null},
    {type: "full", label: "Full crawl", latest: null, saved: null},
  ]);
  graph.getDomainGraphReleases.mockReset().mockResolvedValue([release]);
  graph.searchDomainGraph.mockReset().mockResolvedValue(result);
  domains.loadSeDomainCompanies.mockReset().mockResolvedValue([COMPANY, OTHER]);
  domains.loadSeDomainCompanyCounts
    .mockReset()
    .mockResolvedValue(new Map([["mutual.se", 3]]));
});

describe("domain detail loader", () => {
  it("loads the companies, searches the newest release and marks SE-company neighbours", async () => {
    const data = await get("?direction=mutual&page=2&pageSize=50");
    expect(domains.loadSeDomainCompanies).toHaveBeenCalledWith("example.se");
    expect(graph.searchDomainGraph).toHaveBeenCalledWith({
      domain: "example.se",
      release: release.graph_release,
      direction: "mutual",
      page: 2,
      pageSize: 50,
    });
    expect(domains.loadSeDomainCompanyCounts).toHaveBeenCalledWith(["mutual.se", "outgoing.se"]);
    expect(data.domain).toBe("example.se");
    expect(data.companies).toEqual([COMPANY, OTHER]);
    expect(data.result).toEqual(result);
    expect(data.companyCounts).toEqual({ "mutual.se": 3 });
    expect(data.release).toEqual(release);
    expect(data.error).toBeNull();
  });

  it("shows the companies without a graph search when no release is published", async () => {
    graph.getDomainGraphReleases.mockResolvedValue([]);
    const data = await get();
    expect(graph.searchDomainGraph).not.toHaveBeenCalled();
    expect(data.companies).toEqual([COMPANY, OTHER]);
    expect(data.result).toBeNull();
    expect(data.release).toBeNull();
  });

  it("keeps the companies and reports a retryable graph error when the graph fails", async () => {
    graph.searchDomainGraph.mockRejectedValue(new Error("timeout"));
    const data = await get();
    expect(data.companies).toEqual([COMPANY, OTHER]);
    expect(data.result).toBeNull();
    expect(data.error).toMatch(/could not be loaded/);
    expect(data.error).not.toContain("timeout");
  });

  it("rejects a path segment that is not a domain", async () => {
    await expect(get("", "not a domain")).rejects.toMatchObject({ status: 404 });
    expect(domains.loadSeDomainCompanies).not.toHaveBeenCalled();
  });
});

describe("domain detail page", () => {
  it("offers a separate Crawl tab without loading or showing crawls on Overview", async () => {
    const html = renderPage(await get());
    expect(html).toContain('href="/admin/se/companies/domains/example.se/crawl"');
    expect(html).not.toContain("Crawl data");
    expect(crawls.loadDomainCrawls).not.toHaveBeenCalled();
  });

  it("selects the Crawl tab without showing overview graph or companies", async () => {
    const html = renderPage(await get("/crawl"), "/crawl");
    expect(graph.searchDomainGraph).not.toHaveBeenCalled();
    expect(html).not.toContain("Example AB");
    expect(html).not.toContain("Domain connections");
    expect(html).toMatch(/aria-selected="true"[^>]*href="[^"]*\/crawl"/);
  });

  it("lists every company behind the domain, linked to its Domains tab", async () => {
    const html = renderPage(await get());
    expect(html).toContain(">example.se<");
    expect(html).toContain("2 companies");
    expect(html).toContain('href="/admin/se/company/5560125220/domains"');
    expect(html).toContain('href="/admin/se/company/5560125221/domains"');
    expect(html).toContain("Example AB");
    expect(html).toContain("Other AB");
    expect(html).toContain("90%");
    expect(html).toContain("60%");
    expect(html).toContain("uncertain");
  });

  it("shows the domain's connections, linking neighbours to their own pages", async () => {
    const html = renderPage(await get());
    expect(html).toContain('href="/admin/se/companies/domains/mutual.se"');
    expect(html).toContain('href="/admin/se/companies/domains/outgoing.se"');
    expect(html).toContain("Mutual");
    expect(html).toContain("Links back to searched domain");
    // A neighbour that is itself an SE company domain says so; one that is not, does not.
    const rows = html.split("<tr");
    const mutual = rows.find((row) => row.includes(">mutual.se</a>"))!;
    const outgoing = rows.find((row) => row.includes(">outgoing.se</a>"))!;
    expect(mutual).toContain("3 SE companies");
    expect(outgoing).not.toContain("SE compan");
    // The same search on the Graph page, one click away.
    expect(html).toContain(
      `href="/admin/graph?domain=example.se&amp;release=${release.graph_release}"`,
    );
    // Direction tabs stay on this page.
    expect(html).toContain('href="/admin/se/companies/domains/example.se?direction=mutual"');
  });

  it("says when no SE company claims the domain", async () => {
    domains.loadSeDomainCompanies.mockResolvedValue([]);
    const html = renderPage(await get());
    expect(html).toContain("No SE company claims this domain");
    expect(html).toContain('href="/admin/se/companies/domains/mutual.se"');
  });

  it("distinguishes a domain absent from the graph", async () => {
    graph.searchDomainGraph.mockResolvedValue({ ...result, found: false, rows: [], total: 0 });
    const html = renderPage(await get());
    expect(html).toContain("Domain not found in this release");
    expect(html).toContain("Example AB");
  });
});
