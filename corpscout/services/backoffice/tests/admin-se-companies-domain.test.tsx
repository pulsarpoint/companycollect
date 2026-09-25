import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { DomainGraphRelease, DomainGraphResult } from "~/lib/domain-graph.server";
import type { SeDomainRow } from "~/lib/se-domains-list.server";
import type { DomainCrawlResult } from "~/lib/domain-crawls.server";

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
  it("shows all crawl types without claiming an uncrawled domain failed", async () => {
    const html = renderPage(await get());
    expect(crawls.loadDomainCrawls).toHaveBeenCalledWith("example.se");
    expect(html).toContain("Crawl data");
    expect(html).toContain("Basic info");
    expect(html).toContain("Full crawl");
    expect(html.match(/Not crawled/g)).toHaveLength(3);
    expect(html).toContain('/admin/crawls?domain=example.se&amp;input_domain=example.se');
  });

  it("keeps previous data visible beside the latest failed attempt", async () => {
    const saved: DomainCrawlResult = {request_id: "original", attempt: 1, state: "completed", crawl_status: "finished", successful: true,
      finished_at: "2026-09-20T10:00:00Z", error: "", s3_path: "crawls/original/result.json.gz", s3_state: "uploaded"};
    const latest = {...saved, request_id: "retry", successful: false, state: "failed", crawl_status: "failed", error: "Fetch timed out",
      finished_at: "2026-09-25T11:00:00Z", s3_path: "crawls/retry/result.json.gz"};
    crawls.loadDomainCrawls.mockResolvedValue([{type: "site_info", label: "Basic info", latest, saved}]);
    const html = renderPage(await get());
    expect(html).toContain("Crawled data available");
    expect(html).toContain("From an earlier attempt");
    expect(html).toContain(">Failed<");
    expect(html).toContain("Fetch timed out");
    expect(html).toContain('dateTime="2026-09-25T11:00:00Z"');
    expect(html).toContain('/admin/crawls/results?path=crawls%2Foriginal%2Fresult.json.gz');
    expect(html).toContain('/admin/crawls/results?path=crawls%2Fretry%2Fresult.json.gz');
  });

  it("describes a successful classification skip without calling it crawled", async () => {
    const result = {request_id: "skip", attempt: 1, state: "completed", crawl_status: "skip_crawling", successful: true,
      finished_at: "2026-09-25T10:00:00Z", error: "", s3_path: "", s3_state: "not_configured"};
    crawls.loadDomainCrawls.mockResolvedValue([{type: "site_info", label: "Basic info", latest: result, saved: result}]);
    const html = renderPage(await get());
    expect(html).toContain("Classification only");
    expect(html).toContain("Skipped by classification");
    expect(html).not.toContain("Crawled data available");
    expect(html).not.toContain("/admin/crawls/results?");
  });

  it("shows unavailable status instead of no history when the query fails", async () => {
    crawls.loadDomainCrawls.mockRejectedValue(new Error("private connection failure"));
    const html = renderPage(await get());
    expect(html).toContain("Crawl status unavailable");
    expect(html).not.toContain("Not crawled");
    expect(html).not.toContain("private connection failure");
    expect(html).toContain("Example AB");
  });

  it("does not query crawl history on technology child routes", async () => {
    await get("/web-technologies");
    expect(crawls.loadDomainCrawls).not.toHaveBeenCalled();
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
