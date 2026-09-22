import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, MemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AdminSidebar } from "~/components/admin/admin-sidebar";
import { SidebarProvider } from "~/components/ui/sidebar";

const server = vi.hoisted(() => ({
  listSeDomainsPage: vi.fn(),
  loadSeDomainsCounts: vi.fn(),
  saveSeDomainCrawlInputs: vi.fn(),
  launchSeDomainCrawlWorkflow: vi.fn(),
}));
vi.mock("~/lib/se-domains-list.server", () => server);
vi.mock("~/lib/se-domain-crawl.server", () => server);
import { action, loader } from "~/routes/admin-se-companies-domains";
import { SeDomainsTable } from "~/components/admin/se-domains-table";
import { SeDomainsFilterFields } from "~/components/admin/se-domains-filter-sheet";
import { EMPTY_SE_DOMAINS_FILTERS } from "~/lib/se-domains-filters";
import {
  SE_COMPANIES_TABS,
  seCompaniesTabFromPath,
  seCompaniesTabPath,
} from "~/lib/se-companies-tabs";
import type { SeDomainRow } from "~/lib/se-domains-list.server";

const ROW: SeDomainRow = {
  company_id: "5560125220",
  legal_name: "Example AB",
  root_domain: "example.se",
  website_url: "https://www.example.se/",
  website_host: "www.example.se",
  association: "connected",
  is_primary: 1,
  confidence: 0.9,
  sources: ["wikidata", "brave"],
  supporting_sources: ["wikidata", "brave"],
  verification_status: "success",
  review_status: "unreviewed",
  active: 1,
  inactive_reason: "",
  company_count: 2,
};
const COUNTS = { rows: 2, domains: 1, companies: 2, shared: 1 };

function render(element: React.ReactElement, search = ""): string {
  const router = createMemoryRouter([{ path: "/admin/se/companies/domains", element }], {
    initialEntries: [`/admin/se/companies/domains${search}`],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("sidebar", () => {
  function sidebarItems(pathname: string): Record<string, boolean> {
    const html = renderToStaticMarkup(
      <MemoryRouter initialEntries={[pathname]}>
        <SidebarProvider>
          <AdminSidebar />
        </SidebarProvider>
      </MemoryRouter>,
    );
    const active: Record<string, boolean> = {};
    for (const to of ["/admin/se/companies", "/admin/se/companies/domains", "/admin/se/people"]) {
      const item = html.split("<li").find((chunk) => chunk.includes(`href="${to}"`));
      expect(item, `sidebar entry ${to}`).toBeDefined();
      active[to] = item!.includes('data-active=""');
    }
    return active;
  }

  it("has a Sweden > Domains entry, active on the list and on a domain page but not on other Companies tabs", () => {
    expect(sidebarItems("/admin/se/companies/domains")).toEqual({
      "/admin/se/companies": false,
      "/admin/se/companies/domains": true,
      "/admin/se/people": false,
    });
    expect(sidebarItems("/admin/se/companies/domains/example.se")).toEqual({
      "/admin/se/companies": false,
      "/admin/se/companies/domains": true,
      "/admin/se/people": false,
    });
    expect(sidebarItems("/admin/se/companies/geocoding")).toEqual({
      "/admin/se/companies": true,
      "/admin/se/companies/domains": false,
      "/admin/se/people": false,
    });
    expect(sidebarItems("/admin/se/companies")["/admin/se/companies"]).toBe(true);
  });
});

describe("companies tabs", () => {
  it("has a Domains tab that stays active on a domain's detail page", () => {
    expect(SE_COMPANIES_TABS.map((tab) => tab.value)).toContain("domains");
    expect(seCompaniesTabPath("domains")).toBe("/admin/se/companies/domains");
    expect(seCompaniesTabFromPath("/admin/se/companies/domains")).toBe("domains");
    expect(seCompaniesTabFromPath("/admin/se/companies/domains/example.se")).toBe("domains");
  });
});

describe("admin-se-companies-domains route", () => {
  beforeEach(() => {
    server.listSeDomainsPage.mockReset().mockResolvedValue({ rows: [ROW] });
    server.loadSeDomainsCounts.mockReset().mockResolvedValue(COUNTS);
    server.saveSeDomainCrawlInputs.mockReset();
    server.launchSeDomainCrawlWorkflow.mockReset();
  });

  it("submits the selected domains through the route action and returns the Dagster input run", async () => {
    const selection = { mode: "ids", domains: ["example.se"] };
    const run = { ok: true, requestId: "save-request", runId: "input-run", runUrl: "http://dagster/runs/input-run", table: "corpscout.website_jobs_crawl_requests" };
    server.saveSeDomainCrawlInputs.mockResolvedValue(run);
    const response = await action({ request: new Request("http://x/admin/se/companies/domains", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "save_crawl_inputs", crawlType: "jobs", selection }),
    }) } as never);
    expect(response.data).toEqual({ ...run, workflow: false });
    expect(server.saveSeDomainCrawlInputs).toHaveBeenCalledWith(selection, "jobs");
  });

  it("sends a selection for crawling through the Dagster workflow with the operator", async () => {
    vi.stubEnv("BACKOFFICE_OPERATOR", "operator@example.se");
    const selection = { mode: "ids", domains: ["example.se"] };
    const settings = { api: "deepseek", model: "deepseek-flash" };
    const run = { ok: true, requestId: "task", taskId: "task", runId: "workflow-run", runUrl: "http://dagster/runs/workflow-run" };
    server.launchSeDomainCrawlWorkflow.mockResolvedValue(run);
    const response = await action({ request: new Request("http://x/admin/se/companies/domains", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "send_for_crawl", crawlType: "site_info", selection, settings }),
    }) } as never);
    expect(response.data).toEqual({ ...run, workflow: true });
    expect(server.launchSeDomainCrawlWorkflow).toHaveBeenCalledWith(selection, "site_info", settings, "operator@example.se");
    expect(server.saveSeDomainCrawlInputs).not.toHaveBeenCalled();
    vi.unstubAllEnvs();
  });

  it("returns a failed submission without reporting that inputs were queued", async () => {
    server.saveSeDomainCrawlInputs.mockRejectedValue(new Error("Dagster is unavailable."));
    const response = await action({ request: new Request("http://x/admin/se/companies/domains", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "save_crawl_inputs", crawlType: "full", selection: { mode: "ids", domains: ["example.se"] } }),
    }) } as never);
    expect(response.data).toEqual({ ok: false, error: "Dagster is unavailable." });
    expect(response.init).toEqual({ status: 400 });
  });

  it("selects a shared domain on every company row while counting it once", () => {
    const html = render(<SeDomainsTable
      rows={[ROW, { ...ROW, company_id: "5560049529", legal_name: "Second AB" }]}
      counts={COUNTS} page={1} pageSize={50} filters={EMPTY_SE_DOMAINS_FILTERS}
      selection={{ value: { mode: "ids", domains: ["example.se"] }, onChange: vi.fn(), actions: <button>Add to crawl inputs</button> }}
    />);
    expect(html).toContain("1 domain selected");
    const checkboxes = html.match(/<[^>]+role="checkbox"[^>]*>/g) ?? [];
    expect(checkboxes).toHaveLength(3);
    expect(checkboxes.every((checkbox) => checkbox.includes('aria-checked="true"'))).toBe(true);
    expect(html).toContain("Add to crawl inputs");
  });

  it("shows the count across all pages after domain exclusions", () => {
    const html = render(<SeDomainsTable rows={[ROW]}
      counts={{ ...COUNTS, rows: 120, domains: 100 }} page={2} pageSize={50} filters={EMPTY_SE_DOMAINS_FILTERS}
      selection={{ value: { mode: "query", query: EMPTY_SE_DOMAINS_FILTERS, excludedDomains: ["example.se"] }, onChange: vi.fn(), actions: null }}
    />);
    expect(html).toContain("Select all 100 matching domains");
    expect(html).toContain("99 domains selected across all pages");
    expect(html).toContain("Clear selection");
    const checkboxes = html.match(/<[^>]+role="checkbox"[^>]*>/g) ?? [];
    expect(checkboxes.every((checkbox) => checkbox.includes('aria-checked="false"'))).toBe(true);
  });

  it("pages and counts under the same filters", async () => {
    const data = await loader({
      request: new Request(
        "http://x/admin/se/companies/domains?source=brave&minConfidence=0.7&shared=1&page=3&pageSize=50",
      ),
    } as never);
    const filters = { ...EMPTY_SE_DOMAINS_FILTERS, source: "brave", minConfidence: "0.7", shared: "1" };
    expect(data).toEqual({ rows: [ROW], counts: COUNTS, page: 3, pageSize: 50, filters });
    expect(server.listSeDomainsPage).toHaveBeenCalledWith({ ...filters, page: 3, pageSize: 50 });
    expect(server.loadSeDomainsCounts).toHaveBeenCalledWith(filters);
  });

  it("renders a row linking the domain to its page and the company to its Domains tab", () => {
    const html = render(
      <SeDomainsTable
        rows={[ROW]}
        counts={COUNTS}
        page={1}
        pageSize={50}
        filters={EMPTY_SE_DOMAINS_FILTERS}
      />,
    );
    expect(html).toContain('href="/admin/se/companies/domains/example.se"');
    expect(html).toContain('href="/admin/se/company/5560125220/domains"');
    expect(html).toContain("Example AB");
    expect(html).toContain("90%");
    expect(html).toContain("2 companies");
    expect(html).toContain("connected");
    expect(html).toContain("wikidata");
  });

  it("shows a source once, with how many of its records evidenced the domain", () => {
    const html = render(
      <SeDomainsTable
        rows={[{ ...ROW, sources: ["brave", "esef_filing", "esef_filing", "esef_filing"] }]}
        counts={COUNTS}
        page={1}
        pageSize={50}
        filters={EMPTY_SE_DOMAINS_FILTERS}
      />,
    );
    expect(html.match(/esef_filing/g)?.length).toBe(1);
    expect(html).toContain("esef_filing ×3");
    expect(html).not.toContain("brave ×");
  });

  it("offers confidence bounds and a shared-only filter, and reports the shared count", () => {
    const html = render(
      <SeDomainsTable
        rows={[ROW]}
        counts={COUNTS}
        page={1}
        pageSize={50}
        filters={{ ...EMPTY_SE_DOMAINS_FILTERS, minConfidence: "0.7", shared: "1" }}
      />,
    );
    expect(html).toContain('data-slot="sheet-trigger"');
    expect(html).toContain("Filters");
    expect(html).toContain('aria-label="Remove filter Min confidence 0.7"');
    expect(html).toContain('aria-label="Remove filter Shared only"');
    expect(html).toContain("Shared domains");
    // The Clear link drops every filter.
    expect(html).toContain('href="/admin/se/companies/domains"');
  });

  it("labels the select triggers with words, never the Any sentinel or a stored value", () => {
    const html = render(
      <form><SeDomainsFilterFields
        pageSize={50}
        filters={{ ...EMPTY_SE_DOMAINS_FILTERS, source: "brave", association: "not_connected", minConfidence: "0.75", shared: "1" }}
      /></form>,
    );
    const triggers = html.match(/<button[^>]*role="combobox"[^>]*>.*?<\/button>/gs) ?? [];
    expect(triggers).toHaveLength(3);
    expect(triggers[0]).toContain("Brave");
    expect(triggers[1]).toContain("not connected");
    expect(triggers[1]).not.toContain("not_connected");
    expect(triggers[2]).toContain("Any");
    expect(html).not.toContain("__any__<");
    expect(html).toContain('name="source"');
    expect(html).toContain('name="minConfidence"');
    expect(html).toContain('value="0.75"');
    expect(html).toContain('name="maxConfidence"');
    expect(html).toContain('name="pageSize"');
    expect(html).not.toContain('name="page"');
    expect(html).toMatch(/name="shared"[^>]*checked/);
  });

  it("removes the source chip without losing other filters or page size", () => {
    const html = render(<SeDomainsTable rows={[]} counts={COUNTS} page={3} pageSize={100} filters={{ ...EMPTY_SE_DOMAINS_FILTERS, source: "brave", shared: "1" }} />);
    expect(html).toContain('aria-label="Remove filter Source Brave"');
    expect(html).toContain('href="/admin/se/companies/domains?shared=1&amp;pageSize=100"');
    expect(html).toContain('href="/admin/se/companies/domains?pageSize=100"');
    expect(html).toContain("Clear all");
    expect(html).not.toContain('name="source"');
  });
});
