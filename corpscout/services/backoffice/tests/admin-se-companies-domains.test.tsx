import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

const server = vi.hoisted(() => ({
  listSeDomainsPage: vi.fn(),
  loadSeDomainsCounts: vi.fn(),
}));
vi.mock("~/lib/se-domains-list.server", () => server);
import { loader } from "~/routes/admin-se-companies-domains";
import { SeDomainsTable } from "~/components/admin/se-domains-table";
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
  });

  it("pages and counts under the same filters", async () => {
    const data = await loader({
      request: new Request(
        "http://x/admin/se/companies/domains?minConfidence=0.7&shared=1&page=3&pageSize=50",
      ),
    } as never);
    const filters = { ...EMPTY_SE_DOMAINS_FILTERS, minConfidence: "0.7", shared: "1" };
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
    expect(html).toContain('name="minConfidence"');
    expect(html).toContain('value="0.7"');
    expect(html).toContain('name="maxConfidence"');
    expect(html).toMatch(/name="shared"[^>]*checked/);
    expect(html).toContain("Shared domains");
    // The Clear link drops every filter.
    expect(html).toContain('href="/admin/se/companies/domains"');
  });

  it("labels the select triggers with words, never the Any sentinel or a stored value", () => {
    const html = render(
      <SeDomainsTable
        rows={[]}
        counts={COUNTS}
        page={1}
        pageSize={50}
        filters={{ ...EMPTY_SE_DOMAINS_FILTERS, association: "not_connected" }}
      />,
    );
    // The filter bar precedes the table; the pager's own page-size select follows it.
    const filterBar = html.slice(0, html.indexOf("<table"));
    const triggers = filterBar.match(/<button[^>]*role="combobox"[^>]*>.*?<\/button>/gs) ?? [];
    expect(triggers).toHaveLength(2);
    expect(triggers[0]).toContain("not connected");
    expect(triggers[0]).not.toContain("not_connected");
    expect(triggers[1]).toContain("Any");
    expect(html).not.toContain("__any__<");
  });
});
