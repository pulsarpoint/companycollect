import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { expect, it } from "vitest";
import { WorkspaceDomainsTable } from "~/components/admin/workspace-domains-table";
import WorkspaceDomains from "~/routes/admin-domains";
import WorkspaceDomain from "~/routes/admin-domain";
import {
  parseWorkspaceDomainFilters,
  workspaceDomainsHref,
  workspaceWebtechHref,
} from "~/lib/workspace-domains";

it("retains filters during cursor pagination and safely encodes hostnames", () => {
  const filters = parseWorkspaceDomainFilters(
    new URLSearchParams("prefix=EXAMPLE&suffix=.SE&companies=with&dns=with&source=se_company_domain&source=commoncrawl&sourceMatch=all"),
  );
  expect(workspaceDomainsHref(filters, "example.se")).toBe(
    "/admin/domains?prefix=example&suffix=se&source=commoncrawl&source=se_company_domain&sourceMatch=all&dns=with&companies=with&after=example.se",
  );
  expect(workspaceWebtechHref("example.se", "shop.example.se")).toBe(
    "/admin/domains/example.se/web-technologies?site=shop.example.se",
  );
  expect(
    parseWorkspaceDomainFilters(new URLSearchParams("dns=invalid")).dns,
  ).toBe("any");
});

it("renders separate source counts and an expandable root row", () => {
  const router = createMemoryRouter(
    [
      {
        path: "/admin/domains",
        element: (
          <WorkspaceDomainsTable
            rows={[
              {
                root_domain: "example.se",
                sources: ["commoncrawl", "se_company_domain"],
                has_dns_records: 1, dns_last_observed_at: "2026-09-24 10:00:00",
                website_count: 3, observed_website_count: 2, company_count: 2,
                first_seen_at: "2026-09-20", last_seen_at: "2026-09-24", refreshed_at: "2026-09-24",
              },
            ]}
          />
        ),
      },
    ],
    { initialEntries: ["/admin/domains"] },
  );
  const html = renderToStaticMarkup(<RouterProvider router={router} />);
  for (const label of [
    "DNS records", "Websites", "Companies", "Common Crawl pages", "Swedish companies", "3 websites", "2 observed",
  ])
    expect(html).toContain(label);
  expect(html).toContain('aria-expanded="false"');
  expect(html).toContain('aria-label="Expand websites for example.se"');
  expect(html).toContain('href="/admin/domains/example.se"');
  expect(html).toContain('href="/admin/domains/example.se/dns"');
  expect(html).toContain('href="/admin/domains/example.se/crawl"');
  expect(html).toContain('aria-label="View crawls for example.se"');
});

it("distinguishes the matching count from the full inventory", () => {
  const loaderData = {
    rows: [], filters: parseWorkspaceDomainFilters(new URLSearchParams("suffix=se")),
    after: "", next: "", hasMore: false, total: "123142485", matchingTotal: "676571",
    refreshedAt: "2026-09-24", websiteInventoryTotal: "1",
  };
  const router = createMemoryRouter([{ path: "/admin/domains", element: <WorkspaceDomains {...({ loaderData } as unknown as Parameters<typeof WorkspaceDomains>[0])} /> }], { initialEntries: ["/admin/domains?suffix=se"] });
  const html = renderToStaticMarkup(<RouterProvider router={router} />);
  expect(html).toContain("676,571 domains match the current filters");
  expect(html).toContain("123,142,485 domains in the inventory snapshot");
  expect(html).toContain("0 of 676,571 matching domains shown");
  expect(html).toContain("Select all 676,571 matching domains");
  expect(html).toContain("Add to crawl queue");
  expect(html).toContain("Not attempted");
  expect(html).toContain("Previously attempted");
  expect(html).toContain('aria-label="Select every domain on this page"');
});

it("keeps the crawl tab in the global domain view", () => {
  const router = createMemoryRouter([{ path: "/admin/domains/:domain/crawl", element: <WorkspaceDomain {...({ loaderData: { domain: "example.se" } } as Parameters<typeof WorkspaceDomain>[0])} /> }], { initialEntries: ["/admin/domains/example.se/crawl"] });
  const html = renderToStaticMarkup(<RouterProvider router={router} />);
  expect(html).toContain('href="/admin/domains/example.se/crawl"');
  expect(html).toMatch(/<a[^>]*aria-selected="true"[^>]*href="\/admin\/domains\/example.se\/crawl"/);
});
