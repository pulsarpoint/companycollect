import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { expect, it } from "vitest";
import { WorkspaceDomainsTable } from "~/components/admin/workspace-domains-table";
import {
  parseWorkspaceDomainFilters,
  workspaceDomainsHref,
  workspaceWebtechHref,
} from "~/lib/workspace-domains";

it("retains filters during cursor pagination and safely encodes hostnames", () => {
  const filters = parseWorkspaceDomainFilters(
    new URLSearchParams("prefix=EXAMPLE&companies=with&dns=with&source=se_company_domain&source=commoncrawl&sourceMatch=all"),
  );
  expect(workspaceDomainsHref(filters, "example.se")).toBe(
    "/admin/domains?prefix=example&source=commoncrawl&source=se_company_domain&sourceMatch=all&dns=with&companies=with&after=example.se",
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
});
