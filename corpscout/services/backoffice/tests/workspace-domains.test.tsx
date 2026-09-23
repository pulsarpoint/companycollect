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
    new URLSearchParams("prefix=EXAMPLE&companies=with&webtech=with"),
  );
  expect(workspaceDomainsHref(filters, "example.se")).toBe(
    "/admin/domains?prefix=example&companies=with&webtech=with&after=example.se",
  );
  expect(workspaceWebtechHref("example.se", "shop.example.se")).toBe(
    "/admin/domains/example.se/web-technologies?site=shop.example.se",
  );
  expect(
    parseWorkspaceDomainFilters(new URLSearchParams("webtech=invalid")).webtech,
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
                companies: 2,
                archived: 3,
                dns: 4,
                webtech: 5,
                company_records: [],
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
    "Archived technologies",
    "DNS technologies",
    "Webtech",
    "Sites",
    "5 detected",
    "View sites",
  ])
    expect(html).toContain(label);
  expect(html).toContain('aria-expanded="false"');
  expect(html).toContain('aria-label="Expand sites for example.se"');
  expect(html).toContain('href="/admin/domains/example.se"');
  expect(html).toContain('href="/admin/domains/example.se/web-technologies"');
});
