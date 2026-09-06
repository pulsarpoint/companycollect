import { renderToStaticMarkup } from "react-dom/server";
import {
  createMemoryRouter,
  MemoryRouter,
  RouterProvider,
} from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { CommonCrawlTable } from "~/components/admin/common-crawl-table";
import { AdminSidebar } from "~/components/admin/admin-sidebar";
import { SidebarProvider } from "~/components/ui/sidebar";
import type { CommonCrawlSearchRow } from "~/lib/common-crawl.server";

const searchCommonCrawlDomains = vi.fn(async () => ({ rows: [], total: 0 }));

vi.mock("~/lib/common-crawl.server", () => ({ searchCommonCrawlDomains }));

const { loader } = await import("~/routes/admin-common-crawl");

function get(search: string) {
  return loader({
    request: new Request(`http://backoffice/admin/common-crawl${search}`),
  } as unknown as Parameters<typeof loader>[0]);
}

const resultRow: CommonCrawlSearchRow = {
  rootDomain: "example.com",
  organizationName: "Example Company",
  address: "Example Street 1, 111 22 Stockholm, SE",
  industryCode: "62.01",
  industryLabel: "Computer programming activities",
  latestCrawlId: "CC-MAIN-2026-30",
  latestPageCount: 18,
  crawlCount: 3,
  observedAt: "2026-07-25 12:00:00.000",
};

function renderTable({
  rows = [],
  total = rows.length,
  searched = false,
}: {
  rows?: CommonCrawlSearchRow[];
  total?: number;
  searched?: boolean;
} = {}): string {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <CommonCrawlTable
            rows={rows}
            total={total}
            searched={searched}
            filters={{ domain: "", address: "", industry: "" }}
            view={{ page: 1, pageSize: 50 }}
          />
        ),
      },
    ],
    { initialEntries: ["/admin/common-crawl"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("admin Common Crawl loader", () => {
  beforeEach(() => searchCommonCrawlDomains.mockClear());

  it("does not query ClickHouse before a search is entered", async () => {
    const result = await get("");

    expect(searchCommonCrawlDomains).not.toHaveBeenCalled();
    expect(result.searched).toBe(false);
    expect(result.rows).toEqual([]);
  });

  it("normalizes filters and passes URL-backed paging to the server search", async () => {
    await get(
      "?domain=https%3A%2F%2Fwww.Example.com%2Fabout&address=Stockholm&industry=62.01&page=3&pageSize=100",
    );

    expect(searchCommonCrawlDomains).toHaveBeenCalledWith(
      {
        domain: "example.com",
        address: "Stockholm",
        industry: "62.01",
      },
      3,
      100,
    );
  });

  it("returns a validation message without running a broad one-character search", async () => {
    const result = await get("?address=S");

    expect(searchCommonCrawlDomains).not.toHaveBeenCalled();
    expect(result.filterError).toContain("at least 3 characters");
  });
});

describe("Common Crawl search table", () => {
  it("renders the three filters and an orientation empty state", () => {
    const html = renderTable();

    expect(html).toContain('name="domain"');
    expect(html).toContain('name="address"');
    expect(html).toContain('name="industry"');
    expect(html).toContain("Search the Common Crawl evidence index");
  });

  it("renders source-summary rows linked to the domain evidence page", () => {
    const html = renderTable({ rows: [resultRow], searched: true });

    expect(html).toContain('href="/admin/common-crawl/example.com"');
    expect(html).toContain("Example Company");
    expect(html).toContain("Example Street 1");
    expect(html).toContain("Computer programming activities");
    expect(html).toContain("CC-MAIN-2026-30");
    expect(html).toContain("18 pages");
  });

  it("shows a specific no-match result after a search", () => {
    expect(renderTable({ searched: true })).toContain(
      "No Common Crawl domains match these filters.",
    );
  });
});

describe("admin sidebar Common Crawl entry", () => {
  function sidebarAt(path: string): string {
    return renderToStaticMarkup(
      <MemoryRouter initialEntries={[path]}>
        <SidebarProvider>
          <AdminSidebar />
        </SidebarProvider>
      </MemoryRouter>,
    );
  }

  it("links the workspace and stays active on index and detail pages", () => {
    for (const path of [
      "/admin/common-crawl",
      "/admin/common-crawl/example.com",
    ]) {
      const entry = sidebarAt(path)
        .split("<li")
        .find((chunk) => chunk.includes('href="/admin/common-crawl"'));
      expect(entry).toContain(">Common Crawl<");
      expect(entry).toContain('data-active=""');
    }
  });
});
