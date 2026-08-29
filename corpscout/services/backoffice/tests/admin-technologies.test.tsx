/**
 * `/admin/technologies`: the loader's URL wiring (queries faked at the
 * server-module boundary), the index table's rendering (rows, detail links,
 * search wiring, adoption '—' vs count), and the sidebar's Technologies
 * entry with its whole-subtree active marking.
 */
import { renderToStaticMarkup } from "react-dom/server";
import {
  createMemoryRouter,
  MemoryRouter,
  RouterProvider,
} from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AdminSidebar } from "~/components/admin/admin-sidebar";
import { TechnologiesTable } from "~/components/admin/technologies-table";
import { SidebarProvider } from "~/components/ui/sidebar";
import type { TechnologyListRow } from "~/lib/technologies.server";

const listTechnologiesPage = vi.fn(async (): Promise<TechnologyListRow[]> => []);
const countTechnologies = vi.fn(async () => 0);
const loadTechnologyCategoryOptions = vi.fn(async (): Promise<string[]> => []);

vi.mock("~/lib/technologies.server", () => ({
  listTechnologiesPage,
  countTechnologies,
  loadTechnologyCategoryOptions,
}));

const { loader } = await import("~/routes/admin-technologies");

function get(search: string) {
  return loader({
    request: new Request(`http://backoffice/admin/technologies${search}`),
  } as unknown as Parameters<typeof loader>[0]);
}

function row(overrides: Partial<TechnologyListRow>): TechnologyListRow {
  return {
    technology: "WordPress",
    slug: "wordpress",
    description: "The web's most common CMS.",
    website: "https://wordpress.org",
    categories: ["CMS", "Blogs"],
    has_icon: 1,
    saas: 0,
    oss: 1,
    domain_count: "",
    ...overrides,
  };
}

function render(
  rows: TechnologyListRow[],
  {
    total = rows.length,
    filters = { q: "", category: "" },
    categories = ["CMS"],
  } = {},
): string {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <TechnologiesTable
            rows={rows}
            total={total}
            filters={filters}
            categories={categories}
            view={{ page: 1, pageSize: 50 }}
          />
        ),
      },
    ],
    { initialEntries: ["/admin/technologies"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("admin-technologies loader", () => {
  beforeEach(() => {
    listTechnologiesPage.mockClear();
    countTechnologies.mockClear();
    loadTechnologyCategoryOptions.mockClear();
  });

  it("defaults to page 1, the shared default page size, and no filters", async () => {
    await get("");
    expect(listTechnologiesPage).toHaveBeenCalledWith(
      { q: "", category: "" },
      1,
      50,
    );
    expect(countTechnologies).toHaveBeenCalledWith({ q: "", category: "" });
    expect(loadTechnologyCategoryOptions).toHaveBeenCalledTimes(1);
  });

  it("threads q, category, and paging straight from the URL", async () => {
    await get("?q=word&category=CMS&page=3&pageSize=100");
    expect(listTechnologiesPage).toHaveBeenCalledWith(
      { q: "word", category: "CMS" },
      3,
      100,
    );
    expect(countTechnologies).toHaveBeenCalledWith({ q: "word", category: "CMS" });
  });
});

describe("technologies index table", () => {
  it("renders rows with the proxy icon, a detail link, categories, snippet and badges", () => {
    const html = render([row({})]);

    expect(html).toContain('href="/admin/technologies/wordpress"');
    expect(html).toContain(">WordPress<");
    expect(html).toContain('src="/icons/tech/wordpress"');
    expect(html).toContain(">CMS<");
    expect(html).toContain(">Blogs<");
    expect(html).toContain("most common CMS");
    expect(html).toContain(">OSS<");
    expect(html).not.toContain(">SaaS<");
  });

  it("falls back to the monogram when the catalog has no icon", () => {
    const html = render([row({ has_icon: 0 })]);
    expect(html).toContain('data-slot="technology-monogram"');
    expect(html).not.toContain("/icons/tech/");
  });

  it("shows the adoption count when the rollup has a row, and — while it has none", () => {
    const html = render([
      row({}),
      row({ technology: "Shopify", slug: "shopify", saas: 1, domain_count: "12345" }),
    ]);
    expect(html).toContain("12,345");
    expect(html).toContain("—");
  });

  it("wires the search input and category select to the current filters", () => {
    const html = render([], { filters: { q: "word", category: "" } });
    expect(html).toContain('name="q"');
    expect(html).toContain('value="word"');
    expect(html).toContain("All categories");
  });

  it("says so when nothing matches", () => {
    const html = render([]);
    expect(html).toContain("No technologies match these filters.");
  });
});

describe("admin sidebar", () => {
  function sidebarAt(path: string): string {
    return renderToStaticMarkup(
      <MemoryRouter initialEntries={[path]}>
        <SidebarProvider>
          <AdminSidebar />
        </SidebarProvider>
      </MemoryRouter>,
    );
  }

  it("links Technologies in the Workspace section", () => {
    const html = sidebarAt("/admin/se/people");
    expect(html).toContain('href="/admin/technologies"');
    expect(html).toContain(">Technologies<");
  });

  it("marks the entry active on the index AND every detail page", () => {
    for (const path of ["/admin/technologies", "/admin/technologies/wordpress"]) {
      const html = sidebarAt(path);
      const entry = html
        .split("<li")
        .find((chunk) => chunk.includes('href="/admin/technologies"'));
      expect(entry).toBeDefined();
      // Base UI marks active buttons with a present-empty data-active.
      expect(entry).toContain('data-active=""');
    }
  });

  it("leaves the entry inactive elsewhere", () => {
    const entry = sidebarAt("/admin/esef")
      .split("<li")
      .find((chunk) => chunk.includes('href="/admin/technologies"'));
    expect(entry).toBeDefined();
    expect(entry).not.toContain('data-active=""');
  });
});
