import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, MemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AdminSidebar } from "~/components/admin/admin-sidebar";
import { SidebarProvider } from "~/components/ui/sidebar";
import type {
  DomainGraphRelease,
  DomainGraphResult,
} from "~/lib/domain-graph.server";

const { getDomainGraphReleases, getDomainGraphImports, searchDomainGraph } =
  vi.hoisted(() => ({
    getDomainGraphReleases: vi.fn(),
    getDomainGraphImports: vi.fn(),
    searchDomainGraph: vi.fn(),
  }));
vi.mock("~/lib/domain-graph.server", () => ({
  getDomainGraphReleases,
  getDomainGraphImports,
  searchDomainGraph,
}));
const { default: AdminGraph, loader } = await import("~/routes/admin-graph");

const release: DomainGraphRelease = {
  graph_release: "cc-main-2026-jun-jul-aug",
  node_count: "119722885",
  edge_count: "2450405793",
  published_at: "2026-09-16 12:00:00.000",
};
const result: DomainGraphResult = {
  found: true,
  total: 3,
  page: 1,
  pageSize: 50,
  counts: { all: 3, mutual: 1, outgoing: 1, incoming: 1 },
  rows: [
    {
      connected_domain: "mutual.se",
      outgoing: 1,
      incoming: 1,
      reciprocal: 1,
      n_hosts: 2,
    },
    {
      connected_domain: "outgoing.se",
      outgoing: 1,
      incoming: 0,
      reciprocal: 0,
      n_hosts: 1,
    },
    {
      connected_domain: "incoming.se",
      outgoing: 0,
      incoming: 1,
      reciprocal: 0,
      n_hosts: 1,
    },
  ],
};

function get(query = "") {
  return loader({
    request: new Request(`http://backoffice/admin/graph${query}`),
  } as Parameters<typeof loader>[0]);
}

function renderPage(loaderData: Awaited<ReturnType<typeof loader>>): string {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <AdminGraph
            {...({ loaderData } as Parameters<typeof AdminGraph>[0])}
          />
        ),
      },
    ],
    {
      initialEntries: ["/admin/graph?domain=example.com"],
    },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

beforeEach(() => {
  getDomainGraphReleases.mockReset().mockResolvedValue([release]);
  getDomainGraphImports.mockReset().mockResolvedValue([]);
  searchDomainGraph.mockReset().mockResolvedValue(result);
});

describe("Graph route", () => {
  it("offers an importing release and shows its status before publication", async () => {
    getDomainGraphReleases.mockResolvedValue([]);
    getDomainGraphImports.mockResolvedValue([
      {
        graph_release: release.graph_release,
        label: "Importing",
        active: true,
        run_url: "https://dagster.example/runs/active",
      },
    ]);
    const data = await get("?domain=example.com");
    expect(data.search.release).toBe(release.graph_release);
    expect(searchDomainGraph).not.toHaveBeenCalled();
    const html = renderPage(data);
    expect(html).toContain(`${release.graph_release} — Importing`);
    expect(html).toContain("automatically every 10 seconds");
    expect(html).toContain('href="https://dagster.example/runs/active"');
    expect(html).not.toContain("No graph release is ready yet");
  });

  it("uses the published snapshot once an importing release becomes available", async () => {
    getDomainGraphImports.mockResolvedValue([
      {
        graph_release: release.graph_release,
        label: "Awaiting publication",
        active: true,
        run_url: null,
      },
    ]);
    const data = await get(
      `?domain=example.com&release=${release.graph_release}`,
    );
    expect(data.imports).toEqual([]);
    expect(searchDomainGraph).toHaveBeenCalledOnce();
  });

  it("keeps published graphs searchable when Dagster status fails", async () => {
    getDomainGraphImports.mockRejectedValue(new Error("internal endpoint"));
    const data = await get("?domain=example.com");
    expect(searchDomainGraph).toHaveBeenCalledOnce();
    const html = renderPage(data);
    expect(html).toContain("Import status unavailable");
    expect(html).not.toContain("internal endpoint");
  });

  it("shows failed imports without presenting them as searchable", async () => {
    getDomainGraphReleases.mockResolvedValue([]);
    getDomainGraphImports.mockResolvedValue([
      {
        graph_release: release.graph_release,
        label: "Import failed",
        active: false,
        run_url: null,
      },
    ]);
    const html = renderPage(await get("?domain=example.com"));
    expect(searchDomainGraph).not.toHaveBeenCalled();
    expect(html).toContain("Import failed");
    expect(html).not.toContain("automatically every 10 seconds");
  });
  it("offers releases without running a graph search before a domain is entered", async () => {
    const data = await get();
    expect(searchDomainGraph).not.toHaveBeenCalled();
    expect(renderPage(data)).toContain("Explore a domain’s connections");
  });

  it("normalizes pasted URLs and sends direction and paging to the graph query", async () => {
    await get(
      "?domain=https://www.Example.com/about&direction=mutual&page=3&pageSize=100",
    );
    expect(searchDomainGraph).toHaveBeenCalledWith({
      domain: "example.com",
      release: release.graph_release,
      direction: "mutual",
      page: 3,
      pageSize: 100,
    });
  });

  it("does not search an unpublished or invalid release", async () => {
    const data = await get("?domain=example.com&release=not-published");
    expect(searchDomainGraph).not.toHaveBeenCalled();
    expect(data.error).toContain("not available");
  });

  it("does not search an invalid domain", async () => {
    const data = await get("?domain=a..com");
    expect(searchDomainGraph).not.toHaveBeenCalled();
    expect(data.error).toContain("Enter a domain");
  });

  it("shows pending publication rather than an empty search result", async () => {
    getDomainGraphReleases.mockResolvedValue([]);
    const html = renderPage(await get("?domain=example.com"));
    expect(searchDomainGraph).not.toHaveBeenCalled();
    expect(html).toContain("No graph release is ready yet");
    expect(html).not.toContain("Domain not found");
  });

  it("shows a retryable error without exposing database errors", async () => {
    searchDomainGraph.mockRejectedValue(new Error("private-internal-address"));
    const html = renderPage(await get("?domain=example.com"));
    expect(html).toContain("Graph unavailable");
    expect(html).toContain("Retry");
    expect(html).not.toContain("private-internal-address");
  });

  it("distinguishes absent domains, isolated domains, and an empty direction filter", async () => {
    searchDomainGraph.mockResolvedValue({
      ...result,
      found: false,
      rows: [],
      total: 0,
    });
    expect(renderPage(await get("?domain=example.com"))).toContain(
      "Domain not found in this release",
    );
    searchDomainGraph.mockResolvedValue({
      ...result,
      rows: [],
      total: 0,
      counts: { all: 0, mutual: 0, outgoing: 0, incoming: 0 },
    });
    expect(renderPage(await get("?domain=example.com"))).toContain(
      "No connections in this release",
    );
    searchDomainGraph.mockResolvedValue({ ...result, rows: [], total: 0 });
    expect(
      renderPage(await get("?domain=example.com&direction=incoming")),
    ).toContain("No connections in this direction");
  });

  it("shows both directions for every neighbor and only marks reciprocal pairs mutual", async () => {
    const html = renderPage(await get("?domain=example.com"));
    const rows = html.split("<tr");
    const mutual = rows.find((row) => row.includes(">mutual.se</a>"))!;
    const outgoing = rows.find((row) => row.includes(">outgoing.se</a>"))!;
    const incoming = rows.find((row) => row.includes(">incoming.se</a>"))!;
    expect(mutual).toContain("Mutual");
    expect((mutual.match(/Yes/g) ?? []).length).toBe(2);
    expect(outgoing).not.toContain("Mutual");
    expect(outgoing.indexOf("Yes")).toBeLessThan(outgoing.indexOf(">No<"));
    expect(incoming.indexOf(">No<")).toBeLessThan(incoming.indexOf("Yes"));
    expect(html).toContain("Links back to searched domain");
    expect(html).toContain(
      `href="/admin/graph?domain=mutual.se&amp;release=${release.graph_release}&amp;pageSize=50"`,
    );
    expect(html).toContain('href="/admin/common-crawl/mutual.se"');
    expect(html).toContain("Outgoing only");
    expect(html).toContain("Incoming only");
  });

  it("adds an active Graph entry to Workspace navigation", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter initialEntries={["/admin/graph"]}>
        <SidebarProvider>
          <AdminSidebar />
        </SidebarProvider>
      </MemoryRouter>,
    );
    const item = html
      .split("<li")
      .find((chunk) => chunk.includes('href="/admin/graph"'));
    expect(item).toContain(">Graph<");
    expect(item).toContain('data-active=""');
  });
});
