import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, MemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { AdminSidebar } from "~/components/admin/admin-sidebar";
import { SidebarProvider } from "~/components/ui/sidebar";
import type { NatsMonitorResult, NatsSnapshot } from "~/lib/nats-monitor";
import jszFixture from "./fixtures/nats-jsz.json";
import varzFixture from "./fixtures/nats-varz.json";

const server = vi.hoisted(() => ({ loadNatsMonitor: vi.fn() }));
vi.mock("~/lib/nats-monitor.server", () => server);
import AdminLayout from "~/routes/admin-layout";
import AdminNats, { loader } from "~/routes/admin-nats";

// The page is rendered from the same real payloads the server module is tested
// against, normalized by the real module rather than hand-built.
async function realSnapshot(jsz: unknown = jszFixture): Promise<NatsSnapshot> {
  const actual = await vi.importActual<typeof import("~/lib/nats-monitor.server")>(
    "~/lib/nats-monitor.server",
  );
  const result = await actual.loadNatsMonitor({
    url: "http://192.168.88.129:8222",
    // 2026-09-17T20:05:00Z, eight seconds after the capture's last delivery.
    now: () => 1_789_675_500_000,
    fetchImpl: (async (input: RequestInfo | URL) =>
      Response.json(
        new URL(String(input)).pathname === "/varz" ? varzFixture : jsz,
      )) as typeof fetch,
  });
  if (!result.snapshot) throw new Error(result.error.message);
  return result.snapshot;
}

function render(result: NatsMonitorResult) {
  const element = (
    <AdminNats
      {...({ loaderData: result } as unknown as Parameters<typeof AdminNats>[0])}
    />
  );
  const router = createMemoryRouter([{ path: "/admin/nats", element }], {
    initialEntries: ["/admin/nats"],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

/** The markup of one stream's block, so assertions cannot leak across streams. */
function streamBlock(html: string, name: string): string {
  const block = html
    .split("<article")
    .find((chunk) => chunk.includes(`id="stream-${name}"`));
  if (!block) throw new Error(`no block for stream ${name}`);
  return block;
}

describe("NATS page", () => {
  it("loads one monitoring snapshot", async () => {
    const result: NatsMonitorResult = { snapshot: await realSnapshot(), error: null };
    server.loadNatsMonitor.mockResolvedValue(result);
    expect(await loader()).toBe(result);
  });

  it("shows the server and JetStream usage against its limits", async () => {
    const html = render({ snapshot: await realSnapshot(), error: null });
    expect(html).toContain("nats-server 2.14.7");
    expect(html).toContain("3h54m2s");
    expect(html).toContain("http://192.168.88.129:8222");
    // File store: used of limit, plus what streams have reserved.
    expect(html).toContain("531 B");
    expect(html).toContain("90 GiB");
    expect(html).toContain("1 GiB reserved");
    expect(html).toContain("4 GiB");
    // API errors are surfaced, not hidden in a total.
    expect(html).toContain("7 errors");
  });

  it("shows every stream with its settings and state", async () => {
    const html = render({ snapshot: await realSnapshot(), error: null });
    const crawl = streamBlock(html, "CRAWL_PAGES");
    expect(crawl).toContain("crawl.pages.&gt;");
    expect(crawl).toContain("Work queue");
    expect(crawl).toContain("File");
    expect(crawl).toContain("354 B");
    expect(crawl).toContain("1 GiB");
    expect(crawl).toContain("7d");
    expect(crawl).toContain("5 – 10");
    expect(crawl).not.toContain("No size cap");

    const events = streamBlock(html, "EVENTS");
    expect(events).toContain("events.&gt;");
    expect(events).toContain("Limits");
    expect(events).toContain("No size cap");
    expect(events).toContain("No consumers");
  });

  it("shows how far each consumer has processed", async () => {
    const crawl = streamBlock(
      render({ snapshot: await realSnapshot(), error: null }),
      "CRAWL_PAGES",
    );
    expect(crawl).toContain("analyzer");
    for (const column of ["Unprocessed", "In flight", "Redelivered", "Waiting pulls", "Ack floor"]) {
      expect(crawl).toContain(column);
    }
    const row = crawl.split("<tr").find((chunk) => chunk.includes("analyzer"));
    // unprocessed 4, in flight 2, redelivered 1, waiting 0, ack floor 4
    expect(row?.match(/<td[^>]*>.*?<\/td>/g)?.map((cell) => cell.replace(/<[^>]+>/g, ""))).toEqual(
      expect.arrayContaining(["4", "2", "1", "0"]),
    );
    expect(row).toContain("Pull");
    expect(row).toContain("1s");
  });

  it("says so when the server holds no streams", async () => {
    const empty = {
      ...jszFixture,
      streams: 0,
      consumers: 0,
      messages: 0,
      bytes: 0,
      account_details: [{ name: "CORPSCOUT", id: "CORPSCOUT" }],
    };
    const html = render({ snapshot: await realSnapshot(empty), error: null });
    expect(html).toContain("No streams yet");
    expect(html).toContain("nats-server 2.14.7");
  });

  it("says so when the server runs without JetStream", async () => {
    const html = render({
      snapshot: await realSnapshot({ server_id: "X", now: "2026-09-17T20:04:52Z", disabled: true }),
      error: null,
    });
    expect(html).toContain("JetStream is not enabled");
    expect(html).not.toContain("No streams yet");
  });

  it("shows an unreachable server as a state of the page, with a way to retry", () => {
    const html = render({
      snapshot: null,
      error: {
        kind: "unreachable",
        message: "NATS monitoring at http://192.168.88.129:8222 did not answer: fetch failed",
      },
    });
    expect(html).toContain("NATS server unreachable");
    expect(html).toContain("did not answer: fetch failed");
    expect(html).toContain("Refresh");
    // No section pretends to describe a server that did not answer.
    expect(html).not.toContain('id="nats-server-title"');
    expect(html).not.toContain('id="nats-jetstream-title"');
    expect(html).not.toContain("Online");
  });

  it("explains the missing setting when monitoring is not configured", () => {
    const html = render({
      snapshot: null,
      error: { kind: "not_configured", message: "Set NATS_MONITOR_URL on the backoffice." },
    });
    expect(html).toContain("NATS monitoring is not configured");
    expect(html).toContain("NATS_MONITOR_URL");
  });

  it("is listed in the Workspace navigation and marked active", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter initialEntries={["/admin/nats"]}>
        <SidebarProvider>
          <AdminSidebar />
        </SidebarProvider>
      </MemoryRouter>,
    );
    const item = html.split("<li").find((chunk) => chunk.includes('href="/admin/nats"'));
    expect(item).toContain(">NATS<");
    expect(item).toContain('data-active=""');
  });

  it("has an Admin › NATS breadcrumb", () => {
    const router = createMemoryRouter(
      [{ path: "/admin", element: <AdminLayout />, children: [{ path: "nats", element: <p>page</p> }] }],
      { initialEntries: ["/admin/nats"] },
    );
    const html = renderToStaticMarkup(<RouterProvider router={router} />);
    const crumbs = html.split('aria-label="breadcrumb"')[1]?.split("</nav>")[0] ?? "";
    expect(crumbs).toContain(">Admin<");
    expect(crumbs).toContain(">NATS<");
  });
});
