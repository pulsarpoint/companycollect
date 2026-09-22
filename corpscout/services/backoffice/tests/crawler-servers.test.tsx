import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { BrowserServer } from "~/lib/browser-servers.server";

const backend = vi.hoisted(() => ({ loadBrowserServer: vi.fn(), connectBrowserDesktop: vi.fn() }));
vi.mock("~/lib/browser-servers.server", () => backend);
import AdminBrowserServers, { action, loader } from "~/routes/admin-crawler-servers";

beforeEach(() => vi.clearAllMocks());
afterEach(() => { vi.unstubAllEnvs(); vi.unstubAllGlobals(); vi.useRealTimers(); });

describe("crawler servers", () => {
  it("shows the server and every kind of open session with connection controls", () => {
    const server: BrowserServer = { hostname: "crawler-132", version: "0.34.1", healthy: true, idle_timeout_seconds: 120, leases: [], sessions: [
      { id: "a".repeat(32), kind: "saved", name: "browser-1", request_id: null, url: "https://melexis.com/", state: "running", started_at: "2026-09-18T20:00:00Z" },
      { id: "b".repeat(32), kind: "brave", name: "Brave search", request_id: "search-1", url: "https://search.brave.com/", state: "waiting", started_at: "2026-09-18T20:00:00Z" },
      { id: "c".repeat(32), kind: "interactive", name: "Interactive crawl", request_id: "manual-1", url: null, state: "in_use", started_at: "2026-09-18T20:00:00Z" },
    ] };
    const element = <AdminBrowserServers {...({ loaderData: { server, error: null } } as Parameters<typeof AdminBrowserServers>[0])} />;
    const router = createMemoryRouter([{ path: "/admin/crawls/servers", element }], { initialEntries: ["/admin/crawls/servers"] });
    const html = renderToStaticMarkup(<RouterProvider router={router} />);
    for (const value of ["crawler-132", "Online", "browser-1", "Brave search", "Interactive crawl", "Saved sessions", "Browsers"]) expect(html).toContain(value);
    expect(html.match(/>Connect<\/button>/g)).toHaveLength(3);
  });

  it("rejects cross-origin and invalid desktop requests", async () => {
    for (const [origin, id] of [["https://foreign.test", "a".repeat(32)], ["http://backoffice", "../bad"]]) {
      const request = new Request("http://backoffice/admin/crawls/servers", { method: "POST", headers: { Origin: origin }, body: new URLSearchParams({ desktop_id: id }) });
      expect((await action({ request } as Parameters<typeof action>[0])).error).toBeTruthy();
    }
    expect(backend.connectBrowserDesktop).not.toHaveBeenCalled();
  });

  it("connects the chosen desktop and reports backend failures", async () => {
    backend.connectBrowserDesktop.mockResolvedValue("ws://crawler/short-ticket");
    const id = "b".repeat(32);
    const request = new Request("http://backoffice/admin/crawls/servers", { method: "POST", headers: { Origin: "http://backoffice" }, body: new URLSearchParams({ desktop_id: id }) });
    expect(await action({ request } as Parameters<typeof action>[0])).toEqual({ error: null, id, browserUrl: "ws://crawler/short-ticket" });
    expect(backend.connectBrowserDesktop).toHaveBeenCalledWith(id);
    backend.loadBrowserServer.mockRejectedValue(new Error("Unavailable"));
    expect(await loader()).toEqual({ server: null, error: "Unavailable" });
  });

  it("offers termination for active assignments, disables cleanup in progress, and leaves history alone", () => {
    const states = ["starting", "ready", "stopping", "closed", "idle_timeout", "interrupted", "failed"];
    const server: BrowserServer = { hostname: "browser-host", version: "0.2.0", healthy: true, idle_timeout_seconds: 120, sessions: [], leases: states.map((state, index) => ({
      id: String(index + 1).repeat(32), session_id: "a".repeat(32), request_id: `crawl-${index}`, domain: `${index}.test`, profile_id: "browser-1", state, last_request_at: 1, expires_at: 121,
    })) };
    const element = <AdminBrowserServers {...({ loaderData: { server, error: null } } as Parameters<typeof AdminBrowserServers>[0])} />;
    const router = createMemoryRouter([{ path: "/admin/browsers", element }], { initialEntries: ["/admin/browsers"] });
    const html = renderToStaticMarkup(<RouterProvider router={router} />);
    expect(html.match(/aria-label="Terminate session /g)).toHaveLength(3);
    expect(html.match(/>Terminate<\/button>/g)).toHaveLength(2);
    expect(html).toMatch(/<button[^>]*disabled[^>]*>Terminating…<\/button>/);
    for (const index of [4, 5, 6, 7]) expect(html).not.toContain(`aria-label="Terminate session ${String(index).repeat(32)}"`);
  });

  it("releases the exact assignment ID through the authenticated service API", async () => {
    vi.stubEnv("BROWSER_API_URL", "http://browser-service:8081");
    vi.stubEnv("BROWSER_API_TOKEN", "private-token");
    vi.useFakeTimers();
    const id = "a".repeat(32);
    const fetch = vi.fn(async (_url: URL | RequestInfo, init?: RequestInit) => {
      await vi.advanceTimersByTimeAsync(15_000);
      expect(init?.signal?.aborted).toBe(false); // Recycling may outlast the default management timeout.
      return Response.json({ id, state: "released" });
    });
    vi.stubGlobal("fetch", fetch);
    const request = new Request("http://backoffice/admin/browsers", { method: "POST", headers: { Origin: "http://backoffice" }, body: new URLSearchParams({ intent: "terminate", session_id: id, execution_id: "e".repeat(32), desktop_id: "b".repeat(32) }) });
    const result = await action({ request } as Parameters<typeof action>[0]);
    expect(result).toEqual({ error: null, terminatedId: id });
    const [url, init] = fetch.mock.calls[0];
    expect(String(url)).toBe(`http://browser-service:8081/v1/browser/sessions/${id}`);
    expect(init?.method).toBe("DELETE");
    expect(new Headers(init?.headers).get("X-Browser-Execution-Id")).toBe("e".repeat(32));
    expect(new Headers(init?.headers).get("Authorization")).toBe("Bearer private-token");
    expect(JSON.stringify(result)).not.toContain("private-token");
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(backend.connectBrowserDesktop).not.toHaveBeenCalled();
  });

  it("rejects invalid or cross-origin termination without contacting the service", async () => {
    const fetch = vi.fn(); vi.stubGlobal("fetch", fetch);
    for (const [origin, id, intent] of [["https://foreign.test", "a".repeat(32), "terminate"], ["http://backoffice", "../browser-1", "terminate"], ["http://backoffice", "a".repeat(32), "stop-server"]]) {
      const request = new Request("http://backoffice/admin/browsers", { method: "POST", headers: { Origin: origin }, body: new URLSearchParams({ intent, session_id: id }) });
      expect((await action({ request } as Parameters<typeof action>[0])).error).toBeTruthy();
    }
    expect(fetch).not.toHaveBeenCalled();
    expect(backend.connectBrowserDesktop).not.toHaveBeenCalled();
  });

  it("reports a failed release without claiming the assignment was terminated", async () => {
    vi.stubEnv("BROWSER_API_URL", "http://browser-service:8081");
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ detail: "Browser cleanup failed" }, { status: 503 })));
    const request = new Request("http://backoffice/admin/browsers", { method: "POST", headers: { Origin: "http://backoffice" }, body: new URLSearchParams({ intent: "terminate", session_id: "a".repeat(32) }) });
    expect(await action({ request } as Parameters<typeof action>[0])).toEqual({ error: "Browser cleanup failed" });
  });
});
