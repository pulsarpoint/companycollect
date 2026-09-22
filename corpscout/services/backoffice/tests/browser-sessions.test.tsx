import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { SavedBrowserSession } from "~/lib/browser-sessions";

const server = vi.hoisted(() => ({ loadBrowserSessions: vi.fn(), browserSessionAction: vi.fn() }));
vi.mock("~/lib/browser-sessions.server", () => server);
import AdminBrowserSessions, { action, loader } from "~/routes/admin-browser-sessions";

beforeEach(() => vi.clearAllMocks());

describe("saved browser sessions", () => {
  it("shows independent sessions and preserves profiles when stopped", () => {
    const sessions: SavedBrowserSession[] = [
      { id: "11111111111111111111111111111111", state: "running", pinned: true, request_id: null, lease_id: null, domain: null, generation: "one", saved_at: null, error: null, tabs: [], retained_until: 1800000000, label: "", execution_id: "e".repeat(32) },
      { id: "22222222222222222222222222222222", state: "stopped", pinned: false, request_id: null, lease_id: null, domain: null, generation: null, saved_at: null, error: null, tabs: [], retained_until: 1800000000, label: "", execution_id: null },
    ];
    const element = <AdminBrowserSessions {...({ loaderData: { sessions, error: null } } as Parameters<typeof AdminBrowserSessions>[0])} />;
    const router = createMemoryRouter([{ path: "/admin/browser-sessions", element }], { initialEntries: ["/admin/browser-sessions"] });
    const html = renderToStaticMarkup(<RouterProvider router={router} />);
    for (const label of ["11111111111111111111111111111111", "22222222222222222222222222222222", "Stop and save", "Open headed", "Open browser"]) expect(html).toContain(label);
    expect(html).toContain('aria-label="Keep session 11111111111111111111111111111111"');
    expect(html).toContain('aria-label="Keep session 22222222222222222222222222222222"');
  });

  it("shows the assigned domain and protects the scan controls", () => {
    const sessions: SavedBrowserSession[] = [{ id: "11111111111111111111111111111111", state: "running", pinned: true, request_id: "crawl-one", lease_id: "0123456789abcdef0123456789abcdef", domain: "company.test", generation: "one", saved_at: null, error: null, tabs: [], retained_until: 1800000000, label: "", execution_id: "e".repeat(32) }];
    const element = <AdminBrowserSessions {...({ loaderData: { sessions, error: null } } as Parameters<typeof AdminBrowserSessions>[0])} />;
    const router = createMemoryRouter([{ path: "/admin/browser-sessions", element }], { initialEntries: ["/admin/browser-sessions"] });
    const html = renderToStaticMarkup(<RouterProvider router={router} />);
    expect(html).toContain("company.test");
    expect(html).toContain("crawl-one");
    expect(html).toContain("0123456789abcdef0123456789abcdef");
    expect(html).toContain("in use");
    expect(html).toMatch(/<button[^>]*disabled[^>]*>Stop and save<\/button>/);
    expect(html).not.toMatch(/<button[^>]*disabled[^>]*>[^<]*Open browser/);
  });

  it("rejects cross-origin and invalid actions before contacting browsers", async () => {
    for (const [origin, sessionId, intent] of [["https://foreign.test", "11111111111111111111111111111111", "stop"], ["http://backoffice", "../profile", "start"], ["http://backoffice", "11111111111111111111111111111111", "delete"]]) {
      const request = new Request("http://backoffice/admin/browser-sessions", { method: "POST", headers: { Origin: origin }, body: new URLSearchParams({ session_id: sessionId, intent }) });
      expect((await action({ request } as Parameters<typeof action>[0])).error).toBeTruthy();
    }
    expect(server.browserSessionAction).not.toHaveBeenCalled();
  });

  it("opens a tab within the selected session", async () => {
    server.browserSessionAction.mockResolvedValue({});
    const request = new Request("http://backoffice/admin/browser-sessions", { method: "POST", headers: { Origin: "http://backoffice" }, body: new URLSearchParams({ session_id: "22222222222222222222222222222222", intent: "open-tab", url: "https://melexis.com/" }) });
    expect((await action({ request } as Parameters<typeof action>[0])).error).toBeNull();
    expect(server.browserSessionAction).toHaveBeenCalledWith("22222222222222222222222222222222", "open-tab", { url: "https://melexis.com/", tabId: "", executionId: undefined });
  });

  it("reports service errors without losing the page", async () => {
    server.loadBrowserSessions.mockRejectedValue(new Error("Unavailable"));
    expect(await loader()).toEqual({ sessions: [], error: "Unavailable" });
  });

  it("validates and forwards the per-session retention pin", async () => {
    server.browserSessionAction.mockResolvedValue({});
    for (const value of ["false", "true", "invalid"]) {
      const request = new Request("http://backoffice/admin/browser-sessions", { method: "POST", headers: { Origin: "http://backoffice" }, body: new URLSearchParams({ session_id: "11111111111111111111111111111111", intent: "settings", pinned: value }) });
      const result = await action({ request } as Parameters<typeof action>[0]);
      if (value === "invalid") expect(result.error).toBeTruthy();
      else {
        expect(result.error).toBeNull();
        expect(server.browserSessionAction).toHaveBeenLastCalledWith("11111111111111111111111111111111", "settings", { pinned: value === "true" });
      }
    }
    expect(server.browserSessionAction).toHaveBeenCalledTimes(2);
  });
});
