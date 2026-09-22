import { afterEach, describe, expect, it, vi } from "vitest";
import { action } from "~/routes/admin-browser-settings";
afterEach(() => { vi.unstubAllEnvs(); vi.unstubAllGlobals(); });
describe("browser settings", () => {
  it("saves validated capacity and retention and resets the SQLite override through the service", async () => {
    vi.stubEnv("BROWSER_API_URL", "http://browser-service:8081");
    const fetch = vi.fn(async (_url: URL | RequestInfo, _init?: RequestInit) => Response.json({}, { status: 202 }));
    vi.stubGlobal("fetch", fetch);
    for (const [intent, values, method] of [
      ["settings", { max_browsers: "3", idle_timeout_seconds: "180", session_retention_days: "7" }, "PUT"],
      ["reset", {}, "DELETE"],
    ] as const) {
      const request = new Request("http://backoffice/admin/browsers", { method: "POST", headers: { Origin: "http://backoffice" }, body: new URLSearchParams({ intent, ...values }) });
      expect((await action({ request } as Parameters<typeof action>[0])).error).toBeNull();
      expect(String(fetch.mock.calls.at(-1)?.[0])).toBe("http://browser-service:8081/v1/browser/settings");
      expect(fetch.mock.calls.at(-1)?.[1]).toMatchObject({ method });
    }
    expect(JSON.parse((fetch.mock.calls[0] as unknown as [URL, RequestInit])[1].body as string)).toEqual({ max_browsers: 3, idle_timeout_seconds: 180, session_retention_days: 7 });
  });

  it("rejects invalid capacity without contacting the browser service", async () => {
    const fetch = vi.fn(); vi.stubGlobal("fetch", fetch);
    for (const [headless_count, headed_count] of [["", "2"], ["-1", "2"], ["1.5", "2"], ["65", "1"], ["NaN", "2"]]) {
      const request = new Request("http://backoffice/admin/browsers", { method: "POST", headers: { Origin: "http://backoffice" }, body: new URLSearchParams({ intent: "settings", max_browsers: headless_count, idle_timeout_seconds: "120", session_retention_days: headed_count }) });
      expect((await action({ request } as Parameters<typeof action>[0])).error).toBeTruthy();
    }
    expect(fetch).not.toHaveBeenCalled();
  });

  it("validates and saves the session timeout", async () => {
    vi.stubEnv("BROWSER_API_URL", "http://browser-service:8081");
    const fetch = vi.fn(async (_url: URL | RequestInfo, _init?: RequestInit) => Response.json({}));
    vi.stubGlobal("fetch", fetch);
    for (const value of ["", "0", "-1", "NaN", "Infinity"]) {
      const request = new Request("http://backoffice/admin/browsers/settings", { method: "POST", headers: { Origin: "http://backoffice" }, body: new URLSearchParams({ intent: "settings", max_browsers: "6", session_retention_days: "7", idle_timeout_seconds: value }) });
      expect((await action({ request } as Parameters<typeof action>[0])).error).toBeTruthy();
    }
    expect(fetch).not.toHaveBeenCalled();
    const request = new Request("http://backoffice/admin/browsers/settings", { method: "POST", headers: { Origin: "http://backoffice" }, body: new URLSearchParams({ intent: "settings", max_browsers: "6", session_retention_days: "7", idle_timeout_seconds: "180" }) });
    expect((await action({ request } as Parameters<typeof action>[0])).error).toBeNull();
    expect(JSON.parse(fetch.mock.calls[0][1]?.body as string)).toEqual({ max_browsers: 6, session_retention_days: 7, idle_timeout_seconds: 180 });
    expect(String(fetch.mock.calls[0][0])).toBe("http://browser-service:8081/v1/browser/settings");
  });
});
