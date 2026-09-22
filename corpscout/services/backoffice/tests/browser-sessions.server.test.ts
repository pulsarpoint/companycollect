import { afterEach, expect, it, vi } from "vitest";
import { browserSessionAction } from "~/lib/browser-sessions.server";

afterEach(() => { vi.unstubAllEnvs(); vi.unstubAllGlobals(); });

it("uses only a short-lived browser ticket in the browser-facing URL", async () => {
  vi.stubEnv("BROWSER_API_URL", "http://crawler:8080");
  vi.stubEnv("BROWSER_PUBLIC_URL", "https://browser.test");
  vi.stubEnv("BROWSER_API_TOKEN", "private-token");
  const fetch = vi.fn(async () => Response.json({ websocket_path: "/v1/browser-sessions/browser-1/browser?ticket=temporary" }));
  vi.stubGlobal("fetch", fetch);
  const result = await browserSessionAction("browser-1", "browser-ticket", {});
  expect(result).toEqual({ browserUrl: "wss://browser.test/v1/browser-sessions/browser-1/browser?ticket=temporary" });
  expect(JSON.stringify(result)).not.toContain("private-token");
  const [, options] = fetch.mock.calls[0] as unknown as [URL, RequestInit];
  expect(new Headers(options.headers).get("Authorization")).toBe("Bearer private-token");
});
