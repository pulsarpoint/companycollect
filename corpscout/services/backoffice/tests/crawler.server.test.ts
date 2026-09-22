import { afterEach, describe, expect, it, vi } from "vitest";
import { crawlAction, loadCrawls } from "~/lib/crawler.server";

afterEach(() => {vi.unstubAllEnvs(); vi.unstubAllGlobals();});

describe("crawler HTTP boundary", () => {
  it("keeps the service token server-side and forwards only supported filters", async () => {
    vi.stubEnv("CRAWLER_API_URL", "http://crawler:8080");
    vi.stubEnv("CRAWLER_API_TOKEN", "private-service-token");
    const fetch = vi.fn(async () => Response.json({attempts: [], total: 0}));
    vi.stubGlobal("fetch", fetch);
    const result = await loadCrawls(new URLSearchParams("state=failed&domain=melexis&url=https://untrusted.test"));
    const [url, options] = fetch.mock.calls[0] as unknown as [URL, RequestInit];
    expect(url.host).toBe("crawler:8080");
    expect(url.searchParams.get("domain")).toBe("melexis");
    expect(url.searchParams.has("url")).toBe(false);
    expect(new Headers(options.headers).get("Authorization")).toBe("Bearer private-service-token");
    expect(JSON.stringify(result)).not.toContain("private-service-token");
  });

  it("builds the noVNC URL with a short-lived ticket, never the service credential", async () => {
    vi.stubEnv("CRAWLER_API_URL", "http://crawler:8080");
    vi.stubEnv("BROWSER_PUBLIC_URL", "https://browser.example.test");
    vi.stubEnv("CRAWLER_API_TOKEN", "private-service-token");
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({websocket_path: "/v1/crawls/retry-one/browser?ticket=temporary"})));
    expect(await crawlAction("retry-one", "browser-ticket")).toEqual({browserUrl: "wss://browser.example.test/v1/crawls/retry-one/browser?ticket=temporary"});
  });
});
