import { afterEach, describe, expect, it, vi } from "vitest";
import { runChallengeAgent } from "~/lib/challenge-agent.server";

afterEach(() => {vi.unstubAllEnvs(); vi.unstubAllGlobals();});

describe("challenge agent HTTP boundary", () => {
  it("rejects a manual agent request while automatic assistance owns the tab", async () => {
    vi.stubEnv("CRAWLER_API_URL", "http://crawler:8080");
    const fetch = vi.fn(async () => Response.json({state: "captcha", browser_available: true, browser_lease_id: "a".repeat(32), challenge_agent_running: true}));
    vi.stubGlobal("fetch", fetch);
    await expect(runChallengeAgent("automatic")).rejects.toThrow("already running");
    expect(fetch).toHaveBeenCalledTimes(1);
  });
  it.each(["captcha", "brave_captcha"])("uses the live lease and correct tab for %s without resuming", async blockedReason => {
    vi.stubEnv("CRAWLER_API_URL", "http://crawler:8080");
    vi.stubEnv("BROWSER_API_URL", "http://browser:8081");
    vi.stubEnv("BROWSER_API_TOKEN", "private-browser-token");
    const lease = "a".repeat(32);
    const fetch = vi.fn(async (url: URL, init?: RequestInit) => {
      if (url.host === "crawler:8080") return Response.json({state: "captcha", browser_available: true, browser_lease_id: lease, blocked_reason: blockedReason});
      if (url.pathname === "/v1/browser/extract") return Response.json({url: "https://example.test/", session: {generation: "live"}});
      return Response.json({runId: "test-run", state: "needs_human"});
    });
    vi.stubGlobal("fetch", fetch);
    expect(await runChallengeAgent("retry-one")).toEqual({runId: "test-run", state: "needs_human"});
    expect(fetch).toHaveBeenCalledTimes(3);
    const tab = blockedReason.startsWith("brave") ? "search" : "site";
    expect(JSON.parse(String(fetch.mock.calls[1][1]?.body))).toEqual({session: {id: lease}, tab, browserHtml: false});
    expect(fetch.mock.calls[2][0].pathname).toBe(`/v1/browser/sessions/${lease}/tabs/${tab}/challenge-agent`);
    expect(JSON.parse(String(fetch.mock.calls[2][1]?.body))).toEqual({confirm: true, expectedUrl: "https://example.test/", expectedGeneration: "live"});
    expect(new Headers(fetch.mock.calls[2][1]?.headers).get("Authorization")).toBe("Bearer private-browser-token");
    expect(fetch.mock.calls.some(([url]) => url.pathname.endsWith("/resume"))).toBe(false);
  });

  it("rejects completed or unavailable sessions before sending any browser request", async () => {
    vi.stubEnv("CRAWLER_API_URL", "http://crawler:8080");
    const fetch = vi.fn(async () => Response.json({state: "failed", browser_available: false, browser_lease_id: null}));
    vi.stubGlobal("fetch", fetch);
    await expect(runChallengeAgent("ended")).rejects.toThrow("Open an interactive retry");
    expect(fetch).toHaveBeenCalledTimes(1);
  });
});
