import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import AdminBrowserTesting, { action } from "~/routes/admin-browser-testing";
import { browserFetch } from "~/lib/browser-service.server";
import { browserTestRequest } from "~/lib/browser-testing";

const sessionId = "a".repeat(32);
const origin = "http://backoffice";
function send(fields: Record<string, string> = {}, requestOrigin = origin, signal?: AbortSignal) {
  const request = new Request(`${origin}/admin/browsers/testing`, {
    method: "POST", headers: { Origin: requestOrigin }, signal,
    body: new URLSearchParams({ intent: "send", method: "POST", path: "/v1/browser/extract", body: "{}", ...fields }),
  });
  return action({ request } as Parameters<typeof action>[0]);
}

beforeEach(() => {
  vi.stubEnv("BROWSER_API_URL", "http://browser-service:8081");
  vi.stubEnv("BROWSER_API_TOKEN", "private-test-token");
});
afterEach(() => { vi.unstubAllEnvs(); vi.unstubAllGlobals(); vi.useRealTimers(); });

describe("browser testing API forwarding", () => {
  it("sends the exact raw body and preserves validation errors without exposing authentication", async () => {
    const responseBody = '{"detail":[{"type":"extra_forbidden","loc":["body","newOption"],"input":true}]}';
    const fetch = vi.fn(async () => new Response(responseBody, { status: 422, headers: { "Content-Type": "application/json", "Set-Cookie": "private-cookie" } }));
    vi.stubGlobal("fetch", fetch);
    const body = '{\n  "newOption": true\n}';
    const result = await send({ body });
    expect(result.error).toBeNull();
    expect(result.response).toMatchObject({ status: 422, body: responseBody, request: { method: "POST", path: "/v1/browser/extract", body } });
    const [url, options] = fetch.mock.calls[0] as unknown as [URL, RequestInit];
    expect(url.href).toBe("http://browser-service:8081/v1/browser/extract");
    expect(options.body).toBe(body);
    expect(options.redirect).toBe("error");
    expect(new Headers(options.headers).get("Authorization")).toBe("Bearer private-test-token");
    expect(JSON.stringify(result)).not.toMatch(/private-test-token|private-cookie/);
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it.each([404, 409, 410, 500, 504])("preserves HTTP %s, including non-JSON responses", async status => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response("upstream response", { status })));
    expect((await send()).response).toMatchObject({ status, body: "upstream response" });
  });

  it("lets the service validate malformed JSON, unknown options, and invalid IDs", async () => {
    const fetch = vi.fn(async () => Response.json({ detail: "invalid input" }, { status: 422 }));
    vi.stubGlobal("fetch", fetch);
    await send({ body: "{not json" });
    await send({ method: "GET", path: "/v1/browser/sessions/not-a-uuid", body: "" });
    expect(fetch).toHaveBeenCalledTimes(2);
    expect((fetch.mock.calls[0] as unknown as [URL, RequestInit])[1].body).toBe("{not json");
  });

  it("rejects off-service paths, management controls and cross-origin requests before fetching", async () => {
    const fetch = vi.fn(); vi.stubGlobal("fetch", fetch);
    for (const path of ["https://foreign.test/v1/browser/extract", "//foreign.test/v1/browser/extract", "/v1/browser/../server", "/v1/browser/sessions/%2e%2e", "/v1/browser/sessions/id\\other", "/v1/browser/sessions/id?token=secret", "/v1/server", "/v1/browser-sessions/browser-1/stop", `/v1/browser/sessions/${sessionId}/browser-ticket`]) {
      expect((await send({ path })).error).toBeTruthy();
    }
    expect((await send({}, "https://foreign.test")).error).toBeTruthy();
    expect((await send({}, "")).error).toBeTruthy();
    expect((await send({ method: "PATCH" })).error).toBeTruthy();
    expect((await send({ method: "GET", body: "{}" })).error).toBeTruthy();
    expect((await send({ body: "x".repeat(131_073) })).error).toBeTruthy();
    expect((await send({ body: "€".repeat(50_000) })).error).toBeTruthy();
    expect(fetch).not.toHaveBeenCalled();
  });

  it("forwards lifecycle and named-tab operations without injecting extra requests", async () => {
    const fetch = vi.fn(async () => Response.json({ id: sessionId, state: "ready" }));
    vi.stubGlobal("fetch", fetch);
    for (const preset of ["navigate", "status", "heartbeat", "release", "open", "focus", "close", "recover"] as const) {
      const draft = browserTestRequest(preset, sessionId, "https://example.com/", "site");
      const result = await send(draft);
      expect(result.response?.request).toEqual(draft);
    }
    expect(fetch).toHaveBeenCalledTimes(8);
    const options = (fetch.mock.calls[1] as unknown as [URL, RequestInit])[1];
    expect(options.method).toBe("GET");
    expect(options.body).toBeUndefined();
  });

  it("waits past the management timeout and forwards cancellation without retrying", async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    const fetch = vi.fn(async (_url: URL | RequestInfo, init?: RequestInit) => {
      await vi.advanceTimersByTimeAsync(15_000);
      expect(init?.signal?.aborted).toBe(false);
      controller.abort();
      expect(init?.signal?.aborted).toBe(true);
      throw new Error("transport failure with private internals");
    });
    vi.stubGlobal("fetch", fetch);
    const result = await send({}, origin, controller.signal);
    expect(result.error).toContain("request may already have run");
    expect(result.error).not.toContain("private internals");
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("keeps desktop tickets out of raw request history and uses the public websocket address", async () => {
    vi.stubEnv("BROWSER_PUBLIC_URL", "https://browser.test");
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ websocket_path: `/v1/browser/sessions/${sessionId}/browser?ticket=temporary` })));
    const result = await send({ intent: "connect", session_id: sessionId });
    expect(result.browserUrl).toBe(`wss://browser.test/v1/browser/sessions/${sessionId}/browser?ticket=temporary`);
    expect(result.response).toBeUndefined();
    expect(JSON.stringify(result)).not.toContain("private-test-token");
    expect((await send({ intent: "connect", session_id: "invalid" })).error).toBeTruthy();
  });

  it("retains the existing management helper's error behavior", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ detail: "Browser is assigned" }, { status: 409 })));
    await expect(browserFetch("/v1/browser-sessions/browser-1/stop", { method: "POST" })).rejects.toThrow("Browser is assigned");
  });
});

describe("browser testing workbench", () => {
  it("builds one-session navigation and capture requests without changing tabs or revisiting URLs", () => {
    const navigate = JSON.parse(browserTestRequest("navigate", sessionId, "https://example.com/page", "search").body);
    expect(navigate).toMatchObject({ session: { id: sessionId }, tab: "search", url: "https://example.com/page", browserHtml: true, checkRobotsTxt: true });
    const capture = JSON.parse(browserTestRequest("capture", sessionId, "https://example.com/page", "search").body);
    expect(capture).not.toHaveProperty("url");
    expect(capture.session).toEqual(navigate.session);
    expect(JSON.parse(browserTestRequest("screenshot", sessionId, "https://example.com/", "site").body)).toMatchObject({ screenshot: true, browserHtml: false });
    expect(JSON.parse(browserTestRequest("recover", sessionId, "https://example.com/", "site").body).reopenClosedTab).toBe(false);
    expect(navigate).not.toHaveProperty("browserId");
    const targeted = JSON.parse(browserTestRequest("navigate", sessionId, "https://example.com/", "site", "headed").body);
    expect(targeted.headless).toBe(false);
    expect(targeted.session.id).toBe(sessionId);
  });

  it("renders the testing tab and editable request controls without making API calls", () => {
    const fetch = vi.fn(); vi.stubGlobal("fetch", fetch);
    const element = <AdminBrowserTesting />;
    const router = createMemoryRouter([{ path: "/admin/browsers/testing", element }], { initialEntries: ["/admin/browsers/testing"] });
    const html = renderToStaticMarkup(<RouterProvider router={router} />);
    for (const label of ["Testing", "Session ID", "Target URL", "Tab name", "Raw JSON body", "Reset request", "Send request", "Connect browser", "Ready for your first request", "No automatic retries or heartbeats", "Browser mode", "Headless", "Headed"]) expect(html).toContain(label);
    expect(html).not.toContain("Reserve browser");
    expect(fetch).not.toHaveBeenCalled();
    expect(html).not.toContain("private-test-token");
  });
});
