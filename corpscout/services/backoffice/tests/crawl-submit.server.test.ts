import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { publishTestCrawl } from "~/lib/crawl-submit.server";

describe("test crawl submission over the crawler REST API", () => {
  const payload = {request_id: "backoffice-test", url: "https://novelic.com/", crawl: "full", config: {max_pages: 5}};

  beforeEach(() => {
    vi.stubEnv("CRAWLER_TEST_SUBMIT_ENABLED", "true");
    vi.stubEnv("CRAWLER_API_URL", "http://crawler.test");
    vi.stubEnv("CRAWLER_API_TOKEN", "private-api-token");
  });
  afterEach(() => {vi.unstubAllGlobals(); vi.unstubAllEnvs();});

  it("posts the request to /v1/crawls with the bearer token and returns the job state", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json(
      {request_id: payload.request_id, url: payload.url, state: "queued", source: "rest"}, {status: 202})));
    const body = JSON.stringify(payload);
    await expect(publishTestCrawl(body)).resolves.toEqual({request_id: payload.request_id, url: payload.url, state: "queued"});
    const [endpoint, options] = vi.mocked(fetch).mock.calls[0];
    expect(String(endpoint)).toBe("http://crawler.test/v1/crawls");
    expect(options?.method).toBe("POST");
    expect(options?.body).toBe(body);
    expect(new Headers(options?.headers).get("Authorization")).toBe("Bearer private-api-token");
  });

  it("rejects disabled, malformed and id-less requests without calling the crawler", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    vi.stubEnv("CRAWLER_TEST_SUBMIT_ENABLED", "false");
    await expect(publishTestCrawl(JSON.stringify(payload))).rejects.toThrow("disabled");
    vi.stubEnv("CRAWLER_TEST_SUBMIT_ENABLED", "true");
    await expect(publishTestCrawl("{")).rejects.toThrow("valid JSON");
    await expect(publishTestCrawl("{}")).rejects.toThrow("request_id");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("reports crawler validation errors and request-id conflicts", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json(
      {detail: [{loc: ["body", "config", "max_pages"], msg: "Must be positive", input: "private-input"}]}, {status: 422})));
    await expect(publishTestCrawl(JSON.stringify(payload))).rejects.toThrow("body.config.max_pages: Must be positive");
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({detail: "request_id already used for a different request"}, {status: 409})));
    await expect(publishTestCrawl(JSON.stringify(payload))).rejects.toThrow("different request");
  });
});
