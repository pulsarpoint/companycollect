import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
const verifier = vi.hoisted(() => ({verifySelectedLlm: vi.fn()}));
vi.mock("~/lib/crawl-llm.server", () => verifier);
import { publishTestCrawl } from "~/lib/crawl-submit.server";

function form(overrides: Record<string, string> = {}) {
  const result = new FormData();
  for (const [key, value] of Object.entries({submission_id: "22f43e27-5ca7-4c28-8c59-e3e478faf813", crawl_profile: "full", url: "novelic.com", llm_profile_id: "11111111-1111-4111-8111-111111111111", max_pages: "5", max_model_calls: "20", challenge_agent_max_runs: "3", challenge_agent_model: "deepseek-flash", full_crawl_all: "false", save_artifacts: "true", interactive: "false", ...overrides})) result.set(key, value);
  return result;
}
const envelope = {provider: "deepseek", base_url: "https://api.deepseek.com", model: "deepseek-flash", api_key_encrypted: "v1.encrypted-fixture", profile_id: "11111111-1111-4111-8111-111111111111", profile_revision: 1};

describe("guided crawl submission", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    vi.stubEnv("CRAWLER_TEST_SUBMIT_ENABLED", "true");
    vi.stubEnv("CRAWLER_API_URL", "http://crawler.test");
    vi.stubEnv("CRAWLER_API_TOKEN", "private-api-token");
    verifier.verifySelectedLlm.mockResolvedValue(envelope);
  });
  afterEach(() => {vi.unstubAllGlobals(); vi.unstubAllEnvs();});

  it("keeps basic/full matching on the standard crawl endpoint", async () => {
    vi.stubGlobal("fetch", vi.fn(async (_url, options) => {
      if (options?.method !== "POST") return Response.json({}, {status: 404});
      const body = JSON.parse(options.body);
      return Response.json({request_id: body.request_id, url: body.url, state: "queued"});
    }));
    for (const profile of ["site_info", "full"]) {
      await publishTestCrawl(form({crawl_profile: profile, max_pages: profile === "site_info" ? "1" : "20", match_company: "true", country: "SE", skip_company_matching_if_mapped: "true"}));
      const [url, options] = vi.mocked(fetch).mock.calls.at(-1)!;
      expect(String(url)).toBe("http://crawler.test/v1/crawls");
      expect(JSON.parse(options!.body as string)).toMatchObject({crawl: profile === "site_info" ? false : "full", company_lookup: {country: "SE", skip_if_mapped: true}});
    }
  });
  it("submits company lookup to its separate read-only endpoint with verified models", async () => {
    vi.stubGlobal("fetch", vi.fn(async (_url, options) => {
      if (options?.method !== "POST") return Response.json({}, {status: 404});
      const body = JSON.parse(options.body);
      return Response.json({request_id: body.request_id, url: `https://${body.domain}/`, state: "queued"});
    }));
    const receipt = await publishTestCrawl(form({crawl_profile: "company_lookup", country: "SE", url: "example.se", max_pages: "4"}));
    const [url, options] = vi.mocked(fetch).mock.calls[1];
    expect(String(url)).toBe("http://crawler.test/v1/company-lookups");
    expect(JSON.parse(options!.body as string)).toEqual({request_id: receipt.request_id,
      domain: "example.se", country: "SE", skip_if_mapped: false, llm: envelope, config: {max_pages: 4, max_model_calls: 20, provider: null},
      interactive: false, challenge_agent_model: "deepseek-flash", challenge_agent_max_runs: 3});
    expect(verifier.verifySelectedLlm).toHaveBeenCalledWith(envelope.profile_id, "crawler");
    expect(receipt.url).toBe("https://example.se/");
  });

  it("passes candidate ranking independently of site classification to company lookup", async () => {
    vi.stubGlobal("fetch", vi.fn(async (_url, options) => {
      if (options?.method !== "POST") return Response.json({}, {status: 404});
      return Response.json({...JSON.parse(options.body), url: 'https://example.se/', state: "queued"});
    }));
    const jev = {...envelope, model: 'typesafe/jev-1.13'};
    verifier.verifySelectedLlm.mockResolvedValueOnce(envelope).mockResolvedValueOnce(jev);
    await publishTestCrawl(form({crawl_profile: 'company_lookup', country: 'SE', max_pages: '4', url: 'example.se', 'decision.company_match': 'jev', decision_llm_profile_id: '22222222-2222-4222-8222-222222222222'}));
    expect(JSON.parse(vi.mocked(fetch).mock.calls[1][1]!.body as string)).toMatchObject({decision_llm: jev, decision_tasks: ['company_match']});
  });

  it("verifies the saved LLM and sends its encrypted configuration with the guided parameters", async () => {
    const requests: {request_id: string; url: string}[] = [];
    vi.stubGlobal("fetch", vi.fn(async (_url, options) => {
      if (options?.method !== "POST") return Response.json({detail: "Unknown crawl"}, {status: 404});
      const body = JSON.parse(options.body);
      requests.push(body);
      return Response.json({...body, state: "queued", source: "rest"}, {status: 202});
    }));
    const receipt = await publishTestCrawl(form());
    expect(verifier.verifySelectedLlm).toHaveBeenCalledWith(envelope.profile_id, "crawler");
    expect(requests[0]).toMatchObject({url: "https://novelic.com/", crawl: "full", api: "deepseek", llm: envelope, config: {max_pages: 5, max_model_calls: 20, provider: null}});
    expect(requests[0].request_id).toMatch(/^backoffice-22f43e27-5ca7-4c28-8c59-e3e478faf813-[a-f0-9]{24}$/);
    expect(receipt).toEqual({request_id: requests[0].request_id, url: "https://novelic.com/", state: "queued"});
    expect(JSON.stringify(receipt)).not.toContain("encrypted");
    expect(new Headers(vi.mocked(fetch).mock.calls[1][1]?.headers).get("Authorization")).toBe("Bearer private-api-token");
  });

  it("verifies and encrypts Jev separately before submission", async () => {
    vi.stubGlobal("fetch", vi.fn(async (_url, options) => {
      if (options?.method !== "POST") return Response.json({}, {status: 404});
      return Response.json({...JSON.parse(options.body), state: "queued"});
    }));
    const jev = {...envelope, model: "typesafe/jev-1.13"};
    verifier.verifySelectedLlm.mockResolvedValueOnce(envelope).mockResolvedValueOnce(jev);
    await publishTestCrawl(form({"decision.site_eligibility": "jev", decision_llm_profile_id: "22222222-2222-4222-8222-222222222222"}));
    expect(verifier.verifySelectedLlm).toHaveBeenLastCalledWith("22222222-2222-4222-8222-222222222222", "crawler", false, "decision");
    expect(JSON.parse(vi.mocked(fetch).mock.calls[1][1]!.body as string)).toMatchObject({llm: envelope, decision_llm: jev, decision_tasks: ["site_eligibility"]});
  });
  it("recovers a lost acknowledgement without another crawl or another ciphertext", async () => {
    let saved: {request_id: string; url: string; state: string} | null = null;
    vi.stubGlobal("fetch", vi.fn(async (_url, options) => {
      if (options?.method !== "POST") return saved ? Response.json(saved) : Response.json({detail: "Unknown crawl"}, {status: 404});
      saved = {...JSON.parse(options.body), state: "queued"};
      throw new Error("Acknowledgement lost");
    }));
    await expect(publishTestCrawl(form())).rejects.toThrow("Acknowledgement lost");
    await expect(publishTestCrawl(form())).resolves.toMatchObject({state: "queued"});
    expect(verifier.verifySelectedLlm).toHaveBeenCalledTimes(1);
    expect(vi.mocked(fetch).mock.calls.filter(([, options]) => options?.method === "POST")).toHaveLength(1);
  });

  it("binds submission identity to the selected settings and model", async () => {
    const identifiers: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (_url, options) => {
      if (options?.method !== "POST") return Response.json({}, {status: 404});
      const body = JSON.parse(options.body);
      identifiers.push(body.request_id);
      return Response.json({...body, state: "queued"});
    }));
    await publishTestCrawl(form());
    await publishTestCrawl(form({max_pages: "6"}));
    await publishTestCrawl(form({llm_profile_id: "22222222-2222-4222-8222-222222222222"}));
    expect(new Set(identifiers).size).toBe(3);
  });

  it("reconciles a concurrent submission after a ciphertext conflict", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValueOnce(Response.json({}, {status: 404}))
      .mockResolvedValueOnce(Response.json({detail: "request_id belongs to a different request"}, {status: 409}))
      .mockResolvedValueOnce(Response.json({request_id: "accepted", url: "https://novelic.com/", state: "running"})));
    await expect(publishTestCrawl(form())).resolves.toMatchObject({state: "running"});
  });

  it("never submits when model verification fails", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({}, {status: 404})));
    verifier.verifySelectedLlm.mockRejectedValue(new Error("LLM verification failed"));
    await expect(publishTestCrawl(form())).rejects.toThrow("LLM verification failed");
    expect(vi.mocked(fetch).mock.calls.every(([, options]) => options?.method !== "POST")).toBe(true);
  });

  it("rejects disabled and malformed requests before contacting either service", async () => {
    vi.stubGlobal("fetch", vi.fn());
    vi.stubEnv("CRAWLER_TEST_SUBMIT_ENABLED", "false");
    await expect(publishTestCrawl(form())).rejects.toThrow("disabled");
    vi.stubEnv("CRAWLER_TEST_SUBMIT_ENABLED", "true");
    await expect(publishTestCrawl(form({submission_id: "bad"}))).rejects.toThrow("submission ID");
    await expect(publishTestCrawl(form({llm_profile_id: ""}))).rejects.toThrow("saved LLM");
    expect(fetch).not.toHaveBeenCalled();
    expect(verifier.verifySelectedLlm).not.toHaveBeenCalled();
  });

  it("reports crawler validation failures without including private request input", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValueOnce(Response.json({}, {status: 404}))
      .mockResolvedValueOnce(Response.json({detail: [{loc: ["body", "config", "max_external_pages"], msg: "Must be positive", input: "private-input"}]}, {status: 422})));
    await expect(publishTestCrawl(form({"config.max_external_pages": "3"}))).rejects.toThrow("body.config.max_external_pages: Must be positive");
  });
});
