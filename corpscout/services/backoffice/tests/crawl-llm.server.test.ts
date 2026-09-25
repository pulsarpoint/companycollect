import { createDecipheriv } from "node:crypto";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import fixture from "./fixtures/crawl-llm-envelope.json";
const mocks = vi.hoisted(() => ({getLlmProfile: vi.fn(), getLlmProfileApiKey: vi.fn(), crawlerFetch: vi.fn(), browserFetch: vi.fn()}));
vi.mock("~/lib/llm-settings.server", async importOriginal => ({...await importOriginal<typeof import("~/lib/llm-settings.server")>(), getLlmProfile: mocks.getLlmProfile, getLlmProfileApiKey: mocks.getLlmProfileApiKey}));
import { LlmSettingsValidationError } from "~/lib/llm-settings.server";
vi.mock("~/lib/browser-service.server", () => ({browserFetch: mocks.browserFetch}));
vi.mock("~/lib/crawler.server", () => ({crawlerFetch: mocks.crawlerFetch}));
import { encryptCrawlLlm, prepareCrawlSettings, verifySelectedLlm, type EncryptedCrawlLlm } from "~/lib/crawl-llm.server";

const profile = {profileId: "profile-1", provider: fixture.llm.provider, baseUrl: fixture.llm.base_url, model: fixture.llm.model};
function decrypt(llm: EncryptedCrawlLlm) {
  const [,nonce,encrypted] = llm.api_key_encrypted.split(".");
  const bytes = Buffer.from(encrypted, "base64url");
  const decipher = createDecipheriv("aes-256-gcm", Buffer.from(fixture.shared_key, "hex"), Buffer.from(nonce, "base64url"));
  decipher.setAAD(Buffer.from(`corpscout-crawler-llm:v1\0${llm.provider}\0${llm.base_url}\0${llm.model}`));
  decipher.setAuthTag(bytes.subarray(-16));
  return Buffer.concat([decipher.update(bytes.subarray(0, -16)), decipher.final()]).toString("utf8");
}
beforeEach(() => {
  vi.clearAllMocks();
  vi.stubEnv("BROWSER_API_TOKEN", "test-only-browser-token");
  vi.stubEnv("CRAWLER_API_TOKEN", "test-only-crawler-token");
  vi.stubEnv("CRAWLER_LLM_ENCRYPTION_KEY", fixture.shared_key);
  mocks.getLlmProfileApiKey.mockReturnValue(fixture.api_key);
  mocks.getLlmProfile.mockReturnValue(profile);
  mocks.crawlerFetch.mockResolvedValue(new Response(JSON.stringify({ok: true})));
  mocks.browserFetch.mockResolvedValue(new Response(JSON.stringify({ok: true})));
});
afterEach(() => vi.unstubAllEnvs());

describe("encrypted crawler credentials", () => {
  it("uses fresh nonces with interoperable authenticated encryption", () => {
    const first = encryptCrawlLlm(profile, fixture.api_key, fixture.shared_key);
    const second = encryptCrawlLlm(profile, fixture.api_key, fixture.shared_key);
    expect(first.api_key_encrypted).not.toBe(second.api_key_encrypted);
    expect(JSON.stringify(first)).not.toContain(fixture.api_key);
    expect(decrypt(first)).toBe(fixture.api_key);
    expect(decrypt(fixture.llm)).toBe(fixture.api_key);
    expect(() => decrypt({...first, model: "swapped-model"})).toThrow();
    expect(() => decrypt({...first, base_url: "https://another.example/v1"})).toThrow();
  });
  it.each(["", "short", "x".repeat(64)])("rejects an invalid shared encryption key", (key) => {
    expect(() => encryptCrawlLlm(profile, fixture.api_key, key)).toThrow("CRAWLER_LLM_ENCRYPTION_KEY");
  });
  it.each(["https://key:secret@example.org/v1", "https://example.org/v1?key=secret", "ftp://example.org/v1"])("rejects credential-bearing or invalid endpoint %s", (baseUrl) => {
    expect(() => encryptCrawlLlm({...profile, baseUrl}, fixture.api_key, fixture.shared_key)).toThrow();
  });
});

describe("crawl preflight", () => {
  it("verifies the exact selected profile and forwards only its encrypted key", async () => {
    const config = await prepareCrawlSettings({llm_profile_id: "profile-1", max_pages: 1});
    expect(mocks.getLlmProfile).toHaveBeenCalledWith("profile-1");
    expect(config).toMatchObject({api: "openrouter", model: profile.model, max_pages: 1, crawler_config: {provider: null}});
    expect(config).not.toHaveProperty("llm_profile_id");
    expect(mocks.crawlerFetch).toHaveBeenCalledWith("/v1/llm/verify", expect.objectContaining({method: "POST", body: JSON.stringify({llm: config.llm})}));
    expect(decrypt(config.llm)).toBe(fixture.api_key);
    expect(JSON.stringify(config)).not.toContain(fixture.api_key);
  });
  it.each([undefined, ""])("requires an explicit selection", async (id) => {
    await expect(prepareCrawlSettings({llm_profile_id: id})).rejects.toThrow("Choose an LLM");
    expect(mocks.crawlerFetch).not.toHaveBeenCalled();
  });
  it("requires authenticated crawler verification", async () => {
    vi.stubEnv("CRAWLER_API_TOKEN", "");
    await expect(prepareCrawlSettings({llm_profile_id: "profile-1"})).rejects.toThrow("CRAWLER_API_TOKEN");
    expect(mocks.crawlerFetch).not.toHaveBeenCalled();
  });
  it("rejects deleted profiles and missing server credentials before network access", async () => {
    mocks.getLlmProfile.mockReturnValueOnce(null);
    await expect(prepareCrawlSettings({llm_profile_id: "gone"})).rejects.toThrow("no longer exists");
    mocks.getLlmProfileApiKey.mockImplementationOnce(() => { throw new LlmSettingsValidationError("API key is missing"); });
    await expect(prepareCrawlSettings({llm_profile_id: "profile-1"})).rejects.toThrow("API key is missing");
    expect(mocks.crawlerFetch).not.toHaveBeenCalled();
  });
  it("blocks a provider failure with a useful redacted error", async () => {
    mocks.crawlerFetch.mockResolvedValue(new Response(JSON.stringify({ok: false, error: `No endpoints found: ${fixture.api_key}`})));
    await expect(prepareCrawlSettings({llm_profile_id: "profile-1"})).rejects.toThrow("No endpoints found: [redacted]");
  });
  it("blocks unreachable crawlers without exposing transport errors", async () => {
    mocks.crawlerFetch.mockRejectedValue(new Error(fixture.api_key));
    await expect(prepareCrawlSettings({llm_profile_id: "profile-1"})).rejects.toThrow("Could not verify");
  });
  it.each([{ok: "true"}, {}, null])("rejects unconfirmed verification %j", async (body) => {
    mocks.crawlerFetch.mockResolvedValue(new Response(JSON.stringify(body)));
    await expect(prepareCrawlSettings({llm_profile_id: "profile-1"})).rejects.toThrow("LLM verification failed");
  });
});


describe("Brave assistant preflight", () => {
  it("verifies encrypted credentials with the browser vision endpoint", async () => {
    const llm = await verifySelectedLlm("profile-1", "brave");
    expect(mocks.browserFetch).toHaveBeenCalledWith("/v1/brave/llm/verify", expect.objectContaining({method: "POST", redirect: "error", body: JSON.stringify({llm})}));
    expect(mocks.crawlerFetch).not.toHaveBeenCalled();
    expect(decrypt(llm)).toBe(fixture.api_key);
  });
  it("requires browser authentication independently of crawler authentication", async () => {
    vi.stubEnv("BROWSER_API_TOKEN", "");
    await expect(verifySelectedLlm("profile-1", "brave")).rejects.toThrow("BROWSER_API_TOKEN");
    expect(mocks.browserFetch).not.toHaveBeenCalled();
  });
  it("rejects incompatible vision models with the reason and without secrets", async () => {
    mocks.browserFetch.mockResolvedValue(new Response(JSON.stringify({ok: false, error: `Image input is unsupported: ${fixture.api_key}`})));
    await expect(verifySelectedLlm("profile-1", "brave")).rejects.toThrow("Image input is unsupported: [redacted]");
  });
  it("fails closed on browser transport errors", async () => {
    mocks.browserFetch.mockRejectedValue(new Error(fixture.api_key));
    await expect(verifySelectedLlm("profile-1", "brave")).rejects.toThrow("Could not verify the selected LLM through the Brave browser assistant");
  });
});
