import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { CrawlSettingsFields } from "~/components/admin/crawl-settings-fields";
import { LlmProfileField } from "~/components/admin/llm-profile-field";
import { loader } from "~/routes/admin-crawl-llm-profiles";

const mocks = vi.hoisted(() => ({fetcher: vi.fn(), listLlmProfiles: vi.fn()}));
vi.mock("react-router", async importOriginal => ({...await importOriginal<typeof import("react-router")>(), useFetcher: mocks.fetcher}));
vi.mock("~/lib/llm-settings.server", () => ({listLlmProfiles: mocks.listLlmProfiles}));

const profiles = [
  {profileId: "chosen-model", name: "Saved model", provider: "openrouter", model: "example/model", apiKeyAvailable: true},
  {profileId: "missing-key", name: "Unavailable model", provider: "deepseek", model: "example/other", apiKeyAvailable: false},
];

function render(initialProfileId?: string) {
  return renderToStaticMarkup(<MemoryRouter><CrawlSettingsFields type="site_info" idPrefix="test" initialProfileId={initialProfileId} /></MemoryRouter>);
}

beforeEach(() => {
  vi.resetAllMocks();
  mocks.fetcher.mockReturnValue({state: "idle", data: {profiles, error: null}, load: vi.fn()});
});

describe("crawl LLM selection", () => {
  it("shares the saved-model field with a caller-specific label and description", () => {
    const html = renderToStaticMarkup(<MemoryRouter><LlmProfileField idPrefix="shared" label="Browser assistant LLM"
      description="Verify image understanding before starting." /></MemoryRouter>);
    expect(html).toContain("Browser assistant LLM");
    expect(html).toContain("Verify image understanding before starting.");
    expect(html).toContain('id="shared-llm-profile"');
    expect(html).toMatch(/<select[^>]*name="llm_profile_id"[^>]*required=""/);
    expect(html).toMatch(/<option[^>]*value=""[^>]*selected=""/);
    expect(html).toContain("Saved model · openrouter · example/model");
  });
  it("requires an explicit profile choice instead of accepting raw model fields", () => {
    const html = render();
    expect(html).toMatch(/<select[^>]*name="llm_profile_id"[^>]*required=""/);
    expect(html).toMatch(/<option[^>]*value=""[^>]*selected=""[^>]*>Choose a saved LLM/);
    expect(html).toContain("Saved model · openrouter · example/model");
    expect(html).not.toContain('name="api"');
    expect(html).not.toContain('name="model"');
    expect(html).toContain("checked with the crawler before processing starts");
    expect(html).toContain('href="/admin/settings/llms"');
  });

  it("allows selecting and restoring a profile without authentication", () => {
    const html = render("missing-key");
    expect(html).not.toMatch(/<option[^>]*value="missing-key"[^>]*disabled=""/);
    expect(html).toContain("No API key");
    expect(html).toMatch(/<option[^>]*value="missing-key"[^>]*selected=""/);
  });

  it("restores an explicitly supplied execution profile while still requiring a valid saved model", () => {
    expect(render("chosen-model")).toMatch(/<option[^>]*value="chosen-model"[^>]*selected=""/);
    expect(render("removed-model")).toMatch(/<option[^>]*value=""[^>]*selected=""/);
  });

  it("keeps the selection empty until profiles load and offers recovery on lookup failure", () => {
    mocks.fetcher.mockReturnValue({state: "loading", data: undefined, load: vi.fn()});
    expect(render()).toContain("Loading saved LLMs…");
    mocks.fetcher.mockReturnValue({state: "idle", data: {profiles: [], error: "Could not load saved LLMs."}, load: vi.fn()});
    const html = render();
    expect(html).toContain('role="alert"');
    expect(html).toContain("Could not load saved LLMs.");
    expect(html).toContain("Reload LLMs");
  });

  it("explains when no enabled profiles exist", () => {
    mocks.fetcher.mockReturnValue({state: "idle", data: {profiles: [], error: null}, load: vi.fn()});
    expect(render()).toContain("No enabled LLM is available");
  });
});

describe("crawl LLM profile resource", () => {
  it("exposes only selection metadata, even if stored profiles contain credentials or environment names", async () => {
    mocks.listLlmProfiles.mockReturnValue(profiles.map(profile => ({...profile, apiKey: "do-not-send-this", apiKeyEnvironmentVariable: "SECRET_MODEL_KEY", baseUrl: "https://provider.invalid/v1", isActive: true})));
    expect(await loader({request: new Request("http://backoffice/admin/crawls/llm-profiles")} as Parameters<typeof loader>[0])).toEqual({profiles, error: null});
    expect(JSON.stringify(await loader({request: new Request("http://backoffice/admin/crawls/llm-profiles")} as Parameters<typeof loader>[0]))).not.toContain("SECRET_MODEL_KEY");
    expect(JSON.stringify(await loader({request: new Request("http://backoffice/admin/crawls/llm-profiles")} as Parameters<typeof loader>[0]))).not.toContain("do-not-send-this");
  });

  it("lists Jev only for decision selection while processing stays text-only", async () => {
    mocks.listLlmProfiles.mockResolvedValue([...profiles, {profileId: "jev", name: "Jev 1.13", model: "typesafe/jev-1.13", provider: "OpenRouter", apiKeyAvailable: true}]);
    const selected = await loader({request: new Request("http://backoffice/admin/crawls/llm-profiles?role=decision")} as Parameters<typeof loader>[0]);
    expect(selected.profiles.map(p => p.model)).toEqual(["typesafe/jev-1.13"]);
    expect(mocks.listLlmProfiles).toHaveBeenCalledWith(false, true);
    const processing = await loader({request: new Request("http://backoffice/admin/crawls/llm-profiles")} as Parameters<typeof loader>[0]);
    expect(processing.profiles.map(p => p.model)).not.toContain("typesafe/jev-1.13");
  });

  it("returns a recoverable error without exposing database details", async () => {
    mocks.listLlmProfiles.mockImplementation(() => { throw new Error("private database path"); });
    expect(await loader({request: new Request("http://backoffice/admin/crawls/llm-profiles")} as Parameters<typeof loader>[0])).toEqual({profiles: [], error: "Could not load saved LLMs. Reload the list to try again."});
  });
});

it("shows a default-off full crawl all control only for full crawls", () => {
  const full = renderToStaticMarkup(<MemoryRouter><CrawlSettingsFields type="full" idPrefix="full-test" /></MemoryRouter>);
  expect(full).toContain('name="full_crawl_all"');
  expect(full).toMatch(/<option[^>]*value="false"[^>]*selected=""[^>]*>No · company websites only/);
  expect(full).toContain("include shops and content sites");
  expect(render()).not.toContain('name="full_crawl_all"');
});
