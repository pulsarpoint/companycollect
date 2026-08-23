import { describe, expect, it } from "vitest";
import {
  artifactSelectItems,
  clampConcurrency,
  clampMaxCompanies,
  dagsterApiKeyVariable,
  INFO_ARTIFACT_SOURCES,
  isInfoArtifact,
  MAX_COMPANIES,
  MAX_CONCURRENCY,
  MIN_COMPANIES,
  MIN_CONCURRENCY,
  profileSelectItems,
  type PipelineProfileOption,
} from "~/lib/se-company-info-pipeline";

function profile(overrides: Partial<PipelineProfileOption> = {}): PipelineProfileOption {
  return {
    profileId: "0f8f2a3e-6c1e-4a0f-9c2b-9c9a7f4b1d20",
    name: "DeepSeek production",
    provider: "deepseek",
    model: "deepseek-v4-flash",
    baseUrl: "https://api.deepseek.com",
    isActive: true,
    apiKeyEnvironmentVariable: "DEEPSEEK_API_KEY",
    dagsterApiKeyVariable: "DEEPSEEK_API_KEY",
    ...overrides,
  };
}

describe("the client-safe pipeline helpers", () => {
  it("clamps concurrency to the range the asset accepts", () => {
    // LlmProfileConfig.concurrency is ge=1, le=8: a value outside that range is
    // a config error on the Dagster side, so the form never submits one.
    expect(clampConcurrency(0)).toBe(MIN_CONCURRENCY);
    expect(clampConcurrency(99)).toBe(MAX_CONCURRENCY);
    expect(clampConcurrency(Number.NaN)).toBe(MIN_CONCURRENCY);
    expect(clampConcurrency(3.9)).toBe(3);
  });

  it("clamps max_companies to SECompanyInfoConfig's own range", () => {
    expect(clampMaxCompanies(0)).toBe(MIN_COMPANIES);
    expect(clampMaxCompanies(-5)).toBe(MIN_COMPANIES);
    expect(clampMaxCompanies(9_999_999)).toBe(MAX_COMPANIES);
    expect(clampMaxCompanies(Number.NaN)).toBe(MIN_COMPANIES);
    expect(clampMaxCompanies(1_000)).toBe(1_000);
  });

  it("derives the host variable the key is read from, and never the key", () => {
    expect(dagsterApiKeyVariable("deepseek")).toBe("DEEPSEEK_API_KEY");
    expect(dagsterApiKeyVariable("OpenAI")).toBe("OPENAI_API_KEY");
    expect(dagsterApiKeyVariable("azure_openai")).toBe("AZURE_OPENAI_API_KEY");
  });

  it("refuses a provider name that cannot name an environment variable", () => {
    // Dagster builds the name with a bare provider.upper(), so a name needing
    // normalisation would have the host look up something unreadable. Naming a
    // variable nobody will ever read would be worse than saying there is none.
    expect(dagsterApiKeyVariable("open-ai")).toBeNull();
    expect(dagsterApiKeyVariable("my provider")).toBeNull();
    expect(dagsterApiKeyVariable("3rd-party")).toBeNull();
    expect(dagsterApiKeyVariable("  ")).toBeNull();
    expect(dagsterApiKeyVariable("")).toBeNull();
  });

  it("labels the profile picker by name and model, never by its id", () => {
    // Base UI renders the Select trigger from `items`, so this list IS what the
    // closed trigger shows -- a missing entry means a bare UUID on screen.
    const items = profileSelectItems([
      profile(),
      profile({ profileId: "b2", name: "Local llama", model: "llama-4-70b" }),
    ]);
    expect(items).toEqual([
      {
        label: "DeepSeek production — deepseek-v4-flash",
        value: "0f8f2a3e-6c1e-4a0f-9c2b-9c9a7f4b1d20",
      },
      { label: "Local llama — llama-4-70b", value: "b2" },
    ]);
    for (const item of items) {
      expect(item.label).not.toContain(item.value);
    }
    expect(profileSelectItems([])).toEqual([]);
    expect(artifactSelectItems()).toEqual([
      { label: "scb", value: "scb" },
      { label: "esef", value: "esef" },
      { label: "wikidata", value: "wikidata" },
    ]);
  });

  it("offers exactly the three artifacts and rejects anything else", () => {
    expect([...INFO_ARTIFACT_SOURCES]).toEqual(["scb", "esef", "wikidata"]);
    expect(isInfoArtifact("esef")).toBe(true);
    expect(isInfoArtifact("se_company_info")).toBe(false);
    expect(isInfoArtifact("")).toBe(false);
  });

  it("carries no Dagster asset name into the client bundle", () => {
    // The route component imports this module, so anything named here ships to
    // the browser. Asset names (and the ClickHouse reads behind them) belong in
    // se-company-info-pipeline.server.ts.
    expect(INFO_ARTIFACT_SOURCES.join(" ")).not.toContain("clickhouse");
  });
});
