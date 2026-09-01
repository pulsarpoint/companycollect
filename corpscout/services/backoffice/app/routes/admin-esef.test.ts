import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  launch: vi.fn(),
  loadOverview: vi.fn(),
  loadCountries: vi.fn(),
  localCodexEnabled: false,
  profiles: [
    {
      profileId: "profile-1",
      name: "DeepSeek production",
      provider: "deepseek",
      baseUrl: "https://api.deepseek.com",
      model: "deepseek-v4-flash",
      apiKeyEnvironmentVariable: "DEEPSEEK_API_KEY",
      isActive: true,
      apiKeyAvailable: false,
      createdAt: "2026-08-01T00:00:00.000Z",
      updatedAt: "2026-08-01T00:00:00.000Z",
    },
  ],
}));

vi.mock("~/lib/esef-enrichment-launch.server", async (importOriginal) => ({
  ...(await importOriginal<
    typeof import("~/lib/esef-enrichment-launch.server")
  >()),
  launchEsefDocumentCompanyInformation: (...args: unknown[]) =>
    mocks.launch(...args),
}));

vi.mock("~/lib/esef-operations.server", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/lib/esef-operations.server")>()),
  loadEsefOverview: (...args: unknown[]) => mocks.loadOverview(...args),
}));

vi.mock("~/lib/esef-countries.server", () => ({
  loadEsefCountryCodes: (...args: unknown[]) => mocks.loadCountries(...args),
}));

vi.mock("~/lib/llm-settings.server", () => ({
  listLlmProfiles: () => mocks.profiles,
  isLocalCodexEnabled: () => mocks.localCodexEnabled,
}));

const { action, loader } = await import("~/routes/admin-esef");

type ActionResult = Awaited<ReturnType<typeof action>>;

function post(
  fields: Record<string, string | string[]>,
): Promise<ActionResult> {
  const body = new FormData();
  for (const [key, value] of Object.entries(fields)) {
    for (const item of Array.isArray(value) ? value : [value]) {
      body.append(key, item);
    }
  }
  return action({
    request: new Request("http://backoffice/admin/esef", {
      method: "POST",
      body,
    }),
  } as unknown as Parameters<typeof action>[0]);
}

const VALID_FIELDS = {
  intent: "launch-company-information",
  profile_id: "profile-1",
  country_iso2s: "se",
  limit_documents: "1",
  max_documents: "250",
  source_document_ids: " filing-1\nfiling-2,filing-1 ",
  company_ids: "company-1, company-2 company-1",
  refresh_behavior: "reuse_existing",
  concurrency: "4",
  temperature: "0.2",
  max_evidence_chars: "70000",
  timeout_seconds: "240",
};

beforeEach(() => {
  mocks.launch.mockReset();
  mocks.launch.mockResolvedValue({
    runId: "esef-run-1",
    status: "QUEUED",
    requestId: "request-1",
  });
  mocks.loadCountries.mockReset();
  mocks.loadCountries.mockResolvedValue(["FI", "SE"]);
  mocks.loadOverview.mockReset();
  mocks.loadOverview.mockResolvedValue({
    inventory: { assets: [], activeRuns: [] },
    enrichment: { recentEnrichmentRuns: [] },
  });
  mocks.localCodexEnabled = false;
  vi.stubEnv("BACKOFFICE_OPERATOR", "operator@example.com");
  vi.stubEnv("DAGSTER_UI_URL", "https://dagster.example");
});

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("admin ESEF action", () => {
  it("builds the fixed adapter input from a central profile and validated scope", async () => {
    const result = await post({
      ...VALID_FIELDS,
      job: "hostile_job",
      asset_selection: "hostile_asset",
      provider: "hostile-provider",
      model: "hostile-model",
      prompt_version: "hostile-prompt",
      requested_by: "browser-operator",
    });

    expect(result).toMatchObject({
      ok: true,
      launched: {
        runId: "esef-run-1",
        runUrl: "https://dagster.example/runs/esef-run-1",
        model: "deepseek-v4-flash",
        selection: "2 ESEF filings",
      },
    });
    expect(mocks.launch).toHaveBeenCalledOnce();
    expect(mocks.launch).toHaveBeenCalledWith({
      requestedBy: "operator@example.com",
      countryIso2s: ["SE"],
      companyIds: ["company-1", "company-2"],
      sourceDocumentIds: ["filing-1", "filing-2"],
      maxDocuments: 250,
      refreshBehavior: "reuse_existing",
      maxEvidenceChars: 70_000,
      timeoutSeconds: 240,
      llm: {
        provider: "deepseek",
        model: "deepseek-v4-flash",
        baseUrl: "https://api.deepseek.com",
        apiKeyEnvironmentVariable: "DEEPSEEK_API_KEY",
        temperature: 0.2,
        promptVersion: "esef-company-enrichment-v2",
        concurrency: 4,
      },
    });
  });

  it("launches with the local codex agent when the toggle and endpoint are set", async () => {
    mocks.localCodexEnabled = true;
    vi.stubEnv("LOCAL_CODEX_BASE_URL", "http://graovic-mac:8787/v1");

    const result = await post({ ...VALID_FIELDS, profile_id: "local_codex" });

    expect(result.ok).toBe(true);
    expect(mocks.launch).toHaveBeenCalledOnce();
    expect(mocks.launch.mock.calls[0][0].llm).toMatchObject({
      provider: "local_codex",
      model: "codex",
      baseUrl: "http://graovic-mac:8787/v1",
      apiKeyEnvironmentVariable: "LOCAL_CODEX_API_KEY",
    });
  });

  it("refuses a local codex launch when the toggle is off", async () => {
    mocks.localCodexEnabled = false;
    vi.stubEnv("LOCAL_CODEX_BASE_URL", "http://graovic-mac:8787/v1");

    const result = await post({ ...VALID_FIELDS, profile_id: "local_codex" });

    expect(result.ok).toBe(false);
    expect(result.error).toContain("Local codex");
    expect(mocks.launch).not.toHaveBeenCalled();
  });

  it("refuses a local codex launch when the agent endpoint is not configured", async () => {
    mocks.localCodexEnabled = true;

    const result = await post({ ...VALID_FIELDS, profile_id: "local_codex" });

    expect(result.ok).toBe(false);
    expect(result.error).toContain("LOCAL_CODEX_BASE_URL");
    expect(mocks.launch).not.toHaveBeenCalled();
  });

  it("rejects invalid runtime values before calling Dagster", async () => {
    const result = await post({ ...VALID_FIELDS, concurrency: "99" });

    expect(result.ok).toBe(false);
    expect(result.error).toContain("Concurrency");
    expect(mocks.launch).not.toHaveBeenCalled();
  });

  it("sends no document limit unless the operator enables it", async () => {
    const result = await post({
      ...VALID_FIELDS,
      limit_documents: "",
      max_documents: "250",
      country_iso2s: [],
      company_ids: "",
      source_document_ids: "",
    });

    expect(result).toMatchObject({
      ok: true,
      launched: { selection: "All eligible documents" },
    });
    expect(mocks.launch).toHaveBeenCalledWith(
      expect.objectContaining({ maxDocuments: null }),
    );
  });

  it("accepts several countries from the parsed ESEF country list", async () => {
    const result = await post({
      ...VALID_FIELDS,
      country_iso2s: ["se", "FI", "se"],
      company_ids: "",
      source_document_ids: "",
      limit_documents: "",
    });

    expect(result).toMatchObject({
      ok: true,
      launched: { selection: "All eligible documents in 2 countries" },
    });
    expect(mocks.launch).toHaveBeenCalledWith(
      expect.objectContaining({ countryIso2s: ["FI", "SE"] }),
    );
  });

  it("rejects a country that is not present in parsed ESEF documents", async () => {
    const result = await post({
      ...VALID_FIELDS,
      country_iso2s: "DE",
      company_ids: "",
    });

    expect(result).toEqual({
      ok: false,
      error: "Choose countries from the ESEF document country list.",
      launched: null,
    });
    expect(mocks.launch).not.toHaveBeenCalled();
  });

  it("requires exactly one country when company IDs are supplied", async () => {
    const result = await post({
      ...VALID_FIELDS,
      country_iso2s: ["FI", "SE"],
    });

    expect(result.ok).toBe(false);
    expect(result.error).toContain("Company IDs require exactly one");
    expect(mocks.launch).not.toHaveBeenCalled();
  });

  it("does not accept an arbitrary action", async () => {
    const result = await post({ ...VALID_FIELDS, intent: "run-any-job" });

    expect(result).toEqual({
      ok: false,
      error: "Unknown ESEF action.",
      launched: null,
    });
    expect(mocks.launch).not.toHaveBeenCalled();
  });
});

describe("admin ESEF loader", () => {
  it("does not expose the server-owned prompt version as a form option", async () => {
    const result = await loader({} as Parameters<typeof loader>[0]);

    expect(result.runtimeDefaults).not.toHaveProperty("promptVersion");
  });

  it("offers Local codex as a picker entry when the toggle is on", async () => {
    mocks.localCodexEnabled = true;
    vi.stubEnv("LOCAL_CODEX_BASE_URL", "http://graovic-mac:8787/v1");

    const result = await loader({} as Parameters<typeof loader>[0]);
    const local = result.profiles.find(
      (profile) => profile.profileId === "local_codex",
    );

    expect(local).toMatchObject({
      name: "Local codex",
      provider: "local_codex",
      model: "codex",
      disabled: false,
    });
  });

  it("shows Local codex disabled while the agent endpoint is unset", async () => {
    mocks.localCodexEnabled = true;

    const result = await loader({} as Parameters<typeof loader>[0]);
    const local = result.profiles.find(
      (profile) => profile.profileId === "local_codex",
    );

    expect(local).toMatchObject({ disabled: true });
    expect(local?.disabledReason).toContain("LOCAL_CODEX_BASE_URL");
  });

  it("omits Local codex when the toggle is off", async () => {
    const result = await loader({} as Parameters<typeof loader>[0]);

    expect(
      result.profiles.find((profile) => profile.profileId === "local_codex"),
    ).toBeUndefined();
  });
});
