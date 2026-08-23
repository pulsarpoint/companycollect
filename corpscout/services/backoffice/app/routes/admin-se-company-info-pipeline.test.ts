/**
 * The launch path of the Pipeline page's action.
 *
 * The backoffice has no authentication and these launches spend money, so the
 * property under test is not "does it launch" but "does it refuse to launch
 * anything that was not just confirmed on screen". Dagster, ClickHouse and the
 * settings database are faked at their module boundaries; the token, the run
 * config and the action's own branching are real.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { LlmProfile } from "~/lib/llm-settings.server";
import type { SeCompanyInfoPipelineStats } from "~/lib/se-company-info-pipeline.server";

const launchRun = vi.fn(async () => ({ runId: "run-1", status: "QUEUED" }));
const loadStats = vi.fn(async (): Promise<SeCompanyInfoPipelineStats> => STATS);

const STATS: SeCompanyInfoPipelineStats = {
  selection: {
    companyCount: 3_500_000,
    changedCount: 1_240,
    changedWithoutModelCount: 900,
    wouldCallModelCount: 340,
    neverPublishedCount: 12,
    newEvidenceCounts: { scb: 800, esef: 40, wikidata: 60 },
    ledgerPendingCount: 5,
    pendingModelCount: 410,
  },
  artifacts: [],
  models: [],
};

const PROFILE: LlmProfile = {
  profileId: "0f8f2a3e-6c1e-4a0f-9c2b-9c9a7f4b1d20",
  name: "DeepSeek production",
  provider: "deepseek",
  baseUrl: "https://api.deepseek.com",
  model: "deepseek-v4-flash",
  apiKeyEnvironmentVariable: "DEEPSEEK_API_KEY",
  isActive: true,
  apiKeyAvailable: true,
  createdAt: "2026-08-01T00:00:00.000Z",
  updatedAt: "2026-08-01T00:00:00.000Z",
};

vi.mock("~/components/admin/se-company-info-pipeline", () => ({
  SeCompanyInfoPipeline: () => null,
}));

vi.mock("~/lib/llm-settings.server", () => ({
  listLlmProfiles: () => [PROFILE],
}));

vi.mock("~/lib/dagster.server", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/lib/dagster.server")>()),
  launchRun: (...args: unknown[]) => launchRun(...(args as [])),
  listRuns: async () => [],
  assetMaterializations: async () => [],
  instigatorStates: async () => ({ schedules: [], sensors: [] }),
}));

// Only the ClickHouse read is faked here: the token, the run-config builder and
// the artifact asset names stay real, because those are what is being asserted.
vi.mock("~/lib/se-company-info-pipeline.server", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/lib/se-company-info-pipeline.server")>()),
  loadSeCompanyInfoPipelineStats: () => loadStats(),
}));

const { action } = await import("~/routes/admin-se-company-info-pipeline");

type ActionResult = Awaited<ReturnType<typeof action>>;

function post(fields: Record<string, string>): Promise<ActionResult> {
  const body = new FormData();
  for (const [key, value] of Object.entries(fields)) body.append(key, value);
  return action({
    request: new Request("http://backoffice/admin/se/company-info/pipeline", {
      method: "POST",
      body,
    }),
  } as unknown as Parameters<typeof action>[0]);
}

const RESOLVE_FIELDS = {
  use_model: "1",
  max_companies: "1000",
  concurrency: "2",
  profile_id: PROFILE.profileId,
};

async function confirmResolve(
  overrides: Record<string, string> = {},
): Promise<Record<string, string>> {
  const result = await post({ intent: "confirm-resolve", ...RESOLVE_FIELDS, ...overrides });
  expect(result.confirmation).not.toBeNull();
  return result.confirmation!.fields;
}

beforeEach(() => {
  vi.stubEnv("BACKOFFICE_ACTION_SECRET", "test-secret");
  launchRun.mockClear();
  loadStats.mockClear();
  loadStats.mockImplementation(async () => STATS);
});

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("a launch must be bound to the confirmation that described it", () => {
  it("refuses a launch that carries no token, without calling Dagster", async () => {
    const result = await post({ intent: "launch-resolve", ...RESOLVE_FIELDS });
    expect(result.ok).toBe(false);
    expect(result.error).toMatch(/confirm/i);
    expect(result.launched).toBeNull();
    expect(launchRun).not.toHaveBeenCalled();
  });

  it("refuses a token minted for a different run", async () => {
    // The confirmation said 1,000 companies; the replayed form says 1,000,000.
    // The token signs the config, so the edited field signs a different one.
    const fields = await confirmResolve();
    const result = await post({
      intent: "launch-resolve",
      ...RESOLVE_FIELDS,
      max_companies: "1000000",
      action_token: fields.action_token,
    });
    expect(result.ok).toBe(false);
    expect(result.error).toMatch(/does not match/i);
    expect(launchRun).not.toHaveBeenCalled();

    // ... and the same for the model behind it: a token for the model-on run
    // does not authorise the model-off one, nor the model pass.
    const modelOff = await post({
      intent: "launch-resolve",
      ...RESOLVE_FIELDS,
      use_model: "",
      action_token: fields.action_token,
    });
    expect(modelOff.ok).toBe(false);
    const modelPass = await post({
      intent: "launch-model-pass",
      ...RESOLVE_FIELDS,
      action_token: fields.action_token,
    });
    expect(modelPass.ok).toBe(false);
    expect(launchRun).not.toHaveBeenCalled();
  });

  it("launches the confirmed run with execute: true and the pilot tag", async () => {
    const fields = await confirmResolve();
    const result = await post({
      intent: "launch-resolve",
      ...RESOLVE_FIELDS,
      action_token: fields.action_token,
    });

    expect(result.ok).toBe(true);
    expect(result.launched?.runId).toBe("run-1");
    expect(launchRun).toHaveBeenCalledTimes(1);
    const [input] = launchRun.mock.calls[0] as unknown as [
      {
        job: string;
        runConfig: {
          ops: Record<string, { config: Record<string, unknown> }>;
        };
        tags: Record<string, string>;
      },
    ];
    expect(input.job).toBe("se_company_info_review_job");
    expect(input.tags).toEqual({ pilot: "backoffice" });
    const config = input.runConfig.ops.se_company_info_clickhouse.config;
    expect(config.execute).toBe(true);
    expect(config.max_companies).toBe(1_000);
    expect(config.resolve_multi_source_with_llm).toBe(true);
    expect(config.pending_model_only).toBe(false);
    expect(config.llm).toEqual({
      provider: "deepseek",
      model: "deepseek-v4-flash",
      base_url: "https://api.deepseek.com",
      temperature: 0,
      max_tokens: 6000,
      prompt_version: "se-company-info-description-v3",
      concurrency: 2,
    });
  });

  it("binds an artifact refresh to its own asset", async () => {
    const confirmed = await post({ intent: "confirm-artifact", artifact: "esef" });
    const token = confirmed.confirmation!.fields.action_token;

    const swapped = await post({ intent: "launch-artifact", artifact: "scb", action_token: token });
    expect(swapped.ok).toBe(false);
    expect(launchRun).not.toHaveBeenCalled();

    const launched = await post({
      intent: "launch-artifact",
      artifact: "esef",
      action_token: token,
    });
    expect(launched.ok).toBe(true);
    const [input] = launchRun.mock.calls[0] as unknown as [
      { job: string; assetSelection: string[]; runConfig: unknown; tags: Record<string, string> },
    ];
    expect(input.job).toBe("se_company_info_job");
    expect(input.assetSelection).toEqual(["se_company_info_esef_clickhouse"]);
    expect(input.runConfig).toEqual({});
    expect(input.tags).toEqual({ pilot: "backoffice" });
  });

  it("refuses everything when no signing secret is configured", async () => {
    vi.stubEnv("BACKOFFICE_ACTION_SECRET", "");
    const confirmed = await post({ intent: "confirm-resolve", ...RESOLVE_FIELDS });
    expect(confirmed.ok).toBe(false);
    expect(confirmed.error).toMatch(/BACKOFFICE_ACTION_SECRET/);
    expect(confirmed.confirmation).toBeNull();

    const launched = await post({
      intent: "launch-resolve",
      ...RESOLVE_FIELDS,
      action_token: "1893456000.deadbeef",
    });
    expect(launched.ok).toBe(false);
    expect(launched.error).toMatch(/BACKOFFICE_ACTION_SECRET/);
    expect(launchRun).not.toHaveBeenCalled();
  });
});

describe("the confirmation step", () => {
  it("restates the numbers it just re-read, and clamps what it signs", async () => {
    const fields = await confirmResolve({ max_companies: "99999999", concurrency: "42" });
    expect(loadStats).toHaveBeenCalledTimes(1);
    expect(fields.max_companies).toBe("1000000");
    expect(fields.concurrency).toBe("8");

    const result = await post({ intent: "confirm-resolve", ...RESOLVE_FIELDS });
    expect(result.confirmation?.lines[0]).toContain("1,240 companies match right now");
    expect(result.confirmation?.lines[1]).toContain("340");
    expect(result.confirmation?.intent).toBe("launch-resolve");
  });

  it("confirms nothing when the selection counts cannot be read", async () => {
    loadStats.mockImplementation(async () => {
      throw new Error("ClickHouse: ILLEGAL_AGGREGATION");
    });
    const result = await post({ intent: "confirm-resolve", ...RESOLVE_FIELDS });
    expect(result.ok).toBe(false);
    expect(result.error).toContain("ILLEGAL_AGGREGATION");
    expect(result.confirmation).toBeNull();
    expect(launchRun).not.toHaveBeenCalled();
  });

  it("refuses a model run whose provider cannot name a key variable", async () => {
    const result = await post({
      intent: "confirm-resolve",
      ...RESOLVE_FIELDS,
      profile_id: "missing-profile",
    });
    expect(result.ok).toBe(false);
    expect(result.error).toMatch(/LLM profile/);
  });
});
