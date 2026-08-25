/**
 * The pipeline resource route: what its loader answers, and what a launch then
 * sends to Dagster.
 *
 * These launches spend money, so the assertions are on the exact run config and
 * tags that cross the boundary -- `execute: true` above all, since a run without
 * it is a preview that writes nothing, and the `company_ids` scope, which is the
 * whole of what a reviewer's picks change about a run. Dagster, ClickHouse and
 * the settings database are faked at their module boundaries; the run-config
 * builder, the artifact asset names and the action's own branching are real.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
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

// Only the ClickHouse read is faked: the run-config builder and the artifact
// asset names stay real, because those are what is being asserted.
vi.mock("~/lib/se-company-info-pipeline.server", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/lib/se-company-info-pipeline.server")>()),
  loadSeCompanyInfoPipelineStats: () => loadStats(),
}));

const { action, loader } = await import("~/routes/admin-se-companies-pipeline");

type ActionResult = Awaited<ReturnType<typeof action>>;

/** The URL a fetcher actually posts to: React Router 8 fetches route data at
 * `<path>.data`, and the loader below tells a document GET from a data request
 * by exactly that suffix. */
const DATA_URL = "http://backoffice/admin/se/companies/pipeline.data";

function post(fields: Record<string, string>): Promise<ActionResult> {
  const body = new FormData();
  for (const [key, value] of Object.entries(fields)) body.append(key, value);
  return action({
    request: new Request(DATA_URL, { method: "POST", body }),
  } as unknown as Parameters<typeof action>[0]);
}

function get(url: string, accept: string) {
  return loader({
    request: new Request(url, { headers: { Accept: accept } }),
  } as unknown as Parameters<typeof loader>[0]);
}

const BROWSER_ACCEPT =
  "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";

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
  launchRun.mockClear();
  loadStats.mockClear();
  loadStats.mockImplementation(async () => STATS);
});

describe("launching a confirmed run", () => {
  it("launches the confirmed run with execute: true and the pilot tag", async () => {
    // The two steps as the page performs them: confirm re-reads the numbers and
    // hands back the fields, the launch form posts them straight back.
    const fields = await confirmResolve();
    const result = await post({ intent: "launch-resolve", ...fields });

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
    // Nothing was picked on the list, and the config says which run that is.
    expect(config.company_ids).toEqual([]);
  });

  it("scopes the launch to the picked companies, exactly as confirmed", async () => {
    // What the sheet posts: the selection as ONE field. It has to survive the
    // confirm step -- the launch rebuilds the run from the replayed fields, so
    // a scope that fell out here would start an unbounded run instead.
    const picked = "5560125220,5565200028";
    const fields = await confirmResolve({ company_ids: picked });
    expect(fields.company_ids).toBe(picked);

    const result = await post({ intent: "launch-resolve", ...fields });
    expect(result.ok).toBe(true);
    const [input] = launchRun.mock.calls[0] as unknown as [
      { runConfig: { ops: Record<string, { config: Record<string, unknown> }> } },
    ];
    const config = input.runConfig.ops.se_company_info_clickhouse.config;
    expect(config.company_ids).toEqual(["5560125220", "5565200028"]);
    // The scope narrows the run; it does not turn the safety flags off.
    expect(config.execute).toBe(true);
    expect(config.max_companies).toBe(1_000);
  });

  it("scopes a model pass the same way, and drops blanks and repeats", async () => {
    const confirmed = await post({
      intent: "confirm-model-pass",
      ...RESOLVE_FIELDS,
      company_ids: "5560125220,,5560125220,5565200028",
    });
    expect(confirmed.confirmation?.fields.company_ids).toBe("5560125220,5565200028");

    await post({ intent: "launch-model-pass", ...confirmed.confirmation!.fields });
    const [input] = launchRun.mock.calls[0] as unknown as [
      { runConfig: { ops: Record<string, { config: Record<string, unknown> }> } },
    ];
    const config = input.runConfig.ops.se_company_info_clickhouse.config;
    expect(config.company_ids).toEqual(["5560125220", "5565200028"]);
    expect(config.pending_model_only).toBe(true);
  });

  it("binds an artifact refresh to its own asset", async () => {
    const confirmed = await post({ intent: "confirm-artifact", artifact: "esef" });
    expect(confirmed.confirmation?.fields).toEqual({ artifact: "esef" });

    const launched = await post({ intent: "launch-artifact", ...confirmed.confirmation!.fields });
    expect(launched.ok).toBe(true);
    const [input] = launchRun.mock.calls[0] as unknown as [
      { job: string; assetSelection: string[]; runConfig: unknown; tags: Record<string, string> },
    ];
    expect(input.job).toBe("se_company_info_job");
    // The asset is derived from the artifact, never a fixed one: a refresh of
    // scb must not run esef's asset.
    expect(input.assetSelection).toEqual(["se_company_info_esef_clickhouse"]);
    expect(input.runConfig).toEqual({});
    expect(input.tags).toEqual({ pilot: "backoffice" });

    await post({ intent: "launch-artifact", artifact: "scb" });
    const [second] = launchRun.mock.calls[1] as unknown as [{ assetSelection: string[] }];
    expect(second.assetSelection).toEqual(["se_company_info_scb_clickhouse"]);
  });

  it("refuses an artifact the pipeline does not have, without calling Dagster", async () => {
    const result = await post({ intent: "launch-artifact", artifact: "se_company_info" });
    expect(result.ok).toBe(false);
    expect(launchRun).not.toHaveBeenCalled();
  });

});

describe("the confirmation step", () => {
  it("restates the numbers it just re-read, and clamps what it hands back", async () => {
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

  it("says what a SCOPED run covers instead of the scan's total", async () => {
    // 1,240 companies match the scan, but this run is two picked ids: claiming
    // the scan's number for it would be a lie about what is being agreed to.
    const result = await post({
      intent: "confirm-resolve",
      ...RESOLVE_FIELDS,
      company_ids: "5560125220,5565200028",
    });
    expect(result.confirmation?.lines[0]).toContain("2 selected companies");
    expect(result.confirmation?.lines[0]).toContain(
      "only the ones the change scan still selects are resolved",
    );
    expect(result.confirmation?.lines[0]).not.toContain("1,240");
    // ... and the model line stops promising a call count it cannot know.
    expect(result.confirmation?.lines[1]).not.toContain("340");
  });

  it("tells an artifact refresh apart: it takes no company scope at all", async () => {
    const result = await post({ intent: "confirm-artifact", artifact: "scb" });
    expect(result.confirmation?.fields).toEqual({ artifact: "scb" });
    expect(result.confirmation?.lines.join(" ")).toContain(
      "takes no company scope",
    );
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

describe("the route is not a page any more", () => {
  it("redirects a person who navigates to the old URL back to the list", async () => {
    // A document GET: no `.data` suffix, and a browser's Accept. The redirect is
    // thrown, so it never becomes a JSON body -- and nothing is read on the way,
    // which is the point: the change scan costs a FINAL read of 3.5M rows.
    await expect(
      get("http://backoffice/admin/se/companies/pipeline", BROWSER_ACCEPT),
    ).rejects.toSatisfy((thrown: unknown) => {
      expect(thrown).toBeInstanceOf(Response);
      const response = thrown as Response;
      expect(response.status).toBe(302);
      expect(response.headers.get("Location")).toBe("/admin/se/companies");
      return true;
    });
    expect(loadStats).not.toHaveBeenCalled();
  });

  it("answers the sheet's fetcher, which asks for `<path>.data`", async () => {
    const view = await get(DATA_URL, "*/*");
    expect(view.kind).toBe("view");
    expect(view.stats?.selection.changedCount).toBe(1_240);
    expect(view.profiles[0].name).toBe("DeepSeek production");
    // The key variable the Dagster host will read, beside the stored one.
    expect(view.profiles[0].dagsterApiKeyVariable).toBe("DEEPSEEK_API_KEY");
    expect(loadStats).toHaveBeenCalledTimes(1);
  });

  it("serves data whenever the request is not plainly a navigation", async () => {
    // The failure that keeps the sheet working: a fetch with no Accept header,
    // or one that asks for anything, is answered with data rather than bounced
    // into a navigation the fetcher cannot follow.
    const view = await get(DATA_URL, "");
    expect(view.kind).toBe("view");
    expect(
      (await get("http://backoffice/admin/se/companies/pipeline", "*/*")).kind,
    ).toBe("view");
  });

  it("serves DATA when the two signals disagree: a `.data` URL asking for html", async () => {
    // The one case that decides which signal is in charge. `.data` is the
    // framework's own marker for a data request, so it wins: a fetcher whose
    // request happened to inherit a document Accept must be answered, never
    // redirected -- a redirect there is the navigation-out-of-the-sheet
    // catastrophe the second signal exists to prevent.
    const view = await get(DATA_URL, BROWSER_ACCEPT);
    expect(view.kind).toBe("view");
    expect(view.stats?.selection.changedCount).toBe(1_240);
  });
});
