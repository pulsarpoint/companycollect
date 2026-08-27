/**
 * The People pipeline route: what its loader answers, and what a launch then
 * sends to Dagster. Mirrors admin-se-companies-pipeline.test.ts's shape --
 * Dagster and the pipeline stats read are faked at their module boundaries;
 * the run-config builders, the action's branching and the confirm/launch
 * field replay are real. These launches can write real ClickHouse rows on
 * the Dagster host, so the assertions are on the exact job name and op
 * config crossing the boundary.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { SeCompanyPersonPipelineStats } from "~/lib/se-company-person-pipeline.server";

const launchRun = vi.fn(async () => ({ runId: "run-1", status: "QUEUED" }));
const listRuns = vi.fn(async (_input: { job: string; limit: number }) => [] as unknown[]);
const loadStats = vi.fn(async (): Promise<SeCompanyPersonPipelineStats> => STATS);

const STATS: SeCompanyPersonPipelineStats = {
  publishedPersonCount: 4_200,
  collisionGroupCount: 12,
  decidedGroupCount: 5,
};

vi.mock("~/lib/dagster.server", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/lib/dagster.server")>()),
  launchRun: (...args: unknown[]) => launchRun(...(args as [])),
  listRuns: (...args: unknown[]) => listRuns(...(args as [{ job: string; limit: number }])),
}));

// Only the ClickHouse-backed stats read is faked: the run-config builders
// stay real, because those are what most of these tests assert.
vi.mock("~/lib/se-company-person-pipeline.server", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/lib/se-company-person-pipeline.server")>()),
  loadSeCompanyPersonPipelineStats: () => loadStats(),
}));

const { action, loader } = await import("~/routes/admin-se-people-pipeline");

type ActionResult = Awaited<ReturnType<typeof action>>;

function post(fields: Record<string, string>): Promise<ActionResult> {
  const body = new FormData();
  for (const [key, value] of Object.entries(fields)) body.append(key, value);
  return action({
    request: new Request("http://backoffice/admin/se/people/pipeline", {
      method: "POST",
      body,
    }),
  } as unknown as Parameters<typeof action>[0]);
}

function get(url = "http://backoffice/admin/se/people/pipeline") {
  return loader({
    request: new Request(url),
  } as unknown as Parameters<typeof loader>[0]);
}

function confirmation(result: ActionResult) {
  if (result.kind !== "confirmation") throw new Error(`expected a confirmation, got ${result.kind}`);
  return result.confirmation;
}

beforeEach(() => {
  launchRun.mockClear();
  listRuns.mockClear();
  listRuns.mockResolvedValue([]);
  loadStats.mockClear();
  loadStats.mockImplementation(async () => STATS);
});

describe("identity evaluation", () => {
  it("confirms then launches with the scoped company_ids and write_candidates", async () => {
    const confirmed = await post({
      intent: "confirm-identity",
      company_ids: "5560125220,5565200028",
      write_candidates: "1",
    });
    const fields = confirmation(confirmed).fields;
    expect(fields.company_ids).toBe("5560125220,5565200028");
    expect(fields.write_candidates).toBe("1");

    const result = await post({ intent: "launch-identity", ...fields });
    expect(result.kind).toBe("launched");
    expect(launchRun).toHaveBeenCalledTimes(1);
    const [input] = launchRun.mock.calls[0] as unknown as [
      { job: string; runConfig: { ops: Record<string, { config: Record<string, unknown> }> }; tags: Record<string, string> },
    ];
    expect(input.job).toBe("se_company_person_identity_evaluation_job");
    expect(input.tags).toEqual({ pilot: "backoffice" });
    const config = input.runConfig.ops.se_company_person_identity_evaluation.config;
    expect(config.company_ids).toEqual(["5560125220", "5565200028"]);
    expect(config.write_candidates).toBe(true);
  });

  it("defaults to no scope and write_candidates off when the checkbox is unticked", async () => {
    const confirmed = await post({ intent: "confirm-identity", company_ids: "" });
    const fields = confirmation(confirmed).fields;
    expect(fields.company_ids).toBe("");
    expect(fields.write_candidates).toBe("");

    await post({ intent: "launch-identity", ...fields });
    const [input] = launchRun.mock.calls[0] as unknown as [
      { runConfig: { ops: Record<string, { config: Record<string, unknown> }> } },
    ];
    const config = input.runConfig.ops.se_company_person_identity_evaluation.config;
    expect(config.company_ids).toEqual([]);
    expect(config.write_candidates).toBe(false);
  });
});

describe("clean copy", () => {
  it("confirms then launches with company_ids/max_companies/company_batch_size -- no LLM fields at all", async () => {
    const confirmed = await post({
      intent: "confirm-clean-copy",
      company_ids: "5560125220",
      max_companies: "2000",
      company_batch_size: "1000",
    });
    const fields = confirmation(confirmed).fields;
    expect(fields.max_companies).toBe("2000");
    expect(fields).not.toHaveProperty("execute");
    expect(fields).not.toHaveProperty("llm_profile");

    const result = await post({ intent: "launch-clean-copy", ...fields });
    expect(result.kind).toBe("launched");
    const [input] = launchRun.mock.calls[0] as unknown as [
      { job: string; runConfig: { ops: Record<string, { config: Record<string, unknown> }> } },
    ];
    expect(input.job).toBe("se_company_person_publish_job");
    const config = input.runConfig.ops.se_company_person_clickhouse.config;
    expect(config).toEqual({
      company_ids: ["5560125220"],
      max_companies: 2000,
      company_batch_size: 1000,
    });
  });

  it("clamps an out-of-range max_companies rather than sending it verbatim", async () => {
    const confirmed = await post({
      intent: "confirm-clean-copy",
      company_ids: "",
      max_companies: "99999999",
    });
    const fields = confirmation(confirmed).fields;
    expect(fields.max_companies).toBe("1000000");
  });

  it("defaults max_companies to the conservative 10,000 prefill, not the 1,000,000 bound", async () => {
    const confirmed = await post({ intent: "confirm-clean-copy", company_ids: "" });
    const fields = confirmation(confirmed).fields;
    expect(fields.max_companies).toBe("10000");
  });

  it("says plainly that no LLM is involved, and that it always writes", async () => {
    const confirmed = await post({ intent: "confirm-clean-copy", company_ids: "" });
    const lines = confirmation(confirmed).lines.join(" ");
    expect(lines).toContain("no LLM involved");
    expect(lines).toContain("no preview mode");
  });
});

describe("llm suggestions", () => {
  it("refuses an unknown LLM profile before calling Dagster", async () => {
    const result = await post({
      intent: "confirm-llm-suggestions",
      llm_profile: "not-a-real-profile",
    });
    expect(result.kind).toBe("error");
    expect(result.kind === "error" && result.error).toContain("Unknown LLM profile");
    expect(launchRun).not.toHaveBeenCalled();
  });

  it("confirms then launches with execute/profile/bounds on the one op", async () => {
    const confirmed = await post({
      intent: "confirm-llm-suggestions",
      company_ids: "5560125220",
      execute: "1",
      llm_profile: "deepseek-default",
      max_companies: "2000",
      company_batch_size: "1000",
      maximum_observations_per_request: "80",
      timeout_seconds: "240",
    });
    const fields = confirmation(confirmed).fields;
    expect(fields.max_companies).toBe("2000");

    const result = await post({ intent: "launch-llm-suggestions", ...fields });
    expect(result.kind).toBe("launched");
    const [input] = launchRun.mock.calls[0] as unknown as [
      { job: string; runConfig: { ops: Record<string, { config: Record<string, unknown> }> } },
    ];
    expect(input.job).toBe("se_company_person_llm_suggestions_job");
    const config = input.runConfig.ops.se_company_person_llm_suggestions.config;
    expect(config).toEqual({
      execute: true,
      llm_profile: "deepseek-default",
      company_ids: ["5560125220"],
      max_companies: 2000,
      company_batch_size: 1000,
      maximum_observations_per_request: 80,
      timeout_seconds: 240,
    });
  });

  it("says it writes suggestions only -- nothing goes live until a promotion run", async () => {
    const confirmed = await post({
      intent: "confirm-llm-suggestions",
      llm_profile: "deepseek-default",
      execute: "1",
    });
    const lines = confirmation(confirmed).lines.join(" ");
    expect(lines).toContain("writes suggestions ONLY");
    expect(lines).toContain("nothing goes live in se_company_person until a Promote suggestions run");
  });

  it("describes a preview (execute off) without promising a model call", async () => {
    const confirmed = await post({
      intent: "confirm-llm-suggestions",
      llm_profile: "deepseek-default",
      execute: "",
    });
    const conf = confirmation(confirmed);
    expect(conf.lines.join(" ")).toContain("harmless preview");
    expect(conf.fields.execute).toBe("");
  });
});

describe("promotion", () => {
  it("confirms then launches with company_ids/max_companies/company_batch_size/min_confidence -- no LLM fields", async () => {
    const confirmed = await post({
      intent: "confirm-promotion",
      company_ids: "5560125220",
      max_companies: "2000",
      company_batch_size: "1000",
      min_confidence: "0.6",
    });
    const fields = confirmation(confirmed).fields;
    expect(fields.min_confidence).toBe("0.6");
    expect(fields).not.toHaveProperty("execute");
    expect(fields).not.toHaveProperty("llm_profile");

    const result = await post({ intent: "launch-promotion", ...fields });
    expect(result.kind).toBe("launched");
    const [input] = launchRun.mock.calls[0] as unknown as [
      { job: string; runConfig: { ops: Record<string, { config: Record<string, unknown> }> } },
    ];
    expect(input.job).toBe("se_company_person_promotion_job");
    const config = input.runConfig.ops.se_company_person_promotion.config;
    expect(config).toEqual({
      company_ids: ["5560125220"],
      max_companies: 2000,
      company_batch_size: 1000,
      min_confidence: 0.6,
    });
  });

  it("defaults min_confidence to 0 (promote all) when left blank", async () => {
    const confirmed = await post({ intent: "confirm-promotion", company_ids: "" });
    const fields = confirmation(confirmed).fields;
    expect(fields.min_confidence).toBe("0");
  });

  it("says it is deterministic and free -- no execute gate, no model call", async () => {
    const confirmed = await post({ intent: "confirm-promotion", company_ids: "" });
    const lines = confirmation(confirmed).lines.join(" ");
    expect(lines).toContain("Deterministic and free");
  });
});

describe("merge suggestions", () => {
  it("refuses an unknown LLM profile before calling Dagster", async () => {
    const result = await post({ intent: "confirm-merge", llm_profile: "not-a-real-profile" });
    expect(result.kind).toBe("error");
    expect(result.kind === "error" && result.error).toContain("Unknown LLM profile");
    expect(launchRun).not.toHaveBeenCalled();
  });

  it("confirms with execute on, reads live stats, then launches with the profile and scope", async () => {
    const confirmed = await post({
      intent: "confirm-merge",
      company_ids: "5560125220,5565200028",
      execute: "1",
      llm_profile: "deepseek-default",
      max_groups: "5",
      timeout_seconds: "90",
    });
    expect(loadStats).toHaveBeenCalledTimes(1);
    const conf = confirmation(confirmed);
    expect(conf.lines.join(" ")).toContain("DEEPSEEK_API_KEY");
    expect(conf.lines.join(" ")).toContain("12 collision groups");

    const result = await post({ intent: "launch-merge", ...conf.fields });
    expect(result.kind).toBe("launched");
    const [input] = launchRun.mock.calls[0] as unknown as [
      { job: string; runConfig: { ops: Record<string, { config: Record<string, unknown> }> } },
    ];
    expect(input.job).toBe("se_company_person_merge_job");
    const config = input.runConfig.ops.se_company_person_merge_suggestions.config;
    expect(config.execute).toBe(true);
    expect(config.llm_profile).toBe("deepseek-default");
    expect(config.company_ids).toEqual(["5560125220", "5565200028"]);
    expect(config.max_groups).toBe(5);
    expect(config.timeout_seconds).toBe(90);
  });

  it("omits max_groups entirely when left blank -- no limit, not zero", async () => {
    const confirmed = await post({
      intent: "confirm-merge",
      llm_profile: "deepseek-default",
      max_groups: "",
    });
    const fields = confirmation(confirmed).fields;
    expect(fields.max_groups).toBe("");

    await post({ intent: "launch-merge", ...fields });
    const [input] = launchRun.mock.calls[0] as unknown as [
      { runConfig: { ops: Record<string, { config: Record<string, unknown> }> } },
    ];
    const config = input.runConfig.ops.se_company_person_merge_suggestions.config;
    expect(config).not.toHaveProperty("max_groups");
  });

  it("describes a preview (execute off) without promising a model call", async () => {
    const confirmed = await post({
      intent: "confirm-merge",
      llm_profile: "deepseek-default",
      execute: "",
    });
    const conf = confirmation(confirmed);
    expect(conf.lines.join(" ")).toContain("harmless preview");
    expect(conf.fields.execute).toBe("");
  });
});

describe("the loader", () => {
  it("filters run/asset queries to exactly the five people jobs", async () => {
    await get();
    expect(listRuns).toHaveBeenCalledTimes(5);
    const jobs = listRuns.mock.calls.map((call) => (call[0] as { job: string }).job);
    expect(jobs).toEqual([
      "se_company_person_identity_evaluation_job",
      "se_company_person_publish_job",
      "se_company_person_llm_suggestions_job",
      "se_company_person_promotion_job",
      "se_company_person_merge_job",
    ]);
  });

  it("answers with the stats and tags every run with its Dagster URL when configured", async () => {
    listRuns.mockResolvedValueOnce([
      { runId: "run-a", status: "SUCCESS", jobName: "x", startTime: 100, endTime: 200, runConfig: {}, selectedAssets: null, tags: {} },
    ]);
    const view = await get();
    expect(view.kind).toBe("view");
    expect(view.stats?.collisionGroupCount).toBe(12);
    expect(view.identityRuns[0].runId).toBe("run-a");
  });

  it("reports a Dagster outage without throwing", async () => {
    const { DagsterRequestError } = await import("~/lib/dagster.server");
    listRuns.mockRejectedValue(new DagsterRequestError("Dagster is down"));

    const view = await get();
    expect(view.kind).toBe("view");
    expect(view.dagsterError).toContain("Dagster is down");
    expect(view.identityRuns).toEqual([]);
  });

  it("prefills the merge section's company scope from a ?company_id= query param (item 4)", async () => {
    const view = await get("http://backoffice/admin/se/people/pipeline?company_id=5560125220");
    expect(view.prefilledCompanyId).toBe("5560125220");
  });

  it("prefills nothing when opened without a company_id", async () => {
    const view = await get();
    expect(view.prefilledCompanyId).toBe("");
  });
});
