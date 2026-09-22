import { beforeEach, describe, expect, it, vi } from "vitest";
const db = vi.hoisted(() => ({chQuery: vi.fn()}));
const dagster = vi.hoisted(() => ({launchRun: vi.fn(), dagsterRunUrl: vi.fn(() => "http://dagster/runs/one")}));
vi.mock("~/lib/clickhouse.server", () => db);
vi.mock("~/lib/dagster.server", () => dagster);
import { startSavedCrawls } from "~/lib/crawl-inputs.server";

function submission(type = "site_info", domains: unknown = ["novelic.com"]) {
  const form = new FormData();
  form.set("crawl_type", type);
  form.set("domains", JSON.stringify(domains));
  form.set("batch_id", "ad279a62-a14c-40a3-98ae-01c611dab1d1");
  for (const [key, value] of Object.entries({challenge_agent_model: "deepseek-flash", challenge_agent_max_runs: "3", api: "deepseek", model: "deepseek-flash", max_pages: "1", max_model_calls: "20", page_selection: type === "site_info" ? "basic_info" : "saved"})) form.set(key, value);
  return form;
}
beforeEach(() => { vi.clearAllMocks(); db.chQuery.mockResolvedValue([{domain: "novelic.com", enabled: true}]); dagster.launchRun.mockResolvedValue({runId: "one", status: "QUEUED"}); });
describe("saved crawl starts", () => {
  it.each([["full", "website_full_crawl_results"], ["jobs", "website_jobs_crawl_results"], ["site_info", "website_site_info_results"]])("launches only the %s results job", async (type, asset) => {
    expect(await startSavedCrawls(submission(type, ["novelic.com", "novelic.com"]))).toMatchObject({count: 1, runId: "one"});
    expect(dagster.launchRun).toHaveBeenCalledWith(expect.objectContaining({job: `${asset}_job`, runConfig: {ops: {[asset]: {config: {batch_id: "ad279a62-a14c-40a3-98ae-01c611dab1d1", domains: ["novelic.com"], batch_size: 1, force_refresh: false, max_in_flight: 3, refresh_interval_days: 30, challenge_agent_model: "deepseek-flash", challenge_agent_max_runs: 3, api: "deepseek", model: "deepseek-flash", max_pages: 1, max_model_calls: 20, page_selection: type === "site_info" ? "basic_info" : "saved"}}}}}), {timeoutMs: 15_000});
  });
  it.each([["unknown", ["novelic.com"]], ["full", []], ["jobs", ["' OR 1=1"]], ["jobs", "novelic.com"], ["full", Array(101).fill("novelic.com")]])("rejects invalid selection %j before SQL or launch", async (type, domains) => {
    await expect(startSavedCrawls(submission(type as string, domains))).rejects.toThrow();
    expect(db.chQuery).not.toHaveBeenCalled();
    expect(dagster.launchRun).not.toHaveBeenCalled();
  });
  it.each([{rows: []}, {rows: [{domain: "novelic.com", enabled: false}]}])("refuses missing or disabled inputs", async ({rows}) => {
    db.chQuery.mockResolvedValue(rows);
    await expect(startSavedCrawls(submission())).rejects.toThrow();
    expect(dagster.launchRun).not.toHaveBeenCalled();
  });
});

it.each(["challenge_agent_model", "challenge_agent_max_runs", "api", "model", "max_pages", "max_model_calls", "page_selection"])("requires %s before launching a result asset", async (field) => {
  const form = submission(); form.delete(field);
  await expect(startSavedCrawls(form)).rejects.toThrow();
  expect(dagster.launchRun).not.toHaveBeenCalled();
});
it("forwards mandatory settings and custom instructions to the results asset", async () => {
  const form = submission("jobs");
  form.set("challenge_agent_model", "z-ai/glm-5.3-flash"); form.set("challenge_agent_max_runs", "6");
  form.set("page_selection", "instructions"); form.set("instructions", "Find all open jobs");
  await startSavedCrawls(form);
  expect(dagster.launchRun.mock.calls[0][0].runConfig.ops.website_jobs_crawl_results.config).toMatchObject({challenge_agent_model: "z-ai/glm-5.3-flash", challenge_agent_max_runs: 6, instructions: "Find all open jobs"});
});

it("forwards concurrency and freshness settings from the activation sheet", async () => {
  const form = submission(); form.set("max_in_flight", "5"); form.set("refresh_interval_days", "7");
  await startSavedCrawls(form);
  expect(dagster.launchRun.mock.calls[0][0].runConfig.ops.website_site_info_results.config).toMatchObject({max_in_flight: 5, refresh_interval_days: 7});
});
it.each([["max_in_flight", "0"], ["max_in_flight", "21"], ["refresh_interval_days", "0"], ["refresh_interval_days", "oops"]])("rejects invalid %s=%s", async (key, value) => {
  const form = submission(); form.set(key, value);
  await expect(startSavedCrawls(form)).rejects.toThrow();
  expect(dagster.launchRun).not.toHaveBeenCalled();
});
