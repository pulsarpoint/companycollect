import { beforeEach, describe, expect, it, vi } from "vitest";
import { QUEUE_TEMPLATES, parseQueueFilters } from "~/lib/queues";
import { loadCrawlQueueCounts, loadQueueInputs, parseQueueConfig, startQueueProcessing } from "~/lib/queues.server";
import { chQuery } from "~/lib/clickhouse.server";
import { launchRun, listRuns } from "~/lib/dagster.server";

vi.mock("~/lib/clickhouse.server", () => ({chQuery: vi.fn()}));
vi.mock("~/lib/dagster.server", () => ({launchRun: vi.fn(), listRuns: vi.fn(), dagsterRunUrl: (id: string) => `http://dagster/runs/${id}`}));
const task = "11111111-1111-4111-8111-111111111111";
const request = "22222222-2222-4222-8222-222222222222";
const filters = (type = "webtech", crawlType = "full") => parseQueueFilters(type, new URLSearchParams({task, crawlType, search: "preview-only"}));
beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(chQuery).mockResolvedValue([{total: "17"}]);
  vi.mocked(listRuns).mockResolvedValue([]);
  vi.mocked(launchRun).mockResolvedValue({runId: "launched", status: "QUEUED"});
});

const crawlConfig = {challenge_agent_model: "deepseek-flash", challenge_agent_max_runs: 3,
  api: "deepseek", model: "deepseek-flash", max_pages: 20, max_model_calls: 20,
  page_selection: "saved", max_in_flight: 3, refresh_interval_days: 30, force_refresh: false};

describe("queue processing", () => {
  it.each([
    ["webtech", "webtech_scan_results", "webtech_scan_results_job", QUEUE_TEMPLATES.webtech],
    ["brave", "company_brave_search_results", "company_brave_search_job", QUEUE_TEMPLATES.brave],
    ["ip-enrichment", "ip_enrichment_results", "ip_enrichment_results_job", QUEUE_TEMPLATES["ip-enrichment"]],
    ["crawler", "website_full_crawl_results", "website_full_crawl_results_job", crawlConfig],
  ])("launches only the %s results asset for the whole task", async (type, asset, job, config) => {
    await startQueueProcessing(filters(String(type)), JSON.stringify(config), request, "operator");
    const input = vi.mocked(launchRun).mock.calls[0][0];
    expect(input.job).toBe(job);
    expect(input.assetSelection).toEqual([asset]);
    expect(input.runConfig).toEqual({ops: {[String(asset)]: {config: {...config as object, task_id: task}}}});
    expect(input.tags?.["processing/task_id"]).toBe(task);
    expect(chQuery).toHaveBeenCalledWith(expect.not.stringContaining("preview-only"), {task, crawlType: "full"});
  });
  it.each(["jobs", "site_info"])("uses the %s crawler results job", async type => {
    const config = {...crawlConfig, ...(type === "site_info" ? {max_pages: 1, page_selection: "basic_info"} : {})};
    await startQueueProcessing(filters("crawler", type), JSON.stringify(config), request, "operator");
    expect(launchRun).toHaveBeenCalledWith(expect.objectContaining({job: type === "jobs" ? "website_jobs_crawl_results_job" : "website_site_info_results_job"}));
  });
  it.each(["task_id", "source_relation", "targets", "ops", "resources", "input_relation", "mode"])("rejects selection or workflow override %s", key => {
    expect(() => parseQueueConfig(filters(), JSON.stringify({[key]: "override"}))).toThrow("Unsupported processing parameter");
  });
  it("refuses empty tasks", async () => {
    vi.mocked(chQuery).mockResolvedValue([{total: "0"}]);
    await expect(startQueueProcessing(filters(), "{}", request, "operator")).rejects.toThrow("no inputs");
    expect(launchRun).not.toHaveBeenCalled();
  });
  it("refuses tasks with an active input or results run", async () => {
    vi.mocked(listRuns).mockResolvedValueOnce([]).mockResolvedValueOnce([{runId: "active", status: "STARTED"}] as never);
    await expect(startQueueProcessing(filters(), "{}", request, "operator")).rejects.toThrow("active Dagster run");
    expect(launchRun).not.toHaveBeenCalled();
    expect(listRuns).toHaveBeenLastCalledWith(expect.objectContaining({tags: {"processing/task_id": task}, statuses: expect.arrayContaining(["QUEUED"])}));
  });
  it("recovers a lost launch acknowledgement using the same request ID", async () => {
    await startQueueProcessing(filters(), "{}", request, "operator");
    const tags = vi.mocked(launchRun).mock.calls[0][0].tags!;
    vi.mocked(listRuns).mockResolvedValue([{runId: "launched", status: "SUCCESS", tags}] as never);
    const receipt = await startQueueProcessing(filters(), "{}", request, "operator");
    expect(receipt.runId).toBe("launched");
    expect(launchRun).toHaveBeenCalledTimes(1);
    await expect(startQueueProcessing(filters(), '{"force_rescan":true}', request, "operator")).rejects.toThrow("different parameters");
  });
  it("serializes simultaneous clicks before checking active runs", async () => {
    let launched = false;
    vi.mocked(listRuns).mockImplementation(async input => input.statuses && launched ? [{runId: "active", status: "QUEUED"}] as never : []);
    vi.mocked(launchRun).mockImplementation(async () => {launched = true; return {runId: "active", status: "QUEUED"};});
    const outcomes = await Promise.allSettled([request, "33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444"].map(id => startQueueProcessing(filters(), "{}", id, "operator")));
    expect(outcomes.filter(outcome => outcome.status === "fulfilled")).toHaveLength(1);
    expect(launchRun).toHaveBeenCalledTimes(1);
  });
  it("fails closed when Dagster cannot report active work", async () => {
    vi.mocked(listRuns).mockRejectedValue(new Error("unavailable"));
    await expect(startQueueProcessing(filters(), "{}", request, "operator")).rejects.toThrow("unavailable");
    expect(launchRun).not.toHaveBeenCalled();
  });
  it("uses parameter binding and keeps preview filtering out of task totals", async () => {
    vi.mocked(chQuery).mockResolvedValue([]);
    await loadQueueInputs(filters("crawler", "jobs"));
    for (const [sql, params] of vi.mocked(chQuery).mock.calls) {
      expect(sql).not.toContain(task);
      expect(sql).not.toContain("preview-only");
      expect(params).toMatchObject({task, crawlType: "jobs", search: "preview-only"});
    }
  });
  it("validates route types and task IDs", () => {
    expect(() => filters("arbitrary_table")).toThrow();
    expect(() => parseQueueFilters("webtech", new URLSearchParams({task: "bad"}))).toThrow();
  });
});

it("reads the crawler entry table without FINAL and rejects the retired batch_size", async () => {
  vi.mocked(chQuery).mockResolvedValue([]);
  await loadQueueInputs(filters("crawler", "site_info"));
  for (const [sql] of vi.mocked(chQuery).mock.calls) expect(sql).not.toContain("website_crawl_task_domains FINAL");
  await expect(startQueueProcessing(filters("crawler"), JSON.stringify({...crawlConfig, batch_size: 25}), request, "operator")).rejects.toThrow("batch_size");
});

it.each([{batch_size: 5000}, {recent_days: 3651}, {force_rescan: "true"}, {recent_days: 1.5}, {execution_id: "invalid"}])("rejects invalid parameters before launching: %j", async config => {
  await expect(startQueueProcessing(filters(), JSON.stringify(config), request, "operator")).rejects.toThrow();
  expect(launchRun).not.toHaveBeenCalled();
});

it("keeps task history independent of queue inputs and shows the latest retry status", async () => {
  const {loadQueueHistory} = await import("~/lib/queues.server");
  vi.mocked(listRuns).mockResolvedValue([
    {runId: "completed", status: "SUCCESS", startTime: 1000, tags: {"processing/task_id": task}},
    {runId: "failed", status: "FAILURE", startTime: 900, tags: {"processing/task_id": task}},
    {runId: "unrelated", status: "SUCCESS", tags: {}},
  ] as never);
  expect(await loadQueueHistory(filters())).toEqual([{taskId: task, status: "SUCCESS", runUrl: "http://dagster/runs/completed", startedAt: "1970-01-01T00:16:40.000Z", outcome: null, failedPages: null}]);
  expect(chQuery).not.toHaveBeenCalled();
});

it("distinguishes completed website errors from a failed pipeline run", async () => {
  const {loadQueueHistory} = await import("~/lib/queues.server");
  const tags = {"processing/task_id": task, "webtech/outcome": "completed_with_errors", "webtech/failed_pages": "5"};
  vi.mocked(listRuns).mockResolvedValue([{runId: "saved", status: "SUCCESS", startTime: null, tags}] as never);
  expect(await loadQueueHistory(filters())).toEqual([expect.objectContaining({status: "SUCCESS", outcome: "completed_with_errors", failedPages: 5})]);
  vi.mocked(listRuns).mockResolvedValue([{runId: "failed", status: "FAILURE", startTime: null, tags}] as never);
  expect(await loadQueueHistory(filters())).toEqual([expect.objectContaining({status: "FAILURE", outcome: null, failedPages: null})]);
});

it("counts crawler queue entries per crawl type, zero for empty queues", async () => {
  vi.mocked(chQuery).mockResolvedValue([{crawl_type: "site_info", total: "4"}, {crawl_type: "jobs", total: "1200"}]);
  expect(await loadCrawlQueueCounts()).toEqual({full: 0, jobs: 1200, site_info: 4});
  expect(vi.mocked(chQuery).mock.calls.at(-1)?.[0]).toContain("FROM corpscout.website_crawl_task_domains GROUP BY crawl_type");
});
