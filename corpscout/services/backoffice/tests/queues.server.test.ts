import { beforeEach, describe, expect, it, vi } from "vitest";
import { QUEUE_TEMPLATES, parseQueueFilters } from "~/lib/queues";
import { loadCrawlQueueCounts, loadQueueInputs, parseQueueConfig, startQueueProcessing } from "~/lib/queues.server";
import { chQuery } from "~/lib/clickhouse.server";
import { launchRun, listRuns } from "~/lib/dagster.server";
import { CrawlLlmError, prepareCrawlSettings, verifySelectedLlm } from "~/lib/crawl-llm.server";

vi.mock("~/lib/clickhouse.server", () => ({chQuery: vi.fn()}));
vi.mock("~/lib/dagster.server", () => ({launchRun: vi.fn(), listRuns: vi.fn(), dagsterRunUrl: (id: string) => `http://dagster/runs/${id}`}));
vi.mock("~/lib/crawl-llm.server", () => ({CrawlLlmError: class CrawlLlmError extends Error {}, prepareCrawlSettings: vi.fn(), verifySelectedLlm: vi.fn()}));
import { BraveSearchError, resolveBraveSearch } from "~/lib/brave-searches.server";
vi.mock("~/lib/brave-searches.server", () => ({BraveSearchError: class BraveSearchError extends Error {}, resolveBraveSearch: vi.fn()}));
const search = {search_id: "54d90187-85d5-45dc-9603-cd7c4a7d31d1", search_name: "Official website", search_revision: 1, query_type: "official_website", query_template: "Find the official website of {company_name}."};
const braveConfig = {...QUEUE_TEMPLATES.brave, brave_search_id: search.search_id, brave_search_revision: 1, llm_profile_id: "saved-model"};
const verifiedLlm = {provider: "Saved provider", base_url: "https://provider.example/v1", model: "selected/model", api_key_encrypted: "v1.test.encrypted-key"};
const wireModelConfig = {api: "openrouter", model: "selected/model", llm: verifiedLlm, crawler_config: {provider: null}};
const task = "11111111-1111-4111-8111-111111111111";
const request = "22222222-2222-4222-8222-222222222222";
const filters = (type = "webtech", crawlType = "full") => parseQueueFilters(type, new URLSearchParams({task, crawlType, search: "preview-only"}));
beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(resolveBraveSearch).mockReset().mockResolvedValue(search);
  vi.mocked(chQuery).mockResolvedValue([{total: "17"}]);
  vi.mocked(listRuns).mockResolvedValue([]);
  vi.mocked(launchRun).mockResolvedValue({runId: "launched", status: "QUEUED"});
  vi.mocked(verifySelectedLlm).mockReset().mockResolvedValue(verifiedLlm);
  vi.mocked(prepareCrawlSettings).mockReset().mockImplementation(async ({llm_profile_id: _profileId, ...settings}) => ({...settings, ...wireModelConfig}));
});

const crawlConfig = {challenge_agent_model: "deepseek-flash", challenge_agent_max_runs: 3,
  llm_profile_id: "saved-model", max_pages: 20, max_model_calls: 20,
  page_selection: "saved", max_in_flight: 3, refresh_interval_days: 30, force_refresh: false};

describe("queue processing", () => {
  it.each([
    ["webtech", "webtech_scan_results", "webtech_scan_results_job", QUEUE_TEMPLATES.webtech],
    ["brave", "company_brave_search_results", "company_brave_search_job", braveConfig],
    ["ip-enrichment", "ip_enrichment_results", "ip_enrichment_results_job", QUEUE_TEMPLATES["ip-enrichment"]],
    ["crawler", "website_full_crawl_results", "website_full_crawl_results_job", crawlConfig],
  ])("launches only the %s results asset for the whole task", async (type, asset, job, config) => {
    await startQueueProcessing(filters(String(type)), JSON.stringify(config), request, "operator");
    const input = vi.mocked(launchRun).mock.calls[0][0];
    expect(input.job).toBe(job);
    expect(input.assetSelection).toEqual([asset]);
    const expectedConfig = type === "crawler" ? {...Object.fromEntries(Object.entries(config).filter(([key]) => key !== "llm_profile_id")), ...wireModelConfig, full_crawl_all: false} : type === "brave" ? {...Object.fromEntries(Object.entries(config).filter(([key]) => !["llm_profile_id", "brave_search_id", "brave_search_revision"].includes(key))), ...search, llm: verifiedLlm} : config;
    expect(input.runConfig).toEqual({ops: {[String(asset)]: {config: {...expectedConfig as object, task_id: task}}}});
    if (type === "crawler") {
      expect(prepareCrawlSettings).toHaveBeenCalledWith({...config as object, task_id: task, full_crawl_all: false});
      expect(vi.mocked(prepareCrawlSettings).mock.invocationCallOrder[0]).toBeLessThan(vi.mocked(launchRun).mock.invocationCallOrder[0]);
    } else expect(prepareCrawlSettings).not.toHaveBeenCalled();
    if (type === "brave") expect(verifySelectedLlm).toHaveBeenCalledWith("saved-model", "brave");
    else expect(verifySelectedLlm).not.toHaveBeenCalled();
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
  it("recovers a crawler launch without verifying again or changing the encrypted configuration", async () => {
    const serialized = JSON.stringify(crawlConfig);
    await startQueueProcessing(filters("crawler"), serialized, request, "operator");
    const tags = vi.mocked(launchRun).mock.calls[0][0].tags!;
    vi.mocked(listRuns).mockResolvedValue([{runId: "launched", status: "SUCCESS", tags}] as never);
    vi.mocked(prepareCrawlSettings).mockRejectedValueOnce(new CrawlLlmError("The provider is now unavailable"));
    expect(await startQueueProcessing(filters("crawler"), serialized, request, "operator")).toMatchObject({runId: "launched", status: "SUCCESS"});
    expect(prepareCrawlSettings).toHaveBeenCalledTimes(1);
    expect(launchRun).toHaveBeenCalledTimes(1);
    await expect(startQueueProcessing(filters("crawler"), JSON.stringify({...crawlConfig, llm_profile_id: "another-model"}), request, "operator")).rejects.toThrow("different parameters");
    expect(prepareCrawlSettings).toHaveBeenCalledTimes(1);
  });
  it("blocks launch when LLM verification fails and permits a verified retry", async () => {
    vi.mocked(prepareCrawlSettings).mockRejectedValueOnce(new CrawlLlmError("LLM verification failed: model unavailable"));
    await expect(startQueueProcessing(filters("crawler"), JSON.stringify(crawlConfig), request, "operator")).rejects.toThrow("LLM verification failed: model unavailable");
    expect(launchRun).not.toHaveBeenCalled();
    await expect(startQueueProcessing(filters("crawler"), JSON.stringify(crawlConfig), request, "operator")).resolves.toMatchObject({runId: "launched"});
    expect(prepareCrawlSettings).toHaveBeenCalledTimes(2);
    expect(launchRun).toHaveBeenCalledTimes(1);
  });
  it("requires a saved LLM selection before making any external call", async () => {
    const {llm_profile_id: _profileId, ...config} = crawlConfig;
    await expect(startQueueProcessing(filters("crawler"), JSON.stringify(config), request, "operator")).rejects.toThrow("Choose an LLM");
    expect(prepareCrawlSettings).not.toHaveBeenCalled();
    expect(chQuery).not.toHaveBeenCalled();
    expect(listRuns).not.toHaveBeenCalled();
    expect(launchRun).not.toHaveBeenCalled();
  });
  it.each(["api", "model", "api_key", "api_key_encrypted", "llm"])("rejects client-supplied model or credential override %s", key => {
    expect(() => parseQueueConfig(filters("crawler"), JSON.stringify({...crawlConfig, [key]: "override"}))).toThrow(`Unsupported processing parameter: ${key}`);
    expect(prepareCrawlSettings).not.toHaveBeenCalled();
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
  expect(await loadQueueHistory(filters())).toEqual([{taskId: task, status: "SUCCESS", runUrl: "http://dagster/runs/completed", startedAt: "1970-01-01T00:16:40.000Z", outcome: null, failedPages: null, skippedPages: null, crawlType: null, sources: null, sourcesError: false}]);
  expect(chQuery).toHaveBeenCalledWith(expect.stringContaining("queue_task_sources"), expect.objectContaining({tasks: [task, task]}));
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


it("shows all crawler types on the default full-crawl queue, ordered by latest run", async () => {
  const {loadQueueHistory} = await import("~/lib/queues.server");
  vi.mocked(listRuns).mockImplementation(async ({job}) => job === "website_site_info_results_job" ? [
    {runId: "new", status: "SUCCESS", startTime: 3000, tags: {"processing/task_id": task, "crawler/outcome": "completed_with_errors", "crawler/failed_pages": "1", "crawler/skipped_pages": "2"}},
    {runId: "old", status: "FAILURE", startTime: 1000, tags: {"processing/task_id": task}},
  ] as never : job === "website_jobs_crawl_results_job" ? [
    {runId: "jobs", status: "FAILURE", startTime: 2000, tags: {"processing/task_id": request}},
  ] as never : []);
  const history = await loadQueueHistory(filters("crawler"));
  expect(listRuns).toHaveBeenCalledTimes(3);
  expect(history).toEqual([
    expect.objectContaining({crawlType: "site_info", taskId: task, outcome: "completed_with_errors", failedPages: 1, skippedPages: 2, runUrl: "http://dagster/runs/new"}),
    expect.objectContaining({crawlType: "jobs", taskId: request, status: "FAILURE"}),
  ]);
  expect(chQuery).toHaveBeenCalledWith(expect.stringContaining("website_site_info_results"), expect.objectContaining({kinds: ["jobs", "site_info", "site_info"]}));
});


describe("Brave verified assistant launch", () => {
  const config = () => (braveConfig);
  it("requires a saved search and rejects raw question overrides", async () => {
    for (const key of ["query_type", "query_template", "search_id", "search_revision", "search_name"]) {
      expect(() => parseQueueConfig(filters("brave"), JSON.stringify({...config(), [key]: "override"}))).toThrow("Unsupported processing parameter");
    }
    const {brave_search_id: _unused, ...missingSearch} = config();
    await expect(startQueueProcessing(filters("brave"), JSON.stringify(missingSearch), request, "operator")).rejects.toThrow("Choose a saved Brave search");
    expect(launchRun).not.toHaveBeenCalled();
  });
  it("blocks stale search choices before checking the LLM or launching", async () => {
    vi.mocked(resolveBraveSearch).mockRejectedValueOnce(new BraveSearchError("This search has changed"));
    await expect(startQueueProcessing(filters("brave"), JSON.stringify(config()), request, "operator")).rejects.toThrow("search has changed");
    expect(verifySelectedLlm).not.toHaveBeenCalled();
    expect(launchRun).not.toHaveBeenCalled();
  });
  it("rejects missing selection before accessing services or launching", async () => {
    await expect(startQueueProcessing(filters("brave"), JSON.stringify({...config(), llm_profile_id: ""}), request, "operator")).rejects.toThrow("nonempty string");
    const {llm_profile_id: _unused, ...withoutProfile} = config();
    await expect(startQueueProcessing(filters("brave"), JSON.stringify(withoutProfile), request, "operator")).rejects.toThrow("Choose an LLM");
    expect(listRuns).not.toHaveBeenCalled();
    expect(verifySelectedLlm).not.toHaveBeenCalled();
    expect(launchRun).not.toHaveBeenCalled();
  });
  it("retains the queue on failed vision preflight and permits a valid retry", async () => {
    vi.mocked(verifySelectedLlm).mockRejectedValueOnce(new CrawlLlmError("LLM verification failed: image input unsupported"));
    await expect(startQueueProcessing(filters("brave"), JSON.stringify(config()), request, "operator")).rejects.toThrow("image input unsupported");
    expect(launchRun).not.toHaveBeenCalled();
    await expect(startQueueProcessing(filters("brave"), JSON.stringify(config()), request, "operator")).resolves.toMatchObject({runId: "launched"});
    const runtime = vi.mocked(launchRun).mock.calls[0][0].runConfig.ops as Record<string, {config: Record<string, unknown>}>;
    expect(runtime.company_brave_search_results.config.llm).toEqual(verifiedLlm);
    expect(runtime.company_brave_search_results.config).not.toHaveProperty("llm_profile_id");
    expect(runtime.company_brave_search_results.config).not.toHaveProperty("crawler_config");
  });
  it("recovers launch acknowledgement without changing or rechecking its profile", async () => {
    const serialized = JSON.stringify(config());
    await startQueueProcessing(filters("brave"), serialized, request, "operator");
    const tags = vi.mocked(launchRun).mock.calls[0][0].tags!;
    vi.mocked(listRuns).mockResolvedValue([{runId: "launched", status: "SUCCESS", tags}] as never);
    await expect(startQueueProcessing(filters("brave"), serialized, request, "operator")).resolves.toMatchObject({runId: "launched"});
    expect(verifySelectedLlm).toHaveBeenCalledTimes(1);
    expect(launchRun).toHaveBeenCalledTimes(1);
  });
  it.each(["api_key", "api_key_encrypted", "llm", "challenge_agent_model", "model", "base_url"])("rejects raw model/credential override %s", key => {
    expect(() => parseQueueConfig(filters("brave"), JSON.stringify({...config(), [key]: "override"}))).toThrow("Unsupported processing parameter");
  });
});


it("keeps completed source websites when queue inputs have been removed", async () => {
  const {loadQueueHistory} = await import("~/lib/queues.server");
  const sources = {task_type: "webtech", task_id: task, total: "2", complete: 1, preview: [["100.se", "https://100.se/"], ["example.com", "https://example.com/jobs"]]};
  vi.mocked(listRuns).mockResolvedValue([{runId: "done", status: "SUCCESS", startTime: 1000, tags: {"processing/task_id": task}}] as never);
  vi.mocked(chQuery).mockResolvedValue([sources]);
  expect(await loadQueueHistory(filters())).toEqual([expect.objectContaining({sources, sourcesError: false})]);
});

it("preserves task status when source lookup fails", async () => {
  const {loadQueueHistory} = await import("~/lib/queues.server");
  vi.mocked(listRuns).mockResolvedValue([{runId: "done", status: "FAILURE", startTime: 1000, tags: {"processing/task_id": task}}] as never);
  vi.mocked(chQuery).mockRejectedValueOnce(new Error("source storage offline"));
  expect(await loadQueueHistory(filters())).toEqual([expect.objectContaining({status: "FAILURE", sources: null, sourcesError: true})]);
});

it("defaults full crawl all off and transports an explicit boolean override", async () => {
  expect(parseQueueConfig(filters("crawler"), JSON.stringify(crawlConfig))).toMatchObject({full_crawl_all: false});
  await startQueueProcessing(filters("crawler"), JSON.stringify({...crawlConfig, full_crawl_all: true}), request, "operator");
  expect(vi.mocked(launchRun).mock.calls[0][0].runConfig).toMatchObject({ops: {website_full_crawl_results: {config: {full_crawl_all: true}}}});
});
it.each(["true", "false", 1, 0])("rejects a nonboolean full crawl override: %j", full_crawl_all => {
  expect(() => parseQueueConfig(filters("crawler"), JSON.stringify({...crawlConfig, full_crawl_all}))).toThrow();
});
it("does not enable full crawl all for jobs or basic info", () => {
  expect(() => parseQueueConfig(filters("crawler", "jobs"), JSON.stringify({...crawlConfig, full_crawl_all: true}))).toThrow("full crawls only");
});

it("reads only the current Brave queue, including links to retired tasks", async () => {
  vi.mocked(chQuery).mockReset().mockResolvedValueOnce([]).mockResolvedValueOnce([{total: "0", tasks: "0"}])
    .mockResolvedValueOnce([{total: "0", matching: "0"}]).mockResolvedValueOnce([]);
  const result = await loadQueueInputs(filters("brave"));
  expect(result).toMatchObject({tasks: [], totalInputs: 0, selectedTotal: 0});
  expect(vi.mocked(chQuery).mock.calls).toHaveLength(4);
  for (const [sql] of vi.mocked(chQuery).mock.calls) {
    expect(sql).toContain("company_brave_queue_input");
    expect(sql).not.toContain("company_brave_search_input");
  }
});

it.each(["force", "rescan_old"])("rejects retired Brave processing option %s", name => {
  expect(() => parseQueueConfig(filters("brave"), JSON.stringify({[name]: true}))).toThrow();
});

it("reads Brave completion counts and retained companies after the input queue is cleared", async () => {
  const {loadQueueHistory} = await import("~/lib/queues.server");
  vi.mocked(listRuns).mockResolvedValue([{runId: "saved", status: "SUCCESS", startTime: 1000, tags: {
    "processing/task_id": task, "brave/outcome": "completed_with_errors", "brave/failed_pages": "2", "brave/skipped_pages": "3",
  }}] as never);
  vi.mocked(chQuery).mockResolvedValue([{task_id: task, task_type: "brave", total: "10", complete: 1, preview: [["Company", "SE:123"]]}]);
  expect(await loadQueueHistory(filters("brave"))).toEqual([expect.objectContaining({failedPages: 2, skippedPages: 3,
    outcome: "completed_with_errors", sources: expect.objectContaining({total: "10"})})]);
});
