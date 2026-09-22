import { beforeEach, describe, expect, it, vi } from "vitest";

const dagster = vi.hoisted(() => ({ launchRun: vi.fn(), dagsterRunUrl: vi.fn(() => "http://dagster/runs/input") }));
vi.mock("~/lib/dagster.server", () => dagster);
// Any regression to direct writes must fail this test boundary.
vi.mock("~/lib/clickhouse.server", () => { throw new Error("Input selection belongs to Dagster"); });
import { launchSeDomainCrawlWorkflow, saveSeDomainCrawlInputs } from "~/lib/se-domain-crawl.server";
import { EMPTY_SE_DOMAINS_FILTERS } from "~/lib/se-domains-filters";

describe("SE domain crawl input saving", () => {
  beforeEach(() => {
    dagster.launchRun.mockReset().mockResolvedValue({runId: "input", status: "QUEUED"});
  });

  it.each([["full", "website_full_crawl"], ["jobs", "website_jobs_crawl"], ["site_info", "website_site_info"]])("launches only the %s input asset", async (type, prefix) => {
    const result = await saveSeDomainCrawlInputs({ mode: "ids", domains: ["shared.se", "other.se", "shared.se"] }, type);
    expect(dagster.launchRun).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({
      job: `${prefix}_input_job`,
      runConfig: {ops: {[`${prefix}_requests`]: {config: {
        source_relation: "corpscout.se_company_domain", source_final: true,
        id_column: "root_domain", website_column: "root_domain", ids: ["shared.se", "other.se"],
      }}}},
    }), {timeoutMs: 15_000});
    expect(result).toMatchObject({ok: true, runId: "input", runUrl: "http://dagster/runs/input", table: `corpscout.${prefix}_requests`});
    expect(result).not.toHaveProperty("insertedDomains");
  });

  it("passes all applied filters and exclusions without expanding the selection in Backoffice", async () => {
    await saveSeDomainCrawlInputs({ mode: "query", query: {
      domain: "exa%' or 1=1 --", company: "5560049529", source: "brave", association: "connected",
      status: "inactive", minConfidence: "0.4", maxConfidence: "0.9", shared: "1",
    }, excludedDomains: ["skip.se", "skip.se"] }, "jobs");
    const config = dagster.launchRun.mock.calls[0][0].runConfig.ops.website_jobs_crawl_requests.config;
    expect(config).toMatchObject({select_all: true, excluded_ids: ["skip.se"], se_domain_filters: {
      domain: "exa%' or 1=1 --", company: "5560049529", source: "brave", association: "connected",
      status: "inactive", min_confidence: .4, max_confidence: .9, shared: true,
    }});
    expect(config).not.toHaveProperty("max_domains");
  });

  it("supports an explicitly selected complete unfiltered inventory", async () => {
    await saveSeDomainCrawlInputs({ mode: "query", query: EMPTY_SE_DOMAINS_FILTERS, excludedDomains: [] }, "full");
    expect(dagster.launchRun.mock.calls[0][0].runConfig.ops.website_full_crawl_requests.config.select_all).toBe(true);
  });

  it.each([
    null, { mode: "ids", domains: [] }, { mode: "ids", domains: ["https://example.se"] },
    { mode: "query", query: {}, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, source: "unknown" }, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, status: "typo" }, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, page: "2" }, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, minConfidence: "0.9", maxConfidence: "0.5" }, excludedDomains: [] },
    { mode: "query", query: EMPTY_SE_DOMAINS_FILTERS, excludedDomains: ["x'); DROP TABLE x"] },
  ])("rejects malformed selections without widening them: %j", async (selection) => {
    await expect(saveSeDomainCrawlInputs(selection, "full")).rejects.toThrow();
    expect(dagster.launchRun).not.toHaveBeenCalled();
  });

  it("rejects unknown crawl types before launching", async () => {
    await expect(saveSeDomainCrawlInputs({ mode: "ids", domains: ["valid.se"] }, "other")).rejects.toThrow();
    expect(dagster.launchRun).not.toHaveBeenCalled();
  });

  it("propagates a failed Dagster launch without claiming success", async () => {
    dagster.launchRun.mockRejectedValue(new Error("Dagster unavailable"));
    await expect(saveSeDomainCrawlInputs({ mode: "ids", domains: ["valid.se"] }, "full")).rejects.toThrow("Dagster unavailable");
  });
});

describe("SE domain crawl workflow launch", () => {
  const settings = {challenge_agent_model: "deepseek-flash", challenge_agent_max_runs: 3, api: "deepseek", model: "deepseek-flash",
    max_pages: 20, max_model_calls: 20, page_selection: "saved", force_refresh: false, max_in_flight: 3, refresh_interval_days: 30};
  beforeEach(() => {
    dagster.launchRun.mockReset().mockResolvedValue({runId: "workflow", status: "QUEUED"});
  });

  it.each([["full", "website_full_crawl"], ["jobs", "website_jobs_crawl"], ["site_info", "website_site_info"]])("launches the %s workflow with one task for input and results", async (type, prefix) => {
    const typed = type === "site_info" ? {...settings, max_pages: 1, page_selection: "basic_info"} : settings;
    const result = await launchSeDomainCrawlWorkflow({mode: "ids", domains: ["shared.se", "shared.se", "other.se"]}, type, typed, "operator@example.se");
    const [request, options] = dagster.launchRun.mock.calls[0];
    expect(options).toEqual({timeoutMs: 15_000});
    expect(request.job).toBe(`${prefix}_workflow`);
    const input = request.runConfig.ops[`${prefix}_requests`].config;
    expect(input).toMatchObject({source_relation: "corpscout.se_company_domain", ids: ["shared.se", "other.se"]});
    expect(input.task_id).toMatch(/^[0-9a-f-]{36}$/);
    // The results asset reads the task from the run tag, like Brave.
    expect(request.runConfig.ops[`${prefix}_results`].config).toEqual(typed);
    expect(request.tags).toMatchObject({"processing/task_id": input.task_id, "corpscout/requested_by": "operator@example.se",
      "corpscout/trigger_source": "backoffice", "crawl/type": type});
    expect(result).toMatchObject({ok: true, runId: "workflow", taskId: input.task_id, requestId: input.task_id});
  });

  it("keeps a query selection as filters and exclusions", async () => {
    await launchSeDomainCrawlWorkflow({mode: "query", query: {...EMPTY_SE_DOMAINS_FILTERS, source: "brave"}, excludedDomains: ["skip.se"]}, "full", settings, "backoffice");
    const input = dagster.launchRun.mock.calls[0][0].runConfig.ops.website_full_crawl_requests.config;
    expect(input).toMatchObject({select_all: true, excluded_ids: ["skip.se"], se_domain_filters: {source: "brave"}});
  });

  it.each([
    [{...settings, api: "other"}],
    [{...settings, page_selection: "basic_info"}],
    [{...settings, page_selection: "instructions"}],
    [{...settings, max_pages: 0}],
    [null],
  ])("rejects invalid settings before launching: %j", async (bad) => {
    await expect(launchSeDomainCrawlWorkflow({mode: "ids", domains: ["valid.se"]}, "full", bad, "backoffice")).rejects.toThrow();
    expect(dagster.launchRun).not.toHaveBeenCalled();
  });

  it("rejects an empty selection before launching", async () => {
    await expect(launchSeDomainCrawlWorkflow({mode: "ids", domains: []}, "full", settings, "backoffice")).rejects.toThrow();
    expect(dagster.launchRun).not.toHaveBeenCalled();
  });
});
