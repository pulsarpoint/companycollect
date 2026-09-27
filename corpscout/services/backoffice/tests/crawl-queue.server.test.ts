import {beforeEach, expect, it, vi} from "vitest";
const dagster = vi.hoisted(() => ({launchRun: vi.fn(), listRuns: vi.fn(), runStatus: vi.fn(), dagsterRunUrl: vi.fn((id: string) => `http://dagster/runs/${id}`)}));
vi.mock("~/lib/dagster.server", () => dagster);
import {addDomainsToCrawlQueue, crawlQueueSubmission} from "~/lib/crawl-queue.server";
import {EMPTY_SE_DOMAINS_FILTERS} from "~/lib/se-domains-filters";
import {parseWorkspaceDomainFilters} from "~/lib/workspace-domains";
const submission = "11111111-1111-4111-8111-111111111111";
beforeEach(() => { vi.clearAllMocks(); dagster.listRuns.mockResolvedValue([]); dagster.launchRun.mockResolvedValue({runId: "new", status: "QUEUED"}); });
it.each(["full", "jobs", "site_info"])("adds %s to a draft via the input asset only", async crawlType => {
  await addDomainsToCrawlQueue("sweden", {mode: "ids", domains: ["one.se"]}, crawlType, submission, "operator");
  expect(dagster.launchRun).toHaveBeenCalledWith(expect.objectContaining({job: "website_crawl_input_job", assetSelection: ["website_crawl_input"],
    runConfig: {ops: {website_crawl_input: {config: expect.objectContaining({crawl_type: crawlType, queue_scope: "workspace", submission_id: submission,
      source_relation: "corpscout.se_company_domain", ids: ["one.se"]})}}}}));
  expect(dagster.launchRun.mock.calls[0][0].runConfig.ops.website_crawl_input.config).not.toHaveProperty("task_id");
});
it("preserves all matching filters and exclusions", async () => {
  await addDomainsToCrawlQueue("sweden", {mode: "query", query: {...EMPTY_SE_DOMAINS_FILTERS, source: "brave", status: "inactive"}, excludedDomains: ["skip.se"]}, "full", submission, "operator");
  expect(dagster.launchRun.mock.calls[0][0].runConfig.ops.website_crawl_input.config).toMatchObject({select_all: true, excluded_ids: ["skip.se"], se_domain_filters: {source: "brave", status: "inactive"}});
});
it("recovers an acknowledged submission and rejects changed selection", async () => {
  const selection = {mode: "ids", domains: ["one.se"]};
  await addDomainsToCrawlQueue("sweden", selection, "full", submission, "operator");
  const tags = dagster.launchRun.mock.calls[0][0].tags;
  dagster.listRuns.mockResolvedValue([{runId: "new", status: "SUCCESS", tags}]);
  await addDomainsToCrawlQueue("sweden", selection, "full", submission, "operator");
  expect(dagster.launchRun).toHaveBeenCalledTimes(1);
  await expect(addDomainsToCrawlQueue("sweden", selection, "jobs", submission, "operator")).rejects.toThrow("another selection");
});
it("returns the resolved draft only for crawler import runs", async () => {
  dagster.runStatus.mockResolvedValue({jobName: "website_crawl_input_job", status: "SUCCESS", tags: {"backoffice/action": "add-crawl-input", "processing/task_id": submission}});
  expect(await crawlQueueSubmission(submission)).toMatchObject({finished: true, taskId: submission});
  dagster.runStatus.mockResolvedValue({jobName: "wrong", tags: {}});
  await expect(crawlQueueSubmission(submission)).rejects.toThrow("not found");
});
it("imports every matching inventory domain through the same crawl draft job", async () => {
  const query = parseWorkspaceDomainFilters(new URLSearchParams("suffix=se&companies=without&companyMatching=without"));
  await addDomainsToCrawlQueue("inventory", { mode: "query", query, excludedDomains: ["skip.se"] }, "site_info", submission, "operator");
  expect(dagster.launchRun.mock.calls[0][0]).toMatchObject({ job: "website_crawl_input_job", assetSelection: ["website_crawl_input"],
    runConfig: { ops: { website_crawl_input: { config: { source_relation: "corpscout.domains_search", select_all: true, excluded_ids: ["skip.se"],
      workspace_domain_filters: { suffix: "se", companies: "without", company_matching: "without" } } } } } });
  const config = dagster.launchRun.mock.calls[0][0].runConfig.ops.website_crawl_input.config;
  expect(config).not.toHaveProperty("max_domains");
  expect(config).not.toHaveProperty("after");
  expect(config).not.toHaveProperty("se_domain_filters");
});
