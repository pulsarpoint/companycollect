import {beforeEach, expect, it, vi} from "vitest";
const dagster = vi.hoisted(() => ({launchRun: vi.fn(), listRuns: vi.fn(), runStatus: vi.fn(), dagsterRunUrl: vi.fn((id: string) => `http://dagster/runs/${id}`)}));
vi.mock("~/lib/dagster.server", () => dagster);
import {addSeDomainsToCrawlQueue, crawlQueueSubmission} from "~/lib/crawl-queue.server";
import {EMPTY_SE_DOMAINS_FILTERS} from "~/lib/se-domains-filters";
const submission = "11111111-1111-4111-8111-111111111111";
beforeEach(() => { vi.clearAllMocks(); dagster.listRuns.mockResolvedValue([]); dagster.launchRun.mockResolvedValue({runId: "new", status: "QUEUED"}); });
it.each(["full", "jobs", "site_info"])("adds %s to a draft via the input asset only", async crawlType => {
  await addSeDomainsToCrawlQueue({mode: "ids", domains: ["one.se"]}, crawlType, submission, "operator");
  expect(dagster.launchRun).toHaveBeenCalledWith(expect.objectContaining({job: "website_crawl_input_job", assetSelection: ["website_crawl_input"],
    runConfig: {ops: {website_crawl_input: {config: expect.objectContaining({crawl_type: crawlType, queue_scope: "workspace", submission_id: submission,
      source_relation: "corpscout.se_company_domain", ids: ["one.se"]})}}}}));
  expect(dagster.launchRun.mock.calls[0][0].runConfig.ops.website_crawl_input.config).not.toHaveProperty("task_id");
});
it("preserves all matching filters and exclusions", async () => {
  await addSeDomainsToCrawlQueue({mode: "query", query: {...EMPTY_SE_DOMAINS_FILTERS, source: "brave", status: "inactive"}, excludedDomains: ["skip.se"]}, "full", submission, "operator");
  expect(dagster.launchRun.mock.calls[0][0].runConfig.ops.website_crawl_input.config).toMatchObject({select_all: true, excluded_ids: ["skip.se"], se_domain_filters: {source: "brave", status: "inactive"}});
});
it("recovers an acknowledged submission and rejects changed selection", async () => {
  const selection = {mode: "ids", domains: ["one.se"]};
  await addSeDomainsToCrawlQueue(selection, "full", submission, "operator");
  const tags = dagster.launchRun.mock.calls[0][0].tags;
  dagster.listRuns.mockResolvedValue([{runId: "new", status: "SUCCESS", tags}]);
  await addSeDomainsToCrawlQueue(selection, "full", submission, "operator");
  expect(dagster.launchRun).toHaveBeenCalledTimes(1);
  await expect(addSeDomainsToCrawlQueue(selection, "jobs", submission, "operator")).rejects.toThrow("another selection");
});
it("returns the resolved draft only for crawler import runs", async () => {
  dagster.runStatus.mockResolvedValue({jobName: "website_crawl_input_job", status: "SUCCESS", tags: {"backoffice/action": "add-crawl-input", "processing/task_id": submission}});
  expect(await crawlQueueSubmission(submission)).toMatchObject({finished: true, taskId: submission});
  dagster.runStatus.mockResolvedValue({jobName: "wrong", tags: {}});
  await expect(crawlQueueSubmission(submission)).rejects.toThrow("not found");
});
