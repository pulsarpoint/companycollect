import {beforeEach, expect, it, vi} from "vitest";
import {loadQueueSourcePage, loadQueueSourceSummaries} from "~/lib/queue-history.server";
import {chQuery} from "~/lib/clickhouse.server";
import {listRuns} from "~/lib/dagster.server";
import {loader} from "~/routes/admin-queue-sources";
vi.mock("~/lib/clickhouse.server", () => ({chQuery: vi.fn()}));
vi.mock("~/lib/dagster.server", () => ({listRuns: vi.fn()}));
const task = "11111111-1111-4111-8111-111111111111";
const refs = [{taskId: task, crawlType: "site_info" as const, executionId: "original-execution"}];
beforeEach(() => {vi.resetAllMocks(); vi.mocked(chQuery).mockResolvedValue([]); vi.mocked(listRuns).mockResolvedValue([]);});
it("joins crawler results by the original execution, not the latest retry run", async () => {
  await loadQueueSourceSummaries("crawler", refs);
  const [sql, params] = vi.mocked(chQuery).mock.calls[0];
  expect(params).toEqual({tasks: [task], kinds: ["site_info"], executions: ["original-execution"]});
  expect(sql).toContain("r.run_id = refs.execution");
  expect(sql).toContain("website_site_info_results");
  expect(sql).not.toContain("website_full_crawl_results");
  expect(sql).toContain("GROUP BY task_type, task_id, domain, website_url");
  expect(sql).not.toContain(task);
});
it("pages deterministically after deduplicating URLs and provenance", async () => {
  await loadQueueSourcePage("webtech", [{...refs[0], crawlType: null}], 3);
  expect(chQuery).toHaveBeenCalledWith(expect.stringContaining("ORDER BY domain, website_url"), expect.objectContaining({offset: 100}));
  expect(vi.mocked(chQuery).mock.calls[0][0]).toContain("queue_task_sources");
  expect(vi.mocked(chQuery).mock.calls[0][0]).toContain("webtech_domain_scan_results");
});
it("does not query storage without task references", async () => {
  expect(await loadQueueSourceSummaries("crawler", [])).toEqual([]);
  expect(chQuery).not.toHaveBeenCalled();
});
it("validates the source request before calling services", async () => {
  await expect(loader({params: {type: "webtech"}, request: new Request("http://x?task=invalid")})).rejects.toMatchObject({status: 400});
  expect(listRuns).not.toHaveBeenCalled();
});
it("loads the chosen task independently of the current queue selection", async () => {
  vi.mocked(listRuns).mockResolvedValue([{runId: "retry", tags: {"crawler/execution_id": "original"}}] as never);
  const result = await loader({params: {type: "crawler"}, request: new Request(`http://x?task=${task}&crawlType=jobs&page=2`)});
  expect(listRuns).toHaveBeenCalledWith({job: "website_jobs_crawl_results_job", limit: 100, tags: {"processing/task_id": task}});
  expect(chQuery).toHaveBeenCalledWith(expect.any(String), {tasks: [task], kinds: ["jobs"], executions: ["original"], offset: 50});
  expect(result).toEqual({rows: [], page: 2, error: null});
});
