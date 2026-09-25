import {beforeEach, expect, it, vi} from "vitest";
const server = vi.hoisted(() => ({loadCrawlQueueCounts: vi.fn(), loadQueueInputs: vi.fn(), loadQueueRuns: vi.fn(), loadQueueHistory: vi.fn(), startQueueProcessing: vi.fn(), QueueRequestError: class extends Error {}}));
vi.mock("~/lib/queues.server", () => server);
import {loader} from "~/routes/admin-queue";
const task = "11111111-1111-4111-8111-111111111111";
beforeEach(() => {
  vi.clearAllMocks();
  server.loadQueueInputs.mockResolvedValue({tasks: [], selectedTotal: 0, rows: [], totalInputs: 0, totalTasks: 0});
  server.loadQueueRuns.mockResolvedValue({runs: [], active: false});
  server.loadQueueHistory.mockResolvedValue([{taskId: task, status: "SUCCESS"}]);
  server.loadCrawlQueueCounts.mockResolvedValue({full: 0, jobs: 3, site_info: 1});
});
const load = (search = "") => loader({params: {type: "webtech"}, request: new Request(`http://x/admin/queues/webtech${search}`)} as never);
it("automatically selects the current nonempty queue", async () => {
  server.loadQueueInputs.mockResolvedValue({tasks: [{task_id: task}], selectedTotal: 2});
  await expect(load()).rejects.toMatchObject({status: 302, headers: expect.any(Headers)});
  try { await load(); } catch (response) { expect((response as Response).headers.get("Location")).toBe(`/admin/queues/webtech?task=${task}`); }
});
it("removes an obsolete completed task selection and closes its settings sheet", async () => {
  try { await load(`?task=${task}&configure=1`); throw new Error("expected redirect"); }
  catch (response) { expect((response as Response).headers.get("Location")).toBe("/admin/queues/webtech"); }
});
it("shows history even when the queue is empty", async () => {
  const result = await load();
  expect(result.inputs.rows).toEqual([]);
  expect(result.history).toEqual([{taskId: task, status: "SUCCESS"}]);
});
it("keeps input browsing available when Dagster history is unavailable", async () => {
  server.loadQueueHistory.mockRejectedValue(new Error("offline"));
  const result = await load();
  expect(result.historyError).toContain("unavailable");
  expect(result.inputs.rows).toEqual([]);
});

it("selects the current crawler draft within the chosen crawl type", async () => {
  server.loadQueueInputs.mockResolvedValue({tasks: [{task_id: task}], selectedTotal: 2});
  try { await loader({params: {type: "crawler"}, request: new Request("http://x/admin/queues/crawler?crawlType=jobs")} as never); throw new Error("expected redirect"); }
  catch (response) { expect((response as Response).headers.get("Location")).toBe(`/admin/queues/crawler?crawlType=jobs&task=${task}`); }
});

it("gives the crawler tabs a count per queue and tolerates a failed count", async () => {
  const crawler = () => loader({params: {type: "crawler"}, request: new Request("http://x/admin/queues/crawler?crawlType=full")} as never);
  expect((await crawler()).crawlCounts).toEqual({full: 0, jobs: 3, site_info: 1});
  server.loadCrawlQueueCounts.mockRejectedValue(new Error("offline"));
  expect((await crawler()).crawlCounts).toBeNull();
  expect(server.loadCrawlQueueCounts).toHaveBeenCalledTimes(2);
});

it("does not count crawler queues for other queue types", async () => {
  expect((await load()).crawlCounts).toBeNull();
  expect(server.loadCrawlQueueCounts).not.toHaveBeenCalled();
});
