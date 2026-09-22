import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, expect, it, vi } from "vitest";
const db = vi.hoisted(() => ({chQuery: vi.fn()}));
const dagster = vi.hoisted(() => ({listRuns: vi.fn(), assetMaterializations: vi.fn(), dagsterRunUrl: vi.fn((id: string) => `http://dagster/runs/${id}`)}));
vi.mock("~/lib/clickhouse.server", () => db);
vi.mock("~/lib/dagster.server", () => dagster);
import { loadCrawlProgress } from "~/lib/crawl-progress.server";
import { CrawlProgress } from "~/components/admin/crawl-progress";

const run = {runId: "running-one", status: "STARTED", startTime: 1234, endTime: null,
  runConfig: {ops: {website_site_info_results: {config: {domains: ["one.example", "two.example", "three.example"]}}}}};
beforeEach(() => {
  vi.clearAllMocks(); dagster.listRuns.mockResolvedValue([run]); dagster.assetMaterializations.mockResolvedValue([]);
  db.chQuery.mockResolvedValue([{run_id: "running-one", successful_count: 1, unsuccessful_count: 1}]);
});
it("uses persisted successes and failures for an active batch without inventing skipped counts", async () => {
  const snapshot = await loadCrawlProgress("site_info");
  expect(snapshot.runs[0]).toMatchObject({status: "STARTED", selected: 3, successful: 1, unsuccessful: 1, skipped: null});
  expect(db.chQuery.mock.calls[0][0]).toContain("website_site_info_results FINAL");
  expect(db.chQuery.mock.calls[0][1]).toEqual({runIds: ["running-one"]});
  const html = renderToStaticMarkup(<CrawlProgress snapshot={snapshot} />);
  expect(html).toContain("Running"); expect(html).toContain("2 processed / 3 selected");
});
it("counts fresh skips on a finished run even when no crawler response was needed", async () => {
  dagster.listRuns.mockResolvedValue([{...run, status: "SUCCESS"}]);
  dagster.assetMaterializations.mockResolvedValue([{runId: run.runId, numbers: {completed: 0, unsuccessful: 0, fresh_skipped: 3}}]);
  const snapshot = await loadCrawlProgress("site_info");
  expect(snapshot.runs[0]).toMatchObject({successful: 0, unsuccessful: 0, skipped: 3});
  const html = renderToStaticMarkup(<CrawlProgress snapshot={snapshot} />);
  expect(html).toContain("Finished"); expect(html).toContain("3 processed / 3 selected");
});
it("does not mark a failed run as fully processed", async () => {
  dagster.listRuns.mockResolvedValue([{...run, status: "FAILURE"}]);
  const html = renderToStaticMarkup(<CrawlProgress snapshot={await loadCrawlProgress("site_info")} />);
  expect(html).toContain("Failed"); expect(html).toContain("2 processed / 3 selected");
});
it("keeps Dagster status visible with unavailable counts instead of displaying zeros", async () => {
  db.chQuery.mockRejectedValue(new Error("CH unavailable"));
  const snapshot = await loadCrawlProgress("site_info");
  expect(snapshot.warning).toContain("unavailable"); expect(snapshot.runs[0].successful).toBeNull();
  expect(renderToStaticMarkup(<CrawlProgress snapshot={snapshot} />)).toContain("Counts unavailable");
});
it("does not claim a denominator when a Dagster run selects inputs dynamically", async () => {
  dagster.listRuns.mockResolvedValue([{...run, runConfig: {}}]);
  const snapshot = await loadCrawlProgress("jobs");
  expect(snapshot.runs[0].selected).toBeNull();
  expect(dagster.listRuns).toHaveBeenCalledWith({job: "website_jobs_crawl_results_job", limit: 10}, {timeoutMs: 8000});
});
it("renders an empty state without querying result counts", async () => {
  dagster.listRuns.mockResolvedValue([]);
  const snapshot = await loadCrawlProgress("full");
  expect(db.chQuery).not.toHaveBeenCalled();
  expect(renderToStaticMarkup(<CrawlProgress snapshot={snapshot} />)).toContain("No batches yet");
});
it("reports a status failure instead of presenting it as an empty history", async () => {
  dagster.listRuns.mockRejectedValue(new Error("Dagster unavailable"));
  await expect(loadCrawlProgress("site_info")).rejects.toThrow("Dagster unavailable");
});
