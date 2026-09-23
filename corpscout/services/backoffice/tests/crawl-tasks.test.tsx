import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

const db = vi.hoisted(() => ({chQuery: vi.fn()}));
const dagster = vi.hoisted(() => ({
  listRuns: vi.fn(), assetMaterializations: vi.fn(), launchRun: vi.fn(),
  dagsterRunUrl: vi.fn((id: string) => `http://dagster/runs/${id}`),
}));
vi.mock("~/lib/clickhouse.server", () => db);
vi.mock("~/lib/dagster.server", () => dagster);
import { loadCrawlProgress, resumeCrawlTask } from "~/lib/crawl-progress.server";
import { CrawlProgress } from "~/components/admin/crawl-progress";

const TASK = "57ff45ff-e78a-4225-9ae2-08876831bf7b";
const EXECUTION = "1562550f-3625-44ab-b03e-779df9a1adc8";
const RESULTS_CONFIG = {challenge_agent_model: "deepseek-flash", challenge_agent_max_runs: 3, api: "deepseek", model: "deepseek-flash",
  max_pages: 1, max_model_calls: 20, page_selection: "basic_info", force_refresh: false, max_in_flight: 3, refresh_interval_days: 30};
const execution = JSON.stringify({execution_id: EXECUTION, task_id: TASK, crawl_type: "site_info", total: 4});
const workflowRun = {runId: EXECUTION, status: "FAILURE", startTime: 100, endTime: 200,
  runConfig: {ops: {website_site_info_requests: {config: {task_id: TASK}}, website_site_info_results: {config: RESULTS_CONFIG}}},
  tags: {"processing/task_id": TASK, "website_crawl/execution": execution}};
const resumeRun = {runId: "resume-run", status: "FAILURE", startTime: 300, endTime: 400,
  runConfig: {ops: {website_site_info_results: {config: {...RESULTS_CONFIG, execution_id: EXECUTION}}}},
  tags: {"processing/task_id": TASK, "website_crawl/execution": execution}};

function listings(workflow: unknown[], results: unknown[]) {
  dagster.listRuns.mockImplementation(async ({job}: {job: string}) => job.endsWith("_workflow") ? workflow : results);
}

beforeEach(() => {
  vi.clearAllMocks();
  dagster.assetMaterializations.mockResolvedValue([{runId: "resume-run", numbers: {completed: 1, unsuccessful: 0, fresh_skipped: 1}}]);
  dagster.launchRun.mockResolvedValue({runId: "new-run", status: "QUEUED"});
  db.chQuery.mockImplementation(async (sql: string) => sql.includes("website_crawl_task_domains")
    ? [{task_id: TASK, selected: 4}]
    : [{run_id: EXECUTION, successful_count: 1, unsuccessful_count: 1}, {run_id: "resume-run", successful_count: 1, unsuccessful_count: 0}]);
});

describe("crawl task progress", () => {
  it("groups the workflow run and its resumes into one task with summed counts", async () => {
    listings([workflowRun], [resumeRun]);
    const snapshot = await loadCrawlProgress("site_info");
    expect(snapshot.tasks).toEqual([expect.objectContaining({
      taskId: TASK, executionId: EXECUTION, latestRunId: "resume-run", status: "FAILURE", runs: 2,
      selected: 4, successful: 2, unsuccessful: 1, skipped: 1, resumable: true, startTime: 100,
    })]);
    // Task resumes are not also listed as saved-input batches.
    expect(snapshot.runs).toEqual([]);
    expect(dagster.listRuns).toHaveBeenCalledWith({job: "website_site_info_workflow", limit: 10}, {timeoutMs: 8000});
  });

  it("renders task progress with a resume action for a failed task", async () => {
    listings([workflowRun], [resumeRun]);
    const snapshot = await loadCrawlProgress("site_info");
    const router = createMemoryRouter([{path: "/", element: <CrawlProgress snapshot={snapshot} type="site_info" />}]);
    const html = renderToStaticMarkup(<RouterProvider router={router} />);
    expect(html).toContain("Crawl tasks");
    expect(html).toContain(TASK.slice(0, 8));
    expect(html).toContain("4 processed / 4 selected");
    expect(html).toContain("Resume");
  });

  it("offers no resume while a run of the task is active", async () => {
    listings([{...workflowRun, status: "STARTED", endTime: null}], []);
    const snapshot = await loadCrawlProgress("site_info");
    expect(snapshot.tasks[0]).toMatchObject({status: "STARTED", resumable: false});
  });
});

describe("resuming a crawl task", () => {
  it("relaunches only the results asset with the original settings and execution id", async () => {
    listings([workflowRun], [resumeRun]);
    const result = await resumeCrawlTask("site_info", EXECUTION);
    expect(dagster.launchRun).toHaveBeenCalledExactlyOnceWith({
      job: "website_site_info_results_job",
      runConfig: {ops: {website_site_info_results: {config: {...RESULTS_CONFIG, execution_id: EXECUTION}}}},
      tags: {"processing/task_id": TASK, "crawl/type": "site_info", "backoffice/action": "resume-crawl-task"},
    }, {timeoutMs: 15_000});
    expect(result).toMatchObject({runId: "new-run", taskId: TASK, executionId: EXECUTION});
  });

  it("refuses to resume an active task, an unknown execution, or an invalid id", async () => {
    listings([{...workflowRun, status: "STARTED"}], []);
    await expect(resumeCrawlTask("site_info", EXECUTION)).rejects.toThrow("still running");
    listings([], []);
    await expect(resumeCrawlTask("site_info", EXECUTION)).rejects.toThrow("not found");
    await expect(resumeCrawlTask("site_info", "not-a-uuid")).rejects.toThrow();
    await expect(resumeCrawlTask("other", EXECUTION)).rejects.toThrow();
    expect(dagster.launchRun).not.toHaveBeenCalled();
  });
});
