import { ACTIVE_COMPANY_RUN_STATUSES, COMPANY_ACTION_AREAS } from "~/lib/company-actions";
import { dagsterRunUrl, processingJobRuns, type DagsterOptions } from "~/lib/dagster.server";
import type { ProcessingRun, ProcessingSnapshot } from "~/lib/company-processing";

export async function loadCompanyProcessing(options: DagsterOptions = {}): Promise<ProcessingSnapshot> {
  const jobs = COMPANY_ACTION_AREAS.flatMap((area) => (["sync", "process"] as const).map((operation) => ({ area: area.value, operation, job: area.jobs[operation] })));
  const results = await Promise.allSettled(jobs.map(({ job }) => processingJobRuns(job, ACTIVE_COMPANY_RUN_STATUSES, options)));
  const runs = new Map<string, ProcessingRun>();
  const lastSuccess: ProcessingSnapshot["lastSuccess"] = {};
  const errors: ProcessingSnapshot["errors"] = {};
  results.forEach((result, index) => {
    const { area, operation, job } = jobs[index];
    if (result.status === "rejected") {
      errors[area] = result.reason instanceof Error ? result.reason.message : "Dagster did not return run status.";
      return;
    }
    const project = (run: typeof result.value.recent[number]): ProcessingRun => ({
      runId: run.runId, area, operation, status: run.status, createdAt: run.creationTime,
      startTime: run.startTime, endTime: run.endTime, runUrl: dagsterRunUrl(run.runId, options.url), activeSteps: [],
      operator: run.tags.find((tag) => tag.key === "corpscout/requested_by")?.value ?? "Dagster",
    });
    for (const run of result.value.recent) runs.set(run.runId, project(run));
    for (const run of result.value.active) runs.set(run.runId, {
      ...project(run), activeSteps: run.stepStats.filter((step) => step.status === "IN_PROGRESS").map((step) => step.stepKey),
    });
    const success = result.value.successful[0];
    if (success) lastSuccess[job] = project(success);
  });
  return { runs: [...runs.values()].sort((a, b) => b.createdAt - a.createdAt), lastSuccess, errors, checkedAt: Date.now() / 1000 };
}
