import { chQuery } from "~/lib/clickhouse.server";
import { assetMaterializations, dagsterRunUrl, launchRun, listRuns, type DagsterRun } from "~/lib/dagster.server";
import type { CrawlProgressSnapshot, CrawlTaskProgress } from "~/lib/crawl-progress";
import { DOMAIN_CRAWL_TYPES, type DomainCrawlType } from "~/lib/se-domain-selection";

const TASK_TAG = "processing/task_id";
const EXECUTION_TAG = "website_crawl/execution";
const ACTIVE = new Set(["QUEUED", "NOT_STARTED", "MANAGED", "STARTING", "STARTED", "CANCELING"]);
const tags = (run: DagsterRun): Record<string, string> => run.tags ?? {};
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function names(type: DomainCrawlType) {
  const prefix = type === "site_info" ? "website_site_info" : `website_${type}_crawl`;
  return {asset: `${prefix}_results`, workflow: `${prefix}_workflow`};
}

/** Execution a run belongs to: the original run ID saved in its execution tag. */
function executionOf(run: DagsterRun): string {
  try {
    const saved = JSON.parse(tags(run)[EXECUTION_TAG] ?? "null") as {execution_id?: unknown} | null;
    if (typeof saved?.execution_id === "string") return saved.execution_id;
  } catch { /* an unparsable tag falls back to the run itself */ }
  return run.runId;
}

/** Runs of every task execution on this page, newest first, grouped by execution. */
async function taskRuns(type: DomainCrawlType, results: DagsterRun[]) {
  const workflow = await listRuns({job: names(type).workflow, limit: 10}, {timeoutMs: 8000});
  const executions = new Map<string, DagsterRun[]>();
  for (const run of [...workflow, ...results]) {
    if (!tags(run)[TASK_TAG]) continue;
    const id = executionOf(run);
    executions.set(id, [...(executions.get(id) ?? []), run]);
  }
  for (const runs of executions.values()) runs.sort((a, b) => (b.startTime ?? Infinity) - (a.startTime ?? Infinity));
  return executions;
}

export async function loadCrawlProgress(type: DomainCrawlType): Promise<CrawlProgressSnapshot> {
  const {asset} = names(type);
  const [runs, materializations] = await Promise.allSettled([
    listRuns({job: `${asset}_job`, limit: 10}, {timeoutMs: 8000}),
    assetMaterializations({asset, limit: 20}, {timeoutMs: 8000}),
  ]);
  if (runs.status === "rejected") throw runs.reason;
  const [executionsResult] = await Promise.allSettled([taskRuns(type, runs.value)]);
  const executions = executionsResult.status === "fulfilled" ? executionsResult.value : new Map<string, DagsterRun[]>();
  // Task resumes are shown under their task, not as saved-input batches.
  const batches = runs.value.filter((run) => !tags(run)[TASK_TAG]);
  const allRuns = [...batches, ...[...executions.values()].flat()];
  if (allRuns.length === 0) {
    return {runs: [], tasks: [], warning: executionsResult.status === "rejected" ? "Crawl tasks are unavailable. Batches are still shown." : null};
  }
  const taskIds = [...new Set([...executions.values()].map((group) => tags(group[0])[TASK_TAG]))];
  // Count persisted responses while a run is active. A terminal materialization
  // also accounts for recovered submissions and inputs skipped for freshness.
  const [counts, sizes] = await Promise.allSettled([
    chQuery<{run_id: string; successful_count: number; unsuccessful_count: number}>(`
    SELECT run_id, toUInt32(countIf(successful)) AS successful_count,
      toUInt32(countIf(NOT successful)) AS unsuccessful_count
    FROM corpscout.${asset} FINAL
    WHERE run_id IN {runIds:Array(String)} GROUP BY run_id`, {runIds: allRuns.map((run) => run.runId)}),
    taskIds.length === 0 ? Promise.resolve([]) : chQuery<{task_id: string; selected: number}>(`
    SELECT task_id, toUInt32(count()) AS selected FROM corpscout.website_crawl_task_domains FINAL
    WHERE task_id IN {taskIds:Array(String)} AND crawl_type = {type:String} GROUP BY task_id`, {taskIds, type}),
  ]);
  const saved = (runId: string) => counts.status === "fulfilled" ? counts.value.find((item) => item.run_id === runId) : undefined;
  const metadata = (runId: string) => materializations.status === "fulfilled" ? materializations.value.find((item) => item.runId === runId)?.numbers : undefined;
  const tasks: CrawlTaskProgress[] = [...executions.entries()].map(([executionId, group]) => {
    const latest = group[0];
    const taskId = tags(latest)[TASK_TAG];
    const known = counts.status === "fulfilled";
    // Every run re-checks freshness for the same domains: take the latest finished count.
    const skipped = group.map((run) => metadata(run.runId)?.fresh_skipped).find((value) => value !== undefined) ?? null;
    const starts = group.map((run) => run.startTime).filter((time): time is number => time !== null);
    return {
      taskId, executionId, latestRunId: latest.runId, runUrl: dagsterRunUrl(latest.runId), status: latest.status,
      runs: group.length, startTime: starts.length ? Math.min(...starts) : null,
      selected: sizes.status === "fulfilled" ? sizes.value.find((item) => item.task_id === taskId)?.selected ?? null : null,
      successful: known ? group.reduce((sum, run) => sum + (saved(run.runId)?.successful_count ?? 0), 0) : null,
      unsuccessful: known ? group.reduce((sum, run) => sum + (saved(run.runId)?.unsuccessful_count ?? 0), 0) : null,
      skipped,
      resumable: (latest.status === "FAILURE" || latest.status === "CANCELED") && EXECUTION_TAG in tags(latest),
    };
  }).sort((a, b) => (b.startTime ?? Infinity) - (a.startTime ?? Infinity));
  return {
    warning: counts.status === "rejected" || materializations.status === "rejected" || sizes.status === "rejected" || executionsResult.status === "rejected"
      ? "Some progress counts are unavailable. Run statuses are still shown." : null,
    tasks,
    runs: batches.map(run => {
      const numbers = metadata(run.runId);
      const persisted = saved(run.runId);
      const ops = run.runConfig.ops as Record<string, {config?: {domains?: unknown}}> | undefined;
      const domains = ops?.[asset]?.config?.domains;
      return {
        runId: run.runId, runUrl: dagsterRunUrl(run.runId), status: run.status,
        startTime: run.startTime, endTime: run.endTime,
        selected: Array.isArray(domains) && domains.length > 0 ? new Set(domains).size : null,
        successful: numbers?.completed ?? persisted?.successful_count ?? (counts.status === "fulfilled" ? 0 : null),
        unsuccessful: numbers?.unsuccessful ?? persisted?.unsuccessful_count ?? (counts.status === "fulfilled" ? 0 : null),
        skipped: numbers?.fresh_skipped ?? null,
      };
    }),
  };
}

/**
 * Resume a failed or cancelled crawl task: relaunch only the results asset with
 * the execution's original settings plus execution_id. Dagster keeps the task,
 * content settings and freshness cutoff fixed and never resubmits a domain.
 */
export async function resumeCrawlTask(type: unknown, executionId: unknown) {
  if (!DOMAIN_CRAWL_TYPES.some((item) => item.value === type)) throw new Error("Choose a valid crawl type.");
  if (typeof executionId !== "string" || !UUID.test(executionId)) throw new Error("Invalid crawl task execution.");
  const crawlType = type as DomainCrawlType;
  const {asset} = names(crawlType);
  const results = await listRuns({job: `${asset}_job`, limit: 10}, {timeoutMs: 8000});
  const group = (await taskRuns(crawlType, results)).get(executionId);
  if (!group) throw new Error("Crawl task execution not found. Refresh the list.");
  if (group.some((run) => ACTIVE.has(run.status))) throw new Error("This crawl task is still running.");
  const original = group.find((run) => (run.runConfig.ops as Record<string, unknown> | undefined)?.[asset]);
  const config = (original?.runConfig.ops as Record<string, {config?: Record<string, unknown>}> | undefined)?.[asset]?.config;
  if (!config) throw new Error("The crawl task's original settings are unavailable.");
  const taskId = tags(group[0])[TASK_TAG];
  const run = await launchRun({
    job: `${asset}_job`,
    runConfig: {ops: {[asset]: {config: {...config, execution_id: executionId}}}},
    tags: {[TASK_TAG]: taskId, "crawl/type": crawlType, "backoffice/action": "resume-crawl-task"},
  }, {timeoutMs: 15_000});
  return {...run, runUrl: dagsterRunUrl(run.runId), taskId, executionId};
}
