import { chQuery } from "~/lib/clickhouse.server";
import { assetMaterializations, dagsterRunUrl, listRuns, type DagsterRun } from "~/lib/dagster.server";
import type { CrawlProgressSnapshot } from "~/lib/crawl-progress";
import type { DomainCrawlType } from "~/lib/se-domain-selection";

function assetName(type: DomainCrawlType) {
  return `${type === "site_info" ? "website_site_info" : `website_${type}_crawl`}_results`;
}

function execution(run: DagsterRun, asset: string) {
  const ops = run.runConfig.ops as Record<string, {config?: {domains?: string[]; task_id?: string; execution_id?: string}}> | undefined;
  const config = ops?.[asset]?.config;
  const taskId = run.tags?.["processing/task_id"] ?? config?.task_id ?? null;
  return {
    taskId,
    domains: config?.domains,
    // Queue retries store results under the original execution, not the retry's run ID.
    resultRunId: taskId ? run.tags?.["crawler/execution_id"] ?? config?.execution_id ?? run.tags?.["dagster/root_run_id"] ?? run.runId : run.runId,
  };
}

/** Recent saved-input and queue batches of one crawl type. */
export async function loadCrawlProgress(type: DomainCrawlType): Promise<CrawlProgressSnapshot> {
  const asset = assetName(type);
  const [runs, materializations] = await Promise.allSettled([
    listRuns({job: `${asset}_job`, limit: 10}, {timeoutMs: 8000}),
    assetMaterializations({asset, limit: 20}, {timeoutMs: 8000}),
  ]);
  if (runs.status === "rejected") throw runs.reason;
  const batches = runs.value;
  if (batches.length === 0) return {runs: [], warning: null};
  // Count persisted responses while a run is active. A terminal materialization
  // also accounts for recovered submissions and inputs skipped for freshness.
  let counts: {run_id: string; successful_count: number; unsuccessful_count: number}[] | null;
  try {
    counts = await chQuery<{run_id: string; successful_count: number; unsuccessful_count: number}>(`
    SELECT run_id, toUInt32(countIf(ok)) AS successful_count,
      toUInt32(countIf(NOT ok)) AS unsuccessful_count
    FROM (
      SELECT run_id, request_id, argMax(successful, tuple(finished_at, attempt)) AS ok
      FROM corpscout.${asset} FINAL
      WHERE run_id IN {runIds:Array(String)} GROUP BY run_id, request_id
    ) GROUP BY run_id`, {runIds: [...new Set(batches.map((run) => execution(run, asset).resultRunId))]});
  } catch { counts = null; }
  const saved = (runId: string) => counts?.find((item) => item.run_id === runId);
  const metadata = (runId: string) => materializations.status === "fulfilled" ? materializations.value.find((item) => item.runId === runId)?.numbers : undefined;
  return {
    warning: counts === null || materializations.status === "rejected"
      ? "Some progress counts are unavailable. Run statuses are still shown." : null,
    runs: batches.map(run => {
      const numbers = metadata(run.runId);
      const {domains, taskId, resultRunId} = execution(run, asset);
      const persisted = saved(resultRunId);
      const successful = numbers?.succeeded_pages ?? numbers?.completed ?? persisted?.successful_count ?? (counts !== null ? 0 : null);
      const unsuccessful = numbers?.failed_pages ?? numbers?.unsuccessful ?? persisted?.unsuccessful_count ?? (counts !== null ? 0 : null);
      const skipped = numbers?.skipped_recent ?? numbers?.fresh_skipped ?? null;
      const selected = numbers?.selected_domains ?? (Array.isArray(domains) && domains.length > 0 ? new Set(domains).size
        : numbers?.succeeded_pages !== undefined && numbers.failed_pages !== undefined && numbers.skipped_recent !== undefined
          ? numbers.succeeded_pages + numbers.failed_pages + numbers.skipped_recent : null);
      // Older processors double-counted trailing fresh inputs. Do not turn that
      // historical metadata into a made-up percentage or silently clamp it.
      const countsWarning = selected !== null && successful !== null && unsuccessful !== null && skipped !== null
        && successful + unsuccessful + skipped > selected
        ? "Recorded counts exceed the selection; historical progress is unavailable." : null;
      return {
        runId: run.runId, runUrl: dagsterRunUrl(run.runId), status: run.status,
        startTime: run.startTime, endTime: run.endTime,
        taskId, selected, successful, unsuccessful, skipped, countsWarning,
      };
    }),
  };
}
