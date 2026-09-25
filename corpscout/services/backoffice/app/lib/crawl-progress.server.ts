import { chQuery } from "~/lib/clickhouse.server";
import { assetMaterializations, dagsterRunUrl, listRuns, type DagsterRun } from "~/lib/dagster.server";
import type { CrawlProgressSnapshot } from "~/lib/crawl-progress";
import type { DomainCrawlType } from "~/lib/se-domain-selection";

const TASK_TAG = "processing/task_id";
const tags = (run: DagsterRun): Record<string, string> => run.tags ?? {};

function assetName(type: DomainCrawlType) {
  return `${type === "site_info" ? "website_site_info" : `website_${type}_crawl`}_results`;
}

/** Saved-input batches of one crawl type. Draft-task runs (tagged with a task) live on the Queues page. */
export async function loadCrawlProgress(type: DomainCrawlType): Promise<CrawlProgressSnapshot> {
  const asset = assetName(type);
  const [runs, materializations] = await Promise.allSettled([
    listRuns({job: `${asset}_job`, limit: 10}, {timeoutMs: 8000}),
    assetMaterializations({asset, limit: 20}, {timeoutMs: 8000}),
  ]);
  if (runs.status === "rejected") throw runs.reason;
  const batches = runs.value.filter((run) => !tags(run)[TASK_TAG]);
  if (batches.length === 0) return {runs: [], warning: null};
  // Count persisted responses while a run is active. A terminal materialization
  // also accounts for recovered submissions and inputs skipped for freshness.
  let counts: {run_id: string; successful_count: number; unsuccessful_count: number}[] | null;
  try {
    counts = await chQuery<{run_id: string; successful_count: number; unsuccessful_count: number}>(`
    SELECT run_id, toUInt32(countIf(successful)) AS successful_count,
      toUInt32(countIf(NOT successful)) AS unsuccessful_count
    FROM corpscout.${asset} FINAL
    WHERE run_id IN {runIds:Array(String)} GROUP BY run_id`, {runIds: batches.map((run) => run.runId)});
  } catch { counts = null; }
  const saved = (runId: string) => counts?.find((item) => item.run_id === runId);
  const metadata = (runId: string) => materializations.status === "fulfilled" ? materializations.value.find((item) => item.runId === runId)?.numbers : undefined;
  return {
    warning: counts === null || materializations.status === "rejected"
      ? "Some progress counts are unavailable. Run statuses are still shown." : null,
    runs: batches.map(run => {
      const numbers = metadata(run.runId);
      const persisted = saved(run.runId);
      const ops = run.runConfig.ops as Record<string, {config?: {domains?: unknown}}> | undefined;
      const domains = ops?.[asset]?.config?.domains;
      return {
        runId: run.runId, runUrl: dagsterRunUrl(run.runId), status: run.status,
        startTime: run.startTime, endTime: run.endTime,
        selected: Array.isArray(domains) && domains.length > 0 ? new Set(domains).size : null,
        successful: numbers?.completed ?? persisted?.successful_count ?? (counts !== null ? 0 : null),
        unsuccessful: numbers?.unsuccessful ?? persisted?.unsuccessful_count ?? (counts !== null ? 0 : null),
        skipped: numbers?.fresh_skipped ?? null,
      };
    }),
  };
}
