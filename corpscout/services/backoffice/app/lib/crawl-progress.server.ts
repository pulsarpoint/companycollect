import { chQuery } from "~/lib/clickhouse.server";
import { assetMaterializations, dagsterRunUrl, listRuns } from "~/lib/dagster.server";
import type { CrawlProgressSnapshot } from "~/lib/crawl-progress";
import type { DomainCrawlType } from "~/lib/se-domain-selection";

export async function loadCrawlProgress(type: DomainCrawlType): Promise<CrawlProgressSnapshot> {
  const asset = type === "site_info" ? "website_site_info_results" : `website_${type}_crawl_results`;
  const [runs, materializations] = await Promise.allSettled([
    listRuns({job: `${asset}_job`, limit: 10}, {timeoutMs: 8000}),
    assetMaterializations({asset, limit: 20}, {timeoutMs: 8000}),
  ]);
  if (runs.status === "rejected") throw runs.reason;
  if (runs.value.length === 0) return {runs: [], warning: null};
  // Count persisted responses while a run is active. A terminal materialization
  // also accounts for recovered submissions and inputs skipped for freshness.
  const [counts] = await Promise.allSettled([chQuery<{run_id: string; successful_count: number; unsuccessful_count: number}>(`
    SELECT run_id, toUInt32(countIf(successful)) AS successful_count,
      toUInt32(countIf(NOT successful)) AS unsuccessful_count
    FROM corpscout.${asset} FINAL
    WHERE run_id IN {runIds:Array(String)} GROUP BY run_id`, {runIds: runs.value.map(run => run.runId)})]);
  return {
    warning: counts.status === "rejected" || materializations.status === "rejected"
      ? "Some progress counts are unavailable. Run statuses are still shown." : null,
    runs: runs.value.map(run => {
      const metadata = materializations.status === "fulfilled" ? materializations.value.find(item => item.runId === run.runId)?.numbers : undefined;
      const saved = counts.status === "fulfilled" ? counts.value.find(item => item.run_id === run.runId) : undefined;
      const ops = run.runConfig.ops as Record<string, {config?: {domains?: unknown}}> | undefined;
      const domains = ops?.[asset]?.config?.domains;
      return {
        runId: run.runId, runUrl: dagsterRunUrl(run.runId), status: run.status,
        startTime: run.startTime, endTime: run.endTime,
        selected: Array.isArray(domains) && domains.length > 0 ? new Set(domains).size : null,
        successful: metadata?.completed ?? saved?.successful_count ?? (counts.status === "fulfilled" ? 0 : null),
        unsuccessful: metadata?.unsuccessful ?? saved?.unsuccessful_count ?? (counts.status === "fulfilled" ? 0 : null),
        skipped: metadata?.fresh_skipped ?? null,
      };
    }),
  };
}
