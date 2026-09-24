import { createHash } from "node:crypto";
import { chQuery } from "~/lib/clickhouse.server";
import { dagsterRunUrl, launchRun, listRuns } from "~/lib/dagster.server";
import { objectSettings, parseCrawlSettings } from "~/lib/crawl-settings.server";
import { assertWebtechAvailable } from "~/lib/webtech-maintenance.server";
import { ACTIVE_QUEUE_RUNS, QUEUE_NUMBER_LIMITS, QUEUE_PAGE_SIZE, QUEUE_UUID, type QueueFilters } from "~/lib/queues";

export class QueueRequestError extends Error {}

function queueDefinition(filters: QueueFilters) {
  switch (filters.type) {
    case "webtech": return {
      table: "corpscout.webtech_scan_input", asset: "webtech_scan_results", job: "webtech_scan_results_job",
      from: "corpscout.webtech_scan_input", where: "1", target: "root_domain", detail: "page_url",
      source: "source_name", record: "source_record_id", time: "toString(submitted_at)", id: "input_id",
    };
    case "brave": return {
      table: "corpscout.company_brave_search_input", asset: "company_brave_search_results", job: "company_brave_search_job",
      from: "corpscout.company_brave_search_input", where: "1", target: "company_name",
      detail: "concat(country_code, ':', company_id)", source: "country_code", record: "company_id", time: "''", id: "input_id",
    };
    case "ip-enrichment": return {
      table: "corpscout.ip_enrichment_input", asset: "ip_enrichment_results", job: "ip_enrichment_results_job",
      from: "corpscout.ip_enrichment_input", where: "1", target: "ip", detail: "concat('IPv', toString(ip_version))",
      source: "source_name", record: "source_record_id", time: "toString(submitted_at)", id: "input_id",
    };
    case "crawler": {
      const prefix = filters.crawlType === "site_info" ? "website_site_info" : `website_${filters.crawlType}_crawl`;
      return {
        table: "corpscout.website_crawl_task_domains", asset: `${prefix}_results`, job: `${prefix}_results_job`,
        from: "corpscout.website_crawl_task_domains FINAL", where: "crawl_type = {crawlType:String}",
        target: "domain", detail: "website_url", source: "source_name", record: "domain", time: "toString(created_at)", id: "domain",
      };
    }
  }
}

export interface QueueInput {
  task_id: string; input_id: string; target: string; detail: string; source: string; source_record_id: string; submitted_at: string;
}
export interface QueueTask { task_id: string; total: string; submitted_at: string }

export async function loadQueueInputs(filters: QueueFilters) {
  if (filters.type === "webtech") assertWebtechAvailable();
  const def = queueDefinition(filters);
  const params = { ...filters, limit: QUEUE_PAGE_SIZE, offset: (filters.page - 1) * QUEUE_PAGE_SIZE, taskOffset: (filters.taskPage - 1) * QUEUE_PAGE_SIZE };
  const taskWhere = `${def.where} ${filters.task ? "AND toString(task_id) = {task:String}" : ""}`;
  const matches = filters.search ? `(positionCaseInsensitiveUTF8(${def.target}, {search:String}) > 0
    OR positionCaseInsensitiveUTF8(${def.detail}, {search:String}) > 0)` : "1";
  const searchWhere = `${taskWhere} AND ${matches}`;
  const inputOrder = filters.type === "crawler" ? "task_id, input_id" : "input_id, task_id";
  const [tasks, overview, counts, rows] = await Promise.all([
    chQuery<QueueTask>(`SELECT toString(task_id) AS task_id, toString(count()) AS total, max(${def.time}) AS submitted_at
      FROM ${def.from} WHERE ${def.where} AND toString(task_id) != '' GROUP BY task_id
      ORDER BY submitted_at DESC, task_id LIMIT {limit:UInt32} OFFSET {taskOffset:UInt64}`, params),
    chQuery<{ total: string; tasks: string }>(`SELECT toString(count()) AS total, toString(uniqExactIf(task_id, toString(task_id) != '')) AS tasks FROM ${def.from} WHERE ${def.where}`, params),
    chQuery<{ total: string; matching: string }>(`SELECT toString(count()) AS total,
      toString(countIf(${matches})) AS matching
      FROM ${def.from} WHERE ${taskWhere}`, params),
    chQuery<QueueInput>(`SELECT toString(task_id) AS task_id, ${def.id} AS input_id, ${def.target} AS target,
      ${def.detail} AS detail, ${def.source} AS source, ${def.record} AS source_record_id, ${def.time} AS submitted_at
      FROM ${def.from} WHERE ${searchWhere} ORDER BY ${inputOrder}
      LIMIT {limit:UInt32} OFFSET {offset:UInt64}`, params),
  ]);
  return { table: def.table, asset: def.asset, tasks, rows,
    totalInputs: Number(overview[0]?.total ?? 0), totalTasks: Number(overview[0]?.tasks ?? 0),
    selectedTotal: Number(counts[0]?.total ?? 0), matching: Number(counts[0]?.matching ?? 0) };
}

export async function loadQueueRuns(task: string) {
  if (!task) return { runs: [], active: false };
  // Filter by task in Dagster itself: a busy unrelated job cannot hide this task.
  const [recent, active] = await Promise.all([
    listRuns({ limit: 10, tags: { "processing/task_id": task } }),
    listRuns({ limit: 1, tags: { "processing/task_id": task }, statuses: ACTIVE_QUEUE_RUNS }),
  ]);
  const runs = [...new Map([...active, ...recent].map(run => [run.runId, run])).values()];
  return { active: active.length > 0, runs: runs.map(run => ({
    runId: run.runId, status: run.status, job: run.jobName, runUrl: dagsterRunUrl(run.runId),
  })) };
}

/** Run history survives removal of completed ClickHouse input rows. */
export async function loadQueueHistory(filters: QueueFilters) {
  const runs = await listRuns({job: queueDefinition(filters).job, limit: 50});
  const latest = new Map<string, {taskId: string; status: string; runUrl: string | null; startedAt: string | null; outcome: string | null; failedPages: number | null}>();
  for (const run of runs) {
    const taskId = run.tags["processing/task_id"];
    if (!taskId || !QUEUE_UUID.test(taskId) || latest.has(taskId)) continue;
    const prefix = filters.type === "crawler" ? "crawler" : "webtech";
    const outcome = run.status === "SUCCESS" && ["completed", "completed_with_errors"].includes(run.tags[`${prefix}/outcome`]) ? run.tags[`${prefix}/outcome`] : null;
    latest.set(taskId, {taskId, status: run.status, runUrl: dagsterRunUrl(run.runId), outcome,
      failedPages: outcome && /^\d+$/.test(run.tags[`${prefix}/failed_pages`] ?? "") ? Number(run.tags[`${prefix}/failed_pages`]) : null,
      startedAt: run.startTime == null ? null : new Date(run.startTime * 1000).toISOString()});
  }
  return [...latest.values()];
}

const EXTRA_FIELDS = {
  webtech: ["execution_id", "force_rescan", "recent_days", "batch_size"],
  brave: ["execution_id", "query_type", "query_template", "force", "rescan_old", "requests_per_route", "input_batch_size", "answer_timeout_seconds", "progress_log_every", "progress_log_interval_seconds"],
  "ip-enrichment": ["execution_id", "batch_size", "max_requests", "request_delay_seconds", "parent_depth", "rdap_cache_days", "force_rdap", "rate_limit_retry_seconds", "transient_retry_seconds"],
  crawler: ["execution_id", "batch_size", "max_in_flight", "refresh_interval_days", "force_refresh", "challenge_agent_model", "challenge_agent_max_runs", "api", "model", "max_pages", "max_model_calls", "page_selection", "instructions", "wait_timeout_seconds", "poll_interval_seconds"],
} as const;

export function parseQueueConfig(filters: QueueFilters, serialized: string) {
  if (serialized.length > 30_000) throw new QueueRequestError("Processing parameters are too large.");
  let value: unknown;
  try { value = JSON.parse(serialized); } catch { throw new QueueRequestError("Enter valid JSON processing parameters."); }
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new QueueRequestError("Parameters must be a JSON object.");
  const config = value as Record<string, unknown>;
  const allowed: readonly string[] = EXTRA_FIELDS[filters.type];
  for (const [key, entry] of Object.entries(config)) {
    if (!allowed.includes(key)) throw new QueueRequestError(`Unsupported processing parameter: ${key}.`);
    if (entry !== null && !["string", "number", "boolean"].includes(typeof entry)) throw new QueueRequestError(`Invalid value for ${key}.`);
  }
  if (config.execution_id != null && (typeof config.execution_id !== "string" || !QUEUE_UUID.test(config.execution_id))) throw new QueueRequestError("execution_id must be a UUID from the original execution.");
  const numeric = filters.type === "crawler" ? {
    batch_size: [1, 100], max_in_flight: [1, 20], refresh_interval_days: [1, 3650],
    challenge_agent_max_runs: [3, 1000], max_pages: [1, 500], max_model_calls: [1, 1000],
    wait_timeout_seconds: [Number.MIN_VALUE, 86400, true], poll_interval_seconds: [Number.MIN_VALUE, 30, true],
  } : QUEUE_NUMBER_LIMITS[filters.type];
  const booleanFields = ["force_rescan", "force", "rescan_old", "force_rdap", "force_refresh"];
  for (const [key, entry] of Object.entries(config)) {
    if (entry === null && ["execution_id", "instructions", "max_requests"].includes(key)) continue;
    if (key in numeric) {
      const [min, max, fractional] = numeric[key as keyof typeof numeric] as [number, number, boolean?];
      if (typeof entry !== "number" || !Number.isFinite(entry) || (!fractional && !Number.isSafeInteger(entry)) || entry < min || entry > max) throw new QueueRequestError(`${key} must be ${fractional ? "a number" : "an integer"} between ${min} and ${max}.`);
    } else if (booleanFields.includes(key)) {
      if (typeof entry !== "boolean") throw new QueueRequestError(`${key} must be true or false.`);
    } else if (typeof entry !== "string" || !entry.trim()) throw new QueueRequestError(`${key} must be a nonempty string.`);
  }
  if (filters.type === "crawler") {
    try { Object.assign(config, parseCrawlSettings(objectSettings(config), filters.crawlType)); }
    catch (error) { throw new QueueRequestError(error instanceof Error ? error.message : "Invalid crawler parameters."); }
  }
  // Remaining types and ranges are validated by Dagster's authoritative asset schema before launch.
  return { ...config, task_id: filters.task };
}

// Serialize check+launch for a task within this Backoffice instance. Dagster's
// task locks and input validation remain authoritative across other launchers.
const launching = new Map<string, Promise<unknown>>();
export async function startQueueProcessing(filters: QueueFilters, serialized: string, requestId: string, requestedBy: string) {
  if (!filters.task || !QUEUE_UUID.test(filters.task)) throw new QueueRequestError("Select one task before processing.");
  if (!QUEUE_UUID.test(requestId)) throw new QueueRequestError("Invalid processing request ID.");
  if (filters.type === "webtech") assertWebtechAvailable();
  const config = parseQueueConfig(filters, serialized);
  const previous = launching.get(filters.task);
  const pending = previous ? previous.then(launch, launch) : launch();
  launching.set(filters.task, pending);
  try { return await pending; }
  finally { if (launching.get(filters.task) === pending) launching.delete(filters.task); }

  async function launch() {
    const def = queueDefinition(filters);
    const fingerprint = createHash("sha256").update(JSON.stringify(Object.entries(config).sort(([a], [b]) => a.localeCompare(b)))).digest("hex");
    const existing = await listRuns({job: def.job, limit: 1, tags: { "backoffice/queue_request_id": requestId }});
    const receipt = (run: {runId: string; status: string}) => ({ ok: true as const, ...run, runUrl: dagsterRunUrl(run.runId), taskId: filters.task });
    if (existing[0]) {
      if (existing[0].tags["processing/task_id"] !== filters.task || existing[0].tags["backoffice/queue_config"] !== fingerprint) throw new QueueRequestError("This request was already submitted with different parameters. Refresh before starting another execution.");
      return receipt({runId: existing[0].runId, status: existing[0].status});
    }
    const active = await listRuns({limit: 1, statuses: ACTIVE_QUEUE_RUNS, tags: { "processing/task_id": filters.task }});
    if (active.length) throw new QueueRequestError("This task already has an active Dagster run. Wait for it to finish.");
    const rows = await chQuery<{total: string}>(`SELECT toString(count()) AS total FROM ${def.from}
      WHERE ${def.where} AND toString(task_id) = {task:String}`, {task: filters.task, crawlType: filters.crawlType});
    if (!Number(rows[0]?.total ?? 0)) throw new QueueRequestError("This task has no inputs in the selected queue.");
    const run = await launchRun({job: def.job, assetSelection: [def.asset],
      runConfig: {ops: {[def.asset]: {config}}},
      tags: {"processing/task_id": filters.task, "backoffice/queue_request_id": requestId,
        "backoffice/queue_config": fingerprint, "corpscout/requested_by": requestedBy, "backoffice/action": "process-queue"},
    });
    return receipt(run);
  }
}
