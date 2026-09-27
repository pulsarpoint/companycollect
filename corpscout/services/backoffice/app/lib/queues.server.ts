import { loadBraveSourceSummaries } from "~/lib/brave-queue-history.server";
import { BraveSearchError, resolveBraveSearch } from "~/lib/brave-searches.server";
import { loadQueueSourceSummaries, type QueueHistoryReference, type QueueSourceSummary } from "~/lib/queue-history.server";
import { createHash } from "node:crypto";
import { CrawlLlmError, prepareCrawlSettings, verifySelectedLlm } from "~/lib/crawl-llm.server";
import { chQuery } from "~/lib/clickhouse.server";
import { dagsterRunUrl, launchRun, listRuns } from "~/lib/dagster.server";
import { objectSettings, parseCrawlSettings } from "~/lib/crawl-settings.server";
import { assertWebtechAvailable } from "~/lib/webtech-maintenance.server";
import { CRAWL_QUEUES, ACTIVE_QUEUE_RUNS, IP_ENRICHMENT_PROXY_REGISTRIES, QUEUE_NUMBER_LIMITS, QUEUE_PAGE_SIZE, QUEUE_TEMPLATES, QUEUE_UUID, type CrawlQueueType, type QueueFilters } from "~/lib/queues";

export class QueueRequestError extends Error {}

function queueDefinition(filters: QueueFilters) {
  switch (filters.type) {
    case "webtech": return {
      table: "corpscout.webtech_scan_input", asset: "webtech_scan_results", job: "webtech_scan_results_job",
      from: "corpscout.webtech_scan_input", where: "1", target: "root_domain", detail: "page_url",
      source: "source_name", record: "source_record_id", time: "toString(submitted_at)", id: "input_id",
    };
    case "brave": return {
      table: "corpscout.company_brave_queue_input", asset: "company_brave_search_results", job: "company_brave_search_job",
      from: "corpscout.company_brave_queue_input", where: "1", target: "company_name",
      detail: "concat(country_code, ':', company_id)", source: "source_name", record: "source_record_id", time: "toString(submitted_at)", id: "input_id",
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
        from: "corpscout.website_crawl_task_domains", where: "crawl_type = {crawlType:String}",
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
  const inputOrder = "task_id, input_id";
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

/** Queued entries per crawler queue (full, jobs, site_info), for the queue tabs. */
export async function loadCrawlQueueCounts(): Promise<Record<CrawlQueueType, number>> {
  const rows = await chQuery<{crawl_type: string; total: string}>(
    "SELECT crawl_type, toString(count()) AS total FROM corpscout.website_crawl_task_domains GROUP BY crawl_type", {});
  const counts: Record<CrawlQueueType, number> = {full: 0, jobs: 0, site_info: 0};
  for (const row of rows) if (row.crawl_type in counts) counts[row.crawl_type as CrawlQueueType] = Number(row.total);
  return counts;
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
  const selections = filters.type === "crawler" ? CRAWL_QUEUES.map(queue => ({...filters, crawlType: queue.id})) : [filters];
  const groups = await Promise.all(selections.map(async selection => ({
    crawlType: filters.type === "crawler" ? selection.crawlType : null,
    runs: await listRuns({job: queueDefinition(selection).job, limit: 50}),
  })));
  const references: QueueHistoryReference[] = [];
  const OUTCOME_TAGS: Record<QueueFilters["type"], string | null> = {webtech: "webtech", crawler: "crawler", "ip-enrichment": "ip_enrichment", brave: "brave"};
  const latest = new Map<string, {taskId: string; status: string; runUrl: string | null; startedAt: string | null; outcome: string | null; failedPages: number | null; skippedPages: number | null; crawlType: CrawlQueueType | null}>();
  for (const {runs, crawlType} of groups) for (const run of runs) {
    const taskId = run.tags["processing/task_id"];
    if (!taskId || !QUEUE_UUID.test(taskId)) continue;
    references.push({taskId, crawlType, executionId: run.tags["crawler/execution_id"] || run.runId});
    if (latest.has(`${crawlType}:${taskId}`)) continue;
    const prefix = OUTCOME_TAGS[filters.type];
    const outcome = prefix && run.status === "SUCCESS" && ["completed", "completed_with_errors"].includes(run.tags[`${prefix}/outcome`]) ? run.tags[`${prefix}/outcome`] : null;
    latest.set(`${crawlType}:${taskId}`, {taskId, crawlType, status: run.status, runUrl: dagsterRunUrl(run.runId), outcome,
      failedPages: outcome && /^\d+$/.test(run.tags[`${prefix}/failed_pages`] ?? "") ? Number(run.tags[`${prefix}/failed_pages`]) : null,
      skippedPages: outcome && /^\d+$/.test(run.tags[`${prefix}/skipped_pages`] ?? "") ? Number(run.tags[`${prefix}/skipped_pages`]) : null,
      startedAt: run.startTime == null ? null : new Date(run.startTime * 1000).toISOString()});
  }
  const history = [...latest.values()].sort((a, b) =>
    (b.startedAt ? Date.parse(b.startedAt) : Infinity) - (a.startedAt ? Date.parse(a.startedAt) : Infinity)).slice(0, 50);
  let sources: QueueSourceSummary[] = [];
  let sourcesError = false;
  if (filters.type === "crawler" || filters.type === "webtech") {
    try { sources = await loadQueueSourceSummaries(filters.type, references.filter(ref => history.some(task => task.taskId === ref.taskId && task.crawlType === ref.crawlType))); }
    catch { sourcesError = true; }
  }
  if (filters.type === "brave") {
    try { sources = await loadBraveSourceSummaries(history.map(task => task.taskId)); }
    catch { sourcesError = true; }
  }
  return history.map(task => ({...task, sourcesError,
    sources: sources.find(source => source.task_id === task.taskId && source.task_type === (task.crawlType ?? (filters.type === "brave" ? "brave" : "webtech"))) ?? null}));
}

const EXTRA_FIELDS = {
  // Envelope size stays a Dagster default: it is transport only, not a processing choice.
  webtech: ["execution_id", "force_rescan", "recent_days"],
  brave: ["execution_id", "llm_profile_id", "brave_search_id", "brave_search_revision", "force_rescan", "recent_days", "requests_per_route", "input_batch_size", "answer_timeout_seconds", "progress_log_every", "progress_log_interval_seconds"],
  "ip-enrichment": ["execution_id", "batch_size", "max_in_flight", "max_queue_per_registry", "max_requests", "request_delay_seconds", "registry_request_delays", "registry_daily_budgets", "rate_limit_pause_seconds", "use_proxies", "parent_depth", "rdap_cache_days", "force_rdap", "rate_limit_retry_seconds", "transient_retry_seconds"],
  crawler: ["match_company", "company_country", "skip_company_matching_if_mapped", "execution_id", "full_crawl_all", "max_in_flight", "refresh_interval_days", "force_refresh", "challenge_agent_model", "challenge_agent_max_runs", "llm_profile_id", "max_pages", "max_model_calls", "page_selection", "instructions", "wait_timeout_seconds", "poll_interval_seconds"],
} as const;

/**
 * Per-registry maps merge over the safe defaults, as Dagster's validators do, and the
 * registry limits are refused here too; Dagster validates registry names authoritatively.
 */
function registryMap(key: "registry_request_delays" | "registry_daily_budgets", min: number, max: number, fractional: boolean,
  limits: Record<string, [number, number, string]>) {
  return (entry: unknown) => {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) throw new QueueRequestError(`${key} must be an object of registry names to numbers.`);
    const merged: Record<string, number> = {...QUEUE_TEMPLATES["ip-enrichment"][key] as Record<string, number>};
    for (const [registry, value] of Object.entries(entry)) {
      if (!/^[a-z][a-z.]{1,31}$/.test(registry)) throw new QueueRequestError(`${key}: invalid registry name ${JSON.stringify(registry)}.`);
      if (typeof value !== "number" || !Number.isFinite(value) || (!fractional && !Number.isSafeInteger(value)) || value < min || value > max) throw new QueueRequestError(`${key}.${registry} must be ${fractional ? "a number" : "an integer"} between ${min} and ${max}.`);
      merged[registry] = value;
    }
    for (const [registry, [low, high, why]] of Object.entries(limits)) {
      if (merged[registry] < low || merged[registry] > high) throw new QueueRequestError(`${key}.${registry} must be between ${low} and ${high} (${why}).`);
    }
    return merged;
  };
}
const IP_ENRICHMENT_STRUCTURED: Record<string, (entry: unknown) => unknown> = {
  registry_request_delays: registryMap("registry_request_delays", 0, 60, true, {lacnic: [6, 60, "LACNIC allows 10 queries per minute per address"]}),
  registry_daily_budgets: registryMap("registry_daily_budgets", 1, Number.MAX_SAFE_INTEGER, false, {afrinic: [1, 5000, "AFRINIC blocks an address above 5,000 queries a day"]}),
  use_proxies: entry => {
    if (!Array.isArray(entry) || entry.some(registry => typeof registry !== "string" || !IP_ENRICHMENT_PROXY_REGISTRIES.includes(registry))) {
      throw new QueueRequestError(`use_proxies may only list ${IP_ENRICHMENT_PROXY_REGISTRIES.join(", ")}; RIPE, APNIC and LACNIC always go direct. Proxy URLs belong in the service environment (RDAP_PROXIES).`);
    }
    return entry;
  },
};

export function parseQueueConfig(filters: QueueFilters, serialized: string): Record<string, unknown> & {task_id: string} {
  if (serialized.length > 30_000) throw new QueueRequestError("Processing parameters are too large.");
  let value: unknown;
  try { value = JSON.parse(serialized); } catch { throw new QueueRequestError("Enter valid JSON processing parameters."); }
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new QueueRequestError("Parameters must be a JSON object.");
  const config = value as Record<string, unknown>;
  const allowed: readonly string[] = EXTRA_FIELDS[filters.type];
  for (const [key, entry] of Object.entries(config)) {
    if (!allowed.includes(key)) throw new QueueRequestError(`Unsupported processing parameter: ${key}.`);
    if (filters.type === "ip-enrichment" && Object.hasOwn(IP_ENRICHMENT_STRUCTURED, key)) { config[key] = IP_ENRICHMENT_STRUCTURED[key](entry); continue; }
    if (entry !== null && !["string", "number", "boolean"].includes(typeof entry)) throw new QueueRequestError(`Invalid value for ${key}.`);
  }
  if (config.execution_id != null && (typeof config.execution_id !== "string" || !QUEUE_UUID.test(config.execution_id))) throw new QueueRequestError("execution_id must be a UUID from the original execution.");
  const numeric = filters.type === "crawler" ? {
    max_in_flight: [1, 20], refresh_interval_days: [1, 3650],
    challenge_agent_max_runs: [3, 1000], max_pages: [1, 500], max_model_calls: [1, 1000],
    wait_timeout_seconds: [Number.MIN_VALUE, 86400, true], poll_interval_seconds: [Number.MIN_VALUE, 30, true],
  } : QUEUE_NUMBER_LIMITS[filters.type];
  const booleanFields = ["match_company", "skip_company_matching_if_mapped", "full_crawl_all", "force_rescan", "force_rdap", "force_refresh"];
  for (const [key, entry] of Object.entries(config)) {
    if (filters.type === "ip-enrichment" && Object.hasOwn(IP_ENRICHMENT_STRUCTURED, key)) continue;
    if (entry === null && ["execution_id", "instructions", "max_requests"].includes(key)) continue;
    if (key === "brave_search_revision") {
      if (typeof entry !== "number" || !Number.isSafeInteger(entry) || entry < 0) throw new QueueRequestError("Invalid Brave search version.");
      continue;
    }
    if (key in numeric) {
      const [min, max, fractional] = numeric[key as keyof typeof numeric] as [number, number, boolean?];
      if (typeof entry !== "number" || !Number.isFinite(entry) || (!fractional && !Number.isSafeInteger(entry)) || entry < min || entry > max) throw new QueueRequestError(`${key} must be ${fractional ? "a number" : "an integer"} between ${min} and ${max}.`);
    } else if (booleanFields.includes(key)) {
      if (typeof entry !== "boolean") throw new QueueRequestError(`${key} must be true or false.`);
    } else if (typeof entry !== "string" || !entry.trim()) throw new QueueRequestError(`${key} must be a nonempty string.`);
  }
  if (filters.type === "brave" && (typeof config.llm_profile_id !== "string" || !config.llm_profile_id.trim() || config.llm_profile_id.length > 200)) {
    throw new QueueRequestError("Choose an LLM from LLM settings before starting Brave processing.");
  }
  if (filters.type === "brave" && (typeof config.brave_search_id !== "string" || (config.brave_search_id !== "saved" && !QUEUE_UUID.test(config.brave_search_id))
      || typeof config.brave_search_revision !== "number" || !Number.isSafeInteger(config.brave_search_revision) || config.brave_search_revision < (config.brave_search_id === "saved" ? 0 : 1))) {
    throw new QueueRequestError("Choose a saved Brave search before starting processing.");
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
    let runtimeConfig: Record<string, unknown> = config;
    if (filters.type === "crawler" || filters.type === "brave") {
      try {
        if (filters.type === "crawler") runtimeConfig = await prepareCrawlSettings(config);
        else {
          const {llm_profile_id: profileId, brave_search_id: searchId, brave_search_revision: revision, ...settings} = config;
          const search = await resolveBraveSearch(filters.task, searchId as string, revision as number);
          runtimeConfig = {...settings, ...search, llm: await verifySelectedLlm(profileId, "brave")};
        }
      }
      catch (error) {
        if (error instanceof CrawlLlmError || error instanceof BraveSearchError) throw new QueueRequestError(error.message);
        throw error;
      }
    }
    const run = await launchRun({job: def.job, assetSelection: [def.asset],
      runConfig: {ops: {[def.asset]: {config: runtimeConfig}}},
      tags: {"processing/task_id": filters.task, "backoffice/queue_request_id": requestId,
        "backoffice/queue_config": fingerprint, "corpscout/requested_by": requestedBy, "backoffice/action": "process-queue"},
    });
    return receipt(run);
  }
}
