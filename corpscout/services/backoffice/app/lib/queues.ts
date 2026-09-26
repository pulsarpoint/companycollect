/** Queue navigation and operator templates. Jobs and input relations are server-owned. */
export const QUEUE_TYPES = [
  { id: "webtech", label: "Webtech" },
  { id: "brave", label: "Brave" },
  { id: "crawler", label: "Crawler" },
  { id: "ip-enrichment", label: "IP enrichment" },
] as const;
export type QueueType = typeof QUEUE_TYPES[number]["id"];
export type CrawlQueueType = "full" | "jobs" | "site_info";
export const CRAWL_QUEUES: readonly {id: CrawlQueueType; label: string}[] = [
  {id: "full", label: "Full crawl"},
  {id: "jobs", label: "Jobs"},
  {id: "site_info", label: "Site information"},
];
export const QUEUE_PAGE_SIZE = 25;
export const QUEUE_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const ACTIVE_QUEUE_RUNS = ["QUEUED", "NOT_STARTED", "MANAGED", "STARTING", "STARTED", "CANCELING"] as const;

/** Queues on the shared processing queue contract: one open draft per scope, freeze at Start, partition purge. */
export const DRAFT_QUEUES: readonly QueueType[] = ["webtech", "brave", "crawler", "ip-enrichment"];
export function isDraftQueue(type: QueueType) { return DRAFT_QUEUES.includes(type); }

export const QUEUE_TEMPLATES: Record<Exclude<QueueType, "crawler">, Record<string, unknown>> = {
  webtech: { force_rescan: false, recent_days: 30 },
  brave: {
    brave_search_id: "", brave_search_revision: 0,
    llm_profile_id: "",
    force_rescan: false, recent_days: 30,

  },
  "ip-enrichment": {
    force_rdap: false, rdap_cache_days: 30, parent_depth: 1,
    batch_size: 250, max_requests: null, request_delay_seconds: 1,
    rate_limit_retry_seconds: 3600, transient_retry_seconds: 900,
  },
};

/** Limits mirror the deployed results assets and are shared by form and action validation. */
export const QUEUE_NUMBER_LIMITS: Record<Exclude<QueueType, "crawler">, Record<string, [number, number, boolean?]>> = {
  webtech: { recent_days: [1, 3650] },
  brave: { recent_days: [1, 3650], requests_per_route: [1, 8], input_batch_size: [4, 10000], answer_timeout_seconds: [1, 600], progress_log_every: [1, 100000], progress_log_interval_seconds: [1, 3600] },
  "ip-enrichment": { batch_size: [1, 10000], max_requests: [1, Number.MAX_SAFE_INTEGER], request_delay_seconds: [0, 60, true], parent_depth: [0, 5], rdap_cache_days: [1, Number.MAX_SAFE_INTEGER], rate_limit_retry_seconds: [1, Number.MAX_SAFE_INTEGER], transient_retry_seconds: [1, Number.MAX_SAFE_INTEGER] },
};

export interface QueueFilters {
  type: QueueType;
  crawlType: CrawlQueueType;
  task: string;
  search: string;
  page: number;
  taskPage: number;
}

export function parseQueueFilters(type: string | undefined, params: URLSearchParams): QueueFilters {
  if (!QUEUE_TYPES.some(queue => queue.id === type)) throw new Error("Unknown queue type.");
  const task = (params.get("task") ?? "").trim().toLowerCase();
  if (task && !QUEUE_UUID.test(task)) throw new Error("Enter a valid task UUID.");
  const crawlType = params.get("crawlType") ?? "full";
  if (!["full", "jobs", "site_info"].includes(crawlType)) throw new Error("Unknown crawler queue.");
  const pageNumber = (key: string) => {
    const value = Number(params.get(key) ?? 1);
    return Number.isSafeInteger(value) && value >= 1 ? Math.min(value, Math.floor(Number.MAX_SAFE_INTEGER / QUEUE_PAGE_SIZE)) : 1;
  };
  return { type: type as QueueType, crawlType: crawlType as CrawlQueueType, task,
    search: (params.get("search") ?? params.get("prefix") ?? "").trim().slice(0, 253),
    page: pageNumber("page"), taskPage: pageNumber("taskPage") };
}

export function queuePath(filters: QueueFilters, changes: Partial<QueueFilters> = {}) {
  const value = { ...filters, ...changes };
  const params = new URLSearchParams();
  if (value.type === "crawler") params.set("crawlType", value.crawlType);
  if (value.task) params.set("task", value.task);
  if (value.search) params.set("search", value.search);
  if (value.page > 1) params.set("page", String(value.page));
  if (value.taskPage > 1) params.set("taskPage", String(value.taskPage));
  return `/admin/queues/${value.type}${params.size ? `?${params}` : ""}`;
}
