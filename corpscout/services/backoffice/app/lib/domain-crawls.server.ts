import { createHash } from "node:crypto";
import { readCrawlArchive } from "~/lib/crawl-results.server";
import { resultObject } from "~/lib/crawl-results";
import { graphDomainError } from "~/lib/domain-graph";
import { crawlFailureReason } from "~/lib/domain-crawl-status";
import { chQuery } from "~/lib/clickhouse.server";
import type { DomainCrawlType } from "~/lib/se-domain-selection";

export interface DomainCrawlResult {
  website_id: string;
  website_url: string;
  request_id: string;
  attempt: number;
  state: string;
  crawl_status: string;
  successful: boolean;
  finished_at: string;
  error: string;
  s3_path: string;
  s3_state: string;
}

export interface DomainCrawlSummary {
  type: DomainCrawlType;
  label: string;
  latest: DomainCrawlResult | null;
  saved: DomainCrawlResult | null;
}

const CRAWLS = [
  { type: "site_info", label: "Basic info", table: "website_site_info_results" },
  { type: "jobs", label: "Jobs", table: "website_jobs_crawl_results" },
  { type: "full", label: "Full crawl", table: "website_full_crawl_results" },
] as const;

/** Read the latest attempt and latest accepted result independently: failures do not erase data. */
export async function loadDomainCrawls(domain: string, websiteId?: string): Promise<DomainCrawlSummary[]> {
  const query = CRAWLS.flatMap(({ type, table }) => ["latest", "saved"].map(kind => `(
    SELECT '${type}' AS type, '${kind}' AS kind, website_id, website_url, request_id, attempt, state,
      crawl_status, successful, formatDateTime(finished_at, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS finished_at,
      error, s3_path, s3_state
    FROM corpscout.${table} AS results FINAL
    WHERE ${websiteId ? "website_id = {websiteId:String}" : "domain = {domain:String}"} ${kind === "saved" ? "AND successful AND crawl_status IN ('finished', 'skip_crawling')" : ""}
    ORDER BY results.finished_at DESC, request_id DESC, attempt DESC LIMIT 1
  )`)).join(" UNION ALL ");
  const rows = await chQuery<DomainCrawlResult & {type: DomainCrawlType; kind: string}>(query, websiteId ? {websiteId} : {domain});
  return CRAWLS.map(({ type, label }) => ({
    type, label,
    latest: rows.find(row => row.type === type && row.kind === "latest") ?? null,
    saved: rows.find(row => row.type === type && row.kind === "saved") ?? null,
  }));
}

/** Default to retained good data; explicit attempt identities remain stable beyond the recent list. */
export async function loadDomainCrawlDetails(
  domain: string,
  selectedType: string | null,
  requested: {requestId: string; attempt: number} | null,
  latest = false,
  selectedWebsite: string | null = null,
) {
  // A central domain can own several origins. Choose one explicitly before loading
  // latest/good results, so a success from a sibling origin never hides a failure.
  const websites = await chQuery<{website_id: string; website_url: string}>(`
    SELECT w.website_id, w.website_origin AS website_url
    FROM corpscout.websites AS w
    INNER JOIN (${CRAWLS.map(({table}) => `SELECT website_id, max(finished_at) AS last_attempt
      FROM corpscout.${table} WHERE domain = {domain:String}
        OR website_id IN (SELECT website_id FROM corpscout.websites WHERE root_domain = {domain:String})
      GROUP BY website_id`).join(" UNION ALL ")}) AS history ON history.website_id = w.website_id
    GROUP BY w.website_id, w.website_origin
    ORDER BY max(history.last_attempt) DESC, w.website_origin`, {domain});
  const columns = `website_id, website_url, request_id, attempt, state, crawl_status, successful,
    formatDateTime(finished_at, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS finished_at, error, s3_path, s3_state`;
  const requestedTable = CRAWLS.find(crawl => crawl.type === selectedType)?.table;
  const requestedRows = requested && requestedTable ? await chQuery<DomainCrawlResult>(`
    SELECT ${columns} FROM corpscout.${requestedTable} AS results FINAL
    WHERE (domain = {domain:String} OR website_id IN (
      SELECT website_id FROM corpscout.websites WHERE root_domain = {domain:String}))
      AND request_id = {requestId:String} AND attempt = {attempt:UInt32} LIMIT 1`, {domain, ...requested}) : [];
  if (requested && !requestedRows.length) throw new Response("This crawl attempt was not found for this domain and crawl type.", {status: 404});
  const websiteId = selectedWebsite ?? requestedRows[0]?.website_id ?? websites[0]?.website_id ?? null;
  if ((websiteId && !websites.some(site => site.website_id === websiteId))
    || (requestedRows[0] && selectedWebsite && requestedRows[0].website_id !== selectedWebsite)) {
    throw new Response("This website or attempt does not belong to the selected domain.", {status: 404});
  }
  const crawls = websiteId ? await loadDomainCrawls(domain, websiteId)
    : CRAWLS.map(({type, label}) => ({type, label, latest: null, saved: null}));
  const selected = crawls.find(crawl => crawl.type === selectedType) ?? crawls.find(crawl => crawl.saved) ?? crawls.find(crawl => crawl.latest) ?? crawls[0];
  const table = CRAWLS.find(crawl => crawl.type === selected.type)!.table;
  const attempts = websiteId ? await chQuery<DomainCrawlResult>(`SELECT ${columns} FROM corpscout.${table} AS results FINAL
    WHERE website_id = {websiteId:String}
    ORDER BY results.finished_at DESC, request_id DESC, attempt DESC LIMIT 20`, {websiteId}) : [];
  const result = requested ? requestedRows[0] : latest ? selected.latest : selected.saved ?? selected.latest;
  const showingSaved = result != null && result.request_id === selected.saved?.request_id && result.attempt === selected.saved.attempt;
  const paths = [...new Set([result, selected.latest].flatMap(item => item?.s3_state === "uploaded" && item.s3_path ? [item.s3_path] : []))];
  const archives = new Map<string, {payload: Record<string, unknown> | null; error: string | null}>(await Promise.all(paths.map(async path => {
    try {
      const archive = await readCrawlArchive(path);
      const item = [result, selected.latest].find(item => item?.s3_path === path)!;
      const url = new URL(archive.website_url);
      url.hostname = url.hostname.replace(/[.]$/, "");
      const archivedWebsite = createHash("sha256").update(url.origin).digest("hex");
      if (archivedWebsite !== item.website_id || (archive.website_id && archive.website_id !== item.website_id) || (archive.request_id && archive.request_id !== item.request_id)
        || (archive.attempt != null && archive.attempt !== item.attempt)) throw new Error("Archive attempt mismatch");
      const payload: unknown = JSON.parse(archive.result_json);
      if (!payload || typeof payload !== "object" || Array.isArray(payload)) throw new Error("Invalid result object");
      return [path, {payload: resultObject(payload), error: null}] as const;
    } catch {
      return [path, {payload: null, error: "The saved JSON could not be read. Refresh to retry; the recorded crawl status is still shown."}] as const;
    }
  })));
  // Matching batches publish parsed crawl fields directly to ClickHouse. They
  // have the same detail view even when this attempt has no uploaded S3 object.
  const recorded = new Map<string, {payload: Record<string, unknown> | null; error: string | null}>();
  for (const item of [result, selected.latest]) {
    if (!item || item.s3_path || recorded.has(`${item.request_id}:${item.attempt}`)) continue;
    const key = `${item.request_id}:${item.attempt}`;
    try {
      const [fields] = await chQuery<{site_info: string | null; pages: string; page_observations: string; model_usage: string | null}>(`
        SELECT site_info, pages, page_observations, model_usage FROM corpscout.${table} FINAL
        WHERE website_id = {websiteId:String} AND request_id = {requestId:String} AND attempt = {attempt:UInt32}
        LIMIT 1`, {websiteId: item.website_id, requestId: item.request_id, attempt: item.attempt});
      recorded.set(key, {payload: fields && typeof fields.pages === "string" ? {
        crawl: {status: item.crawl_status, site_url: item.website_url,
          site_info: JSON.parse(fields.site_info || "{}"), pages: JSON.parse(fields.pages || "[]"),
          usage: JSON.parse(fields.model_usage || "{}")},
        documents: (JSON.parse(fields.page_observations || "[]") ?? []).map((observations: unknown) => ({input: {observations}})),
      } : null, error: null});
    } catch {
      recorded.set(key, {payload: null, error: "The recorded crawl fields could not be loaded. Refresh to retry."});
    }
  }
  const withReason = (item: DomainCrawlResult): DomainCrawlResult => {
    if (item.successful || item.error) return item;
    const archive = archives.get(item.s3_path) ?? recorded.get(`${item.request_id}:${item.attempt}`);
    return {...item, error: archive?.payload ? crawlFailureReason(archive.payload) || "No failure reason was recorded."
      : archive?.error ? "Failure reason unavailable because the saved JSON could not be read." : ""};
  };
  const archive = result ? archives.get(result.s3_path) ?? recorded.get(`${result.request_id}:${result.attempt}`) : null;
  return {crawls, websites, websiteId, selectedType: selected.type, showingSaved,
    result: result ? withReason(result) : null,
    latest: selected.latest ? withReason(selected.latest) : null,
    attempts: attempts.map(withReason),
    payload: archive?.payload ?? null, archiveError: archive?.error ?? null};
}

export async function loadDomainCrawlPage(domain: string, request: Request) {
  if (!domain || graphDomainError(domain)) throw new Response("Not found", {status: 404});
  const search = new URL(request.url).searchParams;
  const type = search.get("type");
  const website = search.get("website");
  if (website !== null && !/^[a-f0-9]{64}$/.test(website)) throw new Response("Invalid website identity.", {status: 400});
  const requestId = search.get("request");
  const attempt = search.get("attempt");
  if (type !== null && !["site_info", "jobs", "full"].includes(type)) throw new Response("Unknown crawl type.", {status: 400});
  if ((requestId !== null || attempt !== null) && (type === null || requestId === null || !/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(requestId)
    || attempt === null || !/^[1-9]\d*$/.test(attempt) || Number(attempt) > 4294967295)) {
    throw new Response("Select a crawl type, request and valid attempt number.", {status: 400});
  }
  try {
    return {domain, details: await loadDomainCrawlDetails(domain, type, requestId ? {requestId, attempt: Number(attempt)} : null, search.get("result") === "latest", website), error: null};
  } catch (error) {
    if (error instanceof Response) throw error;
    return {domain, details: null, error: "Crawl results could not be loaded. Refresh crawl status to retry."};
  }
}
