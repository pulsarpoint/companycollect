import { readCrawlArchive } from "~/lib/crawl-results.server";
import { resultObject } from "~/lib/crawl-results";
import { crawlFailureReason } from "~/lib/domain-crawl-status";
import { chQuery } from "~/lib/clickhouse.server";
import type { DomainCrawlType } from "~/lib/se-domain-selection";

export interface DomainCrawlResult {
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
export async function loadDomainCrawls(domain: string): Promise<DomainCrawlSummary[]> {
  const query = CRAWLS.flatMap(({ type, table }) => ["latest", "saved"].map(kind => `(
    SELECT '${type}' AS type, '${kind}' AS kind, request_id, attempt, state,
      crawl_status, successful, formatDateTime(finished_at, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS finished_at,
      error, s3_path, s3_state
    FROM corpscout.${table} AS results FINAL
    WHERE domain = {domain:String} ${kind === "saved" ? "AND successful AND crawl_status IN ('finished', 'skip_crawling')" : ""}
    ORDER BY results.finished_at DESC, request_id DESC, attempt DESC LIMIT 1
  )`)).join(" UNION ALL ");
  const rows = await chQuery<DomainCrawlResult & {type: DomainCrawlType; kind: string}>(query, { domain });
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
) {
  const crawls = await loadDomainCrawls(domain);
  const selected = crawls.find(crawl => crawl.type === selectedType) ?? crawls.find(crawl => crawl.saved) ?? crawls.find(crawl => crawl.latest) ?? crawls[0];
  const table = CRAWLS.find(crawl => crawl.type === selected.type)!.table;
  const columns = `request_id, attempt, state, crawl_status, successful,
    formatDateTime(finished_at, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS finished_at, error, s3_path, s3_state`;
  const [attempts, requestedRows] = await Promise.all([
    chQuery<DomainCrawlResult>(`SELECT ${columns} FROM corpscout.${table} AS results FINAL
      WHERE domain = {domain:String}
      ORDER BY results.finished_at DESC, request_id DESC, attempt DESC LIMIT 20`, {domain}),
    requested ? chQuery<DomainCrawlResult>(`SELECT ${columns} FROM corpscout.${table} AS results FINAL
      WHERE domain = {domain:String} AND request_id = {requestId:String} AND attempt = {attempt:UInt32}
      LIMIT 1`, {domain, ...requested}) : Promise.resolve([]),
  ]);
  if (requested && !requestedRows.length) throw new Response("This crawl attempt was not found for this domain and crawl type.", {status: 404});
  const result = requested ? requestedRows[0] : latest ? selected.latest : selected.saved ?? selected.latest;
  const showingSaved = result != null && result.request_id === selected.saved?.request_id && result.attempt === selected.saved.attempt;
  const paths = [...new Set([result, selected.latest].flatMap(item => item?.s3_state === "uploaded" && item.s3_path ? [item.s3_path] : []))];
  const archives = new Map<string, {payload: Record<string, unknown> | null; error: string | null}>(await Promise.all(paths.map(async path => {
    try {
      const archive = await readCrawlArchive(path);
      if (archive.domain !== domain) throw new Error("Archive domain mismatch");
      const payload: unknown = JSON.parse(archive.result_json);
      if (!payload || typeof payload !== "object" || Array.isArray(payload)) throw new Error("Invalid result object");
      return [path, {payload: resultObject(payload), error: null}] as const;
    } catch {
      return [path, {payload: null, error: "The saved JSON could not be read. Refresh to retry; the recorded crawl status is still shown."}] as const;
    }
  })));
  const withReason = (item: DomainCrawlResult): DomainCrawlResult => {
    if (item.successful || item.error) return item;
    const archive = archives.get(item.s3_path);
    return {...item, error: archive?.payload ? crawlFailureReason(archive.payload) || "No failure reason was recorded."
      : archive?.error ? "Failure reason unavailable because the saved JSON could not be read." : ""};
  };
  const archive = result ? archives.get(result.s3_path) : null;
  return {crawls, selectedType: selected.type, showingSaved,
    result: result ? withReason(result) : null,
    latest: selected.latest ? withReason(selected.latest) : null,
    attempts: attempts.map(withReason),
    payload: archive?.payload ?? null, archiveError: archive?.error ?? null};
}
