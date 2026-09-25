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

/** Read JSON only for known objects belonging to this domain; keep status when an archive is unavailable. */
export async function loadDomainCrawlDetails(domain: string, selectedType: string | null, previous: boolean) {
  const crawls = await loadDomainCrawls(domain);
  const selected = crawls.find(crawl => crawl.type === selectedType) ?? crawls.find(crawl => crawl.latest) ?? crawls[0];
  const paths = [...new Set(crawls.map(crawl => crawl.latest).concat(previous ? [selected.saved] : [])
    .filter(result => result?.s3_state === "uploaded" && result.s3_path).map(result => result!.s3_path))];
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
  const summaries = crawls.map(crawl => {
    if (!crawl.latest) return crawl;
    const archive = archives.get(crawl.latest.s3_path);
    const reason = archive?.payload ? crawlFailureReason(archive.payload) : "";
    return {...crawl, latest: {...crawl.latest, error: crawl.latest.error || (!crawl.latest.successful ? reason || (archive?.error ? "Failure reason unavailable because the saved JSON could not be read." : "No failure reason was recorded.") : "")}};
  });
  const result = previous ? selected.saved : selected.latest;
  const archive = result ? archives.get(result.s3_path) : null;
  return {crawls: summaries, selectedType: selected.type, previous, result,
    payload: archive?.payload ?? null, archiveError: archive?.error ?? null};
}
