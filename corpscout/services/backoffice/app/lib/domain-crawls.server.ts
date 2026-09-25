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
