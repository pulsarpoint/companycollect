import { chQuery } from "~/lib/clickhouse.server";
import type { WorkspaceDomainFilters } from "~/lib/workspace-domains";

const PAGE_SIZE = 25;
export interface DomainEvidence {
  root_domain: string;
  sources: string[];
  has_dns_records: number;
  dns_last_observed_at: string | null;
  website_count: number;
  observed_website_count: number;
  company_count: number;
  first_seen_at: string;
  last_seen_at: string;
  refreshed_at: string;
}
export interface DomainSite {
  website_origin: string;
  evidence_status: string;
  sources: string[];
  last_observed_at: string | null;
}

export async function listWorkspaceDomains(filters: WorkspaceDomainFilters, after: string) {
  const conditions = ["root_domain > {after:String}"];
  if (filters.prefix) conditions.push("startsWith(root_domain, {prefix:String})");
  if (filters.sources.length) conditions.push(`${filters.sourceMatch === "all" ? "hasAll" : "hasAny"}(sources, {sources:Array(String)})`);
  if (filters.dns !== "any") conditions.push(`has_dns_records = ${filters.dns === "with" ? 1 : 0}`);
  if (filters.websites === "observed") conditions.push("has_website = 1 AND observed_website_count > 0");
  else if (filters.websites !== "any") conditions.push(`has_website = ${filters.websites === "with" ? 1 : 0}`);
  if (filters.companies !== "any") conditions.push(`has_company = ${filters.companies === "with" ? 1 : 0}`);
  const [rows, totals, publication] = await Promise.all([
    chQuery<DomainEvidence>(`SELECT root_domain,sources,has_dns_records,dns_last_observed_at,
      toUInt32(website_count) AS website_count,toUInt32(observed_website_count) AS observed_website_count,
      toUInt32(company_count) AS company_count,first_seen_at,last_seen_at,refreshed_at
      FROM corpscout.domains_search
      WHERE ${conditions.join(" AND ")}
      ORDER BY root_domain LIMIT {limit:UInt32}
      SETTINGS optimize_read_in_order=1, max_threads=4, max_execution_time=20`,
      { prefix: filters.prefix, sources: filters.sources, after, limit: PAGE_SIZE + 1 }),
    // MergeTree row-count metadata: no request-time full-table count or filter joins.
    chQuery<{ name: string; total: string }>(`SELECT name,toString(total_rows) AS total FROM system.tables
      WHERE database='corpscout' AND name IN ('domains_search','websites')`),
    chQuery<{ refreshed_at: string }>("SELECT refreshed_at FROM corpscout.domains_search LIMIT 1"),
  ]);
  const visible = rows.slice(0, PAGE_SIZE);
  return { rows: visible, hasMore: rows.length > PAGE_SIZE,
    next: visible.at(-1)?.root_domain ?? "", total: totals.find((row) => row.name === "domains_search")?.total ?? "0",
    websiteInventoryTotal: totals.find((row) => row.name === "websites")?.total ?? "0",
    refreshedAt: publication[0]?.refreshed_at ?? null };
}

export async function listDomainSites(domain: string, after: string) {
  const rows = await chQuery<DomainSite>(`SELECT website_origin,evidence_status,sources,last_observed_at
    FROM corpscout.websites WHERE root_domain={domain:String} AND website_origin>{after:String}
    ORDER BY website_origin LIMIT {limit:UInt32}
    SETTINGS optimize_read_in_order=1, max_execution_time=20`, { domain, after, limit: PAGE_SIZE + 1 });
  const sites = rows.slice(0, PAGE_SIZE);
  return { sites, hasMore: rows.length > PAGE_SIZE, next: sites.at(-1)?.website_origin ?? "" };
}
