import { chQuery } from "~/lib/clickhouse.server";
import type { WorkspaceDomainFilters } from "~/lib/workspace-domains";

// Registry website records cover countries not yet in the reviewed company
// serving projection. Keep the composite country/company identity throughout.
const COMPANY_DOMAINS = `SELECT root_domain, upper(ifNull(country_iso2, '')) AS country_code,
    company_id, company_id_type, website_host
  FROM corpscout.company_website_domains FINAL WHERE is_current = 1
  UNION ALL
  SELECT root_domain, upper(country_code) AS country_code, company_id,
    'registry' AS company_id_type, website_host
  FROM corpscout.company_domains_resolved WHERE is_active = 1`;

const WEBTECH_DOMAINS = `SELECT DISTINCT root_domain FROM corpscout.webtech_domain_technologies_current`;
const PAGE_SIZE = 25;

export interface DomainEvidence {
  root_domain: string;
  archived: number;
  dns: number;
  webtech: number;
  companies: number;
  company_records: [string, string, string][];
}

export interface DomainSite {
  hostname: string;
  archived: number;
  webtech: number;
  technologies: string[];
}

export async function listWorkspaceDomains(
  filters: WorkspaceDomainFilters,
  after: string,
) {
  const conditions = [
    "root_domain > {after:String}",
    // Legacy registry inputs include malformed email/domain strings. Keep the
    // inventory consistent with the hostname validation on detail routes.
    "length(root_domain) <= 253",
    "match(root_domain, '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:[.][a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$')",
    "NOT match(root_domain, '^[0-9]+(?:[.][0-9]+){3}$')",
  ];
  if (filters.prefix)
    conditions.push("startsWith(root_domain, {prefix:String})");
  if (
    filters.companies === "without" ||
    (filters.companies === "with" && filters.webtech === "with")
  )
    conditions.push(
      `root_domain ${filters.companies === "without" ? "NOT " : ""}IN (SELECT root_domain FROM (${COMPANY_DOMAINS}))`,
    );
  if (filters.webtech === "without")
    conditions.push(
      `root_domain ${filters.webtech === "without" ? "NOT " : ""}IN (${WEBTECH_DOMAINS})`,
    );
  // Positive filters provide a smaller, complete candidate set. Otherwise read
  // a bounded sorted slice from each inventory and merge them by root domain.
  const sources =
    filters.webtech === "with"
      ? [`(${WEBTECH_DOMAINS})`]
      : filters.companies === "with"
        ? [`(${COMPANY_DOMAINS})`]
        : [
            "corpscout.domains",
            "corpscout.open_page_rank_domains",
            "corpscout.commoncrawl_domains",
            "corpscout.commoncrawl_domain_dns_scan",
            "corpscout.webtech_domain_scan_results",
            `(${COMPANY_DOMAINS})`,
            "(SELECT domain AS root_domain FROM corpscout.website_crawl_results)",
          ];
  // Graph nodes are sorted by release before domain. Constrain each release
  // so paging reads in key order instead of sorting the whole graph.
  const releases =
    filters.webtech !== "with" && filters.companies !== "with"
      ? await chQuery<{ graph_release: string }>(
          "SELECT graph_release FROM corpscout.commoncrawl_domain_graph_snapshots FINAL",
        )
      : [];
  const queries = [
    ...sources.map((source) => ({ source, release: "" })),
    ...releases.map(({ graph_release }) => ({
      source: "corpscout.commoncrawl_domain_graph_nodes",
      release: graph_release,
    })),
  ];
  const batches = await Promise.all(
    queries.map(({ source, release }) =>
      chQuery<{ root_domain: string }>(
        `SELECT DISTINCT root_domain FROM ${source}
     WHERE ${conditions.join(" AND ")} ${release ? "AND graph_release = {release:String}" : ""}
     ORDER BY root_domain LIMIT {limit:UInt32}
     SETTINGS optimize_read_in_order=1, optimize_distinct_in_order=1, max_threads=4, max_execution_time=20`,
        { prefix: filters.prefix, after, release, limit: PAGE_SIZE + 1 },
      ),
    ),
  );
  const roots = [...new Set(batches.flat().map((row) => row.root_domain))].sort(
    (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)),
  );
  const domains = roots.slice(0, PAGE_SIZE);
  const rows = await loadDomainEvidence(domains);
  return {
    rows,
    hasMore: roots.length > PAGE_SIZE,
    next: domains.at(-1) ?? "",
  };
}

async function loadDomainEvidence(
  domains: string[],
): Promise<DomainEvidence[]> {
  if (!domains.length) return [];
  const [archived, dns, webtech, companies] = await Promise.all([
    chQuery<{ root_domain: string; count: number }>(
      `SELECT root_domain, toUInt32(uniqExact(technology)) AS count
      FROM corpscout.commoncrawl_page_technologies WHERE root_domain IN {domains:Array(String)} GROUP BY root_domain`,
      { domains },
    ),
    chQuery<{ root_domain: string; count: number }>(
      `SELECT root_domain, toUInt32(uniqExact(technology)) AS count
      FROM corpscout.domain_signal_technologies WHERE root_domain IN {domains:Array(String)} GROUP BY root_domain`,
      { domains },
    ),
    chQuery<{ root_domain: string; count: number }>(
      `SELECT root_domain, toUInt32(uniqExact(detected_name)) AS count
      FROM corpscout.webtech_domain_technologies_current WHERE root_domain IN {domains:Array(String)} GROUP BY root_domain`,
      { domains },
    ),
    chQuery<{
      root_domain: string;
      count: number;
      records: [string, string, string][];
    }>(
      `SELECT root_domain,
      toUInt32(uniqExact((country_code, company_id))) AS count,
      groupUniqArray(10)((country_code, company_id, company_id_type)) AS records
      FROM (${COMPANY_DOMAINS}) WHERE root_domain IN {domains:Array(String)} GROUP BY root_domain`,
      { domains },
    ),
  ]);
  const maps = [archived, dns, webtech].map(
    (rows) => new Map(rows.map((row) => [row.root_domain, row.count])),
  );
  const companyMap = new Map(companies.map((row) => [row.root_domain, row]));
  return domains.map((root_domain) => ({
    root_domain,
    archived: maps[0].get(root_domain) ?? 0,
    dns: maps[1].get(root_domain) ?? 0,
    webtech: maps[2].get(root_domain) ?? 0,
    companies: companyMap.get(root_domain)?.count ?? 0,
    company_records: companyMap.get(root_domain)?.records ?? [],
  }));
}

export async function listDomainSites(domain: string, after: string) {
  // A hostname is an observed site candidate, not proof of a working website.
  // The Webtech hostname is the final host, including redirects to other roots.
  const hosts = await chQuery<{ hostname: string }>(
    `SELECT DISTINCT hostname FROM (
      SELECT hostname FROM corpscout.domain_hostnames_state WHERE root_domain = {domain:String}
      UNION ALL SELECT domain(url) AS hostname FROM corpscout.commoncrawl_domains WHERE root_domain = {domain:String}
      UNION ALL SELECT final_hostname AS hostname FROM corpscout.webtech_domain_scan_results FINAL WHERE root_domain = {domain:String}
      UNION ALL SELECT website_host AS hostname FROM (${COMPANY_DOMAINS}) WHERE root_domain = {domain:String}
    ) WHERE hostname != '' AND hostname > {after:String}
    ORDER BY hostname LIMIT 26`,
    { domain, after },
  );
  const names = hosts.slice(0, PAGE_SIZE).map((row) => row.hostname);
  if (!names.length) return { sites: [], hasMore: false, next: "" };
  const [archived, webtech] = await Promise.all([
    chQuery<{ hostname: string; count: number; technologies: string[] }>(
      `SELECT domain(page_url) AS hostname,
      toUInt32(uniqExact(technology)) AS count, arraySort(groupUniqArray(20)(toString(technology))) AS technologies
      FROM corpscout.commoncrawl_page_technologies
      WHERE root_domain = {domain:String} AND domain(page_url) IN {names:Array(String)} GROUP BY hostname`,
      { domain, names },
    ),
    chQuery<{ hostname: string; count: number; technologies: string[] }>(
      `SELECT final_hostname AS hostname,
      toUInt32(uniqExact(detected_name)) AS count, arraySort(groupUniqArray(20)(detected_name)) AS technologies
      FROM corpscout.webtech_domain_technologies_current
      WHERE root_domain = {domain:String} AND final_hostname IN {names:Array(String)} GROUP BY hostname`,
      { domain, names },
    ),
  ]);
  const archiveMap = new Map(archived.map((row) => [row.hostname, row]));
  const webtechMap = new Map(webtech.map((row) => [row.hostname, row]));
  return {
    sites: names.map((hostname): DomainSite => ({
      hostname,
      archived: archiveMap.get(hostname)?.count ?? 0,
      webtech: webtechMap.get(hostname)?.count ?? 0,
      technologies:
        webtechMap.get(hostname)?.technologies ??
        archiveMap.get(hostname)?.technologies ??
        [],
    })),
    hasMore: hosts.length > PAGE_SIZE,
    next: names.at(-1) ?? "",
  };
}
