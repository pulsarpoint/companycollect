import { chQuery } from "~/lib/clickhouse.server";
import { graphDomainError, type DomainGraphSearch } from "~/lib/domain-graph";
import { clampPage, clampPageSize } from "~/lib/paging";

export interface DomainGraphRelease {
  graph_release: string;
  node_count: string;
  edge_count: string;
  published_at: string;
}

export interface DomainConnection {
  connected_domain: string;
  outgoing: number;
  incoming: number;
  reciprocal: number;
  n_hosts: number;
}

export interface DomainGraphCounts {
  all: number;
  mutual: number;
  outgoing: number;
  incoming: number;
}

export interface DomainGraphResult {
  found: boolean;
  rows: DomainConnection[];
  counts: DomainGraphCounts;
  total: number;
  page: number;
  pageSize: number;
}

// Keep interactive searches bounded, including queries against very large hubs.
const QUERY_SETTINGS = `SETTINGS use_query_condition_cache = 0,
  max_execution_time = 20, max_threads = 4, max_memory_usage = 2000000000`;
const CONNECTIONS = `corpscout.commoncrawl_domain_connections(
  graph_release = {release:String}, domain = {domain:String})`;
const DIRECTION_FILTERS = {
  all: "1",
  mutual: "reciprocal = 1",
  outgoing: "outgoing = 1 AND incoming = 0",
  incoming: "incoming = 1 AND outgoing = 0",
} as const;

export function getDomainGraphReleases(): Promise<DomainGraphRelease[]> {
  return chQuery<DomainGraphRelease>(`
    SELECT graph_release, node_count, edge_count, published_at
    FROM corpscout.commoncrawl_domain_graph_snapshots FINAL
    ORDER BY published_at DESC, graph_release DESC
    ${QUERY_SETTINGS}
  `);
}

export async function searchDomainGraph(
  search: DomainGraphSearch,
): Promise<DomainGraphResult> {
  if (!search.domain || graphDomainError(search.domain)) {
    throw new Error("A valid domain is required for graph search.");
  }
  const params = { release: search.release, domain: search.domain };
  const pageSize = clampPageSize(search.pageSize);
  const seed = await chQuery<{ node_id: number }>(
    `
    SELECT node_id FROM corpscout.commoncrawl_domain_graph_nodes
    WHERE graph_release = {release:String} AND root_domain = {domain:String}
      AND graph_release IN (
        SELECT graph_release FROM corpscout.commoncrawl_domain_graph_snapshots FINAL
      )
    LIMIT 1
    ${QUERY_SETTINGS}
  `,
    params,
  );
  if (seed.length === 0) {
    return {
      found: false,
      rows: [],
      counts: { all: 0, mutual: 0, outgoing: 0, incoming: 0 },
      total: 0,
      page: 1,
      pageSize,
    };
  }

  const [countRow] = await chQuery<{
    total: string;
    mutual: string;
    outgoing_only: string;
    incoming_only: string;
  }>(
    `
    SELECT count() AS total,
      countIf(reciprocal = 1) AS mutual,
      countIf(outgoing = 1 AND incoming = 0) AS outgoing_only,
      countIf(incoming = 1 AND outgoing = 0) AS incoming_only
    FROM ${CONNECTIONS}
    ${QUERY_SETTINGS}
  `,
    params,
  );
  const counts = {
    all: Number(countRow.total),
    mutual: Number(countRow.mutual),
    outgoing: Number(countRow.outgoing_only),
    incoming: Number(countRow.incoming_only),
  };
  const total = counts[search.direction];
  const page = Math.min(
    clampPage(search.page),
    Math.max(1, Math.ceil(total / pageSize)),
  );
  const rows =
    total === 0
      ? []
      : await chQuery<DomainConnection>(
          `
    SELECT connected_domain, outgoing, incoming, reciprocal, n_hosts
    FROM ${CONNECTIONS}
    WHERE ${DIRECTION_FILTERS[search.direction]}
    ORDER BY reciprocal DESC, connected_domain ASC
    LIMIT {limit:UInt32} OFFSET {offset:UInt64}
    ${QUERY_SETTINGS}
  `,
          { ...params, limit: pageSize, offset: (page - 1) * pageSize },
        );
  return { found: true, rows, counts, total, page, pageSize };
}
