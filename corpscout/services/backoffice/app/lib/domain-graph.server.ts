import { chQuery } from "~/lib/clickhouse.server";
import { graphDomainError, type DomainGraphSearch } from "~/lib/domain-graph";
import { clampPage, clampPageSize } from "~/lib/paging";
import { dagsterRunUrl, listRuns } from "~/lib/dagster.server";

export interface DomainGraphImport {
  graph_release: string;
  label: string;
  active: boolean;
  run_url: string | null;
}

export async function getDomainGraphImports(): Promise<DomainGraphImport[]> {
  const runs = await listRuns(
    { job: "commoncrawl_domain_graph_job", limit: 100 },
    { timeoutMs: 5000 },
  );
  const imports = new Map<string, DomainGraphImport>();
  // Dagster returns newest runs first; a retry supersedes the previous attempt.
  for (const run of runs) {
    const graph_release = run.tags["dagster/partition"];
    if (!graph_release || imports.has(graph_release)) continue;
    const active = !["FAILURE", "CANCELED"].includes(run.status);
    const label =
      run.status === "FAILURE"
        ? "Import failed"
        : run.status === "CANCELED"
          ? "Import canceled"
          : run.status === "SUCCESS"
            ? "Awaiting publication"
            : ["QUEUED", "NOT_STARTED"].includes(run.status)
              ? "Queued"
              : "Importing";
    imports.set(graph_release, {
      graph_release,
      label,
      active,
      run_url: dagsterRunUrl(run.runId),
    });
  }
  return [...imports.values()];
}

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
// Count and filter on numeric IDs before reading any neighbor names. Both edge
// directions use their existing sorted indexes; self-links are not neighbors.
const ADJACENCY = `
  SELECT node_id, max(outgoing_link) AS outgoing, max(incoming_link) AS incoming,
    toUInt8(outgoing AND incoming) AS reciprocal
  FROM (
    SELECT target_node_id AS node_id, toUInt8(1) AS outgoing_link, toUInt8(0) AS incoming_link
    FROM corpscout.commoncrawl_domain_graph_edges
    WHERE graph_release = {release:String} AND source_node_id = {seed_node_id:UInt32}
    UNION ALL
    SELECT source_node_id AS node_id, toUInt8(0) AS outgoing_link, toUInt8(1) AS incoming_link
    FROM corpscout.commoncrawl_domain_graph_edges
    WHERE graph_release = {release:String} AND target_node_id = {seed_node_id:UInt32}
  )
  WHERE node_id != {seed_node_id:UInt32}
  GROUP BY node_id
`;
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
  const graphParams = {
    release: search.release,
    seed_node_id: seed[0].node_id,
  };

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
    FROM (${ADJACENCY})
    ${QUERY_SETTINGS}
  `,
    graphParams,
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
  let offset = (page - 1) * pageSize;
  let directionFilter: string = DIRECTION_FILTERS[search.direction];
  // Mutual links sort first. Resolve names only for the group on this page.
  if (search.direction === "all") {
    if (Math.min(offset + pageSize, total) <= counts.mutual) {
      directionFilter = "reciprocal = 1";
    } else if (offset >= counts.mutual) {
      directionFilter = "reciprocal = 0";
      offset -= counts.mutual;
    }
  }
  const rows =
    total === 0
      ? []
      : await chQuery<DomainConnection>(
          `
    WITH connections AS (
      SELECT * FROM (${ADJACENCY}) WHERE ${directionFilter}
    )
    SELECT n.root_domain AS connected_domain, c.outgoing, c.incoming, c.reciprocal, n.n_hosts
    FROM (
      SELECT node_id, root_domain, n_hosts
      FROM corpscout.commoncrawl_domain_graph_nodes
      WHERE graph_release = {release:String} AND node_id IN (SELECT node_id FROM connections)
    ) AS n
    INNER JOIN connections AS c USING (node_id)
    ORDER BY reciprocal DESC, connected_domain ASC
    LIMIT {limit:UInt32} OFFSET {offset:UInt64}
    ${QUERY_SETTINGS}
  `,
          { ...graphParams, limit: pageSize, offset },
        );
  return { found: true, rows, counts, total, page, pageSize };
}
