# Common Crawl domain graph and ranking history

Updated 2026-09-25. The release catalog, ranking ingestion, safe active-graph switch,
and latest-only graph retention supersede the original manual-import workflow below.
All historical rankings are retained; only the active full graph remains after cleanup.

The operational contract, configuration and rollout are documented in
[Common Crawl graph operations](../../../../../docs/operations/commoncrawl-graph.md).
The Dagster group remains `commoncrawl_domain_graph`; the full import now selects
nine partitioned assets. Rankings have a separate two-asset job in the same group.
Daily discovery and hourly cleanup are unpartitioned jobs. All new automation is
initially stopped. PostgreSQL owns discovered releases, pinned request manifests,
and the current graph pointer; ClickHouse owns bulk data. Complete rank partitions
are their own publication record.

## Source and asset shape

The source is the Common Crawl **pay-level domain graph**, not the host graph.
Node IDs belong to one graph release; every relationship includes `graph_release`.
The initial audit of `cc-main-2026-jun-jul-aug` found 119,722,885 nodes and
2,450,405,793 directed edges. New releases use their own published counts.

```text
commoncrawl_graph_release_catalog                 (unpartitioned)

commoncrawl_domain_graph_source
  ├── commoncrawl_domain_graph_nodes_raw → commoncrawl_domain_graph_nodes
  └── commoncrawl_domain_graph_edges_raw → commoncrawl_domain_graph_edges
                                                   ↓ both validated
                                      commoncrawl_domain_graph_snapshots
commoncrawl_domain_graph_ranks_raw → commoncrawl_domain_graph_ranks
                                                   ↓ both published
                                      commoncrawl_domain_graph_active

commoncrawl_domain_graph_cleanup                  (unpartitioned)
```

Discovery registers exact official IDs in the dynamic partition set. Source and
rank raw assets pin one immutable catalog manifest per import run. A ranks-only
job does not require graph files. Backoffice queues durable requests; the Dagster
sensor selects the full or rank job. Both use one partition per run. A manual
Dagster full import requires discovery first and uses empty run configuration.

## Storage and ingestion decisions

Migration `000418_corpscout_commoncrawl_domain_graph` owns three tables and a view:

| Object | Grain and access |
| --- | --- |
| `commoncrawl_domain_graph_nodes` | `(graph_release, node_id)`; domain-name sort order plus `by_node_id_lookup` projection |
| `commoncrawl_domain_graph_edges` | `(graph_release, source_node_id, target_node_id)`; outgoing sort order plus `by_target` projection for incoming links |
| `commoncrawl_domain_graph_snapshots` | One published record per complete release, read with `FINAL` |
| `commoncrawl_domain_connections` | Parameterized view returning connected domains and direction flags |

Verified cached gzip TSV is parsed by ClickHouse's native `s3()` reader through the `commoncrawl_graph_cache` named collection. The direct `url()` helper remains for isolated legacy-path regression tests. The vertices contain
`node_id`, reversed domain labels, and host count; SQL reverses the labels into the
normal domain name. Edges contain two UInt32 IDs. Both native tables are partitioned
by immutable release. Projections store both access orders and are maintained by
ClickHouse during inserts and merges. Migration `000419_corpscout_domain_graph_lookup_index`
replaces the original node-ID projection with a lookup projection containing only
release, node ID, domain name and host count. Its 128-row index granules avoid
reading thousands of unrelated names for each scattered neighbor ID. The migration
materializes existing data before removing the old projection, and future imports
inherit the projection through their cloned staging-table DDL. See ClickHouse's
[projection settings](https://clickhouse.com/docs/reference/statements/alter/projection#with-settings).

On a populated graph, materializing this projection can take many minutes and
wait for existing merges. Set `read_timeout=1800` (seconds) in the migration
connection URL, including when `.env` overrides the Makefile default. If the client
disconnects, inspect `system.mutations` before retrying: ClickHouse keeps the
materialization running. Complete and verify all remaining migration statements
before marking that migration version clean; do not rewind the shared ledger.

**Documented deviations from the normal DuckDB pipeline:** this is a direct bulk
file-to-ClickHouse load, with no local analytical transform or company filtering.
Staging billions of edges in DuckDB and then exporting Python rows would duplicate
storage and serialization. ClickHouse itself provides native parsing, bounded
blocks, sorting, compression and staging validation. Unlike a rolling full-refresh
register, each named graph release has a different node-ID namespace, and replacement temporarily retains two releases.
Both Dagster and ClickHouse are therefore partitioned by immutable graph release.
Backfills use one release per run. This also isolates the source IO-manager output,
which would otherwise be overwritten by simultaneous releases of an unpartitioned asset.

No monetary values, translatable labels, or contact records exist in this source.
Domain names are identifiers, not text to translate. PostgreSQL stores small release/file/request records, never per-edge progress. Dagster tracks file steps; the object store caches raw gzip; ClickHouse owns bulk staging and publication.

## Retry and publication contract

- Each request includes `If-Match` with the resolved ETag. Changed files fail instead
  of mixing data from different source versions.
- Each file loads into a unique staging table cloned from migration-owned DDL.
- Nodes must match the published count, have unique contiguous IDs from zero,
  nonempty domain names, and the expected ETag.
- Edges must match the published edge and self-loop counts, reference IDs within
  the node range, and carry the expected ETag.
- Only a validated, nonempty release partition replaces its destination partition.
  Other graph releases remain intact. A failed insert cannot publish partial edges.
- The snapshot marker is written last, after both tables validate. The connections
  view excludes releases without this marker.
- Successful files can be reused after their sibling fails. Completed releases
  reuse their data without downloading again, checking counts against the immutable
  published metadata. A changed ETag for a published release is rejected.
- The shared Dagster pool serializes writers and publication. Imports use four
  query threads, two insert threads, eight bounded download buffers and an 8 GB query memory
  limit. Progress is logged every 30 seconds. File assets retry twice.
- Cancelled or failed imports cancel their own server query and drop their own
  staging table. A cleanup failure logs the staging table name for investigation.

Publication of the two physical tables is not a cross-table transaction. Release
identity plus the final snapshot marker provides the reader contract. Consumers resolve the PostgreSQL active pointer once, then use that release for every node/edge query. The snapshot marker is an additional completeness guard, not the current-release selector.

## SQL usage

Graph snapshot inventory (publication time is ingestion time; do not use this ordering to choose the active graph):

```sql
SELECT graph_release, node_count, edge_count, published_at
FROM corpscout.commoncrawl_domain_graph_snapshots FINAL
ORDER BY published_at DESC;
```

All domains directly connected in either direction:

```sql
SELECT connected_domain, outgoing, incoming, reciprocal, n_hosts
FROM corpscout.commoncrawl_domain_connections(
    graph_release = 'cc-main-2026-jun-jul-aug',
    domain = 'assaabloy.com'
)
ORDER BY reciprocal DESC, connected_domain
SETTINGS use_query_condition_cache = 0;
```

Add `WHERE reciprocal = 1` for domains with both direct `A → B` and `B → A` edges.
Add `WHERE outgoing = 1` or `WHERE incoming = 1` for one direction. Self-links are
excluded from this related-domain view. A missing domain or unpublished release
returns no rows; no rows does not prove the absence of a relationship on the live web.
Pass the normalized pay-level domain, without scheme, path, or `www`.

The view restricts adjacency scans to the seed IDs before resolving neighbor names.
This avoids joining the whole graph just to answer one-domain queries. Verify the
incoming `by_target` and node `by_node_id_lookup` projections with `EXPLAIN`/query logs
after a full import. Broad hub domains naturally return much larger neighborhoods.

## Backoffice graph explorer

Workspace → Graph (`/admin/graph`) searches the active full graph by domain. It also manages discovered releases and ranking imports. Pasted website URLs are normalized to lowercase hostnames, with `www`
removed. Other subdomains are not guessed into registrable domains; use the
pay-level domain from the source graph, including suffixes such as `.co.uk`.

Each connected domain shows outgoing and incoming flags separately. Mutual links
require both direct edges. Filters select all neighbors, mutual links, outgoing-only
or incoming-only links. Mutual results appear first, then domains sort alphabetically.
Clicking a neighbor starts a new graph search in the same release. The evidence
link opens the existing Common Crawl domain page for further investigation.

Search state and pagination live in the URL. Queries use bound parameters, server-side
paging (at most 200 rows), and a 20-second per-query limit. The release picker
shows the active graph and pending full imports. Retired links explain retention.
Active imports refresh every 10 seconds while the page is visible. A Dagster
outage does not block published searches. During a catalog outage, a previously
read active pointer can be used for up to eight minutes, bounded below the
retirement grace period; after that the reader fails closed. This avoids guessing
which snapshot became active. Absent domains, isolated domains, empty direction
filters and query failures remain distinct UI states.

The UI resolves the searched domain once, then computes direction counts using
numeric adjacency IDs only. It applies the chosen direction filter before joining
neighbor IDs to domain names. Counts therefore do not scan the node-name lookup
projection, and a mutual-only search resolves names only for reciprocal neighbors.
Alphabetical ordering within reciprocal/nonreciprocal results remains unchanged.
For "All connections", a page entirely within the mutual or one-way group resolves
names only for that group. Pages crossing the boundary retain both groups; pages
after the boundary subtract the mutual count from their SQL offset.

From `services/backoffice`, run `npm run typecheck`, `npm run build`, and
`npx vitest run tests/domain-graph.server.test.ts tests/admin-graph.test.tsx`.
`VITEST_LIVE=1 npx vitest run tests/domain-graph.live.test.ts` starts and removes
an isolated ClickHouse container; it does not access production. That test exercises
the actual UI SQL against reciprocal links, one-way links, longer cycles, self-links,
isolated domains, and two releases that reuse the same numeric node IDs.

## Enrichment handoff

Join related domains to existing industry classifications to prioritize discovery,
retaining classification scores, confidence and observation dates. Industry mismatch
must not discard a candidate automatically. Crawl each unique candidate domain once
and attach its evidence to every relevant graph pair. Preserve graph release,
original company/domain, link directions, crawl time, source page URL, and quoted
evidence. The existing company-research crawler already has company-profile,
products/services and explicit corporate/commercial relationship objectives.

This graph pipeline does not automatically associate candidates with a company or
replace verified primary domains. Ownership, brands, partners, suppliers and customers
are separate relationship types established by downstream evidence.

## Verification

`tests/test_commoncrawl_domain_graph.py` covers source validation and DDL contracts.
`tests/test_commoncrawl_domain_graph_integration.py` uses an actual ClickHouse 26.5
container and a gzip HTTP fixture. It exercises the production loader, one-way and
reciprocal queries, longer cycles, self-links, missing domains, incomplete release
visibility, retry reuse, changed ETags, incorrect counts and staging cleanup.

Run the source tests, migration contract tests, and `uv run dg check defs` before
deploying. Deploy migrations before code, then materialize the job and verify the
published counts against Common Crawl's `.stats` file.
