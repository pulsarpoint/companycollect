# Common Crawl domain graph

This pipeline loads the complete global directed domain graph into ClickHouse.
It supports domain discovery for every country, independently of company selection,
Brave processing, and crawler enrichment. A graph edge means a captured hyperlink,
not common ownership or even the same industry.

## Source and asset flow

Source: [Common Crawl June–August 2026 domain graph](https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2026-jun-jul-aug/index.html).
The release contains 119,722,885 pay-level domain nodes and 2,450,405,793 directed
edges. Vertices are 893,039,383 compressed bytes; edges are 9,425,755,189 bytes.
The graph uses Common Crawl's public-suffix normalization. Node IDs are stable
within one release only, so every lookup and relationship includes `graph_release`.

Group: `commoncrawl_domain_graph`. Job: `commoncrawl_domain_graph_job`.

```text
commoncrawl_domain_graph_source
  ├── commoncrawl_domain_graph_nodes
  └── commoncrawl_domain_graph_edges
          ↓ both validated
commoncrawl_domain_graph_snapshots
          ↓ query view
commoncrawl_domain_connections(graph_release, domain)
```

The source asset uses dlt's retrying HTTP session to read the release index,
published statistics and HTTP file metadata. Dagster persists its typed output
through the IO manager; the completed snapshot table retains the URLs, ETags,
counts and publishing run ID. Release is the Dagster dynamic partition key, using
`commoncrawl_domain_graph_release`. Add `cc-main-2026-jun-jul-aug` in the partition
picker, select it, and materialize the job. No additional run configuration is needed:

```yaml
{}
```

No schedule is enabled. New releases are selected explicitly and retained alongside
existing releases. The job selects all upstream assets. Each release has separate
Dagster input storage and step progress, and every downstream asset checks its input
release against its materialization partition. Concurrent runs for different
releases cannot overwrite each other's source metadata.

## Storage and ingestion decisions

Migration `000418_corpscout_commoncrawl_domain_graph` owns three tables and a view:

| Object | Grain and access |
| --- | --- |
| `commoncrawl_domain_graph_nodes` | `(graph_release, node_id)`; domain-name sort order plus `by_node_id` projection |
| `commoncrawl_domain_graph_edges` | `(graph_release, source_node_id, target_node_id)`; outgoing sort order plus `by_target` projection for incoming links |
| `commoncrawl_domain_graph_snapshots` | One published record per complete release, read with `FINAL` |
| `commoncrawl_domain_connections` | Parameterized view returning connected domains and direction flags |

Raw gzip TSV is parsed by ClickHouse's native `url()` reader. The vertices contain
`node_id`, reversed domain labels, and host count; SQL reverses the labels into the
normal domain name. Edges contain two UInt32 IDs. Both native tables are partitioned
by immutable release. Projections store both access orders and are maintained by
ClickHouse during inserts and merges.

**Documented deviations from the normal DuckDB pipeline:** this is a direct bulk
file-to-ClickHouse load, with no local analytical transform or company filtering.
Staging billions of edges in DuckDB and then exporting Python rows would duplicate
storage and serialization. ClickHouse itself provides native parsing, bounded
blocks, sorting, compression and staging validation. Unlike a rolling full-refresh
register, each named graph release is retained and has a different node-ID namespace.
Both Dagster and ClickHouse are therefore partitioned by immutable graph release.
Backfills use one release per run. This also isolates the source IO-manager output,
which would otherwise be overwritten by simultaneous releases of an unpartitioned asset.

No monetary values, translatable labels, or contact records exist in this source.
Domain names are identifiers, not text to translate. No S3 result queue or PostgreSQL
per-edge progress records are needed: Dagster tracks file steps, while ClickHouse
owns bulk staging and publication.

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
identity plus the final snapshot marker provides the reader contract. Consumers
that query base tables directly must first check the snapshot marker themselves.

## SQL usage

Published releases, most recently published first:

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
incoming `by_target` and node `by_node_id` projections with `EXPLAIN`/query logs
after a full import. Broad hub domains naturally return much larger neighborhoods.

## Backoffice graph explorer

Workspace → Graph (`/admin/graph`) searches this view by domain and published
release. Pasted website URLs are normalized to lowercase hostnames, with `www`
removed. Other subdomains are not guessed into registrable domains; use the
pay-level domain from the source graph, including suffixes such as `.co.uk`.

Each connected domain shows outgoing and incoming flags separately. Mutual links
require both direct edges. Filters select all neighbors, mutual links, outgoing-only
or incoming-only links. Mutual results appear first, then domains sort alphabetically.
Clicking a neighbor starts a new graph search in the same release. The evidence
link opens the existing Common Crawl domain page for further investigation.

Search state and pagination live in the URL. Queries use bound parameters, server-side
paging (at most 200 rows), and a 20-second per-query limit. The release picker only
lists completed snapshot markers. Unpublished graphs, absent domains, isolated domains,
empty direction filters, and query failures have distinct UI states.

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
