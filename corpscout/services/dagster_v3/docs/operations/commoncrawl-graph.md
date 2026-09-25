# Common Crawl graph releases and ranking history

Implementation status (2026-09-25): tasks 1–7 are implemented locally. Production
migration, deployment, backfill, and schedule activation are task 8 and have **not**
been performed. The running system and legacy rank table are unchanged.

## Runtime configuration

| Runtime | Configuration |
| --- | --- |
| Dagster | `COMMONCRAWL_GRAPH_PG_URL`: application PostgreSQL database with migration 000126, not Dagster's internal metadata database |
| Backoffice | The same application catalog via `COMMONCRAWL_GRAPH_PG_URL`; optional `BACKOFFICE_OPERATOR` supplies the recorded operator identity |
| Dagster | Existing `CORPSCOUT_S3_ENDPOINT`, `CORPSCOUT_S3_ACCESS_KEY`, `CORPSCOUT_S3_SECRET_KEY`, and ClickHouse settings |
| Dagster | `TMPDIR` on a data volume with room for the largest compressed source file plus headroom; downloads check available space before starting |
| ClickHouse | Server-managed named collection **`commoncrawl_graph_cache`**, with `url` ending in `/commoncrawl-graphs/` and S3 read credentials |

The named collection must point to the same **commoncrawl-graphs** bucket that
Dagster writes. Its endpoint must be reachable from the ClickHouse host; a URL
reachable only from the Dagster host is insufficient. Configure `url`,
`access_key_id`, and `secret_access_key` as non-overridable values in protected
server configuration. The importer supplies `filename`, `format`, `structure`,
and `compression_method`; no credentials enter query text or run metadata.
See [ClickHouse named collections](https://clickhouse.com/docs/operations/named-collections)
for the XML configuration and S3 filename syntax.

Catalog tables use the connection's normal public schema. Grant the Dagster and
backoffice roles SELECT/INSERT/UPDATE on the four new catalog tables. The importer
also needs its existing ClickHouse staging-table, INSERT, partition replacement,
partition drop, snapshot mutation, and query-cancellation privileges. Keep admin
access at the backoffice's existing deployment boundary; mutation handlers also
reject cross-origin POSTs. Server errors do not return database credentials.

## Jobs and schedules

All assets belong to `commoncrawl_domain_graph`. Releases use the dynamic
partition set `commoncrawl_domain_graph_release`.

| Job / automation | Purpose |
| --- | --- |
| `commoncrawl_graph_discovery_job` | Fetch official catalogs, release indexes and HTTP metadata; store discoveries and register partition keys |
| `commoncrawl_domain_ranks_job` | Download/cache one rank file and publish its complete ranking partition |
| `commoncrawl_domain_graph_job` | Nine assets: source, node/edge raw files, nodes, edges, snapshot, rank raw file, ranks, active graph |
| `commoncrawl_graph_cleanup_job` | Remove retired node/edge partitions and their cached files after ten minutes; preserve every ranking partition |
| `commoncrawl_graph_discovery_schedule` | Daily at 04:17 UTC; initially stopped |
| `commoncrawl_graph_import_sensor` | Reconcile requests and dispatch at most one import at a time; initially stopped |
| `commoncrawl_graph_cleanup_schedule` | Hourly at :43 UTC; initially stopped |

First discovery records a baseline without automatically importing the entire
historical catalog. Enable automatic imports for releases discovered after that
baseline. Each refresh revisits incomplete files. Among new ready releases, only
the newest full graph is selected; the remaining releases get rankings only.
Historical backfill is a separate explicit backoffice action. Requests stay in
PostgreSQL across daemon restarts. Request UUIDs are Dagster deduplication keys.
Automatic failures get at most three attempts, separated by at least one hour;
manual/backfill failures expose a retry action. Retries preserve the pinned source
manifest. Pause stops new automatic dispatch; manual requests and running imports
continue. A request already handed to Dagster may finish after pause.

## Publication and retention

Each raw file uses an immutable key derived from URL, ETag and compressed size.
Downloads use bounded buffers, whole-stream retries, SHA-256 and complete gzip
integrity checks. Cache objects become visible after upload completion. The exact
five-column ranking format without host counts and the six-column format are
supported; omitted host counts remain NULL. Other headers fail explicitly.

Native ClickHouse readers load validated cached gzip into unique staging tables.
Ranks publish with `REPLACE PARTITION`; a serving ranking partition is therefore
the publication record. There is **no separate ranking publication table**. The
catalog request status describes execution, not data presence. If acknowledgement
is lost after publication, the complete partition remains queryable; a retry
checks source provenance and row count, reuses it, and reconciles the run status.

Existing validated graph/rank partitions can be reused even if their raw cache
objects have expired. Graph activation requires both files, the graph snapshot,
matching-release rankings, lookup projections and bounded adjacency queries.
Activation and cleanup serialize on the PostgreSQL singleton row. Backoffice can retain a previously read active pointer for up to eight minutes during a catalog outage, then fails closed; this fits within the ten-minute cleanup grace. Older runs
cannot move the active pointer backward. Backoffice resolves one active graph;
rank history is ordered by coverage dates, never load time or month-name sorting.

After activation, the previous graph is marked retired. Cleanup waits ten minutes,
skips releases with active import requests, removes the snapshot marker first,
and then drops only nodes/edges and their graph cache. Failures leave extra storage
and can be retried. The domain-inventory asset shares the graph pool so cleanup
cannot remove its input during a long scan. Raw ranking files are currently retained
as rebuildable cache; this implementation does not install a bucket lifecycle rule.

## Task 8: deployment and initial adoption

1. Read the existing migration ledgers and live graph/rank inventory before changing
   anything. PostgreSQL migration **000126** and ClickHouse migration **000447** are
   additive. Apply them through the repository migration tooling before deploying
   their consumers; verify no unrelated pending migration is included accidentally.
   Do not rewind a shared migration ledger or drop the legacy rank table.
2. Configure the catalog connections, object-store access, named collection, and
   temporary disk volume. Check free ClickHouse/object-store/disk capacity for old
   graph + new graph + staging/projections + compressed files, and for the entire
   requested ranking history. The prior live graph was 18.41 GB; this is a measured
   baseline, not an upper bound for replacement or history storage.
3. Deploy through the existing service Ansible workflow described in
   [the deployment runbook](../deployment-runbook.md). Leave the new schedules and
   sensor stopped. Do not start a local daemon using the deployed Dagster database.
4. Run **discovery** once on the deployed Dagster instance. Verify the official
   source identities and coverage dates. Keep automatic imports disabled.
5. Bootstrap the existing published graph before importing a replacement. The last
   read-only audit found `cc-main-2026-jun-jul-aug`. Recheck that value, then manually
   launch `commoncrawl_domain_graph_job` for that existing partition in Dagster.
   Existing verified nodes/edges are reused without re-downloading them; the matching
   canonical rankings are imported and the active pointer is initialized. If its
   manifest/counts disagree, investigate instead of forcing activation. If more
   legacy graph snapshots exist, inventory them explicitly before retirement.
6. After the initial graph is active, set the backoffice catalog connection and
   deploy the backoffice consumer change. Until configured, the old graph/rank read
   paths remain available. The technology rollup uses legacy signals only while no
   canonical ranking partition exists. The domain inventory requires an active
   graph after this deployment, so bootstrap before scheduling that asset again.
7. Start the request sensor. Import the newest full graph through backoffice.
   Verify the active pointer changes, adjacency results work, and matching-release
   rankings exist. Start the cleanup schedule and verify the old graph disappears
   after grace while both ranking releases remain available.
8. Import one cross-year and one older supported ranking release. The legacy label
   `CC-MAIN-2026-apr-may-jun` maps by case to the official lowercase release; reimport
   canonical data instead of copying legacy rows of unknown completeness. Unmapped
   labels remain visibly legacy. Unsupported headers need an explicit adapter.
9. Queue **Import missing rankings**. Monitor one release per run, storage growth,
   invalid-file failures, and the loaded-release inventory. Enable daily discovery
   and automatic imports after the pilot passes. Enablement in backoffice and the
   Dagster sensor/schedule states are separate controls and both are displayed.
10. Retire the manual rank loader operationally after consumers and canonical
    history are verified. Keep the legacy table during compatibility; its eventual
    removal requires a separate migration after all legacy reads are removed.

Useful read-only checks (no rank-table scan required):

```sql
-- ClickHouse: published ranking releases and compressed storage.
SELECT partition AS graph_release, sum(rows) AS rows,
       sum(bytes_on_disk) AS bytes_on_disk
FROM system.parts
WHERE database='corpscout' AND table='commoncrawl_domain_graph_ranks' AND active
GROUP BY partition;

-- ClickHouse: remaining full graph partitions.
SELECT table, partition AS graph_release, sum(rows) AS rows, sum(bytes_on_disk) AS bytes_on_disk
FROM system.parts
WHERE database='corpscout'
  AND table IN ('commoncrawl_domain_graph_nodes','commoncrawl_domain_graph_edges') AND active
GROUP BY table, partition;

-- PostgreSQL: active graph and pending/failed requests.
SELECT active_graph_release, automatic_imports_enabled, discovery_succeeded_at, discovery_error
FROM commoncrawl_graph_state;
SELECT graph_release, selection, status, dagster_run_id, last_error
FROM commoncrawl_graph_import_requests
WHERE status <> 'succeeded' ORDER BY created_at;
```

Roll back operations by disabling automatic imports and stopping the new schedules;
allow running imports to finish or cancel them in Dagster. Do not drop new ranking
partitions as a rollback mechanism. A failed replacement leaves the previous active
graph available; after successful retirement its full graph must be reimported to
restore historical adjacency. Ranking history is unaffected.
