# Common Crawl graph ingestion and ranking history

Date: 2026-09-25. Status: tasks 1–7 implemented and validated. Task 8 is underway: production migrations and Dagster deployment are complete, daily discovery and request dispatch are enabled, and the existing graph is adopted with 119,722,885 matching ranking rows. See the operational handoff below for remaining rollout work.

## Outcome and scope

Extend the existing `commoncrawl_domain_graph` Dagster asset group to discover published Common Crawl graph releases, import a selected missing release from backoffice, and retain complete domain-level harmonic centrality and PageRank history. Import every published domain ranking row, not only tracked companies or a top-N subset. Domain lookups should show changes across releases without needing to ingest that domain retrospectively.

Default scope is the **domain/PLD graph**. Host rankings are a distinct dataset: `www.example.com` and `shop.example.com` are hosts; Common Crawl's domain graph aggregates hosts using its public suffix rules. Do not label host rankings as domain rankings or combine their positions. Host history can be added later as an explicitly separate dataset in the same group if required.

Use an initial historical ranking backfill plus daily discovery and automatic import of newly available releases, with backoffice retaining specific-version ranking import and retry controls. Both paths use the same partitioned importer. Keep all available domain ranking history and only the latest validated full graph; do not routinely backfill historical edge graphs. Enable the daily policy during rollout after validation, not while implementing this plan.

## How unknown releases become partitions

Dynamic partitions do not require a predefined release list. The definition declares a named set (`commoncrawl_domain_graph_release`); Dagster stores the discovered keys and allows new keys to be registered at runtime, without changing code or redeploying.

The initial catalog refresh discovers all currently published releases and registers their IDs. Backoffice's historical backfill action selects the missing supported versions and queues one bounded import run per release. Each day, a scheduled catalog refresh discovers new IDs and newly available artifacts. The request sensor registers any missing partition keys before launching their import runs. Registering a key means the release is known; it does not mean its data has been downloaded or successfully published.

Thus there are two responsibilities: an unpartitioned catalog asset refreshed daily, and the existing ingestion assets partitioned dynamically by release. Historical and future releases go through the same download, parse and validation steps. Daily discovery must also revisit incomplete releases and retryable failures under the retry policy; checking only whether an ID has ever been seen would lose failed imports. Backoffice can pause automatic imports while retaining catalog discovery and manual version selection.

## What exists now

The implementation audit below comes from the checkout and bounded upstream HTTP checks. A subsequent read-only live ClickHouse inventory is recorded in the retention section.

| Area | Verified current behavior |
| --- | --- |
| Rank importer | `services/cc-processor/tools/load-domain-ranks.sh` imports an already downloaded and decompressed **domain-level** rank file. It does not discover or download releases. |
| Rank fields | Stores harmonic score, harmonic position, PageRank score, PageRank position and host count. `host_rev` is the source column name even for domain records; it does not imply host-level data. |
| Rank storage | Migration `000073` creates `commoncrawl_domain_graph_signals`, keyed by `(root_domain, crawl_id)` with `ReplacingMergeTree(resolved_at)`. It can retain multiple releases but has no release partitions, file manifest or completion marker. The script accepts an arbitrary `crawl_id`; its example uses uppercase `CC-MAIN-...`. |
| Graph importer | `defs/commoncrawl_domain_graph/{assets,source,load}.py` already defines source, nodes, edges and snapshot assets. The job imports **domain-level** vertices and edges via ClickHouse's native HTTP reader. |
| Existing safeguards | Dynamic release partitions, one partition per backfill run, shared writer pool, ETag checks, validated staging tables, partition replacement, retry reuse and a final graph publication marker. No separate retained raw-download checkpoint. |
| Backoffice graph | `/admin/graph` lists published releases and recent Dagster attempts and searches graph connections. It has no import action or upstream release catalog. |
| Backoffice ranks | `web-intelligence.server.ts` reads up to 24 raw rows and deduplicates afterward, sorting `crawl_id` strings lexically. This is not a complete, chronologically ordered history. |
| Other consumers | Webtech accepts the signals table as scan input. `technology_catalog/assets.py` chooses ranking rows using import time (`resolved_at`); a historical backfill can therefore make an older graph appear current. |

`uv run dg list defs --assets 'group:commoncrawl_domain_graph' --json` confirmed the four graph assets. Rank ingestion is outside this group today.

## Upstream contract verified on 2026-09-25

The official [graph catalog](https://index.commoncrawl.org/graphinfo.json) listed 54 releases. The newest was `cc-main-2026-jul-aug-sep`: 133,241,980 domain nodes and 2,147,955,520 domain edges. The oldest catalog entry was host-only, so "all history" means all releases that actually publish the requested dataset.

The [Common Crawl graph documentation](https://commoncrawl.org/web-graphs) explains graph levels and links to the catalog. Its [statistics documentation](https://commoncrawl.github.io/cc-webgraph-statistics/) gives the rank file URL convention. HEAD and small streamed samples of the newest files confirmed:

| Dataset | Compressed bytes | Header |
| --- | ---: | --- |
| Domain ranks | 2,515,862,336 | `#harmonicc_pos`, `#harmonicc_val`, `#pr_pos`, `#pr_val`, `#host_rev`, `#n_hosts` |
| Host ranks | 4,845,362,014 | Same first five columns, without `#n_hosts` |

Positions in the sampled current files start at 1. Preserve published positions and scores; do not recompute ranks from row order. Historical formats must be inspected and classified before backfill; unsupported formats are visible failures, not silently skipped records.

The existing release regex rejects real cross-year releases such as `cc-main-2025-26-dec-jan-feb`. Use catalog membership plus safe identifier validation rather than inventing all historical names. Resolve actual download links from each release index; do not assume every old release has today's paths and artifacts.

## One asset group, release partitions, independent checkpoints

Keep group `commoncrawl_domain_graph`, existing asset keys and dynamic partition definition `commoncrawl_domain_graph_release`. Graph releases are not ordinary calendar-month partitions or WARC crawl IDs. One graph release can cover several crawls.

Proposed assets, all in this group:

```text
commoncrawl_graph_release_catalog                 [unpartitioned catalog refresh]

commoncrawl_domain_graph_source                   [one graph_release]
  ├─ commoncrawl_domain_graph_nodes_raw → commoncrawl_domain_graph_nodes ─┐
  ├─ commoncrawl_domain_graph_edges_raw → commoncrawl_domain_graph_edges ─┴─ commoncrawl_domain_graph_snapshots

commoncrawl_domain_graph_ranks_raw → commoncrawl_domain_graph_ranks

commoncrawl_domain_graph_snapshots + commoncrawl_domain_graph_ranks
  → commoncrawl_domain_graph_active

commoncrawl_domain_graph_cleanup                  [unpartitioned cleanup]
```

The catalog supplies valid partition keys. Both import roots pin the selected release's manifest in the catalog; the graph source asset additionally reads graph statistics; it must not depend on importing other releases. It records artifact capabilities independently: missing edges must not block a supported ranks-only import. The unpartitioned catalog refresh has its own small job and is excluded from the partitioned import selections.

Extend `commoncrawl_domain_graph_job` to select both publication branches and all their upstream partitioned assets. Add `commoncrawl_domain_ranks_job` selecting only the rank branch for historical backfill or rank repair. These are two execution selections within one asset group. Keep graph and ranks publication independent so existing searchable graphs remain available while ranks are added. A release is fully imported only when both requested datasets are published.

Extend the existing Python asset implementation rather than introducing a second Common Crawl integration or replacing its asset keys. Continue the documented native bulk-to-ClickHouse approach; avoid Python objects per row and a duplicate multi-billion-edge DuckDB staging copy.

## Download, parse and publication contract

1. Resolve artifact URL, graph level, schema version, expected row count where published, source ETag, byte size and source index URL. Check availability before accepting an import.
2. Cache each gzip separately in shared object storage through the existing object-store resource. Use a release/artifact/source-revision path, temporary upload and a final manifest. Streaming uses dlt's retrying HTTP session, bounded disk/memory and whole-stream retries or verified Range resume. No multi-GB in-memory buffers.
3. Validate response length and gzip integrity before marking an artifact downloaded. Record a computed checksum; ETag is an opaque source validator, not necessarily an MD5 checksum. A partial upload is not a downloaded artifact.
4. Native ClickHouse parsing reads the committed cache object into a run-specific staging table. Verify the chosen object store is reachable from the ClickHouse host and use server-side credentials, never exposed in backoffice or Dagster metadata.
5. Validate before `REPLACE PARTITION`: release matches, positive expected count, strict types, nonempty unique domain identifiers, finite nonnegative scores, valid rank bounds and the correct manifest identity. Rank coverage is checked against the corresponding domain node count when the release guarantees full coverage; historical exceptions require an explicit format adapter. Preserve published tie semantics rather than requiring unique/contiguous ranks blindly.
6. Publish rankings with one atomic `REPLACE PARTITION` from the validated, nonempty staging table. Never stream or append incomplete imports into the serving ranking table. Under this enforced write contract, a release present in the serving table is ready to query; no separate ranking-publication table or asset is needed. Dagster materializes the rank asset after replacement succeeds. Graphs retain their existing completion marker because nodes and edges are two independently loaded tables.
7. Reuse a validated loaded file or committed raw object on retry. An edge failure must not re-download ranks or nodes. Unpublished successful stages carry their source identity so retries cannot mix revisions. Reject upstream changes to already published releases; repair uses a deliberate revision/rebuild procedure.

Retain all parsed rank history. Raw gzip is a rebuildable cache with an explicit lifecycle; expose `cached`, `loaded`, and `published` separately so cache expiry does not make a completed release look missing. Adopt verified existing node/edge partitions without requiring their raw caches to be recreated.

## Full graph retention: latest validated release

Recommended policy after the storage discussion: retain every ranking release, but retain only the latest validated nodes/edges release in steady state. Historical graphs support investigations of gained/lost links and historical neighborhoods, but are not required to show harmonic centrality or PageRank history. Do not calculate new ranks locally; preserve Common Crawl's published rank files.

A follow-up live read on 2026-09-25 found 18.41 GB of nodes/edges including projections for `cc-main-2026-jun-jul-aug`, and 3.53 GB of ranks for `CC-MAIN-2026-apr-may-jun`. Those are two different releases. These are current measurements, not fixed future release-size estimates.

Replacement protocol:

1. Keep serving the current graph while the next release downloads and loads into its own partitions. Keep release partitioning even when only one release will be retained.
2. Validate nodes, edges, projections and publication, then run representative incoming/outgoing graph queries. Require matching-release ranks to be published before activating the new release in the combined graph/ranking experience.
3. Atomically switch one authoritative current-graph release pointer, used by all graph consumers. Resolve the pointer once per request so a query cannot mix node and edge releases. Serialize activation and cleanup across releases; an older import finishing later cannot become current or delete a newer graph.
4. After a short bounded grace period for in-flight readers, retire the previous graph's availability marker and remove its nodes/edges partitions and raw graph cache. Keep release/source metadata and all ranking data. Make activation and retirement separate retryable steps: an import or validation failure leaves the current graph untouched; cleanup failure leaves temporary extra storage and retries safely.
5. Distinguish `graph retired by retention policy` from `graph missing/failed`. Update existing snapshot-gated readers so retired releases are never advertised as queryable. Old backoffice links should explain retirement. Discovery must not automatically reimport retired historical graphs. Mark the corresponding Dagster graph partitions as intentionally retired in operator-facing status; an old materialization event is not evidence that retained data still exists.

Budget temporary space for both old and new graph releases, their staging/projections and raw cache during replacement. The target is one full graph after successful cleanup, not deleting the old graph before there is a tested replacement. The tradeoff is loss of historical edge-level analysis unless old graphs are explicitly reimported later from upstream.

Keep the existing writer pool and bounded ClickHouse settings initially. Use one release per run and bounded backfill concurrency. Capacity estimates must include compressed cache, parsing stage, destination partitions and graph lookup projections; raw HTTP sizes alone are insufficient.

## Storage and release chronology

Add a migration-owned `commoncrawl_domain_graph_ranks` table:

- Grain: `(graph_release, root_domain)`; `PARTITION BY graph_release`, sort by `(root_domain, graph_release)` for one-domain history across partitions.
- Fields: `graph_release`, `root_domain`, the existing four `cc_*` score/rank fields, `n_hosts`, `source_run_id`, `loaded_at`. Preserve scores as Float64 and use UInt64 for rank positions. Optional historical omissions are nullable rather than fabricated zeroes.
- Put file URLs, checksums, ETags, schema/parser versions and row counts in the release/artifact manifest, avoiding large per-row provenance duplication.
- Do not add a ranking-publication table. Obtain loaded releases with `GROUP BY graph_release` (and row counts if needed); the dashboard can use active partition metadata to avoid scanning all ranking rows. Existing graph snapshots keep their current meaning.

The serving ranking table is the source of truth for loaded ranks. PostgreSQL still holds discovered-but-not-loaded releases, artifact provenance and import attempts. If the process dies after ClickHouse replaces the partition but before PostgreSQL/Dagster records success, reconcile the attempt from the serving partition, expected count and stored source run ID. Do not declare data absent based only on a stale request status, or publish legacy rows without validating them first. Retry/rebuild logic must preserve the validated-only write contract for this simplification to remain correct.

ClickHouse documents [`REPLACE PARTITION` as atomic](https://clickhouse.com/docs/reference/statements/alter/partition#replace-partition). The rank asset owns staging, validation and this single publication operation.

Persist a small release catalog and import-request ledger in the existing application PostgreSQL database (separate from Dagster's internal tables). The catalog contains exact release ID, source index, constituent crawl IDs, per-artifact availability/counts/sizes, first-seen time and explicit chronology. Derive `coverage_start`/`coverage_end` from constituent crawl metadata; source publication time remains nullable if unavailable. Never substitute download time for coverage time or sort month-name IDs lexically.

Import requests store request ID, release, selection (`full` or `ranks`), origin, attempt, status, Dagster run ID, requester and timestamps. Enforce at most one active request per domain release, even across overlapping full/ranks requests. The ledger records requests and run association; serving ranking partitions and graph completion markers remain authoritative for data availability.

Migration and compatibility:

1. Inventory existing graph snapshots and rank release labels with read-only production queries during implementation. Do not assume the manual script example proves which releases were loaded.
2. Map legacy `crawl_id` values to official graph release IDs explicitly, including capitalization and any aliases. Quarantine unmappable labels for review; do not pretend an ordinary crawl ID is a graph release.
3. Prefer canonical reimport for old rank rows whose completeness/provenance cannot be established. Keep the legacy table readable until replacements are validated.
4. Update the authority UI, webtech selectors and technology ranking rollups to the new published rank relation. Preserve the legacy relation as a compatibility source during rollout, then retire the manual loader. Avoid dual writes.
5. Define "latest" as the latest published graph by coverage chronology. In latest-release views, a domain absent in that graph has no current rank; last-known rank is an explicitly labeled separate value. Historical imports must not change the latest-release selection.

## Discovery and backoffice trigger

Refresh the lightweight upstream catalog daily with a staggered, initially stopped Dagster schedule, plus an authorized backoffice refresh action. Enable discovery after validation. A temporary catalog outage retains the last good catalog and shows its age; it does not erase releases.

### Backoffice scope and Dagster responsibilities

Extend the existing Graph page and domain authority section. Backoffice answers which data is available, whether it is current, and what requires operator attention. Dagster remains the interface for the asset dependency graph, detailed logs, step-level execution and debugging. Backoffice actions submit the existing durable requests and link to their Dagster runs; they do not implement a second pipeline executor or duplicate Dagster's run console.

The first implementation includes a compact Graph status area, a release management table and a domain ranking-history table. Charts are a follow-up enhancement, not a prerequisite for operating ingestion or browsing history. Normal daily imports are automatic; manual controls support initial setup, missing history and failed imports.

### Graph page status area

At the top of `/admin/graph`, show:

- **Current graph:** active release and its coverage period, resolved from the authoritative current-graph pointer.
- **Rankings available through:** newest published ranking release by coverage chronology, independently of the graph release. Link to the release table for historical gaps; the newest available release does not imply complete history.
- **Last discovery check:** timestamp of the last successful catalog refresh, with stale/error status when the latest attempt failed. Preserve the last successful timestamp during outages.
- **Automatic imports:** enabled or paused. Authorized operators may pause/resume future automatic requests; pausing leaves daily discovery, manual requests and already-running imports intact. Persist this policy server-side and audit changes.
- **Update/attention status:** pending/importing release, failure summary or old-graph cleanup pending. While importing, state that the current graph remains available. A cleanup failure after activation is shown as cleanup pending/failed, without describing the new graph as unavailable.

Display graph and ranking releases separately everywhere they appear together. If they differ, explain the mismatch explicitly; never imply the displayed scores were calculated on the active graph. For the measured initial state, the status would show June–August 2026 graph coverage and April–June 2026 ranking coverage, with a notice that rankings are behind the graph.

Graph exploration defaults to the active full graph and visibly labels its release. Historical versions stay in release management for ranking history and audit, but are not selectable as live graph snapshots after retirement. Previously bookmarked retired graph URLs explain that the graph was retired and offer the current graph and retained ranking history.

### Release management table

List all discovered releases from PostgreSQL, ordered by coverage chronology, and reconcile their artifact/publication state with serving ranking partitions, graph completion markers and the current-graph pointer. Do not derive available versions from only the last 100 Dagster runs. Keep graph and ranking status in separate columns.

Illustrative layout (these rows describe states, not additional measured releases):

| Release / coverage | Full graph | Rankings | Action |
| --- | --- | --- | --- |
| Newest available | Available | Available | Import latest graph + rankings |
| Current release | Active | Ready | View Dagster run |
| Older release | Retired | Ready | View ranking history |
| Historical release | Not retained | Missing | Import rankings only |
| Failed attempt | Failed or unchanged | Failed or ready | Resume failed import / View Dagster run |

Include constituent crawls, expected download sizes, active run and a concise last error in row details. Represent file stages independently in those details: available → downloading → downloaded → parsing → validated → published. Queued, failed, canceled, unsupported/unavailable and intentionally retired are distinct states. Keep low-level logs in the linked Dagster run. Use stage labels and measured counts/bytes when available; do not invent completion percentages.

Page-level actions are **Check for releases** and **Backfill missing rankings**. Backfill offers an explicit release selection or all missing supported ranking releases, displays the release count and known download estimate, and submits one request per release with bounded concurrency. It never bulk-imports historical graphs. Per-release actions are **Import latest graph + rankings**, **Import rankings only**, **Resume failed import** and **View Dagster run**, shown only where applicable.

Reuse existing backoffice authentication and enforce operator authorization on every mutation server-side. Reject duplicate active requests and completed selections server-side even if stale UI still shows a button. After submission, return the durable request ID immediately, show queued status and the run link once available, and refresh status while work is active. A retry uses verified completed file checkpoints.

Retirement is expected retention behavior, not an ingestion error. Keep published graph search and rank history usable when Dagster or upstream discovery is unavailable; show the relevant status as unavailable/stale rather than changing a published release to failed. If authoritative state cannot be checked safely, keep browsing available but fail closed on new import actions.

The authenticated backoffice action writes one durable import request after verifying catalog membership and the requested missing artifacts. A single Dagster request sensor registers the dynamic partition and launches the appropriate job with `run_key=request_id`, the exact partition key and audit tags. This makes repeated clicks and uncertain HTTP responses idempotent without coupling a long import to a browser request. The backoffice response immediately returns the request ID and follows it to its Dagster run.

The sensor reconciles queued requests with Dagster runs before launching again. A crash between launch and recording the run ID must recover through the request tag/run key. A retry after a terminal failure creates a new attempt/request ID; reusing the old sensor run key would suppress it. Failed imports remain retryable and are never marked processed merely because a sensor cursor advanced.

Daily automatic mode creates requests through the same ledger when a newly discovered supported domain release has all required artifacts ready. First enablement records the existing catalog as the historical baseline without automatically enqueuing it all; historical backfill is an explicit bounded selection. Persist this baseline so a restart does not reclassify historical releases as new. Manual and automatic requests share duplicate protection. No automation is enabled by this planning task.

If several releases arrived during an outage, import ranks for every missing supported release and the full graph for the newest ready release only. Historical/retired graph partitions are excluded from automatic missing-data reconciliation.

## Ranking history in backoffice

Use the existing domain authority section and link to it from Graph. Query the serving ranking table for an exact normalized domain across loaded releases, ordered by coverage end. No ranking-publication join is required. The replacement loader ensures one row per domain/release; deduplicate legacy compatibility reads before applying pagination and remove the current 24-row pre-deduplication cap.

In the first implementation, show a paginated history table with release/coverage, harmonic centrality score and position, PageRank score and position, each position's movement from the previous observation, and observed host count. Clearly label the latest available ranking release, independently of the active graph. Link between the domain's authority section and its connections in the current graph.

Position 1 is best. Define movement as `previous_position - current_position`, so positive is an improvement; the first observation has no change value. Preserve the source domain identity; do not strip arbitrary subdomains into guessed PLDs. As a follow-up, add two position charts (harmonic and PageRank), with lower ranks toward the top, and optional rank percentiles. The table remains the exact-value view and works without charts.

Distinguish `not present in a loaded release`, `release not imported`, and `dataset unavailable`. Missing observations produce gaps, not rank zero, and comparisons across gaps must identify the previous observed release. Add release population size to context: rank movement reflects the sampled graph and overlapping crawl windows, not an independently measured whole-web or search-engine ranking.

## Implementation tasks

Tasks 1–7 are implemented. Validation includes real HTTP → RustFS → ClickHouse imports, PostgreSQL request constraints and reconciliation, a complete nine-asset Dagster run and rerun with expired raw cache, graph activation/cleanup, chronological consumer tests, backoffice typecheck/build, and Dagster definition checks. The release panel was visually checked at desktop and 375-pixel mobile widths using fixture data. Production discovery succeeded with 54 releases and 159 available files, including the legacy 2017 domain layouts and ranking headers. A temporary localhost production backoffice server returned HTTP 200 with the live release catalog. Its local catalog connection is configured. A remote backoffice host/URL is still unconfirmed; automatic bulk imports and historical backfill have not started.

Operational handoff: [Common Crawl graph rollout](../../operations/commoncrawl-graph.md).

| Task | Deliverable | Depends on |
| --- | --- | --- |
| 1. Database migrations | Ranking table and PostgreSQL catalog, artifact, request and control tables | Existing source/schema audit |
| 2. Release discovery | Catalog refresh, artifact discovery, chronology and dynamic partition registration | 1 |
| 3. Ranking ingestion | Cached rank download, validated staging and atomic release replacement | 1, 2 |
| 4. Full-graph replacement and retention | Cached graph downloads, safe activation and removal of the previous graph | 1, 2, 3 |
| 5. Daily automation and import requests | Full/ranks-only jobs, request sensor, bounded backfill and retries | 2, 3, 4 |
| 6. Backoffice release management | Status overview, release table and operator controls | 5 |
| 7. Domain ranking history and consumer migration | History table and correct latest-release selection across consumers | 2, 3 |
| 8. Rollout and historical ranking backfill | Production validation, migration of existing data and enabled daily operation | 1–7 |

### Task 1 — Database migrations

Detailed handoff: [Common Crawl task 1 — database migrations](2026-09-25-commoncrawl-task-1-database-migrations.md).

Add the release-partitioned ClickHouse ranking table and application PostgreSQL storage for discovered releases, source files, import requests and singleton control state. Keep current graph nodes/edges/snapshot definitions and existing ranking consumers intact. Do not add a ranking-publication table. Register migrations in the project's existing checks and validate them against isolated databases.

Done when the new schemas and constraints pass migration tests, existing tables/data remain intact, and the schema contract is documented for the following tasks. This task produces migrations; it does not populate historical data, change readers, launch imports or deploy.

### Task 2 — Release discovery

Implement the unpartitioned catalog asset, safe official release-ID handling, actual artifact-link discovery and explicit coverage chronology. Upsert catalog/file metadata without overwriting an import's immutable source identity. Register discovered dynamic partition keys and preserve the last good catalog on failures. Record the initial discovery baseline for later automation.

Done when current, cross-year, historical and host-only fixture releases are represented correctly; unsupported/missing files are explicit; repeated discovery is idempotent; and an upstream failure cannot erase existing releases or claim a successful check. No bulk downloads or automatic import runs yet.

### Task 3 — Ranking ingestion

Add the rank raw-download asset and one rank-loading asset in the existing group. Cache and validate a gzip artifact, parse natively into staging, validate a complete release, then atomically replace its serving partition. Keep all historical ranking partitions. Reconcile crashes after replacement using the serving data and source run identity; loaded releases need no publication-marker table.

Done when a small real ClickHouse/HTTP fixture verifies complete publication, retry reuse, malformed/truncated input rejection, source revision checks, no duplicate rows and recovery after a lost success acknowledgement. The old rank table stays available to existing readers.

### Task 4 — Full-graph replacement and retention

Add separate node/edge raw checkpoints, adapt the existing native graph loader to committed cached input and preserve its asset keys and validation. Adopt existing verified graph partitions. Implement serialized activation of the newest validated graph with matching ranks, followed by independently retryable retirement/cleanup after a bounded reader grace period. Keep graph lookup projections and the graph completion contract.

Done when failures leave the active graph readable; activation cannot mix releases or move backward; cleanup cannot delete the active graph or ranking history; and retired releases are distinguished from failed/missing imports. Test with isolated small fixtures; do not delete the current production graph.

### Task 5 — Daily automation and import requests

Wire the full and ranks-only job selections, durable request sensor, one-release-per-run backfills and bounded retry/concurrency policies. Daily discovery queues all missing rank releases under the selected backfill policy and only the newest ready full graph. First activation respects the historical baseline, and intentionally retired graphs are never automatically reimported. Pausing automatic requests leaves discovery, manual requests and active runs intact.

Done when duplicate requests, concurrent manual/automatic submissions, sensor restarts after launch, failed-run retries, cancellation and resume are covered. Definitions validate with `uv run dg check defs`. Leave deployment schedules/sensors stopped until rollout enables them.

### Task 6 — Backoffice release management

Extend `/admin/graph` with the agreed status area, complete release table, graph/rank mismatch notices, run links and authorized check/import/backfill/resume/pause controls. Show activation and cleanup independently. Read loaded rank releases from ClickHouse, discovery from PostgreSQL and execution detail from Dagster.

Done when server-side authorization/idempotency is tested, historical bulk actions select rankings only, retired graph links are handled, status refresh follows active requests, and Dagster/catalog outages preserve access to published data. Run focused route/server tests, typecheck and build.

### Task 7 — Domain ranking history and consumer migration

Deliver the chronological domain authority table with both scores, positions, movement, host count and explicit release coverage. Remove the existing pre-deduplication history cap. Update technology rollups and webtech input selection to use the new rank relation with coverage-based latest selection. Preserve access to legacy rank data until it has been reimported or validated; coordinate the reader switch with rollout. Charts remain follow-up work.

Done when missing observations/imports are distinct, older backfills cannot become latest by load time, the first observation has no fabricated delta, compatibility remains intact before cutover, and focused backoffice/consumer tests pass.

### Task 8 — Rollout and historical ranking backfill

Deploy migrations before their consumers. Recheck the existing live graph/rank releases and map legacy labels to official IDs. Import and verify one recent ranking release, a cross-year release and an older supported release; verify a new full graph before activation. Cut over rank readers after their required data is available. Queue missing historical rankings with bounded concurrency, enable daily discovery/automatic future imports, and verify old graph cleanup and stable serving.

Done when backoffice shows correct active graph/rank coverage, historical ranking completeness can be audited, daily operation and failure recovery are demonstrated, and previous full-graph partitions are retired without losing rank history. Retire the manual rank loader and legacy rank table only after the consumer/data cutover is verified.

## Acceptance checks across tasks

Critical integration tests: truncated gzip never becomes cached; malformed/empty/wrong-release data never replaces a good partition; source revision changes are rejected; retrying edges reuses ranks/nodes; publication failure does not expose incomplete ranks; existing graph publication survives rank failures; old backfills do not become latest; `example.co.uk` reversal is correct; rank and graph levels cannot be mixed; domain history handles absence and missing imports separately.

Rank publication tests: partial staging never appears in loaded-release queries; a successful replacement exposes the complete release in one step; a failed replacement preserves any previous valid partition; retry does not append duplicate rows; a crash after replacement but before status recording reconciles to the already-loaded data without needing a separate publication marker.

Retention tests: failed new imports leave the current graph readable; activation switches both node/edge lookups together; concurrent old/new imports cannot roll the pointer backward; cleanup never deletes the active release or any ranks; cleanup retries are idempotent; expired graph links report retirement; discovery does not requeue retired graphs.

Backoffice acceptance tests: the overview labels different graph/ranking releases correctly; failed discovery preserves the last successful check and cached catalog; available upstream releases appear before any import attempt; historical backfill submits rankings only; unauthorized actions are rejected; repeated clicks do not duplicate requests; pause affects future automatic requests only; active imports refresh and link to Dagster; cleanup failure does not hide the new active graph; retired graphs are not selectable for live exploration; Dagster outages do not block published-data reads; domain history is chronological, shows signed movement and distinguishes missing observations from missing imports. Verify keyboard access and readable narrow-screen layouts for the new controls and tables using existing backoffice components.

Implementation validation: `uv run dg check defs`, relevant Python source/migration/integration tests, backoffice typecheck/build and focused route/server tests. The implementation is exercised with small isolated fixture materializations; production materializations belong to task 8.

## Primary files to extend

- `services/dagster_v3/src/dagster_v3/defs/commoncrawl_domain_graph/{assets,source,load}.py`, with focused catalog/download/rank/request modules next to them.
- `services/dagster_v3/src/dagster_v3/defs/commoncrawl_domain_graph/docs/commoncrawl-domain-graph-design.md` for the implemented operational contract.
- `clickhouse/migrations/`, `database/migrations/` and relevant migration contract tests.
- `services/backoffice/app/routes/admin-graph.tsx`, `app/lib/domain-graph.server.ts`, and `app/lib/dagster.server.ts` for status integration.
- `services/backoffice/app/lib/web-intelligence.server.ts` and `app/components/detail/web-intelligence-section.tsx` for history.
- `services/dagster_v3/src/dagster_v3/defs/technology_catalog/assets.py` and `defs/webtech/` for rank consumers.

Dagster supports dynamically discovered partition keys and sensor-triggered runs in its [partitioning documentation](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets). Use the project's pinned Dagster APIs and validate generated definitions during implementation.
