# Finland Dagster + Temporal Architecture Design

## Purpose

Introduce Dagster as the dataset orchestration plane for Corpscout sources,
keeping Temporal as the entity/long-running execution plane. This document
designs the full hybrid for the Finland dataset only: PRH YTJ (registry),
PRH XBRL (financial statements), NACE reference, and the Finland explorer
table. Finland is the proving ground; later sources follow the same pattern.

This design supersedes the source orchestration parts of
`2026-06-11-source-package-architecture-design-2.md` (package registry,
`source_package_catalog`, workflow status API, dependency/freshness engine).
The following decisions from that document carry forward unchanged:

- bucket per source in RustFS with `runs/{run_id}/` key layout
- `manifest.json` per run as the durable artifact ledger
- ClickHouse migrations stay central under `golang-migrate`
- one module per source that owns its source-specific code

"Supersedes" does not mean Dagster absorbs the source catalog. The existing
`data_sources` table remains the shared metadata source — bucket names,
ownership, docs URLs, enabled state — read by the UI, the scheduler, and
Dagster asset code alike. Dagster's run storage is execution history only;
it never becomes the catalog.

## Settled Decisions

1. **Pull boundary — split by shape.** The PRH YTJ snapshot pull becomes a
   Dagster asset. PRH XBRL discovery + download stays a Temporal workflow and
   reports its output to Dagster as an external asset.
2. **Importer language — Python.** The Go YTJ parser/normalize/import code is
   ported to Python inside the Dagster code location. The Go version stays
   until the cutover comparison passes.
3. **Artifacts — RustFS now.** Bucket per source; local
   `sourceRunsRoot` run directories are retired for Finland.
4. **Deployment — server, parallel run.** Dagster joins the companycollect
   server via docker compose. The existing Temporal Finland workflows keep
   running until Dagster outputs match, then the old path is deleted.

## Boundary Rule

> If the unit of work is a dataset (file, table, projection), it is a Dagster
> asset. If it is an entity process or a long-running checkpointed job, it is
> a Temporal workflow.

A workload goes to Temporal when any of these is true:

- the work is per-entity rather than per-dataset
- it cannot be restarted from zero cheaply (rate limits, paging cursors,
  paid API calls)
- it needs signals, human-in-the-loop, or multi-day durability
- it drives a browser or LLM session

Everything else — downloads that are cheap to redo, table imports, SQL
transforms, projections, reference syncs — is a Dagster asset.

After cutover, Temporal never schedules dataset refreshes and Dagster never
executes entity work. Scheduling and lineage live in Dagster; durable
per-entity execution lives in Temporal; the run manifest in RustFS is the only
thing that crosses the line.

## Architecture Overview

```
                  +- DAGSTER (dataset plane) ------------------------------+
PRH YTJ API -pull-> raw_snapshot --> normalized_tables -+                  |
                  |    (RustFS)        code_lists       +-> industry_nace_ |
Postgres NACE ----> nace clickhouse_tables -------------+    mappings      |
                  |                                           |            |
                  |                             company_explorer_cache     |
                  |  raw_statements (external) --> [future: xbrl tables]   |
                  +-------^------------------------------------------------+
                          | sensor reads runs/{run_id}/manifest.json
                  +- TEMPORAL (entity plane) --------+
PRH XBRL API ---->| XBRL discovery+download workflow |--> RustFS bucket
                  | (existing, repointed to RustFS)  |
                  | BRREG / crawl / translation      |   (unchanged)
                  +----------------------------------+
```

Dagster's own run and event storage (a new `dagster` database on the existing
Postgres instance) replaces the planned package catalog table, workflow status
API, and freshness engine. Durable run history therefore no longer depends on
Temporal retention.

## Dagster Project Layout

New top-level project `corpscout/dagster/`, Python 3.12:

```
corpscout/dagster/
  pyproject.toml              # dagster, dagster-webserver, clickhouse-connect,
                              # boto3, psycopg, httpx/requests
  dagster.yaml                # instance config: postgres storage, run launcher,
                              # run queue concurrency
  workspace.yaml
  docker-compose.yml          # webserver, daemon, code-location (server deploy)
  .env.example
  dagster_corpscout/
    definitions.py            # Definitions: assets, resources, schedules, sensors
    resources/
      clickhouse.py           # clickhouse-connect client resource
      rustfs.py               # boto3 S3 resource (streaming upload helpers)
      postgres.py             # psycopg resource (NACE read)
    sources/
      finland_prhytj/
        spec.py               # declarative source config: URLs, code lists,
                              # bucket name, table names
        assets.py             # raw_snapshot, normalized_tables, code_lists,
                              # industry_nace_mappings, company_explorer_cache
        parser.py             # port of parser.go
        normalize.py          # port of normalize.go
        tables.py             # table constants + insert column lists
        checks.py             # asset checks: expected ClickHouse tables exist
      finland_prh_xbrl/
        assets.py             # external AssetSpec raw_statements + manifest
                              # sensor; future clickhouse_tables asset slot
        checks.py
    reference/
      nace/
        assets.py             # clickhouse_tables: Postgres -> corpscout_reference
        checks.py
```

The "package owns everything source-specific" principle from design 2
survives as one Python module per source, with asset key prefixes
(`finland_prhytj/...`) and asset groups providing the catalog taxonomy.
`spec.py` keeps the source's declarative inputs in one place; it is the future
input for asset factories when simple sources arrive in volume, but Finland
assets are written by hand.

## Asset Graph

| Asset key | Kind | Does | Trigger |
|---|---|---|---|
| `finland_prhytj/raw_snapshot` | materializable | Pull company snapshot + 8 code lists from PRH API, stream to `source-finland-prhytj` bucket under `runs/{run_id}/`, write `manifest.json` | Schedule (weekly cron, configurable) |
| `finland_prhytj/normalized_tables` | materializable | Read snapshot from bucket, parse + normalize (Python port), batch-insert the 14 `fi_prhytj_*` tables | `AutomationCondition.eager()` on `raw_snapshot` |
| `finland_prhytj/code_lists` | materializable | Import the 8 code-list TSVs into `fi_prhytj_code_lists` | eager on `raw_snapshot` |
| `reference_nace/clickhouse_tables` | materializable | Port of `SyncNACEToClickHouseActivity`: read Postgres `nace_*`, truncate-and-snapshot the 3 `corpscout_reference` tables, derive hierarchy columns | Schedule (weekly) |
| `finland_prhytj/industry_nace_mappings` | materializable | Existing TOIMI->NACE mapping SQL into `fi_prhytj_industry_nace_mappings` | eager on `normalized_tables` + `reference_nace/clickhouse_tables` |
| `finland_prhytj/company_explorer_cache` | materializable | Existing cache-refresh SQL: build scratch table, `EXCHANGE TABLES` swap into `fi_prhytj_company_explorer_cache` | eager on `industry_nace_mappings` |
| `finland_prh_xbrl/raw_statements` | external | Produced by the Temporal XBRL workflow; sensor records materializations | Sensor (60s) on bucket manifests |
| `finland_prh_xbrl/clickhouse_tables` | future slot | XBRL ClickHouse import — not implemented today, out of scope; declared so the graph shows the dependency | — |

### Orchestration mechanics

There is no coordinator workflow. Each asset declares its dependencies and an
automation condition; the dagster-daemon evaluates conditions continuously and
launches runs in dependency order. The weekly snapshot run cascades through
import, mapping, and cache refresh without any "is everything done" code.

`eager()` means "run when any upstream gets new data", not "wait for all
upstreams to update together". If NACE syncs Tuesday and the YTJ snapshot
lands Thursday, the mapping asset runs twice that week. This is correct
because every asset in the chain is an idempotent full refresh
(ReplacingMergeTree inserts, truncate-and-load, atomic swap).

**Requirement: every materializable asset in this graph must be an idempotent
refresh.** Re-running with the same upstream state must converge to the same
table contents. Any future asset that violates this needs partitions or
explicit dedup logic before joining the automation chain.

**Standing rule for future sources: `eager()` is only for cheap idempotent
refreshes.** At 300 sources, eager chains can create noisy recomputation.
Expensive assets (large rebuilds, costly joins, anything beyond minutes of
ClickHouse work) get a cron condition or a custom automation condition that
batches upstream updates instead of reacting to each one. Every Finland
chain asset qualifies as cheap today; this rule exists so the default does
not silently scale into waste.

Manual operation: in the Dagster UI, materialize `raw_snapshot` with
"materialize all downstream" for a full Finland refresh, or materialize
`company_explorer_cache` alone to rebuild the final table from current inputs.

### Run identity

`run_id` for bucket keys is the UTC start timestamp (`20260611T120000Z`),
generated by the pull asset (or by the Temporal workflow for XBRL) and
recorded as materialization metadata together with object keys, sha256s, byte
sizes, and record counts.

## Pull Execution Model

Dagster launches the processes that execute asset code; there is no separate
worker fleet and the crawl-service is not involved in source pulls.

- **Run launcher:** `DockerRunLauncher`. Each run is an ephemeral container
  spawned from the code-location image on the companycollect server. A
  misbehaving download cannot degrade the code-location gRPC server or the
  webserver. Run containers get memory/CPU limits in compose/launcher config.
- **Streaming downloads:** multi-gigabyte files (the YTJ snapshot is a few GB
  of JSON-LD) stream HTTP -> RustFS in chunks via boto3 multipart upload
  (`upload_fileobj` over `requests` with `stream=True`). No buffering of the
  payload in memory or on container disk.
- **Failure semantics:** a failed download re-runs from byte zero under the
  asset `RetryPolicy`. For single bulk files this costs minutes; acceptable.
  Pulls where restart-from-zero is expensive belong on Temporal (see boundary
  rule) — that is why Companies House style paged pulls stay there.
- **Concurrency:** the daemon's run queue caps global concurrent runs;
  tag-based concurrency keys (one per source) guarantee two runs of the same
  source never overlap and let us cap "concurrent source pulls" without
  custom code.

## Temporal Responsibilities

### Permanent, in Finland scope

**PRH XBRL discovery + download** stays a Temporal workflow: hours of
wall-clock at a 1.5s/request rate limit, per-statement download state machine
in Postgres (`financial_xbrl.finland_prh_xbrl_discovery_windows`,
`finland_prh_xbrl_statement_artifacts`), per-item retries, must survive worker
restarts without redoing rate-limited work.

Changes to it in this design:

1. Write XMLs and `statements.ndjson` to the `source-finland-prh-xbrl` RustFS
   bucket instead of the local run directory (the scheduler already has an S3
   client from the object storage browser).
2. The final activity writes `runs/{run_id}/manifest.json` to the bucket.
   This is the handoff event for Dagster.

Postgres state tables, rate limiting, retry/backoff logic are unchanged.

**NACE RDF -> Postgres import** (Go `nacetaxonomy` + its Temporal workflow)
stays as-is. Only the Postgres -> ClickHouse snapshot moves to Dagster.

### Temporary, until cutover

The existing YTJ workflows (`CompanySourceDownloadWorkflow`,
`CompanySourceClickHouseImportWorkflow`,
`CompanySourceExplorerCacheRefreshWorkflow`,
`CompanySourceIndustryNACEMappingWorkflow`, and the composite
`CompanySourceSyncClickHouseWorkflow`) keep running during the parallel-run
window for output comparison. At cutover they are deleted, along with the
`SyncNACEToClickHouse` workflow.

### Outside Finland scope, permanently Temporal

- BRREG pipelines (per-record translation, domain discovery, financial
  conversion)
- crawl-service and translation-service orchestration (browser/LLM sessions)
- Companies House, GLEIF, CVR, Ariregister paged pulls with ContinueAsNew.
  When those sources later migrate to this architecture, their pulls stay
  Temporal and adopt the same manifest -> external asset handoff as XBRL;
  only downstream import/projection moves to Dagster.

## Temporal -> Dagster Handoff

`finland_prh_xbrl/raw_statements` is declared as an external `AssetSpec`. A
Dagster sensor (60s interval) lists `runs/*/manifest.json` in the
`source-finland-prh-xbrl` bucket, keeps the latest seen `run_id` as its
cursor, and records an `AssetMaterialization` for each new manifest with
metadata from it (statement counts, bucket prefix, workflow id).

The sensor deliberately records materializations, not `AssetObservation`
events. Observations are metadata-only: they do not update the asset's
materialized state and do not drive downstream automation conditions. The
future XBRL import asset must auto-run when new statements land, which
requires materialization events.

Sensor pull is chosen over REST push from the Go workflow because it adds no
Dagster dependency to the Temporal worker and loses no events if Dagster is
down — the manifest in the bucket is the durable record; the sensor catches
up whenever it runs.

### Manifest contract

```json
{
  "run_id": "20260611T120000Z",
  "source": "finland_prh_xbrl",
  "workflow_id": "xbrl-download-20260611T120000Z",
  "artifacts": [
    {
      "key": "statements_manifest",
      "object_key": "runs/20260611T120000Z/statements.ndjson",
      "content_sha256": "...",
      "content_length_bytes": 123456,
      "records_written": 1000
    }
  ]
}
```

The Dagster pull assets write the same shape for their own runs, so every
run in every source bucket carries its ledger regardless of which plane
produced it.

## Storage Layout

```
source-finland-prhytj/
  runs/{run_id}/source.jsonld            (bulk download endpoint; if the pull
                                          uses the paginated v3 API instead,
                                          the artifact is source.ndjson — the
                                          import asset reads either from the
                                          bucket, downstream does not care)
  runs/{run_id}/codelists/REK.en.tsv     (8 code lists)
  runs/{run_id}/manifest.json

source-finland-prh-xbrl/
  runs/{run_id}/statements.ndjson
  runs/{run_id}/xml/{business_id}/{financial_date}.xml
  runs/{run_id}/manifest.json
```

Buckets are created once, manually or by a small setup script — not by asset
code. The existing object storage browser is the human inspection path; no
artifact tables are added to Postgres.

## ClickHouse Model

- The central `clickhouse/migrations/` directory applied with
  `golang-migrate` (`make clickhouse-migrate-up`) remains the only thing that
  creates or alters durable tables. Schema change workflow: write migration
  pair, apply, then deploy the Dagster code that uses it.
- Dagster assets only INSERT, truncate-and-load, or run the swap pattern.
- **One allowed runtime-DDL exception:** the explorer-cache refresh creates a
  scratch table and uses `EXCHANGE TABLES`, as the Go code does today.
  Scratch tables inside an atomic-swap pattern are not schema ownership.
- **Asset checks as schema guardrail:** each source module's `checks.py`
  verifies its expected tables exist (`EXISTS TABLE`). Deploying asset code
  before its migration fails fast with a clear message instead of a confusing
  mid-import error.

No new ClickHouse tables are required for this design; all 15 YTJ tables, the
3 NACE reference tables, the mapping table, and the explorer cache already
exist (migrations 000001–000010).

## Postgres Model

- New `dagster` database on the existing Postgres instance for Dagster run,
  event, and schedule storage. This is Dagster-managed; no Corpscout
  migrations touch it.
- `financial_xbrl.*` state tables: unchanged, still owned by the Temporal
  workflow.
- `data_sources` remains the shared source catalog (see Purpose). The
  Finland module's `spec.py` is synced into it on deploy, the same pattern as
  today's `sourcecatalog` JSON sync, so the UI source list, bucket names, and
  docs links keep one home.
- `data_source_files`, `data_source_actions`, `data_source_action_runs`,
  `data_source_file_runs`: unchanged during the parallel run. At cutover, the
  YTJ actions are disabled and the Dagster path does not write run rows —
  Dagster's own storage is the run history. The tables stay for the remaining
  (non-migrated) sources and are revisited when those sources migrate.
- No `source_package_catalog` table is created (superseded).

## UI and API Integration

- **Explorer: zero change.** It reads `fi_prhytj_company_explorer_cache`
  through the existing scheduler endpoints
  (`/api/v1/sources/finland_prhytj/explorer/*`); tables and shapes are
  identical.
- **Operations: link out, no proxy.** New `CORPSCOUT_DAGSTER_UI_URL` env var
  (e.g. `http://100.85.212.113:3500`); the source detail page links to the
  Dagster UI the same way the Jobs page links to the Temporal UI. After
  cutover, the YTJ actions/pipeline tabs point there instead of
  `data_source_*` run history. No GraphQL proxy into Dagster in this phase.

## Deployment

Server (`companycollect`, `/home/graovic/dagster/`), docker compose:

| Container | Role | Port |
|---|---|---|
| `dagster-webserver` | UI + GraphQL | 3500 (3000 PostgREST, 8089 Temporal UI, 8090 scheduler are taken) |
| `dagster-daemon` | schedules, sensors, automation conditions, run queue | — |
| `dagster-code-corpscout` | code location gRPC server, also the run image for `DockerRunLauncher` | internal |

- Instance config (`dagster.yaml`): Postgres storage pointing at the
  `dagster` database, `DockerRunLauncher`, run queue concurrency limits.
- Env vars: ClickHouse native URL, RustFS endpoint + credentials, Postgres
  DSN (corpscout, for NACE read), all via `.env` on the server — same
  localhost-vs-`companycollect` hostname convention as the Temporal workers,
  and the server `.env` is never overwritten from the Mac.
- Deploy: rsync `corpscout/dagster/` to the server (excluding `.env`), then
  `docker compose up -d --build` — identical to the go-worker/python-worker
  routine.
- Local dev: `dagster dev` on the Mac against remote ClickHouse, RustFS, and
  Postgres via Tailscale.

## Error Handling

Project conventions translated to the Python plane:

- library code (parser, normalize, resources) raises exceptions with context;
  no logging in lower layers
- the asset boundary is the log-once layer: let the exception fail the run,
  Dagster records it with the stack trace in run logs
- `RetryPolicy` (e.g. 3 retries, exponential backoff) on network-bound assets
  (pull, NACE Postgres read)
- no secrets, tokens, or credentials in asset metadata, logs, or
  materialization metadata
- Temporal side keeps the existing Go rules (`cockroachdb/errors` wrap,
  log once at the activity boundary)

## Testing Strategy

Python (pytest, in `corpscout/dagster/`):

- parser/normalize unit tests against NDJSON/JSON-LD fixtures, including the
  edge cases the Go parser handles today
- asset tests via in-process materialization with stub ClickHouse/RustFS/
  Postgres resources
- sensor test: manifest discovery, cursor advance, no duplicate
  materializations for seen run_ids
- asset check tests: missing table produces the intended failure

Cross-plane:

- manifest contract round-trip: Go writer (XBRL workflow) output validates
  against the Python sensor's expected schema
- the cutover comparison below is itself the integration test for the import
  port

Go (existing suites unchanged):

- XBRL workflow tests updated for RustFS writes and manifest emission

## Cutover Plan

1. Deploy Dagster; YTJ assets initially write imports to a throwaway bake-off
   database (`corpscout_sources_bakeoff`, created ad hoc with the production
   DDL, explicitly not via golang-migrate). Target database is a resource
   config knob.
2. Run the same snapshot through both pipelines: Temporal/Go into
   `corpscout_sources`, Dagster/Python into the bake-off database.
3. Parity gate, per table (all 15 YTJ tables): equal row counts and equal
   content checksums (`sum(cityHash64(<ordered columns>))`), plus a diff of
   explorer cache output after mapping + refresh run on both sides.
4. On pass: point Dagster at `corpscout_sources`, enable schedules and
   automation conditions, disable YTJ Temporal actions in the catalog.
5. After one clean scheduled cycle: delete the YTJ Temporal
   workflows/activities, the `SyncNACEToClickHouse` workflow, the YTJ entries
   from `sourcecatalog`, and the local-filesystem download path for Finland;
   re-point the YTJ actions/pipeline UI tabs to the Dagster UI link; drop the
   bake-off database.

Rollback at any step before 5 is "keep using the Temporal path" — it remains
fully functional throughout.

## Out of Scope

- XBRL ClickHouse import (asset slot declared; implementation is a separate
  design)
- porting the Go RDF -> Postgres NACE import to Python
- other countries / sources, and asset factories for simple sources
- any change to BRREG, crawl-service, or translation-service pipelines
- Dagster GraphQL proxying through the scheduler API
- alerting/notification wiring for failed runs (Dagster UI is the visibility
  surface in this phase)

## First Implementation Step

Stand up `corpscout/dagster/` with the deployment skeleton and a single
end-to-end vertical slice: `raw_snapshot` pull asset streaming to RustFS with
manifest, `normalized_tables` import into the bake-off database, and the
parity comparison script. That slice exercises every architectural seam
(run launcher, streaming, bucket layout, ClickHouse writes, schedule) before
the remaining assets are filled in.

The slice has explicit pass criteria for the execution model, checked before
any further asset work: one `DockerRunLauncher` run container reaches
ClickHouse, RustFS, and Postgres over the server network, streams a
multi-gigabyte object to a bucket within its memory limit, and the daemon's
schedule and run queue launch and bound runs as configured.
