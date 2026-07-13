# Bounded ClickHouse-Input DNS and AXFR Pipeline Plan

**Date:** 2026-07-11  
**Status:** Proposed  
**Scope:** `corpscout/commoncrawl/cc-dns-worker`, the CommonCrawl hostname Dagster asset, ClickHouse migrations, migration-contract tests, documentation, and the `cc_dns_worker` Ansible deployment  
**Goal:** Make ClickHouse the authoritative domain/hostname input, keep only bounded crash-recovery work and unacknowledged output in SQLite, and run DNS and AXFR as concurrent retry-safe lanes without retaining a complete scan locally.

## Why this plan exists

The current worker copies the full domain corpus into SQLite, mirrors that membership back into
`corpscout.dns_scan_seed_domains`, scans `ctlogs.hostnames` and
`corpscout.commoncrawl_domain_hostnames`, and copies the ranked hostname set into SQLite before DNS
work starts. The same cycle DB then retains DNS results until a later load and retains resolved domains
until a scan-wide AXFR phase finishes.

That architecture made the original phase boundaries resumable, but it duplicates the largest inputs
and lets local disk scale with the complete corpus rather than the amount of active work. It also leaves
ClickHouse and SQLite staging concepts whose only purpose is to copy membership between the two systems.

The replacement keeps the valuable correctness properties—durable dispatch, replay-safe ClickHouse
writes, separate DNS/AXFR rate limits, and exact endpoint identity—while changing storage ownership:

- ClickHouse owns complete domain and hostname input.
- SQLite owns a bounded set of fetched-but-not-finished domains and results not fully acknowledged by
  ClickHouse.
- DNS and AXFR operate concurrently through separate bounded queues.
- A local row is deleted only after every ClickHouse sink that depends on it has accepted a retry-safe
  write.

## Non-goals

- Do not embed a ClickHouse server, chDB, or another analytical database inside the worker.
- Do not create a third combined ClickHouse input table. Read the two authoritative tables directly.
- Do not keep a full per-cycle domain membership table in either SQLite or ClickHouse.
- Do not create a generic queue, pipeline, repository, or service abstraction. Implement the concrete
  DNS and AXFR state machines directly.
- Do not make DNS workers perform AXFR inline.
- Do not use ClickHouse mutations as the steady-state acknowledgement or cleanup mechanism.
- Do not delete legacy ClickHouse tables in the first deploy. Preserve a rollback window.
- Do not preserve compatibility with an active old cycle DB. Production cutover explicitly drains or
  archives the old state before the new binary starts.

## Required engineering rules

- Follow the repository's Go logging/error rules: structured boundary logs, wrapped lower-layer errors,
  one log per failure, and no secrets.
- Defaults live only in CLI/Ansible operator configuration. Runtime structs receive explicit values.
- Every source cursor advance and its corresponding local work insertion happen in one SQLite
  transaction.
- Every local cleanup happens only after all required ClickHouse writes succeed.
- Every ClickHouse output uses stable logical identity so an acknowledgement-loss replay is safe.
- Every crash-safety statement below receives a failure-injection or restart test.
- Every ClickHouse migration uses the next free number at implementation time, has up/down files, is
  registered in `dagster_v3/tests/test_clickhouse_migrations.py`, and contains no semicolons in `--`
  comments.
- Run `gofmt`, `go mod tidy`, `go vet`, `staticcheck`, and `go test -race ./...` before the final commit.

## Authoritative data ownership

### Input tables

The worker reads only:

1. `corpscout.commoncrawl_domains`
   - authoritative root-domain membership;
   - keyset-paged by `root_domain`;
   - grouped to one row per root domain.
2. `corpscout.commoncrawl_domain_hostnames`
   - authoritative hostname labels for scan planning;
   - populated by the Dagster CT sync and by worker AXFR/DNS discoveries;
   - queried only for a bounded root-domain batch.

The worker stops reading `ctlogs.hostnames` directly after the Dagster release gate passes.

### Direct output tables

DNS and AXFR result batches write:

- `corpscout.commoncrawl_domain_dns_scan`
- `corpscout.commoncrawl_domain_dns_record_observations`
- `corpscout.commoncrawl_domain_hostnames`
- `corpscout.dns_axfr_latest`
- `corpscout.dns_axfr_state_changes`

`corpscout.commoncrawl_domain_dns_record_summary` remains a derived refreshable materialized-view
target and is never written by the worker.

`corpscout.dns_axfr_observations` remains read-only legacy input for the existing historical backfill.

## Cycle consistency contract

The cycle is intentionally a keyset traversal, not a global ClickHouse snapshot:

- A root domain inserted after the cursor has passed its lexical position is scanned in the next cycle.
- A hostname inserted after its root domain's batch was fetched is scanned in the next cycle.
- A restart reuses the persisted cycle ID, source cursor, and active local work.
- `--max-domains` counts domains durably inserted into the cycle's work queue and remains deterministic.

This contract avoids snapshot tables and is appropriate for a continuously repeated corpus scan. The
cycle start time is still persisted for metrics and stable event metadata, but it is not used to hide
new rows from domains the cursor has not reached yet.

## Target SQLite schema

The exact DDL may be adjusted for SQLite syntax during implementation, but the ownership and states are
fixed by this plan.

### `scan_state`

One row for the active cycle:

```text
scan_id
domain_cursor
source_exhausted
domains_fetched
started_at
```

`domain_cursor` means every root domain at or before it was durably inserted into `dns_work` at some
point in this cycle. The domain may still be active locally or may already have been acknowledged and
deleted.

### `dns_work`

Only currently active DNS work:

```text
scan_id
root_domain
status                 pending | running | ready
DNS summary fields     nameservers, endpoints, outcomes, counters, timestamps
PRIMARY KEY (scan_id, root_domain)
INDEX (status, root_domain)
```

- `pending`: fetched from ClickHouse but not currently owned by a DNS worker.
- `running`: claimed by this process. Reset to `pending` on process startup.
- `ready`: DNS result and dependent local rows committed; eligible for ClickHouse flush.

### `dns_records`

Bounded record outbox keyed to active `dns_work` rows. It contains ordinary DNS observations and is
deleted with the owning DNS work item after the DNS flush succeeds.

### `axfr_work`

One compact job per resolved domain whose delegation was trustworthy and non-empty:

```text
scan_id
root_domain
ns_endpoints_json
delegation_observed_at
status                 pending | running | ready
error
PRIMARY KEY (scan_id, root_domain)
INDEX (status, root_domain)
```

The JSON payload contains every NS hostname/IP endpoint, including non-dialable and hyperscaler
addresses. Those endpoints are evidence and are required to reconcile delegation activity even when no
network probe is allowed.

### `axfr_probes` and `axfr_zone_records`

Bounded AXFR output keyed to active `axfr_work` rows:

- one probe outcome per `(scan_id, root_domain, name_server, name_server_ip)`;
- transferred records from the first successfully opened zone;
- stable probe/delegation observation timestamps.

No local `axfr_changes` table is required. The AXFR loader can deterministically derive changes from
persisted probe results plus the scoped prior state read from `dns_axfr_latest`.

### Removed local concepts

The new per-cycle DB does not create:

- `scan_meta`
- full-corpus `scan_domains`
- `scan_hostnames`
- `host_load_state`
- `host_load_shards`
- `seed_membership_state`
- `load_state` rowid watermarks
- `completed_domains`
- scan-wide `axfr_load_state`
- local `axfr_changes`

## State transitions and atomic boundaries

### Domain source pump

For each bounded ClickHouse page:

1. Query root domains strictly greater than `scan_state.domain_cursor`.
2. Begin one SQLite transaction.
3. `INSERT OR IGNORE` those roots into `dns_work(status='pending')`.
4. Advance `domain_cursor` to the final root and increment `domains_fetched`.
5. Commit.

If the process dies before commit, the cursor remains old and the page is fetched again. If it dies
after commit, every root before the cursor is represented by durable work or has already completed.

The source pump pauses while the combined active DNS/AXFR/outbox capacity is at its configured limit.

### DNS dispatch and commit

1. Claim a bounded pending-domain batch in one SQLite transaction by changing `pending -> running`.
2. Fetch hostname labels for those exact roots from `commoncrawl_domain_hostnames`.
3. Keep hostname labels in memory only.
4. DNS workers resolve domains through the existing discovery and authoritative schedulers.
5. The single DNS committer transactionally:
   - replaces the domain's local DNS records;
   - writes the DNS summary into `dns_work`;
   - inserts or replaces `axfr_work(status='pending')` when AXFR is enabled and delegation discovery is
     trustworthy;
   - changes `dns_work.status` to `ready`.

If DNS discovery failed or is partial in a way that makes delegation membership untrustworthy, no AXFR
reconciliation job is created. A transient discovery failure must never mark prior endpoints inactive.

### DNS flush and cleanup

For a bounded `dns_work.status='ready'` batch:

1. Build stable DNS record-observation rows.
2. Build stable domain-summary rows.
3. Build hostname-registry rows from successfully resolved CT/AXFR-discovered labels.
4. Insert record observations.
5. Insert domain summaries.
6. Insert hostname-registry rows.
7. Only after all three writes succeed, delete that batch's `dns_records` and `dns_work` rows in one
   SQLite transaction.

If ClickHouse accepted a write but the client lost the acknowledgement, the local rows remain and the
whole batch is replayed. Existing Replacing/Aggregating identities must collapse that replay.

The `axfr_work` row is independent, so bulky DNS records can be removed while AXFR lags behind.

### AXFR claim, probe, and local commit

1. Claim `axfr_work.status='pending'` rows by changing them to `running` in one SQLite transaction.
2. One AXFR worker owns one domain job.
3. Probe every dialable, non-hyperscaler endpoint.
4. Deduplicate the network transfer by IP within the domain, but persist one outcome for each NS
   hostname/IP identity.
5. Preserve all non-dialable/hyperscaler endpoints in the current delegation payload without probing.
6. Retain the first successfully opened zone's records; continue probing other distinct IPs so every
   endpoint receives a current verdict.
7. In one SQLite transaction:
   - replace this job's `axfr_probes`;
   - replace this job's `axfr_zone_records`;
   - change `axfr_work.status` to `ready`.

Unknown is a terminal observation for this scan, not an immediate infinite retry. Process/infrastructure
errors that prevent a complete local commit leave or restore the job as `pending`.

### AXFR flush and cleanup

For a bounded ready-job batch:

1. Query `dns_axfr_latest FINAL` only for the batch's root domains.
2. Compare prior endpoints with current delegation plus fresh probes.
3. Build definitive `dns_axfr_state_changes` rows.
4. Build `dns_axfr_latest` rows for:
   - freshly probed endpoints;
   - prior endpoints still delegated but not probed;
   - prior endpoints removed from the current delegation.
5. Build AXFR zone-record observations.
6. Build AXFR-discovered hostname-registry rows.
7. Write state changes first.
8. Write latest endpoint rows second.
9. Write zone-record observations third.
10. Write hostname-registry rows fourth.
11. Only after every enabled sink succeeds, delete the ready job, probes, and zone records in one
    SQLite transaction.

No ClickHouse transaction spans these tables. Ordering plus replay-safe identities provide recovery.

### Stable AXFR version time

`dns_axfr_latest.updated_at` must be observation time, never loader wall-clock time:

- fresh probe: probe `observed_at`;
- delegation-only carry-forward/removal: `delegation_observed_at`.

This prevents an older batch retried later from replacing a newer endpoint state. Every retry must
produce byte-identical version and change timestamps.

## AXFR semantic rules retained

- Leading SOA proves `open`, even if later collection is capped or interrupted.
- Explicit refusal/not-authoritative outcomes are definitive `closed`.
- Timeout, transport failure, cancellation, malformed response, circuit-open, and skip are `unknown`.
- Unknown updates last-probe metadata but never changes the last definitive state.
- First-ever definitive closed establishes the implicit baseline without a change row.
- First-ever open creates an open change.
- Later open/closed flips create change rows.
- Delegation removal changes only `delegation_active`; it is not reported as AXFR closed.
- Private/special-use endpoints remain visible DNS evidence but are never dialed.

## Hostname ranking from the registry

Removing the direct `ctlogs.hostnames` read must not silently discard current ranking behavior.

Extend `commoncrawl_domain_hostnames` additively with a CT certificate-expiry signal:

```text
last_not_after SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
```

- Dagster CT rows carry `max(ctlogs.hostnames.last_not_after)`.
- Worker/AXFR discoveries use the epoch because they are not certificate observations.

The bounded registry query ranks each root domain by:

1. source priority `axfr > ct > static`;
2. live certificate (`last_not_after >= now()`);
3. `last_resolved` descending;
4. `last_seen` descending;
5. label ascending as a deterministic tie-break.

It performs grouping, explicit numeric source priority, ordering, and `LIMIT host_cap BY root_domain`
in ClickHouse. Go receives only the capped batch and does not construct SQL by interpolating domain
strings; use a driver-supported array/external-table boundary.

## Detailed implementation tasks

Tasks are dependency ordered. Each task should be independently reviewable and committed only after its
acceptance checks pass.

### Task 0: Capture production baseline and freeze the cutover contract

**Files:**

- Create a runbook section in `cc-dns-worker/README.md` or a dedicated current architecture document.
- No production code changes.

**Actions:**

- Record current counts and sizes for the active cycle DB tables.
- Record current ClickHouse row counts and latest timestamps for all five output tables.
- Record current cycle ID/phase, pending DNS count, AXFR queue state, and load watermarks.
- Record worker throughput, SQLite growth rate, AXFR throughput, and ClickHouse load latency.
- State the old-cycle disposition: finish+flush or explicitly abandon after independent verification.

**Acceptance:** Baseline and rollback evidence are captured before schema or binary cutover.

### Task 1: Finish the incremental Dagster hostname-registry source

**Files:**

- `dagster_v3/src/dagster_v3/defs/commoncrawl_hostnames/assets.py`
- `dagster_v3/src/dagster_v3/defs/commoncrawl_hostnames/definitions.py`
- `dagster_v3/tests/test_commoncrawl_hostnames.py`
- New ClickHouse migration pair for the specific sync-state table and `last_not_after`
- `dagster_v3/tests/test_clickhouse_migrations.py`

**Actions:**

- Add `last_not_after` to `commoncrawl_domain_hostnames` with an epoch default.
- Add a specific per-shard sync-state table containing:
  - shard index;
  - `ct_ingested_through`;
  - `domains_resolved_through`;
  - stable completion timestamp/run ID.
- Missing state means full bootstrap for that shard.
- Capture upper CT/domain watermarks before each insert.
- Incremental selection includes:
  - CT rows whose `last_ingested_at` advanced;
  - complete CT history for root domains newly entering `commoncrawl_domains` after the domain watermark.
- Apply the same wildcard/apex/proper-subdomain filters as bootstrap.
- Advance state only after the registry insert succeeds.
- Preserve worker-written AXFR rows; never truncate the registry.

**Tests:**

- Bootstrap includes legacy epoch CT rows.
- Incremental run includes a new CT observation for an existing root.
- Incremental run includes historical CT rows for a newly eligible root.
- Failure before checkpoint replays safely.
- Failure after insert but before checkpoint replays safely.
- AXFR registry rows survive every materialization.
- `dg check defs`, Ruff, focused tests, and the remaining Dagster suite pass.

**Release gate:** All 16 bootstrap partitions complete, incremental materialization succeeds, and
production validation proves the registry contains expected CT labels before Task 3 removes direct CT
reads.

### Task 2: Add AXFR latest transfer metrics

**Files:**

- New additive ClickHouse migration pair
- `dagster_v3/tests/test_clickhouse_migrations.py`
- `cc-dns-worker/internal/model/model.go`
- AXFR row-building tests

**Actions:**

- Extend `dns_axfr_latest` with latest probe metrics already staged locally:
  - `last_probe_records`;
  - `last_probe_bytes`;
  - `last_probe_truncated`.
- Preserve prior metrics on delegation-only or unprobed updates.
- Update them on every fresh probe, including unknown and partial results.
- Do not add a raw every-probe ClickHouse log. `latest` plus compact definitive changes remains the
  agreed collapsed model.

**Acceptance:** No AXFR information needed after local cleanup exists only in SQLite.

### Task 3: Replace input/hostsource with two bounded ClickHouse queries

**Files:**

- Replace or simplify `internal/input/input.go`
- Replace `internal/hostsource/hostsource.go`
- Corresponding unit and integration tests

**Actions:**

- Implement keyset domain pages from `commoncrawl_domains`.
- Implement exact-root bounded hostname reads from `commoncrawl_domain_hostnames` only.
- Keep source precedence and cap in SQL.
- Remove CT shard constants, CT normalization SQL, scan membership joins, and direct
  `ctlogs.hostnames` access.
- Return concrete domain/hostname batches; do not introduce a generic input-source interface.

**Tests:**

- Cursor page has no duplicates or omissions over a stable fixture.
- Empty roots are excluded.
- `--max-domains` stops at the durable fetched count.
- Only requested roots return labels.
- AXFR beats CT/static at the cap boundary.
- Live CT entries rank correctly through `last_not_after`.
- Multi-label subdomains remain intact.
- Query parameters are bound, not interpolated.

### Task 4: Introduce the bounded SQLite schema and recovery operations

**Files:**

- Rewrite `internal/store/store.go` schema and focused methods
- Rewrite/add `internal/store/store_test.go`

**Actions:**

- Create only the target tables described above for new cycle DBs.
- Add transactional domain-page insertion plus cursor advance.
- Add queue-depth and capacity queries.
- Add concrete claim methods for DNS and AXFR (`pending -> running`).
- Reset `running -> pending` once at process startup.
- Add ready-batch reads and acknowledged-batch deletion methods.
- Keep one SQLite writer connection and WAL mode.
- Reuse freed pages during the cycle; checkpoint/truncate WAL periodically and delete the cycle DB at
  cycle completion rather than vacuuming on every batch.

**Tests:**

- Crash before page transaction commit leaves cursor unchanged.
- Commit makes both work rows and cursor visible together.
- Startup resets running jobs without touching ready jobs.
- Claim never returns one job twice in one process.
- Acknowledgement deletion removes only the selected ready batch and dependent rows.
- Queue capacity never exceeds its configured bound.

### Task 5: Refactor DNS dispatch around bounded claims and in-memory hostnames

**Files:**

- `cmd/cc-dns-worker/scan.go`
- DNS pipeline tests
- `internal/model/model.go` only where concrete state shapes change

**Actions:**

- Replace the full seed and `PendingBatch` traversal with a source pump plus claim loop.
- Fetch hostnames after claiming a bounded DNS batch; never persist those input labels.
- Keep the existing discovery/auth scheduler separation.
- Commit DNS result, records, and optional AXFR job atomically.
- Define exactly which DNS statuses are trustworthy enough to reconcile delegation.
- Reset a job to pending or cancel the pipeline on infrastructure commit failure; do not convert it to a
  DNS observation outcome.

**Tests:**

- Kill after claim before result commit reprocesses the domain.
- Kill after DNS result commit preserves the ready result and AXFR job.
- Hostname fetch failure returns claimed DNS jobs to pending on restart.
- Private endpoints are stored in the AXFR payload but never dialed.
- DNS discovery failure does not enqueue false delegation removal work.

### Task 6: Add retry-safe DNS outbox flushing and deletion

**Files:**

- Simplify `internal/load/load.go`
- Loader failure-injection and integration tests
- `cmd/cc-dns-worker/run.go`

**Actions:**

- Replace rowid/domain-sequence watermarks with ready-batch selection and acknowledgement deletion.
- Write record observations, domain summaries, and registry updates for one ready batch.
- Use stable scan/event identity already present in output models.
- Delete local DNS rows only after all sinks succeed.
- Trigger by row threshold, time threshold, and backpressure; final cycle drain uses the same function.

**Tests:**

- Failure on each ClickHouse sink leaves all local rows.
- Acknowledgement loss replays without logical duplicates.
- Zero-record successful domains still write a summary and are deleted locally.
- Registry failure prevents deletion even when record/summary writes succeeded.
- Successful flush frees queue capacity immediately.

### Task 7: Convert AXFR into a concurrent bounded work lane

**Files:**

- `cmd/cc-dns-worker/axfr.go`
- AXFR pipeline tests
- `internal/store/store.go`

**Actions:**

- Remove scan-wide `SeedAXFRDomains` and the join back to DNS summary storage.
- Feed only claimed `axfr_work` rows.
- Preserve the existing separate AXFR scheduler, concurrency, caps, and endpoint-by-IP deduplication.
- Commit probes, selected zone, and `ready` status atomically.
- Treat a job with only skipped endpoints as ready reconciliation work, not as missing work.
- Reset interrupted running jobs to pending at process startup.

**Tests:**

- Kill after claim before commit requeues on restart.
- Commit failure leaves no partial probes/zone records.
- Shared IP is probed once and persisted for every NS hostname.
- All-skipped delegation completes without network calls.
- Open partial/truncated transfer remains open and preserves metrics.
- Unknown outcomes never become closed.
- Race suite proves feeder, workers, committer, and shutdown all join cleanly.

### Task 8: Add scoped AXFR prior lookup, flush, and cleanup

**Files:**

- `internal/load/load.go`
- `internal/load/integration_test.go`
- AXFR row-building tests

**Actions:**

- Replace full-table `LoadAXFRPrior` with a batch-root query.
- Compute latest rows and changes directly from ready jobs/probes/current delegations.
- Remove local change staging.
- Use stable observation/delegation time for `updated_at`.
- Write state changes before latest rows, then AXFR record observations and registry hostnames.
- Delete local AXFR rows only after all writes succeed.

**Tests:**

- Prior query is scoped to the ready root batch.
- First closed creates latest state but no change.
- First open creates a change.
- Open/closed flips create one retry-safe change each.
- Unknown preserves definitive state and changes no history.
- Removed endpoint becomes inactive, not closed.
- Non-dialable still-active endpoint preserves prior definitive state.
- Retried older observation cannot replace a newer latest row.
- Failure at each sink leaves the local job replayable.

### Task 9: Build the continuous bounded orchestrator

**Files:**

- `cmd/cc-dns-worker/run.go`
- `cmd/cc-dns-worker/run_test.go`
- CLI/default tests

**Actions:**

- Replace seeding/scanning/AXFR/flushing global phases with concurrent concrete loops:
  - ClickHouse source pump;
  - DNS claim/worker/committer lane;
  - DNS loader;
  - AXFR claim/worker/committer lane when enabled;
  - AXFR loader when enabled.
- Keep the cycle ID and completion definition explicit.
- New work stops at capacity while already claimed work continues.
- A transient input ClickHouse failure pauses source filling; workers drain existing local work.
- A transient output ClickHouse failure keeps local results and eventually applies backpressure.
- SQLite corruption/write failure cancels and joins every lane.
- Cycle completion requires source exhausted plus every local queue/outbox empty.
- Keep `scan` only if it invokes the same one-cycle engine and exits. Remove the separate `load`
  subcommand because the bounded engine owns acknowledgement and cleanup.

**Operator configuration:**

- domain input page size;
- hostname query batch size;
- active DNS work capacity;
- DNS ready flush batch/interval;
- AXFR work capacity;
- AXFR ready flush batch/interval;
- existing DNS and AXFR worker/QPS/timeouts;
- `--host-enrich` and `--axfr` remain optional with defaults `true`.

**Tests:**

- Synthetic corpus much larger than queue capacity completes with no loss.
- Maximum active SQLite rows remain bounded throughout the run.
- Source exhaustion with pending work does not complete early.
- Disabled AXFR creates no jobs/connections and does not block DNS cleanup.
- Cancellation and every lane failure leave restartable state and no goroutine leaks.

### Task 10: Remove obsolete worker and local-stage code

**Files:**

- Delete obsolete host-load/seed code and tests
- Simplify `internal/store`, `internal/input`, `internal/hostsource`, `internal/load`
- Update README architecture and CLI tables

**Remove:**

- full SQLite domain seeding and markers;
- `scan_hostnames` materialization;
- CT hash-shard host loading;
- seed membership mirroring;
- record/domain rowid watermarks;
- scan-wide AXFR phase/load marker;
- dead compatibility migrations for newly created cycle DBs;
- flags used only by removed seed/host-load phases.

**Acceptance:** Repository search finds no runtime reference to `ctlogs.hostnames`,
`dns_scan_seed_domains`, `scan_hostnames`, `MirrorSeedDomains`, or the removed state tables.

### Task 11: Update Ansible and operational observability

**Files:**

- `deploy/ansible/group_vars/cc_dns/vars.yml`
- `deploy/ansible/roles/cc_dns_worker/templates/cc-dns-scan.service.j2`
- `deploy/ansible/README.md`
- metrics package/tests

**Actions:**

- Add explicit queue/flush capacity flags with one set of defaults at the Ansible boundary.
- Remove obsolete seed/host-shard flags.
- Expose periodic metrics for:
  - domain cursor/fetched count/source exhaustion;
  - pending/running/ready DNS jobs;
  - DNS outbox records;
  - pending/running/ready AXFR jobs;
  - AXFR probe/zone-record backlog;
  - successful/replayed/failed ClickHouse flushes;
  - SQLite page count, free-page count, WAL size;
  - input/hostname query latency.
- Ensure deployment stops an existing service before replacing the binary and restarts it afterward.

**Acceptance:** An operator can distinguish input starvation, DNS saturation, AXFR backlog, ClickHouse
output failure, and SQLite backpressure from logs/metrics without inspecting the DB manually.

### Task 12: Performance and end-to-end verification

**Actions:**

- Run unit, race, static analysis, and ClickHouse integration suites.
- Use a fixed production-like sample containing:
  - domains with zero/one/many registry labels;
  - private and public NS endpoints;
  - shared NS IPs;
  - open, closed, unknown, removed, and all-skipped AXFR endpoints;
  - open zones at record/byte caps.
- Run with corpus size at least 10x configured queue capacity.
- Record peak SQLite main/WAL sizes and verify reuse after acknowledgement deletion.
- Compare DNS/AXFR ClickHouse outputs against the old pipeline for the same sample.
- Confirm no direct CT query is issued by the scanner.

**Acceptance:** Correct output parity, bounded local state, restart recovery, and no race/leak failures.

### Task 13: Production cutover

**Prerequisites:**

- `ctlogs.hostnames.last_ingested_at` migration deployed and new CT writer active.
- Dagster hostname bootstrap complete for all 16 shards.
- At least one incremental Dagster refresh completed successfully after bootstrap.
- Additive registry/AXFR migrations deployed.
- New worker artifact built and reviewed.

**Cutover steps:**

1. Stop `cc-dns-scan` before changing local state.
2. Record old cycle phase and local/ClickHouse watermarks.
3. Choose and document one old-cycle action:
   - preferred: finish and flush DNS, registry, and AXFR outputs with the old binary;
   - explicit abandon: independently prove acceptable output landed, then archive the old DB/state.
4. Copy the old cycle DB, WAL/SHM, and orchestrator state to a timestamped rollback directory.
5. Remove the active old orchestrator state so the new binary starts a new cycle and never attempts to
   interpret the old schema.
6. Deploy the new binary and Ansible configuration.
7. Start with a bounded smoke cycle/sample and verify all queue transitions and ClickHouse outputs.
8. Start the full service.
9. Monitor source cursor progress, bounded SQLite size, DNS throughput, AXFR backlog, replay counts, and
   ClickHouse errors through at least one sustained operating window.
10. Do not drop `dns_scan_seed_domains` during the rollback window.

### Task 14: Rollback and delayed legacy cleanup

**Rollback before cleanup migration:**

1. Stop the new service.
2. Preserve the new cycle DB for diagnosis.
3. Restore the prior binary/configuration and archived old DB/state.
4. Start the old service. Additive ClickHouse columns/tables remain compatible.

**Cleanup only after the rollback window closes:**

- Add a separate ClickHouse migration pair that drops `corpscout.dns_scan_seed_domains`.
- Register it in migration-contract tests.
- Remove obsolete migration-specific tests/docs while retaining historical migration files.
- Decide separately whether the legacy `dns_axfr_observations` table and legacy DNS aggregate baseline
  have completed their retention/backfill purpose; do not couple those drops to this pipeline cutover.

**Post-cleanup rollback:** The old worker requires the cleanup migration's down path to recreate
`dns_scan_seed_domains` before it can run. This is why cleanup is deliberately delayed.

## Release gates summary

1. **Registry gate:** bootstrap plus incremental Dagster CT sync proven.
2. **Schema gate:** additive hostname/AXFR columns deployed and tested.
3. **Correctness gate:** every cursor/claim/flush failure boundary has a restart test.
4. **Boundedness gate:** corpus larger than queue capacity completes without capacity violation.
5. **AXFR gate:** scoped prior lookup, stable event-time versions, and change semantics pass integration
   tests.
6. **Operational gate:** Ansible stop/deploy/start behavior and queue metrics verified.
7. **Cleanup gate:** sustained production window passes before seed staging is dropped.

## Final invariants

The implementation is complete only when all of these are true:

1. ClickHouse is the only complete input store.
2. SQLite contains only active work and unacknowledged output.
3. Source cursor and fetched work cannot diverge.
4. A dispatched-but-uncommitted DNS or AXFR job is retried after restart.
5. A ClickHouse-accepted-but-unacknowledged batch is replayed safely.
6. No local result is deleted before all dependent sinks succeed.
7. DNS can run ahead of AXFR only up to the configured bounded backlog.
8. AXFR unknown never becomes closed, and delegation removal never becomes an AXFR state change.
9. Older retried endpoint state cannot replace newer state.
10. Cycle completion means source exhausted and every local queue/outbox empty.
11. The scanner never reads `ctlogs.hostnames` or writes `dns_scan_seed_domains`.
12. Local disk use is bounded by configured working-set capacity, not corpus size.
