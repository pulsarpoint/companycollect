# dns_detect slice 4 Implementation Plan: service, storage, history, Dagster asset

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the resolver as a stateless HTTP service on the Dagster host, and store per-record resolutions and results in ClickHouse. The results come with history views, and a partitioned Dagster asset resolves new or stale records incrementally.

**Architecture:**
- **Go (`services/provider_recon`):**
  - the knowledge versions are split into `RulesVersion` and `IPVersion`;
  - `knowledge.LoadStore` reads the published documents through the existing `publish.Store`, via `changes/index.json` → `providers/<slug>/latest.json`;
  - a new `internal/detect/server` holds the knowledge in an atomic holder, with a reload loop and three handlers;
  - `dns-detect serve` runs it.
- **Ansible:** a new `dns_detect` role and a `dns-detect.yml` playbook in the same Ansible directory, targeting the `dagster` host.
- **ClickHouse:** migration 000468 with two tables and three views.
- **Dagster** (`defs/dns_detect`):
  - a resource client;
  - the SQL builders;
  - a 128-partition asset;
  - a version sensor and a daily schedule, both STOPPED by default.

**Tech Stack:** Go 1.25 (stdlib `net/http`, `sync/atomic`), Ansible, ClickHouse 26.5 (clickhouse-local for the tests), Python 3.14 with Dagster (uv).

**Spec:** `docs/superpowers/specs/2026-09-28-dns-detect-slice-4-storage-design.md` (owner-approved 2026-09-28).

## Global Constraints

- **The service is stateless.** It never reads ClickHouse. It listens on `127.0.0.1:8096` on the Dagster host, under systemd with `MemoryMax=4G`, `CPUQuota=400%` and `GOMAXPROCS=4`.
- **Request size:** `POST /v1/resolve` takes at most 50,000 records. The response is in input order, and any invalid line gives 400 with no partial output.
- **Staleness:** a resolution is stale when its `rules_version` differs from the current one, or when its `ip_version` differs and the record is A, AAAA or SPF TXT.
- **No deletes.** Views keep only each record's latest resolution. Inserts go in acknowledged batches, never one row per insert.
- **Migration number 000468.** Before applying it, the prod ledger must be checked. If 000466 (another session's) is still unapplied, STOP and ask the owner, because applying 467 first would make golang-migrate skip 466.
- **Git:** commit by explicit path from the `corpscout` root, in a worktree under `companycollect/.worktrees/`. Each commit ends with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- **Checks:**
  - Go: `go vet`, gofmt and `go test -race ./...`.
  - Dagster: `uv run pytest` on the new test files, `tests/test_clickhouse_migrations.py`, and `uv run dg check defs`.
- **Owner ruling:** ordinary tests, no scale harness. One partition is timed before all 128 run.

---

### Task 1: Split knowledge versions

- [ ] **Failing tests:**
  - a rule, key or service change moves `RulesVersion` only;
  - a range add or status change moves `IPVersion` only;
  - `Version()` combines both;
  - a `last_seen` tick moves neither.
- [ ] **Implement:** `Index.RulesVersion()` and `IPVersion()` (sha256 over the "rule/key/service" lines and the "ip" lines respectively); `Version()` = sha256 of both. Add them to the `Knowledge` interface only if the resolver needs them; the server uses `*Index`.
- [ ] **Checks, commit:** `feat(provider_recon): separate rules and IP knowledge versions`.

### Task 2: Load knowledge from the store

- [ ] **Failing tests** (with `publish.FSStore` holding `changes/index.json` and two documents):
  - `LoadStore` compiles both documents and returns the index's digest;
  - a missing `latest.json` for a listed provider is an error;
  - a missing index is an error.
- [ ] **Implement:** `knowledge.LoadStore(ctx, store publish.Store) (*Index, string, error)`. The string is the sha256 of `changes/index.json`, used for change detection.
- [ ] **Checks, commit:** `feat(provider_recon): detect/knowledge — load documents from the provider-recon store`.

### Task 3: Server

- [ ] **Failing tests** (`internal/detect/server`, `httptest`, FSStore):
  - `GET /healthz` gives 200 with the versions;
  - `GET /v1/knowledge` gives both versions, the document count and `loaded_at`;
  - `POST /v1/resolve` with 3 NDJSON records gives 3 lines in order, with `X-Rules-Version`/`X-IP-Version` headers;
  - an invalid record on line 2 gives 400 `line 2: …` and no body lines;
  - 50,001 records give 413;
  - `Reload` leaves the knowledge unchanged when the digest is unchanged, and swaps it when it changed;
  - a failed reload keeps the old knowledge and reports `reload_error` in `/healthz`.
- [ ] **Implement:** `server.New(store, logger)`, `(*Server).Reload(ctx) error`, `(*Server).Run(ctx, interval)` and `Handler()`. Knowledge sits in an `atomic.Pointer`, and a request uses the snapshot taken when it started.
- [ ] **Checks, commit:** `feat(provider_recon): dns-detect HTTP server (resolve, knowledge, health, reload)`.

### Task 4: `dns-detect serve`

- [ ] **Failing test:** `serve` with no S3 environment exits 2 with a message. The existing `resolve` tests still pass.
- [ ] **Implement:** `DNS_DETECT_LISTEN` (default `127.0.0.1:8096`), `DNS_DETECT_RELOAD_INTERVAL` (default 10m), the shared `CORPSCOUT_S3_*` variables and `PROVIDER_RECON_BUCKET`. Graceful shutdown on SIGTERM. The initial load must succeed before listening.
- [ ] **Checks, README section, commit:** `feat(provider_recon): dns-detect serve`.

### Task 5: Ansible role and deploy to the Dagster host

- [ ] **Add:**
  - `ansible/roles/dns_detect` (build, user, directories, env file root-only with `no_log`, systemd unit with the limits, health check on `127.0.0.1:8096`);
  - `ansible/dns-detect.yml`;
  - an inventory group `dns_detect_hosts` holding `dagster`;
  - `group_vars/dns_detect_hosts`;
  - a README section with the deploy command.
- [ ] **Deploy** with the keys exported as for provider-recon. Expected: the playbook finishes with `failed=0`; on the Dagster host `curl 127.0.0.1:8096/healthz` returns `ok` with versions; and the service is not reachable from outside the host.
- [ ] **Commit:** `feat(provider_recon): ansible role for dns-detect on the Dagster host`.

### Task 6: Migration 000468 and view tests

- [ ] **Failing tests** (`services/dagster_v3/tests/test_dns_detect_views.py`, clickhouse-local):
  - latest-resolution filtering hides the rows of an older resolution;
  - a fallback row is dropped where NS overlaps, and kept where no NS covers it;
  - windows 30 days apart merge, while windows 60 days apart stay two intervals;
  - `domain_services_now` keeps only intervals reaching the domain's latest scan of that record type.

  Migration tests: `EXPECTED_MIGRATIONS` plus a contract test.
- [ ] **Implement:** the `000468_corpscout_dns_detect.up/down.sql` migration with both tables and the three views, as in the spec.
- [ ] **Checks, commit:** `feat(clickhouse): 000468 dns-detect resolutions, results and history views`.

### Task 7: Dagster module `defs/dns_detect`

- [ ] **Failing tests:**
  - **Selection SQL:** apex/www/`_`/DKIM names, the routable types, and the anti-join against current resolutions with the IP-staleness rule.
  - **Resource:** `knowledge()` and `resolve(records)` against a fake HTTP server; a 400 raises.
  - **Asset,** with a fake client and a fake service: chunks of at most 50k; resolutions and results inserted in batches; metadata counts; zero candidates means success with 0; a service error fails the run without inserting that chunk.
  - **Sensor:** it records the first versions as a baseline, queues 128 runs when the versions change, and skips while a run is active.
  - **Schedule:** it queues 128.
- [ ] **Implement:** `resource.py` (`DnsDetectResource`, api_url default `http://127.0.0.1:8096`), `sql.py`, `assets.py` (asset `dns_record_services_clickhouse`, pool `dns_detect`, job, sensor, schedule).
- [ ] **Checks:** the new tests, `dg check defs`. Commit: `feat(dagster): dns_record_services asset with version sensor and daily schedule`.

### Task 8: Rollout

- [ ] **Precondition:** the prod ledger. If 000466 is unapplied, STOP and ask.
- [ ] **Apply** 000468 and verify the 5 objects exist.
- [ ] **Deploy dagster_v3** with the pristine-worktree recipe (refresh the dbt state; wait for the deploy lock, never delete it).
- [ ] **One partition:** launch `hash_003`, then record the duration, throughput, and the counts of resolutions and results.
- [ ] **Spot-check** `domain_services_history` for spotify.com, volvo.com and loopia.se against the CLI results from slices 1–3.
- [ ] **STOP** if the partition took more than 30 minutes, or if memory limits were hit. Otherwise launch all 128 as a server-side backfill.
- [ ] **After completion:** check partition coverage, then start the sensor and the schedule.
- [ ] **Update memory.**
