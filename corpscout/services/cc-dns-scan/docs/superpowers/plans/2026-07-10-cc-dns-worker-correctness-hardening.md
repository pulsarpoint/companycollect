# cc-dns-worker Correctness and Production Hardening Plan

**Date:** 2026-07-10  
**Status:** Proposed  
**Scope:** `corpscout/commoncrawl/cc-dns-worker`, its ClickHouse migrations, migration-contract tests, and its Ansible deployment  
**Goal:** Fix every confirmed review finding before AXFR and hostname enrichment are re-enabled in production.

## Why this plan exists

The worker builds, passes `go vet`, `staticcheck`, and the race-enabled unit suite, but several production paths are not safe under untrusted DNS data, transient network failures, ClickHouse retries, or process crashes. The most serious failures can:

- direct authoritative DNS and AXFR traffic to private or loopback addresses;
- record a timeout as an AXFR `open -> closed` transition;
- advance AXFR progress past results that exist only in memory;
- double record scan counts after a retry;
- overwrite last-good DNS summary state with an unobserved value;
- omit successful zero-record domains from incremental summary loading;
- load enrichment for domains outside the current scan and materialize whole ClickHouse shards in Go memory.

This plan treats those as correctness defects, not optional cleanup.

## Non-goals

- Do not add a generic service/facade layer around the worker.
- Do not add interfaces solely to mock concrete packages.
- Do not automatically expose leaked AXFR data to downstream products.
- Do not make incremental ClickHouse materialized views responsible for retry deduplication. They run on inserted blocks and can double-count replayed inserts.
- Do not rewrite historical implementation plans. Update current architecture/spec/README documents and add a superseding note where needed.

## Required engineering rules

- Apply the repository's Go logging/error rules: `log/slog`, `github.com/cockroachdb/errors`, wrap in lower layers, log once at worker boundaries, and never log secrets.
- Put defaults only in CLI/operator configuration. Runtime config is explicit and default-free.
- Keep SQLite and ClickHouse schema changes backward compatible until a deliberate cutover task says otherwise.
- Every ClickHouse migration uses the next free six-digit number at implementation time, includes `.up.sql` and `.down.sql`, and is registered in `dagster_v3/tests/test_clickhouse_migrations.py::EXPECTED_MIGRATIONS`.
- Do not put semicolons in `--` comments inside ClickHouse migration files.
- Every crash-safety claim must have a kill/retry or failure-injection test.
- Run `gofmt`, `go mod tidy`, `go vet`, `staticcheck`, and `go test -race ./...` before the final commit.

## Architecture decisions

### Data-preservation invariant: observation is not dial authorization

Never discard a DNS answer merely because it contains a non-public address. Preserve every discovered NS endpoint and every A/AAAA record verbatim, classify its address scope, and make the security/misconfiguration signal queryable. The public-address policy applies only when selecting a network destination for an authoritative DNS or AXFR connection.

For example, if a public authoritative server returns `A 10.20.30.40`, store that record and classify it as private, but never connect to `10.20.30.40`. Keep two explicit concepts in the model:

- observed endpoints/records: complete DNS evidence;
- dialable authoritative endpoints: the public subset allowed at the socket boundary.

The dedicated scanner resolver must not strip evidence that the worker is expected to record. Any resolver-side `private-address` policy must be evaluated against that requirement; application-side dial filtering and OS egress policy provide the safety boundary.

### 1. A failed probe is not a closed probe

AXFR has three semantic states:

| Class | Outcomes | May change `axfr_open`? |
|---|---|---|
| Definitively open | valid AXFR leading SOA observed, including capped/partial record collection after that proof | yes, to `true` |
| Definitively closed | explicit `REFUSED`, `NOTAUTH`, or configured equivalent | yes, to `false` |
| Unknown | timeout, dial/read/write failure, circuit open, cancellation, `SERVFAIL`, malformed response, skipped target | no |

Use a TCP AXFR preflight that reads the first DNS message and retains its RCODE. Closed servers finish after one request. Only a `NOERROR` response beginning with SOA proceeds to the streaming transfer; open servers are rare, so the second connection used to collect the zone is acceptable. This avoids parsing `miekg/dns`'s unexported `bad xfr rcode` error text.

### 2. Stage first, checkpoint second

AXFR workers never write ClickHouse and never advance a global dispatch cursor. They return per-domain results to one SQLite committer. The committer transactionally writes:

- one outcome per `(scan_id, root_domain, nameserver_hostname, nameserver_ip)`;
- the selected open zone's records, replacing prior AXFR records for that domain/scan;
- the domain's AXFR work status as `done`.

Resume queries pending AXFR domain rows. A killed process can repeat unfinished work, but cannot skip it.

### 3. Keep current state and state-change history separate

Create two ClickHouse tables:

- `dns_axfr_latest`: one latest row per domain/nameserver endpoint, using `ReplacingMergeTree(updated_at)`. It holds the last probe outcome, the last definitive AXFR state separately, and whether the endpoint is still active in the current delegation.
- `dns_axfr_state_changes`: only definitive changes, including stable `scan_id`, using a retry-safe key that keeps separate open periods.

Unknown probes may update `last_probe_outcome`/`last_probed_at`, but do not alter the last definitive AXFR state or create a change row. An endpoint removed from the delegation becomes inactive; removal is not misreported as AXFR closed. The change table remains the compact `open -> closed -> open` timeline requested by the product. The latest table distinguishes known closed, never definitively observed, currently unknown, and no-longer-active endpoints.

### 4. Make DNS record observations retry-safe before aggregating

The current `AggregatingMergeTree` accepts `scans=1`, so replay increments the count. Replace direct aggregate ingestion with:

1. an immutable logical observation table keyed by `scan_id` and record identity, physically implemented with `ReplacingMergeTree(loaded_at)`;
2. a refreshable summary that reads deduplicated observations and calculates `uniqExact(scan_id)`, `min(observed_at)`, `max(observed_at)`, and event-time `argMax` metadata.

Do not use an incremental materialized view for the summary because a replay would trigger the view before `ReplacingMergeTree` reconciles the raw row.

Preserve the pre-cutover aggregate table as a read-only legacy baseline and include it in the refreshed summary so historical counts and first-seen dates are not lost. Gate this design on the deployed ClickHouse version supporting refreshable materialized views. If it does not, use a scheduled full rebuild with atomic table exchange; do not fall back to replay-sensitive incremental aggregation.

### 5. Domain summaries have their own load stream

Add a monotonic SQLite completion queue populated in the same transaction as `CommitBatch`. Incremental loading tracks independent record and domain-summary watermarks. This ensures a successful domain with zero DNS records is still loaded.

### 6. Enrichment is scoped and streamed

When host enrichment is enabled, persist the exact scan membership to a ClickHouse staging table keyed by `(scan_id, root_domain)` with TTL. CT/registry shard queries join that membership, union and rank sources in ClickHouse, and stream bounded row batches into SQLite. No whole-shard `map[string][]HostLabel` is allowed.

## Delivery sequence

The tasks below are ordered dependencies. Tasks 1–6 are the minimum gate for AXFR. Tasks 7–9 are the minimum gate for trustworthy DNS history loading. Task 11 is the gate for host enrichment.

## Task 0: Immediate containment and rollout guard

**Files:**

- `deploy/ansible/group_vars/cc_dns/vars.yml`
- `deploy/ansible/README.md`

**Changes:**

- Remove `--axfr` and `--host-enrich` from the production flag set until their release gates pass.
- Document that this is temporary containment, not feature removal.
- Add an operator note that the base resolver still requires Task 1 because authoritative queries also use untrusted NS addresses.

**Acceptance:**

- Deployed command line has both experimental phases off.
- Rollback is a single variable change, but re-enable instructions point to the gates in this plan.

## Task 1: Classify all authoritative targets and block non-public dials

**Files:**

- Create `internal/resolve/target.go`
- Create `internal/resolve/target_test.go`
- Modify `internal/resolve/discover.go`
- Modify resolver metrics/model fields as needed for blocked-target counts

**Changes:**

- Parse discovered IPs with `net/netip`.
- Preserve every discovered address together with an explicit scope such as `public`, `private`, `loopback`, `link_local`, `cgnat`, `documentation`, `benchmark`, `multicast`, `unspecified`, or `reserved`.
- Derive the dialable authoritative set from only the explicitly supported public address space.
- Never send authoritative DNS or AXFR traffic to private, loopback, link-local, unspecified, multicast, CGNAT, documentation, benchmark, or reserved/bogon destinations.
- Keep configured recursive resolver addresses outside the authoritative dial policy; a local `127.0.0.1:53` resolver is an operator-owned Tier-1 dependency, not an authoritative target.
- Count blocked endpoints without logging a line per domain.
- If all endpoints are non-dialable, persist the observed delegation and return a status that distinguishes `no_public_ns_endpoints` from DNS transport failure.
- Apply the same rule at the final exchanger/AXFR socket boundary so a future caller cannot accidentally bypass discovery classification.

**Tests:**

- Table-driven IPv4/IPv6 allow/deny cases.
- A malicious delegation returning `127.0.0.1`, RFC1918, link-local, and CGNAT never reaches the exchanger.
- A mixed delegation preserves every endpoint but sends queries only to the public subset.
- A fully private delegation remains available as security evidence even though no endpoint is dialed.
- A private A/AAAA record returned by a public authoritative server is persisted verbatim and is never treated as a connection target.
- Race suite remains green.

**Commit gate:** Deploy this task independently before the rest of the feature work.

## Task 2: Preserve nameserver hostname-to-IP identity

**Files:**

- `internal/resolve/discover.go`
- `internal/model/model.go`
- `internal/store/store.go`
- `internal/store/store_test.go`

**Changes:**

- Add a concrete `NameserverEndpoint{Name, IP string}` model.
- Include address scope and dial eligibility in the endpoint model or in an adjacent explicit classification value; do not overload an empty/missing IP to mean blocked.
- Change discovery to retain which A/AAAA address belongs to which NS hostname.
- Keep the existing `Nameservers` and `NSIPs` summary arrays for compatibility, deriving them from all observed endpoints rather than only the dialable subset.
- Add `ns_endpoints` JSON to SQLite `scan_domains` through a checked schema migration.
- Change `AXFRTarget` to carry endpoints instead of a flat IP list.
- Normalize nameserver hostnames to lowercase without trailing dots and canonicalize IP strings.

**Tests:**

- Multiple IPs for one NS name.
- Multiple NS names sharing an IP.
- IPv4 and IPv6 round-trip through SQLite.
- Removed/changed endpoints remain distinguishable in state history.

## Task 3: Introduce typed AXFR outcomes and definitive preflight

**Files:**

- `internal/resolve/axfr.go`
- `internal/resolve/axfr_test.go`
- `internal/resolve/testserver_test.go`
- `internal/model/model.go`

**Changes:**

- Replace the boolean-only result with a typed outcome containing:
  - verdict: `open`, `closed`, or `unknown`;
  - reason: `transferred`, `refused`, `notauth`, `servfail`, `timeout`, `transport_error`, `circuit_open`, `cancelled`, `malformed`, `skipped`;
  - nameserver hostname and IP;
  - records seen, bytes seen, truncated flag, and collected supported records;
  - observation timestamp.
- Perform the first-message TCP preflight so RCODE is available without string matching.
- Treat a leading SOA as definitive evidence that AXFR is open. If later collection fails, retain `open` and mark the result partial/truncated with its reason.
- Treat scheduler errors and all transient transport outcomes as unknown.
- Remove the old NS-set `refused` cache from the per-server pipeline; it conflates different zones and transient errors.

**Tests:**

- `REFUSED` and `NOTAUTH` are closed.
- `SERVFAIL`, timeout, cancellation, circuit open, early disconnect, and malformed response are unknown.
- Leading SOA followed by a disconnect is open but partial.
- Record/byte caps produce open + truncated.
- No transfer goroutine or connection leak on every exit path.

## Task 4: Replace the AXFR cursor with a durable SQLite work queue

**Files:**

- `internal/store/store.go`
- `internal/store/store_test.go`
- `cmd/cc-dns-worker/axfr.go`
- Create `cmd/cc-dns-worker/axfr_test.go`

**SQLite schema:**

- `axfr_domains(scan_id, root_domain, status, error, observed_at, PRIMARY KEY(scan_id, root_domain))`
- `axfr_probes(scan_id, root_domain, name_server, name_server_ip, verdict, reason, records, bytes, truncated, observed_at, PRIMARY KEY(...))`
- `axfr_changes(scan_id, root_domain, name_server, name_server_ip, axfr_open, changed_at, PRIMARY KEY(...))`
- AXFR-specific load state for latest/change writes.

**Changes:**

- Seed `axfr_domains` from successful scan domains idempotently.
- Stream pending AXFR domains through feeder → workers → one committer, matching the proven base scan shape.
- In one SQLite transaction, replace a domain's probe rows and AXFR records, then mark its AXFR work done.
- Remove `axfrCollector`, `axfr_state.cursor`, and the unused `axfr_prev` SQLite mirror.
- On commit failure, cancel feeder/workers, drain/close channels, join every goroutine, and return.
- Mark the AXFR phase complete only after every domain is terminal and every staged ClickHouse write has advanced its load watermark.

**Failure-injection tests:**

- Kill after dispatch but before the first result commit; restart processes every domain.
- Kill after some commits; restart processes only pending domains.
- Fail a SQLite transaction; no domain is marked done.
- Fail a ClickHouse write after staging changes; restart reloads the same stable rows.
- Running the completed phase again makes no network requests.

## Task 5: Add retry-safe AXFR latest/change ClickHouse tables

**Files:**

- New next-free ClickHouse migration pair(s), currently expected to start at `000112`
- `dagster_v3/tests/test_clickhouse_migrations.py`
- `internal/model/model.go`
- `internal/load/load.go`
- `internal/load/load_test.go`
- `internal/load/integration_test.go`

**Tables:**

`corpscout.dns_axfr_latest`:

- key: `(root_domain, name_server, name_server_ip)`;
- version: `updated_at`;
- `delegation_active` and delegation observation time;
- last probe outcome/reason/time;
- `has_definitive_state`, definitive `axfr_open`, definitive state time, and state-producing `scan_id`;
- `ReplacingMergeTree(updated_at)`.

`corpscout.dns_axfr_state_changes`:

- stable retry key includes `scan_id`, domain, NS hostname, and NS IP;
- stores only definitive changes;
- keeps separate open periods because `scan_id` is part of the identity;
- `ReplacingMergeTree(changed_at)`.

**Changes:**

- Load prior definitive state from `dns_axfr_latest` once per AXFR load pass.
- Build `axfr_changes` in SQLite only where the current definitive state differs.
- Do not write an initial closed change for a previously unseen endpoint; write initial open and all later definitive flips. The latest table still records the initial closed state.
- Write changes first, latest state second, then advance SQLite AXFR load state.
- Unknown outcomes update only last-probe metadata in latest while preserving definitive fields.
- Compare the current delegation with prior active endpoints and mark removed endpoints inactive without creating a false close transition.
- Backfill legacy `dns_axfr_observations` rows with `name_server_ip = legacy.name_server` and an empty/explicitly-unknown hostname. Preserve the legacy table until verification is complete.
- Do not derive correctness from background merges; integration reads use `FINAL` or `argMax` as appropriate.

**Tests:**

- `open -> timeout` remains open with no new change.
- `open -> refused -> open` creates exactly three historical changes including the initial open.
- Repeating the same scan/load produces the same row counts and latest state.
- NS hostname/IP changes create separate endpoint history instead of leaving an ambiguously named IP row.

## Task 6: Make AXFR command behavior and summaries coherent

**Files:**

- `cmd/cc-dns-worker/scan.go`
- `cmd/cc-dns-worker/run.go`
- `cmd/cc-dns-worker/main.go`
- `internal/model/model.go`
- `internal/store/store.go`
- `internal/load/load.go`
- ClickHouse migration for deprecated summary columns when downstream audit permits

**Changes:**

- Make standalone `scan --axfr` execute the durable AXFR phase after base resolution.
- Make `run` use the same concrete phase function rather than a second behavior path.
- Stop presenting `commoncrawl_domain_dns_scan.axfr_*` as authoritative. The per-endpoint latest table is the source of truth.
- Audit downstream consumers. If none require the old summary columns, remove them and the dead Go/SQLite plumbing in a migration. If compatibility requires a transition period, mark them deprecated and create an explicit derived view rather than writing temporary false values each cycle.
- Make standalone `load` include staged AXFR latest/change loading.

**Tests:**

- `scan --axfr=false` performs no AXFR work.
- `scan --axfr=true` runs base scan, AXFR staging, and retry-safe load in order.
- `run` and `scan` produce equivalent SQLite/ClickHouse state for one bounded cycle.

## Task 7: Introduce retry-safe raw DNS record observations

**Files:**

- New ClickHouse migration pair(s)
- `dagster_v3/tests/test_clickhouse_migrations.py`
- `internal/model/model.go`
- `internal/load/load.go`
- `internal/load/integration_test.go`
- Any new DNS schema contract test under `dagster_v3/tests`

**Changes:**

- Create `corpscout.commoncrawl_domain_dns_record_observations` with:
  - `scan_id` in the sorting key;
  - record identity fields;
  - `source` and `discovery` as observation fields;
  - TTL, priority, RCODE, event time, and `loaded_at` version;
  - `ReplacingMergeTree(loaded_at)`;
  - a partition strategy that guarantees all retries of one observation remain in the same partition.
- Load SQLite records into this table, not directly into the aggregate summary.
- Preserve current `commoncrawl_domain_dns_records` as a legacy baseline at cutover.
- Build the replacement summary from legacy baseline plus deduplicated new observations.
- Define a cutover boundary: pause the writer, finish or deliberately abandon the active legacy cycle, snapshot the legacy aggregate, and start raw observation ingestion only with new scan IDs. Never let one scan contribute to both baseline and raw counts.
- Record that historical `scans` values may already contain retry inflation; preserve them as legacy facts, but do not claim they can be reconstructed exactly without historical scan IDs.
- Calculate:
  - `scans = legacy_scans + uniqExact(new.scan_id)`;
  - first/last seen by event time;
  - TTL, priority, RCODE, and last run with `argMax` over a deterministic event-time/version tuple;
  - provenance as stable sets/arrays, with optional explicitly named `last_source` and `last_discovery` compatibility fields.
- Confirm deployed ClickHouse supports refreshable materialized views before applying. Otherwise implement the documented scheduled atomic rebuild fallback.

**Tests:**

- Insert the identical observation twice and verify `scans=1` for the new period.
- Retry after a simulated failure between record and summary writes.
- Load observations out of order and verify event-time metadata wins.
- Observe the same record through query and AXFR and retain both provenance values.
- Legacy first-seen/count values survive cutover.

## Task 8: Split record and domain-summary load progress

**Files:**

- `internal/store/store.go`
- `internal/store/store_test.go`
- `internal/load/load.go`
- `internal/load/load_test.go`
- `internal/load/integration_test.go`
- `cmd/cc-dns-worker/load.go`

**Changes:**

- Add `completed_domains` with monotonic sequence, populated transactionally by `CommitBatch` for `status='done'`.
- Track `records_rowid` and `domains_seq` independently in `load_state`.
- Load domain summaries even when a domain produced zero records.
- Convert `FromStore` to the same bounded incremental loop; never materialize a full scan in memory.
- Stamp hostname registry rows with the scan's persisted observation/resolution time, not `time.Now()` at load retry time.
- Treat hostname registry write-back failure as a retryable flush failure; do not mark the cycle done or prune its DB.

**Tests:**

- Successful zero-record domain appears in `commoncrawl_domain_dns_scan`.
- Millions-row synthetic store maintains bounded batch memory.
- Retrying hostname write-back does not advance timestamps.
- A record-load failure cannot advance either watermark.
- A summary-load failure can retry without re-corrupting record counts.

## Task 9: Model DNS observation quality explicitly

**Files:**

- `internal/resolve/discover.go`
- `internal/resolve/query.go`
- `internal/model/model.go`
- `internal/store/store.go`
- ClickHouse scan-summary migration if new status columns are added

**Changes:**

- Replace the unconditional `Status="done"` with explicit `done`, `partial`, and `error` criteria.
- Track outcome for critical summary facts such as DS and DNSKEY independently from their boolean value.
- Do not turn a failed DS/DNSKEY query into “not present.”
- Define the minimum authoritative success required to replace the last-good summary.
- Preserve partial record observations without letting an unobserved boolean overwrite a known value.
- Preserve every returned A/AAAA value, including private, loopback, link-local, CGNAT, ULA, documentation, and reserved addresses.
- Classify returned address records for derived security findings such as `public_dns_private_address`, but never use a returned record value as authorization to dial that address.
- Fix query-error metrics so exhausted `SERVFAIL`/no-response attempts are counted consistently.

**Tests:**

- All Tier-2 attempts fail: summary is not treated as last-good.
- DNSKEY timeout does not become `dnssec_signed=false`.
- Definitive NODATA/NXDOMAIN is distinguishable from transport failure.
- A public authoritative response containing a private A/AAAA record is stored and classified, not filtered.
- Partial scan records remain loadable while last-good summary policy is respected.

## Task 10: Make the circuit breaker enforce its contract

**Files:**

- `internal/scheduler/scheduler.go`
- `internal/scheduler/scheduler_test.go`

**Changes:**

- Move the decisive breaker admission check to after slot acquisition and immediately before work.
- Recheck after a rate-token wait so requests queued before opening do not drain through an open circuit.
- Implement a single half-open probe grant; other callers fast-fail until that probe records success/failure.
- Keep breaker state and transition logic inside the existing concrete scheduler; do not add a service layer.
- Emit aggregate breaker transition metrics rather than per-request logs.

**Tests:**

- Queue many calls, open the breaker with the first failures, and verify queued calls do not execute.
- Exactly one half-open probe runs after cooldown.
- Successful half-open closes; failed half-open reopens.
- Concurrent race test remains green.

## Task 11: Scope and stream CT/registry hostname enrichment

**Files:**

- New ClickHouse migration for `dns_scan_seed_domains` with TTL
- `dagster_v3/tests/test_clickhouse_migrations.py`
- `internal/hostsource/hostsource.go`
- `internal/hostsource/hostsource_test.go`
- `internal/store/store.go`
- `cmd/cc-dns-worker/scan.go`
- Host enrichment integration tests

**Changes:**

- When `--host-enrich` is enabled, mirror the exact seeded membership to `dns_scan_seed_domains` idempotently.
- If resuming an older SQLite DB without mirrored membership, rebuild it by streaming local seeded domains.
- Join both CT and registry queries to `scan_id` membership.
- Union/dedupe/rank CT and registry candidates in ClickHouse and apply the cap to the final union.
- Use an explicit source-priority expression; do not rely on lexical `min` for business precedence.
- Preserve durable AXFR labels ahead of CT-only labels when applying the cap, with live CT and recency as secondary ordering.
- Stream result rows into SQLite in bounded chunks using a callback that represents the real streaming boundary.
- Remove whole-shard map construction and `InsertHostnamesMap`.

**Tests:**

- `--max-domains=3` enriches exactly those three domains.
- A custom seed query cannot add registry/CT rows for unseeded domains.
- AXFR/CT duplicate labels retain both provenance facts or the explicitly defined winning source.
- More than `host-cap` CT labels cannot evict higher-priority durable AXFR labels.
- Synthetic million-row shard stays within the chosen memory ceiling.

## Task 12: Fix cancellation, state validation, and pruning

**Files:**

- `cmd/cc-dns-worker/main.go`
- `cmd/cc-dns-worker/scan.go`
- `cmd/cc-dns-worker/run.go`
- `cmd/cc-dns-worker/run_test.go`
- `cmd/cc-dns-worker/scan_test.go`

**Changes:**

- Create a `signal.NotifyContext` in `main` and pass it through all commands/phases.
- On feeder, worker, committer, or loader failure: cancel, stop reporters, close channels from their owning goroutine, join all goroutines, then return.
- Replace fixed `time.Sleep` retries with context-aware timers.
- Treat only `os.IsNotExist` as “no state.” Return read, decode, or permission errors.
- Validate cycle ID and phase against the known phase set.
- Write state with temp file, file sync, rename, and parent-directory sync.
- Make pruning return errors, validate `keep-dbs >= 0`, and never prune the active DB.
- Ensure `--dir` exists and has expected permissions before starting a cycle.

**Tests:**

- Corrupt JSON, unknown phase, and unreadable state all fail without minting a new cycle.
- Cancellation during seed, scan, AXFR, load, and retry sleep exits promptly.
- Commit failure leaves no leaked workers/reporters under the race detector.
- Negative keep/load/timing flags fail validation rather than panic.

## Task 13: Make SQLite schema and decoding failures explicit

**Files:**

- `internal/store/store.go`
- `internal/store/store_test.go`

**Changes:**

- Replace best-effort `ALTER TABLE` calls with ordered, transactional SQLite migrations tracked by `PRAGMA user_version` or an equally direct version table.
- Return migration failures from `Open` with operation/table context.
- Make timestamp parsing return an error; never silently write Unix epoch.
- Propagate malformed JSON errors for nameserver arrays/endpoints.
- Verify migrated legacy DBs through fixture files representing each supported prior schema.
- Keep `SetMaxOpenConns(1)` and the single-committer model explicit; update comments that currently claim no other writes exist.

**Tests:**

- Fresh schema and each legacy schema reach the same final version.
- Forced migration failure prevents startup.
- Malformed timestamp/JSON fails the load instead of producing default data.

## Task 14: Centralize config validation, logging, and dependencies

**Files:**

- `cmd/cc-dns-worker/*.go`
- `internal/scheduler/scheduler.go`
- Lower-layer files that currently return unwrapped errors
- `go.mod`, `go.sum`
- `internal/metrics/metrics.go`, `internal/metrics/metrics_test.go`

**Changes:**

- Keep defaults in flag declarations only.
- Validate all cross-field and range constraints immediately after parsing.
- Remove `applyScanDefaults` and constructor fallbacks in scheduler/AXFR/host code.
- Pass explicit validated runtime config to constructors; constructors return errors for invalid direct callers where necessary.
- Adopt `slog` with stable fields: command, cycle_id, scan_id, phase, root_domain where appropriate, endpoint, counts, duration, and error.
- Use `cockroachdb/errors` for contextual wrapping and stack traces.
- Log phase errors once at their retry/exit boundary.
- Keep high-frequency metrics aggregated; never log tokens, passwords, environment contents, or DNS bodies.
- Parse and honor `CLICKHOUSE_SECURE`, including TLS configuration, or remove the variable and its deployment claim. Preferred action: honor it consistently with the other Corpscout clients.
- Run `go mod tidy` and correct direct/indirect dependency declarations.
- Apply `gofmt` to the existing metrics drift.

**Tests:**

- Table-driven validation for every flag, including allowed zero-as-disabled cases.
- Logging test verifies required attributes and redaction boundaries without snapshotting entire log lines.
- Secure ClickHouse option test does not require a network connection.

## Task 15: Harden deployment and verify schema before restart

**Files:**

- `deploy/ansible/roles/cc_dns_worker/**`
- `deploy/ansible/group_vars/cc_dns/vars.yml`
- `deploy/ansible/README.md`
- Add a direct `check` command or equivalent schema-check function in `cmd/cc-dns-worker`

**Changes:**

- Create a dedicated unprivileged service account.
- Separate immutable binary/config from writable state, for example `/opt/companycollect/...` and `/var/lib/cc-dns-worker`.
- Add systemd hardening compatible with outbound DNS and ClickHouse: `NoNewPrivileges`, empty capability set, protected system/home, private temp, and explicit writable paths.
- Remove global SSH host-key checking disablement; manage `known_hosts` explicitly.
- Add schema compatibility verification before service restart. The check compares required tables/columns/engines with the binary's expected contract.
- Keep DDL ownership in `clickhouse/migrations`. Deployment documentation runs `make clickhouse-migrate-up` explicitly before the Ansible play; Ansible refuses to start an incompatible binary rather than silently applying production DDL.
- Add service health checks for local unbound, ClickHouse connectivity, writable state directory, and schema compatibility.
- Keep AXFR and host enrichment off until Task 16 rollout gates pass.

**Tests:**

- `ansible-playbook --syntax-check` and `--check` against a fixture inventory.
- Molecule or disposable-VM test confirms non-root service can write state and reach required networks.
- Deployment with missing migration fails before restart and leaves the old service running.

## Task 16: Documentation, integration suite, and controlled rollout

**Files:**

- `README.md`
- `commoncrawl/ARCHITECTURE.md`
- `commoncrawl/docs/axfr-zone-transfer-spec.md`
- `commoncrawl/docs/hostname-discovery-spec.md`
- Deployment runbooks and integration tests

**Documentation changes:**

- Document all three commands and which ones read/write each ClickHouse table.
- Replace stale AXFR summary claims with latest/change table semantics.
- Document `unknown` explicitly and state that absence never means closed.
- Document retry guarantees, independent watermarks, bounded-memory load behavior, and enrichment staging.
- Correct record-type lists, dispatch wording, incremental-load status, and current module layout.
- Mark earlier AXFR/hostname implementation plans as superseded by this hardening plan where their runtime assumptions conflict.

**Required integration scenarios:**

1. Run the same SQLite stage through `load` twice; all raw/summary counts remain unchanged.
2. Inject a failure after ClickHouse accepts records but before SQLite advances; restart converges to the same result.
3. Kill the process at each AXFR checkpoint boundary; restart produces the same latest/change tables as an uninterrupted run.
4. Force an open endpoint to timeout; no close transition is written.
5. Scan a domain with zero records; its successful summary is loaded.
6. Return a private NS IP; no packet is sent to it.
7. Enrich a three-domain custom scan; no fourth domain is staged.
8. Feed out-of-order observations; event-time metadata remains correct.
9. Cancel every phase and assert no goroutine growth under `-race`.

## Release gates

### Gate A: Base scanner safety

- Task 1 deployed.
- Tasks 9, 10, 12, 13, and 14 complete.
- No internal/bogon authoritative dials in a bounded adversarial test.

### Gate B: DNS history loading

- Tasks 7 and 8 complete.
- Double-load and fail-after-insert tests prove exact convergence.
- Refreshable summary lag and runtime are measured on a production-sized sample.

### Gate C: AXFR

- Tasks 2–6 complete.
- `open -> timeout` test produces no false close.
- Kill/restart matrix produces identical latest/change state.
- Enable AXFR alone on a bounded domain sample before full-corpus rollout.

### Gate D: Host enrichment

- Task 11 complete.
- Exact seed scoping is proven with custom query and `--max-domains` tests.
- Peak worker and ClickHouse memory are measured on at least one full shard.
- Enable host enrichment separately from AXFR.

### Gate E: Production deployment

- Task 15 schema preflight and non-root service verified.
- Migrations applied through the repository migration workflow.
- Old binary rollback procedure documented, including schema compatibility.

## Production rollout sequence

1. Deploy the public-target filter with AXFR and host enrichment disabled.
2. Apply raw-observation, summary, and AXFR v2 migrations.
3. Deploy the retry-safe loader and run a bounded scan/load twice.
4. Compare legacy and v2 record summaries for counts, first/last seen, and metadata.
5. Enable AXFR for a bounded sample. Monitor `open`, `closed`, `unknown`, blocked-target, retry, and load-lag metrics.
6. Kill/restart the bounded run intentionally and verify convergence.
7. Enable AXFR full-corpus while host enrichment remains off.
8. Enable host enrichment on a bounded/custom seed and verify exact membership.
9. Enable host enrichment full-corpus after memory/query-cost review.
10. After one agreed retention period, remove legacy tables/columns only after downstream queries are audited.

## Final verification commands

```bash
cd corpscout/commoncrawl/cc-dns-worker
gofmt -w cmd internal
go mod tidy
go test -race -cover ./...
go vet ./...
staticcheck ./...
go build ./...

cd ../../dagster_v3
uv run pytest tests/test_clickhouse_migrations.py -q

cd ../commoncrawl/cc-dns-worker/deploy/ansible
ansible-playbook --syntax-check site.yml
```

Run ClickHouse integration tests only against the disposable/test database unless the operator explicitly authorizes a production smoke test.

## Finding-to-task coverage

| Confirmed finding | Covered by |
|---|---|
| Private/internal NS probing | Tasks 0–1, 15 |
| AXFR transport failure recorded as closed | Tasks 3–5 |
| AXFR cursor advances before durable result | Task 4 |
| Record retry doubles `scans` | Tasks 7–8 |
| Failed DNS queries overwrite last-good state | Task 9 |
| Zero-record summaries omitted | Task 8 |
| `scan --axfr` ignored and AXFR summary fields stale | Task 6 |
| NS hostname conflated with IP and removed endpoints stay ambiguous | Tasks 2, 5 |
| Host enrichment unscoped and whole-shard in memory | Task 11 |
| Circuit breaker drains queued calls and stampedes half-open | Task 10 |
| Corrupt orchestrator state starts/skips cycles | Task 12 |
| Commit failure leaks goroutines/reporters | Tasks 4, 12 |
| Full standalone load materializes tens of GB | Task 8 |
| `anyLast` metadata/provenance is not event-time safe | Task 7 |
| SQLite migrations/decoding silently ignore corruption | Task 13 |
| Duplicate defaults and missing range validation | Task 14 |
| Logging/error handling violates repository rules | Task 14 |
| `CLICKHOUSE_SECURE` is ignored | Task 14 |
| Root service and no migration/schema gate | Task 15 |
| README/spec drift | Task 16 |
| Gofmt and module metadata drift | Task 14 |
