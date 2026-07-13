# CommonCrawl IP Enrichment Backlog Automation Implementation Plan

> **For agentic workers:** Execute this plan task by task and keep the checkboxes current. Use `uv run` for every Dagster, pytest, and Ruff command.

**Goal:** Automatically rematerialize only the stable CommonCrawl IP hash buckets that contain actionable GeoIP or RDAP work, while exposing backlog size and age in Dagster without treating normal, newly arrived IPs as pipeline failures.

**Architecture:** ClickHouse remains the authoritative work ledger. Two independently controlled Dagster polling sensors query exact per-bucket backlog state, select one non-active bucket fairly, and launch the existing partitioned assets. The GeoIP query uses the existing missing-or-stale MaxMind semantics. The RDAP query uses the current IP trie plus exact lookup-result/retry state; it never compares source IP count with RDAP network count. Sensor tick state is the live detector; post-run non-blocking checks report whether the materialized partition's oldest actionable IP is within an operational SLA, and a separate RDAP integrity check detects impossible coverage states. Existing pools, candidate selection, batching, retry markers, and durable tables remain unchanged.

**Tech stack:** Dagster 1.13.9 (`dg.sensor`, partitioned asset jobs, asset checks), `dagster-clickhouse`, ClickHouse 26.5, MaxMind MMDB metadata, pytest.

**Follow-up to:** `docs/superpowers/plans/2026-07-12-commoncrawl-rdap-network-enrichment.md`. That plan is historical and must not be rewritten.

## Current state and root cause

- `dagster_v3/src/dagster_v3/defs/commoncrawl_ip.py` defines 256 stable partitions, `bucket_000` through `bucket_255`, and a non-executable `commoncrawl_ip_addresses` `AssetSpec`.
- ClickHouse assigns each canonical IP independently with `cityHash64(ip) % 256`. Adding an IP affects one bucket; existing IPs never shift.
- `commoncrawl_ip_addresses` is updated by a ClickHouse materialized view, outside Dagster. Its `deps=` edges provide lineage but do not emit materialization or data-version events.
- `commoncrawl_ip_geoip` and `commoncrawl_ip_rdap_networks` are manual-only today. Both already select resumable work correctly inside one requested bucket.
- GeoIP has one current enrichment row per IP. RDAP does not: one discovered CIDR may cover many source IPs, while terminal and delayed-retry lookup rows provide separate completion state.
- The RDAP asset is bounded (`candidate_scan_limit=10_000`, `max_requests=250`) and can legitimately leave a bucket actionable after one successful run. Automation must therefore keep reconsidering backlog, not trigger only once per source-count change.

## Decisions locked in

1. **Use basic polling sensors, not asset sensors or declarative automation.** The source changes inside ClickHouse and produces no Dagster event. Custom polling is the direct Dagster mechanism for this boundary.
2. **ClickHouse decides whether work exists.** Sensor cursors are only for fair bucket rotation; they are not a second durable completion ledger.
3. **Create two sensors and two jobs.** GeoIP and RDAP need independent cadence, rollout, failure isolation, and kill switches.
4. **Keep the existing 256 static partitions.** Do not repartition `commoncrawl_ip_addresses`, change the hash, or add dynamic partitions.
5. **Do not compare global row counts.** GeoIP uses an anti-join plus MMDB build epochs. RDAP uses `dictHas(...)` plus current lookup status and `retry_after`.
6. **At most one outstanding automated run per asset initially.** Both existing pools default to limit 1. Emitting more requests would only grow the Dagster queue.
7. **Use a fair round-robin cursor.** A permanently large bucket must not starve other actionable buckets.
8. **Prevent overlap using Dagster run state.** Before requesting a partition, inspect non-terminal runs that target the same asset/partition, including manually launched asset runs where possible. Pause the relevant sensor during a manual multi-partition backfill because partitions targeted by a backfill may not have child runs yet.
9. **Use cursor-sequenced run keys as a second deduplication guard.** Re-evaluating the same cursor produces the same request identity, while a successfully advanced cursor lets persistent RDAP backlog run again. Never use only the partition key or backlog count.
10. **Apply a failure cooldown.** Respect Dagster automatic retries and suppress a recently failed/canceled partition for one hour after retries are exhausted, preventing a broken MMDB, dictionary, or network dependency from producing a run storm.
11. **Post-run checks measure source-age lag, not exact zero.** New IPs can arrive while an enrichment run is executing. Checks are non-blocking WARN checks that expose pending count and fail only when the oldest actionable source observation exceeds an SLA. They are not an independent polling mechanism; the sensor tick is the live detector.
12. **Deploy both sensors stopped.** Enable GeoIP first. Enable RDAP only after the dictionary-reader migration and manual rate-limit acceptance gates pass.
13. **No ClickHouse migration or new dependency.** Normal views would not reduce query cost, and all required state already exists.
14. **Keep code direct.** Do not add a generic backlog service, repository interface, scheduler factory, or new resource abstraction.

## Operational defaults

Lock these as named constants so tests can assert them and operators can review them in one place:

| Setting | Initial value | Reason |
|---|---:|---|
| GeoIP sensor minimum interval | 60 seconds | Local MMDB enrichment is fast and has no remote rate limit. |
| RDAP sensor minimum interval | 300 seconds | A default 250-request run is already approximately four minutes before parent work. |
| GeoIP outstanding automated runs | 1 | Matches the limit-1 `commoncrawl_geoip` pool without queue buildup. |
| RDAP outstanding automated runs | 1 | Preserves serialized RIR traffic. |
| Terminal failure/cancellation cooldown | 1 hour | Stops repeated automatic launches after Dagster retries are exhausted. |
| Run-key epoch | `v1` | Allows an intentional future cursor reset without colliding with historical run keys. |
| GeoIP pending source-age SLA | 1 hour | Allows normal arrival-during-run races but catches old uncovered source observations quickly. |
| RDAP pending source-age SLA | 24 hours | Allows bounded, rate-limited draining while still surfacing sustained old source observations. |
| Sensor default status | `STOPPED` | Requires an explicit production rollout decision. |

These are first-release values, not hidden environment-dependent defaults. Adjust them in a reviewed follow-up after production measurements.

## Planned file changes

Paths are relative to `corpscout/`.

- Create `dagster_v3/src/dagster_v3/defs/commoncrawl_ip_automation.py`
  - Two partitioned asset jobs.
  - Two polling sensors.
  - Two non-blocking backlog SLA checks plus one RDAP integrity check.
  - Run-state, cursor, fairness, and run-key helpers used by those two sensors only.
  - A `defs` bundle containing jobs, sensors, and checks; the root defs-folder loader discovers it automatically.
- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_ip.py`
  - Add a validated `bucket index -> bucket_NNN` inverse helper.
  - Add the small frozen actionable-bucket routing value shared by lower-level query code and automation, avoiding a circular `automation -> assets -> automation` import.
- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/assets.py`
  - Add all-bucket and one-bucket backlog queries beside `GEOIP_CANDIDATES_SQL`.
  - Add direct query functions returning typed pending-bucket rows.
  - Preserve the candidate predicate as the single semantic contract.
- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/assets.py`
  - Add all-bucket and one-bucket backlog queries beside `RDAP_CANDIDATES_SQL`.
  - Add direct query functions returning typed pending-bucket rows.
  - Preserve separate typed IPv4/IPv6 branches and retry-cache semantics.
- Create `dagster_v3/tests/test_commoncrawl_ip_automation.py`
  - SQL contracts, query result mapping, selection fairness, active-run suppression, run-key behavior, checks, and definition registration.
- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/docs/commoncrawl_geoip-design.md`
  - Replace “manual-only” with the sensor/check operating model and retain manual full-backfill instructions.
- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md`
  - Document exact actionable semantics, repeated bounded runs, sensor kill switch, and rate-limit rollout.

Do not modify the root `dagster_v3/src/dagster_v3/definitions.py`, the ClickHouse migrations, or the existing asset partition/pool definitions.

## Global constraints

- Work in `corpscout/dagster_v3` for implementation and validation commands; plan/document paths are relative to `corpscout/`.
- Do not add `from __future__ import annotations` to any module defining `@dg.asset_check`, a sensor, or another Dagster definition. Dagster 1.13.9 requires the concrete context annotations at definition time.
- Use the existing concrete `ClickhouseResource` and `MaxMindDatabaseResource`. Do not add interfaces or duplicate resource configuration.
- Keep both existing asset pools and processing configs unchanged. Sensors omit `run_config` and inherit asset defaults.
- ClickHouse migrations continue to own schema. This implementation adds read queries only.
- Never log IP addresses, RDAP payloads, MMDB paths, credentials, or connection strings from sensor/check code.
- Commit by explicit path only; never use `git add -A` in the shared worktree.
- Validate with `uv run pytest`, `uv run ruff`, and `uv run dg check defs` before completion.

---

### Task 0: Baseline and production-readiness gates

**Files:** none modified.

- [ ] **Step 1: Confirm the focused baseline**

Run from `corpscout/dagster_v3`:

```bash
uv run pytest \
  tests/test_commoncrawl_geoip_assets.py \
  tests/test_commoncrawl_rdap_assets.py -q
```

Expected at planning time: 44 tests pass. If the baseline has changed, record the new result before editing.

- [ ] **Step 2: Confirm definitions load**

```bash
uv run dg check defs
uv run dg list defs --assets 'key:commoncrawl_ip*' --json
```

Use the JSON listing to record current definition names. Assert the two enrichment assets' 256 static partition keys through a repository-definition Python/test assertion; `dg list defs --json` does not expose partition definitions.

- [ ] **Step 3: Verify RDAP prerequisites before any automated rollout**

Confirm migration `000126_corpscout_rdap_dictionary_reader` is applied and the dictionary is healthy:

```sql
SELECT name, status, element_count, last_exception
FROM system.dictionaries
WHERE database = 'corpscout' AND name = 'rdap_network_trie';
```

The code can be implemented without live credentials, but the RDAP sensor must remain stopped until this gate passes.

---

### Task 1: Pin the shared automation behavior with failing tests

**Files:**

- Create `dagster_v3/tests/test_commoncrawl_ip_automation.py`
- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_ip.py`
- Later create `dagster_v3/src/dagster_v3/defs/commoncrawl_ip_automation.py`

- [ ] **Step 1: Add tests for bucket conversion**

Assert:

- `0 -> bucket_000`, `7 -> bucket_007`, and `255 -> bucket_255`.
- Negative and `>=256` indexes raise `ValueError`.
- Round-tripping through `commoncrawl_ip_bucket_index` returns the original index.

- [ ] **Step 2: Add pure tests for fair bucket selection**

The selector accepts pending bucket indexes, active partition keys, a last-selected cursor, and capacity. Assert that it:

- skips active partitions;
- wraps from bucket 255 to bucket 0;
- rotates rather than always choosing the largest or smallest bucket;
- returns no more than the available capacity;
- returns no work for an empty backlog;
- does not mutate the input collection.

- [ ] **Step 3: Add tests for versioned cursor handling**

Use a small JSON cursor such as:

```json
{"version": 1, "last_bucket_index": 7, "next_dispatch_sequence": 842}
```

Assert that a missing cursor starts before bucket 0 with sequence 0, a valid cursor round-trips, and malformed/unknown-version cursors fail the sensor tick clearly instead of silently resetting fairness or request identity.

- [ ] **Step 4: Add tests for cursor-sequenced run keys**

Assert that the same cursor produces the same key and an advanced dispatch sequence produces a different key. The key must include the sensor/asset name, run-key epoch, dispatch sequence, and partition key, for example:

```text
geoip:v1:842:bucket_007
rdap:v1:842:bucket_007
```

Use `next_dispatch_sequence` directly in the request key, then increment it in the cursor returned with that request. For the example cursor above, the request key is `rdap:v1:842:bucket_007` and the returned cursor contains `next_dispatch_sequence: 843`. This permits a still-actionable RDAP bucket to run again after a completed tick while deduplicating daemon replay of the same cursor. Document that an operator who intentionally resets a cursor must also bump the run-key epoch (`v1` to `v2`) to avoid historical collisions.

- [ ] **Step 5: Run the focused tests and confirm failure**

```bash
uv run pytest tests/test_commoncrawl_ip_automation.py -q
```

Expected: collection/import failures because the automation module and inverse helper do not exist.

- [ ] **Step 6: Implement only the pure shared pieces**

In `commoncrawl_ip.py`, add a frozen actionable-bucket routing value with:

```text
bucket_index
actionable_count
oldest_actionable_dns_observed_at
newest_actionable_dns_observed_at
```

RDAP's domain-specific query result may additionally carry deferred-retry and integrity counts; do not force those semantics into the shared routing value. In `commoncrawl_ip_automation.py`, add direct cursor, fair-selection, active-run, and run-key helpers. Do not add a class hierarchy or protocol. Add the inverse bucket helper to `commoncrawl_ip.py` beside the existing parser.

Active-run detection must inspect non-terminal statuses (`QUEUED`, `NOT_STARTED`, `MANAGED`, `STARTING`, `STARTED`, `CANCELING`) and recognize both the named automation job and other active runs whose asset selection contains the target asset. Use the `dagster/partition` tag to suppress the same partition.

Resolve automatic-retry ancestry instead of treating every historical `dagster/will_retry=true` parent as permanently active. Follow `dagster/auto_retry_run_id` and run parent/root ancestry to the latest attempt:

- block while the latest attempt `is_complete_and_waiting_to_retry`;
- block while its retry child is non-terminal;
- apply failure cooldown only when the latest attempt is terminal failure/cancellation;
- do not cool down an earlier failed parent after a later retry succeeds.

---

### Task 2: Add exact GeoIP backlog queries

**Files:**

- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/assets.py`
- Test `dagster_v3/tests/test_commoncrawl_ip_automation.py`

- [ ] **Step 1: Add failing SQL-contract tests**

The all-bucket query must:

- read `corpscout.commoncrawl_ip_addresses FINAL`;
- join current GeoIP rows by stable bucket and canonical IP;
- identify missing rows through an explicit right-side `toUInt8(1) AS matched` marker and `ifNull(current.matched, 0) = 0` (not `current.ip IS NULL`, which is unsafe with ClickHouse default-valued LEFT JOINs);
- also identify rows whose City or ASN build epoch differs from the currently installed MMDB files;
- group current rows by `(bucket, ip)` in the all-bucket form;
- group by bucket;
- return pending count plus oldest `first_seen` and newest `last_seen`;
- avoid returning individual IP addresses to the sensor.

The one-bucket query used by the check must have the same predicate with a pushed-down `bucket = %(bucket_index)s` filter.

- [ ] **Step 2: Implement one semantic SQL source**

Build both aggregate forms from one fixed SQL template/helper so the sensor and check cannot drift. Preserve the current candidate semantics:

```sql
missing current row
OR current.city_db_build_epoch != installed city build epoch
OR current.asn_db_build_epoch != installed ASN build epoch
```

Use `argMax(..., enriched_at)` or the existing `FINAL` current view so merge timing cannot create false backlog.

`GEOIP_CANDIDATES_SQL` remains separately shaped because it streams individual rows. Add a real-ClickHouse parity test that, for the same bucket and MMDB epochs, proves the aggregate actionable count equals the number of rows returned by the candidate query. This is the guard against semantic drift; string tests alone are insufficient.

- [ ] **Step 3: Resolve installed MMDB epochs at sensor/check evaluation time**

Read the City and ASN build epochs from the paths returned by the existing `MaxMindDatabaseResource`. Keep this as a small concrete helper; do not create a second resource or cache epochs across evaluations. Validate database types exactly as the asset does.

- [ ] **Step 4: Add direct query functions and fake-client tests**

Add functions for:

- all pending GeoIP buckets, used by the sensor;
- one pending GeoIP bucket, used by the asset check.

Test parameter binding, empty results, integer conversion, aware UTC timestamps, and mapping to the shared pending-bucket value. Query errors must propagate so Dagster marks the sensor/check evaluation as failed.

Add a fixture covering missing, current, City-only stale, ASN-only stale, and duplicate physical versions without `OPTIMIZE`; prove `FINAL`/`argMax` produces logical counts.

- [ ] **Step 5: Preserve the existing asset behavior**

Run:

```bash
uv run pytest tests/test_commoncrawl_geoip_assets.py tests/test_commoncrawl_ip_automation.py -q
```

The existing candidate, batching, and MaxMind tests must remain unchanged and green.

---

### Task 3: Add exact RDAP actionable-backlog queries

**Files:**

- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/assets.py`
- Test `dagster_v3/tests/test_commoncrawl_ip_automation.py`

- [ ] **Step 1: Add failing SQL-contract tests**

The all-bucket query must derive directly from `RDAP_CANDIDATES_SQL` and retain:

- separate IPv4 and IPv6 branches;
- `toIPv4` only in the IPv4 branch and `toIPv6` only in the IPv6 branch;
- `dictHas('corpscout.rdap_network_trie', ...) = 0`;
- `rdap_ip_lookup_results_current`;
- eligibility when no exact result exists, detected through an explicit `toUInt8(1) AS matched` marker plus `ifNull(current.matched, 0) = 0`;
- eligibility for `retryable_error` only when `retry_after IS NULL OR retry_after <= now64(3)`;
- exclusion of trie-covered IPs, terminal results, `not_global`, and retries still cooling down;
- grouping by bucket with actionable count and source first/last-seen timestamps;
- separate deferred-retry count and earliest `retry_after`;
- `found_without_trie` and unexpected-status integrity counts.

The query must not compare `count(commoncrawl_ip_addresses)` with `count(rdap_networks)`, because a network row is reusable coverage rather than one row per IP.

- [ ] **Step 2: Implement all-bucket and one-bucket forms from one predicate**

The sensor query has no `candidate_scan_limit`: it measures actionable backlog. The existing asset query keeps its `LIMIT` and processing bounds unchanged. Push the bucket predicate into each source branch for the one-bucket check query.

Classify no-trie rows into mutually exclusive states:

```text
actionable              no current row, or retryable_error now due
deferred_retry          retryable_error with future retry_after
terminal_without_trie   not_global, not_found, or unsupported
found_without_trie      found marker exists but reusable trie coverage is absent
unexpected_status       any other current lookup_status
```

Trie coverage wins before this classification. The sensor launches only buckets with `actionable > 0`. Checks and metadata must still report deferred retries; zero actionable does not mean zero unresolved backlog.

- [ ] **Step 3: Add direct query functions and fake-client tests**

Test empty and multi-bucket responses, timestamp mapping, and bucket-key conversion. Add explicit tests demonstrating:

- an IP covered by the trie is not actionable even without an exact lookup-result row;
- a terminal `not_found` or `not_global` row is not actionable;
- a future retry is not actionable;
- a future retry appears in `deferred_retry_count` with `earliest_retry_after`;
- a due retry is actionable;
- `found_without_trie` and unknown statuses appear as integrity anomalies.

Use a real ClickHouse integration test for `IP_TRIE`; do not attempt to emulate its semantics with a misleading Python fake. For the same bucket fixture and a sufficiently large limit, prove the actionable count equals the row set from `RDAP_CANDIDATES_SQL`. Include IPv4, IPv6, one CIDR covering multiple source IPs, parent-only segments, terminal results, due/future retries, and duplicate physical lookup versions without `OPTIMIZE`.

- [ ] **Step 4: Do not reload the dictionary inside the sensor**

The asset already reloads the dictionary at safe execution boundaries. A sensor-side `SYSTEM RELOAD` would turn a read-only poll into a side-effecting control loop. Accept a harmless no-op run if the dictionary is briefly stale; document the 300–600 second dictionary lifetime.

- [ ] **Step 5: Profile the implemented read-only backlog queries**

Use production-like ClickHouse data to record runtime, rows read, and memory for both all-bucket queries. Run `EXPLAIN indexes = 1` as well as the real `SELECT`. Do not create a view or materialized table pre-emptively. If either query is too expensive for its proposed interval, increase the interval or optimize it before rollout.

The preferred fallback for an expensive all-bucket RDAP query is a rotating, bounded set of bucket probes for routing plus a slower full audit for monitoring. Do not replace exact candidate semantics with network row counts or timestamp guesses merely to make polling cheaper.

- [ ] **Step 6: Preserve the existing RDAP behavior**

```bash
uv run pytest tests/test_commoncrawl_rdap_assets.py tests/test_commoncrawl_ip_automation.py -q
```

All candidate, retry, batching, insert-order, and migration-contract tests must remain green.

---

### Task 4: Define the two jobs and polling sensors

**Files:**

- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_ip_automation.py`
- Test `dagster_v3/tests/test_commoncrawl_ip_automation.py`

- [ ] **Step 1: Add the partitioned asset jobs**

Use explicit names:

```text
commoncrawl_ip_geoip_job
commoncrawl_ip_rdap_networks_job
```

Each job selects only its existing asset (plus its associated check once Task 5 lands) and inherits the asset's 256 static partitions and existing pool.

- [ ] **Step 2: Add stopped-by-default sensors**

Use explicit names:

```text
commoncrawl_ip_geoip_backlog_sensor
commoncrawl_ip_rdap_networks_backlog_sensor
```

Both use `DefaultSensorStatus.STOPPED`. GeoIP requires the existing ClickHouse and MaxMind resources. RDAP requires ClickHouse only.

- [ ] **Step 3: Implement the evaluation order**

For each sensor tick:

1. Find active/non-terminal runs targeting the asset.
2. Treat pending automatic retries and partitions inside the terminal-failure cooldown as unavailable.
3. If the outstanding-run limit is reached, return a concise `SkipReason` without executing the full backlog query.
4. For RDAP, verify the dictionary exists and has no actual load failure/`last_exception`; fail the tick without launching work when unhealthy. Permit `NOT_LOADED` after a restart—the exact `dictHas` backlog query may perform the normal lazy load.
5. Query exact all-bucket backlog from ClickHouse.
6. On RDAP, write a structured error log when `found_without_trie` or unexpected-status counts are nonzero; this is integrity failure, not normal backlog. Continue draining independently actionable buckets, but never count the anomaly as healthy coverage.
7. Write one structured sensor log summary with actionable bucket count, actionable IP total, oldest actionable source timestamp, and—on RDAP—deferred retry totals/earliest retry. Never log individual IPs.
8. Remove unavailable partitions.
9. Select the next bucket after the cursor, wrapping fairly.
10. Return one `RunRequest` with `partition_key`, cursor-sequenced `run_key`, and non-sensitive tags containing trigger type and observed actionable count.
11. Atomically advance both the bucket cursor and dispatch sequence through the same `SensorResult` that contains the request.

When no work exists, return a `SkipReason` that reports zero actionable buckets. Do not advance the cursor.

- [ ] **Step 4: Keep RDAP repeatedly drainable**

Do not store “processed bucket” state in the cursor. Once a bounded RDAP run is terminal, the next sensor evaluation must query ClickHouse again. If the bucket remains actionable, the next dispatch sequence permits another run. After a failed run, let configured Dagster retries finish; if they are exhausted, observe the failure cooldown before another sensor-requested run.

- [ ] **Step 5: Add sensor tests**

Using `dg.instance_for_test()` (a temporary persistent test instance) and `build_sensor_context`, assert:

- empty backlog produces a skip and no run request;
- one pending bucket maps to the correct partition;
- only one request is emitted per tick;
- a running or queued partition is not requested again;
- a manual active run targeting the asset is respected;
- an active run prevents the expensive backlog query;
- cursor rotation is fair across repeated ticks;
- re-evaluating the same cursor produces the same run key;
- persistent backlog can be requested again after the cursor advances;
- a pending automatic retry blocks a duplicate request;
- a non-terminal retry child blocks a duplicate request;
- a successful retry child prevents cooldown of its failed parent;
- an exhausted latest retry failure/cancellation is cooled down, then becomes eligible again;
- an unhealthy RDAP dictionary fails the tick and launches no run;
- a present `NOT_LOADED` RDAP dictionary is allowed to lazy-load through the backlog query;
- RDAP found-without-trie or unexpected-status anomalies produce structured error visibility and are never counted as healthy coverage;
- structured tick summaries expose aggregate actionable/deferred state without individual IPs;
- failed ClickHouse/MMDB access produces a failed tick, not a false “no work” result;
- both sensors are stopped by default.

- [ ] **Step 6: Register definitions without editing root wiring**

Add a module-level `defs = dg.Definitions(jobs=[...], sensors=[...])`. Rely on the existing `load_from_defs_folder`; do not modify root `definitions.py` or duplicate the assets in this bundle.

---

### Task 5: Add post-run backlog and integrity checks

**Files:**

- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_ip_automation.py`
- Test `dagster_v3/tests/test_commoncrawl_ip_automation.py`

- [ ] **Step 1: Define one check per enrichment asset**

Use the same check name, scoped by asset key:

```text
backlog_within_sla
```

Both checks are non-blocking with `AssetCheckSeverity.WARN`.

These checks evaluate after their target partition runs. They verify residual state and preserve metadata on the asset, but they are not the live pre-run detector. While a sensor is running, its structured tick logs and requested-run tags provide current backlog visibility. When a sensor is intentionally stopped, this change does not promise an independent continuously updated audit; adding one would be a separate monitoring feature.

- [ ] **Step 2: Evaluate the materialized partition only**

Read `context.partition_key`, convert it to the stable bucket index, and use the one-bucket form of the exact backlog query.

Return metadata including:

```text
bucket_index
actionable_ip_count
oldest_actionable_dns_observed_at
newest_actionable_dns_observed_at
max_actionable_source_age_seconds
sla_seconds
```

RDAP additionally reports:

```text
deferred_retry_count
unresolved_ip_count
earliest_retry_after
oldest_unresolved_dns_observed_at
found_without_trie_count
unexpected_status_count
```

Do not include IP addresses, RDAP payloads, or MMDB paths in check metadata.

- [ ] **Step 3: Apply age-based pass/fail semantics**

- No actionable IPs: pass the SLA check, but do not label deferred retries as zero unresolved backlog.
- Pending IPs newer than the asset's SLA: pass, while still exposing count/age metadata.
- Oldest actionable IP older than the SLA: fail at WARN severity.

This avoids false alarms when an IP arrives between the asset's candidate read and its post-run check.

`first_seen` is the DNS observation timestamp, not the time the IP entered this enrichment queue. Name the metadata accordingly. A late historical DNS load may therefore warn immediately; this is a source-age/catch-up signal, not a precise queue-latency metric. A true processing-lag SLA would require a durable first-loaded timestamp and is outside this change. A future deferred retry remains healthy until it becomes due, but its count and earliest retry time remain visible.

- [ ] **Step 4: Add the RDAP integrity check**

Add a second non-blocking check on `commoncrawl_ip_rdap_networks` named:

```text
rdap_coverage_integrity
```

Return `AssetCheckSeverity.ERROR` and fail when the dictionary is missing/failed, `found_without_trie_count > 0`, or `unexpected_status_count > 0`. Include counts and dictionary status/exception summary, but never raw responses or IPs. `found` without trie coverage is not normal backlog: network segments are written before the found marker, and the asset reloads the dictionary after successful direct inserts.

- [ ] **Step 5: Ensure the jobs execute their checks**

Select the target asset and its checks explicitly, or add a definition-resolution test proving Dagster includes checks targeting selected assets. Do not create a separate scheduled check job.

- [ ] **Step 6: Add check tests**

Assert zero, recent, overdue, and deferred-only backlog results, including metadata and WARN severity. Test healthy integrity, `found_without_trie`, unexpected status, missing/failed dictionary, and recoverable `NOT_LOADED` state. Use a fixed UTC clock/helper so age tests are deterministic.

- [ ] **Step 7: Register checks**

Extend the module's `defs` bundle with all three checks.

---

### Task 6: Update design documentation and validate definitions

**Files:**

- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/docs/commoncrawl_geoip-design.md`
- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md`

- [ ] **Step 1: Update the GeoIP design**

Document:

- exact missing/stale backlog semantics;
- 60-second polling minimum;
- one outstanding partition;
- stopped-by-default rollout;
- age-SLA check behavior;
- automatic stale-MMDB bucket discovery;
- manual full backfill remains available.
- requirement to pause the GeoIP sensor during manual multi-partition backfills.

- [ ] **Step 2: Update the RDAP design**

Document:

- trie coverage versus exact lookup-result state;
- why network row count cannot measure coverage;
- bounded repeated runs until actionable backlog drains;
- retry cooldown behavior;
- actionable versus deferred/unresolved counts;
- integrity failure for found markers without trie coverage or unknown statuses;
- one outstanding partition and 300-second polling minimum;
- dictionary lifetime and absence of sensor-side reload;
- independent sensor kill switch and rate-limit monitoring.
- requirement to pause the RDAP sensor during manual multi-partition backfills.

- [ ] **Step 3: Run focused tests and Ruff**

```bash
uv run pytest \
  tests/test_commoncrawl_ip_automation.py \
  tests/test_commoncrawl_geoip_assets.py \
  tests/test_commoncrawl_rdap_assets.py -q

uv run ruff check \
  src/dagster_v3/defs/commoncrawl_ip.py \
  src/dagster_v3/defs/commoncrawl_ip_automation.py \
  src/dagster_v3/defs/commoncrawl_geoip \
  src/dagster_v3/defs/commoncrawl_rdap \
  tests/test_commoncrawl_ip_automation.py \
  tests/test_commoncrawl_geoip_assets.py \
  tests/test_commoncrawl_rdap_assets.py
```

- [ ] **Step 4: Validate the Dagster repository**

```bash
uv run dg check defs
uv run dg list defs --json
```

Use the JSON output to confirm both job names, both sensor names, and all three check names are registered. Use repository-definition tests/Python assertions—not `dg list defs`—to prove both jobs expose exactly 256 matching partition keys and both sensors are stopped by default.

- [ ] **Step 5: Run the broader relevant test suite**

```bash
uv run pytest tests/test_clickhouse_migrations.py tests/test_commoncrawl_*.py -q
```

---

### Task 7: Controlled rollout

**Files:** no runtime-code changes unless rollout reveals a reviewed defect. Step 6 updates the two existing design docs with measured operations data.

- [ ] **Step 1: Deploy with both sensors stopped**

Start the normal Dagster services and confirm sensor evaluations can be previewed without launching runs. Use `./scripts/dagster-dev.sh` for local UI testing, never bare `uv run dg dev`.

- [ ] **Step 2: Canary GeoIP manually**

Pick a bucket reported as pending, launch `commoncrawl_ip_geoip` manually, and verify:

- candidate/insert metadata is plausible;
- the one-bucket backlog falls;
- the check metadata is correct;
- no other bucket is remapped or modified unexpectedly.

Pause the GeoIP sensor for any manual multi-partition backfill. A single manual partition run can coexist with the stopped rollout canary; a backfill target may contain partitions whose child runs do not exist yet and therefore cannot be discovered by ordinary active-run inspection.

- [ ] **Step 3: Enable only the GeoIP sensor**

Observe at least one full backlog-drain cycle. Track sensor query duration, run duration, Dagster queue depth, pending bucket count, oldest backlog age, and failures. Stop the sensor immediately if query cost or queue behavior differs materially from the baseline.

- [ ] **Step 4: Complete the RDAP manual acceptance gate**

Before enabling automation:

- migration `000126` is live;
- the dictionary status is healthy;
- a small bucket run with `max_requests=5` or `25` succeeds;
- a rerun demonstrates trie/lookup reuse;
- observed 429/5xx/retryable counts and request pacing are acceptable.

- [ ] **Step 5: Enable the RDAP sensor**

Start with one outstanding run and the existing one-second request delay. Monitor RIR counts, retryable errors, `request_limit_reached`, queue depth, oldest actionable age, and whether the same bucket is fairly revisited until drained.

- [ ] **Step 6: Record production measurements**

Add the observed sensor query/runtime and sustainable backlog-drain rates to the two design docs. Any change to interval, SLA, concurrency, or RDAP request limit is a separate reviewed adjustment.

## Rollback

- Stop the affected sensor in Dagster. The other sensor remains independent.
- Do not delete runs, truncate tables, roll back migrations, or change bucket count.
- Manual partition launches continue to work.
- All durable candidate, enrichment, trie, terminal-result, and retry state remains in ClickHouse.
- If a queued run is genuinely stale, cancel it through normal Dagster controls and verify the corresponding pool slot is released with the existing health-check procedure.

## Acceptance criteria

- A newly inserted canonical IP makes exactly its stable hash bucket actionable.
- The next eligible sensor tick requests that partition only; existing bucket assignments do not shift.
- GeoIP detects both missing enrichment and stale installed MMDB epochs.
- RDAP excludes trie-covered IPs, terminal results, non-global IPs, and retries still in cooldown.
- RDAP reports deferred retries separately and never labels zero actionable work as zero unresolved backlog.
- Found markers without trie coverage and unknown lookup statuses are surfaced as integrity errors.
- A bounded RDAP run that leaves work is safely requested again after it finishes.
- No sensor creates overlapping or unbounded queued runs for its target asset.
- Pending automatic retries and recent terminal failures cannot cause duplicate/failure-storm launches.
- Manual multi-partition backfills are documented to require pausing the corresponding sensor.
- Fair cursor rotation prevents bucket starvation.
- Checks show pending counts and source-observation age without failing for fresh arrivals; overdue backlog produces a non-blocking WARN with the timestamp caveat documented.
- While enabled, sensor tick logs provide current aggregate backlog visibility; post-run checks are explicitly documented as residual verification rather than independent polling.
- Sensor/query failures are visible as failed evaluations and are never converted to “no work.”
- Both sensors are independently controllable and initially stopped.
- No ClickHouse schema, hash contract, existing asset compute behavior, or root definition wiring changes.
- Focused tests, Ruff, `uv run dg check defs`, and definition listing all pass.
