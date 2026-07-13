# CommonCrawl IP Enrichment Manual Checks Implementation Plan

> **For agentic workers:** Execute this plan task by task and keep the checkboxes current. Use
> `uv run` for every Dagster, pytest, and Ruff command.

**Goal:** Let an operator manually evaluate whether newly observed CommonCrawl IP addresses create
GeoIP or RDAP work, see exactly which of the existing 256 stable buckets contain that work, and then
manually materialize only the selected partitions.

**Architecture:** ClickHouse remains the source of truth for both the canonical IP set and completed
enrichment. Two unpartitioned, read-only Dagster asset checks compare
`commoncrawl_ip_addresses` with the logical current GeoIP and RDAP coverage across all buckets. A
single check-only job makes both checks easy to execute manually. The checks report non-zero bucket
counts as metadata but never launch an asset run. There are no sensors, schedules, automation
conditions, or automatic materializations.

**Tech stack:** Python 3.14, Dagster 1.13.9, `dagster-clickhouse`, ClickHouse 26.5, pytest.

**Follow-up to:**
`docs/superpowers/plans/2026-07-12-commoncrawl-rdap-network-enrichment.md`. That plan is historical
and must not be rewritten.

## User-visible behavior

1. The operator opens `commoncrawl_ip_addresses` in Dagster and manually evaluates its checks, or
   manually launches `commoncrawl_ip_enrichment_backlog_checks`.
2. Each check reads ClickHouse once and returns either:
   - **Passed:** no partition currently contains work for that enrichment asset.
   - **WARN / Failed:** one or more partitions contain work, with counts and partition keys in the
     evaluation metadata.
3. The operator manually materializes the reported `bucket_NNN` partitions on
   `commoncrawl_ip_geoip` and/or `commoncrawl_ip_rdap_networks`.
4. The operator manually evaluates the checks again to refresh the displayed result.

The word **Failed** is Dagster's UI label for an unmet asset check. In this design it means "manual
enrichment work is available," not that a pipeline run failed. The check uses WARN severity, is
non-blocking, and leaves the check-only run successful.

## Hard boundary: no automatic detection

`commoncrawl_ip_addresses` is updated inside ClickHouse by a materialized view and does not emit a
Dagster materialization event. Without a sensor, schedule, or external event, Dagster cannot notice
that ClickHouse changed by itself.

Therefore:

- The checks are always visible on the asset; their status is the latest manually evaluated
  snapshot.
- A new source IP does not change the Dagster UI until a person evaluates the checks.
- Materializing an enrichment partition does not refresh the checks; a person evaluates them again.
- Every result includes `checked_at_utc` so an operator can see how old the snapshot is.
- No polling, cursor, daemon, background task, or automatic `RunRequest` is introduced.

This limitation is intentional. Any future request for a live notification would be a separate
automation feature and is outside this plan.

## Current state

- `dagster_v3/src/dagster_v3/defs/commoncrawl_ip.py` defines an unpartitioned external
  `AssetSpec` named `commoncrawl_ip_addresses`.
- The source table is an `AggregatingMergeTree`; logical source reads must use `FINAL`.
- Every IP is independently assigned to `cityHash64(ip) % 256`, exposed as static partitions
  `bucket_000` through `bucket_255`.
- Adding one IP changes one bucket only. Existing addresses do not shift between buckets.
- Updating only an existing IP's `last_seen` value creates no GeoIP or RDAP work and must not warn.
- `commoncrawl_ip_geoip` has one logical current row per enriched IP.
- RDAP is not one row per IP: one CIDR can cover many source IPs, and exact lookup outcomes also
  represent terminal or retryable work state. Source and RDAP table row counts are not comparable.
- Both enrichment assets are already manually partitioned and contain their own candidate-selection
  semantics. Their compute behavior remains unchanged.

## Decisions locked in

1. **Use two manually evaluated asset checks.** Do not add sensors, schedules, freshness sensors,
   auto-materialize policies, automation conditions, or declarative automation.
2. **Attach both checks to the external source asset.** Use
   `asset=COMMONCRAWL_IP_ADDRESSES_ASSET.key`; Dagster 1.13.9 does not accept the `AssetSpec` object
   itself in `@dg.asset_check`.
3. **Keep the checks unpartitioned.** One evaluation queries all 256 buckets and returns only the
   buckets with work. The operator does not evaluate 256 individual checks.
4. **Add one check-only job.** It exists solely as a manual UI/CLI entry point, selects only the two
   checks, has no partitions, contains no executable assets, and emits no materializations.
5. **Use WARN, non-blocking results.** Pending enrichment is normal operational work, not a broken
   pipeline.
6. **Let coverage determine status.** Do not persist a second source fingerprint, watermark, or
   cursor. A warning clears only after downstream state accounts for the relevant source IPs.
7. **GeoIP detects source-driven missing coverage only.** Do not warn because the MaxMind database
   build epoch changed; that is not a change in `commoncrawl_ip_addresses`.
8. **RDAP uses coverage semantics, never row-count equality.** A newly added IP already covered by a
   known RDAP network requires no work and must not warn.
9. **Match the RDAP asset's executable candidate predicate.** An IP is actionable only when the trie
   does not cover it and it has no current exact lookup result, or its current result is a
   `retryable_error` whose `retry_after` is due. Terminal results and future retries are not
   actionable.
10. **Do not expose individual IP addresses.** Metadata contains counts, timestamps, and stable
    partition keys only.
11. **Propagate query errors.** A missing table/dictionary or ClickHouse error fails the check
    execution; it must never be converted into a misleading zero-backlog result.
12. **Keep storage unchanged.** No ClickHouse migration, source-table mutation, dictionary reload,
    or new persistent state belongs in this work.
13. **Keep code direct.** Do not add a scheduler service, repository interface, generic checker
    framework, or abstraction for a two-check workflow.

## Check contracts

### `commoncrawl_geoip_update_available`

Target: `commoncrawl_ip_addresses`.

Read `commoncrawl_ip_addresses FINAL` and anti-join it against logical current rows from
`commoncrawl_ip_geoip`. Join by `(bucket, ip)` and put an explicit `matched = 1` marker on the right
side; ClickHouse outer joins return default values, so `current.ip IS NULL` is not an acceptable
missing-row test.

Group only missing IPs by bucket. The result passes when the total is zero and returns a non-blocking
WARN result otherwise.

Do not include the City or ASN MMDB build epoch in this query. The existing GeoIP asset may still
refresh stale epochs when a person materializes it, but this check answers the narrower question:
"Did the canonical source gain IPs without GeoIP coverage?"

Required metadata:

```text
actionable_ip_count
actionable_bucket_count
actionable_partitions
oldest_uncovered_first_seen
checked_at_utc
```

`actionable_partitions` is JSON sorted by partition key:

```json
[
  {"partition_key": "bucket_007", "actionable_ip_count": 42},
  {"partition_key": "bucket_193", "actionable_ip_count": 3}
]
```

### `commoncrawl_rdap_update_available`

Target: `commoncrawl_ip_addresses`.

Reuse the effective predicate from `RDAP_CANDIDATES_SQL` across all buckets:

- Keep separate IPv4 and IPv6 branches so `dictHas` receives `toIPv4(...)` or `toIPv6(...)` with
  the correct type.
- Exclude an IP when `rdap_network_trie` already covers it.
- For a trie miss, include it when there is no exact current lookup result.
- Also include a `retryable_error` when `retry_after` is null or due.
- Exclude future retries and terminal outcomes such as `not_global` or terminal RDAP errors.
- Use an explicit `matched = 1` marker for the exact-result left join.

The first version should expose the reason split so the operator can distinguish new source work
from retry work:

```text
new_uncovered_ip_count
due_retry_ip_count
```

The check fails at WARN severity when their sum is non-zero. It does not warn merely because the
number of source IPs exceeds the number of RDAP network rows: one discovered network can cover many
IPs.

Required metadata:

```text
actionable_ip_count
new_uncovered_ip_count
due_retry_ip_count
actionable_bucket_count
actionable_partitions
oldest_uncovered_first_seen
checked_at_utc
```

Each entry in `actionable_partitions` includes `partition_key`, `actionable_ip_count`,
`new_uncovered_ip_count`, and `due_retry_ip_count`.

## Planned file changes

Paths below are relative to `corpscout/`.

- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_ip.py`
  - Add a validated `commoncrawl_ip_partition_key(bucket_index: int) -> str` inverse to the existing
    partition-key parser.
- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/assets.py`
  - Add the all-bucket, missing-only coverage query beside `GEOIP_CANDIDATES_SQL`.
  - Add a small direct fetch function returning the grouped rows.
- Modify `dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/assets.py`
  - Add the all-bucket actionable query beside `RDAP_CANDIDATES_SQL`.
  - Add a small direct fetch function returning the grouped rows.
- Create `dagster_v3/src/dagster_v3/defs/commoncrawl_ip_checks.py`
  - Define the two checks, their explicit `AssetCheckKey` values, the manual check-only job, and a
    `defs` bundle containing only the checks and job.
- Create `dagster_v3/tests/test_commoncrawl_ip_checks.py`
  - Test SQL behavior, result metadata, check execution, and definition registration.
- Modify
  `dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/docs/commoncrawl_geoip-design.md`
  - Document the manual source-change check and operator flow.
- Modify
  `dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md`
  - Document coverage semantics, bounded repeated runs, and the manual flow.

Do not modify ClickHouse migrations or the root `dagster_v3/definitions.py`. The defs-folder loader
will discover the new module, and the existing root definitions supply the concrete `clickhouse`
resource. Do not register `COMMONCRAWL_IP_ADDRESSES_ASSET` a second time in the checks bundle.

## Implementation tasks

### Task 0: Record the baseline

- [ ] From `companycollect/corpscout/dagster_v3`, inspect `git status --short` and preserve unrelated
  user changes.
- [ ] Run the focused existing tests:

```bash
uv run pytest \
  tests/test_commoncrawl_geoip_assets.py \
  tests/test_commoncrawl_rdap_assets.py -q
```

- [ ] Validate the current definition graph:

```bash
uv run dg check defs
```

### Task 1: Add failing contracts first

Create `tests/test_commoncrawl_ip_checks.py` before implementation.

- [ ] Test `commoncrawl_ip_partition_key` for `0`, `7`, and `255`, plus negative, `256`, boolean,
  and non-integer rejection.
- [ ] Test the GeoIP SQL contract contains:
  - `commoncrawl_ip_addresses FINAL`;
  - logical current GeoIP coverage;
  - `(bucket, ip)` join semantics;
  - an explicit right-side match marker;
  - no MMDB epoch predicate;
  - grouping and ordering by bucket.
- [ ] Test the RDAP SQL contract contains:
  - separate IPv4/IPv6 branches;
  - `dictHas('corpscout.rdap_network_trie', ...)`;
  - current exact lookup results;
  - explicit match-marker handling;
  - due retry semantics;
  - grouping and ordering by bucket;
  - no global source/network count comparison.
- [ ] Use a focused fake ClickHouse client only for row decoding. Assert that zero rows produce an
  empty backlog and non-zero rows preserve bucket index, count split, and oldest source timestamp.
- [ ] Assert invalid or duplicate bucket rows fail loudly instead of being silently merged.
- [ ] Run the new test and confirm it fails because the implementation does not exist:

```bash
uv run pytest tests/test_commoncrawl_ip_checks.py -q
```

### Task 2: Implement read-only all-bucket queries

- [ ] Add `commoncrawl_ip_partition_key` as the small validated inverse of
  `commoncrawl_ip_bucket_index`; reuse `COMMONCRAWL_IP_BUCKET_COUNT` rather than duplicating 256.
- [ ] Add `GEOIP_MISSING_BY_BUCKET_SQL` beside the existing candidate query.
- [ ] Read logical source and target rows (`FINAL` or the migration-owned current view), anti-join
  with a right-side marker, and return only non-zero buckets.
- [ ] Add `RDAP_ACTIONABLE_BY_BUCKET_SQL` beside the existing RDAP candidate query.
- [ ] Keep the candidate and checker predicates visibly parallel. Do not "simplify" the typed
  address branches into a query that changes ClickHouse dictionary behavior.
- [ ] Return deterministic rows ordered by numeric bucket.
- [ ] Keep the fetch functions read-only: no inserts, `OPTIMIZE`, dictionary reload, or other side
  effects.
- [ ] Run the query-contract and decoding tests until green.

### Task 3: Implement the manual checks

Create `dagster_v3/src/dagster_v3/defs/commoncrawl_ip_checks.py` without
`from __future__ import annotations`.

- [ ] Declare explicit keys:

```python
GEOIP_UPDATE_CHECK_KEY = dg.AssetCheckKey(
    COMMONCRAWL_IP_ADDRESSES_ASSET.key,
    "commoncrawl_geoip_update_available",
)
RDAP_UPDATE_CHECK_KEY = dg.AssetCheckKey(
    COMMONCRAWL_IP_ADDRESSES_ASSET.key,
    "commoncrawl_rdap_update_available",
)
```

- [ ] Implement both checks with:

```python
@dg.asset_check(
    asset=COMMONCRAWL_IP_ADDRESSES_ASSET.key,
    name=...,
    blocking=False,
)
```

- [ ] Acquire the existing concrete `ClickhouseResource`, execute the corresponding read-only
  query, and close the connection through its normal context manager.
- [ ] Return `passed=True` when there are no actionable rows.
- [ ] Otherwise return `passed=False` with `severity=dg.AssetCheckSeverity.WARN` and a direct
  description such as "45 IP addresses in 2 partitions need manual GeoIP enrichment."
- [ ] Put primary counts in integer metadata and bucket details in
  `dg.MetadataValue.json(...)`. Put `checked_at_utc` directly in metadata.
- [ ] Sort partition metadata by `bucket_NNN`; do not expose individual IP values.
- [ ] Let connection and query exceptions propagate as an execution error.
- [ ] Define the manual check-only job:

```python
commoncrawl_ip_enrichment_backlog_checks = dg.define_asset_job(
    "commoncrawl_ip_enrichment_backlog_checks",
    selection=dg.AssetSelection.checks(
        GEOIP_UPDATE_CHECK_KEY,
        RDAP_UPDATE_CHECK_KEY,
    ),
)
```

- [ ] Export `defs = dg.Definitions(asset_checks=[...], jobs=[...])`. Do not put the source
  `AssetSpec`, enrichment assets, resources, sensors, or schedules in this bundle.

### Task 4: Prove the behavior in tests

- [ ] Execute each check with zero backlog and assert a passing result with zero counts.
- [ ] Execute each check with grouped backlog rows and assert WARN/failed status, description,
  timestamp, total counts, and sorted JSON partition metadata.
- [ ] Assert a new GeoIP-missing IP changes only its stable bucket and `last_seen`-only changes do not
  create work.
- [ ] Assert GeoIP rows with older MMDB epochs still count as covered for this source-change check.
- [ ] Assert one RDAP trie network can cover several source IPs without producing work.
- [ ] Assert RDAP trie misses with no exact result are actionable.
- [ ] Assert due retryable results are actionable, while future retries and terminal results are
  excluded.
- [ ] Assert the definitions resolve both checks against the external
  `commoncrawl_ip_addresses` key.
- [ ] Resolve the named job and assert it is unpartitioned, contains only the two check steps, has no
  executable asset keys, and produces no asset materializations in a test execution.
- [ ] Assert the merged repository contains the two checks and manual job but introduces no sensor,
  schedule, or automation condition for this feature.
- [ ] Assert failed WARN checks leave the non-blocking check-only run successful.

### Task 5: Document the operator workflow

Update both existing design documents.

- [ ] Explain that check results are snapshots and never refresh in the background.
- [ ] Explain that the UI displays pending work as a WARN/Failed asset check.
- [ ] Document this manual sequence:

```bash
cd companycollect/corpscout/dagster_v3

# Refresh both read-only checks.
uv run dg launch --job commoncrawl_ip_enrichment_backlog_checks

# Manually run only a reported GeoIP partition.
uv run dg launch \
  --assets commoncrawl_ip_geoip \
  --partition bucket_007

# Manually run only a reported RDAP partition.
uv run dg launch \
  --assets commoncrawl_ip_rdap_networks \
  --partition bucket_007

# Refresh the displayed snapshot after manual work.
uv run dg launch --job commoncrawl_ip_enrichment_backlog_checks
```

- [ ] Note that production operators will normally use Dagster's UI; `dg launch` is the local CLI
  equivalent.
- [ ] Note that RDAP may need repeated manual materializations because one run is bounded by
  `max_requests=250` and a bucket can remain actionable.
- [ ] State explicitly that no background process launches any reported partition.

### Task 6: Validate the completed change

- [ ] Run focused tests:

```bash
uv run pytest \
  tests/test_commoncrawl_ip_checks.py \
  tests/test_commoncrawl_geoip_assets.py \
  tests/test_commoncrawl_rdap_assets.py -q
```

- [ ] Run Ruff on changed Python files:

```bash
uv run ruff check \
  src/dagster_v3/defs/commoncrawl_ip.py \
  src/dagster_v3/defs/commoncrawl_ip_checks.py \
  src/dagster_v3/defs/commoncrawl_geoip/assets.py \
  src/dagster_v3/defs/commoncrawl_rdap/assets.py \
  tests/test_commoncrawl_ip_checks.py
```

- [ ] Validate and inspect the definition graph:

```bash
uv run dg check defs
uv run dg list defs --json
```

- [ ] Run the surrounding regression slice:

```bash
uv run pytest tests/test_clickhouse_migrations.py tests/test_commoncrawl_*.py -q
```

- [ ] Review the final diff and search the new implementation for forbidden automatic behavior:

```bash
rg -n \
  "sensor|schedule|RunRequest|AutomationCondition|AutoMaterializePolicy" \
  src/dagster_v3/defs/commoncrawl_ip_checks.py \
  tests/test_commoncrawl_ip_checks.py
```

Expected result: only negative assertions or explanatory comments in tests, and no automatic
definition or run request in production code.

## Acceptance criteria

- [ ] A person can manually execute one check-only job that reads both backlogs.
- [ ] No check evaluation materializes either enrichment asset.
- [ ] GeoIP warns only for source IPs missing logical current GeoIP coverage, not for source
  `last_seen` changes or MaxMind epoch changes.
- [ ] RDAP reports executable work using trie plus exact-result/retry semantics, not table-count
  equality.
- [ ] Only non-zero stable `bucket_NNN` partitions and aggregate counts are displayed.
- [ ] Pending work is a non-blocking WARN, while query failures remain real execution failures.
- [ ] The displayed `checked_at_utc` makes snapshot age clear.
- [ ] No sensor, schedule, cursor, polling loop, automation condition, or automatic run launch is
  introduced.
- [ ] Documentation tells the operator to evaluate, manually materialize selected partitions, and
  evaluate again.
- [ ] `uv run dg check defs`, focused tests, Ruff, and the CommonCrawl regression slice pass.
