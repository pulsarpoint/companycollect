# CommonCrawl RDAP Network Enrichment Implementation Plan

**Goal:** Add migration-owned ClickHouse storage and a partitioned Dagster asset that discovers
best-known RDAP network registrations for CommonCrawl IP addresses, derives exact CIDR segments,
and uses a ClickHouse `IP_TRIE` dictionary to avoid repeating lookups for already-known segments.

**Architecture:** ClickHouse remains the durable source of truth. `rdap_networks` stores the
normalized current RDAP registration plus its raw response; `rdap_network_segments` stores one
derived CIDR per row; `rdap_ip_lookup_results` records the exact IPs that were queried and provides
negative/retry caching. A migration-owned `IP_TRIE` dictionary maps an arbitrary IP to the
most-specific known `network_key`. A manual, 256-bucket Dagster asset reads
`commoncrawl_ip_addresses`, asks RDAP only for dictionary misses, writes in bounded batches, and
reloads the dictionary after durable inserts.

**Tech stack:** Python 3.14, Dagster 1.13, `dagster-clickhouse`, ClickHouse 26.5,
[`whoisit`](https://pypi.org/project/whoisit/) for IANA-bootstrapped RDAP queries, and `netaddr` for
exact start/end-range-to-CIDR conversion.

## Global constraints and semantics

- Do the ClickHouse migration tasks first. Do not land or launch the asset before the migration is
  applied in the target environment.
- Recheck the highest migration number on disk and the live `schema_migrations` version immediately
  before implementation. `000123` is highest at planning time, so `000124` is expected but not
  reserved.
- Use only the existing concrete `ClickhouseResource`. Do not add an interface, generic API service,
  DuckDB stage, dlt pipeline, or custom Dagster resource.
- The dataset is global RDAP network intelligence even though CommonCrawl supplies the first
  candidates. Table names therefore use `rdap_*`, not `commoncrawl_*`.
- The trie provides **best-known registration** lookup, not proof that an allocation contains no
  undiscovered child assignments. An RDAP parent allocation may overlap more-specific children.
  Store parent-derived segments with `segment_role='parent'` and exclude them from the lookup trie.
  Direct lookup results use `segment_role='lookup_result'`.
- The first version intentionally accepts the remaining best-known limitation: even a directly
  returned range can contain an undiscovered child assignment for a different IP. Document this in
  the asset description and materialization metadata. Exact leaf completeness requires RIR bulk
  registry ingestion or per-IP lookups and is out of scope.
- RDAP network registration does not define the BGP origin ASN. Keep ASN/origin-prefix enrichment in
  the existing MaxMind dataset or a future BGP asset; do not add an unlabeled `asn` column here.
- Preserve exact source data in canonical JSON text. Normalize parsed start/end strings separately
  for stable comparisons and CIDR derivation.
- Initial execution is manual-only. Use one Dagster pool to serialize partitions and protect remote
  RIR rate limits; add automation only after observed runtime and rate-limit behavior are known.

## ClickHouse contract

### `corpscout.rdap_networks`

One current row per RIR-unique registration, keyed logically by `network_key = lower(rir) || ':' ||
handle` and physically by `ORDER BY network_key` with `ReplacingMergeTree(fetched_at)`.

Required columns:

```text
network_key, rir, handle, ip_version, start_address, end_address,
name, registration_type, country_code, status,
registrant_handles, registrant_names,
parent_network_key, parent_handle, self_url, up_url,
registration_date, last_changed_at,
response_sha256, raw_response, fetched_at
```

Use nullable columns only where RDAP can omit the field. Keep registrants as arrays because an RDAP
object can contain multiple entities with the `registrant` role. Compress `raw_response` with ZSTD.
Expose `rdap_networks_current` as `SELECT * FROM ... FINAL`.

### `corpscout.rdap_network_segments`

One exact CIDR fragment per registration response:

```text
network_key, cidr, ip_version, prefix_length,
segment_role, response_sha256, derived_at
```

Use `ReplacingMergeTree(derived_at)` ordered by `(network_key, cidr)`. A registration fetched by an
IP query writes `lookup_result`; recursively fetched ancestors write `parent`. A later direct lookup
may replace the same `(network_key, cidr)` row and promote it to `lookup_result`.

Expose `rdap_network_segments_current` as a `FINAL` view filtered to
`segment_role = 'lookup_result'`. It must return one row per dictionary key:

```text
cidr, matched_cidr, network_key
```

The dictionary is migration-owned:

```sql
CREATE DICTIONARY corpscout.rdap_network_trie
(
    cidr String,
    matched_cidr String,
    network_key String
)
PRIMARY KEY cidr
SOURCE(CLICKHOUSE(DB 'corpscout' TABLE 'rdap_network_segments_current'))
LAYOUT(IP_TRIE)
LIFETIME(MIN 300 MAX 600);
```

`matched_cidr` intentionally duplicates the key so consumers can retrieve the exact matched prefix
without enabling key-as-attribute access.

### `corpscout.rdap_ip_lookup_results`

One current operational row per IP actually considered by the asset:

```text
bucket, ip, ip_version, lookup_status, network_key,
error_code, retry_after, queried_at
```

Use `ReplacingMergeTree(queried_at)` ordered by `(bucket, ip)` plus an
`rdap_ip_lookup_results_current` `FINAL` view. Controlled statuses are `found`, `not_found`,
`not_global`, and `retryable_error`. `network_key` is present only for `found`. Permanent
`not_found`/`not_global` results prevent pointless reruns; retryable failures are eligible again once
`retry_after <= now()`.

## Task 1: Pin the migration contract with failing tests

**Files:**

- Modify: `dagster_v3/tests/test_clickhouse_migrations.py`
- Create: `dagster_v3/tests/test_commoncrawl_rdap_assets.py`

- [x] Append the actual next migration name, expected to be
  `000124_corpscout_rdap_networks`, to `EXPECTED_MIGRATIONS`.
- [x] Add focused tests that read the migration and assert all three tables, three current views, the
  dictionary source, `LAYOUT(IP_TRIE)`, dictionary lifetime, `FINAL`, `segment_role` filtering,
  engine/version columns, ordering keys, raw-response compression, and down-migration drop order.
- [x] Add column-order helpers mirroring `test_commoncrawl_geoip_assets.py` so later Python INSERT
  tuples cannot drift from migration DDL.
- [x] Run from `dagster_v3`:

```bash
uv run pytest tests/test_clickhouse_migrations.py tests/test_commoncrawl_rdap_assets.py -q
```

Expected: fail because the migration files do not exist yet.

## Task 2: Create and apply the ClickHouse migration

**Files:**

- Create: `clickhouse/migrations/0000NN_corpscout_rdap_networks.up.sql`
- Create: `clickhouse/migrations/0000NN_corpscout_rdap_networks.down.sql`

- [x] Start the up migration with `CREATE DATABASE IF NOT EXISTS corpscout;` and create objects in
  dependency order: three tables, three current views, then the dictionary.
- [x] Make the down migration drop the dictionary first, then views, then tables.
- [x] Do not place semicolons inside `--` comments and do not use nullable `ORDER BY` columns.
- [x] Run the targeted migration tests until green.
- [x] Apply through the existing workflow from `corpscout/`:

```bash
make clickhouse-migrate-up
```

- [ ] Smoke-test `SHOW CREATE TABLE` for all objects, then verify an empty dictionary loads:

```sql
SELECT name, status, element_count
FROM system.dictionaries
WHERE database = 'corpscout' AND name = 'rdap_network_trie';
```

- [x] Run an up/down cycle in a disposable ClickHouse 26.5 instance before continuing, including
  IPv4/IPv6 longest-prefix lookups and parent-segment exclusion.
- [x] Add follow-up migration `000126_corpscout_rdap_dictionary_reader` after a password-protected
  ClickHouse smoke test showed that the original dictionary source attempted to authenticate as
  passwordless `default`. The follow-up uses a localhost-only least-privilege reader and passed an
  up/down/up ClickHouse 26.5 cycle.
- [ ] Apply migration `000126_corpscout_rdap_dictionary_reader` to the live ClickHouse deployment
  before launching the RDAP asset.

## Task 3: Add RDAP and range dependencies

**Files:**

- Modify: `dagster_v3/pyproject.toml`
- Modify: `dagster_v3/uv.lock`

- [x] Add compatible bounded dependencies for `whoisit` 4.x and `netaddr` 1.x with `uv add`.
- [x] Verify both import under the repository's Python 3.14 runtime.
- [x] Do not add `ipwhois`, another retry wrapper, or a second RDAP implementation.

## Task 4: Build fixture-driven RDAP normalization

**Files:**

- Create: `dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/rdap.py`
- Create: `dagster_v3/tests/fixtures/rdap/*.json`
- Modify: `dagster_v3/tests/test_commoncrawl_rdap_assets.py`

- [x] Add representative raw fixtures for aligned IPv4, non-aligned IPv4, IPv6, multiple
  registrants, missing optional fields, and an `up` parent link. Fixtures must contain no personal
  contact data beyond documentation/example values.
- [x] Implement direct typed normalization functions that produce one network row and one or more
  segment rows. Use `netaddr.iprange_to_cidrs(startAddress, endAddress)` and assert every derived
  CIDR is the same IP version and stays within the inclusive source range.
- [x] Canonically serialize the complete raw response and compute `response_sha256` from that text.
- [x] Extract only `registrant` entity handles/names into the normalized owner arrays; retain other
  roles only in `raw_response` for the first version.
- [x] Extract `self` and `up` links. Build `parent_network_key` only when both RIR and parent handle
  are known.
- [x] Test that non-aligned ranges produce multiple segments without widening the source range.

## Task 5: Add the concrete RDAP lookup boundary

**Files:**

- Create: `dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/client.py`
- Modify: `dagster_v3/tests/test_commoncrawl_rdap_assets.py`

- [x] Implement one concrete `RdapClient`, not an interface. Bootstrap `whoisit` once per asset run
  from IANA and use `whoisit.ip(..., include_raw=True)` for IP/CIDR queries.
- [ ] Send a stable, truthful product User-Agent/contact value and keep insecure HTTP/TLS overrides
  disabled.
- [ ] Keep requests serial. Apply an operator-configured delay between remote calls and stop the
  partition cleanly at `max_requests`.
- [ ] Follow a parseable `up` link to at most `parent_depth` ancestors. Store ancestor network rows
  and `parent` segments, but never insert them into trie lookup coverage.
- [ ] Treat unsupported/not-found as terminal lookup results. Convert transport, 429, and 5xx
  failures into `retryable_error` with bounded `retry_after`; do not log raw responses or contact
  payloads on errors.
- [x] Test with a focused fake callable/session at the HTTP boundary. Do not use live RDAP in unit
  tests.

## Task 6: Share the CommonCrawl IP bucket contract

**Files:**

- Create: `dagster_v3/src/dagster_v3/defs/commoncrawl_ip.py`
- Modify: `dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/assets.py`
- Modify: `dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/definitions.py`
- Modify: `dagster_v3/tests/test_commoncrawl_geoip_assets.py`

- [x] Move the existing 256-bucket partition definition, bucket-key parser, and
  `COMMONCRAWL_IP_ADDRESSES_ASSET` into the small shared module.
- [x] Rename concepts from GeoIP-specific to IP-specific while keeping partition keys and
  `cityHash64(ip) % 256` behavior unchanged.
- [x] Update GeoIP imports and tests without changing materialization behavior.
- [x] Run the existing GeoIP tests before adding the RDAP asset.

## Task 7: Implement the partitioned Dagster RDAP asset

**Files:**

- Create: `dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/assets.py`
- Create: `dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/definitions.py`
- Create: `dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md`
- Modify: `dagster_v3/tests/test_commoncrawl_rdap_assets.py`

- [x] Define the noun asset `commoncrawl_ip_rdap_networks`, dependent on
  `COMMONCRAWL_IP_ADDRESSES_ASSET`, with the shared 256 static partitions,
  `BackfillPolicy.multi_run(max_partitions_per_run=1)`, and pool `commoncrawl_rdap`.
- [x] Use typed config defaults only at the Dagster boundary: `candidate_scan_limit`,
  `max_requests`, `insert_batch_size`, `request_delay_seconds`, `parent_depth`, and retry-cache
  durations. Reject non-positive limits and negative delays/depth.
- [x] At run start, assert all migration-owned tables/dictionary exist and run
  `SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie` so a previous partial run's durable rows
  suppress duplicate calls.
- [x] Stream one bucket from `commoncrawl_ip_addresses FINAL`. Candidate SQL must use separate typed
  IPv4 and IPv6 dictionary checks and exclude current terminal/not-yet-retryable lookup results.
- [x] Reuse the existing IP-scope classification. Write `not_global` lookup rows without making an
  HTTP call.
- [x] Maintain an in-run `netaddr.IPSet` containing newly discovered `lookup_result` CIDRs. Recheck
  every streamed candidate before calling RDAP so multiple candidates from the same newly learned
  segment result in one request during the run.
- [x] For each successful lookup, flush in safety order: network row, segment rows, then the exact-IP
  `found` result. A crash can therefore cause a harmless repeat but cannot mark an IP complete before
  its reusable segment exists.
- [x] Insert bounded batches through the existing `ClickhouseResource`. After the final successful
  flush, reload the dictionary once. Do not reload after every response.
- [x] Return `MaterializeResult` metadata with candidates scanned, RDAP requests, direct networks,
  parent networks, segments, in-run trie skips, non-global IPs, terminal misses, retryable errors,
  rows written, per-RIR counts, and the best-known-semantics warning.
- [x] Keep the asset manual-only. Auto-loading `definitions.py` should register it without editing
  the root definitions module.

## Task 8: Verify resumability and the asset graph

- [x] Unit-test aligned/non-aligned IPv4 and IPv6 normalization, owner extraction, parent role,
  terminal and retryable failures, in-run deduplication, insert ordering, batch flushing, config
  validation, and materialization metadata.
- [x] Test migration column order against the Python INSERT column tuples.
- [x] Test a partial-run scenario where one network/segment batch already exists: after dictionary
  reload, the rerun must make no duplicate RDAP request for that known segment.
- [x] Run focused checks:

```bash
cd dagster_v3
uv run pytest tests/test_commoncrawl_rdap_assets.py \
  tests/test_commoncrawl_geoip_assets.py \
  tests/test_clickhouse_migrations.py -q
uv run ruff check src/dagster_v3/defs/commoncrawl_rdap \
  src/dagster_v3/defs/commoncrawl_ip.py \
  tests/test_commoncrawl_rdap_assets.py
uv run dg check defs
uv run dg list defs --json
```

- [x] With migrations applied, materialize one bucket using `max_requests=5`, inspect the three
  tables and dictionary status, then rerun the same bucket and verify known-segment requests drop to
  zero.
- [ ] Backfill a small set of buckets before launching all 256. Record per-RIR request/error counts
  and adjust the operator-facing delay only from observed limits.

## Acceptance criteria

- Migrations are applied before the asset is deployable and fully roll back in a disposable test.
- Every stored RDAP network preserves normalized fields plus the canonical raw response and hash.
- Every source start/end range is represented by exact, non-widening CIDR rows.
- `dictGet` returns the most-specific known direct-lookup segment and its `network_key` for both IPv4
  and IPv6.
- Parent registrations are connected by `parent_network_key` but parent-only segments cannot suppress
  direct IP lookups.
- Reruns and partial failures are safe; successful known segments, terminal misses, and delayed
  retries do not generate uncontrolled repeat requests.
- The asset graph, focused tests, Ruff checks, and `dg check defs` pass.
