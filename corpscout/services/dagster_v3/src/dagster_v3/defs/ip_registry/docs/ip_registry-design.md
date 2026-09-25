# ip_registry design doc

Records decisions, not code. Follows `docs/data-source-guidelines.md`; deviations are called out.

## 1. Source overview
- **Registry**: IANA (top-level address space) and the five RIRs (delegated-extended statistics).
  Two kinds of rows are kept from each RIR file: the `available`/`reserved` ranges (special
  segments) and, as a deliberate superset (ruling R2), the `allocated`/`assigned` records wide
  enough to cover an entire IANA block (holder blocks — Comcast's `73.0.0.0/8`, the UK MoD's
  `25.0.0.0/8`, the US DoD's `7.0.0.0/8`, JPNIC's `133.0.0.0/8`). No other allocated/assigned
  delegation is stored (owner decision 2026-09-25: "we don't want a local database for RDAP").
  Reference data, not company data — no entity key, no translation, no currency, no contacts
  (§6–8 of the guidelines do not apply).
- **Module**: `defs/ip_registry/` · no DuckDB file (see §3) · pool `ip_registry`
- **ClickHouse tables**: `corpscout.ip_registry_snapshots`, `ip_registry_iana_blocks`,
  `ip_registry_special_segments`, `ip_registry_holder_blocks`, `rdap_network_registry_class`
  (migration `000450`); trie exclusion in `000451`.
- **Datasets**:
  | dataset | url | format | size | cadence | auth? |
  |---|---|---|---|---|---|
  | IANA IPv4 address space | https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.csv | CSV, 256 rows (204 name an RIR) | 23 KB | rare (Last-Modified) | no |
  | IANA IPv6 unicast assignments | https://www.iana.org/assignments/ipv6-unicast-address-assignments/ipv6-unicast-address-assignments.csv | CSV, 51 rows (34 name an RIR), multi-line quoted notes | 6 KB | rare | no |
  | delegated-{afrinic,apnic,arin,lacnic,ripencc}-extended-latest (+ .md5) | ftp.afrinic.net/pub/stats/afrinic, ftp.apnic.net/stats/apnic, ftp.arin.net/pub/stats/arin, ftp.lacnic.net/pub/stats/lacnic, ftp.ripe.net/pub/stats/ripencc | pipe-separated, version line + summaries + records | 1–18 MB, 654k ipv4/ipv6 records of which 322k are available/reserved (kept as special segments) and ~112 are allocated/assigned records wide enough to cover an IANA block (kept as holder blocks) | daily | no |
- **Record count**: 307 IANA blocks; ≈322k special rows → ≈325k CIDRs in the trie (2026-09-25:
  afrinic 8,239 · apnic 100,042 · arin 81,738 · lacnic 46,821 · ripencc 84,732; IPv6 dominates
  because the RIRs enumerate free IPv6 space in fixed-size chunks). 112 holder rows kept
  (afrinic 0/2, apnic 1/37, arin 18/32, lacnic 0/1, ripencc 2/19, ipv4/ipv6), of which only 4
  actually cover an entire RIR-designated IANA block and change a classification (the ones
  named above); the rest sit in IANA blocks not designated to an RIR or cover no IANA block at
  all. The exact "does this holder cover an entire RIR-designated IANA block" test runs in SQL
  against the current IANA snapshot (view `ip_registry_iana_blocks_rule_current`, flag
  `unheld_rir_block`), so the kept holder rows are deliberately a superset — each RIR loader
  stays independent of the IANA loader. Holder selection uses the default
  `HOLDER_MAX_PREFIX = {4: 8, 6: 23}` (the longest prefix of any RIR-designated IANA block:
  every IPv4 block is a /8, the IPv6 blocks run /12–/23), not a limit re-derived per run;
  `source.holder_prefix_limits()` recomputes it from any IANA snapshot and is used only to
  confirm the constant in a test.

## 2. Ingest mode — and why
- Chosen: single-request full refresh per file, non-partitioned, daily. Every file is the whole
  registry; partitions would only add event-log churn (CLAUDE.md, `exchange_rates_v2` precedent).
- Daily rather than weekly: the files change every day and a block allocated yesterday would
  otherwise be classified `unallocated` (excluded from the trie, every address in it costing an
  RDAP call) for up to a week; the refresh is a 45 MB download plus seconds of work.
- Snapshots are dated (the file's end date / IANA's Last-Modified); `_current` views read the
  newest ledger snapshot per source; the loader keeps the current and the previous snapshot's
  partitions — across the IANA blocks, special segments and holder blocks tables alike — and
  drops the rest (`SNAPSHOTS_KEPT = 2`). The ledger keeps one row per load, with
  `holders_ipv4`/`holders_ipv6` counts alongside `records_*` (whole file) and `segments_*` (kept
  special rows); the IANA loader writes 0 for both holder counts.
- Format quirks: APNIC's `#` banner; RIPE NCC and LACNIC write seven fields for
  available/reserved records; IPv4 `value` is an address count, not always a power of two;
  ARIN's `.md5` is GNU style with a dated file name; the IANA IPv4 header is `Status [1]`; the
  IANA RDAP column glues two URLs; APNIC's version line has an empty start date, AFRINIC's is
  `00000000`, ARIN's asn dates can be `00000000`.

## 3. Loading — deviation from the DuckDB golden path
- Reader: Python (`csv` for IANA, `str.split('|')` for the RIRs), then batched native inserts.
- Why: the files are small (≤ 18 MB, ≤ 261k lines) and the kept rows need address arithmetic
  (IPv4-mapped integers, `ipaddress.summarize_address_range`) that DuckDB does not offer; parsing
  takes seconds. A DuckDB stage would add a file, a pool and no value. This is the deviation
  from §3.
- Validation of the whole file before anything is written: MD5 against the published digest —
  both the body and the `.md5` are refetched once more when the digests disagree
  (`fetch_delegated`; a mismatch that persists after the refetch still refuses the load) —
  version line parsed and its `records` equal to the record lines, each of the asn/ipv4/ipv6
  summaries equal to its type's count over all statuses, known statuses, parsable special/holder
  addresses, a file not older than the current snapshot, and no >5% drop in the whole-file
  ipv4/ipv6 record counts (`allow_shrink` run config overrides). The kept special-segment and
  holder-block counts are not guarded (they swing legitimately; RIPE has 4 available ipv4 rows).
  The ledger row is written after the rows.
- **Write order and convergence (ruling R5).** `store_snapshot()` never lets a reader see the
  current snapshot empty or partial. For a *new* date the `(source|registry, snapshot_date)`
  partition can only hold leftovers of an earlier failed load, so it is dropped first. For the
  *current* date (a re-published file with a different checksum, or a repair) the new rows are
  inserted next to the old ones with a fresh `loaded_at`, the ledger row is written, and only
  then does a synchronous mutation (`ALTER TABLE ... DELETE IN PARTITION (source, snapshot_date)
  WHERE loaded_at < ...`, `mutations_sync=2`) remove the rows written before that `loaded_at` —
  so FINAL readers see the old rows, then a superset, then exactly the new rows. `stored_rows_
  match()` compares FINAL row counts against the file after an identical-checksum snapshot: a
  mismatch (a run died between the ledger row and the delete) makes the next run reload the same
  date and report `repaired=True`; a fully consistent identical file only refreshes
  `verified_at`.
- **Retries and identity.** A loader retries the whole download twice, five minutes apart
  (`dg.RetryPolicy(max_retries=2, delay=300)`, set per-asset since op-level retry cannot be
  disabled through run config on the in-process executor); every download carries
  `User-Agent: CorpScout ip-registry/1.0`.

## 4. Transform
- None outside the load: derived columns (`first_ip`, `last_ip`, `cidrs`, `rir`) are computed in
  Python at load time. The classification of RDAP registrations is set-based SQL
  (`rdap_network_registry_class_derived`), which also applies the holder exclusion by reading
  `ip_registry_iana_blocks_rule_current`.

## 5. ClickHouse schema — and DDL deviations
- Grain: one row per (source, snapshot_date, block) / (registry, snapshot_date, special record)
  / (registry, snapshot_date, holder record) / (network_key) for classes.
  `ReplacingMergeTree(loaded_at | verified_at | classified_at)`.
- Address bounds are `IPv6` columns (IPv4 as `::ffff:a.b.c.d`) compared as `UInt128` so both
  families share one key space; the special-segment dictionary is an `IP_TRIE` over
  `ARRAY JOIN cidrs` (`RANGE_HASHED` cannot look up `UInt128` ranges on 26.5); the IANA rows are
  joined directly. The holder blocks table has the same columns, engine and partitioning as the
  special segments and is written by the same RIR loader before its ledger row; it feeds no
  dictionary.
- `PARTITION BY (source|registry, snapshot_date)`; no TTL (a TTL could delete the current
  snapshot of a source that stops publishing); retention is the loader's `DROP PARTITION`.
- No `raw_*` payloads and no `source_payload_hash` (the ledger keeps one checksum per snapshot).
- **Holder exclusion, one home.** `ip_registry_iana_blocks_rule_current` holds the current IANA
  blocks plus `unheld_rir_block UInt8`: 1 when the block is RIR-designated (`rir != ''`) and no
  holder block covers it entirely (`NOT IN` over a `CROSS JOIN` of the current IANA blocks and
  the current holder blocks). Both `REGISTRY_CONTEXT_SQL` (per-miss, one round trip) and
  `rdap_network_registry_class_derived` (bulk, a constant-key `LEFT JOIN`) count
  `unheld_rir_block = 1` blocks from this one view, so the exclusion is written once; an empty
  holder table excludes nothing. `corpscout_rdap_dictionary` gets no grant on
  `ip_registry_holder_blocks`: only the per-miss context query and the derived view read it.

## 6–7. Translation, contacts, currency
- Not applicable (reference data, no free text, no monetary amounts, no company contacts).

## 8. Scheduling
- `ip_registry_refresh_job` = the seven loaders (IANA + 5 RIRs) → `rdap_network_registry_class`;
  `ip_registry_daily` at `5 6 * * *` UTC (the APNIC file dated D appears on D+1 at +10:00),
  STOPPED by default, started at instance level. Checks: six `snapshot_fresh` checks (one on the
  IANA asset covering both IANA sources, one per RIR loader; RIR snapshot ≤ 3 days, verified ≤ 2
  days; IANA verified only) and `classification_complete`.
- The class asset itself refuses to run while the reference data is not ready (`reference_ready`
  raises `ValueError`) even though the per-miss path degrades gracefully to `unknown` — see §9 of
  the operations doc for what that means for already-classified registrations.

## 9. Issues found during processing
- The special segments are not small for IPv6 (≈309k rows): the plan sizes storage and the trie
  (48 MiB) for it instead of assuming a few hundred rows.
- ClickHouse resolves the `argMax(...) AS network_key` alias inside an outer `WHERE`
  (`ILLEGAL_AGGREGATION`), so the trie view's exclusion lives in a subquery.
- A `CROSS JOIN` with the IANA rows drops every network while the reference table is empty; the
  derived view joins on a constant key (`LEFT JOIN … ON n.one = b.one`) so the not-ready case
  still yields one `unknown` row per network.
- A registration's first address, not the queried IP, is the lookup point, so that the bulk view
  and the per-miss classifier ask the same question.
- APNIC's version line has an empty start date; AFRINIC's is `00000000`; ARIN's asn dates can be
  `00000000`.
- A scalar subquery such as `(SELECT (any(a), any(b), any(c)) FROM ... WHERE ...)` is typed
  `Nullable(Tuple(...))` on ClickHouse 26.5, and `clickhouse_driver` cannot read that type over
  the wire (`UnknownPacketFromServerError`, then a desynchronised connection failing every
  further query). `REGISTRY_CONTEXT_SQL`'s three scalar subqueries are each wrapped in
  `ifNull(..., <default>)`; the bulk derived view never returns a raw scalar subquery to the
  driver, so it did not have this problem.
- "Covered by a holder" is exact containment by a single holder record; an IANA block covered by
  two adjacent holder records together, neither alone sufficient, would still count as
  registry-level (not seen in today's data).

## 10. Verification
- Tests: `tests/test_ip_registry_source.py` (parsers including holder-block selection, the rule,
  freshness), `tests/test_ip_registry.py` (migrations 450/451, snapshot switch, same-date
  republish/repair, new-date failure and retry, retention, the special trie, the holder
  exclusion, loaders with fixture HTTP, SQL/Python parity, trie exclusion), enricher/worker tests
  in `tests/test_ip_enrichment_results.py` and `tests/test_commoncrawl_rdap_assets.py`.
- Live: migrate 450 → light_sync → run `ip_registry_refresh_job` → checks green → review the
  excluded-network report → migrate 451 → reload `rdap_network_trie` → start the schedule
  (`docs/operations/ip-registry-reference-data.md`). Keep the gap between the first reference
  load and applying 451 short: before 451, the RDAP trie (and an enricher's in-run cache, via
  trie hits) can still serve registry-level or unallocated segments.
