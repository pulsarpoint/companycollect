# IP Registry Reference Data Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Load only the special segments of the IP address space into ClickHouse — the IANA top-level blocks with their designation and status, and the `available`/`reserved` ranges of the five RIRs' delegated-extended files — refresh them daily from Dagster, classify every cached RDAP registration against them (reusable / registry_level / unallocated) with one rule written in SQL and in Python, and use that classification to keep registry-level and unallocated blocks such as `APNIC-AP` (103.0.0.0/8) out of `rdap_network_trie` and out of every enricher's reuse caches.

**Architecture:** A new Dagster module `defs/ip_registry` downloads seven small public files daily (IANA ipv4/ipv6 CSVs, `delegated-{afrinic,apnic,arin,lacnic,ripencc}-extended-latest` + `.md5`), validates each whole file (checksum, version line, record and summary counts, not older than the current snapshot, no sharp shrink of the whole-file record count) and inserts only its special segments as a dated snapshot; a ledger row written last makes the snapshot current, and the loader then drops every partition of that source except the current and the previous snapshot. One small `IP_TRIE` dictionary (`ip_registry_special_trie`, ≈325k CIDRs, 48 MiB) answers "is address x in available/reserved space"; the ≈307 IANA rows are joined directly. The view `rdap_network_registry_class_derived` applies the rule to `rdap_networks_current`; the asset `rdap_network_registry_class` persists its output, and migration 000450 makes the trie's source view exclude every network whose class is not `reusable`, so existing poisoned entries stop being served and later reclassifications take effect at the next dictionary reload without code changes. `RdapEnricher` and the legacy bucket worker classify each new registration with the Python twin of the rule (one context query per RDAP miss), write its class row before its segments, and never put a non-reusable registration into their in-run caches. No allocated/assigned delegation is stored anywhere.

**Tech Stack:** Python 3.14 (stdlib `ipaddress.summarize_address_range` for the range→CIDR cover), Dagster 1.13.9, ClickHouse 26.5 (clickhouse-driver 0.2.10: `IPv6`, `UInt128`, `Array(String)` columns), `dlt.sources.helpers.requests`, pytest against the disposable ClickHouse container of `tests/test_ip_enrichment_input.py::server`.

**Spec:** the owner's second revision of 2026-09-25 (quoted verbatim in the next section, binding) narrowing the first revision note `/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect-corpscout/9f2d193f-045d-4f26-91d7-d2b93320d3f5/scratchpad/ip/revision-registry-data.md`, which refined decision D6 of `…/scratchpad/ip/decisions.md`; evidence in `…/scratchpad/ip/review-rdap-geoip.md` (173,991 IPs served from /8 blocks, `ripe:2A00::/11`, `arin:NET6-2600-1`, LACNIC `UNALLOCATED`). This plan is standalone: the queue-contract, batching, GeoLite2 and the ~170k-IP remediation re-run stay in `2026-09-25-ip-enrichment-queue-contract.md` (its migrations 449/450 shift to 451/452 when it merges after this plan — re-check at merge).

## Owner decision (2026-09-25, second revision — binding)

> "We don't want a local database for RDAP; what we only need is these special segments and a Dagster job that updates them daily or weekly."

Resolved in this plan as: (a) load the IANA IPv4 address-space and IPv6 unicast-assignment blocks (307 rows) with designation, RIR and status; (b) from the five delegated-extended files keep **only** the `available`/`reserved` ipv4/ipv6 lines, never `allocated`/`assigned`; keep the whole-file validation and the dated load ledger; rule = registry_level if the registration covers at least one entire RIR-designated IANA block, unallocated if its first address lies in an available/reserved range or in an IANA reserved/unassigned block, else reusable — no "strictly wider than the delegation" branch, no adjacent-record merging, no delegation trie; persisted classes, trie exclusion migration, enricher/worker changes, docs and the reviewed deploy stay; a **daily** schedule, stopped by default.

**Sizing finding the owner should know:** the special segments are small for IPv4 (12,674 lines) but not for IPv6 — the RIRs enumerate their free IPv6 space in fixed-size chunks, so the files carry 308,898 available/reserved IPv6 lines (APNIC 96,567, RIPE NCC 84,384, ARIN 77,692, LACNIC 46,428, AFRINIC 7,668): 321,572 special rows in total, 325,488 CIDRs, out of 653,717 ipv4+ipv6 records. That is still small for ClickHouse (a few MB compressed, a 48 MiB dictionary that loads in 0.3 s) and the plan sizes for it explicitly; merging adjacent same-status ranges would cut rows to 88,249 but leaves the CIDR count at 322,052, so it buys nothing for the trie and is not done.

## Global Constraints

- Commands from `services/dagster_v3`: `uv run --frozen --no-sync pytest … -q -p no:cacheprovider`, `uv run --frozen --no-sync dg check defs`, `uv run --frozen --no-sync ruff format <touched files>` and `uv run --frozen --no-sync ruff check <touched files>` on touched Python files only. Tests that need ClickHouse import the module-scoped `server` fixture of `tests/test_ip_enrichment_input.py` (docker `clickhouse/clickhouse-server:26.5`, user `test`/`test`); never call IANA, the RIRs, RDAP servers or MaxMind from tests — fixtures only.
- ClickHouse migrations: `clickhouse/migrations/`, each name appended to `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`; up files start with `CREATE DATABASE IF NOT EXISTS corpscout;` and create with `IF NOT EXISTS`; down files drop with `IF EXISTS`; no `;` inside `--` comments; no `TRUNCATE TABLE` in an up file; only the `corpscout` database. Next free numbers on main and prod are **000449** and **000450** (`ls clickhouse/migrations | tail -2` shows 448; prod `SELECT max(version) FROM corpscout.schema_migrations WHERE dirty=0` = 448 on 2026-09-25); re-check at merge.
- Dagster conventions (`dagster_v3/CLAUDE.md`): non-partitioned full refresh for whole-dataset-per-request sources, `dlt.sources.helpers.requests` for HTTP, refuse to replace on empty input, one concurrency pool for the module's chain, schedule STOPPED by default and started at instance level, at most three `kinds` per asset, no `from __future__ import annotations` in modules defining assets, `uv run dg check defs` before finishing, a design doc from `docs/source-design-doc-template.md`.
- Commit by explicit path, never `git add -A` (the tree carries unrelated WIP such as `searcher/`). Conventional commits with trailer `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- Do not restart `corpscout-dagster-dev`; deploy by `light_sync`. Task 6 requires the owner's go-ahead. The first reference load must be green (assets + checks) and its excluded-network report reviewed BEFORE migration 000450 makes the exclusion live (Task 6 applies 449 and 450 separately for that reason).
- Keep the enricher/worker change minimal and independent of the queue-contract rewrite: no new segment role, no change to `rdap_network_segments`, `rdap_networks`, `rdap_ip_lookup_results` or `ip_enrichment_*` tables.

## Evidence gathered on 2026-09-25 (real files)

| Source | URL | Facts |
| --- | --- | --- |
| IANA IPv4 | `https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.csv` | 22,972 B, `text/csv`, header `Prefix,Designation,Date,WHOIS,RDAP,Status [1],Note`, 256 rows, prefix zero-padded (`008/8`), statuses `ALLOCATED` 129 / `LEGACY` 92 / `RESERVED` 35, designations `APNIC`, `RIPE NCC`, `Administered by ARIN`, `IANA - Loopback`, `Ford Motor Company`, one quoted designation (`"PSINet, Inc."`), RDAP column sometimes two URLs glued together (`…registryhttp://…`), footnotes such as `[6]` in Note. **204 rows name an RIR** (129 `ALLOCATED` + 75 `LEGACY` "Administered by …"); the other 52 are IANA-reserved (35) or legacy single holders (Ford, PSINet, DISA …). HTTP `Last-Modified: Sat, 19 Sep 2026 00:44:20 GMT`. |
| IANA IPv6 | `https://www.iana.org/assignments/ipv6-unicast-address-assignments/ipv6-unicast-address-assignments.csv` | 5,666 B, header `Prefix,Designation,Date,WHOIS,RDAP,Status,Note`, **51 rows** (multi-line quoted notes, so `wc -l` says 59), statuses `ALLOCATED` 36 / `RESERVED` 15, designations `IANA` 15, `RIPE NCC` 14, `APNIC` 9, `ARIN` 7, `AFRINIC` 2, `LACNIC` 2, `6to4`, `Documentation`; **34 rows name an RIR**. |
| RIR delegated-extended | `https://ftp.{afrinic.net/pub/stats/afrinic,apnic.net/stats/apnic,arin.net/pub/stats/arin,lacnic.net/pub/stats/lacnic,ripe.net/pub/stats/ripencc}/delegated-<rir>-extended-latest` (+ `.md5`) | pipe-separated; APNIC starts with a 27-line `#` banner; version line `version|registry|serial|records|startdate|enddate|UTCoffset` — RIPE `2|ripencc|1790287199|260748|19700101|20260924|+0200` (serial = unix seconds), ARIN `2.3|arin|1790341220831|202978|19700101|20260925|-0400`, APNIC `2.3|apnic|20260926|190190||20260925|+1000` (empty startdate, serial a day after enddate), LACNIC `2.3|lacnic|20260924|97298|19870101|20260924|-0300`, AFRINIC `2|afrinic|20260924|19784|00000000|20260924|00000`; `records` = number of record lines of all types = sum of the three `registry|*|type|*|count|summary` lines; records `registry|cc|type|start|value|date|status|opaque-id`, statuses `allocated`, `assigned`, `available`, `reserved`; RIPE writes **7 fields** for available/reserved (`ripencc||ipv4|85.8.248.0|2048||available`), LACNIC 7 fields for available IPv6, the others 8 with an empty id; AFRINIC uses `cc = ZZ` for available/reserved; IPv4 value = address count, often not a power of two (`768`, `1536`, `3072`, `524288`); IPv6 value = prefix length; dates `YYYYMMDD` or `00000000`. Sizes 1.0–18.1 MB; no overlapping records inside a file. `.md5` formats: BSD `MD5 (delegated-ripencc-extended-latest) = 0ef1edc2…` (RIPE, APNIC, LACNIC, AFRINIC) and GNU `f3c86ced…  delegated-arin-extended-20260925` (ARIN, dated name). |
| Special segments (the rows this plan keeps) | counted from the same files (`scratchpad/ip/sources/`, byte-identical to `scratchpad/ip/fixtures/`) | whole-file ipv4+ipv6 records: afrinic 15,434 · apnic 175,432 · arin 170,000 · lacnic 80,784 · ripencc 212,067 (= 653,717). `available`+`reserved` ipv4/ipv6 rows: afrinic 8,239 (571 v4 / 7,668 v6) · apnic 100,042 (3,475 / 96,567) · arin 81,738 (7,932 / 77,692, ARIN has no `available` ipv4) · lacnic 46,821 (393 / 46,428) · ripencc 84,732 (378 / 84,384) = **321,572 rows → 325,488 CIDRs** (a 768-address ARIN reservation is 2 CIDRs, a RIPE reservation up to 10). Only 4 RIPE and 16 AFRINIC ipv4 rows are `available`; the kept counts swing legitimately from day to day. |
| Example rows | in the files | IANA `103/8,APNIC,2011-02,…,ALLOCATED`, `019/8,Ford Motor Company,1995-05,…,LEGACY`, `045/8,Administered by ARIN,1995-01,…,LEGACY`, `2a10::/12,RIPE NCC,2019-06-05,…,ALLOCATED`; RIR `lacnic||ipv4|45.68.105.0|256||reserved|`, `arin||ipv4|23.128.1.0|768||reserved|` (→ `23.128.1.0/24` + `23.128.2.0/23`), `lacnic||ipv6|2001:1201:20::|43||available` (7 fields), `afrinic|ZZ|ipv4|102.192.0.0|524288||available|`. |

ClickHouse 26.5.7.64 facts verified with `docker run --rm -i clickhouse/clickhouse-server:26.5 clickhouse local --multiquery` (scratchpad `ip/verify3.sql`, `ip/smoke.sql`, `ip/smoke2.sql`, `ip/smoke3.sql`, `ip/verify4.sql`, `ip/trie_mem.sql`):

- `toUInt128(toIPv6('::ffff:1.2.3.4')) = 281470698652420` = Python `int(IPv6Address('::ffff:1.2.3.4'))`; `IP_TRIE` dictionaries accept `UInt128` attributes and a tuple `dictGetOrDefault`; a trie holding IPv4 CIDRs answers both `tuple(toIPv4(x))` and `tuple(toIPv6('::ffff:x'))`; `toIPv6(<UInt128>)` converts back; a view may hold a scalar subquery; the exclusion in the trie view must live in a subquery because ClickHouse resolves the `argMax(...) AS network_key` alias inside an outer `WHERE` (`ILLEGAL_AGGREGATION`).
- **`RANGE_HASHED` is out**: `CREATE DICTIONARY … RANGE(MIN range_first MAX range_last)` accepts `UInt128` bounds, but `dictGetOrDefault` refuses the lookup (`Illegal type UInt128 of fourth argument … must be convertible to Int64`). The special-segment lookup therefore stays an `IP_TRIE` over the CIDR cover of each range.
- **The IANA rows need no dictionary**: "covers at least one entire RIR block" is a set operation, not a point lookup. In the bulk view it is `countIf(...)` over a `LEFT JOIN` of every network with every IANA row on a constant key (`ON n.one = b.one`, ≈15k × 307 rows, streamed) — a `CROSS JOIN` would drop every network while the reference table is empty, the left join keeps them and the rule says `unknown`; per RDAP miss it is a `count()` subquery over the ≈307 rows. `anyIf((designation, rir, status), …)` gives the block holding the first address and returns `('', '', '')` when none does. A scalar subquery `(SELECT (any(a), any(b), any(c)) FROM … WHERE …)` over an empty set returns `('', '', '')` typed `Nullable(Tuple)`, not NULL. (A constant array with `arrayFirst`/`arrayExists` was rejected: ClickHouse replicates a constant array per row of the block, so a 300k-element array would materialize gigabytes.)
- `PARTITION BY (registry, snapshot_date)` with a `LowCardinality(String)` key works and `ALTER TABLE … DROP PARTITION ('apnic', '2026-09-24')` drops exactly that snapshot.
- `verify4.sql` runs the complete revised schema (tables, ledger, views, trie, class table, derived view, the 000450 trie view) with the 22 registrations of Task 2's `CASES`: not ready → all 22 `unknown`; ready → 8 `registry_level`, 8 `reusable`, 6 `unallocated` exactly as expected; the per-miss context query gives `(1, 1, ('APNIC','apnic','ALLOCATED'), ('','',0,0))` for 103.0.0.0/8 and `('lacnic','reserved', …)` for 45.68.105.0/24; after persisting the classes the trie view serves only `apnic:FPT-VN` and `arin:GOOGLE`.
- `trie_mem.sql`: an `IP_TRIE` over all 325,488 real special CIDRs (all unique) = **48.24 MiB, 0.29 s to load**; `45.68.105.9 → ('lacnic','reserved')`, `85.8.250.1 → ('ripencc','available')`, `103.35.64.49` and `8.8.8.8 → ('','')`.

## Design decisions (resolved)

1. **Storage = the current special segments plus a load ledger, not a delegation database.** `ip_registry_iana_blocks` (307 rows per snapshot) and `ip_registry_special_segments` (≈322k rows per snapshot) are `ReplacingMergeTree(loaded_at)`, `PARTITION BY (source|registry, snapshot_date)`, ordered by `(source|registry, snapshot_date, ip_version, first_ip)`. `ip_registry_snapshots` (source, snapshot_date, verified_at, checksum, serial, records_ipv4/ipv6 = whole-file counts, segments_ipv4/ipv6 = kept rows, source_url) is written after the rows, so `_current` views (newest ledger snapshot per source) never expose a partial load; a re-checked identical file only refreshes `verified_at`. **Retention, stated simply:** after its ledger row the loader drops every partition of that source except the two newest ledger snapshots (`SNAPSHOTS_KEPT = 2`: the current one and the one before it, enough to diff a surprise), so the tables never hold more than ≈650k rows (a few MB); the ledger keeps one small row per load forever. No TTL — a TTL could silently delete the current snapshot of a source that stops publishing, and the freshness check already fails after 3 days.
2. **Bounds in one key space.** Every address bound is an `IPv6` column (IPv4 as `::ffff:a.b.c.d`), compared as `UInt128`; Python uses `address_int()` with the same mapping. Special segments keep the published `start_address`/`value`/`cc`/`status` plus derived `first_ip/last_ip` and `cidrs` (stdlib `ipaddress.summarize_address_range`; ClickHouse has no range→CIDR function).
3. **Lookup = one small `IP_TRIE` for the special segments, a plain join for the IANA rows.** `ip_registry_special_trie` (≈325k CIDRs, 48 MiB, attributes registry/status/segment_first/segment_last) over a source view that `ARRAY JOIN cidrs`, read by the existing least-privilege user `corpscout_rdap_dictionary` (migration 000126), `LIFETIME(MIN 3600 MAX 7200)` plus explicit `SYSTEM RELOAD DICTIONARY` after each load. The IANA rows are joined/subqueried directly (see the evidence: `RANGE_HASHED` rejected, constant arrays rejected, `CROSS JOIN` loses the not-ready case).
4. **The rule** (`registry_class`, owner's three branches), for a registration N = [first, last]: (a) N covers at least one entire IANA block designated to an RIR (`rir != ''`, i.e. `APNIC`, `ARIN`, `RIPE NCC`, `LACNIC`, `AFRINIC`, `Administered by …`, whether `ALLOCATED` or `LEGACY`) → `registry_level`; (b) N's first address lies in an `available`/`reserved` RIR segment, or in an IANA `RESERVED` block, or in no IANA block at all → `unallocated`; (c) otherwise `reusable`; `unknown` until all seven sources have a current snapshot (then every cache behaves as today). The point lookup uses the registration's **first address**, not the queried IP, because the unit that is classified, persisted and excluded is the network; for the placeholder objects the RIRs return for unallocated space the network is the range itself, so both coincide, and a holder registration cannot start inside available/reserved space (the files have no overlapping records). **Ford 19.0.0.0/8 → `reusable`**: IANA status `LEGACY` with a non-RIR designation is neither reserved nor unassigned, and the block is one holder's registration; the same holds for the other legacy single-holder /8s (PSINet, DISA …). `8.8.8.0/24` (IANA `008/8` = "Administered by ARIN", `LEGACY`) is `reusable` because it covers no whole block. Special-purpose IANA registries are not loaded: the enrichers already skip non-global addresses (`classify_ip_scope`). Accepted loss (owner's choice): a registration wider than a holder's delegation but not covering a whole IANA block (e.g. an RIR "ALLOCATED UNSPECIFIED" /13 placeholder, or a /14 spanning two holders) is now `reusable`.
5. **One SQL definition, one Python twin.** The rule text lives in `commoncrawl_rdap/registry.py` (`REGISTRY_CLASS_SQL`, embedded verbatim in the view `rdap_network_registry_class_derived` — a contract test compares it with the migration) and `registry_class()`; `tests/test_ip_registry.py` proves both agree on the fixture cases. The classification is **persisted** in `rdap_network_registry_class` (ReplacingMergeTree by `network_key`): the daily asset rewrites it for every cached network from the view, and the enrichers insert a row for each new registration from the Python rule before inserting its segments. The trie view anti-joins that table, so its reader needs no `dictGet` grant and the exclusion is inert until the first classification exists (deploy ordering falls out naturally).
6. **Module** `defs/ip_registry/`: one asset per file (`ip_registry_iana_blocks` loads both IANA CSVs, five `ip_registry_special_segments_<rir>` from a factory) so a failing RIR does not block the others, then `rdap_network_registry_class`; job `ip_registry_refresh_job` (= that asset `.upstream()`, checks included); pool `ip_registry`. **Schedule = daily**, `ip_registry_daily` at `5 6 * * *` UTC (the APNIC file dated D is published on D+1 at +10:00; no other schedule uses 06:05), STOPPED by default. Daily rather than weekly because the RIR files change every day: a block allocated yesterday stays classified `unallocated` (excluded from the trie, so every address in it costs an RDAP call) until the next refresh, and the whole refresh is a 45 MB download plus seconds of work.
7. **Validation (whole file, before anything is written):** MD5 mismatch, missing/foreign version line, `records` ≠ record lines, summary ≠ per-type record count, unknown status, unparsable special address, empty ipv4+ipv6, a file older than the current snapshot, and a >5% drop in the **whole-file** ipv4 or ipv6 record count versus the current snapshot unless the run config says `allow_shrink`. The shrink guard uses the whole-file counts, not the kept special-segment counts, because a truncated download drops records of every status while the special counts legitimately swing (RIPE has 4 available ipv4 rows, AFRINIC 16; a returned block moves rows between statuses daily). A file with zero special rows is loaded (an exhausted registry is legitimate) and shows `segments_ipv4 = 0` in its metadata.
8. **Checks**: one `snapshot_fresh` check per loader (RIR snapshot date ≤ 3 days old and `verified_at` ≤ 2 days old; IANA only `verified_at`) and `classification_complete` on the class asset (ready = 1 and every current network has a class row).

## File Structure

| File | Change | Responsibility |
| --- | --- | --- |
| `services/dagster_v3/src/dagster_v3/defs/ip_registry/__init__.py` | create (empty) | package |
| `services/dagster_v3/src/dagster_v3/defs/ip_registry/tables.py` | create | names, URLs, column tuples, `SNAPSHOTS_KEPT` |
| `services/dagster_v3/src/dagster_v3/defs/ip_registry/source.py` | create | pure parsers: IANA CSV, delegated-extended (special rows only, whole-file counts), `.md5`, `address_int` |
| `services/dagster_v3/src/dagster_v3/defs/ip_registry/assets.py` | create | download, validate, load, retention, classify, checks, job, daily schedule |
| `services/dagster_v3/src/dagster_v3/defs/ip_registry/docs/ip_registry-design.md` | create | design doc (template) |
| `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/registry.py` | create | the rule (Python + SQL text), context query, class row SQL |
| `clickhouse/migrations/000449_corpscout_ip_registry_reference_data.{up,down}.sql` | create | ledger, IANA blocks, special segments, views, special trie, readiness, class table, derived view, grants |
| `clickhouse/migrations/000450_corpscout_rdap_trie_registry_class_exclusion.{up,down}.sql` | create | trie source view excludes non-reusable networks |
| `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py` | modify | classify each new registration, class row before segments, no reuse of non-reusable |
| `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/results.py` | modify | storage assertion, `registry_level_responses` metadata |
| `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/assets.py` | modify | same for the legacy bucket worker |
| `services/dagster_v3/tests/fixtures/ip_registry/*` | create | real excerpts of the seven files |
| `services/dagster_v3/tests/test_ip_registry_source.py` | create | pure tests (parsers, rule, freshness) |
| `services/dagster_v3/tests/test_ip_registry.py` | create | ClickHouse tests: migrations, loaders, retention, trie, parity, exclusion; exports `apply_migration`, `seed_reference_data` |
| `services/dagster_v3/tests/test_clickhouse_migrations.py` | modify | `EXPECTED_MIGRATIONS` |
| `services/dagster_v3/tests/test_commoncrawl_rdap_assets.py` | modify | fake write client answers the context query; new worker test |
| `services/dagster_v3/tests/test_ip_enrichment_results.py` | modify | fixture applies 449/450; new enricher test |
| `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md`, `services/dagster_v3/docs/operations/ip-registry-reference-data.md`, `services/dagster_v3/docs/ip-enrichment-schema.md` | modify/create | docs |

---

### Task 1: Parsers, the rule and the fixtures (pure Python)

**Files:**
- Create: `services/dagster_v3/src/dagster_v3/defs/ip_registry/__init__.py` (empty), `…/ip_registry/tables.py`, `…/ip_registry/source.py`
- Create: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/registry.py`
- Create: `services/dagster_v3/tests/fixtures/ip_registry/{iana-ipv4-excerpt.csv, iana-ipv6-excerpt.csv, delegated-ripencc-extended-excerpt, delegated-apnic-extended-excerpt, delegated-arin-extended-excerpt, delegated-lacnic-extended-excerpt, delegated-afrinic-extended-excerpt}`
- Test: `services/dagster_v3/tests/test_ip_registry_source.py`

**Interfaces:**
- Produces (`dagster_v3.defs.ip_registry.tables`): `DATABASE`, `SNAPSHOTS_TABLE`, `IANA_TABLE`, `SPECIAL_TABLE`, `SPECIAL_TRIE`, `READY_VIEW`, `IP_REGISTRY_POOL`, `SNAPSHOTS_KEPT = 2`, `IANA_SOURCES: dict[str, str]` (`iana_ipv4`, `iana_ipv6`), `RIR_SOURCES: dict[str, str]` (`afrinic`, `apnic`, `arin`, `lacnic`, `ripencc`), `SOURCES`, `SNAPSHOT_COLUMNS`, `IANA_COLUMNS`, `SPECIAL_COLUMNS`.
- Produces (`dagster_v3.defs.ip_registry.source`): `IPV4_MAPPED_OFFSET`, `SPECIAL_STATUSES`, `address_int(value) -> int`, `designation_rir(designation) -> str`, dataclasses `IanaBlock`, `DelegatedHeader`, `SpecialSegment`, `DelegatedFile` (`.header`, `.summaries`, `.special`, `.record_lines`, `.special_count(ip_version)`), `parse_iana_csv(text, source) -> list[IanaBlock]`, `parse_md5(text) -> str`, `parse_delegated(text, registry) -> DelegatedFile`.
- Produces (`dagster_v3.defs.commoncrawl_rdap.registry`): `IanaCoverage(designation, rir, status)`, `SpecialCoverage(registry, status, first, last)`, `RegistryContext(ready, covered_rir_blocks, iana, special)`, `RegistryClassification` (`.registry_class`, `.reusable`, `.clickhouse_values(network_key, classified_at)`), `UNALLOCATED_STATUSES`, `REGISTRY_CONTEXT_SQL`, `REGISTRY_CLASS_SQL`, `REGISTRY_CLASS_COLUMNS`, `REGISTRY_CLASS_INSERT_SQL`, `REGISTRY_CLASS_REFRESH_SQL`, `registry_class(context) -> str`, `mapped_address(address) -> str`, `fetch_registry_context(client, first_address, last_address) -> RegistryContext`, `classify_registration(client, network) -> RegistryClassification`.

- [ ] **Step 1: Write the fixtures**

`tests/fixtures/ip_registry/iana-ipv4-excerpt.csv` (23 real rows, in file order; keep the header's `Status [1]`; every IPv4 block the tests touch is here — 18 of them name an RIR):

```
Prefix,Designation,Date,WHOIS,RDAP,Status [1],Note
000/8,IANA - Local Identification,1981-09,,,RESERVED,[2][3]
001/8,APNIC,2010-01,whois.apnic.net,https://rdap.apnic.net/,ALLOCATED,
002/8,RIPE NCC,2009-09,whois.ripe.net,https://rdap.db.ripe.net/,ALLOCATED,
005/8,RIPE NCC,2010-11,whois.ripe.net,https://rdap.db.ripe.net/,ALLOCATED,
008/8,Administered by ARIN,1992-12,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,LEGACY,
014/8,APNIC,2010-04,whois.apnic.net,https://rdap.apnic.net/,ALLOCATED,[5]
019/8,Ford Motor Company,1995-05,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,LEGACY,
023/8,ARIN,2010-11,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,ALLOCATED,
027/8,APNIC,2010-01,whois.apnic.net,https://rdap.apnic.net/,ALLOCATED,
038/8,"PSINet, Inc.",1994-09,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,LEGACY,
041/8,AFRINIC,2005-04,whois.afrinic.net,https://rdap.afrinic.net/rdap/http://rdap.afrinic.net/rdap/,ALLOCATED,
045/8,Administered by ARIN,1995-01,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,LEGACY,
085/8,RIPE NCC,2004-04,whois.ripe.net,https://rdap.db.ripe.net/,ALLOCATED,
100/8,ARIN,2010-11,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,ALLOCATED,[6]
101/8,APNIC,2010-08,whois.apnic.net,https://rdap.apnic.net/,ALLOCATED,
102/8,AFRINIC,2011-02,whois.afrinic.net,https://rdap.afrinic.net/rdap/http://rdap.afrinic.net/rdap/,ALLOCATED,
103/8,APNIC,2011-02,whois.apnic.net,https://rdap.apnic.net/,ALLOCATED,
104/8,ARIN,2011-02,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,ALLOCATED,
111/8,APNIC,2008-11,whois.apnic.net,https://rdap.apnic.net/,ALLOCATED,
113/8,APNIC,2008-05,whois.apnic.net,https://rdap.apnic.net/,ALLOCATED,
195/8,RIPE NCC,1993-05,whois.ripe.net,https://rdap.db.ripe.net/,ALLOCATED,
240/8,Future use,1981-09,,,RESERVED,[17]
255/8,Future use,1981-09,,,RESERVED,[17][18]
```

`tests/fixtures/ip_registry/iana-ipv6-excerpt.csv` (10 real rows; the `2600::/12` and `2a00::/12` notes span lines inside their quotes exactly as published; 8 rows name an RIR):

```
Prefix,Designation,Date,WHOIS,RDAP,Status,Note
2001::/23,IANA,1999-07-01,whois.iana.org,,ALLOCATED,This range has been partially allocated. See [IPv6 Special-Purpose Address Space] for details.
2001:200::/23,APNIC,1999-07-01,whois.apnic.net,https://rdap.apnic.net/,ALLOCATED,
2001:600::/23,RIPE NCC,1999-07-01,whois.ripe.net,https://rdap.db.ripe.net/,ALLOCATED,
2001:1200::/23,LACNIC,2002-11-01,whois.lacnic.net,https://rdap.lacnic.net/rdap/,ALLOCATED,
2001:2000::/19,RIPE NCC,2019-03-12,whois.ripe.net,https://rdap.db.ripe.net/,ALLOCATED,"2001:2000::/20, 2001:3000::/21, and 2001:3800::/22 were allocated on 2004-05-04. The more recent allocation (2019-03-12) incorporates all these previous allocations."
2001:4800::/23,ARIN,2004-08-24,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,ALLOCATED,
2600::/12,ARIN,2006-10-03,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,ALLOCATED,"2600::/22, 2604::/22, 2608::/22 and 260c::/22 were allocated on 2005-04-19. The more
recent allocation (2006-10-03) incorporates all these previous allocations."
2a00::/12,RIPE NCC,2006-10-03,whois.ripe.net,https://rdap.db.ripe.net/,ALLOCATED,"2a00::/21 was originally allocated on 2005-04-19. 2a01::/23 was allocated on 2005-07-14.
2a01::/16 (incorporating the 2a01::/23) was allocated on 2005-12-15. The more recent allocation
(2006-10-03) incorporates these previous allocations."
2a10::/12,RIPE NCC,2019-06-05,whois.ripe.net,https://rdap.db.ripe.net/,ALLOCATED,
2d00::/8,IANA,1999-07-01,,,RESERVED,
```

`tests/fixtures/ip_registry/delegated-ripencc-extended-excerpt` (real records; header and summary counts rewritten to the excerpt: 11 records = 8 ipv4 + 3 ipv6; only the `available` and `reserved` lines are kept by the parser — 2 of them here):

```
2|ripencc|1790287199|11|19700101|20260924|+0200
ripencc|*|ipv4|*|8|summary
ripencc|*|asn|*|0|summary
ripencc|*|ipv6|*|3|summary
ripencc|SE|ipv4|2.0.0.0|131072|20100712|allocated|12a581c1-ea86-46af-9554-77e3b4ab3df5
ripencc|SE|ipv4|2.2.0.0|65536|20100712|allocated|12a581c1-ea86-46af-9554-77e3b4ab3df5
ripencc|FR|ipv4|2.3.0.0|65536|20100712|allocated|9a489e65-dd78-443e-96ab-e21e016b5113
ripencc|PS|ipv4|1.178.112.0|4096|20071126|allocated|172ce676-8ded-4901-9812-793bd0b4ec77
ripencc|DK|ipv4|195.85.96.0|1536|19970206|allocated|654e8153-9457-446b-bbe9-2db02b7ba0a8
ripencc|UA|ipv4|31.131.128.0|3072|20110606|assigned|8c80cd90-d719-4bb9-a036-4922ec4bab03
ripencc||ipv4|85.8.248.0|2048||available
ripencc||ipv4|5.134.16.0|2048||reserved
ripencc|NL|ipv6|2001:600::|29|19990826|allocated|6c4fb689-12a2-41cf-93d5-eb39bcfae759
ripencc|CZ|ipv6|2001:678:1::|48|20061011|assigned|1c823545-5600-4e44-9ef9-0fb92b56c1cf
ripencc|IE|ipv6|2a00:1450::|29|20091005|allocated|6e75048b-4b67-432d-97ad-6faaaf3ae0c4
```

`tests/fixtures/ip_registry/delegated-apnic-extended-excerpt` (APNIC's banner, empty startdate, 8 records = 6 ipv4 + 2 ipv6; 2 special):

```
######################################################################
#
# 	CONDITIONS OF USE
#
2.3|apnic|20260926|8||20260925|+1000
apnic|*|asn|*|0|summary
apnic|*|ipv4|*|6|summary
apnic|*|ipv6|*|2|summary
apnic|AU|ipv4|1.0.0.0|256|20110811|assigned|A91872ED
apnic|AU|ipv4|101.0.64.0|16384|20101213|allocated|A916B33E
apnic|AU|ipv4|103.0.0.0|65536|20110405|allocated|A91872ED
apnic|VN|ipv4|103.35.64.0|1024|20150813|allocated|A9271131
apnic||ipv4|14.102.240.0|4096||available|
apnic||ipv4|27.0.8.0|1024||reserved|
apnic|JP|ipv6|2001:200::|35|19990813|allocated|A916B6AA
apnic|HK|ipv6|2001:7fa:0:1::|64|20020116|assigned|A91972B6
```

`tests/fixtures/ip_registry/delegated-arin-extended-excerpt` (9 records = 2 asn + 4 ipv4 + 3 ipv6; asn date `00000000`; 1 special, a 768-address reservation that covers two CIDRs):

```
2.3|arin|1790341220831|9|19700101|20260925|-0400
arin|*|asn|*|2|summary
arin|*|ipv4|*|4|summary
arin|*|ipv6|*|3|summary
arin|US|asn|1|1|20010920|assigned|e5e3b9c13678dfc483fb1f819d70883c
arin|US|asn|3|1|00000000|assigned|d98c567cda2db06e693f2b574eafe848
arin|US|ipv4|8.8.8.0|256|20231228|allocated|9d99e3f7d38d1b8026f2ebbea4017c9f
arin|US|ipv4|19.0.0.0|16777216|19880615|allocated|ce4419bfa21e3437eeb38a61702b7326
arin|US|ipv4|104.16.0.0|1048576|20140328|allocated|28408e4f0567c2545afefd9cbce183bc
arin||ipv4|23.128.1.0|768||reserved|
arin|US|ipv6|2001:400::|32|19990803|allocated|04f048163e37eef48d891498545eefc0
arin|US|ipv6|2001:4860::|32|20050314|allocated|9d99e3f7d38d1b8026f2ebbea4017c9f
arin|US|ipv6|2600:1f00::|24|20141017|allocated|20c786e8edd815cc245070645e265298
```

`tests/fixtures/ip_registry/delegated-lacnic-extended-excerpt` (7 records = 3 ipv4 + 4 ipv6; seven-field available IPv6 lines; 3 special):

```
2.3|lacnic|20260924|7|19870101|20260924|-0300
lacnic|*|ipv4|*|3|summary
lacnic|*|ipv6|*|4|summary
lacnic|*|asn|*|0|summary
lacnic|GT|ipv4|2.152.0.0|1024|20260714|allocated|71316
lacnic|PA|ipv4|2.152.252.0|1024|20260826|assigned|75377
lacnic||ipv4|45.68.105.0|256||reserved|
lacnic|MX|ipv6|2001:1200::|32|20030110|allocated|259318
lacnic|MX|ipv6|2001:1201::|44|20190404|assigned|27835
lacnic||ipv6|2001:1201:20::|43||available
lacnic||ipv6|2001:1201:40::|42||available
```

`tests/fixtures/ip_registry/delegated-afrinic-extended-excerpt` (8 records = 1 asn + 5 ipv4 + 2 ipv6; `ZZ` on available/reserved; 2 special):

```
2|afrinic|20260924|8|00000000|20260924|00000
afrinic|*|asn|*|1|summary
afrinic|*|ipv4|*|5|summary
afrinic|*|ipv6|*|2|summary
afrinic|ZA|asn|1228|1|19910301|allocated|F36B9F4B
afrinic|ZZ|ipv4|102.192.0.0|524288||available|
afrinic|ZZ|ipv4|41.57.112.0|2048||reserved|
afrinic|TZ|ipv4|45.220.48.0|256|20170301|assigned|F3638C76
afrinic|ZA|ipv4|196.4.163.0|768|19940128|assigned|F36B0DC0
afrinic|ZA|ipv4|196.4.200.0|3072|19940211|assigned|F3677CA0
afrinic|ZA|ipv6|2001:4200::|32|20051021|allocated|F36B9F4B
afrinic|ZA|ipv6|2001:42d0::|40|20070621|assigned|F3634D22
```

Special rows across the five excerpts: 10 (ripencc 2, apnic 2, arin 1, lacnic 3, afrinic 2; 8 ipv4 + 2 ipv6), 11 CIDRs.

- [ ] **Step 2: Write the failing pure tests**

Create `services/dagster_v3/tests/test_ip_registry_source.py`:

```python
"""Parsers for the IANA CSVs and RIR delegated-extended files, and the registry rule — no I/O."""

from datetime import date
from pathlib import Path

import pytest

from dagster_v3.defs.commoncrawl_rdap import registry
from dagster_v3.defs.ip_registry import source, tables

FIXTURES = Path(__file__).parent / "fixtures" / "ip_registry"


def excerpt(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_address_int_matches_clickhouse_touint128_of_toipv6():
    # ClickHouse: toUInt128(toIPv6('::ffff:1.2.3.4')) = 281470698652420 (verified on 26.5).
    assert source.address_int("1.2.3.4") == 281470698652420
    assert source.address_int("::ffff:1.2.3.4") == 281470698652420
    assert source.address_int("2600::") == 50510663839826803170344668290653093888
    assert source.address_int("103.35.64.49") - source.address_int("103.35.64.0") == 49


@pytest.mark.parametrize(
    ("designation", "rir"),
    [
        ("APNIC", "apnic"),
        ("RIPE NCC", "ripencc"),
        ("Administered by ARIN", "arin"),
        ("Administered by AFRINIC", "afrinic"),
        ("LACNIC", "lacnic"),
        ("IANA - Loopback", ""),
        ("Ford Motor Company", ""),
        ("Future use", ""),
        ("IANA", ""),
    ],
)
def test_designation_rir(designation, rir):
    assert source.designation_rir(designation) == rir


def test_parse_iana_ipv4_excerpt():
    blocks = source.parse_iana_csv(excerpt("iana-ipv4-excerpt.csv"), "iana_ipv4")
    assert len(blocks) == 23
    by_prefix = {block.prefix: block for block in blocks}
    assert set(by_prefix) == {
        f"{octet}.0.0.0/8"
        for octet in (0, 1, 2, 5, 8, 14, 19, 23, 27, 38, 41, 45, 85, 100, 101, 102, 103, 104, 111, 113, 195, 240, 255)
    }
    apnic = by_prefix["103.0.0.0/8"]
    assert (apnic.ip_version, apnic.designation, apnic.rir, apnic.status, apnic.assigned_on) == (
        4, "APNIC", "apnic", "ALLOCATED", "2011-02"
    )
    assert (apnic.first, apnic.last) == (source.address_int("103.0.0.0"), source.address_int("103.255.255.255"))
    assert by_prefix["38.0.0.0/8"].designation == "PSINet, Inc."
    assert by_prefix["8.0.0.0/8"].rir == "arin" and by_prefix["8.0.0.0/8"].status == "LEGACY"
    assert by_prefix["45.0.0.0/8"].rir == "arin" and by_prefix["45.0.0.0/8"].status == "LEGACY"
    assert by_prefix["19.0.0.0/8"].rir == "" and by_prefix["19.0.0.0/8"].status == "LEGACY"
    assert by_prefix["240.0.0.0/8"].rir == "" and by_prefix["240.0.0.0/8"].status == "RESERVED"
    assert by_prefix["100.0.0.0/8"].note == "[6]"
    assert by_prefix["8.0.0.0/8"].rdap == "https://rdap.arin.net/registryhttp://rdap.arin.net/registry"
    assert sum(1 for block in blocks if block.rir) == 18


def test_parse_iana_ipv6_excerpt_handles_multiline_notes():
    blocks = source.parse_iana_csv(excerpt("iana-ipv6-excerpt.csv"), "iana_ipv6")
    assert [block.prefix for block in blocks] == [
        "2001::/23", "2001:200::/23", "2001:600::/23", "2001:1200::/23", "2001:2000::/19",
        "2001:4800::/23", "2600::/12", "2a00::/12", "2a10::/12", "2d00::/8",
    ]
    arin = blocks[6]
    assert (arin.rir, arin.status, arin.ip_version) == ("arin", "ALLOCATED", 6)
    assert arin.note.startswith("2600::/22, 2604::/22") and "recent allocation (2006-10-03)" in arin.note
    assert (arin.first, arin.last) == (
        source.address_int("2600::"), source.address_int("260f:ffff:ffff:ffff:ffff:ffff:ffff:ffff")
    )
    assert blocks[0].rir == "" and blocks[0].designation == "IANA"
    assert blocks[-1].status == "RESERVED"
    assert sum(1 for block in blocks if block.rir) == 8


@pytest.mark.parametrize(
    "text",
    [
        "Prefix,Designation,Date,WHOIS,RDAP,Status,Note\n103/8,APNIC,2011-02,,,PENDING,\n",
        "Prefix,Designation,Date,Status,Note\n103/8,APNIC,2011-02,ALLOCATED,\n",
        "Prefix,Designation,Date,WHOIS,RDAP,Status,Note\n103/8,APNIC\n",
    ],
)
def test_parse_iana_rejects_unknown_status_columns_or_short_rows(text):
    with pytest.raises(ValueError):
        source.parse_iana_csv(text, "iana_ipv4")


def test_parse_md5_accepts_bsd_and_gnu_formats():
    assert source.parse_md5("MD5 (delegated-ripencc-extended-latest) = 0ef1edc28a8a2bbf9b7f02e5be7da195\n") == (
        "0ef1edc28a8a2bbf9b7f02e5be7da195"
    )
    assert source.parse_md5("f3c86ced5a55b1505da587ec785a66b6  delegated-arin-extended-20260925\n") == (
        "f3c86ced5a55b1505da587ec785a66b6"
    )
    with pytest.raises(ValueError):
        source.parse_md5("<html>moved</html>")


def test_parse_delegated_ripencc_excerpt_counts_the_whole_file_but_keeps_only_special_rows():
    parsed = source.parse_delegated(excerpt("delegated-ripencc-extended-excerpt"), "ripencc")
    header = parsed.header
    assert (header.version, header.registry, header.serial, header.records) == ("2", "ripencc", "1790287199", 11)
    assert (header.start_date, header.end_date, header.utc_offset) == ("19700101", date(2026, 9, 24), "+0200")
    assert parsed.summaries == {"ipv4": 8, "asn": 0, "ipv6": 3}
    assert parsed.record_lines == 11
    assert [(segment.start_address, segment.status) for segment in parsed.special] == [
        ("85.8.248.0", "available"), ("5.134.16.0", "reserved"),
    ]
    assert (parsed.special_count(4), parsed.special_count(6)) == (2, 0)
    available = parsed.special[0]  # seven-field line
    assert (available.registry, available.cc, available.ip_version, available.value) == ("ripencc", "", 4, 2048)
    assert available.cidrs == ("85.8.248.0/21",)
    assert (available.first, available.last) == (source.address_int("85.8.248.0"), source.address_int("85.8.255.255"))


def test_parse_delegated_other_registries():
    apnic = source.parse_delegated(excerpt("delegated-apnic-extended-excerpt"), "apnic")
    assert apnic.header.start_date == "" and apnic.header.end_date == date(2026, 9, 25)
    assert {segment.start_address for segment in apnic.special} == {"14.102.240.0", "27.0.8.0"}
    arin = source.parse_delegated(excerpt("delegated-arin-extended-excerpt"), "arin")
    assert arin.header.serial == "1790341220831" and arin.record_lines == 9 and arin.summaries["asn"] == 2
    [reserved] = arin.special  # 768 addresses are not a power of two: two CIDRs
    assert (reserved.cc, reserved.status, reserved.cidrs) == ("", "reserved", ("23.128.1.0/24", "23.128.2.0/23"))
    assert (reserved.first, reserved.last) == (source.address_int("23.128.1.0"), source.address_int("23.128.3.255"))
    lacnic = source.parse_delegated(excerpt("delegated-lacnic-extended-excerpt"), "lacnic")
    assert [(s.ip_version, s.status) for s in lacnic.special] == [(4, "reserved"), (6, "available"), (6, "available")]
    v6 = lacnic.special[1]
    assert (v6.value, v6.cidrs) == (43, ("2001:1201:20::/43",))
    assert v6.last == source.address_int("2001:1201:3f:ffff:ffff:ffff:ffff:ffff")
    afrinic = source.parse_delegated(excerpt("delegated-afrinic-extended-excerpt"), "afrinic")
    assert afrinic.header.start_date == "00000000"
    big = next(s for s in afrinic.special if s.start_address == "102.192.0.0")
    assert (big.cc, big.status, big.cidrs) == ("ZZ", "available", ("102.192.0.0/13",))


@pytest.mark.parametrize(
    ("registry_name", "mutate", "message"),
    [
        ("ripencc", lambda t: t.replace("|11|", "|12|", 1), "announces 12 records"),
        ("ripencc", lambda t: t.replace("ipv4|*|8|summary", "ipv4|*|7|summary"), "ipv4 summary 7"),
        ("apnic", lambda t: t, "belongs to 'ripencc'"),
        ("ripencc", lambda t: t.replace("|allocated|172ce676", "|pending|172ce676"), "status 'pending'"),
        ("ripencc", lambda t: t.replace("2|ripencc|", "ripencc|", 1), "not a version line"),
        ("ripencc", lambda t: t.replace("|85.8.248.0|", "|85.8.248|"), "85.8.248"),
    ],
)
def test_parse_delegated_refuses_inconsistent_files(registry_name, mutate, message):
    # The RIPE excerpt parsed as another registry must be refused as a foreign file.
    text = mutate(excerpt("delegated-ripencc-extended-excerpt"))
    with pytest.raises(ValueError, match=message):
        source.parse_delegated(text, registry_name)


def context(*, ready=True, covered=0, iana=None, special=None):
    return registry.RegistryContext(ready=ready, covered_rir_blocks=covered, iana=iana, special=special)


IANA_APNIC = registry.IanaCoverage("APNIC", "apnic", "ALLOCATED")
IANA_ARIN_LEGACY = registry.IanaCoverage("Administered by ARIN", "arin", "LEGACY")
IANA_FORD = registry.IanaCoverage("Ford Motor Company", "", "LEGACY")
IANA_IANA = registry.IanaCoverage("IANA", "", "ALLOCATED")
IANA_FUTURE = registry.IanaCoverage("Future use", "", "RESERVED")
RESERVED = registry.SpecialCoverage("lacnic", "reserved", source.address_int("45.68.105.0"), source.address_int("45.68.105.255"))
AVAILABLE = registry.SpecialCoverage("ripencc", "available", source.address_int("85.8.248.0"), source.address_int("85.8.255.255"))


@pytest.mark.parametrize(
    ("label", "ctx", "expected"),
    [
        ("reference data not loaded", context(ready=False, covered=1, iana=IANA_APNIC), "unknown"),
        ("covers one RIR block (APNIC-AP 103/8)", context(covered=1, iana=IANA_APNIC), "registry_level"),
        ("covers two RIR blocks (2a00::/11)", context(covered=2, iana=registry.IanaCoverage("RIPE NCC", "ripencc", "ALLOCATED")), "registry_level"),
        ("covers a block although its first address is reserved", context(covered=1, iana=IANA_ARIN_LEGACY, special=RESERVED), "registry_level"),
        ("first address in a reserved range (LACNIC UNALLOCATED)", context(iana=IANA_ARIN_LEGACY, special=RESERVED), "unallocated"),
        ("first address in an available range", context(iana=registry.IanaCoverage("RIPE NCC", "ripencc", "ALLOCATED"), special=AVAILABLE), "unallocated"),
        ("no IANA block at all", context(iana=None), "unallocated"),
        ("IANA reserved block (240/8)", context(iana=IANA_FUTURE), "unallocated"),
        ("legacy single-holder block (Ford 19/8)", context(iana=IANA_FORD), "reusable"),
        ("IANA-designated but allocated block (2001::/23)", context(iana=IANA_IANA), "reusable"),
        ("holder registration (FPT, Cloudflare, Google)", context(iana=IANA_APNIC), "reusable"),
    ],
)
def test_registry_class_rule(label, ctx, expected):
    assert registry.registry_class(ctx) == expected, label


def test_registry_class_sql_mirrors_the_python_rule_text():
    for fragment in (
        "NOT ready, 'unknown'",
        "covered_rir_blocks > 0, 'registry_level'",
        "special.2 IN ('available', 'reserved') OR iana.3 IN ('', 'RESERVED'), 'unallocated'",
        "'reusable') AS registry_class",
    ):
        assert fragment in registry.REGISTRY_CLASS_SQL
    assert registry.UNALLOCATED_STATUSES == ("available", "reserved")
    assert "dictGetOrDefault('corpscout.ip_registry_special_trie'" in registry.REGISTRY_CONTEXT_SQL
    assert registry.REGISTRY_CONTEXT_SQL.count("%(first)s") == 4 and registry.REGISTRY_CONTEXT_SQL.count("%(last)s") == 1
    assert registry.REGISTRY_CLASS_INSERT_SQL.startswith("INSERT INTO corpscout.rdap_network_registry_class (network_key, registry_class,")
    assert "WHERE registry_class != 'unknown'" in registry.REGISTRY_CLASS_REFRESH_SQL
    assert registry.mapped_address("1.2.3.4") in ("::ffff:1.2.3.4", "::ffff:102:304")
    assert registry.mapped_address("2600::") == "2600::"


def test_fixture_urls_are_the_published_ones():
    assert tables.IANA_SOURCES["iana_ipv4"] == "https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.csv"
    assert tables.RIR_SOURCES["ripencc"] == "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest"
    assert tables.SOURCES == ("iana_ipv4", "iana_ipv6", "afrinic", "apnic", "arin", "lacnic", "ripencc")
    assert tables.SNAPSHOTS_KEPT == 2
```

- [ ] **Step 3: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_ip_registry_source.py -q -p no:cacheprovider`
Expected: FAIL — `ModuleNotFoundError: No module named 'dagster_v3.defs.ip_registry'`.

- [ ] **Step 4: Create `tables.py`**

Create `services/dagster_v3/src/dagster_v3/defs/ip_registry/__init__.py` (empty) and `services/dagster_v3/src/dagster_v3/defs/ip_registry/tables.py`:

```python
"""Names, sources and column contracts of the IP registry reference data (migration 000449)."""

DATABASE = "corpscout"
SNAPSHOTS_TABLE = "ip_registry_snapshots"
IANA_TABLE = "ip_registry_iana_blocks"
SPECIAL_TABLE = "ip_registry_special_segments"
SPECIAL_TRIE = "ip_registry_special_trie"
READY_VIEW = "ip_registry_ready"
IP_REGISTRY_POOL = "ip_registry"
# Snapshots kept per source after a load: the current one and the one before it.
SNAPSHOTS_KEPT = 2

IANA_SOURCES = {
    "iana_ipv4": "https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.csv",
    "iana_ipv6": "https://www.iana.org/assignments/ipv6-unicast-address-assignments/ipv6-unicast-address-assignments.csv",
}
# Each RIR publishes the file daily next to a .md5 (BSD "MD5 (name) = hex" or, for ARIN,
# GNU "hex  name"). Only its available/reserved ipv4/ipv6 records are stored.
RIR_SOURCES = {
    "afrinic": "https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest",
    "apnic": "https://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest",
    "arin": "https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest",
    "lacnic": "https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest",
    "ripencc": "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest",
}
# The seven sources the readiness view requires before any classification is trusted.
SOURCES = (*IANA_SOURCES, *RIR_SOURCES)

SNAPSHOT_COLUMNS = (
    "source",
    "snapshot_date",
    "verified_at",
    "checksum",
    "serial",
    "records_ipv4",
    "records_ipv6",
    "segments_ipv4",
    "segments_ipv6",
    "source_url",
)
IANA_COLUMNS = (
    "source",
    "snapshot_date",
    "ip_version",
    "prefix",
    "first_ip",
    "last_ip",
    "designation",
    "rir",
    "status",
    "assigned_on",
    "whois",
    "rdap",
    "note",
    "loaded_at",
)
SPECIAL_COLUMNS = (
    "registry",
    "snapshot_date",
    "ip_version",
    "cc",
    "status",
    "start_address",
    "value",
    "first_ip",
    "last_ip",
    "cidrs",
    "loaded_at",
)
```

- [ ] **Step 5: Create `source.py`**

Create `services/dagster_v3/src/dagster_v3/defs/ip_registry/source.py`:

```python
"""Parsers for the IANA address-space CSVs and the RIR delegated-extended files (no I/O).

Formats verified on 2026-09-25 against the published files; see the fixtures in
tests/fixtures/ip_registry for real excerpts. Of a delegated file only the available and
reserved ipv4/ipv6 records (the special segments) are returned; every record line is still
counted so the file's own bookkeeping (records, per-type summaries) can be checked.
"""

import csv
import io
import re
from dataclasses import dataclass
from datetime import date
from ipaddress import IPv4Address, IPv6Address, ip_address, ip_network, summarize_address_range

# ::ffff:0.0.0.0 — IPv4 addresses live in the IPv4-mapped range so that both families share
# one integer key space, exactly as ClickHouse's toUInt128(toIPv6(...)) maps them.
IPV4_MAPPED_OFFSET = 0xFFFF00000000
RIR_BY_DESIGNATION = {
    "AFRINIC": "afrinic",
    "APNIC": "apnic",
    "ARIN": "arin",
    "LACNIC": "lacnic",
    "RIPE NCC": "ripencc",
}
IANA_STATUSES = frozenset({"ALLOCATED", "LEGACY", "RESERVED"})
DELEGATION_STATUSES = frozenset({"allocated", "assigned", "available", "reserved"})
# The statuses whose records are kept: space no holder has.
SPECIAL_STATUSES = frozenset({"available", "reserved"})
IANA_HEADER = ["Prefix", "Designation", "Date", "WHOIS", "RDAP", "Status", "Note"]
_MD5 = re.compile(r"\b([0-9a-f]{32})\b")


def address_int(value: str | IPv4Address | IPv6Address) -> int:
    """The IPv6-mapped integer of an address (IPv4 as ::ffff:a.b.c.d)."""
    address = ip_address(value) if isinstance(value, str) else value
    if isinstance(address, IPv4Address):
        return IPV4_MAPPED_OFFSET + int(address)
    return int(address)


def designation_rir(designation: str) -> str:
    """The RIR an IANA designation names ('APNIC', 'Administered by ARIN'), else ''."""
    return RIR_BY_DESIGNATION.get(designation.removeprefix("Administered by ").strip(), "")


@dataclass(frozen=True)
class IanaBlock:
    source: str
    prefix: str
    ip_version: int
    first: int
    last: int
    designation: str
    rir: str
    status: str
    assigned_on: str
    whois: str
    rdap: str
    note: str


def parse_iana_csv(text: str, source: str) -> list[IanaBlock]:
    """Rows of ipv4-address-space.csv (source iana_ipv4) or ipv6-unicast-address-assignments.csv.

    The IPv4 file writes 'Status [1]' (a footnote marker) and zero-padded prefixes ('008/8');
    notes may span lines inside quotes; the RDAP column can glue two URLs together and is
    kept as published.
    """
    reader = csv.reader(io.StringIO(text))
    header = [re.sub(r"\s*\[\d+\]$", "", column).strip() for column in next(reader, [])]
    if header != IANA_HEADER:
        raise ValueError(f"{source}: unexpected columns {header}")
    blocks: list[IanaBlock] = []
    for row in reader:
        if not row or not row[0].strip():
            continue
        if len(row) != 7:
            raise ValueError(f"{source}: row {row!r} does not have 7 columns")
        prefix, designation, assigned_on, whois, rdap, status, note = (cell.strip() for cell in row)
        if source == "iana_ipv4":
            octet, length = prefix.split("/")
            network = ip_network(f"{int(octet)}.0.0.0/{length}")
        else:
            network = ip_network(prefix)
        if status not in IANA_STATUSES:
            raise ValueError(f"{source}: unknown status {status!r} for {prefix}")
        blocks.append(
            IanaBlock(
                source=source,
                prefix=str(network),
                ip_version=network.version,
                first=address_int(network[0]),
                last=address_int(network[-1]),
                designation=designation,
                rir=designation_rir(designation),
                status=status,
                assigned_on=assigned_on,
                whois=whois,
                rdap=rdap,
                note=note,
            )
        )
    return blocks


def parse_md5(text: str) -> str:
    """The digest of a .md5 file: BSD 'MD5 (name) = hex' or GNU 'hex  name'."""
    match = _MD5.search(text.lower())
    if match is None:
        raise ValueError(f"no MD5 digest in {text!r}")
    return match.group(1)


@dataclass(frozen=True)
class DelegatedHeader:
    version: str
    registry: str
    serial: str
    records: int
    start_date: str
    end_date: date
    utc_offset: str


@dataclass(frozen=True)
class SpecialSegment:
    """An available or reserved ipv4/ipv6 record of a delegated-extended file."""

    registry: str
    cc: str
    ip_version: int
    status: str
    start_address: str
    value: int
    first: int
    last: int
    cidrs: tuple[str, ...]


@dataclass(frozen=True)
class DelegatedFile:
    header: DelegatedHeader
    summaries: dict[str, int]
    special: tuple[SpecialSegment, ...]
    record_lines: int

    def special_count(self, ip_version: int) -> int:
        return sum(1 for segment in self.special if segment.ip_version == ip_version)


def _delegation_date(value: str) -> date | None:
    if value in ("", "00000000"):
        return None
    return date(int(value[:4]), int(value[4:6]), int(value[6:8]))


def parse_delegated(text: str, registry: str) -> DelegatedFile:
    """A delegated-<registry>-extended file, keeping only its available/reserved records.

    Lines: '#' comments (APNIC's banner), one version line
    'version|registry|serial|records|startdate|enddate|UTCoffset', summary lines
    'registry|*|type|*|count|summary' and records
    'registry|cc|type|start|value|date|status|opaque-id' — seven fields for RIPE NCC's and
    LACNIC's available/reserved records. IPv4 value is an address count (not always a power
    of two), IPv6 value a prefix length. 'records' must equal the number of record lines and
    each ipv4/ipv6 summary its type's count over ALL statuses; asn records are counted only.
    """
    header: DelegatedHeader | None = None
    summaries: dict[str, int] = {}
    special: list[SpecialSegment] = []
    counted = {"ipv4": 0, "ipv6": 0}
    record_lines = 0
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split("|")
        if header is None:
            if len(fields) != 7 or fields[0] not in ("2", "2.3"):
                raise ValueError(f"{registry}: line {line_number} is not a version line: {line!r}")
            if fields[1] != registry:
                raise ValueError(f"{registry}: file belongs to {fields[1]!r}")
            end_date = _delegation_date(fields[5])
            if end_date is None:
                raise ValueError(f"{registry}: version line has no end date: {line!r}")
            header = DelegatedHeader(
                version=fields[0],
                registry=fields[1],
                serial=fields[2],
                records=int(fields[3]),
                start_date=fields[4],
                end_date=end_date,
                utc_offset=fields[6],
            )
            continue
        if len(fields) == 6 and fields[1] == "*" and fields[5] == "summary":
            summaries[fields[2]] = int(fields[4])
            continue
        if len(fields) < 7:
            raise ValueError(f"{registry}: line {line_number} has {len(fields)} fields: {line!r}")
        if fields[0] != registry:
            raise ValueError(f"{registry}: line {line_number} belongs to {fields[0]!r}")
        record_lines += 1
        _, cc, kind, start, value, _delegated, status = fields[:7]
        if status not in DELEGATION_STATUSES:
            raise ValueError(f"{registry}: unknown status {status!r} on line {line_number}")
        if kind == "asn":
            continue
        if kind not in counted:
            raise ValueError(f"{registry}: unknown type {kind!r} on line {line_number}")
        counted[kind] += 1
        if status not in SPECIAL_STATUSES:
            continue
        if kind == "ipv4":
            first_address = IPv4Address(start)
            last_address = first_address + (int(value) - 1)
            first, last = address_int(first_address), address_int(last_address)
            cidrs = tuple(str(cidr) for cidr in summarize_address_range(first_address, last_address))
        else:
            network = ip_network(f"{start}/{value}")
            first, last = address_int(network[0]), address_int(network[-1])
            cidrs = (str(network),)
        special.append(
            SpecialSegment(
                registry=registry,
                cc=cc,
                ip_version=4 if kind == "ipv4" else 6,
                status=status,
                start_address=start,
                value=int(value),
                first=first,
                last=last,
                cidrs=cidrs,
            )
        )
    if header is None:
        raise ValueError(f"{registry}: no version line")
    if record_lines != header.records:
        raise ValueError(
            f"{registry}: header announces {header.records} records, file has {record_lines}"
        )
    for kind in ("ipv4", "ipv6"):
        if summaries.get(kind, 0) != counted[kind]:
            raise ValueError(f"{registry}: {kind} summary {summaries.get(kind, 0)} != {counted[kind]} records")
    return DelegatedFile(
        header=header, summaries=summaries, special=tuple(special), record_lines=record_lines
    )
```

(`IPv4Address("85.8.248")` raises `AddressValueError`, a `ValueError` whose message names the address — that is the refusal the last parametrized case expects.)

- [ ] **Step 6: Create `registry.py`**

Create `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/registry.py`:

```python
"""Data-driven classification of RDAP registrations against the IP registry reference data.

An RDAP answer is reusable coverage only when it is a holder's registration: not a range that
covers at least one entire IANA block designated to an RIR (registry level), and not a range
whose first address lies in space an RIR lists as available or reserved, or in an IANA block
that is reserved or not assigned at all (unallocated). The rule is written twice on purpose:
registry_class() for the enrichers and REGISTRY_CLASS_SQL for the view
rdap_network_registry_class_derived that migration 000449 embeds verbatim;
tests/test_ip_registry.py proves they agree on the same fixtures.
"""

from dataclasses import dataclass
from datetime import datetime
from ipaddress import IPv6Address

from dagster_v3.defs.commoncrawl_rdap.rdap import RdapNetwork
from dagster_v3.defs.ip_registry.source import address_int

UNALLOCATED_STATUSES = ("available", "reserved")
REGISTRY_CLASSES = ("reusable", "registry_level", "unallocated", "unknown")


@dataclass(frozen=True)
class IanaCoverage:
    """The IANA block holding a registration's first address."""

    designation: str
    rir: str
    status: str


@dataclass(frozen=True)
class SpecialCoverage:
    """The available/reserved RIR segment holding a registration's first address."""

    registry: str
    status: str
    first: int
    last: int


@dataclass(frozen=True)
class RegistryContext:
    ready: bool
    covered_rir_blocks: int
    iana: IanaCoverage | None
    special: SpecialCoverage | None


# One round trip per RDAP miss: readiness, how many RIR-designated IANA blocks the
# registration covers entirely, the IANA block and the special segment holding its first
# address. Parameters are IPv6 texts in the shared key space (mapped_address).
REGISTRY_CONTEXT_SQL = """SELECT (SELECT ready FROM corpscout.ip_registry_ready) AS ready,
    (SELECT count() FROM corpscout.ip_registry_iana_blocks_current
     WHERE rir != '' AND toUInt128(first_ip) >= toUInt128(toIPv6(%(first)s)) AND toUInt128(last_ip) <= toUInt128(toIPv6(%(last)s))) AS covered_rir_blocks,
    (SELECT (any(designation), any(rir), any(status)) FROM corpscout.ip_registry_iana_blocks_current
     WHERE toUInt128(first_ip) <= toUInt128(toIPv6(%(first)s)) AND toUInt128(last_ip) >= toUInt128(toIPv6(%(first)s))) AS iana,
    dictGetOrDefault('corpscout.ip_registry_special_trie', ('registry', 'status', 'segment_first', 'segment_last'), tuple(toIPv6(%(first)s)), ('', '', toUInt128(0), toUInt128(0))) AS special"""

# The SQL twin of registry_class() over the aliases the derived view defines: ready,
# covered_rir_blocks, iana (designation, rir, status), special (registry, status, first, last).
# Migration 000449 embeds this text verbatim.
REGISTRY_CLASS_SQL = """multiIf(
        NOT ready, 'unknown',
        covered_rir_blocks > 0, 'registry_level',
        special.2 IN ('available', 'reserved') OR iana.3 IN ('', 'RESERVED'), 'unallocated',
        'reusable') AS registry_class"""

REGISTRY_CLASS_COLUMNS = (
    "network_key",
    "registry_class",
    "network_first",
    "network_last",
    "covered_rir_blocks",
    "iana_designation",
    "iana_rir",
    "iana_status",
    "special_registry",
    "special_status",
    "special_first",
    "special_last",
    "classified_at",
)
REGISTRY_CLASS_INSERT_SQL = (
    "INSERT INTO corpscout.rdap_network_registry_class ("
    + ", ".join(REGISTRY_CLASS_COLUMNS)
    + ") VALUES"
)
# Bulk reclassification of every cached registration from the current reference snapshots.
REGISTRY_CLASS_REFRESH_SQL = (
    "INSERT INTO corpscout.rdap_network_registry_class ("
    + ", ".join(REGISTRY_CLASS_COLUMNS)
    + ")\nSELECT "
    + ", ".join(REGISTRY_CLASS_COLUMNS[:-1])
    + ", now64(3, 'UTC')\nFROM corpscout.rdap_network_registry_class_derived\n"
    "WHERE registry_class != 'unknown'"
)


def registry_class(context: RegistryContext) -> str:
    """'reusable', 'registry_level', 'unallocated', or 'unknown' while reference data is incomplete."""
    if not context.ready:
        return "unknown"
    if context.covered_rir_blocks > 0:
        return "registry_level"
    special, iana = context.special, context.iana
    if (
        (special is not None and special.status in UNALLOCATED_STATUSES)
        or iana is None
        or iana.status == "RESERVED"
    ):
        return "unallocated"
    return "reusable"


def mapped_address(address: str) -> str:
    """The IPv6 text of an address in the shared key space (IPv4 as its ::ffff: mapping)."""
    return str(IPv6Address(address_int(address)))


def fetch_registry_context(client, first_address: str, last_address: str) -> RegistryContext:
    """The context of a registration; an empty answer means not ready."""
    rows = client.execute(
        REGISTRY_CONTEXT_SQL,
        {"first": mapped_address(first_address), "last": mapped_address(last_address)},
    )
    if not rows:
        return RegistryContext(ready=False, covered_rir_blocks=0, iana=None, special=None)
    ready, covered, iana, special = rows[0]
    iana = iana or ("", "", "")
    special = special or ("", "", 0, 0)
    return RegistryContext(
        ready=bool(ready),
        covered_rir_blocks=int(covered or 0),
        iana=IanaCoverage(iana[0], iana[1], iana[2]) if iana[2] else None,
        special=(
            SpecialCoverage(special[0], special[1], int(special[2]), int(special[3]))
            if special[1]
            else None
        ),
    )


@dataclass(frozen=True)
class RegistryClassification:
    registry_class: str
    context: RegistryContext
    first: int
    last: int

    @property
    def reusable(self) -> bool:
        """Unknown counts as reusable: without reference data the caches behave as before."""
        return self.registry_class in ("reusable", "unknown")

    def clickhouse_values(self, network_key: str, classified_at: datetime) -> tuple:
        iana, special = self.context.iana, self.context.special
        return (
            network_key,
            self.registry_class,
            IPv6Address(self.first),
            IPv6Address(self.last),
            self.context.covered_rir_blocks,
            iana.designation if iana else "",
            iana.rir if iana else "",
            iana.status if iana else "",
            special.registry if special else "",
            special.status if special else "",
            IPv6Address(special.first if special else 0),
            IPv6Address(special.last if special else 0),
            classified_at,
        )


def classify_registration(client, network: RdapNetwork) -> RegistryClassification:
    """Classify a normalized registration (the daily asset does the same in SQL)."""
    first, last = address_int(network.start_address), address_int(network.end_address)
    context = fetch_registry_context(client, network.start_address, network.end_address)
    return RegistryClassification(
        registry_class=registry_class(context), context=context, first=first, last=last
    )
```

- [ ] **Step 7: Run the pure tests, format, check definitions still load**

Run: `uv run --frozen --no-sync pytest tests/test_ip_registry_source.py -q -p no:cacheprovider`
Expected: all pass.

Run ruff format/check on `src/dagster_v3/defs/ip_registry/tables.py src/dagster_v3/defs/ip_registry/source.py src/dagster_v3/defs/commoncrawl_rdap/registry.py tests/test_ip_registry_source.py`.

Run: `uv run --frozen --no-sync dg check defs`
Expected: `All definitions loaded successfully.` (the package has no assets yet).

- [ ] **Step 8: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/ip_registry/__init__.py services/dagster_v3/src/dagster_v3/defs/ip_registry/tables.py services/dagster_v3/src/dagster_v3/defs/ip_registry/source.py services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/registry.py services/dagster_v3/tests/fixtures/ip_registry services/dagster_v3/tests/test_ip_registry_source.py
git commit -m "feat(dagster): parsers for IANA address space and RIR special segments, and the data-driven registry-level rule

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Migrations 000449 (ledger, IANA blocks, special segments, special trie, readiness, class table) and 000450 (trie exclusion) with their ClickHouse tests

000450 is written here with 000449 because the test fixture applies both; its exclusion is inert until `rdap_network_registry_class` holds rows, which on prod happens only after the first reference load (Task 6 applies it separately, after the load is reviewed).

**Files:**
- Create: `clickhouse/migrations/000449_corpscout_ip_registry_reference_data.up.sql`, `…down.sql`
- Create: `clickhouse/migrations/000450_corpscout_rdap_trie_registry_class_exclusion.up.sql`, `…down.sql`
- Modify: `services/dagster_v3/tests/test_clickhouse_migrations.py:463` (`EXPECTED_MIGRATIONS`, after `"000448_corpscout_crawl_queue_contract",`)
- Test: `services/dagster_v3/tests/test_ip_registry.py` (new; Task 3 appends to it)

**Interfaces:**
- Consumes: `REGISTRY_CLASS_SQL`, `classify_registration`, `REGISTRY_CLASS_INSERT_SQL`, `REGISTRY_CLASS_REFRESH_SQL` (Task 1), `tests.test_ip_enrichment_input.server`.
- Produces (ClickHouse, 000449): tables `corpscout.ip_registry_snapshots`, `ip_registry_iana_blocks`, `ip_registry_special_segments`, `rdap_network_registry_class`; views `ip_registry_current_snapshots`, `ip_registry_iana_blocks_current`, `ip_registry_special_segments_current`, `ip_registry_special_trie_source`, `ip_registry_ready` (one row, `ready UInt8`), `rdap_network_registry_class_current`, `rdap_network_registry_class_derived`; dictionary `ip_registry_special_trie` (`IP_TRIE`, attributes `registry`, `status`, `segment_first`, `segment_last` as in Task 1's `REGISTRY_CONTEXT_SQL`).
- Produces (ClickHouse, 000450): `corpscout.rdap_network_segments_current` rewritten to exclude networks whose current class is not `reusable`; `rdap_network_trie` recreated unchanged in name, columns and `USER 'corpscout_rdap_dictionary'` source (`_assert_rdap_storage_exists` in `commoncrawl_rdap/assets.py:787-826` checks that string).
- Produces (`tests/test_ip_registry.py`): `MIGRATIONS`, `FIXTURES`, `apply_migration(client, name, *, before=None)`, fixtures `registry_server` (module) and `clean` (function), `seed_reference_data(client)`, `reload_tries(client)`, `network_response(rir, handle, start, end, name)`, `CASES`, `insert_case_networks(client)`, `special_of(client, ip)`, `iana_of(client, ip)`.

- [ ] **Step 1: Write the migrations**

`clickhouse/migrations/000449_corpscout_ip_registry_reference_data.up.sql` (the derived view is the text verified in `scratchpad/ip/verify4.sql`; its `multiIf` is `REGISTRY_CLASS_SQL` verbatim):

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- IP registry reference data, limited to the special segments of the address space: the IANA
-- top-level blocks with their designation and status, and the available/reserved ranges of the
-- five RIRs' delegated-extended statistics. Allocated and assigned delegations are never stored.
-- Loaded daily by the ip_registry Dagster module, which also classifies every cached RDAP
-- registration against them.

-- One row per (source, snapshot) that finished loading. The _current views read the newest
-- snapshot per source from here, so a partial load is never current. verified_at moves on
-- every run that re-checks the same snapshot. records_* count the whole file, segments_* the
-- rows that were kept.
CREATE TABLE IF NOT EXISTS corpscout.ip_registry_snapshots
(
    source          LowCardinality(String),
    snapshot_date   Date,
    verified_at     DateTime64(3, 'UTC'),
    checksum        String,
    serial          String,
    records_ipv4    UInt64,
    records_ipv6    UInt64,
    segments_ipv4   UInt64,
    segments_ipv6   UInt64,
    source_url      String
)
ENGINE = ReplacingMergeTree(verified_at)
ORDER BY (source, snapshot_date);

CREATE VIEW IF NOT EXISTS corpscout.ip_registry_current_snapshots AS
SELECT source, max(snapshot_date) AS snapshot_date
FROM corpscout.ip_registry_snapshots FINAL
GROUP BY source;

-- IANA ipv4-address-space and ipv6-unicast-address-assignments rows. first_ip/last_ip are IPv6
-- (IPv4 as ::ffff:a.b.c.d) so both families compare in one key space. rir is derived from the
-- designation (APNIC, Administered by ARIN, ...) and empty for IANA-reserved and legacy holders.
-- One partition per snapshot: the loader keeps the current and the previous one.
CREATE TABLE IF NOT EXISTS corpscout.ip_registry_iana_blocks
(
    source          LowCardinality(String),
    snapshot_date   Date,
    ip_version      UInt8,
    prefix          String,
    first_ip        IPv6,
    last_ip         IPv6,
    designation     String,
    rir             LowCardinality(String),
    status          LowCardinality(String),
    assigned_on     String,
    whois           String,
    rdap            String,
    note            String,
    loaded_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY (source, snapshot_date)
ORDER BY (source, snapshot_date, ip_version, first_ip);

-- The available/reserved ipv4/ipv6 records of the delegated-extended files as published
-- (start_address, value, cc, status) plus first_ip/last_ip and the CIDR cover of the range
-- (IPv4 counts are not always powers of two). About 322k rows per snapshot, one partition per
-- snapshot: the loader keeps the current and the previous one.
CREATE TABLE IF NOT EXISTS corpscout.ip_registry_special_segments
(
    registry        LowCardinality(String),
    snapshot_date   Date,
    ip_version      UInt8,
    cc              LowCardinality(String),
    status          LowCardinality(String),
    start_address   String,
    value           UInt64,
    first_ip        IPv6,
    last_ip         IPv6,
    cidrs           Array(String),
    loaded_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY (registry, snapshot_date)
ORDER BY (registry, snapshot_date, ip_version, first_ip);

CREATE VIEW IF NOT EXISTS corpscout.ip_registry_iana_blocks_current AS
SELECT *
FROM corpscout.ip_registry_iana_blocks FINAL
WHERE (source, snapshot_date) IN (SELECT source, snapshot_date FROM corpscout.ip_registry_current_snapshots);

CREATE VIEW IF NOT EXISTS corpscout.ip_registry_special_segments_current AS
SELECT *
FROM corpscout.ip_registry_special_segments FINAL
WHERE (registry, snapshot_date) IN (SELECT source, snapshot_date FROM corpscout.ip_registry_current_snapshots);

-- Dictionary source: one row per CIDR with the segment bounds as UInt128 (IP_TRIE attributes).
CREATE VIEW IF NOT EXISTS corpscout.ip_registry_special_trie_source AS
SELECT
    cidr,
    argMax(registry, snapshot_date) AS registry,
    argMax(status, snapshot_date) AS status,
    argMax(toUInt128(first_ip), snapshot_date) AS segment_first,
    argMax(toUInt128(last_ip), snapshot_date) AS segment_last
FROM corpscout.ip_registry_special_segments_current
ARRAY JOIN cidrs AS cidr
GROUP BY cidr;

-- The dictionary reads as the least-privilege local user from migration 000126.
GRANT SELECT ON corpscout.ip_registry_snapshots TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_current_snapshots TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_special_segments TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_special_segments_current TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_special_trie_source TO corpscout_rdap_dictionary;

-- Longest-prefix lookup: the available/reserved segment holding an address (48 MiB for the
-- 325k CIDRs of 2026-09-25).
CREATE DICTIONARY IF NOT EXISTS corpscout.ip_registry_special_trie
(
    cidr            String,
    registry        String,
    status          String,
    segment_first   UInt128,
    segment_last    UInt128
)
PRIMARY KEY cidr
SOURCE(
    CLICKHOUSE(
        USER 'corpscout_rdap_dictionary'
        DB 'corpscout'
        TABLE 'ip_registry_special_trie_source'
    )
)
LAYOUT(IP_TRIE())
LIFETIME(MIN 3600 MAX 7200);

-- Reference data is usable only when every source has a loaded snapshot.
CREATE VIEW IF NOT EXISTS corpscout.ip_registry_ready AS
SELECT (
    SELECT uniqExact(source)
    FROM corpscout.ip_registry_current_snapshots
    WHERE source IN ('iana_ipv4', 'iana_ipv6', 'afrinic', 'apnic', 'arin', 'lacnic', 'ripencc')
) = 7 AS ready;

-- The classification of every cached RDAP registration, recomputed after each reference refresh
-- and written for new registrations by the enrichers. Only reusable networks may feed
-- rdap_network_trie (migration 000450).
CREATE TABLE IF NOT EXISTS corpscout.rdap_network_registry_class
(
    network_key          String,
    registry_class       LowCardinality(String),
    network_first        IPv6,
    network_last         IPv6,
    covered_rir_blocks   UInt16,
    iana_designation     String,
    iana_rir             LowCardinality(String),
    iana_status          LowCardinality(String),
    special_registry     LowCardinality(String),
    special_status       LowCardinality(String),
    special_first        IPv6,
    special_last         IPv6,
    classified_at        DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(classified_at)
ORDER BY network_key;

CREATE VIEW IF NOT EXISTS corpscout.rdap_network_registry_class_current AS
SELECT *
FROM corpscout.rdap_network_registry_class FINAL;

-- The rule, in SQL: the twin of registry_class() in commoncrawl_rdap/registry.py, which also
-- holds this multiIf text (REGISTRY_CLASS_SQL) for the contract test. registry_level when the
-- registration covers at least one entire IANA block designated to an RIR, unallocated when its
-- first address lies in an available/reserved RIR segment or in an IANA block that is reserved
-- or absent, unknown until all seven sources are loaded. Every network is joined with every
-- IANA row on a constant key (about 307 rows) so that an empty reference table still yields
-- one row per network.
CREATE VIEW IF NOT EXISTS corpscout.rdap_network_registry_class_derived AS
WITH (SELECT ready FROM corpscout.ip_registry_ready) AS ready
SELECT
    network_key,
    multiIf(
        NOT ready, 'unknown',
        covered_rir_blocks > 0, 'registry_level',
        special.2 IN ('available', 'reserved') OR iana.3 IN ('', 'RESERVED'), 'unallocated',
        'reusable') AS registry_class,
    toIPv6(net_first) AS network_first,
    toIPv6(net_last) AS network_last,
    toUInt16(covered_rir_blocks) AS covered_rir_blocks,
    iana.1 AS iana_designation,
    iana.2 AS iana_rir,
    iana.3 AS iana_status,
    special.1 AS special_registry,
    special.2 AS special_status,
    toIPv6(special.3) AS special_first,
    toIPv6(special.4) AS special_last
FROM
(
    SELECT
        n.network_key AS network_key,
        n.net_first AS net_first,
        n.net_last AS net_last,
        n.special AS special,
        countIf(b.rir != '' AND toUInt128(b.first_ip) >= n.net_first AND toUInt128(b.last_ip) <= n.net_last) AS covered_rir_blocks,
        anyIf((b.designation, b.rir, b.status), toUInt128(b.first_ip) <= n.net_first AND toUInt128(b.last_ip) >= n.net_first) AS iana
    FROM
    (
        SELECT
            1 AS one,
            network_key,
            toUInt128(toIPv6(if(ip_version = 4, concat('::ffff:', start_address), start_address))) AS net_first,
            toUInt128(toIPv6(if(ip_version = 4, concat('::ffff:', end_address), end_address))) AS net_last,
            dictGetOrDefault('corpscout.ip_registry_special_trie', ('registry', 'status', 'segment_first', 'segment_last'), tuple(toIPv6(net_first)), ('', '', toUInt128(0), toUInt128(0))) AS special
        FROM corpscout.rdap_networks_current
    ) AS n
    LEFT JOIN (SELECT 1 AS one, * FROM corpscout.ip_registry_iana_blocks_current) AS b ON n.one = b.one
    GROUP BY n.network_key, n.net_first, n.net_last, n.special
);
```

`clickhouse/migrations/000449_corpscout_ip_registry_reference_data.down.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

DROP VIEW IF EXISTS corpscout.rdap_network_registry_class_derived;
DROP VIEW IF EXISTS corpscout.rdap_network_registry_class_current;
DROP TABLE IF EXISTS corpscout.rdap_network_registry_class;
DROP VIEW IF EXISTS corpscout.ip_registry_ready;
DROP DICTIONARY IF EXISTS corpscout.ip_registry_special_trie;

REVOKE SELECT ON corpscout.ip_registry_special_trie_source FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_special_segments_current FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_special_segments FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_current_snapshots FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_snapshots FROM corpscout_rdap_dictionary;

DROP VIEW IF EXISTS corpscout.ip_registry_special_trie_source;
DROP VIEW IF EXISTS corpscout.ip_registry_special_segments_current;
DROP VIEW IF EXISTS corpscout.ip_registry_iana_blocks_current;
DROP TABLE IF EXISTS corpscout.ip_registry_special_segments;
DROP TABLE IF EXISTS corpscout.ip_registry_iana_blocks;
DROP VIEW IF EXISTS corpscout.ip_registry_current_snapshots;
DROP TABLE IF EXISTS corpscout.ip_registry_snapshots;
```

`clickhouse/migrations/000450_corpscout_rdap_trie_registry_class_exclusion.up.sql` (the dictionary block is the 000258 one; the view gains the subquery):

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Registrations classified registry_level or unallocated (rdap_network_registry_class, migration
-- 000449) answer only the IP that was queried. The trie source excludes their segments, so the
-- poisoned entries stop being served and a reclassification after a reference refresh takes
-- effect at the next dictionary reload without a code change. The exclusion sits in a subquery
-- because ClickHouse resolves the argMax alias network_key inside an outer WHERE.
GRANT SELECT ON corpscout.rdap_network_registry_class TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.rdap_network_registry_class_current TO corpscout_rdap_dictionary;

DROP DICTIONARY IF EXISTS corpscout.rdap_network_trie;
DROP VIEW IF EXISTS corpscout.rdap_network_segments_current;

CREATE VIEW corpscout.rdap_network_segments_current AS
SELECT
    cidr,
    cidr AS matched_cidr,
    argMax(network_key, tuple(derived_at, network_key)) AS network_key
FROM
(
    SELECT network_key, cidr, derived_at
    FROM corpscout.rdap_network_segments FINAL
    WHERE segment_role = 'lookup_result'
      AND prefix_length > 0
      AND network_key NOT IN
      (
          SELECT network_key
          FROM corpscout.rdap_network_registry_class_current
          WHERE registry_class != 'reusable'
      )
)
GROUP BY cidr;

CREATE DICTIONARY corpscout.rdap_network_trie
(
    cidr          String,
    matched_cidr  String,
    network_key   String
)
PRIMARY KEY cidr
SOURCE(
    CLICKHOUSE(
        USER 'corpscout_rdap_dictionary'
        DB 'corpscout'
        TABLE 'rdap_network_segments_current'
    )
)
LAYOUT(IP_TRIE())
LIFETIME(MIN 300 MAX 600);
```

`clickhouse/migrations/000450_corpscout_rdap_trie_registry_class_exclusion.down.sql` (restores the 000258 view):

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

DROP DICTIONARY IF EXISTS corpscout.rdap_network_trie;
DROP VIEW IF EXISTS corpscout.rdap_network_segments_current;

CREATE VIEW corpscout.rdap_network_segments_current AS
SELECT
    cidr,
    cidr AS matched_cidr,
    argMax(network_key, tuple(derived_at, network_key)) AS network_key
FROM corpscout.rdap_network_segments FINAL
WHERE segment_role = 'lookup_result'
  AND prefix_length > 0
GROUP BY cidr;

CREATE DICTIONARY corpscout.rdap_network_trie
(
    cidr          String,
    matched_cidr  String,
    network_key   String
)
PRIMARY KEY cidr
SOURCE(
    CLICKHOUSE(
        USER 'corpscout_rdap_dictionary'
        DB 'corpscout'
        TABLE 'rdap_network_segments_current'
    )
)
LAYOUT(IP_TRIE())
LIFETIME(MIN 300 MAX 600);

REVOKE SELECT ON corpscout.rdap_network_registry_class_current FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.rdap_network_registry_class FROM corpscout_rdap_dictionary;
```

In `services/dagster_v3/tests/test_clickhouse_migrations.py` append after line 463 (`"000448_corpscout_crawl_queue_contract",`):

```python
    "000449_corpscout_ip_registry_reference_data",
    "000450_corpscout_rdap_trie_registry_class_exclusion",
```

Run: `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py -q -p no:cacheprovider`
Expected: all pass (the files are explicit, create or drop objects, have down files, no `;` in comments).

- [ ] **Step 2: Write the ClickHouse tests**

Create `services/dagster_v3/tests/test_ip_registry.py`:

```python
"""IP registry reference data against a real ClickHouse: migration 000449, snapshots, trie, rule parity."""

import re
from datetime import UTC, date, datetime
from ipaddress import IPv6Address
from pathlib import Path

import pytest

from dagster_v3.defs.commoncrawl_rdap import registry
from dagster_v3.defs.commoncrawl_rdap.assets import RDAP_NETWORK_INSERT_SQL
from dagster_v3.defs.commoncrawl_rdap.rdap import RdapLookupResponse, normalize_rdap_network
from dagster_v3.defs.ip_registry import source, tables
from tests.test_ip_enrichment_input import server as server

MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
FIXTURES = Path(__file__).parent / "fixtures" / "ip_registry"
MIGRATION_449 = "000449_corpscout_ip_registry_reference_data"
MIGRATION_450 = "000450_corpscout_rdap_trie_registry_class_exclusion"
TEST_SOURCE = "HOST 'localhost' PORT 9000 USER 'test' PASSWORD 'test'"
IANA_DATE = date(2026, 9, 19)
SNAPSHOT_INSERT = f"INSERT INTO corpscout.{tables.SNAPSHOTS_TABLE} ({', '.join(tables.SNAPSHOT_COLUMNS)}) VALUES"
IANA_INSERT = f"INSERT INTO corpscout.{tables.IANA_TABLE} ({', '.join(tables.IANA_COLUMNS)}) VALUES"
SPECIAL_INSERT = f"INSERT INTO corpscout.{tables.SPECIAL_TABLE} ({', '.join(tables.SPECIAL_COLUMNS)}) VALUES"


def apply_migration(client, name: str, *, before: str | None = None) -> None:
    """Run a migration's statements against the test server.

    GRANT/REVOKE/CREATE USER are skipped (the test user is an admin), dictionary sources are
    pointed at the test server with the test credentials, and lifetimes are zeroed so
    SYSTEM RELOAD DICTIONARY is the only refresh. ``before`` cuts the file at a marker.
    """
    sql = (MIGRATIONS / name).read_text(encoding="utf-8")
    if before is not None:
        sql = sql.split(before, 1)[0]
    for statement in sql.split(";"):
        lines = [
            line
            for line in statement.splitlines()
            if line.strip() and not line.lstrip().startswith("--")
        ]
        text = "\n".join(lines).strip()
        if not text or text.startswith(("GRANT", "REVOKE", "CREATE USER")):
            continue
        text = text.replace("USER 'corpscout_rdap_dictionary'", TEST_SOURCE)
        text = re.sub(r"LIFETIME\(MIN \d+ MAX \d+\)", "LIFETIME(0)", text)
        client.execute(text)


@pytest.fixture(scope="module")
def registry_server(server):
    client, resource = server
    # 000124 up to its dictionary (000450 recreates rdap_network_trie with the test source).
    apply_migration(client, "000124_corpscout_rdap_networks.up.sql", before="CREATE DICTIONARY")
    apply_migration(client, f"{MIGRATION_449}.up.sql")
    apply_migration(client, f"{MIGRATION_450}.up.sql")
    return client, resource


def reload_tries(client) -> None:
    for name in (tables.SPECIAL_TRIE, "rdap_network_trie"):
        client.execute(f"SYSTEM RELOAD DICTIONARY corpscout.{name}")


@pytest.fixture
def clean(registry_server):
    client, resource = registry_server
    for table in (
        tables.SNAPSHOTS_TABLE,
        tables.IANA_TABLE,
        tables.SPECIAL_TABLE,
        "rdap_network_registry_class",
        "rdap_networks",
        "rdap_network_segments",
        "rdap_ip_lookup_results",
    ):
        client.execute(f"TRUNCATE TABLE corpscout.{table}")
    reload_tries(client)
    return client, resource


def seed_reference_data(client) -> None:
    """Insert the fixture excerpts as the current snapshot of all seven sources and reload the trie."""
    loaded_at = datetime.now(UTC)
    for source_name, url in tables.IANA_SOURCES.items():
        text = (FIXTURES / f"{source_name.replace('_', '-')}-excerpt.csv").read_text(encoding="utf-8")
        blocks = source.parse_iana_csv(text, source_name)
        client.execute(
            IANA_INSERT,
            [
                (b.source, IANA_DATE, b.ip_version, b.prefix, IPv6Address(b.first), IPv6Address(b.last),
                 b.designation, b.rir, b.status, b.assigned_on, b.whois, b.rdap, b.note, loaded_at)
                for b in blocks
            ],
        )
        ipv4, ipv6 = sum(b.ip_version == 4 for b in blocks), sum(b.ip_version == 6 for b in blocks)
        client.execute(SNAPSHOT_INSERT, [(source_name, IANA_DATE, loaded_at, "fixture", "", ipv4, ipv6, ipv4, ipv6, url)])
    for registry_name, url in tables.RIR_SOURCES.items():
        text = (FIXTURES / f"delegated-{registry_name}-extended-excerpt").read_text(encoding="utf-8")
        parsed = source.parse_delegated(text, registry_name)
        snapshot_date = parsed.header.end_date
        client.execute(
            SPECIAL_INSERT,
            [
                (s.registry, snapshot_date, s.ip_version, s.cc, s.status, s.start_address, s.value,
                 IPv6Address(s.first), IPv6Address(s.last), list(s.cidrs), loaded_at)
                for s in parsed.special
            ],
        )
        client.execute(
            SNAPSHOT_INSERT,
            [(registry_name, snapshot_date, loaded_at, "fixture", parsed.header.serial,
              parsed.summaries["ipv4"], parsed.summaries["ipv6"],
              parsed.special_count(4), parsed.special_count(6), url)],
        )
    reload_tries(client)


def network_response(rir, handle, start, end, name):
    return RdapLookupResponse(
        rir=rir,
        raw_response={
            "objectClassName": "ip network",
            "handle": handle,
            "startAddress": start,
            "endAddress": end,
            "ipVersion": "v6" if ":" in start else "v4",
            "name": name,
            "status": ["active"],
        },
    )


# (rir, handle, start, end, name, expected class) — the owner's examples plus the edge cases.
# Every block these touch is in the IANA excerpt, except OUTSIDE (no IANA block on purpose).
CASES = [
    ("apnic", "103.0.0.0 - 103.255.255.255", "103.0.0.0", "103.255.255.255", "APNIC-AP", "registry_level"),
    ("apnic", "101.0.0.0 - 101.255.255.255", "101.0.0.0", "101.255.255.255", "APNIC-101", "registry_level"),
    ("afrinic", "102.0.0.0 - 102.255.255.255", "102.0.0.0", "102.255.255.255", "AFRINIC-102", "registry_level"),
    ("apnic", "113.0.0.0 - 113.255.255.255", "113.0.0.0", "113.255.255.255", "APNIC-113", "registry_level"),
    ("arin", "NET6-2600-1", "2600::", "260f:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "ARIN-6", "registry_level"),
    ("ripencc", "EU-ZZ-2A00", "2a00::", "2a1f:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "EU-ZZ-2A00", "registry_level"),
    ("arin", "WIDE", "100.0.0.0", "103.255.255.255", "SOMEONE", "registry_level"),
    ("afrinic", "MID", "102.128.0.0", "103.255.255.255", "MID-BLOCK", "registry_level"),
    ("lacnic", "45.68.105.0/24", "45.68.105.0", "45.68.105.255", "UNALLOCATED", "unallocated"),
    ("ripencc", "AVAILABLE", "85.8.248.0", "85.8.255.255", "AVAILABLE", "unallocated"),
    ("arin", "RESERVED-2ND-CIDR", "23.128.2.0", "23.128.3.255", "RESERVED", "unallocated"),
    ("lacnic", "AVAILABLE6", "2001:1201:20::", "2001:1201:3f:ffff:ffff:ffff:ffff:ffff", "AVAILABLE6", "unallocated"),
    ("arin", "FUTURE", "240.0.0.0", "240.255.255.255", "FUTURE-USE", "unallocated"),
    ("arin", "OUTSIDE", "4000::", "4000:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "OUTSIDE-UNICAST", "unallocated"),
    ("apnic", "FPT-VN", "103.35.64.0", "103.35.67.255", "FPT-VN", "reusable"),
    ("arin", "NET-104-16-0-0-1", "104.16.0.0", "104.31.255.255", "CLOUDFLARENET", "reusable"),
    ("arin", "GOOGLE", "8.8.8.0", "8.8.8.255", "GOOGLE", "reusable"),
    ("arin", "GOOGLE-IPV6", "2001:4860::", "2001:4860:ffff:ffff:ffff:ffff:ffff:ffff", "GOOGLE-IPV6", "reusable"),
    ("arin", "FORD-NET", "19.0.0.0", "19.255.255.255", "FORD-NET", "reusable"),
    ("ripencc", "DK-NET", "195.85.96.0", "195.85.101.255", "DK-NET", "reusable"),
    # Wider than the holder delegations it spans but not a whole IANA block: reusable by the
    # owner's decision (the "wider than the delegation" branch was removed).
    ("ripencc", "SE-AND-FR", "2.0.0.0", "2.3.255.255", "SE-AND-FR", "reusable"),
    ("apnic", "PARTIAL", "103.35.66.0", "103.35.69.255", "PARTIAL", "reusable"),
]
EXPECTED_COUNTS = {"registry_level": 8, "unallocated": 6, "reusable": 8}


def insert_case_networks(client):
    """Store the CASES as cached registrations; returns network_key -> (network, expected class)."""
    stored = {}
    for rir, handle, start, end, name, expected in CASES:
        normalized = normalize_rdap_network(
            network_response(rir, handle, start, end, name),
            fetched_at=datetime.now(UTC),
            segment_role="lookup_result",
        )
        client.execute(RDAP_NETWORK_INSERT_SQL, [normalized.network.clickhouse_values()])
        stored[normalized.network.network_key] = (normalized.network, expected)
    return stored


def special_of(client, ip):
    [(row,)] = client.execute(
        "SELECT dictGetOrDefault('corpscout.ip_registry_special_trie', ('registry', 'status', 'segment_first', 'segment_last'), tuple(toIPv6(%(ip)s)), ('', '', toUInt128(0), toUInt128(0)))",
        {"ip": registry.mapped_address(ip)},
    )
    return row


def iana_of(client, ip):
    [(row,)] = client.execute(
        "SELECT (any(designation), any(rir), any(status)) FROM corpscout.ip_registry_iana_blocks_current WHERE toUInt128(first_ip) <= toUInt128(toIPv6(%(ip)s)) AND toUInt128(last_ip) >= toUInt128(toIPv6(%(ip)s))",
        {"ip": registry.mapped_address(ip)},
    )
    return tuple(row)


def test_migration_449_embeds_the_rule_and_reads_through_the_dictionary_user():
    up = (MIGRATIONS / f"{MIGRATION_449}.up.sql").read_text()
    down = (MIGRATIONS / f"{MIGRATION_449}.down.sql").read_text()
    normalize = lambda text: " ".join(text.split())  # noqa: E731
    assert normalize(registry.REGISTRY_CLASS_SQL) in normalize(up)
    assert up.count("USER 'corpscout_rdap_dictionary'") == 1
    assert up.count("LIFETIME(MIN 3600 MAX 7200)") == 1
    for name in (
        "ip_registry_snapshots", "ip_registry_current_snapshots", "ip_registry_special_segments",
        "ip_registry_special_segments_current", "ip_registry_special_trie_source",
    ):
        assert f"GRANT SELECT ON corpscout.{name} TO corpscout_rdap_dictionary" in up
        assert f"REVOKE SELECT ON corpscout.{name} FROM corpscout_rdap_dictionary" in down
    assert "PARTITION BY (registry, snapshot_date)" in up and "PARTITION BY (source, snapshot_date)" in up
    assert "TTL" not in up  # retention is the loader's DROP PARTITION, never a TTL
    assert "delegations" not in up  # no allocated/assigned delegation is stored
    assert down.index("DROP DICTIONARY") < down.index("DROP VIEW IF EXISTS corpscout.ip_registry_special_trie_source") < down.index("DROP TABLE IF EXISTS corpscout.ip_registry_special_segments")


def test_seeded_snapshots_answer_lookups(clean):
    client, _ = clean
    seed_reference_data(client)
    assert client.execute("SELECT ready FROM corpscout.ip_registry_ready") == [(1,)]
    assert client.execute(
        "SELECT source, snapshot_date FROM corpscout.ip_registry_current_snapshots ORDER BY source"
    ) == [
        ("afrinic", date(2026, 9, 24)), ("apnic", date(2026, 9, 25)), ("arin", date(2026, 9, 25)),
        ("iana_ipv4", IANA_DATE), ("iana_ipv6", IANA_DATE), ("lacnic", date(2026, 9, 24)),
        ("ripencc", date(2026, 9, 24)),
    ]
    assert client.execute("SELECT count() FROM corpscout.ip_registry_special_segments_current") == [(10,)]
    assert client.execute("SELECT count() FROM corpscout.ip_registry_special_trie_source") == [(11,)]
    assert client.execute("SELECT count() FROM corpscout.ip_registry_iana_blocks_current") == [(33,)]
    assert client.execute("SELECT count() FROM corpscout.ip_registry_iana_blocks_current WHERE rir != ''") == [(26,)]
    mapped = source.address_int
    assert special_of(client, "45.68.105.9") == ("lacnic", "reserved", mapped("45.68.105.0"), mapped("45.68.105.255"))
    assert special_of(client, "23.128.3.7") == ("arin", "reserved", mapped("23.128.1.0"), mapped("23.128.3.255"))  # second CIDR of 768 addresses
    assert special_of(client, "102.199.0.1")[:2] == ("afrinic", "available")
    assert special_of(client, "85.8.250.1")[:2] == ("ripencc", "available")
    assert special_of(client, "2001:1201:20::1")[:2] == ("lacnic", "available")
    assert special_of(client, "103.35.64.49") == ("", "", 0, 0)
    assert special_of(client, "8.8.8.8") == ("", "", 0, 0)
    assert iana_of(client, "103.0.0.0") == ("APNIC", "apnic", "ALLOCATED")
    assert iana_of(client, "19.5.0.1") == ("Ford Motor Company", "", "LEGACY")
    assert iana_of(client, "45.68.105.9") == ("Administered by ARIN", "arin", "LEGACY")
    assert iana_of(client, "2600:1f00::1") == ("ARIN", "arin", "ALLOCATED")
    assert iana_of(client, "4000::1") == ("", "", "")


def test_a_newer_ledger_row_switches_the_current_snapshot_but_a_partial_load_does_not(clean):
    client, _ = clean
    seed_reference_data(client)
    loaded_at = datetime.now(UTC)
    later = date(2026, 9, 25)
    # Rows without a ledger row are not current: 85.8.248.0/21 stays available.
    client.execute(
        SPECIAL_INSERT,
        [("ripencc", later, 4, "", "reserved", "5.134.16.0", 2048, IPv6Address(source.address_int("5.134.16.0")),
          IPv6Address(source.address_int("5.134.23.255")), ["5.134.16.0/21"], loaded_at)],
    )
    reload_tries(client)
    assert special_of(client, "85.8.250.1")[1] == "available"
    assert client.execute("SELECT snapshot_date FROM corpscout.ip_registry_current_snapshots WHERE source = 'ripencc'") == [(date(2026, 9, 24),)]
    client.execute(SNAPSHOT_INSERT, [("ripencc", later, loaded_at, "fixture-2", "1790373599", 8, 3, 1, 0, "")])
    reload_tries(client)
    assert special_of(client, "85.8.250.1") == ("", "", 0, 0)  # the new snapshot lists only the reserved range
    assert special_of(client, "5.134.17.1")[1] == "reserved"
    assert client.execute("SELECT count() FROM corpscout.ip_registry_special_segments WHERE registry = 'ripencc'") == [(3,)]  # both snapshots kept until the loader drops the older one
    client.execute("TRUNCATE TABLE corpscout.ip_registry_snapshots")
    assert client.execute("SELECT ready FROM corpscout.ip_registry_ready") == [(0,)]


def test_python_rule_and_derived_view_agree_on_every_case(clean):
    client, _ = clean
    stored = insert_case_networks(client)
    # Not ready: the view keeps every network (left join) and both rules say unknown.
    assert client.execute("SELECT count(), groupUniqArray(registry_class) FROM corpscout.rdap_network_registry_class_derived") == [(len(CASES), ["unknown"])]
    assert all(registry.classify_registration(client, network).registry_class == "unknown" for network, _ in stored.values())
    seed_reference_data(client)
    from_sql = dict(client.execute("SELECT network_key, registry_class FROM corpscout.rdap_network_registry_class_derived"))
    from_python = {key: registry.classify_registration(client, network).registry_class for key, (network, _) in stored.items()}
    expected = {key: expected for key, (_, expected) in stored.items()}
    assert from_sql == expected
    assert from_python == expected
    assert {cls: list(expected.values()).count(cls) for cls in EXPECTED_COUNTS} == EXPECTED_COUNTS
    assert dict(client.execute("SELECT network_key, covered_rir_blocks FROM corpscout.rdap_network_registry_class_derived WHERE covered_rir_blocks > 0")) == {
        "apnic:103.0.0.0 - 103.255.255.255": 1, "apnic:101.0.0.0 - 101.255.255.255": 1, "afrinic:102.0.0.0 - 102.255.255.255": 1,
        "apnic:113.0.0.0 - 113.255.255.255": 1, "arin:NET6-2600-1": 1, "ripencc:EU-ZZ-2A00": 2, "arin:WIDE": 4, "afrinic:MID": 1,
    }
    # The persisted row shape is the same from both writers.
    for key in ("apnic:FPT-VN", "lacnic:45.68.105.0/24"):
        classification = registry.classify_registration(client, stored[key][0])
        client.execute(registry.REGISTRY_CLASS_INSERT_SQL, [classification.clickhouse_values(key, datetime.now(UTC))])
    client.execute(registry.REGISTRY_CLASS_REFRESH_SQL)
    rows = client.execute(
        "SELECT network_key, registry_class, covered_rir_blocks, iana_designation, iana_rir, iana_status, special_registry, special_status, toString(special_first), toString(special_last) FROM corpscout.rdap_network_registry_class_current WHERE network_key IN ('apnic:FPT-VN', 'lacnic:45.68.105.0/24') ORDER BY network_key"
    )
    assert rows == [
        ("apnic:FPT-VN", "reusable", 0, "APNIC", "apnic", "ALLOCATED", "", "", "::", "::"),
        ("lacnic:45.68.105.0/24", "unallocated", 0, "Administered by ARIN", "arin", "LEGACY", "lacnic", "reserved", "::ffff:45.68.105.0", "::ffff:45.68.105.255"),
    ]
    assert client.execute("SELECT count() FROM corpscout.rdap_network_registry_class_current") == [(len(CASES),)]


def test_migration_450_serves_only_reusable_registrations_and_follows_reclassification(clean):
    from dagster_v3.defs.commoncrawl_rdap.assets import RDAP_SEGMENT_INSERT_SQL

    client, _ = clean
    up = (MIGRATIONS / f"{MIGRATION_450}.up.sql").read_text()
    assert "WHERE registry_class != 'reusable'" in up and "USER 'corpscout_rdap_dictionary'" in up
    assert up.index("DROP DICTIONARY") < up.index("DROP VIEW") < up.index("CREATE VIEW") < up.index("CREATE DICTIONARY")
    stored = {}
    for rir, handle, start, end, name, expected in CASES:
        normalized = normalize_rdap_network(
            network_response(rir, handle, start, end, name), fetched_at=datetime.now(UTC), segment_role="lookup_result"
        )
        client.execute(RDAP_NETWORK_INSERT_SQL, [normalized.network.clickhouse_values()])
        client.execute(RDAP_SEGMENT_INSERT_SQL, [segment.clickhouse_values() for segment in normalized.segments])
        stored[normalized.network.network_key] = expected
    reload_tries(client)
    # Without class rows every lookup_result segment is served, exactly as before 000450.
    assert client.execute(
        "SELECT dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv4('103.15.66.50')), '')"
    ) == [("apnic:103.0.0.0 - 103.255.255.255",)]
    seed_reference_data(client)
    client.execute(registry.REGISTRY_CLASS_REFRESH_SQL)
    reload_tries(client)
    served = {key for (key,) in client.execute("SELECT DISTINCT network_key FROM corpscout.rdap_network_segments_current")}
    assert served == {key for key, expected in stored.items() if expected == "reusable"}
    assert client.execute(
        "SELECT dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv4('103.15.66.50')), ''), dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv4('103.35.64.49')), ''), dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv6('2600:1f00::1')), ''), dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv4('8.8.8.8')), '')"
    ) == [("", "apnic:FPT-VN", "", "arin:GOOGLE")]
    # A later classification wins (ReplacingMergeTree by network_key): mark FPT registry_level, then reusable again.
    for registry_class, expect_served in (("registry_level", False), ("reusable", True)):
        client.execute(
            registry.REGISTRY_CLASS_INSERT_SQL,
            [("apnic:FPT-VN", registry_class, IPv6Address(0), IPv6Address(0), 0, "", "", "", "", "", IPv6Address(0), IPv6Address(0), datetime.now(UTC))],
        )
        reload_tries(client)
        assert ("apnic:FPT-VN" in {key for (key,) in client.execute("SELECT DISTINCT network_key FROM corpscout.rdap_network_segments_current")}) is expect_served
```

- [ ] **Step 3: Run the tests**

Run: `uv run --frozen --no-sync pytest tests/test_ip_registry.py tests/test_clickhouse_migrations.py tests/test_commoncrawl_rdap_assets.py -q -p no:cacheprovider`
Expected: all pass.

Run ruff format/check on `tests/test_ip_registry.py`.

- [ ] **Step 4: Commit**

```bash
git add clickhouse/migrations/000449_corpscout_ip_registry_reference_data.up.sql clickhouse/migrations/000449_corpscout_ip_registry_reference_data.down.sql clickhouse/migrations/000450_corpscout_rdap_trie_registry_class_exclusion.up.sql clickhouse/migrations/000450_corpscout_rdap_trie_registry_class_exclusion.down.sql services/dagster_v3/tests/test_clickhouse_migrations.py services/dagster_v3/tests/test_ip_registry.py
git commit -m "feat(clickhouse): IP registry special segments, their lookup trie and RDAP registration classes (000449, 000450)

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: The `ip_registry` Dagster module — loaders with retention, class asset, freshness checks, job, daily schedule

**Files:**
- Create: `services/dagster_v3/src/dagster_v3/defs/ip_registry/assets.py`
- Modify: `services/dagster_v3/tests/test_ip_registry.py` (append the loader/asset tests)

**Interfaces:**
- Consumes: Task 1 parsers and `REGISTRY_CLASS_REFRESH_SQL`; Task 2 objects; `assert_clickhouse_tables_exist` (`defs/clickhouse/resolved.py:25`).
- Produces (`dagster_v3.defs.ip_registry.assets`): `GROUP_NAME = "ip_registry"`, `INSERT_BATCH`, `MAX_SHRINK_RATIO = 0.05`, `IANA_MIN_ROWS`, `FRESH_SNAPSHOT_DAYS = 3`, `FRESH_VERIFIED_DAYS = 2`, `IpRegistryConfig(allow_shrink: bool = False)`, `fetch(url) -> tuple[bytes, Mapping]`, `current_snapshot(client, source) -> dict | None`, `refuse_shrink(...)`, `record_snapshot(...)`, `insert_rows(...)`, `drop_superseded_snapshots(client, *, table, key_column, source) -> list[date]`, `iana_snapshot_date(headers) -> date`, `load_iana_source(client, source, *, body, headers, url, min_rows=None) -> dict`, `load_delegated_source(client, registry, *, body, md5_text, url, allow_shrink) -> dict`, `snapshot_freshness(sources, rows, now) -> dg.AssetCheckResult`, assets `ip_registry_iana_blocks`, `special_segment_assets` (list of five, names `ip_registry_special_segments_<rir>`), `rdap_network_registry_class`, `checks` (list of seven `AssetChecksDefinition`), `ip_registry_refresh_job`, `ip_registry_daily`, `defs`.

- [ ] **Step 1: Write the failing asset tests**

Append to `services/dagster_v3/tests/test_ip_registry.py` (add `import hashlib`, `from datetime import timedelta`, `import dagster as dg` and `from dagster_v3.defs.ip_registry import assets` to its imports):

```python
IANA_LAST_MODIFIED = "Sat, 19 Sep 2026 00:44:20 GMT"


def fixture_http(monkeypatch, *, tamper=None, iana_last_modified=IANA_LAST_MODIFIED):
    """Serve the fixture excerpts (and matching .md5 files) instead of the internet."""
    bodies = {}
    for source_name, url in tables.IANA_SOURCES.items():
        bodies[url] = ((FIXTURES / f"{source_name.replace('_', '-')}-excerpt.csv").read_bytes(), {"Last-Modified": iana_last_modified})
    for registry_name, url in tables.RIR_SOURCES.items():
        body = (FIXTURES / f"delegated-{registry_name}-extended-excerpt").read_bytes()
        digest = hashlib.md5(body).hexdigest()
        md5 = (
            f"{digest}  delegated-arin-extended-20260925\n"
            if registry_name == "arin"
            else f"MD5 (delegated-{registry_name}-extended-latest) = {digest}\n"
        )
        bodies[url] = (body, {})
        bodies[url + ".md5"] = (md5.encode(), {})
    bodies.update(tamper or {})
    calls = []

    def fetch(url):
        calls.append(url)
        return bodies[url]

    monkeypatch.setattr(assets, "fetch", fetch)
    monkeypatch.setattr(assets, "IANA_MIN_ROWS", {"iana_ipv4": 23, "iana_ipv6": 10})
    return calls


def dated_ripencc(day: bytes, body: bytes | None = None) -> dict:
    """A tamper dict serving the RIPE excerpt with another end date and a matching .md5."""
    body = (FIXTURES / "delegated-ripencc-extended-excerpt").read_bytes() if body is None else body
    dated = body.replace(b"|20260924|+0200", b"|" + day + b"|+0200", 1)
    url = tables.RIR_SOURCES["ripencc"]
    return {url: (dated, {}), url + ".md5": (f"MD5 (x) = {hashlib.md5(dated).hexdigest()}\n".encode(), {})}


def refresh(resource, **config):
    return dg.materialize(
        [assets.ip_registry_iana_blocks, *assets.special_segment_assets, assets.rdap_network_registry_class, *assets.checks],
        resources={"clickhouse": resource},
        run_config={"ops": {asset.op.name: {"config": config} for asset in assets.special_segment_assets}} if config else None,
        raise_on_error=False,
    )


def test_refresh_loads_every_source_classifies_and_passes_the_checks(clean, monkeypatch):
    client, resource = clean
    stored = insert_case_networks(client)
    calls = fixture_http(monkeypatch)
    result = refresh(resource)
    assert result.success
    assert sorted(calls) == sorted([*tables.IANA_SOURCES.values(), *tables.RIR_SOURCES.values(), *(url + ".md5" for url in tables.RIR_SOURCES.values())])
    assert client.execute(
        "SELECT source, snapshot_date, records_ipv4, records_ipv6, segments_ipv4, segments_ipv6 FROM corpscout.ip_registry_snapshots FINAL ORDER BY source"
    ) == [
        ("afrinic", date(2026, 9, 24), 5, 2, 2, 0), ("apnic", date(2026, 9, 25), 6, 2, 2, 0), ("arin", date(2026, 9, 25), 4, 3, 1, 0),
        ("iana_ipv4", IANA_DATE, 23, 0, 23, 0), ("iana_ipv6", IANA_DATE, 0, 10, 0, 10), ("lacnic", date(2026, 9, 24), 3, 4, 1, 2),
        ("ripencc", date(2026, 9, 24), 8, 3, 2, 0),
    ]
    assert client.execute("SELECT serial FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'arin'") == [("1790341220831",)]
    assert client.execute("SELECT count() FROM corpscout.ip_registry_special_segments_current") == [(10,)]
    assert client.execute("SELECT count() FROM corpscout.ip_registry_special_segments") == [(10,)]  # no allocated row anywhere
    assert special_of(client, "45.68.105.9")[:2] == ("lacnic", "reserved")
    assert dict(client.execute("SELECT network_key, registry_class FROM corpscout.rdap_network_registry_class_current")) == {
        key: expected for key, (_, expected) in stored.items()
    }
    materialization = result.asset_materializations_for_node("rdap_network_registry_class")[0].metadata
    assert materialization["networks_registry_level"].value == 8 and materialization["networks_total"].value == len(CASES)
    evaluations = result.get_asset_check_evaluations()
    assert len(evaluations) == 7 and all(evaluation.passed for evaluation in evaluations)
    assert {evaluation.check_name for evaluation in evaluations} == {"snapshot_fresh", "classification_complete"}
    loaded = result.asset_materializations_for_node("ip_registry_special_segments_ripencc")[0].metadata
    assert (loaded["loaded"].value, loaded["records_ipv4"].value, loaded["segments_ipv4"].value, loaded["md5"].value) == (
        True, 8, 2, hashlib.md5((FIXTURES / "delegated-ripencc-extended-excerpt").read_bytes()).hexdigest()
    )


def test_identical_snapshot_is_verified_not_reloaded(clean, monkeypatch):
    client, resource = clean
    fixture_http(monkeypatch)
    assert refresh(resource).success
    [(rows_before, verified_before)] = client.execute("SELECT count(), max(verified_at) FROM corpscout.ip_registry_snapshots FINAL")
    segments_before = client.execute("SELECT count() FROM corpscout.ip_registry_special_segments")
    again = refresh(resource)
    assert again.success
    assert again.asset_materializations_for_node("ip_registry_special_segments_apnic")[0].metadata["loaded"].value is False
    assert again.asset_materializations_for_node("ip_registry_iana_blocks")[0].metadata["iana_ipv4_loaded"].value is False
    [(rows_after, verified_after)] = client.execute("SELECT count(), max(verified_at) FROM corpscout.ip_registry_snapshots FINAL")
    assert (rows_after, rows_before) == (7, 7) and verified_after > verified_before
    assert client.execute("SELECT count() FROM corpscout.ip_registry_special_segments") == segments_before


def test_bad_checksum_older_file_and_shrinking_snapshot_are_refused(clean, monkeypatch):
    client, resource = clean
    ripencc = tables.RIR_SOURCES["ripencc"]
    body = (FIXTURES / "delegated-ripencc-extended-excerpt").read_bytes()
    fixture_http(monkeypatch, tamper={ripencc + ".md5": (b"MD5 (delegated-ripencc-extended-latest) = " + b"0" * 32 + b"\n", {})})
    result = refresh(resource)
    assert not result.success
    assert client.execute("SELECT count() FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'ripencc'") == [(0,)]
    assert client.execute("SELECT count() FROM corpscout.ip_registry_special_segments WHERE registry = 'ripencc'") == [(0,)]
    assert client.execute("SELECT count() FROM corpscout.ip_registry_snapshots FINAL") == [(6,)]  # the other six loaded
    assert client.execute("SELECT ready FROM corpscout.ip_registry_ready") == [(0,)]
    fixture_http(monkeypatch)
    assert refresh(resource).success
    # A file dated before the current snapshot is refused.
    fixture_http(monkeypatch, tamper=dated_ripencc(b"20260923"))
    assert not refresh(resource).success
    # A newer file that lost half its IPv4 records (four allocated ones: the special rows are
    # untouched, the guard watches the whole file) is refused unless allow_shrink is set.
    shrunk = (
        body.replace(b"2|ripencc|1790287199|11|", b"2|ripencc|1790373599|7|", 1)
        .replace(b"ripencc|*|ipv4|*|8|summary", b"ripencc|*|ipv4|*|4|summary", 1)
        .replace(b"ripencc|SE|ipv4|2.0.0.0|131072|20100712|allocated|12a581c1-ea86-46af-9554-77e3b4ab3df5\n", b"")
        .replace(b"ripencc|SE|ipv4|2.2.0.0|65536|20100712|allocated|12a581c1-ea86-46af-9554-77e3b4ab3df5\n", b"")
        .replace(b"ripencc|FR|ipv4|2.3.0.0|65536|20100712|allocated|9a489e65-dd78-443e-96ab-e21e016b5113\n", b"")
        .replace(b"ripencc|PS|ipv4|1.178.112.0|4096|20071126|allocated|172ce676-8ded-4901-9812-793bd0b4ec77\n", b"")
    )
    fixture_http(monkeypatch, tamper=dated_ripencc(b"20260925", shrunk))
    assert not refresh(resource).success
    assert client.execute("SELECT snapshot_date FROM corpscout.ip_registry_current_snapshots WHERE source = 'ripencc'") == [(date(2026, 9, 24),)]
    fixture_http(monkeypatch, tamper=dated_ripencc(b"20260925", shrunk))
    assert refresh(resource, allow_shrink=True).success
    assert client.execute(
        "SELECT snapshot_date, records_ipv4, segments_ipv4 FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'ripencc' ORDER BY snapshot_date DESC LIMIT 1"
    ) == [(date(2026, 9, 25), 4, 2)]


def test_loader_keeps_only_the_current_and_previous_snapshot(clean, monkeypatch):
    client, resource = clean
    for day in (b"20260924", b"20260925", b"20260926"):
        fixture_http(monkeypatch, tamper=dated_ripencc(day))
        result = refresh(resource)
        assert result.success
    assert client.execute(
        "SELECT snapshot_date FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'ripencc' ORDER BY snapshot_date"
    ) == [(date(2026, 9, 24),), (date(2026, 9, 25),), (date(2026, 9, 26),)]  # the ledger keeps every load
    assert client.execute(
        "SELECT DISTINCT snapshot_date FROM corpscout.ip_registry_special_segments WHERE registry = 'ripencc' ORDER BY snapshot_date"
    ) == [(date(2026, 9, 25),), (date(2026, 9, 26),)]  # rows: current + previous only
    assert client.execute("SELECT count() FROM corpscout.ip_registry_special_segments_current WHERE registry = 'ripencc'") == [(2,)]
    assert result.asset_materializations_for_node("ip_registry_special_segments_ripencc")[0].metadata["dropped_snapshots"].value == 1
    assert client.execute("SELECT count() FROM corpscout.ip_registry_special_segments WHERE registry = 'apnic'") == [(2,)]  # untouched source: one snapshot


def test_freshness_check_flags_missing_stale_and_unverified_sources():
    now = datetime(2026, 9, 25, 12, tzinfo=UTC)
    fresh = now - timedelta(hours=6)
    rows = [
        ("ripencc", date(2026, 9, 24), fresh),
        ("apnic", date(2026, 9, 20), fresh),                        # stale RIR snapshot
        ("arin", date(2026, 9, 25), now - timedelta(days=3)),       # not re-verified
        ("iana_ipv4", date(2026, 3, 1), fresh),                     # IANA: old snapshot is fine
    ]
    passed = assets.snapshot_freshness(("ripencc",), rows, now)
    assert passed.passed and passed.metadata["ripencc_snapshot_date"].value == "2026-09-24"
    assert assets.snapshot_freshness(("iana_ipv4",), rows, now).passed
    stale = assets.snapshot_freshness(("apnic",), rows, now)
    assert not stale.passed and "apnic: snapshot 2026-09-20 is stale" in stale.description
    unverified = assets.snapshot_freshness(("arin",), rows, now)
    assert not unverified.passed and "arin: last verified 2026-09-22" in unverified.description
    missing = assets.snapshot_freshness(("lacnic",), rows, now)
    assert not missing.passed and "lacnic: no snapshot" in missing.description


def test_definitions_expose_the_daily_job_stopped_by_default():
    assert assets.ip_registry_daily.cron_schedule == "5 6 * * *"
    assert assets.ip_registry_daily.default_status == dg.DefaultScheduleStatus.STOPPED
    assert assets.ip_registry_daily.job_name == "ip_registry_refresh_job"
    names = {asset.key.to_user_string() for asset in [assets.ip_registry_iana_blocks, *assets.special_segment_assets, assets.rdap_network_registry_class]}
    assert names == {"ip_registry_iana_blocks", "rdap_network_registry_class", *(f"ip_registry_special_segments_{r}" for r in tables.RIR_SOURCES)}
    assert len(assets.checks) == 7
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_ip_registry.py -q -p no:cacheprovider`
Expected: FAIL — `ImportError: cannot import name 'assets' from 'dagster_v3.defs.ip_registry'`.

- [ ] **Step 3: Create `assets.py`**

Create `services/dagster_v3/src/dagster_v3/defs/ip_registry/assets.py`:

```python
"""Daily refresh of the IP registry special segments and the RDAP registration classes.

Seven small public files (IANA ipv4/ipv6 address space, five RIR delegated-extended
statistics, ~45 MB in total) are downloaded and verified as whole files (checksum, version
line, record and summary counts, no sharp shrink of the whole-file record count against the
current snapshot); only the IANA blocks and the RIRs' available/reserved ranges are inserted,
as a dated snapshot whose ledger row is written last. The loader then drops every partition of
that source except the current and the previous snapshot. Then every cached RDAP registration
is classified in SQL and rdap_network_trie is reloaded. Non-partitioned full refresh (the whole
dataset comes back per request), daily schedule stopped by default, one pool for the chain.
"""

import hashlib
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from ipaddress import IPv6Address

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dlt.sources.helpers import requests
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.commoncrawl_rdap.registry import REGISTRY_CLASS_REFRESH_SQL
from dagster_v3.defs.ip_registry import tables
from dagster_v3.defs.ip_registry.source import parse_delegated, parse_iana_csv, parse_md5

GROUP_NAME = "ip_registry"
INSERT_BATCH = 50_000
# Refuse a snapshot whose whole-file ipv4 or ipv6 record count dropped by more than this share
# against the current one (a truncated download, a moved file) unless the run says allow_shrink.
# The kept special-segment counts are not guarded: they swing legitimately every day.
MAX_SHRINK_RATIO = 0.05
# Complete registries: 256 IPv4 /8s; the IPv6 unicast file had 51 rows on 2026-09-25 and
# only grows.
IANA_MIN_ROWS = {"iana_ipv4": 256, "iana_ipv6": 40}
FRESH_SNAPSHOT_DAYS = 3  # the RIRs publish daily
FRESH_VERIFIED_DAYS = 2  # every source must have been re-checked recently
SNAPSHOT_INSERT_SQL = (
    f"INSERT INTO corpscout.{tables.SNAPSHOTS_TABLE} ({', '.join(tables.SNAPSHOT_COLUMNS)}) VALUES"
)
IANA_INSERT_SQL = f"INSERT INTO corpscout.{tables.IANA_TABLE} ({', '.join(tables.IANA_COLUMNS)}) VALUES"
SPECIAL_INSERT_SQL = (
    f"INSERT INTO corpscout.{tables.SPECIAL_TABLE} ({', '.join(tables.SPECIAL_COLUMNS)}) VALUES"
)


class IpRegistryConfig(dg.Config):
    allow_shrink: bool = Field(
        default=False,
        description="Accept a file with more than 5% fewer ipv4 or ipv6 records than the current snapshot.",
    )


def fetch(url: str) -> tuple[bytes, Mapping[str, str]]:
    """The body and headers of a small public file (dlt's session retries connection errors and 5xx)."""
    response = requests.get(url, timeout=(10, 300))
    response.raise_for_status()
    return response.content, response.headers


def current_snapshot(client, source: str) -> dict | None:
    rows = client.execute(
        f"""SELECT snapshot_date, checksum, records_ipv4, records_ipv6
        FROM corpscout.{tables.SNAPSHOTS_TABLE} FINAL
        WHERE source = %(source)s ORDER BY snapshot_date DESC LIMIT 1""",
        {"source": source},
    )
    if not rows:
        return None
    snapshot_date, checksum, records_ipv4, records_ipv6 = rows[0]
    return {
        "snapshot_date": snapshot_date,
        "checksum": checksum,
        "records_ipv4": records_ipv4,
        "records_ipv6": records_ipv6,
    }


def refuse_shrink(source: str, current: dict | None, ipv4: int, ipv6: int, *, allow_shrink: bool) -> None:
    if current is None or allow_shrink:
        return
    for kind, new, old in (("ipv4", ipv4, current["records_ipv4"]), ("ipv6", ipv6, current["records_ipv6"])):
        if old and new < old * (1 - MAX_SHRINK_RATIO):
            raise ValueError(
                f"{source}: {kind} records dropped from {old} to {new} (more than "
                f"{MAX_SHRINK_RATIO:.0%}); set allow_shrink to accept the snapshot"
            )


def record_snapshot(
    client,
    *,
    source: str,
    snapshot_date: date,
    checksum: str,
    serial: str,
    records: tuple[int, int],
    segments: tuple[int, int],
    url: str,
) -> None:
    client.execute(
        SNAPSHOT_INSERT_SQL,
        [(source, snapshot_date, datetime.now(UTC), checksum, serial, *records, *segments, url)],
    )


def insert_rows(client, sql: str, rows: Sequence[tuple]) -> None:
    for offset in range(0, len(rows), INSERT_BATCH):
        client.execute(sql, rows[offset : offset + INSERT_BATCH])


def drop_superseded_snapshots(client, *, table: str, key_column: str, source: str) -> list[date]:
    """Drop every partition of a source except its SNAPSHOTS_KEPT newest ledger snapshots.

    Partitions are (source, snapshot_date); the ledger decides which snapshots are worth
    keeping, so a partial load that never got its ledger row is dropped as well.
    """
    kept = {
        snapshot_date
        for (snapshot_date,) in client.execute(
            f"""SELECT snapshot_date FROM corpscout.{tables.SNAPSHOTS_TABLE} FINAL
            WHERE source = %(source)s ORDER BY snapshot_date DESC LIMIT {tables.SNAPSHOTS_KEPT}""",
            {"source": source},
        )
    }
    present = [
        snapshot_date
        for (snapshot_date,) in client.execute(
            f"SELECT DISTINCT snapshot_date FROM corpscout.{table} WHERE {key_column} = %(source)s ORDER BY snapshot_date",
            {"source": source},
        )
    ]
    dropped = []
    for snapshot_date in present:
        if snapshot_date in kept:
            continue
        client.execute(
            f"ALTER TABLE corpscout.{table} DROP PARTITION (%(source)s, %(snapshot_date)s)",
            {"source": source, "snapshot_date": snapshot_date},
        )
        dropped.append(snapshot_date)
    return dropped


def iana_snapshot_date(headers: Mapping[str, str]) -> date:
    value = headers.get("Last-Modified")
    if not value:
        raise ValueError("IANA response carries no Last-Modified header")
    return parsedate_to_datetime(value).astimezone(UTC).date()


def load_iana_source(
    client, source: str, *, body: bytes, headers: Mapping[str, str], url: str, min_rows: int | None = None
) -> dict:
    """Parse, validate and store one IANA file; an already loaded snapshot only refreshes verified_at."""
    blocks = parse_iana_csv(body.decode("utf-8-sig"), source)
    floor = IANA_MIN_ROWS[source] if min_rows is None else min_rows
    if len(blocks) < floor:
        raise ValueError(f"{source}: {len(blocks)} rows, expected at least {floor}")
    checksum = hashlib.sha256(body).hexdigest()
    snapshot_date = iana_snapshot_date(headers)
    current = current_snapshot(client, source)
    ipv4 = sum(block.ip_version == 4 for block in blocks)
    ipv6 = sum(block.ip_version == 6 for block in blocks)
    loaded = not (
        current is not None
        and current["snapshot_date"] == snapshot_date
        and current["checksum"] == checksum
    )
    if loaded:
        if current is not None and snapshot_date < current["snapshot_date"]:
            raise ValueError(
                f"{source}: Last-Modified {snapshot_date} is older than the current snapshot "
                f"{current['snapshot_date']}"
            )
        loaded_at = datetime.now(UTC)
        insert_rows(
            client,
            IANA_INSERT_SQL,
            [
                (
                    block.source, snapshot_date, block.ip_version, block.prefix,
                    IPv6Address(block.first), IPv6Address(block.last), block.designation, block.rir,
                    block.status, block.assigned_on, block.whois, block.rdap, block.note, loaded_at,
                )
                for block in blocks
            ],
        )
    record_snapshot(
        client, source=source, snapshot_date=snapshot_date, checksum=checksum,
        serial=headers.get("Last-Modified", ""), records=(ipv4, ipv6), segments=(ipv4, ipv6), url=url,
    )
    dropped = drop_superseded_snapshots(client, table=tables.IANA_TABLE, key_column="source", source=source)
    return {
        "source": source,
        "snapshot_date": snapshot_date.isoformat(),
        "rows": len(blocks),
        "loaded": loaded,
        "sha256": checksum,
        "dropped_snapshots": len(dropped),
    }


def load_delegated_source(
    client, registry: str, *, body: bytes, md5_text: str, url: str, allow_shrink: bool
) -> dict:
    """Verify the published MD5, parse the whole file, validate against the current snapshot and store its special segments."""
    digest = hashlib.md5(body).hexdigest()
    expected = parse_md5(md5_text)
    if digest != expected:
        raise ValueError(f"{registry}: MD5 {digest} does not match the published {expected}")
    parsed = parse_delegated(body.decode("utf-8"), registry)
    ipv4, ipv6 = parsed.summaries.get("ipv4", 0), parsed.summaries.get("ipv6", 0)
    if not ipv4 and not ipv6:
        raise ValueError(f"{registry}: no ipv4/ipv6 records")
    segments = (parsed.special_count(4), parsed.special_count(6))
    snapshot_date = parsed.header.end_date
    current = current_snapshot(client, registry)
    loaded = not (
        current is not None
        and current["snapshot_date"] == snapshot_date
        and current["checksum"] == digest
    )
    if loaded:
        if current is not None and snapshot_date < current["snapshot_date"]:
            raise ValueError(
                f"{registry}: file date {snapshot_date} is older than the current snapshot "
                f"{current['snapshot_date']}"
            )
        refuse_shrink(registry, current, ipv4, ipv6, allow_shrink=allow_shrink)
        loaded_at = datetime.now(UTC)
        insert_rows(
            client,
            SPECIAL_INSERT_SQL,
            [
                (
                    segment.registry, snapshot_date, segment.ip_version, segment.cc, segment.status,
                    segment.start_address, segment.value, IPv6Address(segment.first), IPv6Address(segment.last),
                    list(segment.cidrs), loaded_at,
                )
                for segment in parsed.special
            ],
        )
    record_snapshot(
        client, source=registry, snapshot_date=snapshot_date, checksum=digest,
        serial=parsed.header.serial, records=(ipv4, ipv6), segments=segments, url=url,
    )
    dropped = drop_superseded_snapshots(
        client, table=tables.SPECIAL_TABLE, key_column="registry", source=registry
    )
    return {
        "source": registry,
        "snapshot_date": snapshot_date.isoformat(),
        "records_ipv4": ipv4,
        "records_ipv6": ipv6,
        "segments_ipv4": segments[0],
        "segments_ipv6": segments[1],
        "loaded": loaded,
        "md5": digest,
        "serial": parsed.header.serial,
        "dropped_snapshots": len(dropped),
    }


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "iana"},
    pool=tables.IP_REGISTRY_POOL,
    description="Downloads IANA's ipv4-address-space and ipv6-unicast-address-assignments CSVs and "
    "stores them as a dated snapshot (Last-Modified) of the top-level address blocks.",
)
def ip_registry_iana_blocks(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE, tables=(tables.SNAPSHOTS_TABLE, tables.IANA_TABLE)
    )
    metadata = {}
    with clickhouse.get_connection() as client:
        for source, url in tables.IANA_SOURCES.items():
            body, headers = fetch(url)
            result = load_iana_source(client, source, body=body, headers=headers, url=url)
            context.log.info("%s: %s", source, result)
            metadata.update({f"{source}_{key}": value for key, value in result.items() if key != "source"})
    return dg.MaterializeResult(metadata=metadata)


def special_segment_asset(registry: str, url: str) -> dg.AssetsDefinition:
    @dg.asset(
        name=f"ip_registry_special_segments_{registry}",
        group_name=GROUP_NAME,
        kinds={"python", "clickhouse", "rir"},
        pool=tables.IP_REGISTRY_POOL,
        description=f"Downloads {url} and its .md5, verifies the whole file and stores only its "
        f"available/reserved ipv4/ipv6 ranges as the snapshot dated by the file's end date.",
    )
    def _asset(
        context: dg.AssetExecutionContext, config: IpRegistryConfig, clickhouse: ClickhouseResource
    ) -> dg.MaterializeResult:
        assert_clickhouse_tables_exist(
            clickhouse, database=tables.DATABASE, tables=(tables.SNAPSHOTS_TABLE, tables.SPECIAL_TABLE)
        )
        body, _ = fetch(url)
        md5_body, _ = fetch(url + ".md5")
        with clickhouse.get_connection() as client:
            result = load_delegated_source(
                client, registry, body=body, md5_text=md5_body.decode("utf-8"), url=url,
                allow_shrink=config.allow_shrink,
            )
        context.log.info("%s: %s", registry, result)
        return dg.MaterializeResult(metadata=result)

    return _asset


special_segment_assets = [special_segment_asset(registry, url) for registry, url in tables.RIR_SOURCES.items()]


@dg.asset(
    deps=[ip_registry_iana_blocks, *special_segment_assets],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "rdap"},
    pool=tables.IP_REGISTRY_POOL,
    description="Reloads the special-segment trie and reclassifies every cached RDAP registration "
    "(reusable, registry_level, unallocated) from the current snapshots, then reloads "
    "rdap_network_trie so excluded segments stop being served.",
)
def rdap_network_registry_class(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.DATABASE,
        tables=(
            "rdap_networks_current",
            "rdap_network_registry_class",
            "rdap_network_registry_class_derived",
            tables.READY_VIEW,
        ),
    )
    with clickhouse.get_connection() as client:
        client.execute(f"SYSTEM RELOAD DICTIONARY corpscout.{tables.SPECIAL_TRIE}")
        [(ready,)] = client.execute(f"SELECT ready FROM corpscout.{tables.READY_VIEW}")
        if not ready:
            raise ValueError(
                "Reference data is incomplete: every source needs a loaded snapshot before classification"
            )
        client.execute(REGISTRY_CLASS_REFRESH_SQL)
        counts = dict(
            client.execute(
                "SELECT registry_class, count() FROM corpscout.rdap_network_registry_class_current GROUP BY registry_class"
            )
        )
        client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    context.log.info("RDAP registrations classified: %s", counts)
    return dg.MaterializeResult(
        metadata={
            **{f"networks_{registry_class}": count for registry_class, count in counts.items()},
            "networks_total": sum(counts.values()),
        }
    )


def snapshot_freshness(sources: Sequence[str], rows: Sequence[tuple], now: datetime) -> dg.AssetCheckResult:
    """Fail when a source has no snapshot, was not re-verified recently, or (RIRs) is stale."""
    latest = {source: (snapshot_date, verified_at) for source, snapshot_date, verified_at in rows}
    problems = []
    metadata = {}
    for source in sources:
        if source not in latest:
            problems.append(f"{source}: no snapshot")
            continue
        snapshot_date, verified_at = latest[source]
        if verified_at.tzinfo is None:
            verified_at = verified_at.replace(tzinfo=UTC)
        metadata[f"{source}_snapshot_date"] = snapshot_date.isoformat()
        metadata[f"{source}_verified_at"] = verified_at.isoformat()
        if now - verified_at > timedelta(days=FRESH_VERIFIED_DAYS):
            problems.append(f"{source}: last verified {verified_at.date()}")
        if not source.startswith("iana") and now.date() - snapshot_date > timedelta(days=FRESH_SNAPSHOT_DAYS):
            problems.append(f"{source}: snapshot {snapshot_date} is stale")
    return dg.AssetCheckResult(
        passed=not problems,
        severity=dg.AssetCheckSeverity.ERROR,
        description="Reference snapshots are current." if not problems else "; ".join(problems),
        metadata=metadata,
    )


def freshness_check(asset: dg.AssetsDefinition, sources: tuple[str, ...]) -> dg.AssetChecksDefinition:
    @dg.asset_check(
        asset=asset,
        name="snapshot_fresh",
        description=f"Fails when the latest snapshot of {', '.join(sources)} is older than "
        f"{FRESH_SNAPSHOT_DAYS} days (RIRs) or was not re-verified within {FRESH_VERIFIED_DAYS} days.",
    )
    def _check(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
        with clickhouse.get_connection() as client:
            rows = client.execute(
                f"""SELECT source, max(snapshot_date), max(verified_at)
                FROM corpscout.{tables.SNAPSHOTS_TABLE} FINAL
                WHERE source IN %(sources)s GROUP BY source""",
                {"sources": sources},
            )
        return snapshot_freshness(sources, rows, datetime.now(UTC))

    return _check


@dg.asset_check(
    asset=rdap_network_registry_class,
    name="classification_complete",
    description="Fails when the reference data is not ready or a cached registration has no class row.",
)
def classification_complete(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        [(ready,)] = client.execute(f"SELECT ready FROM corpscout.{tables.READY_VIEW}")
        [(missing,)] = client.execute(
            """SELECT count() FROM corpscout.rdap_networks_current
            WHERE network_key NOT IN (SELECT network_key FROM corpscout.rdap_network_registry_class_current)"""
        )
    return dg.AssetCheckResult(
        passed=bool(ready) and missing == 0,
        severity=dg.AssetCheckSeverity.ERROR,
        description=f"ready={bool(ready)}, registrations without a class row: {missing}",
        metadata={"ready": bool(ready), "unclassified_networks": int(missing)},
    )


checks = [
    freshness_check(ip_registry_iana_blocks, tuple(tables.IANA_SOURCES)),
    *(
        freshness_check(asset, (registry,))
        for asset, registry in zip(special_segment_assets, tables.RIR_SOURCES, strict=True)
    ),
    classification_complete,
]

ip_registry_refresh_job = dg.define_asset_job(
    "ip_registry_refresh_job",
    selection=dg.AssetSelection.assets(rdap_network_registry_class).upstream(),
)
# Daily, 06:05 UTC: the RIR files dated D are all published by then (APNIC publishes D's file
# on D+1 at +10:00) and a block allocated yesterday must not stay "unallocated" for a week.
# No other schedule uses this minute. STOPPED by default; start it at instance level.
ip_registry_daily = dg.ScheduleDefinition(
    name="ip_registry_daily",
    job=ip_registry_refresh_job,
    cron_schedule="5 6 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[ip_registry_iana_blocks, *special_segment_assets, rdap_network_registry_class],
    asset_checks=checks,
    jobs=[ip_registry_refresh_job],
    schedules=[ip_registry_daily],
)
```

- [ ] **Step 4: Run the tests and the definitions check**

Run: `uv run --frozen --no-sync pytest tests/test_ip_registry.py tests/test_ip_registry_source.py -q -p no:cacheprovider`
Expected: all pass.

Run: `uv run --frozen --no-sync dg check defs`
Expected: `All definitions loaded successfully.` (group `ip_registry`: 7 assets, 7 checks, job `ip_registry_refresh_job`, schedule `ip_registry_daily`).

Run ruff format/check on `src/dagster_v3/defs/ip_registry/assets.py tests/test_ip_registry.py`.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/ip_registry/assets.py services/dagster_v3/tests/test_ip_registry.py
git commit -m "feat(dagster): ip_registry module loads IANA blocks and RIR special segments daily and classifies cached RDAP registrations

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: RdapEnricher and the legacy bucket worker never index a non-reusable registration

Today both writers store every direct registration as `lookup_result` segments and reuse it at once: `RdapEnricher` (`ip_enrichment/enrichment.py:191-202, 361-363`) via `_persist` + `_remember`, the bucket worker (`commoncrawl_rdap/assets.py:603-613`) via `_insert_normalized_networks` + `in_run_segments`. The change is the same in both: classify the direct registration with the Python rule (one `REGISTRY_CONTEXT_SQL` round trip), write its class row between the network row and the segment rows (so the trie source never sees a segment without its class), and skip the in-run reuse when the class is not reusable. Parents are stored as before (they never feed the trie). Nothing else changes; the trie exclusion itself is data (Task 2).

**Files:**
- Modify: `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py:25, 175, 191-202, 361-363`
- Modify: `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/results.py:151-172, 255-259`
- Modify: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/assets.py:22-27, 414-430, 603-613, 650-688, 787-800`
- Modify: `services/dagster_v3/tests/test_commoncrawl_rdap_assets.py:75-80, 292-301` and append a test
- Modify: `services/dagster_v3/tests/test_ip_enrichment_results.py:84-108` and append a test

**Interfaces:**
- Consumes: `classify_registration`, `RegistryClassification`, `REGISTRY_CLASS_INSERT_SQL`, `REGISTRY_CONTEXT_SQL` (Task 1); `apply_migration`, `seed_reference_data` (Task 2).
- Produces: `RdapEnricher.registry_level_responses: int`, results metadata key `registry_level_responses`; worker count `registry_level_networks`; `_insert_normalized_networks(..., classification: RegistryClassification | None = None)`.

- [ ] **Step 1: Write the failing worker test**

In `services/dagster_v3/tests/test_commoncrawl_rdap_assets.py` replace `FakeRdapWriteClient` (lines 75-80) with:

```python
class FakeRdapWriteClient:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[tuple[object, ...]]]] = []
        self.queries: list[tuple[str, object]] = []
        # What REGISTRY_CONTEXT_SQL answers; empty means the reference data is not loaded.
        self.context_rows: list[tuple[object, ...]] = []

    def execute(self, query: str, rows=None):
        if query.lstrip().startswith("SELECT"):
            self.queries.append((query, rows))
            return list(self.context_rows)
        self.inserts.append((query, list(rows or [])))
        return None
```

In `test_rdap_asset_rejects_the_pre_reader_dictionary_definition` extend `required_tables` (after `("rdap_ip_lookup_results_current",),`) with:

```python
        ("rdap_network_registry_class",),
        ("rdap_network_registry_class_current",),
        ("ip_registry_ready",),
```

Add to the imports `from dagster_v3.defs.commoncrawl_rdap import registry`, and append:

```python
def _apnic_block_response() -> RdapLookupResponse:
    return RdapLookupResponse(
        rir="apnic",
        raw_response={
            "objectClassName": "ip network",
            "handle": "103.0.0.0 - 103.255.255.255",
            "startAddress": "103.0.0.0",
            "endAddress": "103.255.255.255",
            "ipVersion": "v4",
            "name": "APNIC-AP",
            "type": "ALLOCATED PORTABLE",
            "status": ["active"],
        },
    )


def test_rdap_bucket_never_reuses_a_registry_level_registration() -> None:
    read_client = FakeRdapReadClient([("103.35.64.49", 4), ("103.15.66.50", 4)])
    write_client = FakeRdapWriteClient()
    # The /8 answer covers IANA's 103/8 (designated to APNIC): registry level.
    write_client.context_rows = [(1, 1, ("APNIC", "apnic", "ALLOCATED"), ("", "", 0, 0))]
    rdap_client = FakeRdapClient([_apnic_block_response(), _apnic_block_response()])
    config = CommoncrawlRdapConfig(
        candidate_scan_limit=10,
        max_requests=5,
        insert_batch_size=10,
        request_delay_seconds=0,
        parent_depth=0,
        rate_limit_retry_seconds=3600,
        transient_retry_seconds=900,
    )
    with dg.build_asset_context(partition_key="bucket_007") as context:
        result = _enrich_rdap_bucket(
            context=context,
            clickhouse=FakeRdapClickhouseResource(read_client, write_client),
            bucket_index=7,
            config=config,
            rdap_client=rdap_client,
            queried_at=FETCHED_AT,
            sleep=lambda _seconds: None,
        )
    # The second address of the block is looked up itself: the /8 answers only 103.35.64.49.
    assert rdap_client.direct_queries == ["103.35.64.49", "103.15.66.50"]
    assert result["in_run_trie_skips"] == 0 and result["registry_level_networks"] == 2
    assert [query for query, _rows in write_client.queries] == [registry.REGISTRY_CONTEXT_SQL] * 2
    assert [query for query, _rows in write_client.inserts] == [
        RDAP_NETWORK_INSERT_SQL,
        registry.REGISTRY_CLASS_INSERT_SQL,
        RDAP_SEGMENT_INSERT_SQL,
        RDAP_LOOKUP_INSERT_SQL,
    ]
    [class_rows] = [rows for query, rows in write_client.inserts if query == registry.REGISTRY_CLASS_INSERT_SQL]
    assert class_rows[0][:2] == ("apnic:103.0.0.0 - 103.255.255.255", "registry_level")
    assert class_rows[0][4:8] == (1, "APNIC", "apnic", "ALLOCATED") and class_rows[0][-1] == FETCHED_AT
```

- [ ] **Step 2: Write the failing enricher test**

In `services/dagster_v3/tests/test_ip_enrichment_results.py` add to the imports `from tests.test_ip_registry import apply_migration, seed_reference_data` and replace the `for table in (…)` truncation loop of the `environment` fixture (lines 98-108, right after the `CREATE DICTIONARY` statement) with:

```python
    # Reference data, classes and the class-aware trie view (000449/000450), idempotent.
    apply_migration(client, "000449_corpscout_ip_registry_reference_data.up.sql")
    apply_migration(client, "000450_corpscout_rdap_trie_registry_class_exclusion.up.sql")
    for table in (
        "ip_enrichment_input",
        "ip_enrichment_results",
        "rdap_networks",
        "rdap_network_segments",
        "rdap_ip_lookup_results",
        "rdap_network_registry_class",
        "ip_registry_snapshots",
        "ip_registry_iana_blocks",
        "ip_registry_special_segments",
    ):
        client.execute(f"TRUNCATE TABLE corpscout.{table}")
    client.execute("SYSTEM RELOAD DICTIONARY corpscout.ip_registry_special_trie")
```

(The dictionary the fixture creates by hand is replaced by 000450's — same name, columns and test source. Every existing test now runs with the reference tables empty: `ready = 0`, every classification `unknown`, behaviour unchanged.) Append:

```python
def test_registry_level_registration_answers_only_the_queried_ip(environment, monkeypatch):
    env = environment
    seed_reference_data(env.client)
    monkeypatch.setattr(
        enrichment.RdapClient,
        "lookup_ip",
        lambda self, ip: (env.calls.append(ip), response(ip, start="103.0.0.0", end="103.255.255.255"))[1],
    )
    first = run(env, select(env, ["103.35.64.49"]))
    assert first.success
    assert first.asset_materializations_for_node("ip_enrichment_results")[0].metadata["registry_level_responses"].value == 1
    assert env.client.execute(
        "SELECT rdap_lookup_status, rdap_matched_cidr, rdap_start_address FROM corpscout.ip_enrichment_current"
    ) == [("found", "103.0.0.0/8", "103.0.0.0")]
    assert env.client.execute(
        "SELECT network_key, registry_class, covered_rir_blocks, iana_rir, special_status FROM corpscout.rdap_network_registry_class_current"
    ) == [("arin:TEST-103.35.64.49", "registry_level", 1, "apnic", "")]
    env.client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    assert env.client.execute("SELECT count() FROM corpscout.rdap_network_segments_current") == [(0,)]
    # Another address of the block is looked up, not served from the /8 (neither trie nor in-run cache).
    assert run(env, select(env, ["103.15.66.50", "103.15.66.51"]), batch_size=1).success
    assert env.calls == ["103.35.64.49", "103.15.66.50", "103.15.66.51"]
    # A holder registration is classified reusable and serves its neighbours as before.
    monkeypatch.setattr(
        enrichment.RdapClient,
        "lookup_ip",
        lambda self, ip: (env.calls.append(ip), response(ip, start="103.35.64.0", end="103.35.67.255"))[1],
    )
    assert run(env, select(env, ["103.35.64.1", "103.35.64.2"]), batch_size=1).success
    assert env.calls == ["103.35.64.49", "103.15.66.50", "103.15.66.51", "103.35.64.1"]
    assert env.client.execute(
        "SELECT registry_class FROM corpscout.rdap_network_registry_class_current WHERE network_key = 'arin:TEST-103.35.64.1'"
    ) == [("reusable",)]
    env.client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    assert env.client.execute(
        "SELECT dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv4('103.35.65.9')), '')"
    ) == [("arin:TEST-103.35.64.1",)]
```

- [ ] **Step 3: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_commoncrawl_rdap_assets.py -k "never_reuses or pre_reader" tests/test_ip_enrichment_results.py -k "registry_level" -q -p no:cacheprovider`
Expected: FAIL — `KeyError: 'registry_level_networks'` and `KeyError: 'registry_level_responses'`.

- [ ] **Step 4: Change `enrichment.py`**

After line 25 (`from dagster_v3.defs.commoncrawl_rdap.client import RdapClient, RdapClientError`) add:

```python
from dagster_v3.defs.commoncrawl_rdap.registry import (
    REGISTRY_CLASS_INSERT_SQL,
    RegistryClassification,
    classify_registration,
)
```

In `RdapEnricher.__init__` after `self.parent_failures = 0` (line 175) add `self.registry_level_responses = 0`.

Replace `_persist` (lines 191-202) with:

```python
    def _persist(self, normalized, classification: RegistryClassification | None = None):
        self.client.execute(
            RDAP_NETWORK_INSERT_SQL,
            [normalized.network.clickhouse_values()],
            settings={"async_insert": 1, "wait_for_async_insert": 1},
        )
        if classification is not None and classification.registry_class != "unknown":
            # The class row lands before the segments: the trie source (migration 000450)
            # never sees a segment whose class it does not know.
            self.client.execute(
                REGISTRY_CLASS_INSERT_SQL,
                [
                    classification.clickhouse_values(
                        normalized.network.network_key, normalized.network.fetched_at
                    )
                ],
                settings={"async_insert": 1, "wait_for_async_insert": 1},
            )
        self.client.execute(
            RDAP_SEGMENT_INSERT_SQL,
            [segment.clickhouse_values() for segment in normalized.segments],
            settings={"async_insert": 1, "wait_for_async_insert": 1},
        )
        self.networks_written += 1
```

In `lookup` replace (lines 361-363):

```python
        # Coverage is durable before any exact-IP outcome refers to it.
        self._persist(direct)
        self._remember(direct)
```

with:

```python
        # Coverage is durable before any exact-IP outcome refers to it. A registry-level or
        # unallocated registration is stored for this address only: the trie excludes it by
        # class (migration 000450) and the in-run cache never holds it.
        classification = classify_registration(self.client, direct.network)
        self._persist(direct, classification)
        if classification.reusable:
            self._remember(direct)
        else:
            self.registry_level_responses += 1
            self.log.info(
                "RDAP registration %s is %s; it answers only %s",
                direct.network.network_key,
                classification.registry_class,
                ip,
            )
```

In `results.py`: extend the `assert_clickhouse_tables_exist` tuple (lines 154-162) with `"rdap_network_registry_class", "rdap_network_registry_class_current", "ip_registry_ready",`; add `"registry_level_responses": 0,` to the `counts` dict after `"parent_lookup_failures": 0,` (line 171); add `registry_level_responses=rdap.registry_level_responses,` to `counts.update(...)` after `parent_lookup_failures=rdap.parent_failures,` (line 258). The metadata spreads `**counts`, so the key reaches the materialization.

- [ ] **Step 5: Change the legacy bucket worker**

In `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/assets.py` after the `rdap` import block (line 27) add:

```python
from dagster_v3.defs.commoncrawl_rdap.registry import (
    REGISTRY_CLASS_INSERT_SQL,
    RegistryClassification,
    classify_registration,
)
```

In the `Counter` of `_enrich_rdap_bucket` add `"registry_level_networks": 0,` after `"registry_catch_all_responses": 0,`.

Replace (lines 603-613):

```python
            network_rows, segment_rows = _insert_normalized_networks(
                write_client,
                normalized_networks,
                written_network_keys=written_network_keys,
                batch_size=config.insert_batch_size,
            )
            counts["network_rows_written"] += network_rows
            counts["segment_rows_written"] += segment_rows
            counts["segments"] += segment_rows
            for segment in direct.segments:
                in_run_segments.add(segment.cidr)
```

with:

```python
            classification = classify_registration(write_client, direct.network)
            network_rows, segment_rows = _insert_normalized_networks(
                write_client,
                normalized_networks,
                written_network_keys=written_network_keys,
                batch_size=config.insert_batch_size,
                classification=classification,
            )
            counts["network_rows_written"] += network_rows
            counts["segment_rows_written"] += segment_rows
            counts["segments"] += segment_rows
            if classification.reusable:
                for segment in direct.segments:
                    in_run_segments.add(segment.cidr)
            else:
                counts["registry_level_networks"] += 1
                context.log.info(
                    "RDAP registration %s is %s; it answers only %s",
                    direct.network.network_key,
                    classification.registry_class,
                    address,
                )
```

Replace `_insert_normalized_networks` (lines 650-688) with:

```python
def _insert_normalized_networks(
    client: Any,
    normalized_networks: list[NormalizedRdapNetwork],
    *,
    written_network_keys: set[str],
    batch_size: int,
    classification: RegistryClassification | None = None,
) -> tuple[int, int]:
    """Insert networks, then the direct network's class row, then segments (the trie source order)."""
    new_networks = [
        normalized
        for normalized in normalized_networks
        if normalized.network.network_key not in written_network_keys
    ]
    if not new_networks:
        return 0, 0
    network_rows = [
        normalized.network.clickhouse_values() for normalized in new_networks
    ]
    segment_rows = [
        segment.clickhouse_values()
        for normalized in new_networks
        for segment in normalized.segments
    ]
    _insert_rows_in_chunks(
        client,
        RDAP_NETWORK_INSERT_SQL,
        network_rows,
        batch_size=batch_size,
    )
    direct = normalized_networks[0]
    if (
        classification is not None
        and classification.registry_class != "unknown"
        and new_networks[0].network.network_key == direct.network.network_key
    ):
        client.execute(
            REGISTRY_CLASS_INSERT_SQL,
            [classification.clickhouse_values(direct.network.network_key, direct.network.fetched_at)],
        )
    _insert_rows_in_chunks(
        client,
        RDAP_SEGMENT_INSERT_SQL,
        segment_rows,
        batch_size=batch_size,
    )
    written_network_keys.update(
        normalized.network.network_key for normalized in new_networks
    )
    return len(network_rows), len(segment_rows)
```

In `_assert_rdap_storage_exists` (lines 787-800) extend the `tables` tuple with `"rdap_network_registry_class", "rdap_network_registry_class_current", "ip_registry_ready",`.

- [ ] **Step 6: Run the suites**

Run: `uv run --frozen --no-sync pytest tests/test_commoncrawl_rdap_assets.py tests/test_ip_enrichment_results.py tests/test_ip_registry.py -q -p no:cacheprovider`
Expected: all pass (the existing worker tests still see `[network, segment, lookup]` inserts because their fake answers the context query with no rows → `unknown`).

Run: `uv run --frozen --no-sync dg check defs`
Expected: `All definitions loaded successfully.`

Run ruff format/check on `src/dagster_v3/defs/ip_enrichment/enrichment.py src/dagster_v3/defs/ip_enrichment/results.py src/dagster_v3/defs/commoncrawl_rdap/assets.py tests/test_commoncrawl_rdap_assets.py tests/test_ip_enrichment_results.py`.

- [ ] **Step 7: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py services/dagster_v3/src/dagster_v3/defs/ip_enrichment/results.py services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/assets.py services/dagster_v3/tests/test_commoncrawl_rdap_assets.py services/dagster_v3/tests/test_ip_enrichment_results.py
git commit -m "fix(dagster): classify each new RDAP registration against the registry data and never reuse registry-level or unallocated blocks

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Documentation

**Files:**
- Create: `services/dagster_v3/src/dagster_v3/defs/ip_registry/docs/ip_registry-design.md`
- Create: `services/dagster_v3/docs/operations/ip-registry-reference-data.md`
- Modify: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md` (append a section)
- Modify: `services/dagster_v3/docs/ip-enrichment-schema.md` (append a paragraph to "## Migration and validation")

- [ ] **Step 1: Write the design doc**

Create `services/dagster_v3/src/dagster_v3/defs/ip_registry/docs/ip_registry-design.md`:

````markdown
# ip_registry design doc

Records decisions, not code. Follows `docs/data-source-guidelines.md`; deviations are called out.

## 1. Source overview
- **Registry**: IANA (top-level address space) and the five RIRs (delegated-extended statistics), of
  which only the special segments are kept: the IANA blocks and the RIRs' `available`/`reserved`
  ranges. No allocated/assigned delegation is stored (owner decision 2026-09-25: "we don't want a local
  database for RDAP"). Reference data, not company data — no entity key, no translation, no currency,
  no contacts (§6–8 of the guidelines do not apply).
- **Module**: `defs/ip_registry/` · no DuckDB file (see §3) · pool `ip_registry`
- **ClickHouse tables**: `corpscout.ip_registry_snapshots`, `ip_registry_iana_blocks`,
  `ip_registry_special_segments`, `rdap_network_registry_class` (migration `000449`); trie exclusion in
  `000450`.
- **Datasets**:
  | dataset | url | format | size | cadence | auth? |
  |---|---|---|---|---|---|
  | IANA IPv4 address space | https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.csv | CSV, 256 rows (204 name an RIR) | 23 KB | rare (Last-Modified) | no |
  | IANA IPv6 unicast assignments | https://www.iana.org/assignments/ipv6-unicast-address-assignments/ipv6-unicast-address-assignments.csv | CSV, 51 rows (34 name an RIR), multi-line quoted notes | 6 KB | rare | no |
  | delegated-{afrinic,apnic,arin,lacnic,ripencc}-extended-latest (+ .md5) | ftp.afrinic.net/pub/stats/afrinic, ftp.apnic.net/stats/apnic, ftp.arin.net/pub/stats/arin, ftp.lacnic.net/pub/stats/lacnic, ftp.ripe.net/pub/stats/ripencc | pipe-separated, version line + summaries + records | 1–18 MB, 654k ipv4/ipv6 records of which 322k are available/reserved | daily | no |
- **Record count**: 307 IANA blocks; ≈322k special rows → ≈325k CIDRs in the trie (2026-09-25:
  afrinic 8,239 · apnic 100,042 · arin 81,738 · lacnic 46,821 · ripencc 84,732; IPv6 dominates because
  the RIRs enumerate free IPv6 space in fixed-size chunks).

## 2. Ingest mode — and why
- Chosen: single-request full refresh per file, non-partitioned, daily. Every file is the whole
  registry; partitions would only add event-log churn (CLAUDE.md, `exchange_rates_v2` precedent).
- Daily rather than weekly: the files change every day and a block allocated yesterday would otherwise
  be classified `unallocated` (excluded from the trie, every address in it costing an RDAP call) for up
  to a week; the refresh is a 45 MB download plus seconds of work.
- Snapshots are dated (the file's end date / IANA's Last-Modified); `_current` views read the newest
  ledger snapshot per source; the loader keeps the current and the previous snapshot's partitions and
  drops the rest (`SNAPSHOTS_KEPT = 2`). The ledger keeps one row per load.
- Format quirks: APNIC's `#` banner; RIPE NCC and LACNIC write seven fields for available/reserved
  records; IPv4 `value` is an address count, not always a power of two; ARIN's `.md5` is GNU style
  with a dated file name; the IANA IPv4 header is `Status [1]`; the IANA RDAP column glues two URLs.

## 3. Loading — deviation from the DuckDB golden path
- Reader: Python (`csv` for IANA, `str.split('|')` for the RIRs), then batched native inserts.
- Why: the files are small (≤ 18 MB, ≤ 261k lines) and the kept rows need address arithmetic
  (IPv4-mapped integers, `ipaddress.summarize_address_range`) that DuckDB does not offer; parsing takes
  seconds. A DuckDB stage would add a file, a pool and no value. This is the deviation from §3.
- Validation of the whole file before anything is written: MD5 against the published digest, version
  line parsed and its `records` equal to the record lines, each ipv4/ipv6 summary equal to its type's
  count over all statuses, known statuses, parsable special addresses, a file not older than the
  current snapshot, and no >5% drop in the whole-file ipv4/ipv6 record counts (`allow_shrink` run
  config overrides). The kept special-segment counts are not guarded (they swing legitimately; RIPE
  has 4 available ipv4 rows). The ledger row is written after the rows.

## 4. Transform
- None outside the load: derived columns (`first_ip`, `last_ip`, `cidrs`, `rir`) are computed in
  Python at load time. The classification of RDAP registrations is set-based SQL
  (`rdap_network_registry_class_derived`).

## 5. ClickHouse schema — and DDL deviations
- Grain: one row per (source, snapshot_date, block) / (registry, snapshot_date, special record) /
  (network_key) for classes. `ReplacingMergeTree(loaded_at | verified_at | classified_at)`.
- Address bounds are `IPv6` columns (IPv4 as `::ffff:a.b.c.d`) compared as `UInt128` so both families
  share one key space; the special-segment dictionary is an `IP_TRIE` over `ARRAY JOIN cidrs`
  (`RANGE_HASHED` cannot look up `UInt128` ranges on 26.5); the IANA rows are joined directly.
- `PARTITION BY (source|registry, snapshot_date)`; no TTL (a TTL could delete the current snapshot of
  a source that stops publishing); retention is the loader's `DROP PARTITION`.
- No `raw_*` payloads and no `source_payload_hash` (the ledger keeps one checksum per snapshot).

## 6–7. Translation, contacts, currency
- Not applicable (reference data, no free text, no monetary amounts, no company contacts).

## 8. Scheduling
- `ip_registry_refresh_job` = the seven loaders → `rdap_network_registry_class`; `ip_registry_daily`
  at `5 6 * * *` UTC (the APNIC file dated D appears on D+1 at +10:00), STOPPED by default, started at
  instance level. Checks: `snapshot_fresh` per loader (RIR snapshot ≤ 3 days, verified ≤ 2 days; IANA
  verified only) and `classification_complete`.

## 9. Issues found during processing
- The special segments are not small for IPv6 (≈309k rows): the plan sizes storage and the trie
  (48 MiB) for it instead of assuming a few hundred rows.
- ClickHouse resolves the `argMax(...) AS network_key` alias inside an outer `WHERE`
  (`ILLEGAL_AGGREGATION`), so the trie view's exclusion lives in a subquery.
- A `CROSS JOIN` with the IANA rows drops every network while the reference table is empty; the
  derived view joins on a constant key (`LEFT JOIN … ON n.one = b.one`) so the not-ready case still
  yields one `unknown` row per network.
- A registration's first address, not the queried IP, is the lookup point, so that the bulk view and
  the per-miss classifier ask the same question.
- APNIC's version line has an empty start date; AFRINIC's is `00000000`; ARIN's asn dates can be
  `00000000`.

## 10. Verification
- Tests: `tests/test_ip_registry_source.py` (parsers, rule, freshness), `tests/test_ip_registry.py`
  (migrations 449/450, snapshot switch, retention, trie, loaders with fixture HTTP, SQL/Python parity,
  trie exclusion), enricher/worker tests in `tests/test_ip_enrichment_results.py` and
  `tests/test_commoncrawl_rdap_assets.py`.
- Live: migrate 449 → light_sync → run `ip_registry_refresh_job` → checks green → review the
  excluded-network report → migrate 450 → reload `rdap_network_trie` → start the schedule
  (`docs/operations/ip-registry-reference-data.md`).
````

- [ ] **Step 2: Write the operations guide**

Create `services/dagster_v3/docs/operations/ip-registry-reference-data.md`:

````markdown
# IP registry reference data

Group `ip_registry` loads, daily, the special segments of the IP address space into ClickHouse — the
IANA top-level blocks and the `available`/`reserved` ranges of the five RIRs' delegated-extended
statistics (no allocated/assigned delegation is stored) — and classifies every cached RDAP
registration (`corpscout.rdap_networks`) as `reusable`, `registry_level` or `unallocated`. Only
reusable registrations feed `rdap_network_trie` (migration 000450), so an RDAP answer such as
`APNIC-AP` (103.0.0.0/8) is stored for the address that was queried and never served to other
addresses.

## Objects

| Object | Meaning |
| --- | --- |
| `ip_registry_snapshots` | ledger: one row per (source, snapshot_date); `verified_at` moves on every re-check; `records_*` count the whole file, `segments_*` the kept rows; the newest row per source is the current snapshot |
| `ip_registry_iana_blocks` / `_current` | IANA ipv4-address-space + ipv6-unicast rows (`rir` derived from the designation, empty for reserved and legacy single-holder blocks) |
| `ip_registry_special_segments` / `_current` | the RIRs' available/reserved ranges with `first_ip`/`last_ip` and `cidrs`; only the current and the previous snapshot are kept |
| `ip_registry_special_trie` | `IP_TRIE` dictionary (≈325k CIDRs, 48 MiB): `dictGetOrDefault(..., tuple(toIPv6(x)))` gives the special segment holding `x` |
| `ip_registry_ready` | `ready = 1` when all seven sources have a current snapshot |
| `rdap_network_registry_class` / `_current` / `_derived` | persisted class per registration / the rule applied live to `rdap_networks_current` |

## Running it

- Job `ip_registry_refresh_job` (seven loaders, then `rdap_network_registry_class`); schedule
  `ip_registry_daily` 06:05 UTC, stopped by default — start it on the Schedules page.
- A loader refuses (and the class asset does not run) on: MD5 mismatch, malformed version line,
  record/summary count mismatch, a file older than the current snapshot, or a >5% drop in the
  whole-file ipv4/ipv6 record count. For a legitimate drop re-launch with run config
  `ops: ip_registry_special_segments_<rir>: config: {allow_shrink: true}`.
- An identical file only refreshes `verified_at`; a changed file of the same date is loaded again
  (ReplacingMergeTree keeps the latest load). After every load the loader drops the partitions of
  that source older than the previous snapshot (`dropped_snapshots` in the metadata).
- Checks: `snapshot_fresh` on each loader (RIR snapshot ≤ 3 days, verified ≤ 2 days; IANA verified
  only), `classification_complete` on the class asset.

## The rule

For a registration N = [first, last]: `registry_level` when N covers at least one entire IANA block
designated to an RIR (`APNIC`, `ARIN`, `RIPE NCC`, `LACNIC`, `AFRINIC`, `Administered by …`);
`unallocated` when N's first address lies in an `available`/`reserved` RIR segment, in an IANA
`RESERVED` block, or in no IANA block; else `reusable`; `unknown` while `ip_registry_ready = 0` (then
nothing is excluded and the enrichers behave as before). Legacy single-holder blocks (Ford 19/8) are
reusable. `commoncrawl_rdap/registry.py` holds the Python rule and the SQL text the derived view
embeds; `tests/test_ip_registry.py` proves their parity.

## Useful queries

```sql
SELECT registry_class, count() FROM corpscout.rdap_network_registry_class_current GROUP BY registry_class;

-- Non-reusable registrations and how many addresses each one served
SELECT c.network_key, c.registry_class, c.covered_rir_blocks, n.name, n.start_address, n.end_address, ifNull(e.served_ips, 0) AS served_ips
FROM corpscout.rdap_network_registry_class_current AS c
LEFT JOIN corpscout.rdap_networks_current AS n ON n.network_key = c.network_key
LEFT JOIN (SELECT rdap_network_key, count() AS served_ips FROM corpscout.ip_enrichment_current WHERE rdap_lookup_status = 'found' GROUP BY rdap_network_key) AS e ON e.rdap_network_key = c.network_key
WHERE c.registry_class != 'reusable'
ORDER BY served_ips DESC;

-- Why a registration got its class
SELECT * FROM corpscout.rdap_network_registry_class_derived WHERE network_key = 'apnic:103.0.0.0 - 103.255.255.255';

-- Snapshots on disk per source (current + previous)
SELECT registry, snapshot_date, count() FROM corpscout.ip_registry_special_segments GROUP BY registry, snapshot_date ORDER BY registry, snapshot_date;

SELECT name, status, element_count, formatReadableSize(bytes_allocated) FROM system.dictionaries WHERE database = 'corpscout' AND name = 'ip_registry_special_trie';
```

Re-running `rdap_network_registry_class` (or the whole job) reclassifies everything from the
current snapshots and reloads `rdap_network_trie`; nothing else needs a restart.
````

- [ ] **Step 3: Update the two existing docs**

Append to `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md`:

```markdown
## Registry-level registrations (data-driven, 2026-09)

Both writers of `rdap_networks` — this bucket worker and `ip_enrichment`'s `RdapEnricher` —
classify every direct registration against the IP registry special segments
(`defs/ip_registry`, `docs/operations/ip-registry-reference-data.md`) with
`commoncrawl_rdap/registry.py::classify_registration` (one `REGISTRY_CONTEXT_SQL` round trip per
RDAP miss) and insert its `rdap_network_registry_class` row between the network row and the
segment rows. A `registry_level` (covers a whole RIR-designated IANA block) or `unallocated`
(first address in available/reserved or IANA-reserved space) registration is stored, answers the
queried address, and is never added to the in-run reuse set; `rdap_network_segments_current`
(migration 000450) excludes such networks from `rdap_network_trie`, and the daily
`rdap_network_registry_class` asset reclassifies everything from the current snapshots. While the
reference data is incomplete the class is `unknown` and nothing is excluded.
```

Append to the "## Migration and validation" section of `services/dagster_v3/docs/ip-enrichment-schema.md`:

```markdown
Migrations `000449` and `000450` add the IP registry special segments (IANA blocks, RIR
available/reserved ranges) and make `rdap_network_trie` serve only registrations classified
`reusable`; `ip_enrichment_results` reports `registry_level_responses` (registrations that answered
only their queried address). See `docs/operations/ip-registry-reference-data.md`.
```

- [ ] **Step 4: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/ip_registry/docs/ip_registry-design.md services/dagster_v3/docs/operations/ip-registry-reference-data.md services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md services/dagster_v3/docs/ip-enrichment-schema.md
git commit -m "docs: IP registry special segments, the registry-level rule and its operations

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Deploy and first load on prod — REQUIRES THE OWNER'S GO-AHEAD BEFORE STEP 1

Order matters: migration 449 → code → first load green → excluded-network report reviewed → migration 450 (exclusion live) → schedule started. Nothing here re-runs the ~170k affected IPs (that remediation belongs to the queue-contract plan).

**Files:** none.

- [ ] **Step 1: Preconditions**

- Tasks 1–5 merged on `main`; `git status --short` shows only unrelated WIP (`searcher/` etc.).
- `ls clickhouse/migrations | tail -2` → `000450_corpscout_rdap_trie_registry_class_exclusion.{down,up}.sql` are the newest (renumber before merging if another workstream took 449/450 — the queue-contract plan then becomes 451/452).
- Prod ledger: `ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT version, dirty FROM corpscout.schema_migrations ORDER BY version DESC LIMIT 4"'` → the highest version with a `dirty=0` row is `448` and no higher version has only a `dirty=1` row.
- No RDAP writer is running: in the Dagster UI the run lists of `ip_enrichment_results_job`, `ip_enrichment_workflow` and the partitioned `commoncrawl_ip_rdap_networks` asset show nothing `STARTED`/`QUEUED`/`STARTING` (the 48.6M run was terminated on 2026-09-25).
- Not inside the Tuesday 01:05 Stockholm address-chain window.

- [ ] **Step 2: Apply migration 449**

Run from `corpscout/`: `make clickhouse-migrate-up-one </dev/null`
Expected: `449/u corpscout_ip_registry_reference_data`. Then:

```bash
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT name, engine FROM system.tables WHERE database = '"'"'corpscout'"'"' AND (name LIKE '"'"'ip_registry%'"'"' OR name LIKE '"'"'rdap_network_registry_class%'"'"') ORDER BY name FORMAT PrettyCompact"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT name, status FROM system.dictionaries WHERE database = '"'"'corpscout'"'"' ORDER BY name FORMAT PrettyCompact"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT ready FROM corpscout.ip_registry_ready"'
```

Expected: 4 tables (`ip_registry_snapshots`, `ip_registry_iana_blocks`, `ip_registry_special_segments`, `rdap_network_registry_class`) + 7 views (`ip_registry_current_snapshots`, `ip_registry_iana_blocks_current`, `ip_registry_special_segments_current`, `ip_registry_special_trie_source`, `ip_registry_ready`, `rdap_network_registry_class_current`, `rdap_network_registry_class_derived`); dictionary `ip_registry_special_trie` (`NOT_LOADED` until first use is fine) next to `rdap_network_trie`; `ready` = `0`.

- [ ] **Step 3: Deploy Dagster by light_sync**

Run: `cd services/dagster_v3/ansible && ANSIBLE_BECOME_TIMEOUT=60 LC_ALL=en_US.UTF-8 ansible-playbook -i inventory.ini light_sync.yml </dev/null > /private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect-corpscout/9f2d193f-045d-4f26-91d7-d2b93320d3f5/scratchpad/ip/light_sync.log 2>&1; echo rc=$?`
Expected: `rc=0`; the code location reloads. In the UI: asset group `ip_registry` (7 assets, 7 checks), job `ip_registry_refresh_job`, schedule `ip_registry_daily` (stopped). `rdap_network_trie` is still the 000258 view: nothing is excluded yet, and every enricher classifies `unknown` until Step 4 completes.

- [ ] **Step 4: First load and classification**

Launch `ip_registry_refresh_job` from the UI with default config. Expected: all 7 assets succeed (~1–3 minutes: 45 MB download, 322k special rows, 15k classifications) and all 7 checks pass. Verify:

```bash
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT source, snapshot_date, records_ipv4, records_ipv6, segments_ipv4, segments_ipv6, checksum FROM corpscout.ip_registry_snapshots FINAL ORDER BY source FORMAT PrettyCompact"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT registry, snapshot_date, count() FROM corpscout.ip_registry_special_segments GROUP BY registry, snapshot_date ORDER BY registry, snapshot_date FORMAT PrettyCompact"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT name, status, element_count, formatReadableSize(bytes_allocated) FROM system.dictionaries WHERE database = '"'"'corpscout'"'"' AND name = '"'"'ip_registry_special_trie'"'"' FORMAT PrettyCompact"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT ready FROM corpscout.ip_registry_ready"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT registry_class, count() FROM corpscout.rdap_network_registry_class_current GROUP BY registry_class FORMAT PrettyCompact"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT dictGetOrDefault('"'"'corpscout.ip_registry_special_trie'"'"', ('"'"'registry'"'"','"'"'status'"'"'), tuple(toIPv4('"'"'103.35.64.49'"'"')), ('"'"''"'"','"'"''"'"')) AS fpt, dictGetOrDefault('"'"'corpscout.ip_registry_special_trie'"'"', ('"'"'registry'"'"','"'"'status'"'"'), tuple(toIPv4('"'"'45.68.105.9'"'"')), ('"'"''"'"','"'"''"'"')) AS lacnic_reserved"'
```

Expected: seven ledger rows (IANA dated by Last-Modified with 256 / 51 rows, RIRs by yesterday's/today's file date with whole-file counts of the order afrinic 15.4k · apnic 175k · arin 170k · lacnic 81k · ripencc 212k and kept segments of the order afrinic 8.2k · apnic 100k · arin 82k · lacnic 47k · ripencc 85k); one partition per registry (≈322k rows in total); the trie `LOADED` with ≈325k elements and ≈48 MiB; `ready` = `1`; classes over the ~15.3k cached networks — the great majority `reusable`, a few dozen `registry_level`/`unallocated`; `fpt` = `('','')`, `lacnic_reserved` = `('lacnic','reserved')` (if LACNIC still lists that range).

- [ ] **Step 5: Review the excluded-network report (before the exclusion goes live)**

```bash
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT c.network_key, c.registry_class, c.covered_rir_blocks, n.name, n.start_address, n.end_address, ifNull(e.served_ips, 0) AS served_ips FROM corpscout.rdap_network_registry_class_current AS c LEFT JOIN corpscout.rdap_networks_current AS n ON n.network_key = c.network_key LEFT JOIN (SELECT rdap_network_key, count() AS served_ips FROM corpscout.ip_enrichment_current WHERE rdap_lookup_status = '"'"'found'"'"' GROUP BY rdap_network_key) AS e ON e.rdap_network_key = c.network_key WHERE c.registry_class != '"'"'reusable'"'"' ORDER BY served_ips DESC FORMAT PrettyCompact"' | tee /private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect-corpscout/9f2d193f-045d-4f26-91d7-d2b93320d3f5/scratchpad/ip/excluded-networks.txt
```

Expected: `apnic:103.0.0.0 - 103.255.255.255` (APNIC-AP, ≈135,677 served), the apnic 101/8, 111/8, 113/8 and afrinic 102/8 blocks, `ripe` EU-ZZ-2A00 (2a00::/11, covers 2 blocks), `arin:NET6-2600-1` (2600::/12) and the LACNIC UNALLOCATED ranges from the review; the served_ips column sums to roughly 170k. Spot-check that no ordinary holder is listed (FPT-VN, DIGITALPACIFIC, Hetzner, Google's `8.8.8.0/24` must be `reusable`: `SELECT network_key, registry_class FROM corpscout.rdap_network_registry_class_current WHERE network_key IN (SELECT network_key FROM corpscout.rdap_networks_current WHERE start_address IN ('8.8.8.0', '103.35.64.0'))`). If a legitimate holder appears, STOP: inspect it in `rdap_network_registry_class_derived`, fix the rule or the data, and do not apply 450. Report the table and the served-IP total to the owner.

- [ ] **Step 6: Apply migration 450 and reload the trie**

Run from `corpscout/`: `make clickhouse-migrate-up-one </dev/null`
Expected: `450/u corpscout_rdap_trie_registry_class_exclusion`. Then:

```bash
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT dictGetOrDefault('"'"'corpscout.rdap_network_trie'"'"', '"'"'network_key'"'"', tuple(toIPv4('"'"'103.15.66.50'"'"')), '"'"''"'"') AS apnic_block, dictGetOrDefault('"'"'corpscout.rdap_network_trie'"'"', '"'"'network_key'"'"', tuple(toIPv4('"'"'8.8.8.8'"'"')), '"'"''"'"') AS google, dictGetOrDefault('"'"'corpscout.rdap_network_trie'"'"', '"'"'network_key'"'"', tuple(toIPv6('"'"'2600:1f00::1'"'"')), '"'"''"'"') AS arin6"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT count() FROM system.dictionaries WHERE database = '"'"'corpscout'"'"' AND name = '"'"'rdap_network_trie'"'"' AND status = '"'"'LOADED'"'"'"'
```

Expected: `apnic_block` = `''` (no longer served), `google` = Google's `8.8.8.0/24` key, `arin6` = `''` (NET6-2600-1 excluded); `1`.

- [ ] **Step 7: Start the schedule and hand over**

Start `ip_registry_daily` on the Schedules page. Record in the hand-over to the owner: the excluded-network report and served-IP total from Step 5 (the input for the remediation draft in the queue-contract plan), the trie memory and per-registry row counts from Step 4, and that the next enrichment runs report `registry_level_responses`.

---

## Decision coverage (owner revisions of 2026-09-25)

| Requirement (second revision, binding) | Task(s) |
| --- | --- |
| Load only special segments: IANA IPv4/IPv6 blocks with designation/status (which RIR, legacy, reserved) | 1 (`parse_iana_csv`, `designation_rir`), 3 (`ip_registry_iana_blocks`) |
| From the five delegated-extended files only `available`/`reserved` lines; never allocated/assigned | 1 (`parse_delegated` keeps `SPECIAL_STATUSES` only), 2 (`ip_registry_special_segments`, contract test asserts no delegation table), 3 |
| Keep validation: MD5, version line, record/summary counts over the whole file, not older than current, shrink guard (decided: whole-file record counts) | 1, 3 (`load_delegated_source`, `refuse_shrink`) |
| Dated load ledger; storage sized for the real counts; retention stated simply (current + previous snapshot, `DROP PARTITION`, no TTL) | 2 (000449), 3 (`drop_superseded_snapshots`), Design decision 1 |
| Rule: registry_level if N covers an entire RIR-designated IANA block; unallocated if the address is in available/reserved or IANA reserved/unassigned; else reusable; Python + SQL twin, parity test | 1 (`registry_class`, `REGISTRY_CLASS_SQL`), 2 (derived view, `test_python_rule_and_derived_view_agree_on_every_case`) |
| Remove the "strictly wider than the delegation" branch, `merge_adjacent`, the delegation `IP_TRIE`; small lookup decided from verified ClickHouse behaviour (one `IP_TRIE` for special segments, plain join for IANA; `RANGE_HASHED` rejected with evidence) | Design decision 3, Evidence section, 2 |
| Test cases: APNIC 103/8, 101/8, 102/8 AFRINIC, 113/8 → registry_level; NET6-2600-1 → registry_level; RIPE 2A00::/11 → registry_level; LACNIC UNALLOCATED → unallocated; FPT → reusable; Cloudflare /12 → reusable; Ford 19/8 → reusable (decided per IANA status LEGACY, non-RIR designation); 8.8.8.8 → reusable | 1 (pure rule cases), 2 (`CASES`, verified on 26.5 in `verify4.sql`) |
| Persisted classification per cached network + trie exclusion migration (inert until first load) | 2 (000449 class table, 000450), 3 |
| RdapEnricher / legacy worker never reuse non-reusable answers | 4 |
| Docs | 5 |
| Deploy with the "print excluded networks and served IPs" review before the exclusion goes live | 6 |
| Daily-or-weekly schedule stopped by default (decided: daily, reason stated) | 3, Design decision 6 |

## Risks and open questions

- **IPv6 special segments are ≈309k rows, not "a few"** (see the sizing finding). The plan handles it (48 MiB trie, ≈650k rows on disk at most), but the owner should confirm this still matches the intent of "no local database" — the alternative of loading IPv4 special segments only would leave IPv6 unallocated space undetected.
- **The removed "wider than the delegation" branch** makes mid-level RIR placeholders reusable (an "ALLOCATED UNSPECIFIED" /13 that contains an available /21, a /14 spanning two holders). Such a registration then serves every address inside it, including unallocated sub-ranges; RDAP would return the same placeholder for those addresses anyway, so the answer is not wrong, only coarse. Reinstating the branch means storing allocated/assigned delegations again (the design the owner rejected).
- **Freshly allocated space** is `unallocated` until the next daily refresh: its registrations are stored but excluded from the trie, so each address in it costs an RDAP call for up to a day. Daily cadence bounds this; a weekly schedule would extend it to a week.
- **`ip_registry_ready` requires all seven sources**: while one RIR file is broken for days, every new classification is `unknown` (behaviour as today, nothing excluded) and the existing persisted classes keep working — the freshness check on that loader is the alarm.
- **Migration numbers** 449/450 are free on main and prod today; the queue-contract plan's numbers shift when this plan merges first — re-check at merge.
- **First-address judgement vs the owner's "x lies in …"**: evaluated at the registration's first address (Design decision 4); the two coincide for the placeholder objects the RIRs return for unallocated space. If a live case surfaces where they differ, the per-miss classifier can pass the queried IP as a second lookup point without a schema change.

## Follow-ups (out of scope here)

- The remediation re-run of the ≈170k addresses served by the now-excluded networks (`force_rdap` draft) — queue-contract plan, Task 9.
- Using `ip_registry_special_trie` to skip RDAP entirely for addresses in `available`/`reserved` space (they can only get a placeholder answer), and RIR-level bulk dumps as a range dictionary (decision D9).
- Loading the IANA special-purpose registries if a consumer other than `classify_ip_scope` ever needs them.
- Storing the `asn` special rows (available/reserved ASNs) if an ASN consumer appears; they are counted today but not kept.
- Merging adjacent same-status special ranges (322k → 88k rows) if row count ever matters; it does not shrink the trie (322k CIDRs), so it was not done.
