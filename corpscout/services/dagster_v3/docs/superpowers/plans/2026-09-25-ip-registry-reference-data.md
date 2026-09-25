# IP Registry Reference Data Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Load the IANA address-space registries and the five RIRs' delegated-extended statistics into ClickHouse as dated snapshots, classify every cached RDAP registration against them (reusable / registry_level / unallocated) with one rule written in SQL and in Python, and use that classification to keep registry-level and unallocated blocks such as `APNIC-AP` (103.0.0.0/8) out of `rdap_network_trie` and out of every enricher's reuse caches.

**Architecture:** A new Dagster module `defs/ip_registry` downloads seven small public files daily (IANA ipv4/ipv6 CSVs, `delegated-{afrinic,apnic,arin,lacnic,ripencc}-extended-latest` + `.md5`), validates them (checksum, version line, per-type record counts, no sharp shrink against the current snapshot) and inserts them as snapshots; a ledger row written last makes a snapshot current. Two `IP_TRIE` dictionaries answer "which IANA block / which delegation holds address x". A view `rdap_network_registry_class_derived` applies the rule to `rdap_networks_current`; the asset `rdap_network_registry_class` persists its output, and migration 000450 makes the trie's source view exclude every network whose class is not `reusable`, so existing poisoned entries stop being served and later reclassifications take effect at the next dictionary reload without code changes. `RdapEnricher` and the legacy bucket worker classify each new registration with the Python twin of the rule (one context query per RDAP miss), write its class row before its segments, and never put a non-reusable registration into their in-run caches.

**Tech Stack:** Python 3.14, Dagster 1.13.9, ClickHouse 26.5 (clickhouse-driver 0.2.10: `IPv6`, `UInt128`, `Array(String)` columns), netaddr (`iprange_to_cidrs`), `dlt.sources.helpers.requests`, pytest against the disposable ClickHouse container of `tests/test_ip_enrichment_input.py::server`.

**Spec:** the owner's revision note `/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect-corpscout/9f2d193f-045d-4f26-91d7-d2b93320d3f5/scratchpad/ip/revision-registry-data.md` (binding) refining decision D6 of `…/scratchpad/ip/decisions.md`; evidence in `…/scratchpad/ip/review-rdap-geoip.md` (173,991 IPs served from /8 blocks, `ripe:2A00::/11`, `arin:NET6-2600-1`, LACNIC `UNALLOCATED`). This plan is standalone: the queue-contract, batching, GeoLite2 and the ~170k-IP remediation re-run stay in `2026-09-25-ip-enrichment-queue-contract.md` (its migrations 449/450 shift to 451/452 when it merges after this plan — re-check at merge).

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
| IANA IPv4 | `https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.csv` | 22,972 B, `text/csv`, header `Prefix,Designation,Date,WHOIS,RDAP,Status [1],Note`, 256 rows, prefix zero-padded (`008/8`), statuses `ALLOCATED` 129 / `LEGACY` 91 / `RESERVED` 35, designations `APNIC`, `RIPE NCC`, `Administered by ARIN`, `IANA - Loopback`, `Ford Motor Company`, one quoted designation (`"PSINet, Inc."`), RDAP column sometimes two URLs glued together (`…registryhttp://…`), footnotes such as `[6]` in Note. HTTP `Last-Modified: Sat, 19 Sep 2026 00:44:20 GMT`. |
| IANA IPv6 | `https://www.iana.org/assignments/ipv6-unicast-address-assignments/ipv6-unicast-address-assignments.csv` | 5,666 B, header `Prefix,Designation,Date,WHOIS,RDAP,Status,Note`, **51 rows** (multi-line quoted notes, so `wc -l` says 59), statuses `ALLOCATED` 36 / `RESERVED` 15, designations `IANA` 15, `RIPE NCC` 14, `APNIC` 9, `ARIN` 7, `AFRINIC` 2, `LACNIC` 2, `6to4`, `Documentation`. |
| RIR delegated-extended | `https://ftp.{afrinic.net/pub/stats/afrinic,apnic.net/stats/apnic,arin.net/pub/stats/arin,lacnic.net/pub/stats/lacnic,ripe.net/pub/stats/ripencc}/delegated-<rir>-extended-latest` (+ `.md5`) | pipe-separated; APNIC starts with a 27-line `#` banner; version line `version|registry|serial|records|startdate|enddate|UTCoffset` — RIPE `2|ripencc|1790287199|260748|19700101|20260924|+0200` (serial = unix seconds), ARIN `2.3|arin|1790341220831|202978|19700101|20260925|-0400`, APNIC `2.3|apnic|20260926|190190||20260925|+1000` (empty startdate, serial a day after enddate), LACNIC `2.3|lacnic|20260924|97298|19870101|20260924|-0300`, AFRINIC `2|afrinic|20260924|19784|00000000|20260924|00000`; `records` = number of record lines of all types = sum of the three `registry|*|type|*|count|summary` lines; records `registry|cc|type|start|value|date|status|opaque-id`, statuses `allocated`, `assigned`, `available`, `reserved`; RIPE writes **7 fields** for available/reserved (`ripencc||ipv4|85.8.248.0|2048||available`), LACNIC 7 fields for available IPv6, the others 8 with an empty id; AFRINIC uses `cc = ZZ` for available/reserved; IPv4 value = address count, often not a power of two (`1536`, `3072`, `768`, `524288`); IPv6 value = prefix length; dates `YYYYMMDD` or `00000000`. Sizes 1.0–18.1 MB; 653,717 ipv4+ipv6 records across the five files → 659,245 CIDRs; no overlapping records inside a file; 8,017 of RIPE's 100,847 IPv4 records continue the previous record of the same holder (same opaque-id and status), 13,472 of ARIN's 80,846. `.md5` formats: BSD `MD5 (delegated-ripencc-extended-latest) = 0ef1edc2…` (RIPE, APNIC, LACNIC, AFRINIC) and GNU `f3c86ced…  delegated-arin-extended-20260925` (ARIN, dated name). |
| Example delegations | in the files | `apnic|VN|ipv4|103.35.64.0|1024|20150813|allocated|A9271131` (FPT /22), `apnic|AU|ipv4|103.0.0.0|65536|…|allocated|A91872ED`, `arin|US|ipv4|104.16.0.0|1048576|…|allocated|…` (/12), `arin|US|ipv4|8.8.8.0|256|…|allocated|…`, `arin|US|ipv4|19.0.0.0|16777216|19880615|allocated|…` (Ford's whole /8 is one delegation), `arin|US|ipv6|2001:4860::|32|…|allocated|…`, `apnic|AU|ipv4|101.0.64.0|16384|…` (the /19 DIGITALPACIFIC answer is more specific than this /18). |

ClickHouse 26.5 facts verified with `clickhouse-local` (scratchpad `ip/verify3.sql`): `toUInt128(toIPv6('::ffff:1.2.3.4')) = 281470698652420` = Python `int(IPv6Address('::ffff:1.2.3.4'))`; `IP_TRIE` dictionaries accept `UInt128` attributes and a tuple `dictGetOrDefault`; a trie holding IPv4 CIDRs answers both `tuple(toIPv4(x))` and `tuple(toIPv6('::ffff:x'))`; `toIPv6(<UInt128>)` converts back; a view may hold a scalar subquery (`(SELECT ready FROM …)`); the exclusion in the trie view must live in a subquery because ClickHouse resolves the `argMax(...) AS network_key` alias inside an outer `WHERE` (`ILLEGAL_AGGREGATION`).

## Design decisions (resolved)

1. **Storage = dated snapshots + ledger.** `ip_registry_iana_blocks` and `ip_registry_delegations` are `ReplacingMergeTree(loaded_at)`, `PARTITION BY toYYYYMM(snapshot_date)`, ordered by `(source|registry, snapshot_date, ip_version, first_ip)`; delegations carry `TTL snapshot_date + INTERVAL 2 YEAR` (≈650k rows/day ≈ 240M rows/year, a few GB compressed; thin older months with `DROP PARTITION` if that ever matters). `ip_registry_snapshots` (source, snapshot_date, verified_at, checksum, serial, records_ipv4, records_ipv6, source_url) is written after the rows, so `_current` views (newest ledger snapshot per source) never expose a partial load. A re-checked identical snapshot only refreshes `verified_at` (the freshness heartbeat; IANA changes rarely).
2. **Bounds in one key space.** Every address bound is an `IPv6` column (IPv4 as `::ffff:a.b.c.d`), compared as `UInt128`; Python uses `address_int()` with the same mapping. Delegations keep the published `start_address`/`value` plus derived `first_ip/last_ip`, `block_first/block_last` (the record merged with adjacent records of the same registry, version, status and opaque-id — `merge_adjacent`) and `cidrs` (netaddr `iprange_to_cidrs`, computed in Python because ClickHouse has no range→CIDR function).
3. **Lookup = two `IP_TRIE` dictionaries** (`ip_registry_iana_trie`, `ip_registry_delegation_trie`) over source views that `ARRAY JOIN cidrs`, read by the existing least-privilege user `corpscout_rdap_dictionary` (migration 000126), `LIFETIME(MIN 3600 MAX 7200)` plus explicit `SYSTEM RELOAD DICTIONARY` after each load. Chosen over `RANGE_HASHED` because the repo already runs and tests `IP_TRIE` (`rdap_network_trie`), the trie mixes both families, and range→CIDR expansion adds only 0.8% rows (659,245 CIDRs for 653,717 records, ~100 MB of dictionary memory).
4. **The rule**, judged by a registration's first address (so the bulk view and the per-miss classifier ask the same question): (a) covers the whole IANA block that holds it and that block is designated to an RIR (`APNIC`, `ARIN`, `RIPE NCC`, `LACNIC`, `AFRINIC`, `Administered by …`) → `registry_level`; (b) that address has no `allocated`/`assigned` delegation (`available`, `reserved` or no record) → `unallocated`; (c) the registration strictly contains the (merged) delegation → `registry_level`; else `reusable`; `unknown` until all seven sources have a current snapshot (then every cache behaves as today). Legacy single-holder /8s (Ford 19/8, designation without an RIR) stay reusable because their delegation equals the registration. Special-purpose IANA registries are not loaded: the enrichers already skip non-global addresses (`classify_ip_scope`).
5. **One SQL definition, one Python twin.** The rule text lives in `commoncrawl_rdap/registry.py` (`REGISTRY_CLASS_SQL`, embedded verbatim in the view `rdap_network_registry_class_derived` — a contract test greps the migration) and `registry_class()`; `tests/test_ip_registry.py` proves both agree on the fixture cases. The classification is **persisted** in `rdap_network_registry_class` (ReplacingMergeTree by `network_key`): the daily asset rewrites it for every cached network from the view, and the enrichers insert a row for each new registration from the Python rule before inserting its segments. The trie view anti-joins that table, so its reader needs no `dictGet` grant and the exclusion is inert until the first classification exists (deploy ordering falls out naturally).
6. **Module** `defs/ip_registry/`: one asset per file (`ip_registry_iana_blocks` loads both IANA CSVs, five `ip_registry_delegations_<rir>` from a factory) so a failing RIR does not block the others, then `rdap_network_registry_class`; job `ip_registry_refresh_job` (= that asset `.upstream()`, checks included); schedule `ip_registry_daily` at `5 6 * * *` UTC (the APNIC file dated D is published on D+1 at +10:00; no other schedule uses 06:05), STOPPED by default; pool `ip_registry`. Validation refuses: MD5 mismatch, missing/foreign version line, `records` ≠ record lines, summary ≠ per-type count, unknown status, unparsable address, empty ipv4+ipv6, a file older than the current snapshot, and a >5% drop in ipv4 or ipv6 records versus the current snapshot unless the run config says `allow_shrink`.
7. **Checks**: one `snapshot_fresh` check per loader (RIR snapshot date ≤ 3 days old and `verified_at` ≤ 2 days old; IANA only `verified_at`) and `classification_complete` on the class asset (ready = 1 and every current network has a class row).

## File Structure

| File | Change | Responsibility |
| --- | --- | --- |
| `services/dagster_v3/src/dagster_v3/defs/ip_registry/__init__.py` | create (empty) | package |
| `services/dagster_v3/src/dagster_v3/defs/ip_registry/tables.py` | create | names, URLs, column tuples |
| `services/dagster_v3/src/dagster_v3/defs/ip_registry/source.py` | create | pure parsers: IANA CSV, delegated-extended, `.md5`, `merge_adjacent`, `address_int` |
| `services/dagster_v3/src/dagster_v3/defs/ip_registry/assets.py` | create | download, validate, load, classify, checks, job, schedule |
| `services/dagster_v3/src/dagster_v3/defs/ip_registry/docs/ip_registry-design.md` | create | design doc (template) |
| `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/registry.py` | create | the rule (Python + SQL text), context query, class row SQL |
| `clickhouse/migrations/000449_corpscout_ip_registry_reference_data.{up,down}.sql` | create | tables, ledger, views, tries, readiness, class table, derived view, grants |
| `clickhouse/migrations/000450_corpscout_rdap_trie_registry_class_exclusion.{up,down}.sql` | create | trie source view excludes non-reusable networks |
| `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py` | modify | classify each new registration, class row before segments, no reuse of non-reusable |
| `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/results.py` | modify | storage assertion, `registry_level_responses` metadata |
| `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/assets.py` | modify | same for the legacy bucket worker |
| `services/dagster_v3/tests/fixtures/ip_registry/*` | create | real excerpts of the seven files |
| `services/dagster_v3/tests/test_ip_registry_source.py` | create | pure tests (parsers, rule, freshness) |
| `services/dagster_v3/tests/test_ip_registry.py` | create | ClickHouse tests: migrations, loaders, dictionaries, parity, exclusion; exports `apply_migration`, `seed_reference_data` |
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
- Produces (`dagster_v3.defs.ip_registry.tables`): `DATABASE`, `SNAPSHOTS_TABLE`, `IANA_TABLE`, `DELEGATIONS_TABLE`, `IANA_TRIE`, `DELEGATION_TRIE`, `READY_VIEW`, `IP_REGISTRY_POOL`, `IANA_SOURCES: dict[str, str]` (`iana_ipv4`, `iana_ipv6`), `RIR_SOURCES: dict[str, str]` (`afrinic`, `apnic`, `arin`, `lacnic`, `ripencc`), `SOURCES`, `SNAPSHOT_COLUMNS`, `IANA_COLUMNS`, `DELEGATION_COLUMNS`.
- Produces (`dagster_v3.defs.ip_registry.source`): `IPV4_MAPPED_OFFSET`, `address_int(value) -> int`, `designation_rir(designation) -> str`, dataclasses `IanaBlock`, `DelegatedHeader`, `Delegation`, `DelegatedFile`, `parse_iana_csv(text, source) -> list[IanaBlock]`, `parse_md5(text) -> str`, `parse_delegated(text, registry) -> DelegatedFile`, `merge_adjacent(records) -> tuple[Delegation, ...]`.
- Produces (`dagster_v3.defs.commoncrawl_rdap.registry`): `IanaCoverage`, `DelegationCoverage`, `RegistryContext`, `RegistryClassification` (`.registry_class`, `.reusable`, `.clickhouse_values(network_key, classified_at)`), `HOLDER_STATUSES`, `REGISTRY_CONTEXT_SQL`, `REGISTRY_CLASS_SQL`, `REGISTRY_CLASS_COLUMNS`, `REGISTRY_CLASS_INSERT_SQL`, `REGISTRY_CLASS_REFRESH_SQL`, `registry_class(first, last, context) -> str`, `fetch_registry_context(client, first_address) -> RegistryContext`, `classify_registration(client, network) -> RegistryClassification`.

- [ ] **Step 1: Write the fixtures**

`tests/fixtures/ip_registry/iana-ipv4-excerpt.csv` (real rows; keep the header's `Status [1]`):

```
Prefix,Designation,Date,WHOIS,RDAP,Status [1],Note
000/8,IANA - Local Identification,1981-09,,,RESERVED,[2][3]
008/8,Administered by ARIN,1992-12,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,LEGACY,
019/8,Ford Motor Company,1995-05,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,LEGACY,
038/8,"PSINet, Inc.",1994-09,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,LEGACY,
100/8,ARIN,2010-11,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,ALLOCATED,[6]
101/8,APNIC,2010-08,whois.apnic.net,https://rdap.apnic.net/,ALLOCATED,
102/8,AFRINIC,2011-02,whois.afrinic.net,https://rdap.afrinic.net/rdap/http://rdap.afrinic.net/rdap/,ALLOCATED,
103/8,APNIC,2011-02,whois.apnic.net,https://rdap.apnic.net/,ALLOCATED,
104/8,ARIN,2011-02,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,ALLOCATED,
240/8,Future use,1981-09,,,RESERVED,[17]
255/8,Future use,1981-09,,,RESERVED,[17][18]
```

`tests/fixtures/ip_registry/iana-ipv6-excerpt.csv` (real rows; the `2600::/12` and `2a00::/12` notes span lines inside their quotes exactly as published):

```
Prefix,Designation,Date,WHOIS,RDAP,Status,Note
2001::/23,IANA,1999-07-01,whois.iana.org,,ALLOCATED,This range has been partially allocated. See [IPv6 Special-Purpose Address Space] for details.
2001:200::/23,APNIC,1999-07-01,whois.apnic.net,https://rdap.apnic.net/,ALLOCATED,
2001:2000::/19,RIPE NCC,2019-03-12,whois.ripe.net,https://rdap.db.ripe.net/,ALLOCATED,"2001:2000::/20, 2001:3000::/21, and 2001:3800::/22 were allocated on 2004-05-04. The more recent allocation (2019-03-12) incorporates all these previous allocations."
2001:4800::/23,ARIN,2004-08-24,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,ALLOCATED,
2600::/12,ARIN,2006-10-03,whois.arin.net,https://rdap.arin.net/registryhttp://rdap.arin.net/registry,ALLOCATED,"2600::/22, 2604::/22, 2608::/22 and 260c::/22 were allocated on 2005-04-19. The more
recent allocation (2006-10-03) incorporates all these previous allocations."
2a00::/12,RIPE NCC,2006-10-03,whois.ripe.net,https://rdap.db.ripe.net/,ALLOCATED,"2a00::/21 was originally allocated on 2005-04-19. 2a01::/23 was allocated on 2005-07-14.
2a01::/16 (incorporating the 2a01::/23) was allocated on 2005-12-15. The more recent allocation
(2006-10-03) incorporates these previous allocations."
2d00::/8,IANA,1999-07-01,,,RESERVED,
```

`tests/fixtures/ip_registry/delegated-ripencc-extended-excerpt` (real records; header and summary counts rewritten to the excerpt: 11 records = 8 ipv4 + 3 ipv6; the two `2.0.0.0`/`2.2.0.0` records are one holder's adjacent allocations, `2.3.0.0` is another holder's):

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

`tests/fixtures/ip_registry/delegated-apnic-extended-excerpt` (APNIC's banner, empty startdate, 8 records = 6 ipv4 + 2 ipv6):

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

`tests/fixtures/ip_registry/delegated-arin-extended-excerpt` (9 records = 2 asn + 4 ipv4 + 3 ipv6; asn date `00000000`):

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

`tests/fixtures/ip_registry/delegated-lacnic-extended-excerpt` (7 records = 3 ipv4 + 4 ipv6; seven-field available IPv6 lines):

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

`tests/fixtures/ip_registry/delegated-afrinic-extended-excerpt` (8 records = 1 asn + 5 ipv4 + 2 ipv6; `ZZ` on available/reserved):

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

- [ ] **Step 2: Write the failing pure tests**

Create `services/dagster_v3/tests/test_ip_registry_source.py`:

```python
"""Parsers for the IANA CSVs and RIR delegated-extended files, the registry rule, freshness — no I/O."""

import hashlib
from datetime import UTC, date, datetime, timedelta
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
    assert len(blocks) == 11
    by_prefix = {block.prefix: block for block in blocks}
    assert set(by_prefix) == {
        "0.0.0.0/8", "8.0.0.0/8", "19.0.0.0/8", "38.0.0.0/8", "100.0.0.0/8", "101.0.0.0/8",
        "102.0.0.0/8", "103.0.0.0/8", "104.0.0.0/8", "240.0.0.0/8", "255.0.0.0/8",
    }
    apnic = by_prefix["103.0.0.0/8"]
    assert (apnic.ip_version, apnic.designation, apnic.rir, apnic.status, apnic.assigned_on) == (
        4, "APNIC", "apnic", "ALLOCATED", "2011-02"
    )
    assert (apnic.first, apnic.last) == (source.address_int("103.0.0.0"), source.address_int("103.255.255.255"))
    assert by_prefix["38.0.0.0/8"].designation == "PSINet, Inc."
    assert by_prefix["8.0.0.0/8"].rir == "arin" and by_prefix["8.0.0.0/8"].status == "LEGACY"
    assert by_prefix["19.0.0.0/8"].rir == ""
    assert by_prefix["100.0.0.0/8"].note == "[6]"
    assert by_prefix["8.0.0.0/8"].rdap == "https://rdap.arin.net/registryhttp://rdap.arin.net/registry"


def test_parse_iana_ipv6_excerpt_handles_multiline_notes():
    blocks = source.parse_iana_csv(excerpt("iana-ipv6-excerpt.csv"), "iana_ipv6")
    assert [block.prefix for block in blocks] == [
        "2001::/23", "2001:200::/23", "2001:2000::/19", "2001:4800::/23", "2600::/12", "2a00::/12", "2d00::/8",
    ]
    arin = blocks[4]
    assert (arin.rir, arin.status, arin.ip_version) == ("arin", "ALLOCATED", 6)
    assert arin.note.startswith("2600::/22, 2604::/22") and "recent allocation (2006-10-03)" in arin.note
    assert (arin.first, arin.last) == (
        source.address_int("2600::"), source.address_int("260f:ffff:ffff:ffff:ffff:ffff:ffff:ffff")
    )
    assert blocks[0].rir == "" and blocks[0].designation == "IANA"
    assert blocks[-1].status == "RESERVED"


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


def test_parse_delegated_ripencc_excerpt():
    parsed = source.parse_delegated(excerpt("delegated-ripencc-extended-excerpt"), "ripencc")
    header = parsed.header
    assert (header.version, header.registry, header.serial, header.records) == ("2", "ripencc", "1790287199", 11)
    assert (header.start_date, header.end_date, header.utc_offset) == ("19700101", date(2026, 9, 24), "+0200")
    assert parsed.summaries == {"ipv4": 8, "asn": 0, "ipv6": 3}
    assert parsed.record_lines == 11 and len(parsed.records) == 11
    by_start = {record.start_address: record for record in parsed.records}
    dk = by_start["195.85.96.0"]
    assert (dk.cc, dk.value, dk.status, dk.delegated_on, dk.opaque_id) == (
        "DK", 1536, "allocated", date(1997, 2, 6), "654e8153-9457-446b-bbe9-2db02b7ba0a8"
    )
    assert dk.cidrs == ("195.85.96.0/22", "195.85.100.0/23")
    assert (dk.first, dk.last) == (source.address_int("195.85.96.0"), source.address_int("195.85.101.255"))
    available = by_start["85.8.248.0"]  # seven-field line
    assert (available.cc, available.status, available.opaque_id, available.delegated_on) == ("", "available", "", None)
    nl = by_start["2001:600::"]
    assert (nl.ip_version, nl.value, nl.cidrs) == (6, 29, ("2001:600::/29",))
    assert nl.last == source.address_int("2001:607:ffff:ffff:ffff:ffff:ffff:ffff")


def test_parse_delegated_other_registries():
    apnic = source.parse_delegated(excerpt("delegated-apnic-extended-excerpt"), "apnic")
    assert apnic.header.start_date == "" and apnic.header.end_date == date(2026, 9, 25)
    assert {r.start_address for r in apnic.records if r.status == "available"} == {"14.102.240.0"}
    arin = source.parse_delegated(excerpt("delegated-arin-extended-excerpt"), "arin")
    assert arin.header.serial == "1790341220831" and arin.record_lines == 9 and len(arin.records) == 7
    ford = next(r for r in arin.records if r.start_address == "19.0.0.0")
    assert ford.cidrs == ("19.0.0.0/8",) and ford.delegated_on == date(1988, 6, 15)
    lacnic = source.parse_delegated(excerpt("delegated-lacnic-extended-excerpt"), "lacnic")
    assert [r.status for r in lacnic.records if r.ip_version == 6] == ["allocated", "assigned", "available", "available"]
    afrinic = source.parse_delegated(excerpt("delegated-afrinic-extended-excerpt"), "afrinic")
    assert afrinic.header.start_date == "00000000"
    big = next(r for r in afrinic.records if r.start_address == "102.192.0.0")
    assert (big.cc, big.status, big.cidrs) == ("ZZ", "available", ("102.192.0.0/13",))


@pytest.mark.parametrize(
    ("registry_name", "mutate", "message"),
    [
        ("ripencc", lambda t: t.replace("|11|", "|12|", 1), "announces 12 records"),
        ("ripencc", lambda t: t.replace("ipv4|*|8|summary", "ipv4|*|7|summary"), "ipv4 summary 7"),
        ("apnic", lambda t: t, "belongs to 'ripencc'"),
        ("ripencc", lambda t: t.replace("|allocated|172ce676", "|pending|172ce676"), "status 'pending'"),
        ("ripencc", lambda t: t.replace("2|ripencc|", "ripencc|", 1), "not a version line"),
        ("ripencc", lambda t: t.replace("|1.178.112.0|", "|1.178.112|"), "1.178.112"),
    ],
)
def test_parse_delegated_refuses_inconsistent_files(registry_name, mutate, message):
    # The RIPE excerpt parsed as another registry must be refused as a foreign file.
    text = mutate(excerpt("delegated-ripencc-extended-excerpt"))
    with pytest.raises(ValueError, match=message):
        source.parse_delegated(text, registry_name)


def test_merge_adjacent_unites_one_holders_consecutive_records_only():
    parsed = source.parse_delegated(excerpt("delegated-ripencc-extended-excerpt"), "ripencc")
    by_start = {record.start_address: record for record in parsed.records}
    first, second, other = by_start["2.0.0.0"], by_start["2.2.0.0"], by_start["2.3.0.0"]
    assert (first.block_first, first.block_last) == (source.address_int("2.0.0.0"), source.address_int("2.2.255.255"))
    assert (second.block_first, second.block_last) == (first.block_first, first.block_last)
    assert (first.first, first.last) == (source.address_int("2.0.0.0"), source.address_int("2.1.255.255"))
    # Adjacent to 2.2.x.x but another holder: keeps its own bounds.
    assert (other.block_first, other.block_last) == (other.first, other.last)
    # Adjacent records without an opaque id (available/reserved) are never merged.
    empty = [r for r in parsed.records if not r.opaque_id]
    assert all((r.block_first, r.block_last) == (r.first, r.last) for r in empty)


def context(*, ready=True, iana=None, delegation=None):
    return registry.RegistryContext(ready=ready, iana=iana, delegation=delegation)


IANA_103 = registry.IanaCoverage("APNIC", "apnic", "ALLOCATED", source.address_int("103.0.0.0"), source.address_int("103.255.255.255"))
IANA_19 = registry.IanaCoverage("Ford Motor Company", "", "LEGACY", source.address_int("19.0.0.0"), source.address_int("19.255.255.255"))
IANA_2600 = registry.IanaCoverage("ARIN", "arin", "ALLOCATED", source.address_int("2600::"), source.address_int("260f:ffff:ffff:ffff:ffff:ffff:ffff:ffff"))
D_103_0_16 = registry.DelegationCoverage("apnic", "AU", "allocated", source.address_int("103.0.0.0"), source.address_int("103.0.255.255"))
D_FPT = registry.DelegationCoverage("apnic", "VN", "allocated", source.address_int("103.35.64.0"), source.address_int("103.35.67.255"))
D_2600_48 = registry.DelegationCoverage("arin", "US", "allocated", source.address_int("2600::"), source.address_int("2600:0:0:ffff:ffff:ffff:ffff:ffff"))
D_104_12 = registry.DelegationCoverage("arin", "US", "allocated", source.address_int("104.16.0.0"), source.address_int("104.31.255.255"))
D_FORD = registry.DelegationCoverage("arin", "US", "allocated", source.address_int("19.0.0.0"), source.address_int("19.255.255.255"))
D_RESERVED = registry.DelegationCoverage("lacnic", "", "reserved", source.address_int("45.68.105.0"), source.address_int("45.68.105.255"))


@pytest.mark.parametrize(
    ("label", "first", "last", "ctx", "expected"),
    [
        ("APNIC-AP 103/8 over a /16 delegation", "103.0.0.0", "103.255.255.255", context(iana=IANA_103, delegation=D_103_0_16), "registry_level"),
        ("FPT /22 equals its delegation", "103.35.64.0", "103.35.67.255", context(iana=IANA_103, delegation=D_FPT), "reusable"),
        ("more specific than the delegation", "103.35.64.0", "103.35.64.255", context(iana=IANA_103, delegation=D_FPT), "reusable"),
        ("NET6-2600-1 /12 equals the IANA block", "2600::", "260f:ffff:ffff:ffff:ffff:ffff:ffff:ffff", context(iana=IANA_2600, delegation=D_2600_48), "registry_level"),
        ("NET-104-16-0-0-1 /12 equals its delegation", "104.16.0.0", "104.31.255.255", context(iana=registry.IanaCoverage("ARIN", "arin", "ALLOCATED", source.address_int("104.0.0.0"), source.address_int("104.255.255.255")), delegation=D_104_12), "reusable"),
        ("reserved range", "45.68.105.0", "45.68.105.255", context(iana=registry.IanaCoverage("LACNIC", "lacnic", "ALLOCATED", source.address_int("45.0.0.0"), source.address_int("45.255.255.255")), delegation=D_RESERVED), "unallocated"),
        ("no delegation at all", "45.68.105.0", "45.68.105.255", context(iana=None, delegation=None), "unallocated"),
        ("Ford's whole legacy /8 equals its delegation", "19.0.0.0", "19.255.255.255", context(iana=IANA_19, delegation=D_FORD), "reusable"),
        ("wider than a /8 without any marker", "100.0.0.0", "103.255.255.255", context(iana=registry.IanaCoverage("ARIN", "arin", "ALLOCATED", source.address_int("100.0.0.0"), source.address_int("100.255.255.255")), delegation=D_103_0_16), "registry_level"),
        ("partial overlap is not wider", "103.35.66.0", "103.35.69.255", context(iana=IANA_103, delegation=D_FPT), "reusable"),
        ("reference data not loaded", "103.0.0.0", "103.255.255.255", context(ready=False, iana=IANA_103, delegation=D_103_0_16), "unknown"),
    ],
)
def test_registry_class_rule(label, first, last, ctx, expected):
    assert registry.registry_class(source.address_int(first), source.address_int(last), ctx) == expected, label


def test_registry_class_sql_mirrors_the_python_rule_text():
    for fragment in (
        "NOT ready, 'unknown'",
        "iana.2 != '' AND toUInt128(first_ip) <= iana.4 AND toUInt128(last_ip) >= iana.5, 'registry_level'",
        "delegation.3 NOT IN ('allocated', 'assigned'), 'unallocated'",
        "toUInt128(first_ip) <= delegation.4 AND toUInt128(last_ip) >= delegation.5 AND (toUInt128(first_ip) < delegation.4 OR toUInt128(last_ip) > delegation.5), 'registry_level'",
        "'reusable') AS registry_class",
    ):
        assert fragment in registry.REGISTRY_CLASS_SQL
    assert registry.HOLDER_STATUSES == ("allocated", "assigned")
    assert registry.REGISTRY_CLASS_INSERT_SQL.startswith("INSERT INTO corpscout.rdap_network_registry_class (network_key, registry_class,")
    assert "WHERE registry_class != 'unknown'" in registry.REGISTRY_CLASS_REFRESH_SQL


def test_fixture_urls_are_the_published_ones():
    assert tables.IANA_SOURCES["iana_ipv4"] == "https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.csv"
    assert tables.RIR_SOURCES["ripencc"] == "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest"
    assert tables.SOURCES == ("iana_ipv4", "iana_ipv6", "afrinic", "apnic", "arin", "lacnic", "ripencc")
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
DELEGATIONS_TABLE = "ip_registry_delegations"
IANA_TRIE = "ip_registry_iana_trie"
DELEGATION_TRIE = "ip_registry_delegation_trie"
READY_VIEW = "ip_registry_ready"
IP_REGISTRY_POOL = "ip_registry"

IANA_SOURCES = {
    "iana_ipv4": "https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.csv",
    "iana_ipv6": "https://www.iana.org/assignments/ipv6-unicast-address-assignments/ipv6-unicast-address-assignments.csv",
}
# Each RIR publishes the file daily next to a .md5 (BSD "MD5 (name) = hex" or, for ARIN,
# GNU "hex  name").
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
DELEGATION_COLUMNS = (
    "registry",
    "snapshot_date",
    "ip_version",
    "cc",
    "status",
    "start_address",
    "value",
    "first_ip",
    "last_ip",
    "block_first",
    "block_last",
    "cidrs",
    "delegated_on",
    "opaque_id",
    "loaded_at",
)
```

- [ ] **Step 5: Create `source.py`**

Create `services/dagster_v3/src/dagster_v3/defs/ip_registry/source.py`:

```python
"""Parsers for the IANA address-space CSVs and the RIR delegated-extended files (no I/O).

Formats verified on 2026-09-25 against the published files; see the fixtures in
tests/fixtures/ip_registry for real excerpts.
"""

import csv
import io
import re
from dataclasses import dataclass, replace
from datetime import date
from ipaddress import IPv4Address, IPv6Address, ip_address, ip_network

from netaddr import iprange_to_cidrs

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
class Delegation:
    registry: str
    cc: str
    ip_version: int
    start_address: str
    value: int
    first: int
    last: int
    block_first: int
    block_last: int
    cidrs: tuple[str, ...]
    delegated_on: date | None
    status: str
    opaque_id: str


@dataclass(frozen=True)
class DelegatedFile:
    header: DelegatedHeader
    summaries: dict[str, int]
    records: tuple[Delegation, ...]
    record_lines: int


def _delegation_date(value: str) -> date | None:
    if value in ("", "00000000"):
        return None
    return date(int(value[:4]), int(value[4:6]), int(value[6:8]))


def parse_delegated(text: str, registry: str) -> DelegatedFile:
    """A delegated-<registry>-extended file.

    Lines: '#' comments (APNIC's banner), one version line
    'version|registry|serial|records|startdate|enddate|UTCoffset', summary lines
    'registry|*|type|*|count|summary' and records
    'registry|cc|type|start|value|date|status|opaque-id' — seven fields for RIPE NCC's and
    LACNIC's available/reserved records. IPv4 value is an address count (not always a power
    of two), IPv6 value a prefix length. 'records' must equal the number of record lines and
    each summary its type's count; asn records are counted but not returned.
    """
    header: DelegatedHeader | None = None
    summaries: dict[str, int] = {}
    records: list[Delegation] = []
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
        _, cc, kind, start, value, delegated, status = fields[:7]
        opaque_id = fields[7] if len(fields) > 7 else ""
        if status not in DELEGATION_STATUSES:
            raise ValueError(f"{registry}: unknown status {status!r} on line {line_number}")
        if kind == "asn":
            continue
        if kind == "ipv4":
            first = address_int(IPv4Address(start))
            last = first + int(value) - 1
            cidrs = tuple(
                str(cidr)
                for cidr in iprange_to_cidrs(start, str(IPv4Address(last - IPV4_MAPPED_OFFSET)))
            )
        elif kind == "ipv6":
            network = ip_network(f"{start}/{value}")
            first, last = address_int(network[0]), address_int(network[-1])
            cidrs = (str(network),)
        else:
            raise ValueError(f"{registry}: unknown type {kind!r} on line {line_number}")
        records.append(
            Delegation(
                registry=registry,
                cc=cc,
                ip_version=4 if kind == "ipv4" else 6,
                start_address=start,
                value=int(value),
                first=first,
                last=last,
                block_first=first,
                block_last=last,
                cidrs=cidrs,
                delegated_on=_delegation_date(delegated),
                status=status,
                opaque_id=opaque_id,
            )
        )
    if header is None:
        raise ValueError(f"{registry}: no version line")
    if record_lines != header.records:
        raise ValueError(
            f"{registry}: header announces {header.records} records, file has {record_lines}"
        )
    for kind, version in (("ipv4", 4), ("ipv6", 6)):
        parsed = sum(1 for record in records if record.ip_version == version)
        if summaries.get(kind, 0) != parsed:
            raise ValueError(f"{registry}: {kind} summary {summaries.get(kind, 0)} != {parsed} records")
    return DelegatedFile(
        header=header, summaries=summaries, records=merge_adjacent(records), record_lines=record_lines
    )


def merge_adjacent(records: list[Delegation]) -> tuple[Delegation, ...]:
    """Give one holder's consecutive records (same version, status, non-empty opaque-id) the bounds of their union.

    RIRs split a holder's contiguous space into several records (8,017 of RIPE NCC's 100,847
    IPv4 records continue the previous record of the same holder). An RDAP registration that
    covers the union must count as equal to its delegation, not wider than it. Records keep
    their own start, value and CIDRs; only block_first/block_last change.
    """
    merged = list(records)
    order = sorted(range(len(merged)), key=lambda index: (merged[index].ip_version, merged[index].first))
    group: list[int] = []

    def close() -> None:
        if len(group) > 1:
            first, last = merged[group[0]].first, merged[group[-1]].last
            for index in group:
                merged[index] = replace(merged[index], block_first=first, block_last=last)
        group.clear()

    for index in order:
        record = merged[index]
        if group:
            previous = merged[group[-1]]
            if (
                record.opaque_id
                and record.opaque_id == previous.opaque_id
                and record.ip_version == previous.ip_version
                and record.status == previous.status
                and record.first == previous.last + 1
            ):
                group.append(index)
                continue
            close()
        group.append(index)
    close()
    return tuple(merged)
```

- [ ] **Step 6: Create `registry.py`**

Create `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/registry.py`:

```python
"""Data-driven registry-level classification of RDAP registrations.

An RDAP answer is reusable coverage only when it is a holder's registration: not the whole
IANA block designated to an RIR (safety net), not space the RIR lists as available/reserved
or not at all (unallocated), and not strictly wider than the delegation holding its first
address (registry level). The rule is written twice on purpose: registry_class() for the
enrichers and REGISTRY_CLASS_SQL for the view rdap_network_registry_class_derived that
migration 000449 embeds verbatim; tests/test_ip_registry.py proves they agree.
"""

from dataclasses import dataclass
from datetime import datetime
from ipaddress import IPv6Address

from dagster_v3.defs.commoncrawl_rdap.rdap import RdapNetwork
from dagster_v3.defs.ip_registry.source import address_int

HOLDER_STATUSES = ("allocated", "assigned")
REGISTRY_CLASSES = ("reusable", "registry_level", "unallocated", "unknown")


@dataclass(frozen=True)
class IanaCoverage:
    designation: str
    rir: str
    status: str
    first: int
    last: int


@dataclass(frozen=True)
class DelegationCoverage:
    registry: str
    cc: str
    status: str
    first: int
    last: int


@dataclass(frozen=True)
class RegistryContext:
    ready: bool
    iana: IanaCoverage | None
    delegation: DelegationCoverage | None


# One round trip per RDAP miss: readiness, the IANA block and the delegation holding an address.
REGISTRY_CONTEXT_SQL = """SELECT (SELECT ready FROM corpscout.ip_registry_ready) AS ready,
    dictGetOrDefault('corpscout.ip_registry_iana_trie', ('designation', 'rir', 'status', 'block_first', 'block_last'), tuple(toIPv6(%(ip)s)), ('', '', '', toUInt128(0), toUInt128(0))) AS iana,
    dictGetOrDefault('corpscout.ip_registry_delegation_trie', ('registry', 'cc', 'status', 'block_first', 'block_last'), tuple(toIPv6(%(ip)s)), ('', '', '', toUInt128(0), toUInt128(0))) AS delegation"""

# The SQL twin of registry_class() over the aliases the derived view defines: ready, iana
# (designation, rir, status, first, last), delegation (registry, cc, status, first, last),
# first_ip, last_ip. Migration 000449 embeds this text verbatim.
REGISTRY_CLASS_SQL = """multiIf(
        NOT ready, 'unknown',
        iana.2 != '' AND toUInt128(first_ip) <= iana.4 AND toUInt128(last_ip) >= iana.5, 'registry_level',
        delegation.3 NOT IN ('allocated', 'assigned'), 'unallocated',
        toUInt128(first_ip) <= delegation.4 AND toUInt128(last_ip) >= delegation.5 AND (toUInt128(first_ip) < delegation.4 OR toUInt128(last_ip) > delegation.5), 'registry_level',
        'reusable') AS registry_class"""

REGISTRY_CLASS_COLUMNS = (
    "network_key",
    "registry_class",
    "network_first",
    "network_last",
    "delegation_registry",
    "delegation_status",
    "delegation_first",
    "delegation_last",
    "iana_designation",
    "iana_rir",
    "iana_status",
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


def registry_class(first: int, last: int, context: RegistryContext) -> str:
    """'reusable', 'registry_level', 'unallocated', or 'unknown' while reference data is incomplete."""
    if not context.ready:
        return "unknown"
    iana = context.iana
    if iana is not None and iana.rir and first <= iana.first and last >= iana.last:
        return "registry_level"
    delegation = context.delegation
    if delegation is None or delegation.status not in HOLDER_STATUSES:
        return "unallocated"
    if (
        first <= delegation.first
        and last >= delegation.last
        and (first < delegation.first or last > delegation.last)
    ):
        return "registry_level"
    return "reusable"


def fetch_registry_context(client, first_address: str) -> RegistryContext:
    """The context of a registration's first address; an empty answer means not ready."""
    rows = client.execute(
        REGISTRY_CONTEXT_SQL, {"ip": str(IPv6Address(address_int(first_address)))}
    )
    if not rows:
        return RegistryContext(ready=False, iana=None, delegation=None)
    ready, iana, delegation = rows[0]
    return RegistryContext(
        ready=bool(ready),
        iana=(
            IanaCoverage(iana[0], iana[1], iana[2], int(iana[3]), int(iana[4]))
            if iana[0]
            else None
        ),
        delegation=(
            DelegationCoverage(
                delegation[0], delegation[1], delegation[2], int(delegation[3]), int(delegation[4])
            )
            if delegation[0]
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
        delegation, iana = self.context.delegation, self.context.iana
        return (
            network_key,
            self.registry_class,
            IPv6Address(self.first),
            IPv6Address(self.last),
            delegation.registry if delegation else "",
            delegation.status if delegation else "",
            IPv6Address(delegation.first if delegation else 0),
            IPv6Address(delegation.last if delegation else 0),
            iana.designation if iana else "",
            iana.rir if iana else "",
            iana.status if iana else "",
            classified_at,
        )


def classify_registration(client, network: RdapNetwork) -> RegistryClassification:
    """Classify a normalized registration by its first address (the daily asset does the same in SQL)."""
    first, last = address_int(network.start_address), address_int(network.end_address)
    context = fetch_registry_context(client, network.start_address)
    return RegistryClassification(
        registry_class=registry_class(first, last, context), context=context, first=first, last=last
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
git commit -m "feat(dagster): parsers for IANA address space and RIR delegated stats, and the data-driven registry-level rule

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Migrations 000449 (reference tables, tries, readiness, class table) and 000450 (trie exclusion) with their ClickHouse tests

000450 is written here with 000449 because the test fixture applies both; its exclusion is inert until `rdap_network_registry_class` holds rows, which on prod happens only after the first reference load (Task 6 applies it separately, after the load is reviewed).

**Files:**
- Create: `clickhouse/migrations/000449_corpscout_ip_registry_reference_data.up.sql`, `…down.sql`
- Create: `clickhouse/migrations/000450_corpscout_rdap_trie_registry_class_exclusion.up.sql`, `…down.sql`
- Modify: `services/dagster_v3/tests/test_clickhouse_migrations.py:463` (`EXPECTED_MIGRATIONS`, after `"000448_corpscout_crawl_queue_contract",`)
- Test: `services/dagster_v3/tests/test_ip_registry.py` (new; Task 3 appends to it)

**Interfaces:**
- Consumes: `REGISTRY_CLASS_SQL`, `classify_registration`, `REGISTRY_CLASS_INSERT_SQL`, `REGISTRY_CLASS_REFRESH_SQL` (Task 1), `tests.test_ip_enrichment_input.server`.
- Produces (ClickHouse, 000449): tables `corpscout.ip_registry_snapshots`, `ip_registry_iana_blocks`, `ip_registry_delegations`, `rdap_network_registry_class`; views `ip_registry_current_snapshots`, `ip_registry_iana_blocks_current`, `ip_registry_delegations_current`, `ip_registry_iana_trie_source`, `ip_registry_delegation_trie_source`, `ip_registry_ready` (one row, `ready UInt8`), `rdap_network_registry_class_current`, `rdap_network_registry_class_derived`; dictionaries `ip_registry_iana_trie`, `ip_registry_delegation_trie` (`IP_TRIE`, attributes as in Task 1's `REGISTRY_CONTEXT_SQL`).
- Produces (ClickHouse, 000450): `corpscout.rdap_network_segments_current` rewritten to exclude networks whose current class is not `reusable`; `rdap_network_trie` recreated unchanged in name, columns and `USER 'corpscout_rdap_dictionary'` source (`_assert_rdap_storage_exists` in `commoncrawl_rdap/assets.py:814-824` checks that string).
- Produces (`tests/test_ip_registry.py`): `MIGRATIONS`, `FIXTURES`, `apply_migration(client, name, *, before=None)`, fixtures `registry_server` (module) and `clean` (function), `seed_reference_data(client)`, `reload_tries(client)`, `network_response(rir, handle, start, end, name)`, `CASES`.

- [ ] **Step 1: Write the migration**

`clickhouse/migrations/000449_corpscout_ip_registry_reference_data.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- IP registry reference data: the IANA top-level address blocks and the five RIRs' daily
-- delegated-extended statistics, kept as dated snapshots, plus the classification of every
-- cached RDAP registration against them. Loaded by the ip_registry Dagster module.

-- One row per (source, snapshot) that finished loading. The _current views read the newest
-- snapshot per source from here, so a partial load is never current. verified_at moves on
-- every run that re-checks the same snapshot (IANA changes rarely, freshness needs a heartbeat).
CREATE TABLE IF NOT EXISTS corpscout.ip_registry_snapshots
(
    source          LowCardinality(String),
    snapshot_date   Date,
    verified_at     DateTime64(3, 'UTC'),
    checksum        String,
    serial          String,
    records_ipv4    UInt64,
    records_ipv6    UInt64,
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
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (source, snapshot_date, ip_version, first_ip);

-- RIR delegated-extended ipv4/ipv6 records as published (start_address, value, status, opaque_id)
-- plus derived bounds: first_ip/last_ip of the record, block_first/block_last of the record
-- merged with adjacent records of the same holder and status, and the CIDR cover of the record
-- (IPv4 counts are not always powers of two). Daily snapshots, one partition per month,
-- kept for two years.
CREATE TABLE IF NOT EXISTS corpscout.ip_registry_delegations
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
    block_first     IPv6,
    block_last      IPv6,
    cidrs           Array(String),
    delegated_on    Nullable(Date),
    opaque_id       String,
    loaded_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (registry, snapshot_date, ip_version, first_ip)
TTL snapshot_date + INTERVAL 2 YEAR;

CREATE VIEW IF NOT EXISTS corpscout.ip_registry_iana_blocks_current AS
SELECT *
FROM corpscout.ip_registry_iana_blocks FINAL
WHERE (source, snapshot_date) IN (SELECT source, snapshot_date FROM corpscout.ip_registry_current_snapshots);

CREATE VIEW IF NOT EXISTS corpscout.ip_registry_delegations_current AS
SELECT *
FROM corpscout.ip_registry_delegations FINAL
WHERE (registry, snapshot_date) IN (SELECT source, snapshot_date FROM corpscout.ip_registry_current_snapshots);

-- Dictionary sources: one row per CIDR with the bounds as UInt128 (IP_TRIE attributes).
CREATE VIEW IF NOT EXISTS corpscout.ip_registry_iana_trie_source AS
SELECT
    prefix AS cidr,
    argMax(designation, snapshot_date) AS designation,
    argMax(rir, snapshot_date) AS rir,
    argMax(status, snapshot_date) AS status,
    argMax(toUInt128(first_ip), snapshot_date) AS block_first,
    argMax(toUInt128(last_ip), snapshot_date) AS block_last
FROM corpscout.ip_registry_iana_blocks_current
GROUP BY prefix;

CREATE VIEW IF NOT EXISTS corpscout.ip_registry_delegation_trie_source AS
SELECT
    cidr,
    argMax(registry, snapshot_date) AS registry,
    argMax(cc, snapshot_date) AS cc,
    argMax(status, snapshot_date) AS status,
    argMax(toUInt128(block_first), snapshot_date) AS block_first,
    argMax(toUInt128(block_last), snapshot_date) AS block_last
FROM corpscout.ip_registry_delegations_current
ARRAY JOIN cidrs AS cidr
GROUP BY cidr;

-- The dictionaries read as the least-privilege local user from migration 000126.
GRANT SELECT ON corpscout.ip_registry_snapshots TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_current_snapshots TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_iana_blocks TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_iana_blocks_current TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_iana_trie_source TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_delegations TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_delegations_current TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_delegation_trie_source TO corpscout_rdap_dictionary;

-- Longest-prefix lookups: the IANA block and the delegation holding an address.
CREATE DICTIONARY IF NOT EXISTS corpscout.ip_registry_iana_trie
(
    cidr          String,
    designation   String,
    rir           String,
    status        String,
    block_first   UInt128,
    block_last    UInt128
)
PRIMARY KEY cidr
SOURCE(
    CLICKHOUSE(
        USER 'corpscout_rdap_dictionary'
        DB 'corpscout'
        TABLE 'ip_registry_iana_trie_source'
    )
)
LAYOUT(IP_TRIE())
LIFETIME(MIN 3600 MAX 7200);

CREATE DICTIONARY IF NOT EXISTS corpscout.ip_registry_delegation_trie
(
    cidr          String,
    registry      String,
    cc            String,
    status        String,
    block_first   UInt128,
    block_last    UInt128
)
PRIMARY KEY cidr
SOURCE(
    CLICKHOUSE(
        USER 'corpscout_rdap_dictionary'
        DB 'corpscout'
        TABLE 'ip_registry_delegation_trie_source'
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
    network_key           String,
    registry_class        LowCardinality(String),
    network_first         IPv6,
    network_last          IPv6,
    delegation_registry   LowCardinality(String),
    delegation_status     LowCardinality(String),
    delegation_first      IPv6,
    delegation_last       IPv6,
    iana_designation      String,
    iana_rir              LowCardinality(String),
    iana_status           LowCardinality(String),
    classified_at         DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(classified_at)
ORDER BY network_key;

CREATE VIEW IF NOT EXISTS corpscout.rdap_network_registry_class_current AS
SELECT *
FROM corpscout.rdap_network_registry_class FINAL;

-- The rule, in SQL: the twin of registry_class() in commoncrawl_rdap/registry.py, which also
-- holds this multiIf text (REGISTRY_CLASS_SQL) for the contract test. A registration is judged
-- by its first address: registry level when it covers the whole IANA block designated to an
-- RIR or is strictly wider than the holder delegation, unallocated when that address has no
-- allocated/assigned delegation, unknown until all seven sources are loaded.
CREATE VIEW IF NOT EXISTS corpscout.rdap_network_registry_class_derived AS
WITH
    toIPv6(if(ip_version = 4, concat('::ffff:', start_address), start_address)) AS first_ip,
    toIPv6(if(ip_version = 4, concat('::ffff:', end_address), end_address)) AS last_ip,
    dictGetOrDefault('corpscout.ip_registry_iana_trie', ('designation', 'rir', 'status', 'block_first', 'block_last'), tuple(first_ip), ('', '', '', toUInt128(0), toUInt128(0))) AS iana,
    dictGetOrDefault('corpscout.ip_registry_delegation_trie', ('registry', 'cc', 'status', 'block_first', 'block_last'), tuple(first_ip), ('', '', '', toUInt128(0), toUInt128(0))) AS delegation,
    (SELECT ready FROM corpscout.ip_registry_ready) AS ready
SELECT
    network_key,
    multiIf(
        NOT ready, 'unknown',
        iana.2 != '' AND toUInt128(first_ip) <= iana.4 AND toUInt128(last_ip) >= iana.5, 'registry_level',
        delegation.3 NOT IN ('allocated', 'assigned'), 'unallocated',
        toUInt128(first_ip) <= delegation.4 AND toUInt128(last_ip) >= delegation.5 AND (toUInt128(first_ip) < delegation.4 OR toUInt128(last_ip) > delegation.5), 'registry_level',
        'reusable') AS registry_class,
    first_ip AS network_first,
    last_ip AS network_last,
    delegation.1 AS delegation_registry,
    delegation.3 AS delegation_status,
    toIPv6(delegation.4) AS delegation_first,
    toIPv6(delegation.5) AS delegation_last,
    iana.1 AS iana_designation,
    iana.2 AS iana_rir,
    iana.3 AS iana_status
FROM corpscout.rdap_networks_current;
```

`clickhouse/migrations/000449_corpscout_ip_registry_reference_data.down.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

DROP VIEW IF EXISTS corpscout.rdap_network_registry_class_derived;
DROP VIEW IF EXISTS corpscout.rdap_network_registry_class_current;
DROP TABLE IF EXISTS corpscout.rdap_network_registry_class;
DROP VIEW IF EXISTS corpscout.ip_registry_ready;
DROP DICTIONARY IF EXISTS corpscout.ip_registry_delegation_trie;
DROP DICTIONARY IF EXISTS corpscout.ip_registry_iana_trie;

REVOKE SELECT ON corpscout.ip_registry_delegation_trie_source FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_delegations_current FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_delegations FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_iana_trie_source FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_iana_blocks_current FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_iana_blocks FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_current_snapshots FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_snapshots FROM corpscout_rdap_dictionary;

DROP VIEW IF EXISTS corpscout.ip_registry_delegation_trie_source;
DROP VIEW IF EXISTS corpscout.ip_registry_iana_trie_source;
DROP VIEW IF EXISTS corpscout.ip_registry_delegations_current;
DROP VIEW IF EXISTS corpscout.ip_registry_iana_blocks_current;
DROP TABLE IF EXISTS corpscout.ip_registry_delegations;
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
"""IP registry reference data against a real ClickHouse: migration 000449, snapshots, tries, rule parity."""

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
DELEGATION_INSERT = f"INSERT INTO corpscout.{tables.DELEGATIONS_TABLE} ({', '.join(tables.DELEGATION_COLUMNS)}) VALUES"


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
    for name in (tables.IANA_TRIE, tables.DELEGATION_TRIE, "rdap_network_trie"):
        client.execute(f"SYSTEM RELOAD DICTIONARY corpscout.{name}")


@pytest.fixture
def clean(registry_server):
    client, resource = registry_server
    for table in (
        tables.SNAPSHOTS_TABLE,
        tables.IANA_TABLE,
        tables.DELEGATIONS_TABLE,
        "rdap_network_registry_class",
        "rdap_networks",
        "rdap_network_segments",
        "rdap_ip_lookup_results",
    ):
        client.execute(f"TRUNCATE TABLE corpscout.{table}")
    reload_tries(client)
    return client, resource


def seed_reference_data(client) -> None:
    """Insert the fixture excerpts as the current snapshot of all seven sources and reload the tries."""
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
        client.execute(
            SNAPSHOT_INSERT,
            [(source_name, IANA_DATE, loaded_at, "fixture", "", sum(b.ip_version == 4 for b in blocks),
              sum(b.ip_version == 6 for b in blocks), url)],
        )
    for registry_name, url in tables.RIR_SOURCES.items():
        text = (FIXTURES / f"delegated-{registry_name}-extended-excerpt").read_text(encoding="utf-8")
        parsed = source.parse_delegated(text, registry_name)
        snapshot_date = parsed.header.end_date
        client.execute(
            DELEGATION_INSERT,
            [
                (r.registry, snapshot_date, r.ip_version, r.cc, r.status, r.start_address, r.value,
                 IPv6Address(r.first), IPv6Address(r.last), IPv6Address(r.block_first), IPv6Address(r.block_last),
                 list(r.cidrs), r.delegated_on, r.opaque_id, loaded_at)
                for r in parsed.records
            ],
        )
        client.execute(
            SNAPSHOT_INSERT,
            [(registry_name, snapshot_date, loaded_at, "fixture", parsed.header.serial,
              parsed.summaries["ipv4"], parsed.summaries["ipv6"], url)],
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
CASES = [
    ("apnic", "103.0.0.0 - 103.255.255.255", "103.0.0.0", "103.255.255.255", "APNIC-AP", "registry_level"),
    ("apnic", "FPT-VN", "103.35.64.0", "103.35.67.255", "FPT-VN", "reusable"),
    ("arin", "NET6-2600-1", "2600::", "260f:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "ARIN-6", "registry_level"),
    ("arin", "NET-104-16-0-0-1", "104.16.0.0", "104.31.255.255", "CLOUDFLARENET", "reusable"),
    ("arin", "GOOGLE-IPV6", "2001:4860::", "2001:4860:ffff:ffff:ffff:ffff:ffff:ffff", "GOOGLE-IPV6", "reusable"),
    ("lacnic", "45.68.105.0/24", "45.68.105.0", "45.68.105.255", "UNALLOCATED", "unallocated"),
    ("arin", "FORD-NET", "19.0.0.0", "19.255.255.255", "FORD-NET", "reusable"),
    ("ripencc", "DK-NET", "195.85.96.0", "195.85.101.255", "DK-NET", "reusable"),
    ("ripencc", "SE-BOTH", "2.0.0.0", "2.2.255.255", "SE-BOTH", "reusable"),
    ("ripencc", "SE-AND-FR", "2.0.0.0", "2.3.255.255", "SE-AND-FR", "registry_level"),
    ("ripencc", "EU-ZZ-2A00", "2a00::", "2a1f:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "EU-ZZ-2A00", "registry_level"),
    ("ripencc", "AVAILABLE", "85.8.248.0", "85.8.255.255", "AVAILABLE", "unallocated"),
    ("arin", "WIDE", "100.0.0.0", "103.255.255.255", "SOMEONE", "registry_level"),
]


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


def delegation_of(client, ip):
    [(row,)] = client.execute(
        "SELECT dictGetOrDefault('corpscout.ip_registry_delegation_trie', ('registry', 'cc', 'status', 'block_first', 'block_last'), tuple(toIPv6(%(ip)s)), ('', '', '', toUInt128(0), toUInt128(0)))",
        {"ip": str(IPv6Address(source.address_int(ip)))},
    )
    return row


def iana_of(client, ip):
    [(row,)] = client.execute(
        "SELECT dictGetOrDefault('corpscout.ip_registry_iana_trie', ('designation', 'rir', 'status'), tuple(toIPv6(%(ip)s)), ('', '', ''))",
        {"ip": str(IPv6Address(source.address_int(ip)))},
    )
    return row


def test_migration_449_embeds_the_rule_and_reads_through_the_dictionary_user():
    up = (MIGRATIONS / f"{MIGRATION_449}.up.sql").read_text()
    down = (MIGRATIONS / f"{MIGRATION_449}.down.sql").read_text()
    normalize = lambda text: " ".join(text.split())  # noqa: E731
    assert normalize(registry.REGISTRY_CLASS_SQL) in normalize(up)
    assert up.count("USER 'corpscout_rdap_dictionary'") == 2
    assert up.count("LIFETIME(MIN 3600 MAX 7200)") == 2
    for name in (
        "ip_registry_snapshots", "ip_registry_current_snapshots", "ip_registry_iana_blocks",
        "ip_registry_iana_blocks_current", "ip_registry_iana_trie_source", "ip_registry_delegations",
        "ip_registry_delegations_current", "ip_registry_delegation_trie_source",
    ):
        assert f"GRANT SELECT ON corpscout.{name} TO corpscout_rdap_dictionary" in up
        assert f"REVOKE SELECT ON corpscout.{name} FROM corpscout_rdap_dictionary" in down
    assert "TTL snapshot_date + INTERVAL 2 YEAR" in up
    assert down.index("DROP DICTIONARY") < down.index("DROP VIEW IF EXISTS corpscout.ip_registry_delegation_trie_source") < down.index("DROP TABLE IF EXISTS corpscout.ip_registry_delegations")


def test_seeded_snapshots_answer_lookups_and_merge_one_holders_records(clean):
    client, _ = clean
    seed_reference_data(client)
    assert client.execute("SELECT ready FROM corpscout.ip_registry_ready") == [(1,)]
    assert client.execute(
        f"SELECT source, snapshot_date FROM corpscout.ip_registry_current_snapshots ORDER BY source"
    ) == [
        ("afrinic", date(2026, 9, 24)), ("apnic", date(2026, 9, 25)), ("arin", date(2026, 9, 25)),
        ("iana_ipv4", IANA_DATE), ("iana_ipv6", IANA_DATE), ("lacnic", date(2026, 9, 24)),
        ("ripencc", date(2026, 9, 24)),
    ]
    assert client.execute("SELECT count() FROM corpscout.ip_registry_delegations_current") == [(40,)]
    assert client.execute("SELECT count() FROM corpscout.ip_registry_iana_blocks_current") == [(18,)]
    mapped = source.address_int
    assert delegation_of(client, "103.35.64.49") == ("apnic", "VN", "allocated", mapped("103.35.64.0"), mapped("103.35.67.255"))
    assert delegation_of(client, "2.2.0.5") == ("ripencc", "SE", "allocated", mapped("2.0.0.0"), mapped("2.2.255.255"))
    assert delegation_of(client, "2.3.0.1")[3:] == (mapped("2.3.0.0"), mapped("2.3.255.255"))
    assert delegation_of(client, "195.85.101.7")[:3] == ("ripencc", "DK", "allocated")  # second CIDR of 1536 addresses
    assert delegation_of(client, "45.68.105.9")[2] == "reserved"
    assert delegation_of(client, "102.199.0.1")[:3] == ("afrinic", "ZZ", "available")
    assert delegation_of(client, "2001:1201:20::1")[2] == "available"
    assert delegation_of(client, "2001:4860::8888")[:3] == ("arin", "US", "allocated")
    assert delegation_of(client, "9.9.9.9") == ("", "", "", 0, 0)
    assert iana_of(client, "103.0.0.0") == ("APNIC", "apnic", "ALLOCATED")
    assert iana_of(client, "19.5.0.1") == ("Ford Motor Company", "", "LEGACY")
    assert iana_of(client, "2600:1f00::1") == ("ARIN", "arin", "ALLOCATED")
    assert iana_of(client, "1.1.1.1") == ("", "", "")


def test_a_newer_ledger_row_switches_the_current_snapshot_but_a_partial_load_does_not(clean):
    client, _ = clean
    seed_reference_data(client)
    loaded_at = datetime.now(UTC)
    later = date(2026, 9, 25)
    # Rows without a ledger row are not current.
    client.execute(
        DELEGATION_INSERT,
        [("ripencc", later, 4, "DK", "reserved", "195.85.96.0", 1536, IPv6Address(source.address_int("195.85.96.0")),
          IPv6Address(source.address_int("195.85.101.255")), IPv6Address(source.address_int("195.85.96.0")),
          IPv6Address(source.address_int("195.85.101.255")), ["195.85.96.0/22", "195.85.100.0/23"], None, "", loaded_at)],
    )
    reload_tries(client)
    assert delegation_of(client, "195.85.96.1")[2] == "allocated"
    assert client.execute("SELECT snapshot_date FROM corpscout.ip_registry_current_snapshots WHERE source = 'ripencc'") == [(date(2026, 9, 24),)]
    client.execute(SNAPSHOT_INSERT, [("ripencc", later, loaded_at, "fixture-2", "1790373599", 1, 0, "")])
    reload_tries(client)
    assert delegation_of(client, "195.85.96.1")[2] == "reserved"
    assert delegation_of(client, "2.2.0.5") == ("", "", "", 0, 0)  # the new snapshot has only one record
    assert client.execute("SELECT count() FROM corpscout.ip_registry_delegations WHERE registry = 'ripencc'") == [(12,)]  # history kept
    client.execute("TRUNCATE TABLE corpscout.ip_registry_snapshots")
    assert client.execute("SELECT ready FROM corpscout.ip_registry_ready") == [(0,)]


def test_python_rule_and_derived_view_agree_on_every_case(clean):
    client, _ = clean
    stored = insert_case_networks(client)
    # Not ready: the view and the Python rule both say unknown.
    assert set(client.execute("SELECT DISTINCT registry_class FROM corpscout.rdap_network_registry_class_derived")) == {("unknown",)}
    assert all(registry.classify_registration(client, network).registry_class == "unknown" for network, _ in stored.values())
    seed_reference_data(client)
    from_sql = dict(client.execute("SELECT network_key, registry_class FROM corpscout.rdap_network_registry_class_derived"))
    from_python = {key: registry.classify_registration(client, network).registry_class for key, (network, _) in stored.items()}
    expected = {key: expected for key, (_, expected) in stored.items()}
    assert from_sql == expected
    assert from_python == expected
    # The persisted row shape is the same from both writers.
    classification = registry.classify_registration(client, stored["apnic:FPT-VN"][0])
    client.execute(registry.REGISTRY_CLASS_INSERT_SQL, [classification.clickhouse_values("apnic:FPT-VN", datetime.now(UTC))])
    client.execute(registry.REGISTRY_CLASS_REFRESH_SQL)
    rows = client.execute(
        "SELECT registry_class, delegation_registry, delegation_status, toString(delegation_first), toString(delegation_last), iana_designation, iana_rir FROM corpscout.rdap_network_registry_class_current WHERE network_key = 'apnic:FPT-VN'"
    )
    assert rows == [("reusable", "apnic", "allocated", "::ffff:103.35.64.0", "::ffff:103.35.67.255", "APNIC", "apnic")]
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
        "SELECT dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv4('103.15.66.50')), ''), dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv4('103.35.64.49')), ''), dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv6('2600:1f00::1')), '')"
    ) == [("", "apnic:FPT-VN", "")]
    # A later classification wins (ReplacingMergeTree by network_key): mark FPT registry_level, then reusable again.
    for registry_class, expect_served in (("registry_level", False), ("reusable", True)):
        client.execute(
            "INSERT INTO corpscout.rdap_network_registry_class (network_key, registry_class, network_first, network_last, delegation_registry, delegation_status, delegation_first, delegation_last, iana_designation, iana_rir, iana_status, classified_at) VALUES",
            [("apnic:FPT-VN", registry_class, IPv6Address(0), IPv6Address(0), "", "", IPv6Address(0), IPv6Address(0), "", "", "", datetime.now(UTC))],
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
git commit -m "feat(clickhouse): IP registry reference snapshots, lookup tries and RDAP registration classes (000449, 000450)

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: The `ip_registry` Dagster module — loaders, class asset, freshness checks, job, daily schedule

**Files:**
- Create: `services/dagster_v3/src/dagster_v3/defs/ip_registry/assets.py`
- Modify: `services/dagster_v3/tests/test_ip_registry.py` (append the loader/asset tests)

**Interfaces:**
- Consumes: Task 1 parsers and `REGISTRY_CLASS_REFRESH_SQL`; Task 2 objects; `assert_clickhouse_tables_exist` (`defs/clickhouse/resolved.py`).
- Produces (`dagster_v3.defs.ip_registry.assets`): `GROUP_NAME = "ip_registry"`, `INSERT_BATCH`, `MAX_SHRINK_RATIO = 0.05`, `IANA_MIN_ROWS`, `FRESH_SNAPSHOT_DAYS = 3`, `FRESH_VERIFIED_DAYS = 2`, `IpRegistryConfig(allow_shrink: bool = False)`, `fetch(url) -> tuple[bytes, Mapping]`, `current_snapshot(client, source) -> dict | None`, `refuse_shrink(...)`, `record_snapshot(...)`, `iana_snapshot_date(headers) -> date`, `load_iana_source(client, source, *, body, headers, url, min_rows=None) -> dict`, `load_delegated_source(client, registry, *, body, md5_text, url, allow_shrink) -> dict`, `snapshot_freshness(sources, rows, now) -> dg.AssetCheckResult`, assets `ip_registry_iana_blocks`, `delegation_assets` (list of five, names `ip_registry_delegations_<rir>`), `rdap_network_registry_class`, `checks` (list of seven `AssetChecksDefinition`), `ip_registry_refresh_job`, `ip_registry_daily`, `defs`.

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
    monkeypatch.setattr(assets, "IANA_MIN_ROWS", {"iana_ipv4": 11, "iana_ipv6": 7})
    return calls


def refresh(resource, **config):
    return dg.materialize(
        [assets.ip_registry_iana_blocks, *assets.delegation_assets, assets.rdap_network_registry_class, *assets.checks],
        resources={"clickhouse": resource},
        run_config={"ops": {asset.op.name: {"config": config} for asset in assets.delegation_assets}} if config else None,
        raise_on_error=False,
    )


def test_refresh_loads_every_source_classifies_and_passes_the_checks(clean, monkeypatch):
    client, resource = clean
    stored = insert_case_networks(client)
    calls = fixture_http(monkeypatch)
    result = refresh(resource)
    assert result.success
    assert sorted(calls) == sorted([*tables.IANA_SOURCES.values(), *tables.RIR_SOURCES.values(), *(url + ".md5" for url in tables.RIR_SOURCES.values())])
    assert client.execute("SELECT source, snapshot_date, records_ipv4, records_ipv6 FROM corpscout.ip_registry_snapshots FINAL ORDER BY source") == [
        ("afrinic", date(2026, 9, 24), 5, 2), ("apnic", date(2026, 9, 25), 6, 2), ("arin", date(2026, 9, 25), 4, 3),
        ("iana_ipv4", IANA_DATE, 11, 0), ("iana_ipv6", IANA_DATE, 0, 7), ("lacnic", date(2026, 9, 24), 3, 4),
        ("ripencc", date(2026, 9, 24), 8, 3),
    ]
    assert client.execute("SELECT serial FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'arin'") == [("1790341220831",)]
    assert client.execute("SELECT count() FROM corpscout.ip_registry_delegations_current") == [(40,)]
    assert delegation_of(client, "103.35.64.49")[:3] == ("apnic", "VN", "allocated")
    assert dict(client.execute("SELECT network_key, registry_class FROM corpscout.rdap_network_registry_class_current")) == {
        key: expected for key, (_, expected) in stored.items()
    }
    materialization = result.asset_materializations_for_node("rdap_network_registry_class")[0].metadata
    assert materialization["networks_registry_level"].value == 5 and materialization["networks_total"].value == len(CASES)
    evaluations = result.get_asset_check_evaluations()
    assert len(evaluations) == 7 and all(evaluation.passed for evaluation in evaluations)
    assert {evaluation.check_name for evaluation in evaluations} == {"snapshot_fresh", "classification_complete"}
    loaded = result.asset_materializations_for_node("ip_registry_delegations_ripencc")[0].metadata
    assert (loaded["loaded"].value, loaded["records_ipv4"].value, loaded["md5"].value) == (True, 8, hashlib.md5((FIXTURES / "delegated-ripencc-extended-excerpt").read_bytes()).hexdigest())


def test_identical_snapshot_is_verified_not_reloaded(clean, monkeypatch):
    client, resource = clean
    fixture_http(monkeypatch)
    assert refresh(resource).success
    [(rows_before, verified_before)] = client.execute("SELECT count(), max(verified_at) FROM corpscout.ip_registry_snapshots FINAL")
    delegations_before = client.execute("SELECT count() FROM corpscout.ip_registry_delegations")
    again = refresh(resource)
    assert again.success
    assert again.asset_materializations_for_node("ip_registry_delegations_apnic")[0].metadata["loaded"].value is False
    assert again.asset_materializations_for_node("ip_registry_iana_blocks")[0].metadata["iana_ipv4_loaded"].value is False
    [(rows_after, verified_after)] = client.execute("SELECT count(), max(verified_at) FROM corpscout.ip_registry_snapshots FINAL")
    assert (rows_after, rows_before) == (7, 7) and verified_after > verified_before
    assert client.execute("SELECT count() FROM corpscout.ip_registry_delegations") == delegations_before


def test_bad_checksum_older_file_and_shrinking_snapshot_are_refused(clean, monkeypatch):
    client, resource = clean
    ripencc = tables.RIR_SOURCES["ripencc"]
    body = (FIXTURES / "delegated-ripencc-extended-excerpt").read_bytes()
    fixture_http(monkeypatch, tamper={ripencc + ".md5": (b"MD5 (delegated-ripencc-extended-latest) = " + b"0" * 32 + b"\n", {})})
    result = refresh(resource)
    assert not result.success
    assert client.execute("SELECT count() FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'ripencc'") == [(0,)]
    assert client.execute("SELECT count() FROM corpscout.ip_registry_delegations WHERE registry = 'ripencc'") == [(0,)]
    assert client.execute("SELECT count() FROM corpscout.ip_registry_snapshots FINAL") == [(6,)]  # the other six loaded
    assert client.execute("SELECT ready FROM corpscout.ip_registry_ready") == [(0,)]
    fixture_http(monkeypatch)
    assert refresh(resource).success
    # A file dated before the current snapshot is refused.
    older = body.replace(b"|20260924|+0200", b"|20260923|+0200", 1)
    fixture_http(monkeypatch, tamper={ripencc: (older, {}), ripencc + ".md5": (f"MD5 (x) = {hashlib.md5(older).hexdigest()}\n".encode(), {})})
    assert not refresh(resource).success
    # A newer file that lost half its IPv4 records is refused unless allow_shrink is set.
    shrunk = (
        body.replace(b"|20260924|+0200", b"|20260925|+0200", 1)
        .replace(b"2|ripencc|1790287199|11|", b"2|ripencc|1790373599|7|", 1)
        .replace(b"ripencc|*|ipv4|*|8|summary", b"ripencc|*|ipv4|*|4|summary", 1)
        .replace(b"ripencc|SE|ipv4|2.0.0.0|131072|20100712|allocated|12a581c1-ea86-46af-9554-77e3b4ab3df5\n", b"")
        .replace(b"ripencc|SE|ipv4|2.2.0.0|65536|20100712|allocated|12a581c1-ea86-46af-9554-77e3b4ab3df5\n", b"")
        .replace(b"ripencc|FR|ipv4|2.3.0.0|65536|20100712|allocated|9a489e65-dd78-443e-96ab-e21e016b5113\n", b"")
        .replace(b"ripencc|PS|ipv4|1.178.112.0|4096|20071126|allocated|172ce676-8ded-4901-9812-793bd0b4ec77\n", b"")
    )
    tamper = {ripencc: (shrunk, {}), ripencc + ".md5": (f"MD5 (x) = {hashlib.md5(shrunk).hexdigest()}\n".encode(), {})}
    fixture_http(monkeypatch, tamper=tamper)
    assert not refresh(resource).success
    assert client.execute("SELECT snapshot_date FROM corpscout.ip_registry_current_snapshots WHERE source = 'ripencc'") == [(date(2026, 9, 24),)]
    fixture_http(monkeypatch, tamper=tamper)
    assert refresh(resource, allow_shrink=True).success
    assert client.execute("SELECT snapshot_date, records_ipv4 FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'ripencc' ORDER BY snapshot_date DESC LIMIT 1") == [(date(2026, 9, 25), 4)]


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
    names = {asset.key.to_user_string() for asset in [assets.ip_registry_iana_blocks, *assets.delegation_assets, assets.rdap_network_registry_class]}
    assert names == {"ip_registry_iana_blocks", "rdap_network_registry_class", *(f"ip_registry_delegations_{r}" for r in tables.RIR_SOURCES)}
    assert len(assets.checks) == 7
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_ip_registry.py -q -p no:cacheprovider`
Expected: FAIL — `ImportError: cannot import name 'assets' from 'dagster_v3.defs.ip_registry'`.

- [ ] **Step 3: Create `assets.py`**

Create `services/dagster_v3/src/dagster_v3/defs/ip_registry/assets.py`:

```python
"""Daily refresh of the IP registry reference data and the RDAP registration classes.

Seven small public files (IANA ipv4/ipv6 address space, five RIR delegated-extended
statistics, ~45 MB in total) are downloaded, verified (checksum, version line, per-type
record counts, no sharp shrink against the current snapshot) and inserted as dated snapshots;
the ledger row that makes a snapshot current is written last. Then every cached RDAP
registration is classified in SQL and rdap_network_trie is reloaded. Non-partitioned full
refresh (the whole dataset comes back per request), daily schedule stopped by default, one
pool for the whole chain.
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
# Refuse a snapshot with more than this share of records missing against the current one
# (a truncated download, a moved file) unless the run says allow_shrink.
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
DELEGATION_INSERT_SQL = (
    f"INSERT INTO corpscout.{tables.DELEGATIONS_TABLE} ({', '.join(tables.DELEGATION_COLUMNS)}) VALUES"
)


class IpRegistryConfig(dg.Config):
    allow_shrink: bool = Field(
        default=False,
        description="Accept a snapshot with more than 5% fewer ipv4 or ipv6 records than the current one.",
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
    client, *, source: str, snapshot_date: date, checksum: str, serial: str, ipv4: int, ipv6: int, url: str
) -> None:
    client.execute(
        SNAPSHOT_INSERT_SQL,
        [(source, snapshot_date, datetime.now(UTC), checksum, serial, ipv4, ipv6, url)],
    )


def insert_rows(client, sql: str, rows: Sequence[tuple]) -> None:
    for offset in range(0, len(rows), INSERT_BATCH):
        client.execute(sql, rows[offset : offset + INSERT_BATCH])


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
        serial=headers.get("Last-Modified", ""), ipv4=ipv4, ipv6=ipv6, url=url,
    )
    return {
        "source": source,
        "snapshot_date": snapshot_date.isoformat(),
        "rows": len(blocks),
        "loaded": loaded,
        "sha256": checksum,
    }


def load_delegated_source(
    client, registry: str, *, body: bytes, md5_text: str, url: str, allow_shrink: bool
) -> dict:
    """Verify the published MD5, parse, validate against the current snapshot and store."""
    digest = hashlib.md5(body).hexdigest()
    expected = parse_md5(md5_text)
    if digest != expected:
        raise ValueError(f"{registry}: MD5 {digest} does not match the published {expected}")
    parsed = parse_delegated(body.decode("utf-8"), registry)
    if not parsed.records:
        raise ValueError(f"{registry}: no ipv4/ipv6 records")
    ipv4, ipv6 = parsed.summaries.get("ipv4", 0), parsed.summaries.get("ipv6", 0)
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
            DELEGATION_INSERT_SQL,
            [
                (
                    record.registry, snapshot_date, record.ip_version, record.cc, record.status,
                    record.start_address, record.value, IPv6Address(record.first), IPv6Address(record.last),
                    IPv6Address(record.block_first), IPv6Address(record.block_last), list(record.cidrs),
                    record.delegated_on, record.opaque_id, loaded_at,
                )
                for record in parsed.records
            ],
        )
    record_snapshot(
        client, source=registry, snapshot_date=snapshot_date, checksum=digest,
        serial=parsed.header.serial, ipv4=ipv4, ipv6=ipv6, url=url,
    )
    return {
        "source": registry,
        "snapshot_date": snapshot_date.isoformat(),
        "records_ipv4": ipv4,
        "records_ipv6": ipv6,
        "loaded": loaded,
        "md5": digest,
        "serial": parsed.header.serial,
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


def delegation_asset(registry: str, url: str) -> dg.AssetsDefinition:
    @dg.asset(
        name=f"ip_registry_delegations_{registry}",
        group_name=GROUP_NAME,
        kinds={"python", "clickhouse", "rir"},
        pool=tables.IP_REGISTRY_POOL,
        description=f"Downloads {url} and its .md5, verifies the file and stores its ipv4/ipv6 "
        f"delegations as the snapshot dated by the file's end date.",
    )
    def _asset(
        context: dg.AssetExecutionContext, config: IpRegistryConfig, clickhouse: ClickhouseResource
    ) -> dg.MaterializeResult:
        assert_clickhouse_tables_exist(
            clickhouse, database=tables.DATABASE, tables=(tables.SNAPSHOTS_TABLE, tables.DELEGATIONS_TABLE)
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


delegation_assets = [delegation_asset(registry, url) for registry, url in tables.RIR_SOURCES.items()]


@dg.asset(
    deps=[ip_registry_iana_blocks, *delegation_assets],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "rdap"},
    pool=tables.IP_REGISTRY_POOL,
    description="Reloads the reference tries and reclassifies every cached RDAP registration "
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
        for name in (tables.IANA_TRIE, tables.DELEGATION_TRIE):
            client.execute(f"SYSTEM RELOAD DICTIONARY corpscout.{name}")
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
        for asset, registry in zip(delegation_assets, tables.RIR_SOURCES, strict=True)
    ),
    classification_complete,
]

ip_registry_refresh_job = dg.define_asset_job(
    "ip_registry_refresh_job",
    selection=dg.AssetSelection.assets(rdap_network_registry_class).upstream(),
)
# 06:05 UTC: the RIR files dated D are all published by then (APNIC publishes D's file on D+1
# at +10:00). No other schedule uses this minute. STOPPED by default; start it at instance level.
ip_registry_daily = dg.ScheduleDefinition(
    name="ip_registry_daily",
    job=ip_registry_refresh_job,
    cron_schedule="5 6 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[ip_registry_iana_blocks, *delegation_assets, rdap_network_registry_class],
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
git commit -m "feat(dagster): ip_registry module loads IANA and RIR reference snapshots daily and classifies cached RDAP registrations

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: RdapEnricher and the legacy bucket worker never index a non-reusable registration

Today both writers store every direct registration as `lookup_result` segments and reuse it at once: `RdapEnricher` (`ip_enrichment/enrichment.py:191-202, 356-358`) via `_persist` + `_remember`, the bucket worker (`commoncrawl_rdap/assets.py:596-606`) via `_insert_normalized_networks` + `in_run_segments`. The change is the same in both: classify the direct registration with the Python rule (one `REGISTRY_CONTEXT_SQL` round trip), write its class row between the network row and the segment rows (so the trie source never sees a segment without its class), and skip the in-run reuse when the class is not reusable. Parents are stored as before (they never feed the trie). Nothing else changes; the trie exclusion itself is data (Task 2).

**Files:**
- Modify: `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py:25, 173, 191-202, 356-358`
- Modify: `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/results.py:151-172, 255-259`
- Modify: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/assets.py:22-27, 414-430, 596-606, 650-688, 787-800`
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

Add to the imports `from dagster_v3.defs.commoncrawl_rdap import registry` and `from dagster_v3.defs.ip_registry.source import address_int`, and append:

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
    # 103/8 is IANA -> APNIC and 103.0.0.0/16 is a holder delegation: the /8 answer is registry level.
    write_client.context_rows = [
        (
            1,
            ("APNIC", "apnic", "ALLOCATED", address_int("103.0.0.0"), address_int("103.255.255.255")),
            ("apnic", "AU", "allocated", address_int("103.0.0.0"), address_int("103.0.255.255")),
        )
    ]
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
    assert class_rows[0][4:6] == ("apnic", "allocated") and class_rows[0][-1] == FETCHED_AT
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
        "ip_registry_delegations",
    ):
        client.execute(f"TRUNCATE TABLE corpscout.{table}")
    for name in ("ip_registry_iana_trie", "ip_registry_delegation_trie"):
        client.execute(f"SYSTEM RELOAD DICTIONARY corpscout.{name}")
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
        "SELECT network_key, registry_class, delegation_status, iana_rir FROM corpscout.rdap_network_registry_class_current"
    ) == [("arin:TEST-103.35.64.49", "registry_level", "allocated", "apnic")]
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

In `RdapEnricher.__init__` after `self.parent_failures = 0` add `self.registry_level_responses = 0`.

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

In `lookup` replace (lines 356-358):

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

Replace (lines 596-606):

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
- **Registry**: IANA (top-level address space) and the five RIRs (delegated-extended statistics). Reference
  data, not company data — no entity key, no translation, no currency, no contacts (§6–8 of the
  guidelines do not apply).
- **Module**: `defs/ip_registry/` · no DuckDB file (see §3) · pool `ip_registry`
- **ClickHouse tables**: `corpscout.ip_registry_snapshots`, `ip_registry_iana_blocks`, `ip_registry_delegations`,
  `rdap_network_registry_class` (migration `000449`); trie exclusion in `000450`.
- **Datasets**:
  | dataset | url | format | size | cadence | auth? |
  |---|---|---|---|---|---|
  | IANA IPv4 address space | https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.csv | CSV, 256 rows | 23 KB | rare (Last-Modified) | no |
  | IANA IPv6 unicast assignments | https://www.iana.org/assignments/ipv6-unicast-address-assignments/ipv6-unicast-address-assignments.csv | CSV, 51 rows, multi-line quoted notes | 6 KB | rare | no |
  | delegated-{afrinic,apnic,arin,lacnic,ripencc}-extended-latest (+ .md5) | ftp.afrinic.net/pub/stats/afrinic, ftp.apnic.net/stats/apnic, ftp.arin.net/pub/stats/arin, ftp.lacnic.net/pub/stats/lacnic, ftp.ripe.net/pub/stats/ripencc | pipe-separated, version line + summaries + records | 1–18 MB, 654k ipv4/ipv6 records | daily | no |
- **Record count**: ~660k CIDRs across the delegation trie, 307 IANA blocks.

## 2. Ingest mode — and why
- Chosen: single-request full refresh per file, non-partitioned, daily. Every file is the whole
  registry; partitions would only add event-log churn (CLAUDE.md, `exchange_rates_v2` precedent).
- Snapshots are kept (dated by the file's end date / IANA's Last-Modified) so changes over time stay
  visible; `_current` views read the newest ledger snapshot per source.
- Format quirks: APNIC's `#` banner; RIPE NCC and LACNIC write seven fields for available/reserved
  records; IPv4 `value` is an address count, not always a power of two; ARIN's `.md5` is GNU style
  with a dated file name; the IANA IPv4 header is `Status [1]`; the IANA RDAP column glues two URLs.

## 3. Loading — deviation from the DuckDB golden path
- Reader: Python (`csv` for IANA, `str.split('|')` for the RIRs), then batched native inserts.
- Why: the files are small (≤ 18 MB, ≤ 261k lines) and every row needs address arithmetic (IPv4-mapped
  integers, `iprange_to_cidrs`, adjacent-record merge) that DuckDB does not offer; parsing takes
  seconds. A DuckDB stage would add a file, a pool and no value. This is the deviation from §3.
- Validation before anything is written: MD5 against the published digest, version line parsed and
  its `records` equal to the record lines, each summary equal to its type's count, known statuses,
  parsable addresses, a file not older than the current snapshot, and no >5% drop in ipv4/ipv6 records
  (`allow_shrink` run config overrides). The ledger row is written after the rows.

## 4. Transform
- None outside the load: derived columns (`first_ip`, `last_ip`, `block_first`, `block_last`, `cidrs`,
  `rir`) are computed in Python at load time. The classification of RDAP registrations is set-based
  SQL (`rdap_network_registry_class_derived`).

## 5. ClickHouse schema — and DDL deviations
- Grain: one row per (source, snapshot_date, block) / (registry, snapshot_date, record) /
  (network_key) for classes. `ReplacingMergeTree(loaded_at | verified_at | classified_at)`.
- Address bounds are `IPv6` columns (IPv4 as `::ffff:a.b.c.d`) compared as `UInt128` so both families
  share one key space; dictionaries are `IP_TRIE` over `ARRAY JOIN cidrs`.
- `PARTITION BY toYYYYMM(snapshot_date)`; delegations `TTL snapshot_date + INTERVAL 2 YEAR`.
- No `raw_*` payloads and no `source_payload_hash` (the ledger keeps one checksum per snapshot).

## 6–7. Translation, contacts, currency
- Not applicable (reference data, no free text, no monetary amounts, no company contacts).

## 8. Scheduling
- `ip_registry_refresh_job` = the seven loaders → `rdap_network_registry_class`; `ip_registry_daily`
  at `5 6 * * *` UTC (the APNIC file dated D appears on D+1 at +10:00), STOPPED by default, started at
  instance level. Checks: `snapshot_fresh` per loader (RIR snapshot ≤ 3 days, verified ≤ 2 days; IANA
  verified only) and `classification_complete`.

## 9. Issues found during processing
- ClickHouse resolves the `argMax(...) AS network_key` alias inside an outer `WHERE`
  (`ILLEGAL_AGGREGATION`), so the trie view's exclusion lives in a subquery.
- A registration's first address, not the queried IP, is the lookup point, so that the bulk view and
  the per-miss classifier ask the same question.
- 8,017 of RIPE NCC's IPv4 records continue the previous record of the same holder; without merging
  adjacent same-holder records a legitimate registration covering both would be judged "wider than
  its delegation".
- APNIC's version line has an empty start date; AFRINIC's is `00000000`; ARIN's asn dates can be
  `00000000`.

## 10. Verification
- Tests: `tests/test_ip_registry_source.py` (parsers, rule, freshness), `tests/test_ip_registry.py`
  (migrations 449/450, snapshot switch, tries, loaders with fixture HTTP, SQL/Python parity, trie
  exclusion), enricher/worker tests in `tests/test_ip_enrichment_results.py` and
  `tests/test_commoncrawl_rdap_assets.py`.
- Live: migrate 449 → light_sync → run `ip_registry_refresh_job` → checks green → review the
  excluded-network report → migrate 450 → reload `rdap_network_trie` → start the schedule
  (`docs/operations/ip-registry-reference-data.md`).
````

- [ ] **Step 2: Write the operations guide**

Create `services/dagster_v3/docs/operations/ip-registry-reference-data.md`:

````markdown
# IP registry reference data

Group `ip_registry` loads, daily, the IANA address-space registries and the five RIRs'
delegated-extended statistics into ClickHouse and classifies every cached RDAP registration
(`corpscout.rdap_networks`) as `reusable`, `registry_level` or `unallocated`. Only reusable
registrations feed `rdap_network_trie` (migration 000450), so an RDAP answer such as `APNIC-AP`
(103.0.0.0/8) is stored for the address that was queried and never served to other addresses.

## Objects

| Object | Meaning |
| --- | --- |
| `ip_registry_snapshots` | ledger: one row per (source, snapshot_date); `verified_at` moves on every re-check; the newest row per source is the current snapshot |
| `ip_registry_iana_blocks` / `_current` | IANA ipv4-address-space + ipv6-unicast rows (`rir` derived from the designation) |
| `ip_registry_delegations` / `_current` | RIR records with `first_ip`/`last_ip`, merged `block_first`/`block_last`, `cidrs` |
| `ip_registry_iana_trie`, `ip_registry_delegation_trie` | `IP_TRIE` dictionaries: `dictGetOrDefault(..., tuple(toIPv6(x)))` gives the block/delegation holding `x` |
| `ip_registry_ready` | `ready = 1` when all seven sources have a current snapshot |
| `rdap_network_registry_class` / `_current` / `_derived` | persisted class per registration / the rule applied live to `rdap_networks_current` |

## Running it

- Job `ip_registry_refresh_job` (seven loaders, then `rdap_network_registry_class`); schedule
  `ip_registry_daily` 06:05 UTC, stopped by default — start it on the Schedules page.
- A loader refuses (and the class asset does not run) on: MD5 mismatch, malformed version line,
  record/summary count mismatch, a file older than the current snapshot, or a >5% drop in ipv4/ipv6
  records. For a legitimate drop re-launch with run config `ops: ip_registry_delegations_<rir>:
  config: {allow_shrink: true}`.
- An identical file only refreshes `verified_at`; a changed file of the same date is loaded again
  (ReplacingMergeTree keeps the latest load).
- Checks: `snapshot_fresh` on each loader (RIR snapshot ≤ 3 days, verified ≤ 2 days; IANA verified
  only), `classification_complete` on the class asset.

## The rule

For a registration judged by its first address: `registry_level` when it covers the whole IANA block
designated to an RIR, or is strictly wider than the (merged) `allocated`/`assigned` delegation
holding that address; `unallocated` when that address has no such delegation (`available`,
`reserved`, no record); else `reusable`; `unknown` while `ip_registry_ready = 0` (then nothing is
excluded and the enrichers behave as before). `commoncrawl_rdap/registry.py` holds the Python rule
and the SQL text the derived view embeds; `tests/test_ip_registry.py` proves their parity.

## Useful queries

```sql
SELECT registry_class, count() FROM corpscout.rdap_network_registry_class_current GROUP BY registry_class;

-- Non-reusable registrations and how many addresses each one served
SELECT c.network_key, c.registry_class, n.name, n.start_address, n.end_address, ifNull(e.served_ips, 0) AS served_ips
FROM corpscout.rdap_network_registry_class_current AS c
LEFT JOIN corpscout.rdap_networks_current AS n ON n.network_key = c.network_key
LEFT JOIN (SELECT rdap_network_key, count() AS served_ips FROM corpscout.ip_enrichment_current WHERE rdap_lookup_status = 'found' GROUP BY rdap_network_key) AS e ON e.rdap_network_key = c.network_key
WHERE c.registry_class != 'reusable'
ORDER BY served_ips DESC;

-- Why a registration got its class
SELECT * FROM corpscout.rdap_network_registry_class_derived WHERE network_key = 'apnic:103.0.0.0 - 103.255.255.255';

SELECT name, status, element_count, formatReadableSize(bytes_allocated) FROM system.dictionaries WHERE database = 'corpscout' AND name LIKE 'ip_registry%';
```

Re-running `rdap_network_registry_class` (or the whole job) reclassifies everything from the
current snapshots and reloads `rdap_network_trie`; nothing else needs a restart.
````

- [ ] **Step 3: Update the two existing docs**

Append to `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md`:

```markdown
## Registry-level registrations (data-driven, 2026-09)

Both writers of `rdap_networks` — this bucket worker and `ip_enrichment`'s `RdapEnricher` —
classify every direct registration against the IP registry reference data
(`defs/ip_registry`, `docs/operations/ip-registry-reference-data.md`) with
`commoncrawl_rdap/registry.py::classify_registration` (one `REGISTRY_CONTEXT_SQL` round trip per
RDAP miss) and insert its `rdap_network_registry_class` row between the network row and the
segment rows. A `registry_level` or `unallocated` registration is stored, answers the queried
address, and is never added to the in-run reuse set; `rdap_network_segments_current` (migration
000450) excludes such networks from `rdap_network_trie`, and the daily `rdap_network_registry_class`
asset reclassifies everything from the current snapshots. While the reference data is incomplete
the class is `unknown` and nothing is excluded.
```

Append to the "## Migration and validation" section of `services/dagster_v3/docs/ip-enrichment-schema.md`:

```markdown
Migrations `000449` and `000450` add the IP registry reference data and make `rdap_network_trie`
serve only registrations classified `reusable`; `ip_enrichment_results` reports
`registry_level_responses` (registrations that answered only their queried address). See
`docs/operations/ip-registry-reference-data.md`.
```

- [ ] **Step 4: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/ip_registry/docs/ip_registry-design.md services/dagster_v3/docs/operations/ip-registry-reference-data.md services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md services/dagster_v3/docs/ip-enrichment-schema.md
git commit -m "docs: IP registry reference data, the registry-level rule and its operations

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
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT name, engine FROM system.tables WHERE database = '"'"'corpscout'"'"' AND name LIKE '"'"'ip_registry%'"'"' OR name LIKE '"'"'rdap_network_registry_class%'"'"' ORDER BY name FORMAT PrettyCompact"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT name, status FROM system.dictionaries WHERE database = '"'"'corpscout'"'"' ORDER BY name FORMAT PrettyCompact"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT ready FROM corpscout.ip_registry_ready"'
```

Expected: 3 tables + 9 views (`ip_registry_snapshots`, `ip_registry_current_snapshots`, `ip_registry_iana_blocks`, `_current`, `ip_registry_iana_trie_source`, `ip_registry_delegations`, `_current`, `ip_registry_delegation_trie_source`, `ip_registry_ready`, `rdap_network_registry_class`, `_current`, `_derived`); dictionaries `ip_registry_iana_trie`, `ip_registry_delegation_trie` (`NOT_LOADED` until first use is fine) next to `rdap_network_trie`; `ready` = `0`.

- [ ] **Step 3: Deploy Dagster by light_sync**

Run: `cd services/dagster_v3/ansible && ANSIBLE_BECOME_TIMEOUT=60 LC_ALL=en_US.UTF-8 ansible-playbook -i inventory.ini light_sync.yml </dev/null > /private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect-corpscout/9f2d193f-045d-4f26-91d7-d2b93320d3f5/scratchpad/ip/light_sync.log 2>&1; echo rc=$?`
Expected: `rc=0`; the code location reloads. In the UI: asset group `ip_registry` (7 assets, 7 checks), job `ip_registry_refresh_job`, schedule `ip_registry_daily` (stopped). `rdap_network_trie` is still the 000258 view: nothing is excluded yet, and every enricher classifies `unknown` until Step 4 completes.

- [ ] **Step 4: First load and classification**

Launch `ip_registry_refresh_job` from the UI with default config. Expected: all 7 assets succeed (~1–3 minutes: 45 MB download, 654k rows, 15k classifications) and all 7 checks pass. Verify:

```bash
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT source, snapshot_date, records_ipv4, records_ipv6, checksum FROM corpscout.ip_registry_snapshots FINAL ORDER BY source FORMAT PrettyCompact"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT registry, count() FROM corpscout.ip_registry_delegations_current GROUP BY registry ORDER BY registry FORMAT PrettyCompact"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT name, status, element_count, formatReadableSize(bytes_allocated) FROM system.dictionaries WHERE database = '"'"'corpscout'"'"' AND name LIKE '"'"'ip_registry%'"'"' FORMAT PrettyCompact"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT ready FROM corpscout.ip_registry_ready"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT registry_class, count() FROM corpscout.rdap_network_registry_class_current GROUP BY registry_class FORMAT PrettyCompact"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT dictGetOrDefault('"'"'corpscout.ip_registry_delegation_trie'"'"', ('"'"'registry'"'"','"'"'cc'"'"','"'"'status'"'"'), tuple(toIPv4('"'"'103.35.64.49'"'"')), ('"'"''"'"','"'"''"'"','"'"''"'"'))"'
```

Expected: seven ledger rows (IANA dated by Last-Modified, RIRs by yesterday's/today's file date); per-registry counts of the same order as the 2026-09-25 files (afrinic ≈ 15.4k, apnic ≈ 175k, arin ≈ 170k, lacnic ≈ 81k, ripencc ≈ 212k); both tries `LOADED` (≈ 660k and ≈ 300 elements, ~100 MB); `ready` = `1`; classes over the ~15.3k cached networks — the great majority `reusable`, a few dozen `registry_level`/`unallocated`; the FPT delegation `('apnic','VN','allocated')`.

- [ ] **Step 5: Review the excluded-network report (before the exclusion goes live)**

```bash
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT c.network_key, c.registry_class, n.name, n.start_address, n.end_address, ifNull(e.served_ips, 0) AS served_ips FROM corpscout.rdap_network_registry_class_current AS c LEFT JOIN corpscout.rdap_networks_current AS n ON n.network_key = c.network_key LEFT JOIN (SELECT rdap_network_key, count() AS served_ips FROM corpscout.ip_enrichment_current WHERE rdap_lookup_status = '"'"'found'"'"' GROUP BY rdap_network_key) AS e ON e.rdap_network_key = c.network_key WHERE c.registry_class != '"'"'reusable'"'"' ORDER BY served_ips DESC FORMAT PrettyCompact"' | tee /private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect-corpscout/9f2d193f-045d-4f26-91d7-d2b93320d3f5/scratchpad/ip/excluded-networks.txt
```

Expected: `apnic:103.0.0.0 - 103.255.255.255` (APNIC-AP, ≈135,677 served), the apnic 101/8, 111/8, 113/8 and afrinic 102/8 blocks, `ripe` EU-ZZ-2A00 (2a00::/11), `arin:NET6-2600-1` (2600::/12) and the LACNIC UNALLOCATED ranges from the review; the served_ips column sums to roughly 170k. Spot-check that no ordinary holder is listed (FPT-VN, DIGITALPACIFIC, Hetzner, Google's `8.8.8.0/24` must be `reusable`: `SELECT network_key, registry_class FROM corpscout.rdap_network_registry_class_current WHERE network_key IN (SELECT network_key FROM corpscout.rdap_networks_current WHERE start_address IN ('8.8.8.0', '103.35.64.0'))`). If a legitimate holder appears, STOP: inspect it in `rdap_network_registry_class_derived`, fix the rule or the data, and do not apply 450. Report the table and the served-IP total to the owner.

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

Start `ip_registry_daily` on the Schedules page. Record in the hand-over to the owner: the excluded-network report and served-IP total from Step 5 (the input for the remediation draft in the queue-contract plan), the dictionary memory from Step 4, and that the next enrichment runs report `registry_level_responses`.

---

## Decision coverage (owner revision of 2026-09-25)

| Requirement | Task(s) |
| --- | --- |
| IANA CSVs + five RIR delegated-extended files, URLs/columns verified by fetching | Evidence table; 1 |
| ClickHouse tables keyed by (source, snapshot_date, range), dated snapshots kept, `_current` views, retention/partition stated, delegation lookup dictionary | 2 (000449) |
| Dagster assets: download, parse, validate (checksum, version line, non-empty, row count not dropping >X%), insert, freshness checks, daily schedule STOPPED by default, staggered cron | 3 |
| Data-driven rule (registry_level / unallocated / IANA safety net / reusable) with the example cases as tests; Python classifier and trie exclusion use the same data; parity test | 1, 2, 4 |
| Trie view excludes registry-level segments from the same data; existing poisoned entries stop being served; new delegations honoured without code changes | 2 (000450), 6 |
| Classification persisted per network and recomputed after each refresh (chosen over a dictGet inside the trie source) | 2, 3 |
| Deploy: migration, light_sync, first load + checks green before the exclusion, then print excluded networks and served IPs | 6 |

## Follow-ups (out of scope here)

- The remediation re-run of the ≈170k addresses served by the now-excluded networks (`force_rdap` draft) — queue-contract plan, Task 9.
- Using the delegation trie to skip RDAP entirely for addresses in `available`/`reserved` space, and RIR-level bulk dumps as a range dictionary (decisions D9).
- Loading the IANA special-purpose registries if a consumer other than `classify_ip_scope` ever needs them.
- Storing the `asn` records of the delegated files (cheap; useful for ASN → country/registry).
- Thinning old delegation partitions (`DROP PARTITION`) if two years of daily snapshots ever matter.
