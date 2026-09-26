# IP Enrichment Queue Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move IP enrichment onto the shared processing queue contract (drafts with receipts, a task-partitioned entry table, live remaining work, acknowledged result batches, completion from results, `DROP PARTITION` cleanup), cut the per-IP ClickHouse round trips that made the 48.6M-IP run take 76 days, keep RDAP inside the registries' acceptable-use limits with a per-registry request budget, make GeoLite2 staleness visible, and then wipe the enrichment tables and the RDAP cache and re-run the whole inventory through the new execution.

**Architecture:** The draft import (`ip_enrichment_input`) appends selections to the open workspace draft with one `INSERT … SELECT` from `selected_ips_sql`, keyed by an `input_id` that starts with the IP's 256-way bucket. The execution (`ip_enrichment_results`) freezes the draft through `defs/common/queue_execution.py`, walks the task bucket by bucket with a live "remaining" query (entries without a result of this execution, each query one primary-key range of `ip_enrichment_results`), resolves each page's RDAP coverage with a fixed number of ClickHouse round trips (one negative-cache read, one trie `dictGet`, one read of the network rows the page needs, one insert of lookup markers) and HTTP only for misses, classifies every miss with the deployed `classify_registration` (registry-level and unallocated answers serve only their address), resolves RIPE misses through the RIPE Database REST search and APNIC misses through APNIC's port-43 whois with `-r` (both without personal data; NIR-managed space falls back to RDAP), every other registry through RDAP, with an optional per-registry request budget (a miss of a registry at its budget is deferred, never failed), stores outcomes through `ResultBuffer`, and finishes with counts derived from results. A check on `ip_enrichment_results` fails when either GeoLite2 file is older than 14 days; the files are updated by hand. The backoffice "Enrich" action becomes "Add to enrichment queue" and processing starts from `/admin/queues/ip-enrichment`. The deploy wipes `ip_enrichment_input`, `ip_enrichment_results` (including the legacy GeoIP import) and the RDAP cache after a backup check and the owner's go-ahead, then queues the full inventory again.

**Tech Stack:** Python 3.14, Dagster 1.13.9, ClickHouse 26.5 (clickhouse-driver), PostgreSQL (psycopg2), whoisit 4.0.4 (RDAP, IANA bootstrap), maxminddb, netaddr, React Router 8 + vitest backoffice, pytest against disposable ClickHouse/PostgreSQL containers.

**Spec:** `services/dagster_v3/docs/superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md`
**Binding decisions:** the owner's decisions file of 2026-09-25 (D1–D10, `scratchpad/ip/decisions.md`) as revised by the owner's evening revision of 2026-09-25 (R1–R5, `scratchpad/ip/revision-clean-rerun.md`), with the two review reports (`scratchpad/ip/review-rdap-geoip.md`, `scratchpad/ip/queue-map.md`) as evidence. Reference implementations on prod: webtech (`defs/webtech/{input,execution,task_assets}.py`, migration `000446`), crawl (`defs/website_crawl/{queue_input,queue_execution}.py`, migration `000448`, plan `2026-09-25-crawl-queue-contract.md`), shared `defs/common/{queue_execution,draft_queue,result_buffer}.py`; registry reference data (plan `2026-09-25-ip-registry-reference-data.md`, migrations `000450`/`000451`, module `defs/ip_registry`, `defs/commoncrawl_rdap/registry.py`).

## What is already live on prod (build on it, do not re-implement)

- **Registry reference data and the data-driven registry-level rule** (main `b33ca08d2`): ClickHouse `000450` (`ip_registry_*` tables/views, `ip_registry_special_trie`, holder blocks, `rdap_network_registry_class` + `_current` + `_derived`, rule view `ip_registry_iana_blocks_rule_current`, `ip_registry_ready`) and `000451` (`rdap_network_segments_current` / `rdap_network_trie` serve only registrations classified `reusable`). Dagster module `defs/ip_registry` (7 loader assets, 7 checks, `ip_registry_refresh_job`, schedule `ip_registry_daily` at 06:05 UTC, RUNNING). On 2026-09-25 evening: `ip_registry_ready = 1`, 15,319 classified networks (15,307 `reusable`, 11 `registry_level`, 1 `unallocated`), trie 16,595 elements.
- `defs/commoncrawl_rdap/registry.py`: `classify_registration(client, network) -> RegistryClassification` (`.registry_class`, `.reusable`, `.clickhouse_values(network_key, classified_at)`), `REGISTRY_CONTEXT_SQL` (one round trip per RDAP miss), `REGISTRY_CLASS_SQL`, `REGISTRY_CLASS_INSERT_SQL`. `RdapEnricher` (`defs/ip_enrichment/enrichment.py:384-395`) and the legacy bucket worker (`commoncrawl_rdap/assets.py:609`) already classify each direct registration, write the class row between the network row and its segments, and never put a non-reusable registration into their in-run caches. **R1: this plan keeps that behaviour inside the per-page resolver and adds no classifier, no segment role and no trie-exclusion migration of its own.**
- ClickHouse ledger on prod: `max(version) WHERE dirty=0` = **452** on 2026-09-25 evening (`000452_corpscout_website_crawl_normalized` belongs to the crawl-normalization workstream; `000449` is `queue_task_sources`). This plan needs **one** migration; it took **000453** and was renumbered to **000456** on 2026-09-26 (final review C1: prod's ledger had reached 454 from the unmerged `codex/crawl-normalization-schema` branch — 452 website_crawl_normalized, 453 brave_draft_queue, 454 retire_brave_legacy_input — with an untracked 455 beside it); re-check main AND prod at merge (Task 8 Step 1).

## Production state this plan starts from (read-only inventory, 2026-09-25 evening)

| Object | Rows / size | Note |
| --- | --- | --- |
| `corpscout.ip_enrichment_input` | 48,596,636 rows, 3.03 GiB, 4 tasks | `4802549d-…` 48,596,631 (the terminated run's task), `a0a328d6-…` 3, `422ce6d8-…` 1, `6f3377b2-…` 1 |
| `processing.tasks` (`ip-enrichment-v1`) | 4 rows | all `selected`, `queue_scope NULL`, never frozen; 0 `input_submissions` |
| `corpscout.ip_enrichment_results` | 9,374,666 rows, 740 MiB | `legacy-geoip-import-v1` 8,291,326 (task/execution `cd603d91-8a85-52f4-9b74-61f95d2f763a`, City/ASN builds of 2026-07-10, RDAP `not_attempted`); `ip-enrichment-v1` 1,083,340 (task `4802549d` 1,083,335 + 5) |
| `corpscout.rdap_networks` | 16,632 rows (15,319 current), 7.71 MiB | ripe 5,893 · apnic 3,727 · arin 2,942 · jpnic 1,115 · afrinic 643 · idnic 402 · twnic 191 · krnic 182 · registro.br 123 · lacnic 101; IPv4 13,305 / IPv6 2,014 |
| `corpscout.rdap_network_segments` | 17,308 rows | trie source |
| `corpscout.rdap_ip_lookup_results` | 227,084 rows, 3.83 MiB | not_global 209,651 · found 16,995 · retryable_error 113 · terminal_error 43 · not_found 5 |
| `corpscout.rdap_network_registry_class` | 15,319 rows | regenerated per miss and by `ip_registry_daily` |
| `corpscout.commoncrawl_ip_addresses` | 79,610,151 rows, 49,296,517 distinct IPs | ≈11.35M IPv4 (arin 5.85M, ripencc 3.15M, apnic 1.34M, lacnic 0.48M, legacy/other 0.33M, afrinic 0.18M by IANA /8) and ≈37.9M IPv6 (RIPE-dominated: 2a02::/16 alone 32.5M rows; 14,858 distinct /32, 71,414 distinct /48, 10,302 distinct /29) |

Last writers: the terminated run's last `ip_enrichment_results` insert was 2026-09-25 14:47 UTC; nothing writes these tables now. The legacy worker `commoncrawl_ip_rdap_networks` has no schedule (`commoncrawl_rdap/definitions.py` registers only the asset).

## Global Constraints

- ClickHouse holds entry lists, PostgreSQL holds coordination. Never `UPDATE`/`DELETE` individual entry rows; the only exception is the submission retry `DELETE … WHERE task_id AND submission_id` while the draft is open, which is why the table keeps `SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1` (D1).
- Entry table `corpscout.ip_enrichment_input`: `ENGINE = MergeTree`, `PARTITION BY task_id`, `ORDER BY (task_id, input_id)`, `task_id String`, required `submission_id`. Plain `MergeTree` rejects `FINAL`; the backoffice reads it ordered by `(task_id, input_id)` without `FINAL` (D1, D8).
- `input_id` = `leftPad(bucket, 3, '0') + ':' + toJSONString(tuple(source_name, source_record_id, ip))`, computed in ClickHouse (`INPUT_ID_SQL`, Task 1) and enforced by a `CHECK`. This is the one deliberate change to today's `toJSONString(tuple(...))` identity: it makes `ORDER BY (task_id, input_id)` walk a task bucket by bucket, so every remaining/completion query anti-joins one primary-key range (`bucket = b`) of `ip_enrichment_results` instead of scanning the whole results table per page (49M-row tasks). Results keep carrying `input_id`; identity is per task. After the wipe (Task 8) no result row with the old identity format exists.
- Results table `corpscout.ip_enrichment_results` and view `ip_enrichment_current` keep their DDL (`000433`); their rows are wiped once, in Task 8, by `TRUNCATE` after the owner's go-ahead (R3). No migration touches them.
- Freeze via `queue_execution.start_execution`: frozen profile `force_rdap, rdap_cache_days, parent_depth, rate_limit_retry_seconds, transient_retry_seconds, ripe_rest, apnic_whois, processor_version`; transport keys `batch_size, max_requests, request_delay_seconds, registry_daily_budgets`; `execution_id` = the original Dagster run id (`default_execution_id = root run id`); an explicit `execution_id` only resumes (D4).
- RDAP cache freshness is bounded by the frozen execution: a network row or negative marker counts as fresh when its time is `>= freshness_cutoff = started_at − rdap_cache_days`; a retryable-error marker is honoured when `retry_after > started_at`. Never `datetime.now()` per lookup. The window has no upper bound at `started_at` on purpose: a network this execution fetched (`fetched_at > started_at`) must stay reusable on resume (D4).
- Remaining = live ClickHouse anti-join per bucket; done means a result row exists; an RDAP error is a published outcome (`completed_with_errors`); failed IPs are retried by a new draft (`retry_failed_task_id`, Task 2) (D4).
- Per page: ONE negative-cache query, ONE trie lookup, ONE read of uncached network rows (never `raw_response`), ONE lookup-marker insert, results through `ResultBuffer`; RDAP HTTP only for misses; a test asserts the ClickHouse query count per page is bounded (D5). Per miss (≈1% of addresses, each already ≥ 1.4 s of HTTP + pacing): the registry-context query of `classify_registration`, then the network row, its class row (unless `unknown`) and its segments, in that order, so coverage and its class are durable before the result that references it (R1). Lookup markers are buffered per page.
- Registry-level rule (R1): the deployed `classify_registration` decides; `registry_level`/`unallocated` registrations are stored with segment role `lookup_result` like every other registration, excluded from `rdap_network_trie` by their class row (`000451`), never added to the in-run `recent` cache, and answer only the queried address; the address's own `found` marker in `rdap_ip_lookup_results` serves it next time. The resolver treats the trie as already excluding non-reusable networks.
- RIPE acceptable-use policy (R4, corrected 2026-09-26 from the published AUP and two live answers): the limit is **1,000 personal data sets per 24 hours per source address** (20,000 only for a proxy registered with the RIPE NCC); queries are "Unlimited" within reasonable use, at most 3 simultaneous connections; a client over the limit is blocked until the end of the calendar day, repeatedly → permanently; the "Anti-avoidance and Connected Persons" clause treats pooling limits across addresses as a violation. RIPE's RDAP `ip` answers carry 1–5 person objects, so they count; the owner needs no personal data, so **RIPE misses use the RIPE Database REST search with `flags=no-referenced`** (`ripe_rest.py`, Task 4): the most specific `inetnum`/`inet6num` without person or role objects, which counts nothing. RDAP stays for the other registries. **APNIC misses use APNIC's port-43 whois with `-r`** (`apnic_whois.py`, Task 4; verified 2026-09-26: the most specific `inetnum`/`inet6num` with contact handles only, no person/role/irt objects; the HTTP gateway `wq.apnic.net` does not honour `-r` and is not used); the holder name is the first `descr` line (only 2.4% of APNIC objects carry `org:`), and an answer that is an NIR's own allocation object (JPNIC, KRNIC, TWNIC, IDNIC, CNNIC, IRINN, VNNIC) falls back to RDAP, which the IANA bootstrap routes to the NIR server. ARIN, LACNIC and AFRINIC stay on RDAP. `registry_daily_budgets` (transport, default `{}`) is an optional rolling 24-hour *request* budget per registry; a miss of a registry at its budget is **deferred** (no result row, no error) and the run waits only when a whole pass resolved nothing else. Proxy egress lanes were considered at the owner's request and dropped on 2026-09-26 (no speed gain, unnecessary with the no-personal-data paths, and pooling a registry's allowance across addresses is the anti-avoidance case above). Every run reports requests and person entities per registry (`rdap_person_entities_by_registry`, expected to have neither a `ripe` nor an `apnic` key).
- GeoLite2 (R2): no MaxMind account; the owner replaces `GeoLite2-City.mmdb`/`GeoLite2-ASN.mmdb` by hand. This plan keeps only the check `geolite2_databases_fresh` (fails when either loaded file's build epoch is older than 14 days; no credentials), the `.env.example:70-71` fix and the manual procedure in the docs. `MaxMindDatabaseResource.database_paths()` resolves the paths at execution time and `ip_enrichment_results` opens the files per run, so a replaced file is used by the next run without a restart; replace by `mv` (rename), never by copying over the open file (maxminddb maps it).
- `ip_enrichment_workflow` is removed; the backoffice "Enrich" adds to the draft (`ip_enrichment_input_job`), processing starts from the queue page (D8).
- Out of scope, listed as follow-ups at the end (D9).
- Destructive migrations carry an inline `throwIf` gate; migration comments must not contain `;`; no `TRUNCATE` inside a migration. This plan's single migration is **000456** (`corpscout_ip_enrichment_queue_contract`; 000453 until 2026-09-26); re-check at merge with `ls clickhouse/migrations | tail -2` and prod `SELECT max(version) FROM corpscout.schema_migrations WHERE dirty=0` (452 on 2026-09-25 evening; the ledger is TinyLog with a dirty=1 and a dirty=0 row per version). If another workstream took 456, renumber both files and every mention in Tasks 1–8.
- Commands from `services/dagster_v3`: `uv run --frozen --no-sync pytest … -q -p no:cacheprovider`, `uv run --frozen --no-sync dg check defs`, `uv run --frozen --no-sync ruff format <touched files>` and `uv run --frozen --no-sync ruff check <touched files>` on touched Python files only. Backoffice from `services/backoffice`: `npm run typecheck` and targeted `npx vitest run <file>` only (the full suite hits prod ClickHouse). Test fixtures that start containers wait for `docker port` and probe with `docker exec … clickhouse-client` before use (already the case in `tests/test_ip_enrichment_input.py:server`). Never call real RDAP servers, IANA or MaxMind from tests.
- Commit by explicit path, never `git add -A` (`searcher/` and other sessions' files are unrelated untracked work). Conventional commits with trailer `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- Do not restart `corpscout-dagster-dev`; deploy by `light_sync`. Task 8 requires the owner's go-ahead before its first step and again before the wipe. Heavy runs (the smoke batch and the full re-run) run on the prod Dagster host, never locally; anything spanning more than one run is a server-side procedure, not a loop on the workstation.

## Task ordering note

Task 1 (migration 000456) precedes the import rewrite because the new table requires `submission_id` and the bucket-prefixed `input_id`, which today's `ip_enrichment_input` does not write; the input suite is red between Task 1 and Task 2 and the results suite between Task 1 and Task 5 (both are rewritten in those tasks), so run only the files each task names. Task 3 (the GeoLite2 check) comes before the resolver and the execution loop because Task 5 imports its `MAX_AGE`/`freshness` to report the build dates of every run. Task 4 (resolver) and Task 5 (loop) are the throughput work; Task 6 the backoffice; Task 7 the docs; Task 8 the deploy with the wipe, the smoke batch and the full re-run.

## File Structure

| File | Change | Responsibility after this plan |
| --- | --- | --- |
| `clickhouse/migrations/000456_corpscout_ip_enrichment_queue_contract.{up,down}.sql` | create | partitioned entry table (gated rebuild while empty) |
| `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/input.py` | rewrite | config, `selected_ips_sql` (+ failed-of-task mode), `INPUT_ID_SQL`, `load_ip_draft`, input asset/job |
| `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/client.py` | modify | `RdapClient.registry_for` (registry of an address from whoisit's bootstrap data, no HTTP) |
| `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/ripe_rest.py` | create | RIPE Database REST search with `no-referenced` (no person/role objects) reshaped into the RDAP document the normalizer reads |
| `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/apnic_whois.py` | create | APNIC port-43 whois with `-r` (contact handles only), RPSL parsing, NIR-object rule, reshaped into the same RDAP document |
| `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py` | rewrite | GeoIP per address, `RdapEnricher.resolve_page` (bounded round trips), classes via `classify_registration`, RIPE via REST, APNIC via whois `-r`, optional per-registry budget |
| `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/results.py` | rewrite | freeze, bucket walk of remaining entries, `ResultBuffer`, budget waits, finish, purge; no workflow job |
| `services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/freshness.py` | create | build-time reader, `freshness`, check `geolite2_databases_fresh`, check-only job |
| `services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/definitions.py` | modify | registers the check and its job |
| `services/dagster_v3/.env.example` | modify | MaxMind directory comment (manual updates, no credentials) |
| `services/dagster_v3/tests/test_ip_enrichment_input.py` | rewrite | draft import against disposable ClickHouse/PostgreSQL; owns the module `server` fixture other suites import |
| `services/dagster_v3/tests/test_ip_enrichment_results.py` | rewrite | resolver (page batching, classes, budget), execution loop, bounded queries, completion/purge |
| `services/dagster_v3/tests/test_ip_enrichment_clickhouse_local.py` | modify | new entry-table layout |
| `services/dagster_v3/tests/test_clickhouse_migrations.py` | modify | `EXPECTED_MIGRATIONS` |
| `services/dagster_v3/tests/test_geolite2_freshness.py` | create | freshness unit tests |
| `services/backoffice/app/lib/ip-enrichment.server.ts` | rewrite | `addIpsToEnrichmentQueue`, `ipEnrichmentQueueSubmission`, selection parsing |
| `services/backoffice/app/routes/admin-ip-enrichment-queue-submission.ts` | create | import status route |
| `services/backoffice/app/routes/admin-ip-addresses.tsx`, `app/routes.ts`, `app/components/admin/queue-import-status.tsx`, `app/lib/queues.ts`, `app/lib/queues.server.ts`, `app/routes/admin-queue.tsx`, `app/components/admin/queue-process-sheet.tsx` | modify | draft semantics for the IP enrichment queue |
| `services/backoffice/tests/{ip-enrichment.server.test.ts, admin-ip-addresses-action.test.ts, queues.server.test.ts, queue-route.test.ts}` | modify | |
| `services/dagster_v3/docs/operations/ip-enrichment-draft-queue.md` | create | operations guide (queue, budget, manual GeoLite2 update, the 2026-09 clean re-run) |
| `services/dagster_v3/docs/ip-enrichment-schema.md`, `services/backoffice/docs/queues.md`, `defs/commoncrawl_geoip/docs/commoncrawl_geoip-design.md`, `defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md`, `services/dagster_v3/docs/deployment-runbook.md`, `services/dagster_v3/docs/operations/ip-registry-reference-data.md` | modify | |
| spec status line | modify (Task 8) | |

Removed from the previous version of this plan (R1–R3): the registry-level classifier and migration 449 (`registry_level_reason`, `REGISTRY_LEVEL_SQL`, segment role `registry_level`), the GeoLite2 download asset/job/weekly schedule and `MAXMIND_ACCOUNT_ID`/`MAXMIND_LICENSE_KEY`, and the ~170k-IP remediation draft (superseded by the clean re-run). Corrected on 2026-09-26: the first draft of R4 assumed a 20,000-request RIPE allowance and a default budget of 18,000 requests/day; the AUP limit is 1,000 *personal data sets* per address, which the REST path avoids entirely (Task 4). Also considered and dropped on 2026-09-26: bulk RIR database dumps (terms of use, NIR precision) and proxy egress lanes with per-lane budgets (no speed gain, anti-avoidance clause).

---
### Task 1: Partitioned entry table (migration 000456) and the entry-table contract test

`task_id` becomes `String` (as in 446/448: the partition key and the shared `purge_completed_inputs` pass `DROP PARTITION %(task)s` as a string). The results table keeps `task_id UUID`; comparisons with a string parameter already work today (`results.py:83`). The migration rebuilds the table only while it is empty; on prod that is true after Task 8's wipe.

**Files:**
- Create: `clickhouse/migrations/000456_corpscout_ip_enrichment_queue_contract.up.sql`, `…down.sql`
- Modify: `services/dagster_v3/tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS`, after its last entry)
- Modify: `services/dagster_v3/tests/test_ip_enrichment_input.py:88-96` (`server` fixture) and append the contract test
- Modify: `services/dagster_v3/tests/test_ip_enrichment_clickhouse_local.py:12-13, 21-27, 84-98, 101, 128-134`
- Modify: `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/input.py:21` (add `INPUT_ID_SQL`)

**Interfaces:**
- Produces: `corpscout.ip_enrichment_input(task_id String, input_id String, ip String, ip_version UInt8 MATERIALIZED, bucket UInt16 MATERIALIZED, source_name LowCardinality(String), source_record_id String, source_run_id String, submission_id String, observed_at Nullable(DateTime64(6,'UTC')), submitted_at DateTime64(6,'UTC'))`, `ENGINE = MergeTree PARTITION BY task_id ORDER BY (task_id, input_id)`, constraints `valid_identity` (non-empty `task_id`/`submission_id`, `input_id` equals the bucket-prefixed JSON tuple), `valid_source`, `canonical_ip`.
- Produces (module `dagster_v3.defs.ip_enrichment.input`): `INPUT_ID_SQL: str` with `{ip}`, `{source}`, `{record}` placeholders.
- The `server` fixture of `tests/test_ip_enrichment_input.py` (imported by `test_ip_enrichment_results.py`, `test_ip_registry.py`, `test_webtech_input.py`, `test_webtech_draft_execution.py`, `test_domains_inventory.py`, `test_web_inventory.py`, `test_domains_search.py`) applies 000433 then 000456; its name and shape are unchanged.

- [ ] **Step 1: Confirm the migration number is free**

Run: `ls clickhouse/migrations | tail -2`
Expected: the highest number is `000452` (`000452_corpscout_website_crawl_normalized` — another session's files; if they are not on your branch yet, the highest is `000451`). Either way `000453` is free.

Run: `ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT max(version) FROM corpscout.schema_migrations WHERE dirty=0"'`
Expected: `452`. If another workstream took 453 on main or prod, renumber both files of this task and every mention in Tasks 2–8.

*(2026-09-26: this happened — the codex branch took 453–455, so the migration is now 000456; the file names below are updated. `EXPECTED_MIGRATIONS` lists only the files on this branch; 452–455 are added when the codex branch is merged first, Task 8 Step 1.)*

- [ ] **Step 2: Write the migrations**

`clickhouse/migrations/000456_corpscout_ip_enrichment_queue_contract.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Shared queue contract for IP enrichment drafts: one partition per task so cleanup is
-- DROP PARTITION, sorted by task and input_id, and a required submission_id so a retried
-- import replaces only its own rows. input_id starts with the address's 256-way bucket
-- (leftPad(bucket, 3, '0') then ':' then the JSON tuple of source, record and IP), so a
-- task is walked bucket by bucket and each remaining or completion query joins exactly
-- one primary-key range of ip_enrichment_results. The table is rebuilt only while empty
-- (the 2026-09 clean re-run truncates it first, by hand, after the owner's go-ahead).
-- The mutation-pool setting stays because the submission retry deletes its own rows with
-- a lightweight DELETE.
SELECT throwIf(count() > 0, 'ip_enrichment_input must be empty before its layout changes')
FROM corpscout.ip_enrichment_input;

DROP TABLE IF EXISTS corpscout.ip_enrichment_input;

CREATE TABLE corpscout.ip_enrichment_input
(
    task_id String,
    input_id String,
    ip String,
    ip_version UInt8 MATERIALIZED if(isIPv4String(ip), toUInt8(4), toUInt8(6)),
    bucket UInt16 MATERIALIZED toUInt16(cityHash64(ip) % 256),
    source_name LowCardinality(String),
    source_record_id String,
    source_run_id String,
    submission_id String,
    observed_at Nullable(DateTime64(6, 'UTC')),
    submitted_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_identity CHECK notEmpty(task_id) AND notEmpty(submission_id)
        AND input_id = concat(leftPad(toString(toUInt16(cityHash64(ip) % 256)), 3, '0'), ':', toJSONString(tuple(toString(source_name), source_record_id, ip))),
    CONSTRAINT valid_source CHECK notEmpty(trimBoth(source_name))
        AND notEmpty(source_record_id) AND position(source_record_id, char(0)) = 0,
    CONSTRAINT canonical_ip CHECK
        ifNull(ip = toString(toIPv4OrNull(ip)), 0)
        OR ifNull(ip = toString(toIPv6OrNull(ip)), 0)
)
ENGINE = MergeTree
PARTITION BY task_id
ORDER BY (task_id, input_id)
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1;
```

`clickhouse/migrations/000456_corpscout_ip_enrichment_queue_contract.down.sql` (the 000433 layout):

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

SELECT throwIf(count() > 0, 'ip_enrichment_input must be empty before its layout changes')
FROM corpscout.ip_enrichment_input;

DROP TABLE IF EXISTS corpscout.ip_enrichment_input;

CREATE TABLE corpscout.ip_enrichment_input
(
    task_id UUID,
    input_id String,
    ip String,
    ip_version UInt8 MATERIALIZED if(isIPv4String(ip), toUInt8(4), toUInt8(6)),
    bucket UInt16 MATERIALIZED toUInt16(cityHash64(ip) % 256),
    source_name LowCardinality(String),
    source_record_id String,
    source_run_id String,
    observed_at Nullable(DateTime64(6, 'UTC')),
    submitted_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_input CHECK notEmpty(trimBoth(input_id))
        AND position(input_id, char(0)) = 0 AND notEmpty(trimBoth(source_name)),
    CONSTRAINT canonical_ip CHECK
        ifNull(ip = toString(toIPv4OrNull(ip)), 0)
        OR ifNull(ip = toString(toIPv6OrNull(ip)), 0)
)
ENGINE = MergeTree
ORDER BY (input_id, task_id);
```

In `services/dagster_v3/tests/test_clickhouse_migrations.py` append `"000456_corpscout_ip_enrichment_queue_contract",` as the last entry of `EXPECTED_MIGRATIONS` (after `"000452_corpscout_website_crawl_normalized",` when that entry is present on your branch, else after `"000451_corpscout_rdap_trie_registry_class_exclusion",`).

- [ ] **Step 3: Apply it in the shared fixture and write the contract test**

In `services/dagster_v3/tests/test_ip_enrichment_input.py` replace lines 88-96 (the `with resource.get_connection() as client:` block of `server`) with:

```python
        with resource.get_connection() as client:
            migrations = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
            for name in (
                "000433_corpscout_ip_enrichment.up.sql",
                "000456_corpscout_ip_enrichment_queue_contract.up.sql",
            ):
                for statement in (migrations / name).read_text(encoding="utf-8").split(";"):
                    if statement.strip():
                        client.execute(statement)
            yield client, resource
```

Append to the same file (the rest of the file is rewritten in Task 2; this test survives unchanged):

```python
def test_entry_table_follows_the_queue_contract(database):
    from clickhouse_driver.errors import ServerException

    from dagster_v3.defs.ip_enrichment.input import INPUT_ID_SQL

    client, _ = database
    assert client.execute(
        "SELECT engine, partition_key, sorting_key FROM system.tables WHERE database='corpscout' AND name='ip_enrichment_input'"
    ) == [("MergeTree", "task_id", "task_id, input_id")]
    identity = INPUT_ID_SQL.format(ip="'8.8.8.8'", source="'manual'", record="'8.8.8.8'")
    # input_id is the bucket-prefixed identity computed in ClickHouse, nothing else.
    with pytest.raises(ServerException, match="valid_identity"):
        client.execute(
            f"INSERT INTO {INPUT_RELATION} (task_id,input_id,ip,source_name,source_record_id,source_run_id,submission_id) VALUES",
            [("task", "017:x", "8.8.8.8", "manual", "8.8.8.8", "run", "submission")],
        )
    # Every row names its submission; the retry delete relies on it.
    with pytest.raises(ServerException, match="valid_identity"):
        client.execute(
            f"INSERT INTO {INPUT_RELATION} (task_id,input_id,ip,source_name,source_record_id,source_run_id) "
            f"SELECT 'task', {identity}, '8.8.8.8', 'manual', '8.8.8.8', 'run'"
        )
    client.execute(
        f"INSERT INTO {INPUT_RELATION} (task_id,input_id,ip,source_name,source_record_id,source_run_id,submission_id) "
        f"SELECT 'task', {identity}, '8.8.8.8', 'manual', '8.8.8.8', 'run', 'submission'"
    )
    [(input_id, bucket)] = client.execute(f"SELECT input_id, bucket FROM {INPUT_RELATION}")
    assert input_id == f"{bucket:03d}:" + '["manual","8.8.8.8","8.8.8.8"]'
```

`INPUT_ID_SQL` does not exist until Task 2; add it now to `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/input.py` after `PROCESSOR_VERSION` (line 21) so this task is green on its own:

```python
# The entry identity, computed where the entries live: the address's 256-way bucket first,
# so a task is walked bucket by bucket, then the JSON tuple of source, record and IP.
# Migration 000456 enforces the same expression in its valid_identity CHECK.
INPUT_ID_SQL = (
    "concat(leftPad(toString(toUInt16(cityHash64({ip}) % 256)), 3, '0'), ':', "
    "toJSONString(tuple({source}, {record}, {ip})))"
)
```

- [ ] **Step 4: Update the clickhouse-local test to the new layout**

In `services/dagster_v3/tests/test_ip_enrichment_clickhouse_local.py`:

Replace lines 12-13 with:

```python
MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
MIGRATION = "000433_corpscout_ip_enrichment"
QUEUE_MIGRATION = "000456_corpscout_ip_enrichment_queue_contract"
IDENTITY = "concat(leftPad(toString(toUInt16(cityHash64(ip) % 256)), 3, '0'), ':', toJSONString(tuple(source_name, source_record_id, ip)))"
```

Replace the input insert and count (lines 21-27) with:

```sql
    INSERT INTO corpscout.ip_enrichment_input
        (task_id, input_id, ip, source_name, source_record_id, source_run_id, submission_id)
    SELECT task_id, {IDENTITY}, ip, source_name, source_record_id, 'run', 'submission'
    FROM (SELECT '00000000-0000-0000-0000-000000000001' AS task_id, '8.8.8.8' AS ip, 'dns' AS source_name, '1' AS source_record_id
          UNION ALL
          SELECT '00000000-0000-0000-0000-000000000002', '8.8.8.8', 'commoncrawl', '2');
    SELECT count(), uniqExact(ip), uniqExact(source_name), countIf(input_id LIKE '___:%')
    FROM corpscout.ip_enrichment_input FORMAT JSONCompactEachRow;
```

(the `sql` string becomes an f-string: `sql = f"""…"""`; escape nothing else — it contains no braces). At the end of `sql`, before the closing `"""`, add the test-only cleanup so the gated down migrations can run:

```sql
    TRUNCATE TABLE corpscout.ip_enrichment_input;
```

Replace lines 84-98 (the `subprocess.run` input) with:

```python
    queue_up = (MIGRATIONS / f"{QUEUE_MIGRATION}.up.sql").read_text()
    queue_down = (MIGRATIONS / f"{QUEUE_MIGRATION}.down.sql").read_text()
    result = subprocess.run(
        clickhouse_local_command(),
        input=up
        + queue_up
        + sql
        + queue_down
        + down
        + up
        + queue_up
        + queue_down
        + down
        + """
        SELECT count() FROM system.tables WHERE database='corpscout' FORMAT JSONCompactEachRow;
        """,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
```

and the first expected row (line 101) `[2, 1, 2],` becomes `[2, 1, 2, 2],`.

In `test_ip_enrichment_rejects_invalid_or_noncanonical_ips` replace lines 128-134 with:

```python
    up = (MIGRATIONS / f"{MIGRATION}.up.sql").read_text() + (
        MIGRATIONS / f"{QUEUE_MIGRATION}.up.sql"
    ).read_text()
    if table == "ip_enrichment_input":
        insert = f"""INSERT INTO corpscout.{table}
        (task_id, input_id, ip, source_name, source_record_id, source_run_id, submission_id)
        SELECT 'task', {IDENTITY}, ip, source_name, source_record_id, 'run', 'submission'
        FROM (SELECT {literal(ip)} AS ip, 'dns' AS source_name, 'record' AS source_record_id);"""
    else:
        insert = f"""INSERT INTO corpscout.{table} (ip, result_id)
        VALUES ({literal(ip)}, '00000000-0000-0000-0000-000000000001');"""
```

- [ ] **Step 5: Run the migration suites**

Run: `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py tests/test_ip_enrichment_clickhouse_local.py tests/test_legacy_geoip_migration.py tests/test_ip_enrichment_input.py::test_entry_table_follows_the_queue_contract tests/test_ip_registry.py -q -p no:cacheprovider`
Expected: all pass. (`test_legacy_geoip_migration.py` only applies 000433 and touches the results table; `test_ip_registry.py` shares the `server` fixture and must still be green.) The other tests of `test_ip_enrichment_input.py` and of `test_ip_enrichment_results.py` are red until Tasks 2 and 5.

- [ ] **Step 6: Commit**

```bash
git add clickhouse/migrations/000456_corpscout_ip_enrichment_queue_contract.up.sql clickhouse/migrations/000456_corpscout_ip_enrichment_queue_contract.down.sql services/dagster_v3/tests/test_clickhouse_migrations.py services/dagster_v3/tests/test_ip_enrichment_input.py services/dagster_v3/tests/test_ip_enrichment_clickhouse_local.py services/dagster_v3/src/dagster_v3/defs/ip_enrichment/input.py
git commit -m "feat(clickhouse): partition ip_enrichment_input by task with bucket-prefixed input ids and a required submission_id

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---
### Task 2: Draft import through the shared draft queue

Today `ip_enrichment_input` (`input.py:244-357`) uses `store.prepare_selection`/`finish_selection` (legacy, `queue_scope NULL`), a task-wide `ALTER TABLE … DELETE` retry and no receipts. It becomes the crawl-style import (`website_crawl/queue_input.py:41-205`): `draft_queue.find_draft` → `prepare_submission` → `KILL QUERY` + `DELETE … WHERE task_id AND submission_id` + one `INSERT … SELECT` → `finish_submission`, `fail_submission` on any error. `input_count` is the number of rows this submission added to the draft (after dedup against the draft), not a separate evaluation of the selection: evaluating a 49M-row selection twice costs minutes and the receipt only needs a count. `source_info.unique_ips` disappears (a `uniqExact` over 49M strings is not worth a freeze-time query); the materialization reports `input_count` and `total`. No draft size cap: entries cost nothing in PostgreSQL. A third selection mode, `retry_failed_task_id`, queues the addresses whose result in that task has any component in an error status (D4's "failed IPs of task X").

**Files:**
- Rewrite: `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/input.py`
- Rewrite: `services/dagster_v3/tests/test_ip_enrichment_input.py` (keep `server`, `database`, `materialize`, the contract test)

**Interfaces:**
- Produces (module `dagster_v3.defs.ip_enrichment.input`):
  - `INPUT_RELATION`, `RESULT_RELATION = "corpscout.ip_enrichment_results"`, `PROCESSOR_VERSION = "ip-enrichment-v1"`, `ERROR_STATUSES = ("retryable_error", "terminal_error")`, `INPUT_ID_SQL`, `bucket_prefix(bucket: int) -> str` (`"017:"`).
  - `IpEnrichmentInputConfig` — today's fields plus `submission_id: str | None`, `queue_scope: str = "workspace"`, `retry_failed_task_id: str | None`; exactly one of `ips`, `source_relation`, `retry_failed_task_id`.
  - `selected_ips_sql(config) -> tuple[str, dict]` — columns `ip, source_record_id, observed_at` (unchanged shape; new mode added).
  - `load_ip_draft(config, submission_id: str, store: ProcessingStore, clickhouse: ClickhouseResource, *, run_id: str) -> dict` with keys `task_id, submission_id, input_count, total`.
  - Asset `ip_enrichment_input`, job `ip_enrichment_input_job` (names unchanged).
- Removes: the `ClickHouseInputQueue` use, `store.prepare_selection`/`finish_selection` for IPs, `processing/task_id`-derived task ids (the run tag `processing/task_id` is still written after import for the backoffice).

- [ ] **Step 1: Write the failing tests**

Replace everything in `services/dagster_v3/tests/test_ip_enrichment_input.py` after the `database` fixture (keep lines 1-110 as modified in Task 1, and keep `test_entry_table_follows_the_queue_contract`) with:

```python
def materialize(resource, dsn, **config):
    return dg.materialize(
        [ip_enrichment_input],
        resources={
            "clickhouse": resource,
            "processing": ProcessingResource(postgres_url=dsn),
        },
        run_config={"ops": {"ip_enrichment_input": {"config": config}}},
    )


def metadata(result):
    return {
        key: value.value
        for key, value in result.asset_materializations_for_node("ip_enrichment_input")[0]
        .metadata.items()
    }


def scope():
    return "scope-" + uuid4().hex


@pytest.mark.parametrize(
    "config",
    [
        {},
        {"ips": []},
        {"ips": ["8.8.8.8/24"]},
        {"ips": ["fe80::1%eth0"]},
        {"ips": ["bad"]},
        {"ips": ["8.8.8.8"], "source_relation": "corpscout.ips"},
        {"ips": ["8.8.8.8"], "filters": {"country": ["US"]}},
        {"source_relation": "corpscout.ips"},
        {"source_relation": INPUT_RELATION, "select_all": True},
        {"source_relation": "corpscout.ips; DROP TABLE x", "select_all": True},
        {"source_relation": "corpscout.ips", "ip_column": "ip`", "select_all": True},
        {"source_relation": "corpscout.ips", "filters": {"ip": []}},
        {"ips": ["8.8.8.8"], "max_rows": 0},
        {"ips": ["8.8.8.8"], "submission_id": "not-a-uuid"},
        {"ips": ["8.8.8.8"], "queue_scope": "  "},
        {"retry_failed_task_id": "not-a-uuid"},
        {"retry_failed_task_id": str(uuid4()), "ips": ["8.8.8.8"]},
        {"retry_failed_task_id": str(uuid4()), "filters": {"country": ["US"]}},
        {"retry_failed_task_id": str(uuid4()), "source_relation": "corpscout.ips"},
    ],
)
def test_invalid_or_ambiguous_selection_is_rejected(config):
    with pytest.raises(ValidationError):
        IpEnrichmentInputConfig(**config)


def test_explicit_ips_are_canonical_deduplicated_and_appended_to_one_draft(database, store):
    client, resource = database
    queue, dsn = store
    space = scope()
    submission = str(uuid4())
    ips = [
        " 8.8.8.8 ",
        "8.8.8.8",
        "2001:4860:4860:0:0:0:0:8888",
        "127.0.0.1",
        "::ffff:0808:0808",
    ]
    first = metadata(
        materialize(resource, dsn, queue_scope=space, submission_id=submission, ips=ips, source_name="manual-test")
    )
    assert (first["input_count"], first["total"]) == (4, 4)
    assert client.execute(
        f"SELECT ip, ip_version, source_name, source_record_id, submission_id FROM {INPUT_RELATION} ORDER BY ip"
    ) == [
        ("127.0.0.1", 4, "manual-test", "127.0.0.1", submission),
        ("2001:4860:4860::8888", 6, "manual-test", "2001:4860:4860::8888", submission),
        ("8.8.8.8", 4, "manual-test", "8.8.8.8", submission),
        ("::ffff:8.8.8.8", 6, "manual-test", "::ffff:8.8.8.8", submission),
    ]
    assert client.execute(
        f"SELECT count() FROM {INPUT_RELATION} WHERE input_id LIKE '___:%' AND toString(task_id) = %(task)s",
        {"task": first["task_id"]},
    ) == [(4,)]
    rows_before = client.execute(f"SELECT * FROM {INPUT_RELATION} ORDER BY input_id")
    # The same submission with the same selection is a no-op, even with a changed source.
    replay = metadata(
        materialize(resource, dsn, queue_scope=space, submission_id=submission, ips=list(reversed(ips)), source_name="manual-test")
    )
    assert replay == first
    assert client.execute(f"SELECT * FROM {INPUT_RELATION} ORDER BY input_id") == rows_before
    # Another submission appends to the same draft; overlapping addresses are kept once.
    second = metadata(
        materialize(resource, dsn, queue_scope=space, ips=["8.8.8.8", "1.1.1.1"], source_name="manual-test")
    )
    assert second["task_id"] == first["task_id"]
    assert (second["input_count"], second["total"]) == (1, 5)
    task = queue.task(first["task_id"])
    assert task["status"] == "draft" and task["queue_scope"] == space and task["total"] == 5
    with queue.transaction() as cursor:
        cursor.execute("SELECT count(*) AS n FROM processing.items")
        assert cursor.fetchone()["n"] == 0
        cursor.execute(
            "SELECT status, input_count, selection_config->'ips' AS ips FROM processing.input_submissions WHERE submission_id=%s",
            (submission,),
        )
        receipt = cursor.fetchone()
    assert receipt["status"] == "completed" and receipt["input_count"] == 4
    assert set(receipt["ips"]) == {"count", "sha256"}  # bulk values stay out of PostgreSQL
    with pytest.raises(ValueError, match="different selection"):
        materialize(resource, dsn, queue_scope=space, submission_id=submission, ips=["9.9.9.9"], source_name="manual-test")


def test_table_filters_final_and_source_lineage(database, store):
    client, resource = database
    queue, dsn = store
    client.execute("""INSERT INTO corpscout.ip_source_test VALUES
        ('a', '8.8.8.8', 1, 'US', '2026-09-01', 1),
        ('a', '8.8.8.8', 1, 'US', '2026-09-02', 2),
        ('b', '8.8.8.8', 1, 'US', '2026-09-01', 1),
        ('a', '2001:4860:4860:0:0:0:0:8888', 1, 'US', '2026-09-01', 1),
        ('c', '1.1.1.1', 1, 'US', '2026-09-01', 1),
        ('c', '1.1.1.1', 0, 'US', '2026-09-02', 2),
        ('d', '9.9.9.9', 1, 'DE', '2026-09-01', 1),
        ('e', 'bad', 1, 'US', '2026-09-01', 1),
        ('f', NULL, 1, 'US', '2026-09-01', 1)""")
    space = scope()
    config = dict(
        queue_scope=space,
        source_relation="corpscout.ip_source_test",
        ip_column="address",
        source_record_id_column="record_id",
        observed_at_column="observed_at",
        source_final=True,
        filters={"active": ["1"], "country": ["US"]},
    )
    submission = str(uuid4())
    first = metadata(materialize(resource, dsn, submission_id=submission, **config))
    assert client.execute(
        f"SELECT ip, source_record_id, toString(observed_at), source_name FROM {INPUT_RELATION} ORDER BY ip, source_record_id"
    ) == [
        ("2001:4860:4860::8888", "a", "2026-09-01 00:00:00.000000", "corpscout.ip_source_test"),
        ("8.8.8.8", "a", "2026-09-02 00:00:00.000000", "corpscout.ip_source_test"),
        ("8.8.8.8", "b", "2026-09-01 00:00:00.000000", "corpscout.ip_source_test"),
    ]
    client.execute(
        "INSERT INTO corpscout.ip_source_test VALUES ('new', '4.4.4.4', 1, 'US', '2026-09-01', 1)"
    )
    # A completed receipt is not re-evaluated; a new submission sees the new row.
    assert metadata(materialize(resource, dsn, submission_id=submission, **config)) == first
    assert queue.task(first["task_id"])["total"] == 3
    later = metadata(materialize(resource, dsn, **config))
    assert (later["input_count"], later["total"]) == (1, 4)
    # A bounded selection in another scope freezes only the first two distinct submissions.
    bounded = metadata(materialize(resource, dsn, **{**config, "queue_scope": scope(), "max_rows": 2}))
    assert (bounded["input_count"], bounded["total"]) == (2, 2)
    empty = metadata(
        materialize(resource, dsn, **{**config, "queue_scope": scope(), "filters": {"country": ["US') OR 1=1 --"]}})
    )
    assert (empty["input_count"], empty["total"]) == (0, 0)
    assert queue.task(empty["task_id"])["status"] == "draft"


def test_retry_replaces_only_its_own_submission_rows(database, store, monkeypatch):
    from clickhouse_driver import Client

    from dagster_v3.defs.common import draft_queue

    client, resource = database
    queue, dsn = store
    space = scope()
    kept = metadata(materialize(resource, dsn, queue_scope=space, ips=["1.1.1.1"]))
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if query.lstrip().startswith(f"INSERT INTO {INPUT_RELATION}") and not interrupted:
            interrupted = True
            raise ConnectionError("lost insert acknowledgement")
        return value

    monkeypatch.setattr(Client, "execute", lost_ack)
    submission = str(uuid4())
    with pytest.raises(ConnectionError, match="acknowledgement"):
        materialize(resource, dsn, queue_scope=space, submission_id=submission, ips=["8.8.8.8", "1.1.1.1"])
    assert draft_queue.submission(queue, submission)["status"] == "failed"
    # The lost insert did land; the retry deletes only this submission's rows and re-inserts them.
    result = metadata(materialize(resource, dsn, queue_scope=space, submission_id=submission, ips=["8.8.8.8", "1.1.1.1"]))
    assert result["task_id"] == kept["task_id"] and (result["input_count"], result["total"]) == (1, 2)
    assert client.execute(
        f"SELECT ip, submission_id FROM {INPUT_RELATION} ORDER BY ip"
    ) == [("1.1.1.1", kept["submission_id"]), ("8.8.8.8", submission)]
    assert draft_queue.submission(queue, submission)["status"] == "completed"


def test_missing_source_column_fails_the_submission_without_rows(database, store):
    from dagster_v3.defs.common import draft_queue

    client, resource = database
    queue, dsn = store
    submission = str(uuid4())
    with pytest.raises(ValueError, match="source is missing columns: missing"):
        materialize(
            resource,
            dsn,
            queue_scope=scope(),
            submission_id=submission,
            source_relation="corpscout.ip_source_test",
            ip_column="missing",
            select_all=True,
        )
    assert draft_queue.submission(queue, submission)["status"] == "failed"
    assert client.execute(f"SELECT count() FROM {INPUT_RELATION}") == [(0,)]


@pytest.mark.parametrize(
    ("search", "excluded", "expected"),
    [
        ("8.8.", ["8.8.4.4"], ["8.8.8.8"]),
        ("8.8.8.8", [], ["8.8.8.8"]),
        ("2001:4860:0:0:0:0:0:8888", [], ["2001:4860::8888"]),
        ("", ["2001:4860:0:0:0:0:0:8888"], ["8.8.4.4", "8.8.8.8", "9.9.9.9"]),
    ],
)
def test_inventory_search_and_exclusions_match_all_pages(
    database, store, search, excluded, expected
):
    client, resource = database
    client.execute("""INSERT INTO corpscout.ip_source_test VALUES
        ('a', '8.8.8.8', 1, 'US', '2026-09-01', 1),
        ('b', '8.8.8.8', 1, 'US', '2026-09-02', 1),
        ('c', '8.8.4.4', 1, 'US', '2026-09-01', 1),
        ('d', '9.9.9.9', 1, 'US', '2026-09-01', 1),
        ('e', '2001:4860::8888', 1, 'US', '2026-09-01', 1),
        ('f', '8.8.0.1', 0, 'US', '2026-09-01', 1)""")
    assert materialize(
        resource,
        store[1],
        queue_scope=scope(),
        source_relation="corpscout.ip_source_test",
        ip_column="address",
        observed_at_column="observed_at",
        filters={"active": ["1"]},
        select_all=True,
        ip_search=search,
        excluded_ips=excluded,
    ).success
    assert [
        row[0] for row in client.execute(f"SELECT ip FROM {INPUT_RELATION} ORDER BY ip")
    ] == expected


@pytest.mark.parametrize(
    "config",
    [
        {"ips": ["8.8.8.8"], "ip_search": "8.8."},
        {"ips": ["8.8.8.8"], "excluded_ips": ["8.8.8.8"]},
        {"source_relation": "corpscout.ips", "ip_search": "%' OR 1=1"},
        {
            "source_relation": "corpscout.ips",
            "select_all": True,
            "excluded_ips": ["bad"],
        },
    ],
)
def test_invalid_inventory_filters(config):
    with pytest.raises(ValidationError):
        IpEnrichmentInputConfig(**config)


def test_failed_results_of_a_task_can_be_queued_again(database, store):
    client, resource = database
    failed_task = str(uuid4())
    completed = datetime(2026, 9, 1, tzinfo=UTC)
    client.execute(
        "INSERT INTO corpscout.ip_enrichment_results (ip, result_id, task_id, execution_id, input_id, completed_at, city_lookup_status, asn_lookup_status, rdap_lookup_status) VALUES",
        [
            ("8.8.8.8", str(uuid4()), failed_task, failed_task, "a", completed, "found", "found", "retryable_error"),
            ("1.1.1.1", str(uuid4()), failed_task, failed_task, "b", completed, "found", "found", "found"),
            ("9.9.9.9", str(uuid4()), str(uuid4()), failed_task, "c", completed, "terminal_error", "found", "found"),
        ],
    )
    result = metadata(materialize(resource, store[1], queue_scope=scope(), retry_failed_task_id=failed_task))
    assert (result["input_count"], result["total"]) == (1, 1)
    assert client.execute(f"SELECT ip, source_name, source_record_id FROM {INPUT_RELATION}") == [
        ("8.8.8.8", "retry:" + failed_task, "8.8.8.8")
    ]


def test_new_submissions_after_start_form_the_next_draft(database, store):
    from dagster_v3.defs.common import queue_execution
    from dagster_v3.defs.ip_enrichment.input import PROCESSOR_VERSION

    _, resource = database
    queue, dsn = store
    space = scope()
    first = metadata(materialize(resource, dsn, queue_scope=space, ips=["8.8.8.8"]))
    with queue.selection_lock(first["task_id"]):
        queue_execution.start_execution(
            queue,
            task_id=first["task_id"],
            processor=PROCESSOR_VERSION,
            profile={},
            execution_id=None,
            freshness_days=30,
            run_id=str(uuid4()),
            snapshot=lambda: ({"relation": INPUT_RELATION, "total": 1}, 1),
        )
    second = metadata(materialize(resource, dsn, queue_scope=space, ips=["1.1.1.1"]))
    assert second["task_id"] != first["task_id"]
    with pytest.raises(ValueError, match="open draft"):
        materialize(resource, dsn, queue_scope=space, task_id=first["task_id"], ips=["1.1.1.1"])
```

(The import block at the top keeps `dg`, `pytest`, `ClickhouseResource`, `ValidationError`, `ProcessingResource`, `INPUT_RELATION`, `IpEnrichmentInputConfig`, `ip_enrichment_input`, the clickhouse_local helpers and the store fixtures; add `from datetime import UTC, datetime`; delete `from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue`. The `database` fixture keeps truncating `INPUT_RELATION`; add `client.execute("TRUNCATE TABLE corpscout.ip_enrichment_results")` next to it so the retry-failed test starts clean.)

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_ip_enrichment_input.py -q -p no:cacheprovider`
Expected: FAIL — `ValidationError` for `submission_id`/`queue_scope`/`retry_failed_task_id` ("extra fields not permitted") and `KeyError: 'input_count'`.

- [ ] **Step 3: Rewrite `input.py`**

Replace the whole of `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/input.py` with:

```python
"""Append IP selections to the open IP enrichment draft (shared queue contract).

A submission selects addresses from an explicit list, a filtered ClickHouse relation
or the failed results of an earlier task, normalizes them in ClickHouse and appends
the ones the draft does not hold yet. Nothing is frozen here; the results asset
freezes the draft when it starts.
"""

import hashlib
import json
import re
from ipaddress import IPv6Address, ip_address
from typing import Self
from uuid import UUID

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator, model_validator

from dagster_v3.defs.common import draft_queue
from dagster_v3.defs.common.clickhouse_queue import validate_relation
from dagster_v3.defs.common.processing import ProcessingResource, ProcessingStore

INPUT_RELATION = "corpscout.ip_enrichment_input"
RESULT_RELATION = "corpscout.ip_enrichment_results"
PROCESSOR_VERSION = "ip-enrichment-v1"
ERROR_STATUSES = ("retryable_error", "terminal_error")
# The entry identity, computed where the entries live: the address's 256-way bucket first,
# so a task is walked bucket by bucket, then the JSON tuple of source, record and IP.
# Migration 000456 enforces the same expression in its valid_identity CHECK.
INPUT_ID_SQL = (
    "concat(leftPad(toString(toUInt16(cityHash64({ip}) % 256)), 3, '0'), ':', "
    "toJSONString(tuple({source}, {record}, {ip})))"
)


def bucket_prefix(bucket: int) -> str:
    """Every input_id of ``bucket`` starts with this; ``bucket_prefix(bucket + 1)`` ends the range."""
    return f"{bucket:03d}:"


class IpEnrichmentInputConfig(dg.Config):
    task_id: str | None = Field(
        default=None,
        description="Open draft to append to. Omit to use the scope's open draft.",
    )
    submission_id: str | None = Field(
        default=None,
        description="Stable receipt UUID; retry a failed import with the same value.",
    )
    queue_scope: str = Field(default="workspace", min_length=1)
    ips: list[str] = Field(
        default_factory=list,
        description="Explicit IPv4/IPv6 addresses. Use one of ips, source_relation, retry_failed_task_id.",
    )
    source_relation: str | None = Field(
        default=None,
        description="Source ClickHouse table/view, as corpscout.table.",
    )
    retry_failed_task_id: str | None = Field(
        default=None,
        description="Queue the addresses whose result in this task has a City, ASN or RDAP error.",
    )
    ip_column: str = "ip"
    source_record_id_column: str | None = Field(
        default=None,
        description="Optional source record identity. Defaults to the canonical IP.",
    )
    observed_at_column: str | None = Field(
        default=None,
        description="Optional source observation timestamp column.",
    )
    filters: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Exact matches: OR within each value list, AND between columns.",
    )
    ip_search: str = Field(
        default="", description="Exact IP or literal IP prefix to select."
    )
    excluded_ips: list[str] = Field(default_factory=list)
    source_final: bool = False
    select_all: bool = False
    max_rows: int | None = Field(
        default=None,
        ge=1,
        description="Limit distinct source-record/IP submissions, sorted by IP and record ID.",
    )
    source_name: str | None = Field(
        default=None,
        description="Provenance label. Defaults to source_relation, retry:<task> or manual.",
    )
    source_run_id: str | None = Field(
        default=None,
        description="Original source run ID. Defaults to this Dagster run ID.",
    )

    @field_validator("task_id", "submission_id", "retry_failed_task_id")
    @classmethod
    def valid_uuid(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

    @field_validator("queue_scope")
    @classmethod
    def valid_scope(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("queue_scope must not be blank")
        return value

    @field_validator("ips", "excluded_ips")
    @classmethod
    def canonical_ips(cls, values: list[str]) -> list[str]:
        normalized = set()
        for value in values:
            if "%" in value:
                raise ValueError("scoped IPv6 addresses are not supported")
            address = ip_address(value.strip())
            # ClickHouse renders IPv4-mapped IPv6 with dotted IPv4 notation.
            if isinstance(address, IPv6Address) and address.ipv4_mapped is not None:
                normalized.add(f"::ffff:{address.ipv4_mapped}")
            else:
                normalized.add(str(address))
        return sorted(normalized)

    @field_validator("ip_search")
    @classmethod
    def valid_search(cls, value: str) -> str:
        value = value.strip().lower()
        if value and re.fullmatch(r"[0-9a-f:.]{1,45}", value) is None:
            raise ValueError("ip_search must be an IP address or literal IP prefix")
        return value

    @field_validator("source_relation")
    @classmethod
    def valid_source(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = validate_relation(value)
        if value.split(".")[0] != "corpscout":
            raise ValueError("source_relation must be in the corpscout database")
        if value == INPUT_RELATION:
            raise ValueError("the source must differ from the input queue")
        return value

    @field_validator("source_name")
    @classmethod
    def valid_source_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or "\0" in value:
            raise ValueError("source_name must be nonempty and contain no NUL")
        return value

    @model_validator(mode="after")
    def valid_selection(self) -> Self:
        modes = [
            bool(self.ips),
            self.source_relation is not None,
            self.retry_failed_task_id is not None,
        ]
        if sum(modes) != 1:
            raise ValueError(
                "provide exactly one of nonempty ips, source_relation or retry_failed_task_id"
            )
        for column in (
            self.ip_column,
            self.source_record_id_column,
            self.observed_at_column,
            *self.filters,
        ):
            if (
                column is not None
                and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column) is None
            ):
                raise ValueError("column names must be simple SQL identifiers")
        if any(not values for values in self.filters.values()):
            raise ValueError("each filter needs at least one value")
        if self.source_relation is None:
            if (
                self.filters
                or self.ip_search
                or self.excluded_ips
                or self.source_final
                or self.select_all
                or self.ip_column != "ip"
                or self.source_record_id_column is not None
                or self.observed_at_column is not None
            ):
                raise ValueError("table selection options require source_relation")
        elif not (
            self.filters
            or self.ip_search
            or self.max_rows is not None
            or self.select_all
        ):
            raise ValueError("provide filters, max_rows, or explicit select_all=true")
        return self


def selected_ips_sql(config: IpEnrichmentInputConfig) -> tuple[str, dict]:
    """Normalize in ClickHouse and group submissions without fetching the source into Python."""
    params = {}
    where = ""
    if config.retry_failed_task_id is not None:
        failed = " OR ".join(
            f"{column} IN %(errors)s"
            for column in ("city_lookup_status", "asn_lookup_status", "rdap_lookup_status")
        )
        relation = (
            f"(SELECT ip FROM {RESULT_RELATION} FINAL "
            f"WHERE task_id = %(failed_task)s AND ({failed})) AS source"
        )
        ip_value = "source.ip"
        params.update(failed_task=config.retry_failed_task_id, errors=ERROR_STATUSES)
    elif config.source_relation is None:
        relation = "(SELECT arrayJoin(%(ips)s) AS explicit_ip) AS source"
        ip_value = "source.explicit_ip"
        params["ips"] = config.ips
    else:
        relation = (
            config.source_relation
            + " AS source"
            + (" FINAL" if config.source_final else "")
        )
        ip_value = f"trimBoth(ifNull(toString(source.`{config.ip_column}`), ''))"
        predicates = []
        for index, (column, values) in enumerate(sorted(config.filters.items())):
            predicates.append(f"source.`{column}` IN %(filter_{index})s")
            params[f"filter_{index}"] = tuple(sorted(set(values)))
        if config.ip_search:
            try:
                address = ip_address(config.ip_search)
            except ValueError:
                predicates.extend(
                    [
                        f"source.`{config.ip_column}` >= %(ip_prefix)s",
                        f"source.`{config.ip_column}` < %(ip_prefix_end)s",
                    ]
                )
                params["ip_prefix"] = config.ip_search
                params["ip_prefix_end"] = config.ip_search[:-1] + chr(
                    ord(config.ip_search[-1]) + 1
                )
            else:
                canonical = (
                    f"::ffff:{address.ipv4_mapped}"
                    if isinstance(address, IPv6Address)
                    and address.ipv4_mapped is not None
                    else str(address)
                )
                predicates.append(f"{ip_value} = %(exact_ip)s")
                params["exact_ip"] = canonical
        where = " WHERE " + " AND ".join(predicates) if predicates else ""
    record_id = (
        f"trimBoth(ifNull(toString(source.`{config.source_record_id_column}`), ''))"
        if config.source_record_id_column is not None
        else "ifNull(normalized_ip, '')"
    )
    observed_at = (
        f"toDateTime64(source.`{config.observed_at_column}`, 6, 'UTC')"
        if config.observed_at_column is not None
        else "CAST(NULL AS Nullable(DateTime64(6, 'UTC')))"
    )
    excluded = ""
    if config.excluded_ips:
        excluded = " AND normalized_ip NOT IN %(excluded_ips)s"
        params["excluded_ips"] = tuple(config.excluded_ips)
    limit = ""
    if config.max_rows is not None:
        limit = " LIMIT %(limit)s"
        params["limit"] = config.max_rows
    return (
        f"""SELECT assumeNotNull(normalized_ip) AS ip,
        record_ref AS source_record_id, max(source_observed_at) AS observed_at
    FROM (
        SELECT coalesce(toString(toIPv4OrNull({ip_value})),
                        toString(toIPv6OrNull({ip_value}))) AS normalized_ip,
            {record_id} AS record_ref, {observed_at} AS source_observed_at
        FROM {relation}{where}
    )
    WHERE normalized_ip IS NOT NULL{excluded}
    GROUP BY normalized_ip, record_ref
    ORDER BY ip, source_record_id{limit}""",
        params,
    )


def source_label(config: IpEnrichmentInputConfig) -> str:
    if config.source_name is not None:
        return config.source_name
    if config.source_relation is not None:
        return config.source_relation
    if config.retry_failed_task_id is not None:
        return "retry:" + config.retry_failed_task_id
    return "manual"


def load_ip_draft(
    config: IpEnrichmentInputConfig,
    submission_id: str,
    store: ProcessingStore,
    clickhouse: ClickhouseResource,
    *,
    run_id: str,
) -> dict:
    selection = config.model_dump(exclude={"task_id", "submission_id", "queue_scope"})
    selection["filters"] = {
        key: sorted(set(values)) for key, values in config.filters.items()
    }
    fingerprint = hashlib.sha256(
        json.dumps(selection, sort_keys=True).encode()
    ).hexdigest()
    # Keep bulk manual values out of PostgreSQL receipts.
    for key in ("ips", "excluded_ips"):
        selection[key] = {
            "count": len(selection[key]),
            "sha256": hashlib.sha256(json.dumps(selection[key]).encode()).hexdigest(),
        }
    source = source_label(config)
    receipt = draft_queue.submission(store, submission_id)
    task_id = str(receipt["task_id"]) if receipt else None
    if receipt:
        task = store.task(task_id)
        if (
            task["processor"] != PROCESSOR_VERSION
            or task["queue_scope"] != config.queue_scope
            or (config.task_id is not None and config.task_id != task_id)
            or receipt["selection_fingerprint"] != fingerprint
        ):
            raise ValueError(
                "submission_id belongs to a different selection, task or scope"
            )
        if receipt["status"] == "completed":
            return {
                "task_id": task_id,
                "submission_id": submission_id,
                "input_count": receipt["input_count"],
                "total": task["total"],
            }
    while True:
        if receipt is None:
            task_id = draft_queue.find_draft(
                store,
                scope=config.queue_scope,
                processor=PROCESSOR_VERSION,
                task_id=config.task_id,
            )
        with store.selection_lock(task_id):
            latest = draft_queue.submission(store, submission_id)
            if latest is not None:
                if (
                    str(latest["task_id"]) != task_id
                    or latest["selection_fingerprint"] != fingerprint
                ):
                    raise ValueError(
                        "submission_id belongs to another task or selection"
                    )
                if latest["status"] == "completed":
                    return {
                        "task_id": task_id,
                        "submission_id": submission_id,
                        "input_count": latest["input_count"],
                        "total": store.task(task_id)["total"],
                    }
            if (
                store.task(task_id)["status"] != "draft"
                and receipt is None
                and config.task_id is None
            ):
                continue  # Start won the race: re-resolve to the next draft.
            receipt = draft_queue.prepare_submission(
                store,
                task_id=task_id,
                submission_id=submission_id,
                source=source,
                selection=selection,
                fingerprint=fingerprint,
            )
            if receipt["status"] == "completed":
                return {
                    "task_id": task_id,
                    "submission_id": submission_id,
                    "input_count": receipt["input_count"],
                    "total": store.task(task_id)["total"],
                }
            try:
                with clickhouse.get_connection() as client:
                    query_id = "ip-queue-import:" + submission_id
                    client.execute(
                        "KILL QUERY WHERE query_id=%(id)s SYNC", {"id": query_id}
                    )
                    # A retry replaces only this submission's rows, from the current source.
                    client.execute(
                        f"DELETE FROM {INPUT_RELATION} WHERE task_id=%(task)s AND submission_id=%(submission)s",
                        {"task": task_id, "submission": submission_id},
                        settings={"lightweight_deletes_sync": 2},
                    )
                    if config.source_relation is not None:
                        columns = {
                            row[0]
                            for row in client.execute(
                                f"DESCRIBE TABLE {config.source_relation}"
                            )
                        }
                        required = {config.ip_column, *config.filters}
                        if config.source_record_id_column is not None:
                            required.add(config.source_record_id_column)
                        if config.observed_at_column is not None:
                            required.add(config.observed_at_column)
                        if required - columns:
                            raise ValueError(
                                "source is missing columns: "
                                + ", ".join(sorted(required - columns))
                            )
                    selected_sql, params = selected_ips_sql(config)
                    params.update(
                        task=task_id,
                        submission=submission_id,
                        source_name=source,
                        source_run_id=config.source_run_id or run_id,
                    )
                    identity = INPUT_ID_SQL.format(
                        ip="chosen.ip", source="%(source_name)s", record="chosen.source_record_id"
                    )
                    # Entries land straight in the task's partition; the draft keeps one row
                    # per identity, so overlapping submissions append only what is new.
                    client.execute(
                        f"""INSERT INTO {INPUT_RELATION}
                        (task_id, input_id, ip, source_name, source_record_id, source_run_id, observed_at, submission_id)
                        SELECT %(task)s, s.input_id, s.ip, %(source_name)s, s.source_record_id,
                            %(source_run_id)s, s.observed_at, %(submission)s
                        FROM (
                            SELECT {identity} AS input_id, chosen.ip AS ip,
                                chosen.source_record_id AS source_record_id, chosen.observed_at AS observed_at
                            FROM ({selected_sql}) AS chosen
                        ) AS s
                        LEFT ANTI JOIN (
                            SELECT input_id FROM {INPUT_RELATION} WHERE task_id=%(task)s
                        ) AS e USING (input_id)""",
                        params,
                        query_id=query_id,
                        settings={
                            "async_insert": 0,
                            "use_query_cache": 0,
                            "max_threads": 4,
                            "max_bytes_before_external_group_by": 536870912,
                            "max_bytes_before_external_sort": 536870912,
                            # The draft side of the anti-join can hold tens of millions of ids.
                            "join_algorithm": "grace_hash",
                        },
                    )
                    [(count,)] = client.execute(
                        f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s AND submission_id=%(submission)s",
                        {"task": task_id, "submission": submission_id},
                    )
                    [(total,)] = client.execute(
                        f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s",
                        {"task": task_id},
                    )
                draft_queue.finish_submission(
                    store,
                    submission_id=submission_id,
                    task_id=task_id,
                    count=count,
                    total=total,
                )
                return {
                    "task_id": task_id,
                    "submission_id": submission_id,
                    "input_count": count,
                    "total": total,
                }
            except BaseException:
                draft_queue.fail_submission(store, submission_id)
                raise


@dg.asset(
    group_name="ip_enrichment",
    kinds={"clickhouse", "postgres"},
    pool="ip_enrichment_input",
    metadata={"dagster/table_name": INPUT_RELATION},
    description="Append IPs from a list, a filtered ClickHouse relation or a task's failed "
    "results to the open IP enrichment draft. Does not start enrichment.",
)
def ip_enrichment_input(
    context: dg.AssetExecutionContext,
    config: IpEnrichmentInputConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
) -> dg.MaterializeResult:
    submission_id = str(
        UUID(
            config.submission_id
            or context.run.tags.get("processing/submission_id")
            or context.run.root_run_id
            or context.run.run_id
        )
    )
    context.instance.add_run_tags(
        context.run.run_id, {"processing/submission_id": submission_id}
    )
    with processing.get_store() as store:
        metadata = load_ip_draft(
            config, submission_id, store, clickhouse, run_id=context.run.run_id
        )
    context.instance.add_run_tags(
        context.run.run_id, {"processing/task_id": metadata["task_id"]}
    )
    context.log.info(
        "IP enrichment draft %s: submission %s added %s rows, total %s",
        metadata["task_id"],
        submission_id,
        metadata["input_count"],
        metadata["total"],
    )
    return dg.MaterializeResult(
        metadata={**metadata, "input_relation": INPUT_RELATION}
    )


ip_enrichment_input_job = dg.define_asset_job(
    "ip_enrichment_input_job",
    selection=dg.AssetSelection.assets(ip_enrichment_input),
)
defs = dg.Definitions(assets=[ip_enrichment_input], jobs=[ip_enrichment_input_job])
```

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen --no-sync pytest tests/test_ip_enrichment_input.py -q -p no:cacheprovider`
Expected: all pass.

Run: `uv run --frozen --no-sync dg check defs`
Expected: `All definitions loaded successfully.` (`results.py` still imports `INPUT_RELATION`/`PROCESSOR_VERSION`, which exist.)

Run ruff format/check on `src/dagster_v3/defs/ip_enrichment/input.py tests/test_ip_enrichment_input.py`.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/ip_enrichment/input.py services/dagster_v3/tests/test_ip_enrichment_input.py
git commit -m "feat(dagster): IP enrichment drafts with receipts; imports insert straight into the task partition

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---
### Task 3: GeoLite2 freshness check and the `.env.example` fix (no download, no credentials)

Evidence: every result row on prod carries City build `2026-07-10 06:34` and ASN build `2026-07-10 08:15`; there is no update mechanism and the owner has no MaxMind account (R2), so the files are replaced by hand. `MaxMindDatabaseResource.database_paths()` (`commoncrawl_geoip/resources.py:11-16`) resolves `<dir>/GeoLite2-City.mmdb` and `<dir>/GeoLite2-ASN.mmdb` at call time, and `ip_enrichment_results` opens both with `maxminddb.open_database` inside the run, so a file replaced by `mv` is used by the next run without restarting anything. `.env.example:70` still describes versioned `GeoLite2-City_*` folders, which `resources.py` does not read. Checks are registered like `commoncrawl_ip_checks.py` (an `@dg.asset_check` on another module's asset key plus a check-only job). The check is attached to `ip_enrichment_results` so it runs with every enrichment run launched with its checks and can be executed alone from `geolite2_freshness_job`; Task 5 additionally reports the build dates in every run's metadata.

**Files:**
- Create: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/freshness.py`
- Modify: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/definitions.py`
- Create: `services/dagster_v3/tests/test_geolite2_freshness.py`
- Modify: `services/dagster_v3/.env.example:70-71`

**Interfaces:**
- Produces (module `dagster_v3.defs.commoncrawl_geoip.freshness`): `EDITIONS = ("GeoLite2-City", "GeoLite2-ASN")`, `MAX_AGE = timedelta(days=14)`, `RESULTS_ASSET = dg.AssetKey("ip_enrichment_results")`, `database_build_times(resource: MaxMindDatabaseResource, *, opener=maxminddb.open_database) -> dict[str, datetime]`, `freshness(times: dict[str, datetime], now: datetime) -> dg.AssetCheckResult`, check `geolite2_databases_fresh` (asset `ip_enrichment_results`, `blocking=False`), job `geolite2_freshness_job` (checks only). Task 5 imports `MAX_AGE` and `freshness`.

- [ ] **Step 1: Write the failing tests**

Create `services/dagster_v3/tests/test_geolite2_freshness.py`:

```python
"""GeoLite2 freshness with fake readers; no MaxMind traffic, no real .mmdb files."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from dagster_v3.defs.commoncrawl_geoip import freshness
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource


class Reader:
    def __init__(self, path, build_epoch):
        self.path, self.build_epoch = path, build_epoch

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def metadata(self):
        return SimpleNamespace(database_type="GeoLite2-City", build_epoch=self.build_epoch)


def test_build_times_are_read_per_edition_from_the_resource_paths(tmp_path):
    for edition in freshness.EDITIONS:
        (tmp_path / f"{edition}.mmdb").touch()
    resource = MaxMindDatabaseResource(database_directory=str(tmp_path))
    opened = []

    def opener(path):
        opened.append(path.name)
        return Reader(path, 1_790_000_000)

    times = freshness.database_build_times(resource, opener=opener)
    assert opened == ["GeoLite2-City.mmdb", "GeoLite2-ASN.mmdb"]
    assert times == dict.fromkeys(freshness.EDITIONS, datetime.fromtimestamp(1_790_000_000, UTC))


def test_freshness_check_fails_after_fourteen_days():
    now = datetime(2026, 9, 25, tzinfo=UTC)
    fresh = now - timedelta(days=3)
    stale = now - timedelta(days=20)
    passed = freshness.freshness({"GeoLite2-City": fresh, "GeoLite2-ASN": fresh}, now)
    assert passed.passed and passed.metadata["max_age_days"].value == 14
    assert passed.metadata["GeoLite2-City_build"].value == fresh.isoformat()
    result = freshness.freshness({"GeoLite2-City": fresh, "GeoLite2-ASN": stale}, now)
    assert not result.passed and "GeoLite2-ASN built 2026-09-05" in result.description
    boundary = freshness.freshness({"GeoLite2-City": now - freshness.MAX_AGE, "GeoLite2-ASN": fresh}, now)
    assert boundary.passed  # exactly 14 days old is still current


def test_check_targets_the_results_asset_and_the_job_selects_only_the_check():
    assert freshness.geolite2_databases_fresh.check_key == freshness.CHECK_KEY
    assert freshness.CHECK_KEY.asset_key == freshness.RESULTS_ASSET
    assert freshness.geolite2_freshness_job.name == "geolite2_freshness_job"
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_geolite2_freshness.py -q -p no:cacheprovider`
Expected: FAIL — `ImportError: cannot import name 'freshness' from 'dagster_v3.defs.commoncrawl_geoip'`.

- [ ] **Step 3: Create the freshness module and register it**

Create `services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/freshness.py`:

```python
"""GeoLite2 freshness: the files are replaced by hand, this check says when it is due.

MaxMind publishes GeoLite2 twice a week; there is no MaxMind account here, so nothing
downloads. The check reads the build epoch from each installed file's metadata and
fails when either GeoLite2-City.mmdb or GeoLite2-ASN.mmdb is older than 14 days.
ip_enrichment_results opens the files per run through MaxMindDatabaseResource, so a
file replaced with mv is used by the next run without a restart; see
docs/operations/ip-enrichment-draft-queue.md for the manual procedure.
"""

from datetime import UTC, datetime, timedelta

import dagster as dg
import maxminddb

from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource

EDITIONS = ("GeoLite2-City", "GeoLite2-ASN")
MAX_AGE = timedelta(days=14)
RESULTS_ASSET = dg.AssetKey("ip_enrichment_results")
CHECK_KEY = dg.AssetCheckKey(RESULTS_ASSET, "geolite2_databases_fresh")


def database_build_times(
    resource: MaxMindDatabaseResource, *, opener=maxminddb.open_database
) -> dict[str, datetime]:
    """Build time per edition, read from the installed files' metadata."""
    times = {}
    for edition, path in zip(EDITIONS, resource.database_paths(), strict=True):
        with opener(path) as reader:
            times[edition] = datetime.fromtimestamp(reader.metadata().build_epoch, UTC)
    return times


def freshness(times: dict[str, datetime], now: datetime) -> dg.AssetCheckResult:
    stale = {edition: built for edition, built in times.items() if now - built > MAX_AGE}
    return dg.AssetCheckResult(
        passed=not stale,
        severity=dg.AssetCheckSeverity.ERROR,
        description=(
            "GeoLite2 databases are current."
            if not stale
            else "Stale GeoLite2 databases, replace them by hand: "
            + ", ".join(
                f"{edition} built {built.date().isoformat()}"
                for edition, built in stale.items()
            )
        ),
        metadata={
            **{f"{edition}_build": built.isoformat() for edition, built in times.items()},
            "max_age_days": MAX_AGE.days,
        },
    )


@dg.asset_check(
    asset=RESULTS_ASSET,
    name=CHECK_KEY.name,
    description="Fails when GeoLite2-City.mmdb or GeoLite2-ASN.mmdb in "
    "MAXMIND_DATABASE_DIRECTORY was built more than 14 days ago (MaxMind publishes "
    "twice a week; the files are updated by hand).",
    blocking=False,
)
def geolite2_databases_fresh(
    maxmind_geoip: MaxMindDatabaseResource,
) -> dg.AssetCheckResult:
    return freshness(database_build_times(maxmind_geoip), datetime.now(UTC))


# Runs the check alone (Dagster UI or dg launch); no schedule, per the owner's decision.
geolite2_freshness_job = dg.define_asset_job(
    "geolite2_freshness_job", selection=dg.AssetSelection.checks(CHECK_KEY)
)
```

Replace the whole of `services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/definitions.py` with:

```python
import dagster as dg

from dagster_v3.defs.commoncrawl_geoip.freshness import (
    geolite2_databases_fresh,
    geolite2_freshness_job,
)
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource


defs = dg.Definitions(
    resources={
        "maxmind_geoip": MaxMindDatabaseResource(),
    },
    asset_checks=[geolite2_databases_fresh],
    jobs=[geolite2_freshness_job],
)
```

- [ ] **Step 4: Fix `.env.example`**

Replace lines 70-71 of `services/dagster_v3/.env.example` with:

```
# Directory holding GeoLite2-City.mmdb and GeoLite2-ASN.mmdb directly (no versioned
# subfolders). Replaced by hand (mv, never cp over the open file); the check
# geolite2_databases_fresh on ip_enrichment_results fails once either is 14 days old.
MAXMIND_DATABASE_DIRECTORY=/path/to/geoip
```

- [ ] **Step 5: Run the tests and the definitions check**

Run: `uv run --frozen --no-sync pytest tests/test_geolite2_freshness.py tests/test_commoncrawl_geoip_assets.py tests/test_schedule_cron_contracts.py -q -p no:cacheprovider`
Expected: all pass (no schedule was added, so the cron contract is untouched).

Run: `uv run --frozen --no-sync dg check defs`
Expected: `All definitions loaded successfully.` (the check resolves `ip_enrichment_results` from `defs/ip_enrichment/results.py`; `geolite2_freshness_job` loads).

Run: `rg -n "MAXMIND_ACCOUNT_ID|MAXMIND_LICENSE_KEY|geolite2_update|GeoLite2-City_\*" services/dagster_v3/src services/dagster_v3/.env.example services/dagster_v3/ansible services/dagster_v3/docs`
Expected: no matches (nothing in the repo refers to credentials or a download).

Run ruff format/check on `src/dagster_v3/defs/commoncrawl_geoip/freshness.py src/dagster_v3/defs/commoncrawl_geoip/definitions.py tests/test_geolite2_freshness.py`.

- [ ] **Step 6: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/freshness.py services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/definitions.py services/dagster_v3/tests/test_geolite2_freshness.py services/dagster_v3/.env.example
git commit -m "feat(dagster): geolite2_databases_fresh check on ip_enrichment_results; GeoLite2 files are replaced by hand

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---
### Task 4: Page-batched RDAP resolution, registry classes per miss, RIPE and APNIC without personal data, optional per-registry budget

Today `RdapEnricher.lookup` (`enrichment.py:322-434`) issues, per IP, one `rdap_ip_lookup_results_current` query, one `rdap_networks_current FINAL` query that returns `raw_response` and re-parses it, and one `INSERT` per lookup marker; the review measured 90% of wall time in these round trips. The new `RdapEnricher.resolve_page(rows)` answers a whole page with four round trips at most, keeps two in-process caches (`cached`: fresh network rows read from ClickHouse, `recent`: reusable networks fetched over HTTP in this run) and asks the registries only for misses. Each miss keeps today's deployed behaviour (`enrichment.py:381-395`): `classify_registration` (one context round trip), the network row, its class row (unless `unknown`), its segments; a non-reusable registration is counted in `registry_level_responses`, never enters `recent`, and answers only its address — a later page or task asking for that address is served by its own `found` marker in `rdap_ip_lookup_results` (per-address hit), while other addresses of the block get their own lookup.

**RIPE (R4, corrected on 2026-09-26 from the published AUP and two live answers).** The RIPE Database AUP limits the *personal data sets* (person/role objects) one source address may receive to **1,000 per 24 hours** (20,000 applies only to a proxy registered with the RIPE NCC); queries themselves are "Unlimited" within reasonable use, at most 3 simultaneous connections, and its "Anti-avoidance and Connected Persons" clause treats pooling limits across addresses as a violation. RIPE's RDAP `ip` answers embed 1–5 person objects with real names (`kind: individual`), so every RDAP request to RIPE counts. The owner does not need personal data, so RIPE misses go to the RIPE Database REST search with `flags=no-referenced` (whois `-r`): it returns the most specific `inetnum`/`inet6num` with `netname`, `country`, `status`, `org`, `mnt-by`, dates and contact *handles* only — no person or role objects, so nothing counts against the limit — and for unallocated space the root object `0.0.0.0 - 255.255.255.255`, which the existing catch-all check rejects. `ripe_rest.rdap_shape` turns that object into the RDAP document `normalize_rdap_network` already understands (same `network_key` `ripe:<range>` as an RDAP answer, `descr`/`admin-c`/`tech-c`/`remarks` dropped on purpose), so classification, segments, caches and results are unchanged. **APNIC (owner decision 2026-09-26: "RIPE and APNIC first").** APNIC's RDAP answers embed contact entities too. Its whois service on port 43 honours the `-r` flag (verified by the controller on 2026-09-26: `whois -h whois.apnic.net -- "-r 103.35.64.49"` returns the `inetnum` `103.35.64.0 - 103.35.67.255`, netname `FPT-VN`, `descr` `FPT Telecom` plus an address line, `admin-c`/`tech-c` as handles only, country `VN`, `mnt-by MAINT-VN-VNNIC`, `mnt-irt IRT-VNNIC-AP`, status `ALLOCATED PORTABLE`, `last-modified`, no person/role/irt objects, followed by matching `route` objects); the HTTP gateway `https://wq.apnic.net/query?searchtext=…&flags=r` does **not** honour `-r` (it returned an irt object with an address) and is not used. `apnic_whois.py` speaks the port-43 protocol directly (one TCP connection per query, `-r <ip>`, read to EOF, 10 s connect / 30 s read, paced by `request_delay_seconds` like every request), keeps only the first `inetnum`/`inet6num` object and reshapes it like the RIPE answer. Holder name: only 2.4% of APNIC's inetnum objects carry `org:` (2026-09-25 dump), so the **first `descr` line** is the registrant name; further `descr` lines (addresses) and all contact handles are dropped; `status` is upper-cased and whitespace-collapsed (the dump spells `Allocated non-portable`, `ASSIGNED  NON-PORTABLE`). **NIR rule:** the answer is an NIR's own allocation object — not a holder's — when its `netname` starts with `JPNIC`, `KRNIC`, `TWNIC`, `IDNIC`, `CNNIC`, `IRINN` or `VNNIC`, or its first `descr` names the NIR (`Japan Network Information Center`, `Korea Network Information Center` / `Korea Internet`, `Taiwan Network Information Center`, `Indonesia Network Information Center`, `China Internet Network Information Center`, `Indian Registry for Internet Names and Numbers`, `Vietnam Internet Network Information Centre`); then the end holder lives in the NIR's database and the resolver falls back to RDAP, which the IANA bootstrap routes to the NIR server. `mnt-by` decides nothing: FPT's `103.35.64.0/22` is maintained by `MAINT-VN-VNNIC` and *is* the holder's allocation (the real answer above is the test fixture). ISP-level allocations that NIRs keep in APNIC's database (JPNIC's `MEGAEGG 1.0.64.0/18`, KRNIC's `KORNET 168.126.0.0/16`) are holders and are used as they are. Placeholders (`0.0.0.0 - 255.255.255.255 IANA-BLOCK`, `APNIC-AP` blocks) go through the catch-all rejection and the registry class like any other answer. `apnic_whois: true` is frozen in the profile. RDAP stays for ARIN, LACNIC and AFRINIC (their contact entities carry no documented daily cap; each run reports `rdap_person_entities_by_registry`, and a `ripe` or `apnic` key there means a no-personal-data path is not in use).

**Budget (R4).** `registry_daily_budgets` (transport, default `{}`) is an optional rolling 24-hour *request* budget per registry: a miss of a registry at its budget is **deferred** (no result, no error) and `wait_for_registry_budget()` sleeps until an hour's share of that registry's budget frees. The window is seeded on start from `rdap_networks.fetched_at` of the last day (every writer, so a resume and the legacy worker share it). With the REST path RIPE needs no budget; the guard exists for a registry that starts rate-limiting the direct address. Proxy egress lanes were considered at the owner's request and dropped on 2026-09-26: they would not shorten the run (pacing is global and the loop single-threaded), RIPE no longer needs them, and pooling a registry's allowance across addresses is the RIPE AUP's anti-avoidance case.

**Files:**
- Create: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/ripe_rest.py`
- Create: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/apnic_whois.py`
- Rewrite: `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py`
- Modify: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/client.py:1-21, 52-57` (imports, `RIR_BY_HOST`, `registry_for`)
- Modify: `services/dagster_v3/tests/test_ip_enrichment_results.py:1-25, 61-81, 84-147` (imports, `response`, `environment`) and append the resolver tests (they drive `RdapEnricher` directly with the `environment` fixture)

**Interfaces:**
- Produces (module `dagster_v3.defs.commoncrawl_rdap.client`): `RIR_BY_HOST: dict[str, str]` (RDAP host → whoisit registry name, from `whoisit.bootstrap.BaseBootstrap.RIR_RDAP_ENDPOINTS`), `RdapClient.registry_for(ip_address_or_network: str) -> str` (`"ripe"`, `"arin"`, … or `""` when whoisit cannot map the address).
- Produces (module `dagster_v3.defs.commoncrawl_rdap.ripe_rest`): `SEARCH_URL`, `OBJECT_URL`, `NETWORK_TYPES`, `rdap_shape(obj: dict) -> dict`, `RipeRestClient(*, user_agent: str, session: requests.Session | None = None)` with `lookup_ip(ip) -> RdapLookupResponse` (raises `RdapClientError` with the same codes as `RdapClient`) and `close()`.
- Produces (module `dagster_v3.defs.commoncrawl_rdap.apnic_whois`): `WHOIS_HOST = "whois.apnic.net"`, `WHOIS_PORT = 43`, `CONNECT_TIMEOUT = 10.0`, `READ_TIMEOUT = 30.0`, `NETWORK_TYPES`, `NIR_NETNAME_PREFIXES`, `NIR_DESCR_PHRASES`, `parse_answer(text: str) -> list[list[tuple[str, str]]]`, `network_object(objects) -> list[tuple[str, str]] | None`, `nir_of(netname: str, descr: str) -> str`, `rdap_shape(obj) -> dict` (its `corpscout` block carries `source: apnic-whois`, `flags: -r`, `nir`, `mnt_by`, `mnt_irt`), `is_nir_object(raw: Mapping) -> bool`, `ApnicWhoisClient(*, host=WHOIS_HOST, port=WHOIS_PORT, connect=socket.create_connection)` with `query(ip) -> str` (the port-43 exchange), `lookup_ip(ip) -> RdapLookupResponse` (raises `RdapClientError` with the RDAP client's codes) and `close()`.
- Produces (module `dagster_v3.defs.ip_enrichment.enrichment`):
  - `IpEnrichmentResultsConfig` — today's fields and defaults (`batch_size=250`, `max_requests=250`, `request_delay_seconds=1.0`, `parent_depth=1`, `rdap_cache_days=30`, `force_rdap=False`, `rate_limit_retry_seconds=3600`, `transient_retry_seconds=900`) plus `registry_daily_budgets: dict[str, int]` (default `{}`, keys lower-cased whoisit names, values ≥ 1; a transport setting), `ripe_rest: bool = True` (frozen) and `apnic_whois: bool = True` (frozen).
  - `BUDGET_WINDOW_SECONDS = 86_400`, `NEGATIVE_STATUSES`, `NETWORK_COLUMNS` (every `rdap_networks` column except `raw_response`), `WRITE_SETTINGS`.
  - `geoip_result(...)`, `rdap_result(...)`, `matching_cidr(...)` (unchanged), `cidr_containing(start: str, end: str, address) -> str | None`, `cached_network_row(row) -> RdapNetwork` (with `raw_response=""`), `person_entities(raw: Mapping) -> int` (vCards of kind `individual`, nested included).
  - `RdapEnricher(client, rdap: RdapClient, ripe: RipeRestClient, apnic: ApnicWhoisClient, config, log, *, started_at: datetime, cache_cutoff: datetime, clock=None, sleep=None)` with counters `requests, cache_hits, networks_written, parent_failures, registry_level_responses`, dicts `requests_by_registry, person_entities_by_registry, deferrals_by_registry` (cumulative) and `deferred` (since the last `reset_pass()`), flag `budget_reached` (the `max_requests` budget), and methods `resolve_page(rows: list[dict]) -> dict[str, dict]` (each row's `ip` → the `rdap_*` result fields; an IP is absent when it was deferred by a registry budget or the `max_requests` budget ran out), `seed_registry_usage(rows: list[tuple[str, float]])` (registry, seconds ago; chronological), `seconds_until_budget_frees() -> float`, `wait_for_registry_budget() -> float` (seconds slept; resets `deferred`), `reset_pass()`.
- Removes: `RdapEnricher.lookup`. `RequestBudgetReached` stays defined (unused) so `results.py` keeps importing until Task 5 deletes both; the `maxminddb` re-export stays (`results.py` and tests patch `enrichment.maxminddb.open_database`).

- [ ] **Step 1: Adjust the fixture and the helpers**

In `services/dagster_v3/tests/test_ip_enrichment_results.py`:

Replace the import block (lines 1-25) with:

```python
"""Task-scoped enrichment with real storage and controlled external lookup responses."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import dagster as dg
import pytest
from clickhouse_driver import Client

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource
from dagster_v3.defs.commoncrawl_rdap import client as rdap_client
from dagster_v3.defs.commoncrawl_rdap import apnic_whois, ripe_rest
from dagster_v3.defs.commoncrawl_rdap.assets import (
    RDAP_NETWORK_INSERT_SQL,
    RDAP_SEGMENT_INSERT_SQL,
)
from dagster_v3.defs.commoncrawl_rdap.client import RdapClient, RdapClientError
from dagster_v3.defs.commoncrawl_rdap.rdap import (
    RdapLookupResponse,
    normalize_rdap_network,
)
from dagster_v3.defs.commoncrawl_rdap.apnic_whois import ApnicWhoisClient
from dagster_v3.defs.commoncrawl_rdap.ripe_rest import RipeRestClient
from dagster_v3.defs.ip_enrichment import enrichment, results
from dagster_v3.defs.ip_enrichment.enrichment import (
    IpEnrichmentResultsConfig,
    RdapEnricher,
)
from tests.test_ip_enrichment_input import (
    materialize as prepare_input,
    server as server,
)
from tests.test_ip_registry import apply_migration, seed_reference_data
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
    store as store,
)
```

(`Path` stays imported for the fixture; `from dagster_v3.defs.ip_enrichment.input import ip_enrichment_input` goes — the workflow test that used it is deleted in Task 5.)

Change the `response` helper (lines 61-81) to accept raw overrides, and add `rest_object` after it:

```python
def response(ip, *, start=None, end=None, **raw):
    ipv6 = ":" in ip
    return RdapLookupResponse(
        rir="arin",
        raw_response={
            "objectClassName": "ip network",
            "handle": "TEST-" + ip,
            "startAddress": start
            or ("2001:4860::" if ipv6 else ip.rsplit(".", 1)[0] + ".0"),
            "endAddress": end
            or (
                "2001:4860:ffff:ffff:ffff:ffff:ffff:ffff"
                if ipv6
                else ip.rsplit(".", 1)[0] + ".255"
            ),
            "ipVersion": "v6" if ipv6 else "v4",
            "name": "Test registration",
            "country": "CA",
            "status": ["active"],
            **raw,
        },
    )


def rest_object(ip, *, start=None, end=None, netname="RIPE-TEST-NET", org="ORG-TEST1-RIPE"):
    """A RIPE REST search object (inetnum) as rest.db.ripe.net returns it with no-referenced."""
    start = start or ip.rsplit(".", 1)[0] + ".0"
    end = end or ip.rsplit(".", 1)[0] + ".255"
    attributes = [
        {"name": "inetnum", "value": f"{start} - {end}"},
        {"name": "netname", "value": netname},
        {"name": "descr", "value": "A person's name may appear here"},
        {"name": "country", "value": "SE"},
        {"name": "admin-c", "value": "AB1234-RIPE"},
        {"name": "tech-c", "value": "AB1234-RIPE"},
        {"name": "status", "value": "ASSIGNED PA"},
        {"name": "mnt-by", "value": "TEST-MNT"},
        {"name": "created", "value": "2010-05-04T10:00:00Z"},
        {"name": "last-modified", "value": "2024-01-02T03:04:05Z"},
        {"name": "source", "value": "RIPE"},
    ]
    if org:
        attributes.insert(4, {"name": "org", "value": org})
    return {
        "type": "inetnum",
        "primary-key": {"attribute": [{"name": "inetnum", "value": f"{start} - {end}"}]},
        "attributes": {"attribute": attributes},
    }


# The answer of `whois -h whois.apnic.net -- "-r 103.35.64.49"` on 2026-09-26: the object
# attributes as returned (the address line and the route block are representative).
APNIC_FPT_ANSWER = """% [whois.apnic.net]
% Whois data copyright terms    http://www.apnic.net/db/dbcopyright.html

% Information related to '103.35.64.0 - 103.35.67.255'

% Abuse contact for '103.35.64.0 - 103.35.67.255' is 'hm-changed@vnnic.vn'

inetnum:        103.35.64.0 - 103.35.67.255
netname:        FPT-VN
descr:          FPT Telecom
descr:          9th floor, FPT Building, Duy Tan street, Cau Giay, Ha Noi
country:        VN
admin-c:        FHIG1-AP
tech-c:         FHIG1-AP
mnt-by:         MAINT-VN-VNNIC
mnt-irt:        IRT-VNNIC-AP
status:         ALLOCATED PORTABLE
last-modified:  2019-03-13T05:19:11Z
source:         APNIC

% Information related to '103.35.64.0/22AS18403'

route:          103.35.64.0/22
descr:          FPT Telecom Company
origin:         AS18403
mnt-by:         MAINT-VN-FPT
source:         APNIC

% This query was served by the APNIC Whois Service version 1.88.34 (WHOIS-AU1)
"""

# JPNIC's own object for a /24 it holds (from the 2026-09-25 dump): NIR-managed space whose
# holder lives in JPNIC's database.
APNIC_JPNIC_ANSWER = """% [whois.apnic.net]

inetnum:        202.12.14.0 - 202.12.14.255
netname:        JPNIC-NET-JP
descr:          Japan Network Information Center
country:        JP
admin-c:        JNIC1-AP
tech-c:         JNIC1-AP
status:         ASSIGNED PORTABLE
mnt-by:         MAINT-JPNIC
last-modified:  2008-09-04T06:51:28Z
source:         APNIC
"""


def apnic_answer(ip, *, netname="APNIC-TEST-NET", descr=("Test Holder Pty Ltd", "1 Test Street, Sydney NSW 2000"), status="ASSIGNED NON-PORTABLE", mnt_by="MAINT-AU-TEST"):
    """A port-43 answer with -r for the /24 around ``ip``, in the shape whois.apnic.net returns."""
    start, end = ip.rsplit(".", 1)[0] + ".0", ip.rsplit(".", 1)[0] + ".255"
    lines = [
        f"inetnum:        {start} - {end}",
        f"netname:        {netname}",
        *(f"descr:          {line}" for line in descr),
        "country:        AU",
        "admin-c:        TEST1-AP",
        "tech-c:         TEST1-AP",
        f"mnt-by:         {mnt_by}",
        f"status:         {status}",
        "last-modified:  2024-01-02T03:04:05Z",
        "source:         APNIC",
    ]
    return (
        "% [whois.apnic.net]\n% Whois data copyright terms    http://www.apnic.net/db/dbcopyright.html\n\n"
        + "\n".join(lines)
        + "\n\n% This query was served by the APNIC Whois Service version 1.88.34 (WHOIS-AU1)\n"
    )
```

In the `environment` fixture keep everything (the 000124 statements, the test dictionary, `apply_migration` of 000450/000451, the truncations, the readers, the `lookup_ip` monkeypatch, `DagsterInstance.ephemeral()`) and add, right after the `RdapClient.lookup_ip` monkeypatch (line 136):

```python
    # RIPE addresses (5/8 and 2a0x:: in these tests) go to the REST client, 202/8 to APNIC's
    # whois (its real parser over a stubbed port-43 exchange); everything else to RDAP as
    # ARIN. Every stub records the address in env.calls.
    monkeypatch.setattr(
        RdapClient,
        "registry_for",
        lambda self, ip: "ripe" if ip.startswith(("5.", "2a0")) else "apnic" if ip.startswith("202.") else "arin",
    )

    def rest_lookup(self, ip):
        calls.append(ip)
        return RdapLookupResponse(rir="ripe", raw_response=ripe_rest.rdap_shape(rest_object(ip)))

    def apnic_query(self, ip):
        calls.append(ip)
        return apnic_answer(ip)

    monkeypatch.setattr(RipeRestClient, "lookup_ip", rest_lookup)
    monkeypatch.setattr(ApnicWhoisClient, "query", apnic_query)
```

Append the helpers and the resolver tests at the end of the file:

```python
def resolver(env, *, started_at=None, cache_days=30, clock=None, sleep=None, **config):
    started = started_at or datetime.now(UTC)
    settings = IpEnrichmentResultsConfig(
        task_id=str(uuid4()), request_delay_seconds=0, rdap_cache_days=cache_days, **config
    )
    return RdapEnricher(
        env.client,
        RdapClient(user_agent="test"),
        RipeRestClient(user_agent="test"),
        ApnicWhoisClient(),
        settings,
        SimpleNamespace(info=lambda *a: None, warning=lambda *a: None),
        started_at=started,
        cache_cutoff=started - timedelta(days=cache_days),
        clock=clock,
        sleep=sleep,
    )


def page(env, *ips):
    """Rows as the results loop would read them, with ClickHouse's own buckets."""
    buckets = dict(
        env.client.execute(
            "SELECT ip, toUInt16(cityHash64(ip) % 256) FROM (SELECT arrayJoin(%(ips)s) AS ip)",
            {"ips": list(ips)},
        )
    )
    return [
        {"input_id": f"{buckets[ip]:03d}:{ip}", "ip": ip, "ip_version": 4 if "." in ip else 6, "bucket": buckets[ip]}
        for ip in ips
    ]


def seed_network(env, ip, *, fetched_at, **raw):
    """A reusable registration in the RDAP cache, visible to the trie after a reload."""
    normalized = normalize_rdap_network(response(ip, **raw), fetched_at=fetched_at, segment_role="lookup_result")
    env.client.execute(RDAP_NETWORK_INSERT_SQL, [normalized.network.clickhouse_values()])
    env.client.execute(RDAP_SEGMENT_INSERT_SQL, [s.clickhouse_values() for s in normalized.segments])
    env.client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    return normalized


def query_kinds(queries):
    return {
        "negative": sum(q.lstrip().startswith("SELECT ip, lookup_status") for q in queries),
        "trie": sum("dictGetOrDefault('corpscout.rdap_network_trie'" in q for q in queries),
        "networks": sum(q.lstrip().startswith("SELECT network_key, rir") for q in queries),
        "markers": sum(q.lstrip().startswith("INSERT INTO corpscout.rdap_ip_lookup_results") for q in queries),
        "context": sum(q.lstrip().startswith("SELECT ifNull((SELECT ready") for q in queries),
        "results": sum(q.lstrip().startswith("INSERT INTO corpscout.ip_enrichment_results") for q in queries),
    }


def test_page_resolution_uses_a_fixed_number_of_round_trips(environment, monkeypatch):
    env = environment
    seed_network(env, "8.8.8.1", fetched_at=datetime.now(UTC))
    queries = []
    execute = Client.execute
    monkeypatch.setattr(Client, "execute", lambda self, query, *a, **k: (queries.append(query), execute(self, query, *a, **k))[1])
    rows = page(env, *[f"8.8.8.{n}" for n in range(1, 41)], "127.0.0.1", "10.0.0.1")
    enricher = resolver(env)
    resolved = enricher.resolve_page(rows)
    assert env.calls == []  # every global address was a trie hit
    assert {ip: r["rdap_lookup_status"] for ip, r in resolved.items()} == {
        **{row["ip"]: "found" for row in rows[:40]}, "127.0.0.1": "not_global", "10.0.0.1": "not_global"}
    assert resolved["8.8.8.40"]["rdap_matched_cidr"] == "8.8.8.0/24"
    assert query_kinds(queries) == {"negative": 1, "trie": 1, "networks": 1, "markers": 1, "context": 0, "results": 0}
    assert not any("raw_response" in q for q in queries)
    # A second page of the same run hits the in-process network cache: no network read.
    queries.clear()
    assert enricher.resolve_page(page(env, "8.8.8.7"))["8.8.8.7"]["rdap_lookup_status"] == "found"
    assert query_kinds(queries)["networks"] == 0
    # A fresh resolver (a resume) reads the row once.
    resolver(env).resolve_page(page(env, "8.8.8.7"))
    assert query_kinds(queries)["networks"] == 1


def test_cache_window_is_the_frozen_execution_not_now(environment):
    env = environment
    started = datetime.now(UTC)
    seed_network(env, "8.8.8.1", fetched_at=started - timedelta(days=40))
    stale = resolver(env, started_at=started)  # 30-day window: the network is stale
    resolved = stale.resolve_page(page(env, "8.8.8.8"))
    assert env.calls == ["8.8.8.8"] and resolved["8.8.8.8"]["rdap_lookup_status"] == "found"
    assert stale.cache_hits == 0 and stale.requests == 1
    wide = resolver(env, started_at=started, cache_days=60)
    env.calls.clear()
    # A network fetched after started_at (by this or another execution) is still reusable.
    env.client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    assert wide.resolve_page(page(env, "8.8.8.9"))["8.8.8.9"]["rdap_lookup_status"] == "found"
    assert env.calls == [] and wide.cache_hits == 1


def test_negative_markers_are_honoured_by_the_frozen_start(environment, monkeypatch):
    env = environment
    started = datetime.now(UTC)

    def limited(self, ip):
        env.calls.append(ip)
        raise RdapClientError("rate limit", code="rate_limited", retryable=True, status_code=429)

    monkeypatch.setattr(RdapClient, "lookup_ip", limited)
    first = resolver(env, started_at=started, rate_limit_retry_seconds=3600).resolve_page(page(env, "8.8.8.8"))
    assert first["8.8.8.8"]["rdap_lookup_status"] == "retryable_error"
    assert env.calls == ["8.8.8.8"]
    # Still inside the backoff relative to a later execution's start: served from the marker.
    again = resolver(env, started_at=started + timedelta(minutes=5)).resolve_page(page(env, "8.8.8.8"))
    assert again["8.8.8.8"]["rdap_error_code"] == "rate_limited" and env.calls == ["8.8.8.8"]
    # An execution that starts after retry_after asks again; force_rdap always asks.
    later = resolver(env, started_at=started + timedelta(hours=2)).resolve_page(page(env, "8.8.8.8"))
    assert later["8.8.8.8"]["rdap_lookup_status"] == "retryable_error" and env.calls == ["8.8.8.8", "8.8.8.8"]
    resolver(env, started_at=started, force_rdap=True).resolve_page(page(env, "8.8.8.8"))
    assert env.calls == ["8.8.8.8"] * 3


def test_registry_level_response_answers_only_its_ip(environment, monkeypatch):
    env = environment
    seed_reference_data(env.client)  # ip_registry_ready = 1: classes are decided
    monkeypatch.setattr(
        RdapClient,
        "lookup_ip",
        lambda self, ip: (env.calls.append(ip), response(ip, start="103.0.0.0", end="103.255.255.255", handle="103.0.0.0 - 103.255.255.255", name="APNIC-AP"))[1],
    )
    first = resolver(env)
    resolved = first.resolve_page(page(env, "103.35.64.49"))
    assert resolved["103.35.64.49"]["rdap_lookup_status"] == "found"
    assert resolved["103.35.64.49"]["rdap_name"] == "APNIC-AP"
    assert resolved["103.35.64.49"]["rdap_matched_cidr"] == "103.0.0.0/8"
    assert first.registry_level_responses == 1
    # Stored with the ordinary segment role; its class row keeps it out of the trie.
    assert env.client.execute("SELECT DISTINCT segment_role FROM corpscout.rdap_network_segments") == [("lookup_result",)]
    assert env.client.execute(
        "SELECT network_key, registry_class, covered_rir_blocks FROM corpscout.rdap_network_registry_class_current"
    ) == [("arin:103.0.0.0 - 103.255.255.255", "registry_level", 1)]
    env.client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    assert env.client.execute("SELECT count() FROM corpscout.rdap_network_segments_current") == [(0,)]
    # Another address in the block is not served by the trie nor by the in-run cache.
    second = first.resolve_page(page(env, "103.15.66.50"))
    assert second["103.15.66.50"]["rdap_lookup_status"] == "found"
    assert env.calls == ["103.35.64.49", "103.15.66.50"]
    # The queried address itself is served by its own lookup marker next time.
    third = resolver(env)
    assert third.resolve_page(page(env, "103.35.64.49"))["103.35.64.49"]["rdap_name"] == "APNIC-AP"
    assert env.calls == ["103.35.64.49", "103.15.66.50"] and third.cache_hits == 1
    # A holder registration is classified reusable and serves its neighbours.
    monkeypatch.setattr(
        RdapClient,
        "lookup_ip",
        lambda self, ip: (env.calls.append(ip), response(ip, start="103.35.64.0", end="103.35.67.255"))[1],
    )
    holder = resolver(env)
    assert holder.resolve_page(page(env, "103.35.64.1", "103.35.64.2"))["103.35.64.2"]["rdap_matched_cidr"] == "103.35.64.0/22"
    assert env.calls[-1] == "103.35.64.1" and holder.requests == 1 and holder.cache_hits == 1
    assert env.client.execute(
        "SELECT registry_class FROM corpscout.rdap_network_registry_class_current WHERE network_key = 'arin:TEST-103.35.64.1'"
    ) == [("reusable",)]


def test_misses_reuse_networks_fetched_earlier_in_the_run_and_stop_at_the_budget(environment, monkeypatch):
    env = environment
    queries = []
    execute = Client.execute
    monkeypatch.setattr(Client, "execute", lambda self, query, *a, **k: (queries.append(query), execute(self, query, *a, **k))[1])
    enricher = resolver(env, max_requests=1)
    resolved = enricher.resolve_page(page(env, "8.8.8.8", "8.8.8.9", "1.1.1.1"))
    assert env.calls == ["8.8.8.8"]
    assert resolved["8.8.8.9"]["rdap_matched_cidr"] == "8.8.8.0/24"  # in-run reuse, no HTTP
    assert "1.1.1.1" not in resolved and enricher.budget_reached
    assert enricher.cache_hits == 1 and enricher.requests == 1
    assert query_kinds(queries)["context"] == 1  # one classification round trip per miss
    # The page's markers were written for what was resolved.
    assert env.client.execute(
        "SELECT ip, lookup_status FROM corpscout.rdap_ip_lookup_results_current ORDER BY ip"
    ) == [("8.8.8.8", "found")]


def test_ripe_addresses_use_the_rest_api_and_carry_no_person_data(environment, monkeypatch):
    env = environment
    rdap_calls = []
    monkeypatch.setattr(RdapClient, "lookup_ip", lambda self, ip: (rdap_calls.append(ip), response(ip))[1])
    enricher = resolver(env)
    resolved = enricher.resolve_page(page(env, "5.1.1.1", "5.1.1.2"))
    assert env.calls == ["5.1.1.1"] and rdap_calls == []  # REST once, the neighbour from the in-run cache
    found = resolved["5.1.1.2"]
    assert (found["rdap_rir"], found["rdap_name"], found["rdap_registration_type"], found["rdap_country_code"]) == ("ripe", "RIPE-TEST-NET", "ASSIGNED PA", "SE")
    assert found["rdap_handle"] == "5.1.1.0 - 5.1.1.255" and found["rdap_network_key"] == "ripe:5.1.1.0 - 5.1.1.255"
    assert found["rdap_registrant_handles"] == ["ORG-TEST1-RIPE"] and found["rdap_registrant_names"] == []
    assert found["rdap_self_url"] == "https://rest.db.ripe.net/ripe/inetnum/5.1.1.0%20-%205.1.1.255"
    assert enricher.person_entities_by_registry == {} and enricher.requests_by_registry == {"ripe": 1}
    [(raw,)] = env.client.execute("SELECT raw_response FROM corpscout.rdap_networks")
    assert "descr" not in raw and "admin-c" not in raw and '"source":"ripe-rest"' in raw
    # The root object for unallocated space is a catch-all: one RDAP request follows, once.
    monkeypatch.setattr(
        RipeRestClient,
        "lookup_ip",
        lambda self, ip: (env.calls.append(ip), RdapLookupResponse(rir="ripe", raw_response=ripe_rest.rdap_shape(rest_object(ip, start="0.0.0.0", end="255.255.255.255", netname="IANA-BLK"))))[1],
    )
    resolved = enricher.resolve_page(page(env, "5.9.9.9"))
    assert env.calls == ["5.1.1.1", "5.9.9.9"] and rdap_calls == ["5.9.9.9"]
    assert resolved["5.9.9.9"]["rdap_lookup_status"] == "found" and resolved["5.9.9.9"]["rdap_rir"] == "arin"
    # ripe_rest=false keeps RDAP for RIPE addresses.
    plain = resolver(env, ripe_rest=False)
    plain.resolve_page(page(env, "5.7.7.7"))
    assert rdap_calls == ["5.9.9.9", "5.7.7.7"]


def test_rdap_shape_builds_an_ip_network_without_contacts():
    shaped = ripe_rest.rdap_shape(rest_object("5.1.1.1"))
    assert shaped["objectClassName"] == "ip network" and shaped["ipVersion"] == "v4"
    assert (shaped["handle"], shaped["startAddress"], shaped["endAddress"]) == ("5.1.1.0 - 5.1.1.255", "5.1.1.0", "5.1.1.255")
    assert (shaped["name"], shaped["type"], shaped["country"], shaped["status"]) == ("RIPE-TEST-NET", "ASSIGNED PA", "SE", ["active"])
    assert shaped["entities"] == [{"objectClassName": "entity", "handle": "ORG-TEST1-RIPE", "roles": ["registrant"]}]
    assert shaped["events"] == [
        {"eventAction": "registration", "eventDate": "2010-05-04T10:00:00Z"},
        {"eventAction": "last changed", "eventDate": "2024-01-02T03:04:05Z"},
    ]
    assert shaped["corpscout"] == {"source": "ripe-rest", "flags": "no-referenced", "mnt_by": ["TEST-MNT"]}
    assert "descr" not in str(shaped) and "admin-c" not in str(shaped)
    six = ripe_rest.rdap_shape({
        "type": "inet6num",
        "attributes": {"attribute": [{"name": "inet6num", "value": "2001:638:501::/48"}, {"name": "netname", "value": "UNI-ESSEN"}, {"name": "status", "value": "ASSIGNED"}]},
    })
    assert (six["startAddress"], six["endAddress"], six["ipVersion"], six["entities"], six["events"]) == (
        "2001:638:501::", "2001:638:501:ffff:ffff:ffff:ffff:ffff", "v6", [], [])
    assert ripe_rest.rdap_shape(rest_object("5.1.1.1", org=None))["entities"] == []
    with pytest.raises(ValueError, match="not a network object"):
        ripe_rest.rdap_shape({"type": "route", "attributes": {"attribute": [{"name": "route", "value": "5.0.0.0/8"}]}})


def test_ripe_rest_client_maps_answers_and_errors(monkeypatch):
    class Response:
        def __init__(self, status, payload=None):
            self.status_code = status
            self._payload = payload

        def json(self):
            if self._payload is None:
                raise ValueError("no json")
            return self._payload

    seen = []

    class Session:
        headers = {}

        def get(self, url, *, params, headers, timeout):
            seen.append((url, params, headers["Accept"]))
            return answers.pop(0)

    client = RipeRestClient(user_agent="test", session=Session())
    answers = [Response(200, {"objects": {"object": [{"type": "route", "attributes": {"attribute": []}}, rest_object("5.1.1.1")]}})]
    found = client.lookup_ip("5.1.1.1")
    assert found.rir == "ripe" and found.raw_response["handle"] == "5.1.1.0 - 5.1.1.255"
    assert seen == [(
        ripe_rest.SEARCH_URL,
        [("query-string", "5.1.1.1"), ("flags", "no-referenced"), ("source", "ripe"), ("type-filter", "inetnum"), ("type-filter", "inet6num")],
        "application/json",
    )]
    for status, payload, code, retryable in [
        (404, None, "not_found", False),
        (429, None, "rate_limited", True),
        (503, None, "remote_server", True),
        (403, None, "access_denied", False),
        (418, None, "query_error", False),
        (200, None, "invalid_response", False),
        (200, {"objects": {"object": []}}, "not_found", False),
    ]:
        answers = [Response(status, payload)]
        with pytest.raises(RdapClientError) as raised:
            client.lookup_ip("5.1.1.1")
        assert (raised.value.code, raised.value.retryable) == (code, retryable), status


def test_apnic_addresses_use_whois_r_and_take_the_holder_from_the_first_descr(environment, monkeypatch):
    env = environment
    rdap_calls = []
    monkeypatch.setattr(RdapClient, "lookup_ip", lambda self, ip: (rdap_calls.append(ip), response(ip))[1])
    monkeypatch.setattr(RdapClient, "registry_for", lambda self, ip: "apnic")
    monkeypatch.setattr(ApnicWhoisClient, "query", lambda self, ip: (env.calls.append(ip), APNIC_FPT_ANSWER)[1])
    enricher = resolver(env)
    resolved = enricher.resolve_page(page(env, "103.35.64.49", "103.35.65.7"))
    assert env.calls == ["103.35.64.49"] and rdap_calls == []  # whois once, the neighbour from the in-run cache
    found = resolved["103.35.65.7"]
    assert (found["rdap_rir"], found["rdap_name"], found["rdap_registration_type"], found["rdap_country_code"]) == ("apnic", "FPT-VN", "ALLOCATED PORTABLE", "VN")
    assert found["rdap_handle"] == "103.35.64.0 - 103.35.67.255" and found["rdap_matched_cidr"] == "103.35.64.0/22"
    assert found["rdap_registrant_names"] == ["FPT Telecom"] and found["rdap_registrant_handles"] == []
    assert found["rdap_self_url"] == "https://rdap.apnic.net/ip/103.35.64.0"
    assert found["rdap_last_changed_at"] == datetime(2019, 3, 13, 5, 19, 11, tzinfo=UTC)
    assert enricher.person_entities_by_registry == {} and enricher.requests_by_registry == {"apnic": 1}
    [(raw,)] = env.client.execute("SELECT raw_response FROM corpscout.rdap_networks")
    assert "FHIG1-AP" not in raw and "Duy Tan" not in raw and '"source":"apnic-whois"' in raw


def test_apnic_nir_allocation_objects_fall_back_to_rdap(environment, monkeypatch):
    env = environment
    rdap_calls = []
    monkeypatch.setattr(RdapClient, "lookup_ip", lambda self, ip: (rdap_calls.append(ip), response(ip))[1])
    monkeypatch.setattr(ApnicWhoisClient, "query", lambda self, ip: (env.calls.append(ip), APNIC_JPNIC_ANSWER)[1])
    resolved = resolver(env).resolve_page(page(env, "202.12.14.5"))  # 202/8 is APNIC in the fixture
    assert env.calls == ["202.12.14.5"] and rdap_calls == ["202.12.14.5"]
    assert resolved["202.12.14.5"]["rdap_lookup_status"] == "found" and resolved["202.12.14.5"]["rdap_rir"] == "arin"
    # The rule: the NIR's own object, never the maintainer. FPT's /22 is maintained by VNNIC and is a holder's.
    fpt = apnic_whois.network_object(apnic_whois.parse_answer(APNIC_FPT_ANSWER))
    assert apnic_whois.nir_of("FPT-VN", "FPT Telecom") == "" and not apnic_whois.is_nir_object(apnic_whois.rdap_shape(fpt))
    assert apnic_whois.is_nir_object(apnic_whois.rdap_shape(apnic_whois.network_object(apnic_whois.parse_answer(APNIC_JPNIC_ANSWER))))
    assert apnic_whois.nir_of("JPNIC-NET-JP-ERX", "") == "jpnic"
    assert apnic_whois.nir_of("CIDR-BLK3-TW", "Taiwan Network Information Center") == "twnic"
    assert apnic_whois.nir_of("KORNET", "Korea Telecom") == ""
    assert apnic_whois.nir_of("MEGAEGG", "MEGA EGG") == ""
    # apnic_whois=false keeps RDAP for APNIC addresses.
    resolver(env, apnic_whois=False).resolve_page(page(env, "202.12.9.9"))
    assert env.calls == ["202.12.14.5"] and rdap_calls == ["202.12.14.5", "202.12.9.9"]


def test_apnic_whois_client_parses_answers_and_maps_errors():
    sent = []

    class Socket:
        def __init__(self, answer):
            self.answer = answer.encode()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def settimeout(self, seconds):
            pass

        def sendall(self, data):
            sent.append(data)

        def recv(self, size):
            chunk, self.answer = self.answer[:size], self.answer[size:]
            return chunk

    answers = []

    def connect(address, timeout):
        assert address == ("whois.apnic.net", 43) and timeout == 10.0
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return Socket(answer)

    client = ApnicWhoisClient(connect=connect)
    answers = [APNIC_FPT_ANSWER]
    found = client.lookup_ip("103.35.64.49")
    assert sent == [b"-r 103.35.64.49\r\n"]
    assert found.rir == "apnic" and found.raw_response["handle"] == "103.35.64.0 - 103.35.67.255"
    assert found.raw_response["name"] == "FPT-VN" and found.raw_response["type"] == "ALLOCATED PORTABLE"
    assert found.raw_response["entities"] == [{"objectClassName": "entity", "handle": None, "roles": ["registrant"], "vcardArray": ["vcard", [["version", {}, "text", "4.0"], ["kind", {}, "text", "org"], ["fn", {}, "text", "FPT Telecom"]]]}]
    assert found.raw_response["corpscout"] == {"source": "apnic-whois", "flags": "-r", "nir": "", "mnt_by": ["MAINT-VN-VNNIC"], "mnt_irt": ["IRT-VNNIC-AP"]}
    assert "FHIG1-AP" not in str(found.raw_response) and "Duy Tan" not in str(found.raw_response)
    # Status spellings of the dump are normalised; inet6num ranges are prefixes.
    six = apnic_whois.rdap_shape(apnic_whois.network_object(apnic_whois.parse_answer(
        "inet6num:       2001:200::/35\nnetname:        WIDE-JP\ndescr:          WIDE Project\nstatus:         Allocated non-portable\nsource:         APNIC\n")))
    assert (six["startAddress"], six["endAddress"], six["ipVersion"], six["type"]) == ("2001:200::", "2001:200:1fff:ffff:ffff:ffff:ffff:ffff", "v6", "ALLOCATED NON-PORTABLE")
    assert apnic_whois.rdap_shape([("inetnum", "1.0.0.0 - 1.0.0.255"), ("netname", "X"), ("status", "ASSIGNED  NON-PORTABLE")])["type"] == "ASSIGNED NON-PORTABLE"
    with pytest.raises(ValueError, match="not a network object"):
        apnic_whois.rdap_shape([("route", "1.0.0.0/24")])
    for answer, code, retryable in [
        ("%ERROR:101: no entries found\n", "not_found", False),
        ("%ERROR:201: access denied\n", "rate_limited", True),
        ("%ERROR:305: connection limit\n", "query_error", True),
        ("route:          1.0.0.0/24\norigin:         AS1\nsource:         APNIC\n", "not_found", False),
        (OSError("connection refused"), "transport_error", True),
    ]:
        answers = [answer]
        with pytest.raises(RdapClientError) as raised:
            client.lookup_ip("1.0.0.1")
        assert (raised.value.code, raised.value.retryable) == (code, retryable), answer


def test_person_entities_counts_individual_vcards_nested_included():
    assert enrichment.person_entities({}) == 0
    raw = {
        "entities": [
            {"objectClassName": "entity", "handle": "ORG-A", "roles": ["registrant"], "vcardArray": ["vcard", [["kind", {}, "text", "org"], ["fn", {}, "text", "A GmbH"]]]},
            {
                "objectClassName": "entity",
                "handle": "P1",
                "vcardArray": ["vcard", [["kind", {}, "text", "individual"], ["fn", {}, "text", "A Person"]]],
                "entities": [{"objectClassName": "entity", "handle": "P2", "vcardArray": ["vcard", [["kind", {}, "text", "individual"]]]}],
            },
            {"objectClassName": "entity", "handle": "R1", "vcardArray": ["vcard", [["kind", {}, "text", "group"]]]},
        ]
    }
    assert enrichment.person_entities(raw) == 2


def test_registry_budget_defers_misses_and_frees_after_the_window(environment):
    env = environment
    clock = {"now": 1000.0}
    slept = []

    def sleep(seconds):
        slept.append(seconds)
        clock["now"] += seconds

    enricher = resolver(env, registry_daily_budgets={"arin": 2}, clock=lambda: clock["now"], sleep=sleep)
    resolved = enricher.resolve_page(page(env, "8.8.8.8", "8.8.4.4", "1.1.1.1", "5.1.1.1"))
    assert env.calls == ["8.8.8.8", "8.8.4.4", "5.1.1.1"]  # the third ARIN miss is deferred, RIPE is not budgeted
    assert "1.1.1.1" not in resolved and not enricher.budget_reached
    assert enricher.deferred == {"arin": 1} and enricher.deferrals_by_registry == {"arin": 1}
    assert enricher.requests_by_registry == {"arin": 2, "ripe": 1}
    assert enricher.person_entities_by_registry == {}  # the test responses carry no vCards
    assert enricher.seconds_until_budget_frees() == pytest.approx(86_400)
    clock["now"] += 3600
    assert enricher.resolve_page(page(env, "1.1.1.1")) == {} and enricher.deferred == {"arin": 2}
    assert enricher.wait_for_registry_budget() == pytest.approx(86_400 - 3600)
    assert enricher.deferred == {} and sum(slept) == pytest.approx(86_400 - 3600)
    assert enricher.resolve_page(page(env, "1.1.1.1"))["1.1.1.1"]["rdap_lookup_status"] == "found"
    # Requests other runs made in the last day count against the same window.
    fresh = resolver(env, registry_daily_budgets={"arin": 2}, clock=lambda: clock["now"])
    fresh.seed_registry_usage([("arin", 100.0), ("arin", 50.0), ("ripe", 10.0)])
    assert fresh.resolve_page(page(env, "9.9.9.9")) == {} and fresh.deferred == {"arin": 1}
    assert fresh.resolve_page(page(env, "5.4.4.4"))["5.4.4.4"]["rdap_lookup_status"] == "found"


def test_registry_for_maps_the_bootstrap_endpoint_to_the_registry_name(monkeypatch):
    client = rdap_client.RdapClient(user_agent="test")
    monkeypatch.setattr(client, "_ensure_bootstrapped", lambda: None)
    urls = {
        "5.134.16.1": "https://rdap.db.ripe.net/ip/5.134.16.1",
        "8.8.8.8": "https://rdap.arin.net/registry/ip/8.8.8.8",
        "2001:db8::1": "https://unknown.example/ip/2001:db8::1",
    }
    monkeypatch.setattr(
        rdap_client.whoisit, "build_query", lambda *, query_type, query_value: ("GET", urls[query_value], True)
    )
    assert client.registry_for("5.134.16.1") == "ripe"
    assert client.registry_for("8.8.8.8") == "arin"
    assert client.registry_for("2001:db8::1") == ""

    def refused(**kwargs):
        raise rdap_client.QueryError("You need to load bootstrap data before making any queries")

    monkeypatch.setattr(rdap_client.whoisit, "build_query", refused)
    assert client.registry_for("8.8.8.8") == ""
    assert rdap_client.RIR_BY_HOST["rdap.db.ripe.net"] == "ripe"
```

(The bucket comes from ClickHouse's `cityHash64(ip) % 256` in `page()`; Python has no twin of that hash, which is also why `input_id` is computed in SQL. `registry_for` and both `lookup_ip` methods are stubbed on the classes in the fixture; no test bootstraps whoisit or contacts RIPE.)

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_ip_enrichment_results.py -k "page_resolution or cache_window or negative_markers or registry_level_response or misses_reuse or ripe or rdap_shape or apnic or person_entities or registry_budget or registry_for" -q -p no:cacheprovider`
Expected: FAIL — `ImportError: cannot import name 'apnic_whois' from 'dagster_v3.defs.commoncrawl_rdap'` (collection error; once both modules exist, `AttributeError: RdapClient has no attribute 'registry_for'` from the fixture and `TypeError: RdapEnricher.__init__() got an unexpected keyword argument 'started_at'`).

- [ ] **Step 3: Add `registry_for` to the RDAP client**

In `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/client.py` replace lines 1-20 (the imports) with:

```python
from collections.abc import Mapping
from ipaddress import ip_network
from typing import Any
from urllib.parse import unquote, urlsplit

import requests
import whoisit
from whoisit.bootstrap import BaseBootstrap
from whoisit.errors import (
    ArgumentError,
    BootstrapError,
    ParseError,
    QueryError,
    RateLimitedError,
    RemoteServerError,
    ResourceAccessDeniedError,
    ResourceDoesNotExist,
    UnsupportedError,
)

from dagster_v3.defs.commoncrawl_rdap.rdap import RdapLookupResponse

# RDAP host -> whoisit's registry name ('ripe', 'arin', 'apnic', 'jpnic', ...), the same
# names whoisit reports as `rir` in a parsed response.
RIR_BY_HOST = {
    urlsplit(url).netloc: name for name, url in BaseBootstrap.RIR_RDAP_ENDPOINTS.items()
}
```

After `lookup_up_url` (line 56) add:

```python
    def registry_for(self, ip_address_or_network: str) -> str:
        """The registry whoisit would ask for this address, or '' when it cannot tell.

        Resolved from the IANA bootstrap data already loaded for lookups (no HTTP), so
        the RIPE REST path and the per-registry budget are chosen before a request is sent.
        """
        self._ensure_bootstrapped()
        try:
            _, url, _ = whoisit.build_query(
                query_type="ip", query_value=ip_address_or_network
            )
        except (QueryError, BootstrapError, ArgumentError):
            return ""
        return RIR_BY_HOST.get(urlsplit(url).netloc, "")
```

- [ ] **Step 4: Create the RIPE REST client**

Create `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/ripe_rest.py`:

```python
"""RIPE Database REST lookups without personal data.

RIPE's acceptable use policy limits the personal data sets (person and role objects) one
source address may receive to 1,000 per 24 hours, while queries themselves are unlimited
within reasonable use. RIPE's RDAP `ip` answers embed the contacts as person objects, so
every one of them counts. The REST search with flags=no-referenced (whois -r) returns the
most specific inetnum/inet6num alone: netname, country, status, org, mnt-by, dates and
contact handles, but no person or role object, so nothing counts. The object is reshaped
into the RDAP document normalize_rdap_network expects, with the same handle RIPE's RDAP
uses (the range text), so network keys, segments, classes and results are unchanged.
descr, admin-c, tech-c and remarks are dropped on purpose: no personal data, not even a
name written into descr.
"""

from ipaddress import ip_network
from urllib.parse import quote

import requests

from dagster_v3.defs.commoncrawl_rdap.client import RdapClientError
from dagster_v3.defs.commoncrawl_rdap.rdap import RdapLookupResponse

SEARCH_URL = "https://rest.db.ripe.net/search.json"
OBJECT_URL = "https://rest.db.ripe.net/ripe/{kind}/{key}"
NETWORK_TYPES = ("inetnum", "inet6num")
QUERY_FLAGS = (("flags", "no-referenced"), ("source", "ripe"), ("type-filter", "inetnum"), ("type-filter", "inet6num"))


def rdap_shape(obj: dict) -> dict:
    """An RDAP 'ip network' document built from one REST search object."""
    kind = obj.get("type")
    attributes = [
        (attribute.get("name"), attribute.get("value", ""))
        for attribute in obj.get("attributes", {}).get("attribute", [])
    ]
    values: dict[str, str] = {}
    for name, value in attributes:
        values.setdefault(name, value)
    key = values.get(kind or "")
    if kind not in NETWORK_TYPES or not key:
        raise ValueError(f"not a network object: {kind!r}")
    if kind == "inetnum":
        start, end = (part.strip() for part in key.split("-", 1))
        version = "v4"
    else:
        network = ip_network(key, strict=False)
        start, end, version = str(network[0]), str(network[-1]), "v6"
    events = [
        {"eventAction": action, "eventDate": values[attribute]}
        for attribute, action in (("created", "registration"), ("last-modified", "last changed"))
        if values.get(attribute)
    ]
    entities = (
        [{"objectClassName": "entity", "handle": values["org"], "roles": ["registrant"]}]
        if values.get("org")
        else []
    )
    return {
        "objectClassName": "ip network",
        "handle": key,
        "startAddress": start,
        "endAddress": end,
        "ipVersion": version,
        "name": values.get("netname"),
        "type": values.get("status"),
        "country": values.get("country"),
        "status": ["active"],
        "entities": entities,
        "links": [{"rel": "self", "href": OBJECT_URL.format(kind=kind, key=quote(key, safe=""))}],
        "events": events,
        "port43": "whois.ripe.net",
        "corpscout": {
            "source": "ripe-rest",
            "flags": "no-referenced",
            "mnt_by": [value for name, value in attributes if name == "mnt-by"],
        },
    }


class RipeRestClient:
    """One session; errors use RdapClient's codes."""

    def __init__(self, *, user_agent: str, session: requests.Session | None = None) -> None:
        if user_agent.strip() == "":
            raise ValueError("user_agent must not be empty")
        self._owns_session = session is None
        self._session = session if session is not None else requests.Session()
        self._session.headers["User-Agent"] = user_agent.strip()

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def lookup_ip(self, ip: str) -> RdapLookupResponse:
        try:
            response = self._session.get(
                SEARCH_URL,
                params=[("query-string", ip), *QUERY_FLAGS],
                headers={"Accept": "application/json"},
                timeout=(10, 30),
            )
        except requests.RequestException as error:
            raise RdapClientError(str(error), code="transport_error", retryable=True) from error
        status = response.status_code
        if status == 404:
            raise RdapClientError("no RIPE object", code="not_found", retryable=False, status_code=404)
        if status == 429:
            raise RdapClientError("RIPE REST rate limit", code="rate_limited", retryable=True, status_code=429)
        if status >= 500:
            raise RdapClientError(f"RIPE REST {status}", code="remote_server", retryable=True, status_code=status)
        if status == 403:
            raise RdapClientError("RIPE REST access denied", code="access_denied", retryable=False, status_code=403)
        if status != 200:
            raise RdapClientError(f"RIPE REST {status}", code="query_error", retryable=False, status_code=status)
        try:
            objects = response.json().get("objects", {}).get("object", [])
        except ValueError as error:
            raise RdapClientError("RIPE REST answer is not JSON", code="invalid_response", retryable=False) from error
        network = next((item for item in objects if item.get("type") in NETWORK_TYPES), None)
        if network is None:
            raise RdapClientError("no network object in the RIPE answer", code="not_found", retryable=False, status_code=200)
        try:
            return RdapLookupResponse(rir="ripe", raw_response=rdap_shape(network))
        except ValueError as error:
            raise RdapClientError(str(error), code="invalid_response", retryable=False) from error
```

(A plain `requests.Session`, like `RdapClient`: dlt's retrying session would swallow the 429s and 5xx that the error mapping must see.)

- [ ] **Step 5: Create the APNIC whois client**

Create `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/apnic_whois.py`:

```python
"""APNIC whois over port 43 with -r: the holder object without personal data.

APNIC's RDAP answers embed contact entities; its whois service with the -r flag returns
the most specific inetnum/inet6num with contact handles only (no person, role or irt
objects), followed by matching route objects, which are ignored. The HTTP gateway
(wq.apnic.net) does not honour -r (verified 2026-09-26: it returned an irt object with an
address), so this client speaks the port-43 protocol directly: one TCP connection per
query, `-r <ip>`, read to EOF. The object is reshaped into the RDAP document
normalize_rdap_network reads. Holder name: APNIC objects rarely carry org: (2.4% of the
inetnum objects in the 2026-09-25 dump), so the FIRST descr line is the holder name;
further descr lines (addresses) and every contact handle are dropped. NIR-managed space:
when the answer is the NIR's own allocation object (netname or first descr naming JPNIC,
KRNIC, TWNIC, IDNIC, CNNIC, IRINN or VNNIC) the end holder lives in the NIR's database and
the caller falls back to RDAP, which the IANA bootstrap routes to the NIR server. mnt-by
alone decides nothing: FPT's 103.35.64.0/22 is maintained by MAINT-VN-VNNIC and IS the
holder's allocation.
"""

import re
import socket
from collections.abc import Mapping
from ipaddress import ip_network

from dagster_v3.defs.commoncrawl_rdap.client import RdapClientError
from dagster_v3.defs.commoncrawl_rdap.rdap import RdapLookupResponse

WHOIS_HOST = "whois.apnic.net"
WHOIS_PORT = 43
CONNECT_TIMEOUT = 10.0
READ_TIMEOUT = 30.0
NETWORK_TYPES = ("inetnum", "inet6num")
OBJECT_URL = "https://rdap.apnic.net/ip/{start}"
# The NIRs' own allocation objects (not the holders' allocations the NIRs maintain).
NIR_NETNAME_PREFIXES = {
    "JPNIC": "jpnic",
    "KRNIC": "krnic",
    "TWNIC": "twnic",
    "IDNIC": "idnic",
    "CNNIC": "cnnic",
    "IRINN": "irinn",
    "VNNIC": "vnnic",
}
NIR_DESCR_PHRASES = {
    "JAPAN NETWORK INFORMATION CENTER": "jpnic",
    "KOREA NETWORK INFORMATION CENTER": "krnic",
    "KOREA INTERNET": "krnic",
    "TAIWAN NETWORK INFORMATION CENTER": "twnic",
    "INDONESIA NETWORK INFORMATION CENTER": "idnic",
    "CHINA INTERNET NETWORK INFORMATION CENTER": "cnnic",
    "INDIAN REGISTRY FOR INTERNET NAMES AND NUMBERS": "irinn",
    "VIETNAM INTERNET NETWORK INFORMATION CENTRE": "vnnic",
}
_ERROR = re.compile(r"^%\s*ERROR:\s*(\d+)", re.MULTILINE)


def parse_answer(text: str) -> list[list[tuple[str, str]]]:
    """RPSL objects of a whois answer as lists of (attribute, value); % lines are comments."""
    objects: list[list[tuple[str, str]]] = []
    block: list[tuple[str, str]] = []
    for raw in text.splitlines():
        line = raw.rstrip("\r")
        if not line.strip():
            if block:
                objects.append(block)
                block = []
            continue
        if line.startswith("%"):
            continue
        if line[0] in " \t+" and block:
            block[-1] = (block[-1][0], (block[-1][1] + " " + line.strip(" \t+")).strip())
        else:
            key, _, value = line.partition(":")
            block.append((key.strip().lower(), value.strip()))
    if block:
        objects.append(block)
    return objects


def network_object(objects: list[list[tuple[str, str]]]) -> list[tuple[str, str]] | None:
    """The first inetnum/inet6num object: whois answers the most specific one first."""
    return next((obj for obj in objects if obj and obj[0][0] in NETWORK_TYPES), None)


def nir_of(netname: str, descr: str) -> str:
    """The NIR whose own allocation object this is, or '' for a holder's object."""
    for prefix, nir in NIR_NETNAME_PREFIXES.items():
        if netname.upper().startswith(prefix):
            return nir
    upper = descr.upper()
    for phrase, nir in NIR_DESCR_PHRASES.items():
        if phrase in upper:
            return nir
    return ""


def rdap_shape(obj: list[tuple[str, str]]) -> dict:
    """An RDAP 'ip network' document built from one whois object."""
    kind, key = obj[0]
    if kind not in NETWORK_TYPES or not key:
        raise ValueError(f"not a network object: {kind!r}")
    values: dict[str, str] = {}
    descr: list[str] = []
    for name, value in obj:
        if name == "descr":
            descr.append(value)
        else:
            values.setdefault(name, value)
    if kind == "inetnum":
        start, end = (part.strip() for part in key.split("-", 1))
        version = "v4"
    else:
        network = ip_network(key, strict=False)
        start, end, version = str(network[0]), str(network[-1]), "v6"
    holder = descr[0] if descr else values.get("org")
    status = " ".join(values.get("status", "").upper().split())
    entities = []
    if holder:
        entities.append(
            {
                "objectClassName": "entity",
                "handle": values.get("org"),
                "roles": ["registrant"],
                "vcardArray": ["vcard", [["version", {}, "text", "4.0"], ["kind", {}, "text", "org"], ["fn", {}, "text", holder]]],
            }
        )
    events = [
        {"eventAction": action, "eventDate": values[attribute]}
        for attribute, action in (("created", "registration"), ("last-modified", "last changed"))
        if values.get(attribute)
    ]
    return {
        "objectClassName": "ip network",
        "handle": key,
        "startAddress": start,
        "endAddress": end,
        "ipVersion": version,
        "name": values.get("netname"),
        "type": status or None,
        "country": values.get("country"),
        "status": ["active"],
        "entities": entities,
        "links": [{"rel": "self", "href": OBJECT_URL.format(start=start)}],
        "events": events,
        "port43": WHOIS_HOST,
        "corpscout": {
            "source": "apnic-whois",
            "flags": "-r",
            "nir": nir_of(values.get("netname", ""), descr[0] if descr else ""),
            "mnt_by": [value for name, value in obj if name == "mnt-by"],
            "mnt_irt": [value for name, value in obj if name == "mnt-irt"],
        },
    }


def is_nir_object(raw: Mapping) -> bool:
    return bool(raw.get("corpscout", {}).get("nir"))


class ApnicWhoisClient:
    """One TCP connection per query to whois.apnic.net:43 with -r; errors use RdapClient's codes."""

    def __init__(self, *, host: str = WHOIS_HOST, port: int = WHOIS_PORT, connect=socket.create_connection) -> None:
        self._host, self._port, self._connect = host, port, connect

    def close(self) -> None:
        return None  # nothing is kept open between queries

    def query(self, ip: str) -> str:
        try:
            with self._connect((self._host, self._port), timeout=CONNECT_TIMEOUT) as sock:
                sock.settimeout(READ_TIMEOUT)
                sock.sendall(f"-r {ip}\r\n".encode("ascii"))
                chunks = []
                while chunk := sock.recv(65536):
                    chunks.append(chunk)
        except (OSError, ValueError) as error:
            raise RdapClientError(str(error), code="transport_error", retryable=True) from error
        return b"".join(chunks).decode("utf-8", errors="replace")

    def lookup_ip(self, ip: str) -> RdapLookupResponse:
        text = self.query(ip)
        error = _ERROR.search(text)
        if error is not None:
            number = int(error.group(1))
            if number == 101:
                raise RdapClientError("no entries found", code="not_found", retryable=False)
            if number == 201:
                # APNIC answers 201 when its query limit is hit; treated as a rate limit.
                raise RdapClientError("APNIC whois access denied", code="rate_limited", retryable=True)
            raise RdapClientError(f"APNIC whois error {number}", code="query_error", retryable=number >= 300)
        obj = network_object(parse_answer(text))
        if obj is None:
            raise RdapClientError("no network object in the APNIC answer", code="not_found", retryable=False)
        try:
            return RdapLookupResponse(rir="apnic", raw_response=rdap_shape(obj))
        except ValueError as error:
            raise RdapClientError(str(error), code="invalid_response", retryable=False) from error
```

- [ ] **Step 6: Rewrite `enrichment.py`**

Replace the whole of `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py` with:

```python
"""Shared IP lookups: GeoIP per address, RDAP resolved one page at a time.

A page's ClickHouse work is a fixed number of round trips whatever its size: one
negative-cache read, one trie lookup, one read of the network rows the page needs
(never raw_response) and one insert of the page's lookup markers. Registry requests
happen only for misses; each miss also costs the registry-class context query
(commoncrawl_rdap/registry.py), so registry-level and unallocated answers are stored
for their address only. Freshness is judged against the frozen execution start, so a
resume gives the same answers.

RIPE addresses are resolved through the RIPE Database REST search and APNIC addresses
through APNIC's port-43 whois with -r, both without personal data (commoncrawl_rdap/
ripe_rest.py and apnic_whois.py; a placeholder or an NIR's own object falls back to RDAP);
every other registry through RDAP. An optional
rolling 24-hour request budget per registry defers misses instead of exceeding it; the
loop waits for the window only when nothing else remains.
"""

from collections import OrderedDict, deque
from collections.abc import Callable, Mapping
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from ipaddress import ip_address, ip_network
from time import monotonic, sleep
from uuid import UUID

import dagster as dg
import maxminddb
from netaddr import iprange_to_cidrs
from pydantic import Field, field_validator

from dagster_v3.defs.commoncrawl_geoip.maxmind import (
    build_geoip_enrichment,
    classify_ip_scope,
    lookup_maxmind_record,
)
from dagster_v3.defs.commoncrawl_rdap.apnic_whois import ApnicWhoisClient, is_nir_object
from dagster_v3.defs.commoncrawl_rdap.assets import (
    RDAP_LOOKUP_INSERT_SQL,
    RDAP_NETWORK_INSERT_SQL,
    RDAP_SEGMENT_INSERT_SQL,
)
from dagster_v3.defs.commoncrawl_rdap.client import RdapClient, RdapClientError
from dagster_v3.defs.commoncrawl_rdap.registry import (
    REGISTRY_CLASS_INSERT_SQL,
    RegistryClassification,
    classify_registration,
)
from dagster_v3.defs.commoncrawl_rdap.rdap import (
    NormalizedRdapNetwork,
    RdapLookupResponse,
    RdapNetwork,
    is_registry_catch_all,
    normalize_rdap_network,
)
from dagster_v3.defs.commoncrawl_rdap.ripe_rest import RipeRestClient

NEGATIVE_STATUSES = {"not_found", "unsupported", "terminal_error"}
# Every rdap_networks column except raw_response; the cache never loads the JSON.
NETWORK_COLUMNS = (
    "network_key",
    "rir",
    "handle",
    "ip_version",
    "start_address",
    "end_address",
    "name",
    "registration_type",
    "country_code",
    "status",
    "registrant_handles",
    "registrant_names",
    "parent_network_key",
    "parent_handle",
    "self_url",
    "up_url",
    "registration_date",
    "last_changed_at",
    "response_sha256",
    "fetched_at",
)
WRITE_SETTINGS = {"async_insert": 1, "wait_for_async_insert": 1}
BUDGET_WINDOW_SECONDS = 86_400


class RequestBudgetReached(Exception):
    """Kept for results.py until Task 5 replaces it with RdapEnricher.budget_reached."""


class IpEnrichmentResultsConfig(dg.Config):
    task_id: str = Field(description="IP enrichment draft (task) UUID.")
    execution_id: str | None = Field(
        default=None,
        description="Saved execution to resume. Omit to start, or to resume the task's saved execution.",
    )
    batch_size: int = Field(default=250, ge=1, le=10_000)
    max_requests: int | None = Field(
        default=250,
        ge=1,
        description="Registry HTTP request budget for this run, including parents. Null processes the whole task.",
    )
    request_delay_seconds: float = Field(default=1.0, ge=0, le=60)
    registry_daily_budgets: dict[str, int] = Field(
        default_factory=dict,
        description="Optional rolling 24-hour request budget per registry, keyed by whoisit's "
        "registry names (ripe, arin, apnic, lacnic, afrinic, jpnic, ...). A miss of a registry "
        "at its budget is deferred, never failed; the run waits when nothing else remains.",
    )
    ripe_rest: bool = Field(
        default=True,
        description="Resolve RIPE addresses through the RIPE Database REST search with "
        "no-referenced (no person or role objects) instead of RDAP.",
    )
    apnic_whois: bool = Field(
        default=True,
        description="Resolve APNIC addresses through APNIC's port-43 whois with -r (contact "
        "handles only) instead of RDAP; an NIR's own allocation object falls back to RDAP.",
    )
    parent_depth: int = Field(default=1, ge=0, le=5)
    rdap_cache_days: int = Field(default=30, ge=1)
    force_rdap: bool = False
    rate_limit_retry_seconds: int = Field(default=3600, ge=1)
    transient_retry_seconds: int = Field(default=900, ge=1)

    @field_validator("task_id", "execution_id")
    @classmethod
    def uuid_identity(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

    @field_validator("registry_daily_budgets")
    @classmethod
    def positive_budgets(cls, value: dict[str, int]) -> dict[str, int]:
        budgets = {}
        for registry, budget in value.items():
            if not registry.strip() or budget < 1:
                raise ValueError("registry_daily_budgets needs registry names and budgets >= 1")
            budgets[registry.strip().lower()] = budget
        return budgets


def geoip_result(ip, city_reader, asn_reader, *, checked_at, retry_seconds):
    address = ip_address(ip)
    city_meta, asn_meta = city_reader.metadata(), asn_reader.metadata()
    errors = {}
    lookups = {}
    for component, reader in (("city", city_reader), ("asn", asn_reader)):
        lookups[component] = None
        if classify_ip_scope(address) == "global":
            try:
                lookups[component] = lookup_maxmind_record(reader, address)
            except ValueError, maxminddb.InvalidDatabaseError:
                errors[component] = "maxmind_lookup_error"
    result = asdict(
        build_geoip_enrichment(
            bucket=0,
            address=address,
            city_lookup=lookups["city"],
            asn_lookup=lookups["asn"],
            city_build_epoch=datetime.fromtimestamp(city_meta.build_epoch, UTC),
            asn_build_epoch=datetime.fromtimestamp(asn_meta.build_epoch, UTC),
            enriched_at=checked_at,
        )
    )
    for key in ("ip", "ip_version", "bucket", "enriched_at"):
        del result[key]
    for component in ("city", "asn"):
        result[f"{component}_checked_at"] = checked_at
        result[f"{component}_error_code"] = errors.get(component)
        result[f"{component}_retry_after"] = None
        if component in errors:
            result[f"{component}_lookup_status"] = "retryable_error"
            result[f"{component}_retry_after"] = checked_at + timedelta(
                seconds=retry_seconds
            )
    return result


def rdap_result(
    *, status, checked_at, error_code=None, retry_after=None, network=None, cidr=None
):
    result = dict.fromkeys(
        (
            "rdap_network_key",
            "rdap_matched_cidr",
            "rdap_rir",
            "rdap_handle",
            "rdap_start_address",
            "rdap_end_address",
            "rdap_name",
            "rdap_registration_type",
            "rdap_country_code",
            "rdap_parent_network_key",
            "rdap_parent_handle",
            "rdap_self_url",
            "rdap_registration_date",
            "rdap_last_changed_at",
        )
    )
    result.update(
        rdap_lookup_status=status,
        rdap_checked_at=checked_at,
        rdap_error_code=error_code,
        rdap_retry_after=retry_after,
        rdap_statuses=[],
        rdap_registrant_handles=[],
        rdap_registrant_names=[],
    )
    if network is not None:
        for field in (
            "network_key",
            "rir",
            "handle",
            "start_address",
            "end_address",
            "name",
            "registration_type",
            "country_code",
            "parent_network_key",
            "parent_handle",
            "self_url",
            "registration_date",
            "last_changed_at",
        ):
            result[f"rdap_{field}"] = getattr(network, field)
        result.update(
            rdap_matched_cidr=cidr,
            rdap_statuses=list(network.status),
            rdap_registrant_handles=list(network.registrant_handles),
            rdap_registrant_names=list(network.registrant_names),
        )
    return result


def matching_cidr(normalized: NormalizedRdapNetwork, address) -> str | None:
    return next(
        (s.cidr for s in normalized.segments if address in ip_network(s.cidr)), None
    )


def cidr_containing(start: str, end: str, address) -> str | None:
    """The exact CIDR fragment of an inclusive range that holds ``address`` (None outside it)."""
    return next(
        (
            str(cidr)
            for cidr in iprange_to_cidrs(start, end)
            if cidr.version == address.version and cidr.first <= int(address) <= cidr.last
        ),
        None,
    )


def cached_network_row(row) -> RdapNetwork:
    """A network as read from the cache; raw_response is never loaded."""
    values = dict(zip(NETWORK_COLUMNS, row, strict=True))
    for key in ("status", "registrant_handles", "registrant_names"):
        values[key] = tuple(values[key])
    return RdapNetwork(raw_response="", **values)


def person_entities(raw: Mapping) -> int:
    """Entities whose vCard is of kind 'individual', nested included.

    RIPE counts person objects against its daily limit; RDAP answers of the other
    registries carry them too. An upper bound (RIPE marks maintainers 'individual' as well).
    """
    count = 0
    pending = list(raw.get("entities") or [])
    while pending:
        entity = pending.pop()
        if not isinstance(entity, Mapping):
            continue
        vcard = entity.get("vcardArray")
        if isinstance(vcard, list) and len(vcard) == 2 and isinstance(vcard[1], list):
            if any(
                isinstance(item, list) and len(item) == 4 and item[0] == "kind" and item[3] == "individual"
                for item in vcard[1]
            ):
                count += 1
        pending.extend(entity.get("entities") or [])
    return count


def _clickhouse_time(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")


class RdapEnricher:
    """Resolve registry coverage for a page of addresses with a bounded number of round trips."""

    def __init__(
        self,
        client,
        rdap: RdapClient,
        ripe: RipeRestClient,
        apnic: ApnicWhoisClient,
        config: IpEnrichmentResultsConfig,
        log,
        *,
        started_at: datetime,
        cache_cutoff: datetime,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], None] | None = None,
    ):
        self.client = client
        self.rdap = rdap
        self.ripe = ripe
        self.apnic = apnic
        self.config = config
        self.log = log
        self.started_at = started_at
        self.cache_cutoff = cache_cutoff
        # Resolved at construction so tests can patch the module names.
        self._clock = clock or globals()["monotonic"]
        self._sleep = sleep or globals()["sleep"]
        self.requests = 0
        self.cache_hits = 0
        self.networks_written = 0
        self.parent_failures = 0
        self.registry_level_responses = 0
        self.budget_reached = False
        self.requests_by_registry: dict[str, int] = {}
        self.person_entities_by_registry: dict[str, int] = {}
        self.deferrals_by_registry: dict[str, int] = {}
        self.deferred: dict[str, int] = {}  # since the last reset_pass()
        # Reusable networks fetched over HTTP in this run, checked before any request.
        self.recent: OrderedDict[str, NormalizedRdapNetwork] = OrderedDict()
        # Fresh network rows read from ClickHouse, keyed by network_key.
        self.cached: OrderedDict[str, RdapNetwork] = OrderedDict()
        # Monotonic send times per registry inside the rolling window, oldest first.
        self._sent: dict[str, deque[float]] = {}

    # --- page resolution -------------------------------------------------------

    def resolve_page(self, rows: list[dict]) -> dict[str, dict]:
        """Registry fields per address of the page.

        An address is absent when its registry's daily budget deferred it or when the
        run's max_requests budget ran out (``budget_reached`` is then True).
        """
        addresses = {row["ip"]: ip_address(row["ip"]) for row in rows}
        buckets = {row["ip"]: row["bucket"] for row in rows}
        results: dict[str, dict] = {}
        markers: list[tuple] = []
        pending: list[str] = []
        for ip, address in addresses.items():
            if classify_ip_scope(address) != "global":
                results[ip] = rdap_result(status="not_global", checked_at=datetime.now(UTC))
                markers.append(self._marker(ip, address, buckets[ip], results[ip]))
            else:
                pending.append(ip)
        if pending and not self.config.force_rdap:
            hits: dict[str, str] = {}
            for ip, status, network_key, code, retry_after, checked_at, fresh, backoff in (
                self._markers_of(pending, addresses, buckets)
            ):
                if status == "retryable_error" and backoff:
                    results[ip] = rdap_result(
                        status=status, checked_at=checked_at, error_code=code, retry_after=retry_after
                    )
                elif status in NEGATIVE_STATUSES and fresh:
                    results[ip] = rdap_result(
                        status="terminal_error" if status == "unsupported" else status,
                        checked_at=checked_at,
                        error_code=code,
                        retry_after=retry_after,
                    )
                elif status == "found" and network_key and fresh:
                    hits[ip] = network_key  # a per-address answer, e.g. a registry-level block
                else:
                    continue
                if ip in results:
                    self.cache_hits += 1
            pending = [ip for ip in pending if ip not in results and ip not in hits]
            for ip, network_key in self._trie(pending, addresses):
                if network_key:
                    hits[ip] = network_key
            pending = [ip for ip in pending if ip not in hits]
            self._load_networks({key for key in hits.values() if key not in self.cached})
            for ip, network_key in hits.items():
                network = self.cached.get(network_key)
                cidr = (
                    cidr_containing(network.start_address, network.end_address, addresses[ip])
                    if network is not None
                    else None
                )
                if cidr is None:  # stale, gone or not containing the address: ask again
                    pending.append(ip)
                    continue
                results[ip] = rdap_result(
                    status="found", checked_at=network.fetched_at, network=network, cidr=cidr
                )
                self.cache_hits += 1
        for ip in pending:
            recent = self._recent_match(addresses[ip])
            if recent is not None:  # fetched earlier in this run: no HTTP, no marker
                normalized, cidr = recent
                self.cache_hits += 1
                results[ip] = rdap_result(
                    status="found", checked_at=normalized.network.fetched_at, network=normalized.network, cidr=cidr
                )
                continue
            if self._budget_exhausted():
                self.budget_reached = True
                break
            result = self._request_ip(ip, addresses[ip], buckets[ip], markers)
            if result is not None:
                results[ip] = result
        if markers:
            self.client.execute(RDAP_LOOKUP_INSERT_SQL, markers, settings=WRITE_SETTINGS)
        return results

    def _markers_of(self, ips, addresses, buckets):
        return self.client.execute(
            """SELECT ip, lookup_status, ifNull(network_key, ''), error_code, retry_after, queried_at,
                queried_at >= toDateTime64(%(cutoff)s, 6, 'UTC') AS fresh,
                ifNull(retry_after > toDateTime64(%(started)s, 6, 'UTC'), 0) AS backoff
            FROM corpscout.rdap_ip_lookup_results_current
            WHERE (bucket, ip_version, ip) IN %(keys)s""",
            {
                "keys": tuple((buckets[ip], addresses[ip].version, ip) for ip in ips),
                "cutoff": _clickhouse_time(self.cache_cutoff),
                "started": _clickhouse_time(self.started_at),
            },
        )

    def _trie(self, ips, addresses):
        if not ips:
            return []
        return self.client.execute(
            """SELECT ip, dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv4(ip)), '')
            FROM (SELECT arrayJoin(%(v4)s) AS ip)
            UNION ALL
            SELECT ip, dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv6(ip)), '')
            FROM (SELECT arrayJoin(%(v6)s) AS ip)""",
            {
                "v4": [ip for ip in ips if addresses[ip].version == 4],
                "v6": [ip for ip in ips if addresses[ip].version == 6],
            },
        )

    def _load_networks(self, keys: set[str]) -> None:
        if not keys:
            return
        rows = self.client.execute(
            f"""SELECT {", ".join(NETWORK_COLUMNS)} FROM corpscout.rdap_networks_current
            WHERE network_key IN %(keys)s AND fetched_at >= toDateTime64(%(cutoff)s, 6, 'UTC')""",
            {"keys": tuple(keys), "cutoff": _clickhouse_time(self.cache_cutoff)},
        )
        for row in rows:
            network = cached_network_row(row)
            self.cached[network.network_key] = network
            self.cached.move_to_end(network.network_key)
        while len(self.cached) > 4096:
            self.cached.popitem(last=False)

    # --- registry budgets -------------------------------------------------------

    def seed_registry_usage(self, rows: list[tuple[str, float]]) -> None:
        """Requests any run made in the last day (registry, seconds ago), oldest first."""
        now = self._clock()
        for registry, seconds_ago in rows:
            if seconds_ago < BUDGET_WINDOW_SECONDS:
                self._sent.setdefault(registry, deque()).append(now - seconds_ago)

    def _window(self, registry: str) -> deque[float]:
        times = self._sent.setdefault(registry, deque())
        horizon = self._clock() - BUDGET_WINDOW_SECONDS
        while times and times[0] <= horizon:
            times.popleft()
        return times

    def _over_budget(self, registry: str) -> bool:
        budget = self.config.registry_daily_budgets.get(registry)
        return budget is not None and len(self._window(registry)) >= budget

    def _defer(self, registry: str) -> None:
        self.deferred[registry] = self.deferred.get(registry, 0) + 1
        self.deferrals_by_registry[registry] = self.deferrals_by_registry.get(registry, 0) + 1

    def reset_pass(self) -> None:
        self.deferred = {}

    def seconds_until_budget_frees(self) -> float:
        """Seconds until a deferred registry may be asked again (0 when none is deferred).

        Waits for an hour's share of the registry's budget (at least one request), so
        the pass that follows is worth its ClickHouse queries.
        """
        waits = []
        for registry, deferred in self.deferred.items():
            budget = self.config.registry_daily_budgets.get(registry)
            times = self._window(registry)
            if budget is None or len(times) < budget:
                return 0.0
            slots = max(1, min(deferred, budget // 24))
            waits.append(times[len(times) - budget + slots - 1] + BUDGET_WINDOW_SECONDS - self._clock())
        return max(0.0, min(waits)) if waits else 0.0

    def wait_for_registry_budget(self) -> float:
        """Sleep, in slices of at most a minute, until a deferred registry frees slots."""
        total = self.seconds_until_budget_frees()
        waited = 0.0
        while waited < total:
            step = min(60.0, total - waited)
            self._sleep(step)
            waited += step
            if int(waited) % 600 == 0:
                self.log.info("Registry budget wait: %.0f of %.0f s", waited, total)
        self.reset_pass()
        return waited

    # --- misses ---------------------------------------------------------------

    def _budget_exhausted(self) -> bool:
        return (
            self.config.max_requests is not None
            and self.requests >= self.config.max_requests
        )

    def _request(self, target: str, *, registry: str, rir: str | None = None, source: str = "rdap") -> RdapLookupResponse:
        """One paced request through the registry's source: 'rdap', 'ripe_rest' or 'apnic_whois'."""
        if self.requests and self.config.request_delay_seconds:
            self._sleep(self.config.request_delay_seconds)
        self.requests += 1
        self._sent.setdefault(registry, deque()).append(self._clock())
        self.requests_by_registry[registry] = self.requests_by_registry.get(registry, 0) + 1
        if rir is not None:
            response = self.rdap.lookup_up_url(target, rir=rir)
        elif source == "ripe_rest":
            response = self.ripe.lookup_ip(target)
        elif source == "apnic_whois":
            response = self.apnic.lookup_ip(target)
        else:
            response = self.rdap.lookup_ip(target)
        persons = person_entities(response.raw_response)
        if persons:
            self.person_entities_by_registry[registry] = self.person_entities_by_registry.get(registry, 0) + persons
        return response

    def _persist(
        self, normalized: NormalizedRdapNetwork, classification: RegistryClassification | None = None
    ) -> None:
        self.client.execute(
            RDAP_NETWORK_INSERT_SQL,
            [normalized.network.clickhouse_values()],
            settings=WRITE_SETTINGS,
        )
        if classification is not None and classification.registry_class != "unknown":
            # The class row lands before the segments: the trie source (migration 000451)
            # never sees a segment whose class it does not know.
            self.client.execute(
                REGISTRY_CLASS_INSERT_SQL,
                [
                    classification.clickhouse_values(
                        normalized.network.network_key, normalized.network.fetched_at
                    )
                ],
                settings=WRITE_SETTINGS,
            )
        self.client.execute(
            RDAP_SEGMENT_INSERT_SQL,
            [segment.clickhouse_values() for segment in normalized.segments],
            settings=WRITE_SETTINGS,
        )
        self.networks_written += 1

    def _remember(self, normalized: NormalizedRdapNetwork) -> None:
        self.recent[normalized.network.network_key] = normalized
        self.recent.move_to_end(normalized.network.network_key)
        if len(self.recent) > 1024:
            self.recent.popitem(last=False)

    def _recent_match(self, address):
        matches = []
        for normalized in self.recent.values():
            cidr = matching_cidr(normalized, address)
            if cidr is not None:
                matches.append((ip_network(cidr).prefixlen, normalized.network.fetched_at, normalized.network.network_key, normalized, cidr))
        if not matches:
            return None
        _, _, _, normalized, cidr = max(matches, key=lambda match: match[:3])
        return normalized, cidr

    def _marker(self, ip, address, bucket, result) -> tuple:
        return (
            bucket,
            ip,
            address.version,
            result["rdap_lookup_status"],
            result["rdap_network_key"],
            result["rdap_error_code"],
            result["rdap_retry_after"],
            result["rdap_checked_at"],
        )

    def _request_ip(self, ip, address, bucket, markers: list[tuple]) -> dict | None:
        registry = self.rdap.registry_for(ip)
        if self._over_budget(registry):
            self._defer(registry)
            return None
        source = "rdap"
        if registry == "ripe" and self.config.ripe_rest:
            source = "ripe_rest"
        elif registry == "apnic" and self.config.apnic_whois:
            source = "apnic_whois"
        try:
            response = self._request(ip, registry=registry, source=source)
            checked_at = datetime.now(UTC)
            direct = normalize_rdap_network(response, fetched_at=checked_at, segment_role="lookup_result")
            if source != "rdap" and (
                is_registry_catch_all(direct) or is_nir_object(response.raw_response)
            ):
                # RIPE's root object or APNIC's placeholder for unallocated / non-authoritative
                # space, or an NIR's own allocation object: RDAP, routed by the IANA bootstrap
                # (which redirects to the NIR server), knows the holder. One request past the
                # budget at most.
                self.log.info(
                    "%s answer for %s is %s; asking RDAP",
                    source,
                    ip,
                    "an NIR object (nir_fallback)" if is_nir_object(response.raw_response) else "a catch-all",
                )
                response = self._request(ip, registry=registry, source="rdap")
                checked_at = datetime.now(UTC)
                direct = normalize_rdap_network(response, fetched_at=checked_at, segment_role="lookup_result")
            cidr = matching_cidr(direct, address)
            if is_registry_catch_all(direct):
                raise RdapClientError(
                    "Universal registry coverage", code="registry_catch_all", retryable=False
                )
            if cidr is None:
                raise RdapClientError(
                    "Response range does not contain requested IP", code="range_mismatch", retryable=False
                )
        except (RdapClientError, ValueError) as error:
            checked_at = datetime.now(UTC)
            if isinstance(error, RdapClientError):
                code = error.code
                status = "retryable_error" if error.retryable else "terminal_error"
                if code == "not_found":
                    status = "not_found"
            else:
                code, status = "invalid_response", "terminal_error"
            retry_after = None
            if status == "retryable_error":
                seconds = (
                    self.config.rate_limit_retry_seconds
                    if code == "rate_limited"
                    else self.config.transient_retry_seconds
                )
                retry_after = checked_at + timedelta(seconds=seconds)
            result = rdap_result(
                status=status, checked_at=checked_at, error_code=code, retry_after=retry_after
            )
            markers.append(self._marker(ip, address, bucket, result))
            return result
        # Coverage and its class are durable before any exact-IP outcome refers to them.
        # A registry-level or unallocated registration is stored for this address only:
        # the trie excludes it by class (migration 000451) and the in-run cache never holds it.
        classification = classify_registration(self.client, direct.network)
        self._persist(direct, classification)
        if classification.reusable:
            self._remember(direct)
        else:
            self.registry_level_responses += 1
            self.log.info(
                "Registration %s is %s; it answers only %s",
                direct.network.network_key,
                classification.registry_class,
                ip,
            )
        current = direct
        visited = {direct.network.network_key}
        for _ in range(self.config.parent_depth):
            if (
                current.network.up_url is None
                or self._budget_exhausted()
                or self._over_budget(current.network.rir)  # parents are optional
            ):
                break
            try:
                parent = normalize_rdap_network(
                    self._request(current.network.up_url, registry=current.network.rir, rir=current.network.rir),
                    fetched_at=datetime.now(UTC),
                    segment_role="parent",
                )
                if parent.network.network_key in visited:
                    break
                visited.add(parent.network.network_key)
                self._persist(parent)
                current = parent
            except (RdapClientError, ValueError) as error:
                self.parent_failures += 1
                self.log.warning(
                    "Optional parent lookup failed for %s: %s", ip, type(error).__name__
                )
                break
        result = rdap_result(
            status="found", checked_at=direct.network.fetched_at, network=direct.network, cidr=cidr
        )
        markers.append(self._marker(ip, address, bucket, result))
        return result
```

Notes for the implementer: `RdapLookupResponse` stays imported because tests build responses through it; `_markers_of` and `_load_networks` compare times in SQL so the driver's timezone handling never enters the decision; `rdap_result(checked_at=network.fetched_at)` keeps today's semantics (the cached registration's fetch time is the RDAP check time); `globals()["monotonic"]` / `globals()["sleep"]` read the module names at construction so Task 5's loop test can patch `enrichment.monotonic` and `enrichment.sleep`; parents are charged to the registry that answered the direct registration (`network.rir`), which is where the `up` link points; the REST and whois answers and the RDAP fallback all go through `_request`, so pacing, the request counters and the budget window treat them alike.

- [ ] **Step 7: Run the resolver tests**

Run: `uv run --frozen --no-sync pytest tests/test_ip_enrichment_results.py -k "page_resolution or cache_window or negative_markers or registry_level_response or misses_reuse or ripe or rdap_shape or apnic or person_entities or registry_budget or registry_for" tests/test_commoncrawl_rdap_assets.py tests/test_ip_registry.py -q -p no:cacheprovider`
Expected: all pass. The asset-level tests of this file stay red until Task 5 rewrites `results.py` and them; `-k` selects only the resolver tests. The module still imports because `RequestBudgetReached` is kept as an unused class until Task 5 deletes it. `test_commoncrawl_rdap_assets.py` proves the legacy worker is untouched by the client change.

Run ruff format/check on `src/dagster_v3/defs/ip_enrichment/enrichment.py src/dagster_v3/defs/commoncrawl_rdap/client.py src/dagster_v3/defs/commoncrawl_rdap/ripe_rest.py src/dagster_v3/defs/commoncrawl_rdap/apnic_whois.py tests/test_ip_enrichment_results.py`.

- [ ] **Step 8: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/client.py services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/ripe_rest.py services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/apnic_whois.py services/dagster_v3/tests/test_ip_enrichment_results.py
git commit -m "perf(dagster): resolve registry coverage per page with bounded round trips; RIPE via REST and APNIC via whois -r without personal data; optional per-registry daily budget

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---
### Task 5: Execution loop over live remaining work, budget waits, completion from results, partition purge

`results.py` is rewritten around the shared lifecycle (`common/queue_execution.py`, crawl's `website_crawl/queue_execution.py` as the model). Design decided here: the task is walked bucket by bucket (`SELECT DISTINCT bucket` once per pass); within a bucket, pages come from a cursor on `input_id` and the anti-join subquery reads `ip_enrichment_results WHERE bucket = b AND task_id AND execution_id` — one primary-key range of the results table (`ORDER BY (bucket, ip, result_id)`), so the cost per page is independent of the task size. Results go through a `ResultBuffer` (500 rows / 5 s) and the cursor never re-reads a page inside a pass; after each pass the buffer is flushed and the walk restarts, and a pass that finds nothing ends the loop. A pass that read entries but resolved none because a registry budget deferred them makes the run wait (`wait_for_registry_budget`) and walk again — so a RIPE-bound run keeps processing every other registry and only idles when nothing else remains. Completion counts use the same bucket-scoped queries. `attempt` is always 1: a task has one execution and failed addresses are retried in a new draft. A reached `max_requests` budget flushes what was resolved and raises `dg.Failure` (task stays `selected`, re-running the task resumes: the documented pause-and-resume procedure). Every run reports the GeoLite2 build dates and warns when they are older than `MAX_AGE`. `ip_enrichment_workflow` goes.

**Files:**
- Rewrite: `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/results.py`
- Modify: `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py` (delete the `RequestBudgetReached` shim)
- Modify: `services/dagster_v3/tests/test_ip_enrichment_results.py` (replace the asset-level tests from `def select(env, ips):` through the end of `test_registry_level_registration_answers_only_the_queried_ip`; keep the Task 4 helpers and tests appended after them)

**Interfaces:**
- Produces (module `dagster_v3.defs.ip_enrichment.results`):
  - `TRANSPORT_SETTINGS = ("batch_size", "max_requests", "request_delay_seconds", "registry_daily_budgets")`, `NOT_FROZEN`, `LOOKUP_STATUSES`, `FAILED_SQL`, `RESULT_BATCH = 500`, `REGISTRY_USAGE_SQL` (rows `(rir, seconds_ago)` of the last day).
  - `start_ip_execution(store, client, config, run_id, *, default_execution_id=None) -> dict` (via `queue_execution.start_execution`, profile = config minus `task_id, execution_id` and the transport settings, plus `processor_version`; `freshness_days = rdap_cache_days`; label `"IP enrichment"`).
  - `task_buckets(client, task) -> list[int]`, `remaining_entries(client, task, *, bucket: int, after: str | None = None, limit: int) -> list[dict]` (keys `input_id, ip, ip_version, bucket`, ordered by `input_id`), `count_outcomes(client, task, buckets) -> tuple[int, int, int]` (`remaining, succeeded, failed`).
  - `store_results(client, records: list[dict]) -> None`, `run_ip_enrichment(context, client, task, config, *, enricher, city_reader, asn_reader) -> dict` with keys `written, pages, request_limit_reached, budget_waits, budget_wait_seconds`.
  - `finish_ip_execution(store, client, task) -> dict`.
  - Asset `ip_enrichment_results` with metadata keys `task_id, execution_id, results_table, coverage_semantics, geolite2_city_build, geolite2_asn_build, rdap_requests, rdap_cache_hits, parent_lookup_failures, registry_level_responses, rdap_requests_by_registry, rdap_person_entities_by_registry, rdap_deferrals_by_registry, written, pages, request_limit_reached, budget_waits, budget_wait_seconds, completion_status, succeeded_pages, failed_pages, skipped_recent, inputs_purged` (or `already_completed`); run tags `processing/task_id, ip_enrichment/execution_id, ip_enrichment/execution, ip_enrichment/outcome, ip_enrichment/succeeded_pages, ip_enrichment/failed_pages, ip_enrichment/skipped_pages`; job `ip_enrichment_results_job`.
- Consumes: `freshness.MAX_AGE`, `freshness.freshness` (Task 3); `RdapEnricher`, `RipeRestClient`, `ApnicWhoisClient`, the per-registry counters (Task 4); `bucket_prefix`, `ERROR_STATUSES`, `RESULT_RELATION` (Task 2).
- Removes: `ip_enrichment_workflow`, `prepare_execution`, `page_outcomes`, `insert_result`, `EXECUTION_TAG`, `RequestBudgetReached`.

- [ ] **Step 1: Write the failing tests**

In `services/dagster_v3/tests/test_ip_enrichment_results.py` replace everything from `def select(env, ips):` through the end of `test_registry_level_registration_answers_only_the_queried_ip` (the last test that existed before Task 4; the Task 4 helpers and tests stay below) with:

```python
def select(env, ips, *, scope=None, **config):
    """Append ``ips`` to the open draft of ``scope`` (a fresh scope by default); the task id."""
    result = prepare_input(
        env.resource, env.dsn, queue_scope=scope or "scope-" + uuid4().hex, ips=ips, **config
    )
    assert result.success
    return result.asset_materializations_for_node("ip_enrichment_input")[0].metadata["task_id"].value


def run(env, task, **config):
    return dg.materialize(
        [results.ip_enrichment_results, dg.AssetSpec("ip_enrichment_input")],
        instance=env.instance,
        resources={
            "clickhouse": env.resource,
            "processing": ProcessingResource(postgres_url=env.dsn),
            "maxmind_geoip": env.maxmind,
        },
        run_config={
            "ops": {
                "ip_enrichment_results": {
                    "config": {"task_id": task, "request_delay_seconds": 0, **config}
                }
            }
        },
        raise_on_error=False,
    )


def outcome(result):
    return {
        key: value.value
        for key, value in result.asset_materializations_for_node("ip_enrichment_results")[0]
        .metadata.items()
    }


def task_row(env, task):
    with ProcessingResource(postgres_url=env.dsn).get_store() as store:
        return store.task(task)


def test_draft_geoip_rdap_segments_completion_and_purge(environment):
    env = environment
    select(env, ["9.9.9.9"])
    task = select(env, ["8.8.8.8", "8.8.8.9", "2001:4860::8888", "127.0.0.1"])
    first = run(env, task, batch_size=1)
    assert first.success
    assert sorted(env.calls) == ["2001:4860::8888", "8.8.8.8"]
    assert "127.0.0.1" not in env.city.calls
    assert env.client.execute("""SELECT ip, country_iso_code, rdap_country_code,
        city_network, asn_network, rdap_matched_cidr FROM corpscout.ip_enrichment_current
        WHERE ip='8.8.8.8'""") == [
        ("8.8.8.8", "US", "CA", "8.8.8.0/24", "8.8.8.0/24", "8.8.8.0/24")
    ]
    assert env.client.execute(
        "SELECT count(), uniqExact(task_id), uniqExact(execution_id), min(attempt) FROM corpscout.ip_enrichment_results FINAL"
    ) == [(4, 1, 1, 1)]
    assert env.client.execute(
        "SELECT rdap_lookup_status, ip_scope FROM corpscout.ip_enrichment_current WHERE ip='127.0.0.1'"
    ) == [("not_global", "loopback")]
    assert env.client.execute(
        "SELECT count() FROM corpscout.rdap_network_segments FINAL"
    ) == [(2,)]
    metadata = outcome(first)
    assert metadata["completion_status"] == "completed"
    assert (metadata["written"], metadata["succeeded_pages"], metadata["failed_pages"], metadata["skipped_recent"]) == (4, 4, 0, 0)
    assert metadata["execution_id"] == first.run_id
    assert metadata["rdap_requests_by_registry"] == {"arin": 2}
    assert metadata["geolite2_city_build"] == "2026-08-01"  # the fixture readers' build epoch 1785542400
    assert (metadata["budget_waits"], metadata["rdap_deferrals_by_registry"]) == (0, {})
    record = task_row(env, task)
    assert record["status"] == "completed" and record["inputs_purged_at"] is not None
    assert (record["succeeded_count"], record["terminal_failed_count"], record["skipped_count"]) == (4, 0, 0)
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_input WHERE task_id = %(task)s", {"task": task}
    ) == [(0,)]
    tags = env.instance.get_run_by_id(first.run_id).tags
    assert tags["ip_enrichment/outcome"] == "completed" and tags["ip_enrichment/succeeded_pages"] == "4"
    calls_before = list(env.calls)
    # A completed task re-run only retries cleanup; another draft reuses the cached coverage.
    assert outcome(run(env, task))["already_completed"] is True
    assert run(env, select(env, ["8.8.8.10"])).success
    assert env.calls == calls_before


def test_errors_are_published_outcomes_and_backoff_holds_until_forced(environment, monkeypatch):
    env = environment
    attempts = []

    def unavailable(self, ip):
        attempts.append(ip)
        raise RdapClientError(
            "rate limit", code="rate_limited", retryable=True, status_code=429
        )

    monkeypatch.setattr(RdapClient, "lookup_ip", unavailable)
    task = select(env, ["8.8.8.8"])
    first = run(env, task)
    assert first.success and outcome(first)["completion_status"] == "completed_with_errors"
    assert env.client.execute("""SELECT city_lookup_status, asn_lookup_status, rdap_lookup_status,
        country_iso_code, rdap_error_code, rdap_retry_after > rdap_checked_at
        FROM corpscout.ip_enrichment_current""") == [
        ("found", "found", "retryable_error", "US", "rate_limited", 1)
    ]
    record = task_row(env, task)
    assert (record["status"], record["terminal_failed_count"], record["inputs_purged_at"] is not None) == ("completed", 1, True)
    # The failed address goes to a new draft (retry mode); the saved backoff still applies there.
    retry = prepare_input(
        env.resource, env.dsn, queue_scope="scope-" + uuid4().hex, retry_failed_task_id=task
    )
    assert retry.success
    again = retry.asset_materializations_for_node("ip_enrichment_input")[0].metadata["task_id"].value
    assert task_row(env, again)["total"] == 1
    assert run(env, again).success and attempts == ["8.8.8.8"]
    monkeypatch.setattr(RdapClient, "lookup_ip", lambda self, ip: response(ip))
    assert run(env, select(env, ["8.8.8.8"]), force_rdap=True).success
    assert env.client.execute(
        "SELECT rdap_lookup_status FROM corpscout.ip_enrichment_current"
    ) == [("found",)]


def test_request_budget_fails_the_run_and_the_same_task_resumes(environment):
    env = environment
    task = select(env, ["1.1.1.1", "8.8.8.8"])
    first = run(env, task, max_requests=1)
    assert not first.success
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(1,)]
    assert task_row(env, task)["status"] == "selected"
    # Transport settings may change; the saved execution is resumed without execution_id.
    resumed = run(env, task, max_requests=1, batch_size=7, registry_daily_budgets={"ripe": 5})
    assert resumed.success and outcome(resumed)["execution_id"] == first.run_id
    assert env.calls == ["1.1.1.1", "8.8.8.8"]
    assert env.client.execute(
        "SELECT count(), uniqExact(execution_id) FROM corpscout.ip_enrichment_results FINAL"
    ) == [(2, 1)]


def test_registry_budget_defers_and_the_run_waits_for_the_window(environment, monkeypatch):
    env = environment
    clock = {"now": 0.0}
    slept = []

    def fake_sleep(seconds):
        slept.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(enrichment, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(enrichment, "sleep", fake_sleep)
    task = select(env, ["5.1.1.1", "5.2.2.2", "8.8.8.8"])
    result = run(env, task, batch_size=1, registry_daily_budgets={"ripe": 1})
    assert result.success
    # One RIPE miss per day: whichever RIPE address comes first in bucket order is fetched,
    # the other is deferred, ARIN continues, then the run waits a full window.
    assert len(env.calls) == 3 and "8.8.8.8" in env.calls[:2] and env.calls[2].startswith("5.")
    metadata = outcome(result)
    assert metadata["completion_status"] == "completed" and metadata["written"] == 3
    assert metadata["budget_waits"] == 1 and metadata["budget_wait_seconds"] == pytest.approx(86_400)
    assert metadata["rdap_deferrals_by_registry"] == {"ripe": 2}  # once per pass until it slept
    assert metadata["rdap_requests_by_registry"] == {"ripe": 2, "arin": 1}
    assert "ripe" not in metadata["rdap_person_entities_by_registry"]
    assert sum(slept) == pytest.approx(86_400)


def test_registry_usage_sql_reads_the_last_day(environment):
    env = environment
    seed_network(env, "5.1.1.1", fetched_at=datetime.now(UTC) - timedelta(hours=1), handle="RIPE-1")
    seed_network(env, "5.2.2.2", fetched_at=datetime.now(UTC) - timedelta(days=2), handle="RIPE-2")
    rows = env.client.execute(results.REGISTRY_USAGE_SQL)
    assert [(rir, 3500 < seconds < 3700) for rir, seconds in rows] == [("arin", True)]
    enricher = resolver(env, registry_daily_budgets={"arin": 1})
    enricher.seed_registry_usage(rows)
    assert enricher.resolve_page(page(env, "8.8.8.8")) == {} and enricher.deferred == {"arin": 1}


@pytest.mark.parametrize("kind", ["catch_all", "wrong_range"])
def test_invalid_registration_coverage_is_a_terminal_error(environment, monkeypatch, kind):
    env = environment
    monkeypatch.setattr(
        RdapClient,
        "lookup_ip",
        lambda self, ip: response(
            ip,
            start="0.0.0.0" if kind == "catch_all" else "9.9.9.0",
            end="255.255.255.255" if kind == "catch_all" else "9.9.9.255",
        ),
    )
    result = run(env, select(env, ["8.8.8.8"]))
    assert result.success and outcome(result)["failed_pages"] == 1
    assert env.client.execute(
        "SELECT rdap_lookup_status, city_lookup_status FROM corpscout.ip_enrichment_current"
    ) == [("terminal_error", "found")]
    assert env.client.execute("SELECT count() FROM corpscout.rdap_networks") == [(0,)]


def test_city_lookup_failure_does_not_discard_asn_or_rdap(environment):
    env = environment
    env.city.fail = True
    result = run(env, select(env, ["8.8.8.8"]))
    assert result.success and outcome(result)["completion_status"] == "completed_with_errors"
    assert env.client.execute(
        "SELECT city_lookup_status, asn_lookup_status, rdap_lookup_status, asn FROM corpscout.ip_enrichment_current"
    ) == [("retryable_error", "found", "found", 15169)]


def test_task_id_must_name_a_draft(environment):
    env = environment
    assert not run(env, str(uuid4())).success
    legacy = str(uuid4())
    with ProcessingResource(postgres_url=env.dsn).get_store() as store:
        store.prepare_selection(legacy, processor="ip-enrichment-v1", fingerprint="legacy")
        assert store.task(legacy)["queue_scope"] is None
    assert not run(env, legacy).success
    assert env.calls == env.city.calls == []


def test_resume_after_lost_write_acknowledgement_does_not_repeat_lookups(environment, monkeypatch):
    env = environment
    task = select(env, ["8.8.8.8"])
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if query.lstrip().startswith("INSERT INTO corpscout.ip_enrichment_results") and not interrupted:
            interrupted = True
            raise ConnectionError("lost acknowledgement after durable insert")
        return value

    monkeypatch.setattr(Client, "execute", lost_ack)
    first = run(env, task)
    assert not first.success
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(1,)]
    assert run(env, task).success
    assert env.calls == env.city.calls == ["8.8.8.8"]
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(1,)]


def test_non_aligned_rdap_range_saves_exact_matching_segment(environment, monkeypatch):
    env = environment
    monkeypatch.setattr(
        RdapClient,
        "lookup_ip",
        lambda self, ip: response(ip, start="8.8.8.1", end="8.8.8.10"),
    )
    assert run(env, select(env, ["8.8.8.8"])).success
    assert env.client.execute("""SELECT rdap_start_address, rdap_end_address, rdap_matched_cidr
        FROM corpscout.ip_enrichment_current""") == [
        ("8.8.8.1", "8.8.8.10", "8.8.8.8/31")
    ]
    assert env.client.execute(
        "SELECT count() FROM corpscout.rdap_network_segments FINAL"
    ) == [(5,)]


def test_optional_parent_failure_preserves_direct_registration(environment, monkeypatch):
    env = environment

    def direct(self, ip):
        found = response(ip)
        found.raw_response["links"] = [
            {"rel": "up", "href": "https://rdap.arin.net/registry/ip/8.0.0.0/8"}
        ]
        return found

    def parent(self, url, *, rir):
        raise RdapClientError("parent unavailable", code="remote_server", retryable=True)

    monkeypatch.setattr(RdapClient, "lookup_ip", direct)
    monkeypatch.setattr(RdapClient, "lookup_up_url", parent)
    result = run(env, select(env, ["8.8.8.8"]))
    assert result.success
    metadata = outcome(result)
    assert metadata["parent_lookup_failures"] == 1 and metadata["rdap_requests"] == 2
    assert env.client.execute(
        "SELECT rdap_lookup_status, rdap_matched_cidr FROM corpscout.ip_enrichment_current"
    ) == [("found", "8.8.8.0/24")]


def test_resume_rejects_a_changed_lookup_policy_or_a_foreign_execution(environment):
    env = environment
    task = select(env, ["1.1.1.1", "8.8.8.8"])
    first = run(env, task, max_requests=1)
    assert not first.success
    assert not run(env, task, force_rdap=True).success  # frozen profile
    assert not run(env, task, rdap_cache_days=5).success
    assert not run(env, task, execution_id=str(uuid4())).success  # only the saved execution resumes
    assert env.calls == ["1.1.1.1"]
    assert run(env, task, execution_id=first.run_id, max_requests=5).success


def test_page_work_is_bounded_per_page_not_per_address(environment, monkeypatch):
    env = environment
    seed_network(env, "8.8.8.1", fetched_at=datetime.now(UTC))
    ips = [f"8.8.8.{n}" for n in range(1, 41)]
    queries = []
    execute = Client.execute

    def counting(self, query, *args, **kwargs):
        queries.append(query)
        return execute(self, query, *args, **kwargs)

    monkeypatch.setattr(Client, "execute", counting)
    assert run(env, select(env, ips), batch_size=10).success
    assert env.calls == []
    four_pages = query_kinds(queries)
    assert four_pages["negative"] <= 4 and four_pages["trie"] <= 4 and four_pages["networks"] <= 1
    assert four_pages["markers"] == 0 and four_pages["results"] == 1  # 40 rows < 500: one flush
    assert four_pages["context"] == 0
    assert not any("raw_response" in q for q in queries)
    queries.clear()
    assert run(env, select(env, ips), batch_size=40).success
    one_page = query_kinds(queries)
    assert one_page["negative"] <= 1 and one_page["trie"] <= 1 and one_page["results"] == 1
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(80,)]


def test_lost_cleanup_ack_does_not_repeat_lookups(environment, monkeypatch):
    env = environment
    task = select(env, ["8.8.8.8"])
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if query.startswith("ALTER TABLE corpscout.ip_enrichment_input DROP PARTITION") and not interrupted:
            interrupted = True
            raise ConnectionError("lost cleanup acknowledgement")
        return value

    monkeypatch.setattr(Client, "execute", lost_ack)
    assert not run(env, task).success
    record = task_row(env, task)
    assert record["status"] == "completed" and record["inputs_purged_at"] is None
    before = list(env.calls)
    assert run(env, task).success
    assert env.calls == before and task_row(env, task)["inputs_purged_at"] is not None


def test_new_submissions_after_start_form_the_next_draft(environment):
    env = environment
    scope = "scope-" + uuid4().hex
    first = select(env, ["8.8.8.8"], scope=scope)
    assert run(env, first).success
    second = select(env, ["1.1.1.1"], scope=scope)
    assert second != first and task_row(env, second)["status"] == "draft"
```

Fixture note for the deferral test: `Reader.metadata()` returns `build_epoch=1785542400` (2026-08-01T00:00Z), which is what `geolite2_city_build` reports. In `test_registry_budget_defers_and_the_run_waits_for_the_window` the loop's clock is faked at 0 and the fixture truncates `rdap_networks`, so the `REGISTRY_USAGE_SQL` seed is empty; the first RIPE request is logged at fake time 0, the other RIPE address is deferred in pass 1 and again in pass 2 (nothing else resolved → the run waits exactly one window from time 0), pass 3 fetches it and pass 4 finds nothing: `ripe` deferrals 2, one wait of 86,400 s. `batch_size=1` keeps every address in its own page; bucket order (cityHash64) decides which RIPE address is first.

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_ip_enrichment_results.py -q -p no:cacheprovider`
Expected: the new asset tests FAIL (`ValueError: materialize ip_enrichment_input first with this task_id` from the old asset, `KeyError: 'completion_status'`, `AttributeError: module 'dagster_v3.defs.ip_enrichment.results' has no attribute 'REGISTRY_USAGE_SQL'`); the Task 4 tests pass.

- [ ] **Step 3: Rewrite `results.py`**

Replace the whole of `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/results.py` with:

```python
"""Process a frozen IP enrichment draft: pages of remaining entries until nothing remains.

Remaining work is a live ClickHouse query per 256-way bucket (entries without a result
of this execution; each query anti-joins one primary-key range of the results table),
every page costs a fixed number of ClickHouse round trips whatever its size, outcomes
are stored in acknowledged micro-batches, and completion counts come from the results
table. Nothing about a page is persisted, so a resume is the same loop again. Entries
a registry budget deferred stay remaining; when a whole pass resolved nothing else the
run waits for the budget window, then walks again.
"""

import json
from contextlib import closing
from datetime import UTC, datetime
from uuid import UUID, uuid5

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common import queue_execution
from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.common.result_buffer import ResultBuffer
from dagster_v3.defs.commoncrawl_geoip.freshness import MAX_AGE, freshness
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource
from dagster_v3.defs.commoncrawl_rdap.apnic_whois import ApnicWhoisClient
from dagster_v3.defs.commoncrawl_rdap.assets import (
    BEST_KNOWN_SEMANTICS_WARNING,
    RDAP_USER_AGENT,
)
from dagster_v3.defs.commoncrawl_rdap.ripe_rest import RipeRestClient
from dagster_v3.defs.ip_enrichment.enrichment import (
    IpEnrichmentResultsConfig,
    RdapClient,
    RdapEnricher,
    geoip_result,
    maxminddb,
)
from dagster_v3.defs.ip_enrichment.input import (
    ERROR_STATUSES,
    INPUT_RELATION,
    PROCESSOR_VERSION,
    RESULT_RELATION,
    bucket_prefix,
)

LOOKUP_STATUSES = ("city_lookup_status", "asn_lookup_status", "rdap_lookup_status")
# Page size, request budgets and pacing are transport: they may change between resumes.
TRANSPORT_SETTINGS = (
    "batch_size",
    "max_requests",
    "request_delay_seconds",
    "registry_daily_budgets",
)
NOT_FROZEN = {"task_id", "execution_id", *TRANSPORT_SETTINGS}
FAILED_SQL = " OR ".join(f"{column} IN %(errors)s" for column in LOOKUP_STATUSES)
RESULT_BATCH = 500
# Requests any writer made in the last day, per registry, oldest first: the per-registry
# budget window survives a resume and counts the legacy bucket worker too.
REGISTRY_USAGE_SQL = """SELECT rir, toFloat64(dateDiff('second', fetched_at, now64(6)))
    FROM corpscout.rdap_networks
    WHERE fetched_at >= now64(6) - INTERVAL 1 DAY
    ORDER BY fetched_at"""


def execution_parameters(task: dict) -> dict:
    execution = task["config"]["execution"]
    return {
        "task": str(task["task_id"]),
        "exec": execution["execution_id"],
        "errors": ERROR_STATUSES,
    }


def start_ip_execution(
    store, client, config: IpEnrichmentResultsConfig, run_id: str, *, default_execution_id=None
) -> dict:
    """Freeze the draft into an execution, or return the saved one to resume."""

    def snapshot() -> tuple[dict, int]:
        task = store.task(config.task_id)
        [(total,)] = client.execute(
            f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s",
            {"task": config.task_id},
        )
        if total == 0 or total != task["total"]:
            raise ValueError("Queue is empty or its membership changed")
        return {"relation": INPUT_RELATION, "total": total}, total

    return queue_execution.start_execution(
        store,
        task_id=config.task_id,
        processor=PROCESSOR_VERSION,
        profile={
            **config.model_dump(exclude=NOT_FROZEN),
            "processor_version": PROCESSOR_VERSION,
        },
        execution_id=config.execution_id,
        freshness_days=config.rdap_cache_days,
        run_id=run_id,
        snapshot=snapshot,
        transport_keys=TRANSPORT_SETTINGS,
        default_execution_id=default_execution_id,
        label="IP enrichment",
    )


def task_buckets(client, task: dict) -> list[int]:
    return [
        bucket
        for (bucket,) in client.execute(
            f"SELECT DISTINCT bucket FROM {INPUT_RELATION} WHERE task_id=%(task)s ORDER BY bucket",
            {"task": str(task["task_id"])},
        )
    ]


def _bucket_parameters(task: dict, bucket: int) -> dict:
    return {
        **execution_parameters(task),
        "bucket": bucket,
        "after": bucket_prefix(bucket),
        "stop": bucket_prefix(bucket + 1),
    }


def remaining_entries(
    client, task: dict, *, bucket: int, after: str | None = None, limit: int
) -> list[dict]:
    """Frozen entries of one bucket after ``after`` without a result of this execution."""
    parameters = {**_bucket_parameters(task, bucket), "limit": limit}
    if after is not None:
        parameters["after"] = after
    rows = client.execute(
        f"""SELECT input_id, ip, ip_version, bucket FROM {INPUT_RELATION}
        WHERE task_id = %(task)s AND input_id > %(after)s AND input_id < %(stop)s
          AND input_id NOT IN (
              SELECT input_id FROM {RESULT_RELATION}
              WHERE bucket = %(bucket)s AND task_id = %(task)s AND execution_id = %(exec)s)
        ORDER BY input_id LIMIT %(limit)s""",
        parameters,
    )
    return [
        dict(zip(("input_id", "ip", "ip_version", "bucket"), row, strict=True))
        for row in rows
    ]


def count_outcomes(client, task: dict, buckets: list[int]) -> tuple[int, int, int]:
    """(remaining, succeeded, failed) over the task; every query is one primary-key range."""
    remaining = succeeded = failed = 0
    for bucket in buckets:
        parameters = _bucket_parameters(task, bucket)
        [(left,)] = client.execute(
            f"""SELECT count() FROM {INPUT_RELATION}
            WHERE task_id = %(task)s AND input_id > %(after)s AND input_id < %(stop)s
              AND input_id NOT IN (
                  SELECT input_id FROM {RESULT_RELATION}
                  WHERE bucket = %(bucket)s AND task_id = %(task)s AND execution_id = %(exec)s)""",
            parameters,
        )
        [(ok, bad)] = client.execute(
            f"""SELECT countIf(NOT failed), countIf(failed) FROM (
                SELECT input_id, argMax({FAILED_SQL}, tuple(completed_at, result_id)) AS failed
                FROM {RESULT_RELATION}
                WHERE bucket = %(bucket)s AND task_id = %(task)s AND execution_id = %(exec)s
                  AND input_id IN (
                      SELECT input_id FROM {INPUT_RELATION}
                      WHERE task_id = %(task)s AND input_id > %(after)s AND input_id < %(stop)s)
                GROUP BY input_id)""",
            parameters,
        )
        remaining += left
        succeeded += ok
        failed += bad
    return remaining, succeeded, failed


def store_results(client, records: list[dict]) -> None:
    columns = list(records[0])
    client.execute(
        f"INSERT INTO {RESULT_RELATION} ({','.join(columns)}) VALUES",
        [tuple(record[column] for column in columns) for record in records],
        settings={"async_insert": 1, "wait_for_async_insert": 1},
    )


def run_ip_enrichment(
    context, client, task: dict, config: IpEnrichmentResultsConfig, *, enricher, city_reader, asn_reader
) -> dict:
    """Walk the task bucket by bucket until a whole pass finds nothing remaining.

    A page's outcomes enter the buffer together and the cursor moves past the page, so
    a page is never read twice inside a pass; the buffer is flushed at the end of every
    pass and a further pass confirms that nothing remains. A reached max_requests
    budget flushes what was resolved and stops; the rest stays remaining for the
    resume. A pass that resolved nothing while a registry budget deferred entries waits
    for the budget window and walks again.
    """
    execution = task["config"]["execution"]
    execution_uuid = UUID(execution["execution_id"])
    counts = {
        "written": 0,
        "pages": 0,
        "request_limit_reached": False,
        "budget_waits": 0,
        "budget_wait_seconds": 0.0,
    }

    def flush(records: list[dict]) -> None:
        store_results(client, records)
        counts["written"] += len(records)

    buffer: ResultBuffer[dict] = ResultBuffer(flush, max_items=RESULT_BATCH, max_seconds=5.0)
    buckets = task_buckets(client, task)
    while True:
        processed = 0
        enricher.reset_pass()
        for bucket in buckets:
            after = None
            while rows := remaining_entries(client, task, bucket=bucket, after=after, limit=config.batch_size):
                after = rows[-1]["input_id"]
                rdap = enricher.resolve_page(rows)
                records = []
                for row in rows:
                    if row["ip"] not in rdap:
                        continue  # deferred by a registry budget, or the request budget ran out
                    checked_at = datetime.now(UTC)
                    records.append(
                        {
                            "ip": row["ip"],
                            "result_id": str(uuid5(execution_uuid, row["input_id"])),
                            "task_id": str(task["task_id"]),
                            "execution_id": execution["execution_id"],
                            "input_id": row["input_id"],
                            "source_run_id": context.run.run_id,
                            "processor_version": PROCESSOR_VERSION,
                            "attempt": 1,
                            "completed_at": checked_at,
                            **geoip_result(
                                row["ip"],
                                city_reader,
                                asn_reader,
                                checked_at=checked_at,
                                retry_seconds=config.transient_retry_seconds,
                            ),
                            **rdap[row["ip"]],
                        }
                    )
                buffer.add(records)
                processed += len(records)
                counts["pages"] += 1
                context.log.info(
                    "IP enrichment execution=%s bucket=%s page=%s resolved=%s rdap_requests=%s cache_hits=%s deferred=%s",
                    execution["execution_id"],
                    bucket,
                    counts["pages"],
                    counts["written"] + len(buffer),
                    enricher.requests,
                    enricher.cache_hits,
                    enricher.deferred,
                )
                if enricher.budget_reached:
                    buffer.flush()
                    counts["request_limit_reached"] = True
                    return counts
        buffer.flush()  # an unacknowledged batch fails the run; the resume re-reads its rows
        if processed == 0:
            if not enricher.deferred:
                return counts
            context.log.warning(
                "Registry budget reached (%s); waiting %.0f s before the next pass",
                enricher.deferred,
                enricher.seconds_until_budget_frees(),
            )
            counts["budget_waits"] += 1
            counts["budget_wait_seconds"] += enricher.wait_for_registry_budget()


def finish_ip_execution(store, client, task: dict) -> dict:
    """Counts come from this execution's results; skipped is the rest of the total (expected 0)."""
    remaining, succeeded, failed = count_outcomes(client, task, task_buckets(client, task))
    return queue_execution.record_completion(
        store, task_id=str(task["task_id"]), remaining=remaining, succeeded=succeeded, failed=failed
    )


@dg.asset(
    deps=["ip_enrichment_input"],
    group_name="ip_enrichment",
    kinds={"python", "clickhouse", "maxmind", "rdap"},
    # Share the RDAP pool with the legacy crawler so both respect its concurrency limit.
    pool="commoncrawl_rdap",
    metadata={"dagster/table_name": RESULT_RELATION},
    description="Freeze an IP enrichment draft, then enrich the remaining addresses page by page "
    "with GeoIP, ASN and RDAP; outcomes are stored in acknowledged batches. Re-run the task to "
    "resume its saved execution. Completed tasks drop their input partition; retry failed "
    "addresses in a new draft (retry_failed_task_id).",
)
def ip_enrichment_results(
    context: dg.AssetExecutionContext,
    config: IpEnrichmentResultsConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
    maxmind_geoip: MaxMindDatabaseResource,
) -> dg.MaterializeResult:
    def complete(store, task) -> dict:
        return queue_execution.complete_task(
            context,
            store,
            clickhouse,
            task,
            processor=PROCESSOR_VERSION,
            relation=INPUT_RELATION,
            tag_prefix="ip_enrichment",
            label="IP enrichment",
        )

    with (
        processing.get_store() as store,
        store.selection_lock(config.task_id),
        clickhouse.get_connection() as client,
    ):
        task = store.task(config.task_id)
        if (
            task is None
            or task["processor"] != PROCESSOR_VERSION
            or task["queue_scope"] is None
        ):
            raise ValueError(
                "task_id must name an IP enrichment draft; add addresses with ip_enrichment_input first"
            )
        task = start_ip_execution(
            store,
            client,
            config,
            context.run.run_id,
            default_execution_id=context.run.root_run_id or context.run.run_id,
        )
        execution = task["config"]["execution"]
        context.instance.add_run_tags(
            context.run.run_id,
            {
                "processing/task_id": config.task_id,
                "ip_enrichment/execution_id": execution["execution_id"],
                "ip_enrichment/execution": json.dumps(execution, sort_keys=True),
            },
        )
        if task["status"] == "completed":
            return dg.MaterializeResult(
                metadata={
                    "task_id": config.task_id,
                    "execution_id": execution["execution_id"],
                    "already_completed": True,
                    **complete(store, task),
                }
            )
        assert_clickhouse_tables_exist(
            clickhouse,
            database="corpscout",
            tables=(
                "ip_enrichment_results",
                "ip_enrichment_current",
                "rdap_networks",
                "rdap_networks_current",
                "rdap_network_segments",
                "rdap_network_segments_current",
                "rdap_ip_lookup_results_current",
                "rdap_network_registry_class",
                "rdap_network_registry_class_current",
                "ip_registry_ready",
            ),
        )
        city_path, asn_path = maxmind_geoip.database_paths()
        with (
            maxminddb.open_database(city_path) as city_reader,
            maxminddb.open_database(asn_path) as asn_reader,
            closing(RdapClient(user_agent=RDAP_USER_AGENT)) as rdap_client,
            closing(RipeRestClient(user_agent=RDAP_USER_AGENT)) as ripe_client,
            closing(ApnicWhoisClient()) as apnic_client,
        ):
            builds = {}
            for kind, reader in (("City", city_reader), ("ASN", asn_reader)):
                if kind not in reader.metadata().database_type:
                    raise ValueError(f"expected a MaxMind {kind} database")
                builds[f"GeoLite2-{kind}"] = datetime.fromtimestamp(
                    reader.metadata().build_epoch, UTC
                )
            geolite2 = freshness(builds, datetime.now(UTC))
            if not geolite2.passed:
                context.log.warning(
                    "%s Older than %s days; replace the files by hand "
                    "(docs/operations/ip-enrichment-draft-queue.md).",
                    geolite2.description,
                    MAX_AGE.days,
                )
            client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
            enricher = RdapEnricher(
                client,
                rdap_client,
                ripe_client,
                apnic_client,
                config,
                context.log,
                started_at=datetime.fromisoformat(execution["started_at"]),
                cache_cutoff=datetime.fromisoformat(execution["freshness_cutoff"]),
            )
            enricher.seed_registry_usage(client.execute(REGISTRY_USAGE_SQL))
            counts = run_ip_enrichment(
                context, client, task, config,
                enricher=enricher, city_reader=city_reader, asn_reader=asn_reader,
            )
            if enricher.networks_written:
                client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
        metadata = {
            "task_id": config.task_id,
            "execution_id": execution["execution_id"],
            "results_table": RESULT_RELATION,
            "coverage_semantics": BEST_KNOWN_SEMANTICS_WARNING,
            "geolite2_city_build": builds["GeoLite2-City"].date().isoformat(),
            "geolite2_asn_build": builds["GeoLite2-ASN"].date().isoformat(),
            "rdap_requests": enricher.requests,
            "rdap_cache_hits": enricher.cache_hits,
            "parent_lookup_failures": enricher.parent_failures,
            "registry_level_responses": enricher.registry_level_responses,
            "rdap_requests_by_registry": enricher.requests_by_registry,
            "rdap_person_entities_by_registry": enricher.person_entities_by_registry,
            "rdap_deferrals_by_registry": enricher.deferrals_by_registry,
            **counts,
        }
        if counts["request_limit_reached"]:
            raise dg.Failure(
                "RDAP request budget reached; results saved. Re-run this task to resume the saved execution.",
                metadata=metadata,
                allow_retries=False,
            )
        task = finish_ip_execution(store, client, task)
        return dg.MaterializeResult(metadata={**metadata, **complete(store, task)})


ip_enrichment_results_job = dg.define_asset_job(
    "ip_enrichment_results_job",
    selection=dg.AssetSelection.assets(ip_enrichment_results),
)
defs = dg.Definitions(assets=[ip_enrichment_results], jobs=[ip_enrichment_results_job])
```

Delete the `RequestBudgetReached` class from `enrichment.py`.

- [ ] **Step 4: Run the suites and the definitions check**

Run: `uv run --frozen --no-sync pytest tests/test_ip_enrichment_results.py tests/test_ip_enrichment_input.py tests/test_queue_execution_common.py tests/test_geolite2_freshness.py -q -p no:cacheprovider`
Expected: all pass.

Run: `uv run --frozen --no-sync dg check defs`
Expected: `All definitions loaded successfully.` (no `ip_enrichment_workflow`; `geolite2_databases_fresh` still resolves its asset).

Run: `rg -n "ip_enrichment_workflow|RequestBudgetReached|prepare_execution|page_outcomes|insert_result|EXECUTION_TAG|ClickHouseInputQueue|contact_entities|use_rdap_proxies|RDAP_PROXIES|build_lanes|egress" services/dagster_v3/src/dagster_v3/defs/ip_enrichment services/dagster_v3/tests/test_ip_enrichment_results.py services/dagster_v3/tests/test_ip_enrichment_input.py`
Expected: no matches.

Run ruff format/check on `src/dagster_v3/defs/ip_enrichment/results.py src/dagster_v3/defs/ip_enrichment/enrichment.py tests/test_ip_enrichment_results.py`.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/ip_enrichment/results.py services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py services/dagster_v3/tests/test_ip_enrichment_results.py
git commit -m "feat(dagster): IP enrichment runs the shared queue lifecycle over live remaining work, waits for registry budgets and finishes from results

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---
### Task 6: Backoffice — "Add to enrichment queue", draft semantics on the queue page

Determined from the code on main: `launchIpEnrichment` (`app/lib/ip-enrichment.server.ts:86-140`) is the only launcher of `ip_enrichment_workflow`; it is called from `routes/admin-ip-addresses.tsx:131` and mocked in `tests/admin-ip-addresses-action.test.ts`. The queue page reads `ip_enrichment_input` with `ORDER BY input_id, task_id` (`queues.server.ts:53`, the non-crawler branch), never with `FINAL`, and treats only `webtech`/`crawler` as draft queues (`admin-queue.tsx:24, 61, 104, 120, 132, 141, 147`; `queue-process-sheet.tsx:83-85, 106, 113`; `queues.server.ts:108-109` maps history outcome tags for `crawler`/`webtech` only). `QueueImportStatus`/`useQueueSubmission` (`components/admin/queue-import-status.tsx`) poll `/admin/<queue>/queue-submissions/<runId>` and know two queues. `ip-enrichment` keeps `batch_size`, `max_requests` and `request_delay_seconds` as processing parameters (they are transport keys the results asset accepts on resume). `registry_daily_budgets` is not exposed in the sheet (the Dagster default applies; set it from the Dagster launchpad when it must differ — follow-up).

Run all commands from `services/backoffice`. New route file: the long-running dev server on 5183 needs a restart to see it (memory: stale Vite fs cache); verify on a second port if needed.

**Files:**
- Rewrite: `app/lib/ip-enrichment.server.ts`
- Create: `app/routes/admin-ip-enrichment-queue-submission.ts`
- Modify: `app/routes.ts:139`
- Modify: `app/components/admin/queue-import-status.tsx:8, 19-26, 38`
- Modify: `app/routes/admin-ip-addresses.tsx:6-9, 113-152, 162-194, 243-264, 294-316`
- Modify: `app/lib/queues.ts:17`
- Modify: `app/lib/queues.server.ts:53, 95-109`
- Modify: `app/routes/admin-queue.tsx:13, 24, 61, 104, 120, 132, 141, 147, 149`
- Modify: `app/components/admin/queue-process-sheet.tsx:4, 83-85, 106, 113`
- Rewrite: `tests/ip-enrichment.server.test.ts`
- Modify: `tests/admin-ip-addresses-action.test.ts`, `tests/queues.server.test.ts`, `tests/queue-route.test.ts`

**Interfaces:**
- Produces: `addIpsToEnrichmentQueue(value: unknown, submissionId: string, requestedBy: string, options?: DagsterOptions) -> {ok: true, runId, status, runUrl}` (launches `ip_enrichment_input_job`, asset `ip_enrichment_input`, config `{...inputConfig(selection), queue_scope: "workspace", submission_id}`, tags `processing/submission_id`, `ip_enrichment/selection_sha256`, `corpscout/requested_by`, `backoffice/action: "add-ip-enrichment-input"`); `ipEnrichmentQueueSubmission(runId, options?) -> {ok: true, runId, status, finished, taskId}`; `inputConfig(selection: WorkspaceIpSelection)`; `parseIpEnrichmentSelection`, `IpEnrichmentSelectionError` unchanged. `queues.ts`: `DRAFT_QUEUES`, `isDraftQueue(type: QueueType): boolean`. `queue-import-status.tsx`: `ImportQueue = "webtech" | "crawler" | "ip-enrichment"`, `useQueueSubmission(receipt, queue: ImportQueue)`, `QueueImportStatus` accepts `queue?: ImportQueue`.
- Removes: `launchIpEnrichment`.

- [ ] **Step 1: Write the failing tests**

Replace `tests/ip-enrichment.server.test.ts` with:

```ts
import { beforeEach, describe, expect, it, vi } from "vitest";
const dagster = vi.hoisted(() => ({launchRun: vi.fn(), listRuns: vi.fn(), runStatus: vi.fn(), dagsterRunUrl: vi.fn((id: string) => `http://dagster/runs/${id}`)}));
vi.mock("~/lib/dagster.server", () => dagster);
import { addIpsToEnrichmentQueue, ipEnrichmentQueueSubmission } from "~/lib/ip-enrichment.server";

const submission = "11111111-1111-4111-8111-111111111111";
beforeEach(() => { vi.clearAllMocks(); dagster.listRuns.mockResolvedValue([]); dagster.launchRun.mockResolvedValue({runId: "new", status: "QUEUED"}); });

describe("IP enrichment queue submission", () => {
  it("appends selected addresses from multiple pages to the workspace draft via the input asset only", async () => {
    const result = await addIpsToEnrichmentQueue({ mode: "ips", ips: ["8.8.8.8", "2001:4860::8888", "8.8.8.8"] }, submission, "operator");
    expect(dagster.launchRun).toHaveBeenCalledWith(expect.objectContaining({job: "ip_enrichment_input_job", assetSelection: ["ip_enrichment_input"],
      runConfig: {ops: {ip_enrichment_input: {config: {
        source_name: "backoffice:ip-addresses", source_relation: "corpscout.commoncrawl_ip_addresses", observed_at_column: "last_seen",
        filters: { ip: ["8.8.8.8", "2001:4860::8888"] }, queue_scope: "workspace", submission_id: submission,
      }}}},
      tags: expect.objectContaining({"processing/submission_id": submission, "backoffice/action": "add-ip-enrichment-input", "corpscout/requested_by": "operator"}),
    }));
    const config = dagster.launchRun.mock.calls[0][0].runConfig.ops.ip_enrichment_input.config;
    expect(config).not.toHaveProperty("task_id");
    expect(result).toEqual({ok: true, runId: "new", status: "QUEUED", runUrl: "http://dagster/runs/new"});
  });

  it.each(["any", "4", "6"])("submits all matching addresses compactly for version %s", async (version) => {
    await addIpsToEnrichmentQueue({ mode: "all", filters: { search: "2001:db8:", version }, excludedIps: ["2001:db8::1", "2001:db8::1"] }, submission, "operator");
    const config = dagster.launchRun.mock.calls[0][0].runConfig.ops.ip_enrichment_input.config;
    expect(config).toMatchObject({ source_relation: "corpscout.commoncrawl_ip_addresses", observed_at_column: "last_seen", select_all: true, ip_search: "2001:db8:",
      filters: version === "any" ? {} : { ip_version: [version] }, excluded_ips: ["2001:db8::1"] });
    expect(config).not.toHaveProperty("max_rows");
    expect(config).not.toHaveProperty("ips");
  });

  it("supports the entire inventory without a pagination limit", async () => {
    await addIpsToEnrichmentQueue({ mode: "all", filters: { search: "", version: "any" }, excludedIps: [] }, submission, "operator");
    expect(dagster.launchRun.mock.calls[0][0].runConfig.ops.ip_enrichment_input.config).toMatchObject({ select_all: true, ip_search: "", filters: {} });
  });

  it("recovers an acknowledged submission and rejects a changed selection", async () => {
    const selection = { mode: "ips", ips: ["8.8.8.8"] };
    await addIpsToEnrichmentQueue(selection, submission, "operator");
    const tags = dagster.launchRun.mock.calls[0][0].tags;
    dagster.listRuns.mockResolvedValue([{runId: "new", status: "SUCCESS", tags}]);
    expect(await addIpsToEnrichmentQueue(selection, submission, "operator")).toMatchObject({runId: "new"});
    expect(dagster.launchRun).toHaveBeenCalledTimes(1);
    await expect(addIpsToEnrichmentQueue({ mode: "ips", ips: ["1.1.1.1"] }, submission, "operator")).rejects.toThrow("another selection");
    dagster.listRuns.mockResolvedValue([{runId: "failed", status: "FAILURE", tags}]);
    await addIpsToEnrichmentQueue(selection, submission, "operator");
    expect(dagster.launchRun).toHaveBeenCalledTimes(2);  // a failed import is retried with the same receipt
  });

  it.each([
    null,
    { mode: "ips", ips: [] },
    { mode: "ips", ips: ["bad"] },
    { mode: "ips", ips: ["fe80::1%eth0"] },
    { mode: "ips", ips: ["8.8.8.8"], source_relation: "arbitrary" },
    { mode: "all", filters: { search: "' OR 1=1", version: "any" }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: 4 }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: "any", sql: "1=1" }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: "any" }, excludedIps: ["invalid"] },
  ])("rejects invalid selections before contacting Dagster: %j", async (selection) => {
    await expect(addIpsToEnrichmentQueue(selection, submission, "operator")).rejects.toThrow();
    expect(dagster.launchRun).not.toHaveBeenCalled();
  });

  it("rejects a malformed submission id before contacting Dagster", async () => {
    await expect(addIpsToEnrichmentQueue({ mode: "ips", ips: ["8.8.8.8"] }, "not-a-uuid", "operator")).rejects.toThrow("submission ID");
    expect(dagster.listRuns).not.toHaveBeenCalled();
  });

  it("returns the resolved draft only for IP enrichment import runs", async () => {
    dagster.runStatus.mockResolvedValue({jobName: "ip_enrichment_input_job", status: "SUCCESS", tags: {"backoffice/action": "add-ip-enrichment-input", "processing/task_id": submission}});
    expect(await ipEnrichmentQueueSubmission(submission)).toMatchObject({finished: true, taskId: submission});
    dagster.runStatus.mockResolvedValue({jobName: "ip_enrichment_workflow", status: "SUCCESS", tags: {}});
    await expect(ipEnrichmentQueueSubmission(submission)).rejects.toThrow("not found");
  });
});
```

In `tests/admin-ip-addresses-action.test.ts` rename the hoisted mock: line 5 `addIpsToEnrichmentQueue: launch,` and the first test (lines 24-31) becomes:

```ts
it("uses the submitted selection and submission id independently of page filters", async () => {
  const selection = { mode: "ips", ips: ["8.8.8.8"] };
  const submissionId = "11111111-1111-4111-8111-111111111111";
  launch.mockResolvedValue({ ok: true, runId: "run", status: "QUEUED", runUrl: null });
  expect(await submit({ action: "enrich", selection, submissionId })).toMatchObject({ data: { ok: true, runId: "run" } });
  expect(launch).toHaveBeenCalledWith(selection, submissionId, expect.any(String));
});
```

(the "launch failure" test keeps working: its `submit` body gains no `submissionId`, which the action forwards as `""`).

Append to `tests/queues.server.test.ts` (inside its `describe`, after the existing cases; `filters`, `chQuery`, `listRuns` and `loadQueueInputs` are already imported there):

```ts
it("reads the IP enrichment entry table in sort-key order without FINAL and maps its outcome tags", async () => {
  vi.mocked(chQuery).mockResolvedValue([]);
  await loadQueueInputs(filters("ip-enrichment"));
  const sql = vi.mocked(chQuery).mock.calls.map(([query]) => query);
  expect(sql.some(query => query.includes("FROM corpscout.ip_enrichment_input") && query.includes("ORDER BY task_id, input_id"))).toBe(true);
  for (const query of sql) expect(query).not.toContain("ip_enrichment_input FINAL");
  const {loadQueueHistory} = await import("~/lib/queues.server");
  vi.mocked(listRuns).mockResolvedValue([{runId: "saved", status: "SUCCESS", startTime: null, tags: {"processing/task_id": task, "ip_enrichment/outcome": "completed_with_errors", "ip_enrichment/failed_pages": "3"}}] as never);
  expect(await loadQueueHistory(filters("ip-enrichment"))).toEqual([expect.objectContaining({outcome: "completed_with_errors", failedPages: 3})]);
});
```

Append to `tests/queue-route.test.ts`:

```ts
it("selects the current IP enrichment draft like Webtech", async () => {
  server.loadQueueInputs.mockResolvedValue({tasks: [{task_id: task}], selectedTotal: 2});
  try { await loader({params: {type: "ip-enrichment"}, request: new Request("http://x/admin/queues/ip-enrichment")} as never); throw new Error("expected redirect"); }
  catch (response) { expect((response as Response).headers.get("Location")).toBe(`/admin/queues/ip-enrichment?task=${task}`); }
});
```

- [ ] **Step 2: Run to verify they fail**

Run: `npx vitest run tests/ip-enrichment.server.test.ts tests/admin-ip-addresses-action.test.ts tests/queues.server.test.ts tests/queue-route.test.ts`
Expected: FAIL — `addIpsToEnrichmentQueue is not a function`, the ORDER BY assertion, the history outcome and the redirect.

- [ ] **Step 3: Rewrite `ip-enrichment.server.ts`**

Keep `IpEnrichmentSelectionError`, `record`, `ipList` and `parseIpEnrichmentSelection` (lines 10-84) and replace the imports (lines 1-8) and `launchIpEnrichment` (lines 86-140) with:

```ts
import { createHash } from "node:crypto";
import { isIP } from "node:net";
import { dagsterRunUrl, launchRun, listRuns, runStatus, type DagsterOptions } from "~/lib/dagster.server";
import { QUEUE_UUID } from "~/lib/queues";
import type { WorkspaceIpSelection } from "~/lib/workspace-ip-selection";

const JOB = "ip_enrichment_input_job";
const ASSET = "ip_enrichment_input";
const submissions = new Map<string, Promise<unknown>>();
```

and, after `parseIpEnrichmentSelection`:

```ts
export function inputConfig(selection: WorkspaceIpSelection): Record<string, unknown> {
  const input: Record<string, unknown> = {
    source_name: "backoffice:ip-addresses",
    source_relation: "corpscout.commoncrawl_ip_addresses",
    observed_at_column: "last_seen",
  };
  if (selection.mode === "ips") {
    input.filters = { ip: selection.ips };
  } else {
    // Dagster selects the whole matching inventory in ClickHouse, independently of pagination.
    Object.assign(input, {
      select_all: true,
      ip_search: selection.filters.search,
      filters: selection.filters.version === "any" ? {} : { ip_version: [selection.filters.version] },
      excluded_ips: selection.excludedIps,
    });
  }
  return input;
}

/** Append the selection to the open workspace draft. Dagster chooses/creates the task atomically. */
export async function addIpsToEnrichmentQueue(value: unknown, submissionId: string, requestedBy: string, options: DagsterOptions = {}) {
  const selection = parseIpEnrichmentSelection(value);
  if (!QUEUE_UUID.test(submissionId)) throw new IpEnrichmentSelectionError("Invalid submission ID. Reload the page.");
  const config = { ...inputConfig(selection), queue_scope: "workspace", submission_id: submissionId };
  const fingerprint = createHash("sha256").update(JSON.stringify(config)).digest("hex");
  const previous = submissions.get(submissionId);
  const pending = previous ? previous.then(submit, submit) : submit();
  submissions.set(submissionId, pending);
  try { return await pending; }
  finally { if (submissions.get(submissionId) === pending) submissions.delete(submissionId); }

  async function submit() {
    const [existing] = await listRuns({ job: JOB, limit: 1, tags: { "processing/submission_id": submissionId } }, options);
    if (existing && existing.tags["ip_enrichment/selection_sha256"] !== fingerprint) throw new IpEnrichmentSelectionError("This submission ID belongs to another selection.");
    // Retry failed imports using the same durable receipt, not a new selection.
    if (existing && !["FAILURE", "CANCELED"].includes(existing.status)) {
      return { ok: true as const, runId: existing.runId, status: existing.status, runUrl: dagsterRunUrl(existing.runId, options.url) };
    }
    const run = await launchRun({
      job: JOB, assetSelection: [ASSET], runConfig: { ops: { [ASSET]: { config } } },
      tags: {
        "processing/submission_id": submissionId, "ip_enrichment/selection_sha256": fingerprint,
        "corpscout/requested_by": requestedBy, "backoffice/action": "add-ip-enrichment-input",
      },
    }, options);
    return { ok: true as const, ...run, runUrl: dagsterRunUrl(run.runId, options.url) };
  }
}

export async function ipEnrichmentQueueSubmission(runId: string, options: DagsterOptions = {}) {
  if (!QUEUE_UUID.test(runId)) throw new IpEnrichmentSelectionError("Invalid import run ID.");
  const run = await runStatus(runId, options);
  if (run.jobName !== JOB || run.tags["backoffice/action"] !== "add-ip-enrichment-input") throw new IpEnrichmentSelectionError("IP enrichment queue submission not found.");
  const task = run.tags["processing/task_id"];
  return { ok: true as const, runId, status: run.status, finished: ["SUCCESS", "FAILURE", "CANCELED"].includes(run.status), taskId: task && QUEUE_UUID.test(task) ? task : null };
}
```

`import { randomUUID } from "node:crypto";` (line 1) and the old `launchIpEnrichment` are gone.

- [ ] **Step 4: Add the status route and generalize the import status component**

Create `app/routes/admin-ip-enrichment-queue-submission.ts`:

```ts
import { data } from "react-router";
import type { Route } from "./+types/admin-ip-enrichment-queue-submission";
import { IpEnrichmentSelectionError, ipEnrichmentQueueSubmission } from "~/lib/ip-enrichment.server";
export async function loader({params}: Route.LoaderArgs) {
  try { return data(await ipEnrichmentQueueSubmission(params.runId), {headers: {"Cache-Control": "no-store"}}); }
  catch (error) {
    return data({ok: false as const, error: error instanceof IpEnrichmentSelectionError ? error.message : "Import status is unavailable. Check the Dagster run or retry the status check."}, {status: error instanceof IpEnrichmentSelectionError ? 400 : 503, headers: {"Cache-Control": "no-store"}});
  }
}
```

In `app/routes.ts` after line 139 (`route("webtech/queue-submissions/:runId", …)`) add:

```ts
    route("ip-enrichment/queue-submissions/:runId", "routes/admin-ip-enrichment-queue-submission.ts"),
```

In `app/components/admin/queue-import-status.tsx` replace line 8 and lines 19-26 so the component knows three queues:

```ts
export type ImportQueue = "webtech" | "crawler" | "ip-enrichment";
const LABELS: Record<ImportQueue, string> = {webtech: "Webtech", crawler: "Crawler", "ip-enrichment": "IP enrichment"};

export function useQueueSubmission(receipt: QueueImportReceipt | null, queue: ImportQueue = "webtech") {
```

```ts
export function QueueImportStatus({receipt, state, fallbackSearch = "", crawlType, queue}: {
  receipt: QueueImportReceipt;
  state: ReturnType<typeof useQueueSubmission>["state"];
  fallbackSearch?: string;
  crawlType?: string;
  queue?: ImportQueue;
}) {
  const target: ImportQueue = queue ?? (crawlType ? "crawler" : "webtech");
  const label = LABELS[target];
```

and the link (line 38) becomes ``to={`/admin/queues/${target}${params.size ? `?${params}` : ""}`}``.

- [ ] **Step 5: The IP addresses page adds to the draft**

In `app/routes/admin-ip-addresses.tsx`:

Imports (lines 6-9) become:

```ts
import { addIpsToEnrichmentQueue, IpEnrichmentSelectionError } from "~/lib/ip-enrichment.server";
import { QueueImportStatus, useQueueSubmission } from "~/components/admin/queue-import-status";
```

The `action` (lines 113-152) becomes:

```ts
export async function action({ request }: Route.ActionArgs) {
  let body;
  try {
    body = await request.json();
  } catch {
    return data({ ok: false as const, error: "Invalid enrichment request." }, { status: 400 });
  }
  if (body?.action !== "enrich") {
    return data({ ok: false as const, error: "Choose a supported IP address action." }, { status: 400 });
  }
  try {
    return data(
      await addIpsToEnrichmentQueue(
        body.selection,
        String(body.submissionId ?? ""),
        process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice",
      ),
    );
  } catch (error) {
    if (error instanceof IpEnrichmentSelectionError) {
      return data({ ok: false as const, error: error.message }, { status: 400 });
    }
    return data(
      { ok: false as const, error: "Could not submit the queue import. Check Dagster for a submitted run before retrying." },
      { status: 502 },
    );
  }
}
```

In the component, replace lines 162-194 (from `const navigationBusy` through `const hasSelection = …`) with:

```ts
  type SelectionState = { filterKey: string; selection: WorkspaceIpSelection };
  const navigationBusy = useNavigation().state !== "idle";
  const fetcher = useFetcher<typeof action>();
  const receipt = fetcher.data?.ok ? fetcher.data : null;
  const { state: importState, error: importError } = useQueueSubmission(receipt, "ip-enrichment");
  const submitting = fetcher.state !== "idle" || Boolean(receipt && !importState?.finished);
  const busy = navigationBusy || submitting;
  const filterKey = JSON.stringify(filters);
  const [selectionState, setSelectionState] = useState<SelectionState>({ filterKey, selection: { mode: "ips", ips: [] } });
  const currentState =
    selectionState.filterKey === filterKey
      ? selectionState
      : { filterKey, selection: { mode: "ips" as const, ips: [] } };
  if (currentState !== selectionState) setSelectionState(currentState);
  const selection = currentState.selection;
  const setSelection = (selection: WorkspaceIpSelection) => setSelectionState({ filterKey, selection });
  const request = useRef<{ id: string; state: SelectionState } | null>(null);
  const [retry, setRetry] = useState(false);
  useEffect(() => {
    if (importState?.status === "SUCCESS") {
      // Only a saved import clears the selection; an accepted launch alone does not.
      setSelectionState((current) =>
        current === request.current?.state ? { ...current, selection: { mode: "ips", ips: [] } } : current,
      );
      setRetry(false);
    } else if (importState?.status === "FAILURE" || importState?.status === "CANCELED" || fetcher.data?.ok === false) {
      setRetry(true);
    }
  }, [importState?.status, fetcher.data]);
  const pageSelected = rows.filter((row) => isIpSelected(selection, row.ip)).length;
  const hasSelection = selection.mode === "all" || selection.ips.length > 0;
  const submitEnrichment = (again = false) => {
    if (busy || (!again && !hasSelection)) return;
    const id = again && request.current ? request.current.id : crypto.randomUUID();
    const state = again && request.current ? request.current.state : currentState;
    request.current = { id, state };
    setRetry(false);
    void fetcher.submit(
      JSON.stringify({ action: "enrich", selection: state.selection, submissionId: id }),
      { method: "post", encType: "application/json", action: "/admin/ip-addresses" },
    );
  };
```

Replace the result `Alert` block (lines 243-264, `{fetcher.data && !submitting ? (` … `) : null}`) with:

```tsx
      {receipt && <QueueImportStatus receipt={receipt} state={importState} queue="ip-enrichment" />}
      {(fetcher.data?.ok === false || importError) && (
        <Alert variant="destructive">
          <AlertDescription>{fetcher.data?.ok === false ? fetcher.data.error : importError}</AlertDescription>
        </Alert>
      )}
      {retry && request.current && (
        <Button variant="outline" size="sm" disabled={busy} onClick={() => submitEnrichment(true)}>
          Retry queue import
        </Button>
      )}
```

Replace the "Enrich IP addresses" button (lines 294-311) with:

```tsx
          <Button size="sm" disabled={busy || !hasSelection} onClick={() => submitEnrichment()}>
            {submitting ? "Submitting…" : "Add to enrichment queue"}
          </Button>
```

and the paragraph below it (lines 313-316) with:

```tsx
        <p className="text-muted-foreground text-xs">
          Adds the selection to the open IP enrichment draft. Start processing from{" "}
          <Link to="/admin/queues/ip-enrichment">Queues → IP enrichment</Link>; GeoIP, ASN and
          RDAP are saved per address and fresh RDAP coverage is reused.
        </p>
```

- [ ] **Step 6: Queue page: IP enrichment is a draft queue**

`app/lib/queues.ts` — after line 17 (`ACTIVE_QUEUE_RUNS`) add:

```ts
/** Queues on the shared processing queue contract: one open draft per scope, freeze at Start, partition purge. */
export const DRAFT_QUEUES: readonly QueueType[] = ["webtech", "crawler", "ip-enrichment"];
export function isDraftQueue(type: QueueType) { return DRAFT_QUEUES.includes(type); }
```

`app/lib/queues.server.ts`:
- line 53: `const inputOrder = filters.type === "brave" ? "input_id, task_id" : "task_id, input_id";` (Brave's table is still sorted the legacy way).
- lines 95-109 (`loadQueueHistory`): before the `for` loop add `const OUTCOME_TAGS: Record<QueueFilters["type"], string | null> = {webtech: "webtech", crawler: "crawler", "ip-enrichment": "ip_enrichment", brave: null};` and replace lines 108-109 with:

```ts
    const prefix = OUTCOME_TAGS[filters.type];
    const outcome = prefix && run.status === "SUCCESS" && ["completed", "completed_with_errors"].includes(run.tags[`${prefix}/outcome`]) ? run.tags[`${prefix}/outcome`] : null;
```

`app/routes/admin-queue.tsx`:
- line 13: add `isDraftQueue` to the `~/lib/queues` import.
- line 24: `if (isDraftQueue(filters.type) && (!filters.task || inputs.value.selectedTotal === 0)) {`
- line 61: `const processingBlocked = !filters.task ? (isDraftQueue(filters.type) ? "The queue is empty. Add inputs to prepare the next execution." : "Choose a task below to configure processing.")`
- line 104: `{((!isDraftQueue(filters.type) && !filters.task) || (isDraftQueue(filters.type) && inputs.totalTasks > 1)) && <>`
- line 120: `<p className="text-sm text-muted-foreground">{isDraftQueue(filters.type) ? "Inputs can be appended while the task is a draft. Dagster checks submissions and freezes the task when execution begins." : "This queue uses a fixed input selection."}</p>`
- line 132: `colSpan={isDraftQueue(filters.type) ? 3 : 4}`
- line 141: `Completed inputs are removed from Webtech, Crawler and IP enrichment queues; results and history remain available.`
- line 147: `{filters.type === "crawler" ? "crawl errors" : filters.type === "ip-enrichment" ? "address errors" : "page errors"}`
- line 149 stays (the history source column exists only for crawler/webtech; IP enrichment has no `queue_task_sources` rows).

`app/components/admin/queue-process-sheet.tsx`:
- line 4: import `isDraftQueue` too.
- lines 83-85: the paragraph becomes

```tsx
            <p className="text-sm text-muted-foreground">{filters.type === "ip-enrichment"
              ? "A draft freezes when the results asset begins. GeoIP, ASN and RDAP are saved per address in acknowledged batches; cached RDAP coverage is judged against the frozen start time and the cache window. Leave the RDAP request budget empty to process the whole task; a reached budget keeps the task resumable. RIPE and APNIC are asked without personal data (RIPE REST search, APNIC whois -r); per-registry budgets are a Dagster launchpad setting."
              : isDraftQueue(filters.type)
              ? "Freshness is checked when execution is prepared. Recent inputs remain in the queue and are counted as skipped. A draft freezes when the results asset begins."
              : "Searches use the saved company inputs. Freshness and force options are evaluated during processing."}</p>
```

- line 106: `{isDraftQueue(filters.type) ? "For draft queues, leave empty …" : "Leave empty for a new execution. …"}` (same two texts as today).
- line 113: `{isDraftQueue(filters.type) ? "Inputs are removed when every entry has a saved outcome or is skipped as recent. Lookup errors remain in results and history. Pipeline failures keep inputs for recovery. Use the processing profile to control this execution." : "Existing input rows are retained for retries."}`

- [ ] **Step 7: Typecheck and targeted tests**

Run: `npm run typecheck`
Expected: clean.

Run: `npx vitest run tests/ip-enrichment.server.test.ts tests/admin-ip-addresses-action.test.ts tests/queues.server.test.ts tests/queue-route.test.ts tests/crawl-queue.server.test.ts tests/webtech-queue.server.test.ts`
Expected: all pass (these files mock ClickHouse and Dagster; do not run the full suite).

Run: `rg -n "launchIpEnrichment|ip_enrichment_workflow|ip_enrichment_input FINAL" app tests docs` → no matches.

- [ ] **Step 8: Commit**

```bash
git add app/lib/ip-enrichment.server.ts app/routes/admin-ip-enrichment-queue-submission.ts app/routes.ts app/components/admin/queue-import-status.tsx app/routes/admin-ip-addresses.tsx app/lib/queues.ts app/lib/queues.server.ts app/routes/admin-queue.tsx app/components/admin/queue-process-sheet.tsx tests/ip-enrichment.server.test.ts tests/admin-ip-addresses-action.test.ts tests/queues.server.test.ts tests/queue-route.test.ts
git commit -m "feat(backoffice): IP addresses add to the enrichment draft; the IP enrichment queue behaves like Webtech and Crawler

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---
### Task 7: Documentation

**Files:**
- Create: `services/dagster_v3/docs/operations/ip-enrichment-draft-queue.md`
- Modify: `services/dagster_v3/docs/ip-enrichment-schema.md:3-8, 10-29, 92-151, 153-221, 248-268`
- Modify: `services/backoffice/docs/queues.md:11, 20, 22` and append a section
- Modify: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/docs/commoncrawl_geoip-design.md` (append)
- Modify: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md` (append a section)
- Modify: `services/dagster_v3/docs/deployment-runbook.md:40`
- Modify: `services/dagster_v3/docs/operations/ip-registry-reference-data.md:75-81` (the second "Known costs" bullet)

- [ ] **Step 1: Write the operations guide**

Create `services/dagster_v3/docs/operations/ip-enrichment-draft-queue.md`:

````markdown
# IP enrichment draft queues

Backoffice **Admin → IP addresses → Add to enrichment queue** launches only
`ip_enrichment_input_job`. There is one open draft per `queue_scope` (default `workspace`);
table selections, explicit IP lists and "failed addresses of task X" append to it. Adding
inputs never looks anything up. Since ClickHouse migration 456 the draft follows the shared
processing queue contract
([spec](../superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md)), like
Webtech and the crawler.

## Storage

- ClickHouse `corpscout.ip_enrichment_input`: the only stored copy of each entry
  (`task_id, input_id, ip, source_name, source_record_id, source_run_id, submission_id,
  observed_at, submitted_at`; `ip_version` and `bucket` materialized), `MergeTree`,
  `PARTITION BY task_id`, `ORDER BY (task_id, input_id)`. Read it without `FINAL`.
  `input_id` is `bbb:` (the address's 256-way bucket, zero-padded) followed by the JSON
  tuple `[source_name, source_record_id, ip]`, computed in ClickHouse and enforced by a
  `CHECK`, so a task is walked bucket by bucket.
- PostgreSQL `processing.tasks`: lifecycle, the frozen execution (`config.execution`) and the
  counters written at completion. `processing.input_submissions`: idempotent import receipts
  (selection fingerprint and count; bulk IP lists are stored as count + sha256).
- `corpscout.ip_enrichment_results` (`ORDER BY (bucket, ip, result_id)`): one row per address
  and execution (`attempt` is always 1; a retry is a new draft). `ip_enrichment_current`
  serves the latest conclusive data per address. RDAP coverage stays in `rdap_networks`,
  `rdap_network_segments` (roles `lookup_result`, `parent`), `rdap_ip_lookup_results` and the
  class table `rdap_network_registry_class`
  ([ip-registry-reference-data.md](ip-registry-reference-data.md)).

## Import

Materialize `ip_enrichment_input` with a stable `submission_id` and exactly one of:

```yaml
ips: [116.203.39.236, 2001:4860:4860::8888]          # explicit list
source_relation: corpscout.commoncrawl_ip_addresses  # + filters / ip_search / max_rows / select_all
retry_failed_task_id: <task uuid>                    # addresses whose result in that task has an error
```

Selection, canonicalization and grouping run inside ClickHouse (`selected_ips_sql`); the
import is one `INSERT … SELECT` minus the identities the draft already holds, then two counts
for the receipt (`input_count` = rows this submission added, `total` = the draft). No size
cap. A failed import blocks Start; retry it with the same `submission_id` and selection: the
retry kills its ClickHouse query (`ip-queue-import:<submission_id>`), deletes only that
submission's rows (synchronous lightweight delete) and reselects from the source as it is now.
Imports and Start share the task's PostgreSQL advisory lock.

## Start and resume

**Queues → IP enrichment** selects the current draft; **Configure processing** launches
`ip_enrichment_results_job` with `task_id`. Under the task lock, the results asset:

1. freezes the draft (`status=selected`, `frozen_at`) and saves the execution in
   `processing.tasks.config.execution`: `execution_id` (the original Dagster run id),
   `profile` (`force_rdap`, `rdap_cache_days`, `parent_depth`, `rate_limit_retry_seconds`,
   `transient_retry_seconds`, `ripe_rest`, `apnic_whois`, `processor_version`), `started_at` and
   `freshness_cutoff = started_at − rdap_cache_days`. `batch_size`, `max_requests`,
   `request_delay_seconds` and `registry_daily_budgets` are transport settings and may
   change between resumes. The default-draft slot is released at once, so new additions form the
   next draft;
2. loops until nothing remains. *Remaining* is a live query per bucket: the task's entries
   in that bucket whose `input_id` has no row in `ip_enrichment_results` for this execution
   (`bucket = b AND task_id AND execution_id`, one primary-key range). Pages of `batch_size`
   follow an `input_id` cursor inside the bucket; each page costs one negative-cache read
   (`rdap_ip_lookup_results_current`), one trie `dictGet`, one read of the network rows the
   page needs (never `raw_response`), one insert of lookup markers and a share of one result
   insert. Freshness is judged against the frozen execution: a network or marker counts when
   its time is `>= freshness_cutoff`, a retryable error when `retry_after > started_at`;
   `force_rdap` skips both caches. Registry requests (RIPE through its REST search, APNIC
   through whois `-r`, the others through RDAP) happen only for misses, paced by `request_delay_seconds`; a miss also
   costs one registry-class context query, and networks fetched earlier in the run are reused
   before any request. GeoIP City/ASN are read locally
   per address;
3. stores outcomes through a `ResultBuffer` (500 rows or 5 seconds, acknowledged
   `async_insert`); the cursor never re-reads a page inside a pass, the buffer is flushed
   after every pass and a further pass confirms nothing remains. A reached `max_requests`
   flushes what was resolved and fails the run with "budget reached"; the task stays
   `selected` and re-running it resumes (this is the pause-and-resume procedure: terminate
   or let the run stop, change transport settings if needed, re-run the task);
4. finishes when a pass finds nothing: counts succeeded/failed per bucket from the results
   (failed = any of City/ASN/RDAP in `retryable_error`/`terminal_error`), `skipped = total −
   succeeded − failed` (expected 0), marks the task `completed` (`completed_with_errors` in
   the run tags when any address failed), drops the task's partition and records
   `inputs_purged_at`.

Re-running the same task with the same profile resumes the saved execution (`execution_id`
may be omitted; an explicit one must be the saved one). A completed task re-run only retries
cleanup. A changed profile is rejected; to re-look addresses up, add them to a new draft
(`force_rdap: true` there bypasses caches; `retry_failed_task_id` selects the failures).
History stays readable from run tags (`ip_enrichment/execution_id`, `ip_enrichment/outcome`,
`ip_enrichment/succeeded_pages`, `ip_enrichment/failed_pages`, `ip_enrichment/skipped_pages`).

## RIPE and APNIC without personal data, and the per-registry budget

The RIPE Database acceptable use policy limits the **personal data sets** (person and role
objects) one source address may receive to **1,000 per 24 hours**; queries themselves are
unlimited within reasonable use (3 simultaneous connections at most), and pooling limits
across addresses is named as avoidance. RIPE's RDAP `ip` answers embed 1–5 person objects,
so every one counts. We do not need contacts, so RIPE misses go to the RIPE Database REST
search with `flags=no-referenced` (`commoncrawl_rdap/ripe_rest.py`): the most specific
`inetnum`/`inet6num` with `netname`, `country`, `status`, `org` handle, `mnt-by` and dates,
no person or role object, so nothing counts. The answer is stored like an RDAP one (same
`ripe:<range>` network key, `rdap_self_url` under `rest.db.ripe.net`, empty registrant
names; `descr` is dropped). Unallocated or non-authoritative space answers with RIPE's root
object; the resolver then asks RDAP once. `ripe_rest: true` is part of the frozen profile.

APNIC misses go to APNIC's whois service on port 43 with the `-r` flag
(`commoncrawl_rdap/apnic_whois.py`; the HTTP gateway `wq.apnic.net` does not honour `-r`):
the most specific `inetnum`/`inet6num` with contact handles only, followed by route objects
that are ignored. The holder name is the **first `descr` line** (APNIC objects rarely carry
`org:`); further `descr` lines (addresses) and every contact handle are dropped; `status` is
upper-cased and whitespace-collapsed. An answer that is an NIR's own allocation object —
`netname` starting with `JPNIC`, `KRNIC`, `TWNIC`, `IDNIC`, `CNNIC`, `IRINN` or `VNNIC`, or a
first `descr` naming the NIR — falls back to RDAP, which the IANA bootstrap routes to the
NIR server; `mnt-by` alone decides nothing (FPT's `103.35.64.0/22` is maintained by
`MAINT-VN-VNNIC` and is the holder's allocation). `apnic_whois: true` is part of the frozen
profile. ARIN, LACNIC and AFRINIC stay on RDAP.

Every run reports `rdap_person_entities_by_registry` (vCards of kind `individual`); it must
never have a `ripe` or an `apnic` key.

`registry_daily_budgets` (transport, default `{}`, keys are whoisit's names: `ripe`, `arin`,
`apnic`, `lacnic`, `afrinic`, `jpnic`, `idnic`, `krnic`, `twnic`, `registro.br`) is an
optional rolling 24-hour **request** budget per registry. The registry of a miss is resolved
from whoisit's bootstrap data before the request (`RdapClient.registry_for`); a miss of a
registry at its budget is deferred (no result row, no error), the run keeps processing
everything else, and when a whole pass resolved nothing it sleeps until an hour's share of
the budget frees (`wait_for_registry_budget`, logged every 10 minutes). The window is seeded
on start from `rdap_networks.fetched_at` of the last day (every writer). Each run reports
`rdap_requests_by_registry` and `rdap_deferrals_by_registry`. Proxy egress lanes were
considered and dropped on 2026-09-26: they would not shorten a run (pacing is global), the
no-personal-data paths make them unnecessary, and pooling a registry's allowance across
addresses is the AUP's anti-avoidance case. The budget is not in the backoffice sheet; set it
in the Dagster launchpad.

## Registry-level registrations

Every miss is classified with the deployed rule (`classify_registration`, data from
`ip_registry_daily`): `reusable`, `registry_level`, `unallocated` or `unknown` while the
reference data is incomplete. A `registry_level`/`unallocated` registration is stored like
any other (role `lookup_result`) with its class row; `rdap_network_trie` excludes it
(migration 451) and the in-run cache never remembers it, so it answers only the queried
address — which is served next time by its own `found` marker in `rdap_ip_lookup_results`.
Other addresses of the block get their own lookup. Details and queries:
[ip-registry-reference-data.md](ip-registry-reference-data.md).

## GeoLite2 (manual updates)

There is no MaxMind account, so the files are replaced by hand. `MAXMIND_DATABASE_DIRECTORY`
on the Dagster host holds `GeoLite2-City.mmdb` and `GeoLite2-ASN.mmdb` directly. Procedure:

1. Download `GeoLite2-City.tar.gz` and `GeoLite2-ASN.tar.gz` from MaxMind (owner's browser
   login), copy them to the host and extract:
   `tar -xzf GeoLite2-City.tar.gz --strip-components=1 -C /tmp '*/GeoLite2-City.mmdb'`
   (same for ASN).
2. Move each file over the old one **with `mv`** (a rename; never `cp` over the existing
   file — a running enrichment maps it): `mv /tmp/GeoLite2-City.mmdb "$DIR/GeoLite2-City.mmdb"`
   (same for ASN), then `chown` to the Dagster service user.
3. No restart: `ip_enrichment_results` opens the files per run. A running run keeps its old
   inode until it ends; the next run uses the new files.
4. Execute the check `geolite2_databases_fresh` (job `geolite2_freshness_job`, or launch
   `ip_enrichment_results` with its checks): it fails when either build epoch is older than
   14 days. Every results run also reports `geolite2_city_build`/`geolite2_asn_build` and
   warns when stale.

## The 2026-09 clean re-run

On 2026-09-25 the 48.6M-IP run (task `4802549d-…`) was terminated after 1.08M addresses:
~90% of its wall time were per-address ClickHouse round trips and 174k addresses had been
answered by registry-level blocks (`APNIC-AP` 103.0.0.0/8 and similar). The owner decided a
clean re-run instead of a remediation: at deploy (plan
`2026-09-25-ip-enrichment-queue-contract.md`, Task 8) the four legacy PostgreSQL tasks were
cancelled and `ip_enrichment_input`, `ip_enrichment_results` (including the 8.29M legacy
GeoIP import rows of 2026-07-10, task `cd603d91-…`), `rdap_networks`,
`rdap_network_segments`, `rdap_ip_lookup_results` and `rdap_network_registry_class` were
truncated after checking the latest ClickHouse B2 backup; the `ip_registry_*` reference
tables and `ip_registry_daily` were kept. Until the re-run reaches an address,
`ip_enrichment_current` has nothing for it (backoffice IP pages, `commoncrawl_ip_checks`).
Classes regenerate per miss and at the daily refresh.

## Validation

`tests/test_ip_enrichment_input.py` (disposable ClickHouse + PostgreSQL): entry-table contract,
canonical dedup and append, receipt replay, a retry replacing only its own rows, source
filters, inventory search, the failed-results mode, freeze → next draft.
`tests/test_ip_enrichment_results.py`: bounded round trips per page, frozen cache window,
negative markers, registry classes per miss (trie exclusion, per-address marker), in-run reuse
and the request budget, RIPE via REST and APNIC via whois `-r` without person objects (the
root-object and NIR fallbacks, the real FPT answer), budget deferral and wait (resolver and loop), completion
with partition purge, errors as published outcomes, budget resume, lost write and cleanup
acknowledgements, changed-profile refusal. `tests/test_geolite2_freshness.py`: build times,
14-day rule, check wiring.
````

- [ ] **Step 2: Update the schema doc**

In `services/dagster_v3/docs/ip-enrichment-schema.md`:
- lines 3-8: replace "The `ip_enrichment_input` Dagster asset prepares input batches from a list or a source relation. `ip_enrichment_results` processes a prepared task using the existing MaxMind mapping and RDAP network cache. The Workspace IP addresses page submits both steps through `ip_enrichment_workflow`." with "Since migration `000456` the input table follows the shared processing queue contract: `ip_enrichment_input` appends to an open draft and `ip_enrichment_results` freezes and processes it (see [ip-enrichment-draft-queue.md](operations/ip-enrichment-draft-queue.md)). The Workspace IP addresses page adds to the draft; processing starts from the queue page." Keep the sentence about migrations 434–435 and add: "Their imported rows were removed in the 2026-09 clean re-run; every row now comes from `ip-enrichment-v1`."
- lines 12-16: replace with "`ip_enrichment_input` is `MergeTree`, `PARTITION BY task_id`, `ORDER BY (task_id, input_id)`, read without `FINAL`. `input_id` is the bucket-prefixed JSON tuple of `source_name`, `source_record_id` and `ip`, computed and enforced in ClickHouse; the draft keeps one row per identity and a completed task drops its partition."
- lines 92-151 ("Materializing the input asset"): replace "Apply migration 000433 before using" with "Apply migrations 000433 and 000456 before using"; replace the first paragraph's asset description with the draft semantics (stable `submission_id`, `queue_scope`, one of `ips`/`source_relation`/`retry_failed_task_id`), keep the two YAML examples, add `retry_failed_task_id: "<task uuid>"` as a third, and replace the paragraphs from "The materialization reports `task_id`, `selected_inputs`, and `selected_ips`." to the end of the section with: "The materialization reports `task_id`, `submission_id`, `input_count` (rows this submission added) and `total`. Repeating a `submission_id` with the same selection is a no-op; a different selection under it is rejected; a failed import is retried by reselecting the source. New submissions after Start go to the next draft. `tests/test_ip_enrichment_input.py` exercises every mode and crash recovery using disposable ClickHouse and PostgreSQL servers."
- lines 153-221 ("Materializing enrichment results"): keep the YAML example and add `registry_daily_budgets: {}`, `ripe_rest: true` and `apnic_whois: true` to it; replace from "The input batch must be fully prepared." through the end of the section with the summary: freeze via the shared lifecycle, per-bucket live remaining query, bounded round trips per page, one class-context query per miss, `ResultBuffer` writes, frozen cache window, RIPE via the REST search and APNIC via whois `-r` without personal data (NIR space falls back to RDAP), optional per-registry budgets with deferral and waits, `max_requests` budget → failed run that resumes on re-run, errors are published outcomes (`completed_with_errors`), completion drops the partition, `attempt` always 1, retries via a new draft, GeoLite2 build dates in the metadata; end with the pointer to the operations guide.
- lines 248-268 ("Workspace IP selection"): replace "`ip_enrichment_workflow` runs input preparation before results processing with one shared task UUID." with "**Add to enrichment queue** launches `ip_enrichment_input_job` with a stable `submission_id` and `queue_scope: workspace`; processing is started from Queues → IP enrichment." and replace the last paragraph with "The queue sheet's template sends `max_requests: null` so the whole task is processed; the standalone asset default remains 250 requests. Large drafts run in the background and the queue page links to their Dagster run."

- [ ] **Step 3: Update the backoffice queue doc and the design docs**

`services/backoffice/docs/queues.md`: line 11 `| IP enrichment | `ip_enrichment_input` (draft) | `ip_enrichment_results_job` |`; line 20 "Other processors retain their existing fixed input selection lifecycle." → "Brave retains its fixed input selection lifecycle."; line 22 "Other processors use the original results run ID for resumption." → "Brave uses the original results run ID for resumption."; append:

```markdown
## IP enrichment drafts

Admin → IP addresses → **Add to enrichment queue** imports the selection into
`ip_enrichment_input` through `ip_enrichment_input_job` with a stable submission ID; the
status component polls `/admin/ip-enrichment/queue-submissions/<runId>` and links the saved
task. A successful import clears the selection; an accepted launch alone does not. The
entry table is partitioned by task and read without `FINAL`.

Queues → IP enrichment automatically selects the current draft; Start launches
`ip_enrichment_results` with `task_id`. `batch_size`, `max_requests` and
`request_delay_seconds` may change between resumes; the RDAP policy fields are frozen at
Start. Lookup errors complete the task “with errors”; add the failed addresses to a new
draft (`retry_failed_task_id`) to retry them. RIPE and APNIC are asked without personal data
(REST search, whois `-r`); the per-registry request budget (`registry_daily_budgets`) is a
Dagster launchpad setting, not a sheet field. The old one-shot `ip_enrichment_workflow` is
removed.
```

`commoncrawl_geoip-design.md`: append "The GeoLite2 files are replaced by hand (no MaxMind account); the check `geolite2_databases_fresh` on `ip_enrichment_results` (`freshness.py`, job `geolite2_freshness_job`) fails when either file is older than 14 days, and every results run reports the build dates. Procedure: [ip-enrichment-draft-queue.md](../../../../../docs/operations/ip-enrichment-draft-queue.md)."

`commoncrawl_rdap-design.md`: append a section "## IP enrichment: page batching and registry budgets" — per page one negative-cache read, one trie `dictGet`, one network read without `raw_response`, one marker insert; per miss the class-context query then network, class row, segments; `RdapClient.registry_for`; the RIPE REST search without personal data (`ripe_rest.py`, and why: the AUP's limit of 1,000 personal data sets per day, the 1–5 person objects in a RIPE RDAP answer); APNIC whois `-r` (`apnic_whois.py`: port 43, first-`descr` holder name, status normalisation, the NIR-object rule and why `mnt-by` does not decide, the `wq.apnic.net` gateway that ignores `-r`); the optional per-registry daily budget (deferral, wait, seeding from `rdap_networks` of the last day); the legacy bucket worker keeps its per-IP RDAP lookups — RIPE and APNIC included, so it does consume personal data sets — and shares the budget window only through the seed.

`deployment-runbook.md` line 40: `| MaxMind dir | `MAXMIND_DATABASE_DIRECTORY` (files replaced by hand; check `geolite2_databases_fresh`) | ip_enrichment |`.

`docs/operations/ip-registry-reference-data.md` lines 75-81 (the second "Known costs" bullet): replace with "- IP enrichment serves an address answered by a registry-level or unallocated registration from that address's own `found` marker in `rdap_ip_lookup_results` (per-address positive cache, since the page-batched resolver); other addresses of the block are looked up over RDAP once per task while they fall inside `rdap_cache_days`. The legacy bucket worker has no positive per-IP cache and asks again."

- [ ] **Step 4: Check for stale wording**

Run from `corpscout/`: `rg -n "ip_enrichment_workflow|prepare_selection|selected_ips\b|unique_ips|input_id, task_id|Enrich IP addresses|MAXMIND_ACCOUNT_ID|geolite2_update|18,?000|contact_entities|use_rdap_proxies|RDAP_PROXIES|egress" services/dagster_v3/docs services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/docs services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs services/backoffice/docs`
Expected: only the sentences that say the workflow is removed.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/docs/operations/ip-enrichment-draft-queue.md services/dagster_v3/docs/ip-enrichment-schema.md services/backoffice/docs/queues.md services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/docs/commoncrawl_geoip-design.md services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md services/dagster_v3/docs/deployment-runbook.md services/dagster_v3/docs/operations/ip-registry-reference-data.md
git commit -m "docs: IP enrichment on the shared queue contract, registry budgets, manual GeoLite2 updates, the 2026-09 clean re-run

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---
### Task 8: Deploy, clean wipe, smoke batch and the full re-run on prod — REQUIRES THE OWNER'S GO-AHEAD BEFORE STEP 1 AND AGAIN BEFORE STEP 4

**Files:** none until Step 12 (spec status line). Everything here runs against prod; the executor runs the commands (ask only when a permission gate refuses), the owner gives the two go-aheads.

**Wipe design (R3), decided:** `TRUNCATE TABLE`, not drop-and-recreate. The tables keep the DDL their migrations own (000124, 000433, 000450), nothing changes in the ledger, and truncation of MergeTree families is a metadata operation whatever the row count. Order: PostgreSQL tasks → `ip_enrichment_input` → `ip_enrichment_results` → `rdap_ip_lookup_results` → `rdap_network_segments` → `rdap_networks` → `rdap_network_registry_class`, then `SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie`. Effects: the ordinary views (`ip_enrichment_current`, `rdap_*_current`, `rdap_network_registry_class_derived`) need no change and return nothing until refilled; the trie reloads to 0 elements (status `LOADED`); `ip_registry_*` tables, `ip_registry_special_trie`, `ip_registry_ready` and `ip_registry_daily` are untouched (the daily class refresh inserts nothing while `rdap_networks_current` is empty and reclassifies whatever the re-run stores later; its check `classification_complete` passes with 0 networks); the backoffice IP pages (`workspace-ip-addresses.server.ts`, `queries.server.ts`) and `commoncrawl_ip_checks` read `ip_enrichment_current` only and show no enrichment until the re-run reaches an address; the 8.29M legacy GeoIP rows (City/ASN builds of 2026-07-10) are gone for good — the B2 backup checked in Step 2 is the only rollback. The legacy worker `commoncrawl_ip_rdap_networks` has no schedule; it must not run during the wipe (Step 1) and should not be launched during the re-run (it shares the `commoncrawl_rdap` pool, limit 1, so it would only queue behind the run, but its per-IP lookups would count against the registry windows without the batching).

- [ ] **Step 1: Preconditions**

- **Merge order (final review C2, 2026-09-26).** Prod's ClickHouse ledger reached 454 from the unmerged branch `codex/crawl-normalization-schema` (`000452_corpscout_website_crawl_normalized`, `000453_corpscout_brave_draft_queue`, `000454_corpscout_retire_brave_legacy_input`, plus an untracked `000455_corpscout_brave_search_versions` in the main checkout); `origin/main` was at 451 (`b33ca08d2`). Before anything below:
  - (a) `codex/crawl-normalization-schema` is merged to `main` first — by the owner or that branch's session, not by this task. Check: `git merge-base --is-ancestor codex/crawl-normalization-schema main && echo merged`.
  - (b) This branch is then merged (or rebased) onto that `main` with the conflicts resolved so that Brave is a draft queue too. The shared backoffice lines both branches edit: `app/lib/queues.ts` (`DRAFT_QUEUES` includes `brave` if codex made it one), `app/lib/queues.server.ts` (the `inputOrder` rule becomes "draft queues → `(task_id, input_id)`", i.e. `isDraftQueue(filters.type) ? "task_id, input_id" : "input_id, task_id"`; `OUTCOME_TAGS` keeps codex's `brave` prefix next to `ip_enrichment`), `admin-queue.tsx` (the three draft-queue conditions and the empty-row `colSpan`), `queue-process-sheet.tsx` (its Brave copy) and `queue-import-status.tsx`. After the merge, from `services/backoffice`: `npm run typecheck` and `npx vitest run tests/ip-enrichment.server.test.ts tests/admin-ip-addresses-action.test.ts tests/queues.server.test.ts tests/queue-route.test.ts tests/crawl-queue.server.test.ts tests/webtech-queue.server.test.ts` → clean and green; in `services/dagster_v3` the suites named in the final-fix report stay green. `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py` then lists 452–455 (from codex) before `000456_corpscout_ip_enrichment_queue_contract`.
  - (c) The code prod dagster_v3 runs is contained in the `main` being deployed, so Step 7's light_sync does not roll back the Brave/crawl code whose migrations are applied. light_sync rsyncs trees (`src/dagster_v3`, `exchange_rates`, `pyproject.toml`/`uv.lock`) to the Dagster host (`dagster` in `ansible/inventory.ini`, deploy dir `/opt/companycollect/corpscout/dagster_v3` from `group_vars/dagster_hosts/vars.yml`) and records no commit, so confirm it by content: ask the session that last deployed which commit it synced and check `git merge-base --is-ancestor <that commit> main`, and in any case preview from the main checkout with `rsync -rnc --delete --itemize-changes --exclude __pycache__ services/dagster_v3/src/dagster_v3/ dagster:/opt/companycollect/corpscout/dagster_v3/src/dagster_v3/` (dry run, nothing is written). Paths this plan changed (`defs/ip_enrichment/`, `defs/commoncrawl_rdap/`, `defs/commoncrawl_geoip/` and the definitions that register them) are expected. Any other listed path is either newer on `main` (a commit after prod's last sync — fine) or prod-only: fetch the deployed file (`ssh dagster cat …`) and find a `main` commit with that content (`git log main -- <path>`). A `*deleting` line (a file only prod has) or deployed content no `main` commit has means prod runs code `main` lacks — stop and find its commit before Step 7.
- Everything is merged on `main`, the tree is clean apart from other sessions' untracked files, and `ls clickhouse/migrations | tail -2` shows `000456_corpscout_ip_enrichment_queue_contract.{down,up}.sql` (renumbered from 000453 on 2026-09-26; renumber again before merging if another workstream took 456).
- Prod ledger: `ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT version, dirty FROM corpscout.schema_migrations ORDER BY version DESC LIMIT 6"'` → the highest version with a `dirty=0` row is `455` (or `454` if codex's `000455` has not been applied), no higher version has only a `dirty=1` row, and nothing is at or above 456. Two more conditions, because `migrate up 1` applies the next file after the ledger's version: **main contains the file of every applied version** (`for v in $(ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT DISTINCT version FROM corpscout.schema_migrations WHERE dirty=0 AND version >= 440 ORDER BY version"'); do ls clickhouse/migrations/$(printf %06d $v)_*.up.sql >/dev/null || echo "missing $v"; done` prints nothing), and **the ledger's highest version is the highest file on main below 456** (if main has a `000455_*.up.sql` that prod has not applied, `up 1` would apply that one instead of ours: stop and have its owner apply it first).
- No active runs: in the Dagster UI the run lists of `ip_enrichment_results_job`, `ip_enrichment_input_job`, `ip_enrichment_workflow`, the asset `commoncrawl_ip_rdap_networks` and `ip_registry_refresh_job` filtered to `STARTED`/`QUEUED`/`STARTING` are empty (the 48.6M run `83283501-…` was terminated on 2026-09-25). Do not start between 06:00 and 06:30 UTC (`ip_registry_daily` at 06:05) nor inside the Tuesday 01:05 Stockholm address-chain window.
- Confirm nothing wrote the tables recently: `ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT max(event_time) FROM system.query_log WHERE event_date >= today() - 1 AND type = '"'"'QueryFinish'"'"' AND query_kind = '"'"'Insert'"'"' AND hasAny(tables, ['"'"'corpscout.ip_enrichment_results'"'"', '"'"'corpscout.rdap_ip_lookup_results'"'"', '"'"'corpscout.ip_enrichment_input'"'"'])"'` → a time before the terminated run ended (2026-09-25 14:47 UTC) or earlier today.
- GeoLite2 files on the host: `ssh dagster 'ls -l "$(sudo grep "^MAXMIND_DATABASE_DIRECTORY=" /opt/companycollect/corpscout/dagster_v3/.env | cut -d= -f2)"'` → `GeoLite2-City.mmdb` and `GeoLite2-ASN.mmdb`. Their builds are 2026-07-10: tell the owner that `geolite2_databases_fresh` will fail until the files are replaced by hand (procedure in `docs/operations/ip-enrichment-draft-queue.md`) and recommend doing that before Step 10, so the 49M new rows carry current GeoIP. Do not fetch the files yourself (no account).
- Outbound port 43 to `whois.apnic.net` must be open from the Dagster host (the APNIC path is TCP whois, not HTTPS): `ssh dagster 'timeout 15 bash -c "printf \"-r 103.35.64.49\\r\\n\" | nc -w 10 whois.apnic.net 43" | grep -E "^(inetnum|netname|descr|person|role|irt):"'` → `inetnum: 103.35.64.0 - 103.35.67.255`, `netname: FPT-VN`, `descr: FPT Telecom`, and no `person:`/`role:`/`irt:` line.
- Record the inventory of what will be wiped (SELECT only) into the scratchpad as `wipe-inventory-before.out`:

```bash
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT (SELECT count() FROM corpscout.ip_enrichment_input) AS input_rows, (SELECT uniqExact(task_id) FROM corpscout.ip_enrichment_input) AS input_tasks, (SELECT count() FROM corpscout.ip_enrichment_results) AS result_rows, (SELECT countIf(processor_version = '"'"'legacy-geoip-import-v1'"'"') FROM corpscout.ip_enrichment_results) AS legacy_geoip_rows, (SELECT count() FROM corpscout.rdap_networks) AS networks, (SELECT count() FROM corpscout.rdap_network_segments) AS segments, (SELECT count() FROM corpscout.rdap_ip_lookup_results) AS markers, (SELECT count() FROM corpscout.rdap_network_registry_class) AS classes FORMAT PrettyCompact"'
```

Expected orders of magnitude (2026-09-25 evening): input 48,596,636 rows / 4 tasks, results 9,374,666 rows of which 8,291,326 legacy GeoIP, rdap_networks 16,632, segments 17,308, lookup results 227,084, class rows 15,319. A materially larger results count means something wrote after planning: stop and find the writer before wiping.

- [ ] **Step 2: Backup safety check (no backups are run here)**

```bash
ssh companycollect 'docker exec clickhouse-clickhouse-backup-1 clickhouse-backup list remote'
ssh companycollect 'docker logs --tail 30 clickhouse-clickhouse-backup-1'
```

Expected: the newest remote backup is dated today or yesterday (the sidecar runs an incremental every 24 h and a full every 72 h, keeping 3), it is not marked broken, and the log shows no upload in progress (an in-progress backup looks broken until its `metadata.json` lands — wait for it to finish rather than truncating under it). Write the newest backup's name into the scratchpad (`scratchpad/ip/rollback-backup.txt`); it is the rollback point. If there is no backup from the last 48 hours, stop and tell the owner (the owner decides whether to trigger one; never run `clickhouse-backup create`/`upload`/`delete` from here — see the memory `clickhouse-b2-backup-retention`).

- [ ] **Step 3: Owner go-ahead for the wipe**

Show the owner `wipe-inventory-before.out`, the rollback backup name and the effects listed in the wipe design above. Continue only on an explicit yes.

- [ ] **Step 4: Cancel the legacy PostgreSQL tasks**

```bash
ssh companycollect "docker exec ppoint-postgres psql -U corpscout -d corpscout -Atc \"SELECT task_id, status, total, queue_scope FROM processing.tasks WHERE processor='ip-enrichment-v1' ORDER BY created_at\""
```

Expected: exactly `a0a328d6-15ea-4c68-b09d-4ae9bd65c222` (3), `6f3377b2-6ee7-4d51-b4a2-d1ab53474b7d` (1), `422ce6d8-b87c-4748-b0ef-bdb0fe002f33` (1), `4802549d-c320-482f-9bbf-6f21b8ffcd18` (48,596,631), all `selected`, `queue_scope` empty. Then (`tasks_queue_lifecycle_check` allows only `preparing/selected/ready/cancelled` for `queue_scope IS NULL`):

```bash
ssh companycollect "docker exec ppoint-postgres psql -U corpscout -d corpscout -Atc \"UPDATE processing.tasks SET status='cancelled' WHERE processor='ip-enrichment-v1' AND queue_scope IS NULL AND status='selected' RETURNING task_id\""
```

Expected: the four ids.

- [ ] **Step 5: Wipe the enrichment tables and the RDAP cache**

```bash
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "TRUNCATE TABLE corpscout.ip_enrichment_input"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "TRUNCATE TABLE corpscout.ip_enrichment_results"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "TRUNCATE TABLE corpscout.rdap_ip_lookup_results"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "TRUNCATE TABLE corpscout.rdap_network_segments"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "TRUNCATE TABLE corpscout.rdap_networks"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "TRUNCATE TABLE corpscout.rdap_network_registry_class"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT (SELECT count() FROM corpscout.ip_enrichment_input) AS input, (SELECT count() FROM corpscout.ip_enrichment_results) AS results, (SELECT count() FROM corpscout.ip_enrichment_current) AS current, (SELECT count() FROM corpscout.rdap_networks) AS networks, (SELECT count() FROM corpscout.rdap_network_segments) AS segments, (SELECT count() FROM corpscout.rdap_ip_lookup_results) AS markers, (SELECT count() FROM corpscout.rdap_network_registry_class) AS classes, (SELECT count() FROM corpscout.rdap_network_registry_class_derived) AS derived, (SELECT ready FROM corpscout.ip_registry_ready) AS ready, (SELECT count() FROM corpscout.ip_registry_special_segments) AS special FORMAT PrettyCompact"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT name, status, element_count FROM system.dictionaries WHERE database = '"'"'corpscout'"'"' AND name IN ('"'"'rdap_network_trie'"'"', '"'"'ip_registry_special_trie'"'"') FORMAT PrettyCompact"'
```

Expected: `0 0 0 0 0 0 0 0 1 <hundreds of thousands>`; `rdap_network_trie LOADED 0`, `ip_registry_special_trie LOADED` with its previous element count. Save the output as `scratchpad/ip/wipe-after.out`.

- [ ] **Step 6: Apply migration 000456**

Run from `corpscout/`: `make clickhouse-migrate-up-one </dev/null`
Expected: `456/u corpscout_ip_enrichment_queue_contract` (the `throwIf` gate passes on the empty table; if it names 455 or any other number, the ledger check of Step 1 was wrong — stop), then

```bash
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT engine, partition_key, sorting_key FROM system.tables WHERE database='"'"'corpscout'"'"' AND name='"'"'ip_enrichment_input'"'"'"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT max(version) FROM corpscout.schema_migrations WHERE dirty=0"'
```

→ `MergeTree	task_id	task_id, input_id` and `456`.

- [ ] **Step 7: Deploy Dagster by light_sync**

Re-check Step 1 (c) right before this step: the commit prod was deployed from must still be an ancestor of the `main` being synced (another session may have deployed in between).

Run: `cd services/dagster_v3/ansible && ANSIBLE_BECOME_TIMEOUT=60 LC_ALL=en_US.UTF-8 ansible-playbook -i inventory.ini light_sync.yml </dev/null > /private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect-corpscout/9f2d193f-045d-4f26-91d7-d2b93320d3f5/scratchpad/ip/light_sync.log 2>&1; echo rc=$?`
Expected: `rc=0`; the code location reloads. In the Dagster UI: `ip_enrichment_input_job`, `ip_enrichment_results_job` (tag `dagster/max_retries: 0`) and `geolite2_freshness_job` exist, no `ip_enrichment_workflow`, the check `geolite2_databases_fresh` is listed under `ip_enrichment_results`, `ip_registry_daily` is still RUNNING. Execute `geolite2_freshness_job` once: it fails while the 2026-07-10 files are installed (expected; it passes after the owner replaces them) and its metadata shows both build dates.

- [ ] **Step 8: Backoffice (runs locally from main: merge = deploy)**

The owner restarts the local backoffice dev server on `main` (a new route file was added). Open `/admin/queues/ip-enrichment` — the empty queue renders with "The queue is empty…" and no legacy task rows; `/admin/ip-addresses` shows **Add to enrichment queue**.

- [ ] **Step 9: Smoke batch 1 — a handful of addresses**

Launch `ip_enrichment_input_job` from the Dagster launchpad:

```yaml
ops:
  ip_enrichment_input:
    config:
      submission_id: "<new uuid>"
      source_name: smoke-2026-09
      ips: [8.8.8.8, 103.35.64.49, 185.28.20.221, 2001:4860:4860::8888, 2a02:4780::1, 127.0.0.1]
```

Note `task_id` in the run metadata. On the backoffice, `/admin/queues/ip-enrichment` selects that draft; use **Configure processing** with the defaults (`max_requests` empty). Verify:

```bash
ssh companycollect "docker exec ppoint-postgres psql -U corpscout -d corpscout -Atc \"SELECT status, total, succeeded_count, terminal_failed_count, skipped_count, inputs_purged_at IS NOT NULL, config->'execution'->>'execution_id' FROM processing.tasks WHERE task_id='<task_id>'\""
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT ip, rdap_lookup_status, rdap_rir, rdap_name, rdap_registrant_names, rdap_registration_type, rdap_self_url, rdap_matched_cidr, country_iso_code, toDate(city_db_build_epoch) FROM corpscout.ip_enrichment_current ORDER BY ip FORMAT PrettyCompact"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT s.network_key, c.registry_class FROM (SELECT DISTINCT network_key FROM corpscout.rdap_network_segments WHERE segment_role = '"'"'lookup_result'"'"') AS s LEFT JOIN corpscout.rdap_network_registry_class_current AS c USING (network_key) ORDER BY s.network_key FORMAT PrettyCompact"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT count() FROM corpscout.ip_enrichment_input WHERE task_id = '"'"'<task_id>'"'"'"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT rir, count(), sum(length(registrant_names)) AS names, groupArray(self_url) FROM corpscout.rdap_networks GROUP BY rir FORMAT PrettyCompact"'
# Source-based leak check (final review I1): RIPE/APNIC rows not written by the REST or whois path.
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT rir, count() AS rows, countIf(JSONExtractString(raw_response, '"'"'corpscout'"'"', '"'"'source'"'"') NOT IN ('"'"'ripe-rest'"'"', '"'"'apnic-whois'"'"')) AS not_rest_or_whois FROM corpscout.rdap_networks WHERE rir IN ('"'"'ripe'"'"', '"'"'apnic'"'"') GROUP BY rir FORMAT PrettyCompact"'
```

Expected: `completed | 6 | 6 | 0 | 0 | t | <execution_id>` (the results run id); `103.35.64.49` is `found` with `rdap_rir = apnic`, `rdap_name = FPT-VN`, `rdap_registrant_names = ['FPT Telecom']`, `rdap_registration_type = ALLOCATED PORTABLE`, a `rdap_self_url` under `rdap.apnic.net` and a /22 match (the whois `-r` path; not `APNIC-AP`); `185.28.20.221` and `2a02:4780::1` are `found` with `rdap_rir = ripe`, a `rdap_self_url` under `rest.db.ripe.net` and empty `rdap_registrant_names` (the REST path); `2001:4860:4860::8888` is `found`; `127.0.0.1` is `not_global`; every `lookup_result` network has a class row (`reusable`; parent networks, such as 8.8.8.8's ARIN parent, are stored without one until `ip_registry_daily` at 06:05, and an empty class means `unknown` or missing — investigate before Step 10); `0` input rows; the run's tags carry `ip_enrichment/outcome=completed` and its metadata `rdap_requests_by_registry` (`ripe` and `apnic` present), `rdap_person_entities_by_registry` (**neither a `ripe` nor an `apnic` key** — the proof that both were asked without personal data; the counter counts person (`individual`) and role (`group`) vCards alike since the final-fix pass, so a role-only RIPE RDAP answer shows up too), `rdap_fallbacks_by_registry`, `geolite2_city_build`. **Source-based leak check (stop criterion):** in the last query, `not_rest_or_whois` for `ripe` must be ≤ `rdap_fallbacks_by_registry.ripe` and for `apnic` ≤ `rdap_fallbacks_by_registry.apnic` (the sources the code writes are `ripe-rest` in `ripe_rest.py` and `apnic-whois` in `apnic_whois.py`; an RDAP body has neither; the table holds only this plan's runs since the wipe, so compare with the sum of the fallbacks of every results run so far). A larger count means RIPE or APNIC RDAP bodies were fetched outside the counted fallbacks — stop and find the path before Step 10. Then queue `103.35.64.49` once more in a new draft and process it: the results run's metadata shows `rdap_requests: 0` (served from coverage).

- [ ] **Step 10: Smoke batch 2 — one hash bucket of the inventory (throughput and the RIPE ratio)**

Launch `ip_enrichment_input_job`:

```yaml
ops:
  ip_enrichment_input:
    config:
      submission_id: "<new uuid>"
      source_name: smoke-bucket-2026-09
      source_relation: corpscout.commoncrawl_ip_addresses
      source_final: true
      observed_at_column: last_seen
      filters:
        bucket: ["0"]
      select_all: true
```

Expected: `input_count` ≈ 49.3M / 256 ≈ 190,000 (a hash bucket is a representative slice of registries and IP versions). Process it from the queue page with the defaults (`max_requests` empty, `request_delay_seconds: 1`, `parent_depth: 1`). Expected: a few thousand registry requests (≈1–2% of the addresses, more than the steady state because the cache is empty), 1–3 hours, `completion_status` `completed` or `completed_with_errors` (rate-limited or unreachable registries produce published errors; they are retried later with `retry_failed_task_id`). Record from the run metadata: `pages`, wall time, `rdap_requests_by_registry`, `rdap_person_entities_by_registry` (must have **no `ripe` and no `apnic` key**; the other registries' counts are informational), the count of `nir_fallback` log lines (APNIC answers that were an NIR's own object), `rdap_deferrals_by_registry` (expected `{}`), `registry_level_responses`, the rate-limit evidence per registry from the run itself — `pauses_by_registry` and `rdap_requests_by_registry` in the metadata and the `Registry '<rir>' is rate limiting or blocking; paused for … s` warnings in the run log (error rows carry `rdap_rir = NULL`, so ClickHouse cannot group them by registry; `SELECT rdap_error_code, count() FROM corpscout.ip_enrichment_current WHERE rdap_lookup_status = 'retryable_error' GROUP BY rdap_error_code` gives the totals only) — and compute throughput = `written / wall seconds` (expected ≥ 100 addresses/s; the terminated run did 7.3/s).

Also verify the queue page history shows the task as completed and that `SELECT count() FROM corpscout.ip_enrichment_current` equals the draft's `total`.

Re-run the **source-based leak check** of Step 9 (same query; `not_rest_or_whois` per registry ≤ the sum of `rdap_fallbacks_by_registry` of the Step 9 and Step 10 runs for that registry), then the **NIR netname check** (final review I2; the NIR-object rule `nir_of` matches netname prefixes, so a holder's own netname such as `IDNIC-<HOLDER>-ID` could be misread as an NIR's allocation and sent to RDAP):

```bash
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT rir, name, registrant_names, start_address, end_address, self_url, JSONExtractString(raw_response, '"'"'corpscout'"'"', '"'"'source'"'"') AS source FROM corpscout.rdap_networks_current WHERE match(name, '"'"'^(IDNIC|CNNIC|IRINN|TWNIC|JPNIC|KRNIC|VNNIC)-'"'"') ORDER BY fetched_at DESC LIMIT 50 FORMAT PrettyCompact"'
```

and compare the count of `nir_fallback` log lines of the Step 10 run with `rdap_fallbacks_by_registry.apnic` (the difference is RIPE-style catch-all fallbacks: `APNIC-AP`/`IANA-BLOCK` answers, logged as "a catch-all"). **Decision rule:** a row whose RDAP answer (source empty) repeats the whois netname and range and names a holder that is not the NIR itself (e.g. `IDNIC-FOO-ID` with registrant `PT Foo`) is a false positive of `nir_of` — tighten the rule (exact NIR netnames, or the first `descr` line only) and re-test before Step 11. Rows that are the NIR's own object answered by the NIR's RDAP server are the intended fallback.

**Owner decision point:** (a) if `rdap_person_entities_by_registry` has a `ripe` or an `apnic` key, or the source-based leak check exceeds the fallbacks, a no-personal-data path is not being used — stop and fix before the full run; (a2) a `nir_of` false positive from the NIR netname check — tighten the rule before Step 11; (b) `registry_daily_budgets`: leave `{}` unless a registry returned 429s, then an entry below the observed ceiling; (c) if APNIC's whois answered `%ERROR:201` (access denied / query limit) during the bucket, lower the request rate or set an `apnic` budget; (d) estimate the full run's duration from the measured request rate and page throughput (see the estimate below) and tell the owner before Step 11.

- [ ] **Step 11: The full re-run (owner go-ahead, runs on the prod Dagster host)**

Queue the whole inventory either from the backoffice (`/admin/ip-addresses` → **Select all matching addresses** → **Add to enrichment queue**, which sends `select_all: true, ip_search: "", filters: {}, excluded_ips: []`) or from the launchpad:

```yaml
ops:
  ip_enrichment_input:
    config:
      submission_id: "<new uuid>"
      source_name: backoffice:ip-addresses
      source_relation: corpscout.commoncrawl_ip_addresses
      observed_at_column: last_seen
      select_all: true
```

Expected: the import takes minutes (one `INSERT … SELECT` over 79.6M rows grouped to ≈49.3M identities with a `grace_hash` anti-join against the empty draft), `total` ≈ 49.3M (the bucket-0 addresses of Step 10 are included again; they are cache hits). Then start processing from the Dagster launchpad (the sheet cannot set `registry_daily_budgets`):

```yaml
ops:
  ip_enrichment_results:
    config:
      task_id: "<task_id>"
      batch_size: 500
      max_requests: null
      request_delay_seconds: 1
      registry_daily_budgets: {}
      ripe_rest: true
      apnic_whois: true
      parent_depth: 1
      rdap_cache_days: 30
```

**Volume estimate (from the 2026-09-25 inventory and the terminated run's sample):** IPv4 ≈ 11.35M distinct addresses; the sample gave 77 addresses per reusable network overall but registry densities differ (ARIN ≈ 212 addresses per network, RIPE ≈ 40, APNIC ≈ 45 once the /8 answers are gone), so direct IPv4 requests ≈ ARIN 5.85M/212 ≈ 28k + RIPE 3.15M/40 ≈ 79k + APNIC 1.34M/45 ≈ 30k + LACNIC 0.48M/40 ≈ 12k + AFRINIC 4k + legacy 3k ≈ **155k** (range 120k–250k). IPv6 ≈ 37.9M distinct addresses in 14,858 /32s and 71,414 /48s; about half of today's cached IPv6 answers are /48 or smaller, so **35k–70k** requests, RIPE-dominated (2a02::/16 alone holds 32.5M rows). Parents (ARIN/LACNIC only, ≈8.6% of their direct requests in the sample) ≈ 5k–15k. Total ≈ **200k–330k requests**, ≈ 1.43 s each (1 s pacing + HTTP) ≈ 80–130 h of registry time. RIPE's share ≈ 110k–140k requests goes through the REST search and APNIC's ≈ 30k through whois `-r`, both without personal data, so no daily limit binds; the loop is single-threaded, so the request time adds to the ClickHouse page work (≈49M addresses at 100–250/s ≈ 2–6 days). Expect **6–12 days** for the single run; `budget_waits` stays 0 unless a `registry_daily_budgets` entry is set. Proxies were dropped (they would not shorten it: pacing is global); a lower `request_delay_seconds` would (reasonable use; RIPE allows 3 simultaneous connections, we use one).

Monitoring while it runs (SELECT only, any time):

```bash
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT toStartOfHour(completed_at) AS h, count() FROM corpscout.ip_enrichment_results WHERE completed_at >= now() - INTERVAL 1 DAY GROUP BY h ORDER BY h FORMAT PrettyCompact"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT rir, JSONExtractString(raw_response, '"'"'corpscout'"'"', '"'"'source'"'"') AS source, count() FROM corpscout.rdap_networks WHERE fetched_at >= now64(6) - INTERVAL 1 DAY GROUP BY rir, source ORDER BY rir, source FORMAT PrettyCompact"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT registry_class, count() FROM corpscout.rdap_network_registry_class_current GROUP BY registry_class FORMAT PrettyCompact"'
```

The second query is the per-registry 24-hour usage by source: `ripe` rows come from `ripe-rest` and `apnic` rows from `apnic-whois`; rows with an empty source are RDAP bodies and must be only the counted fallbacks (`self_url` cannot tell them apart: an APNIC fallback is also under `rdap.apnic.net`). The run's metadata (`rdap_fallbacks_by_registry`) is published only when the run ends, so while it runs compare the empty-source `ripe`/`apnic` rows with the `answer for … is a catch-all; asking RDAP` / `(nir_fallback)` info lines of the last day in the run log; more RDAP rows than fallback lines — stop the run. A registry with a `registry_daily_budgets` entry must stay below it. Pause-and-resume: terminating the run in Dagster (or a host restart) leaves the task `selected`; re-running `ip_enrichment_results_job` with the same `task_id` resumes the saved execution, transport settings may change, and the budget window is seeded from ClickHouse. The job carries `dagster/max_retries: "0"`, so Dagster's run retries (`dagster.yaml`: enabled, `max_retries: 2`, which also relaunch a `dg.Failure(allow_retries=False)`) never relaunch it; an unattended resume after a crash or host restart needs a sensor (follow-up); until then the operator re-runs the task.

Post-run verification:

```bash
ssh companycollect "docker exec ppoint-postgres psql -U corpscout -d corpscout -Atc \"SELECT status, total, succeeded_count, terminal_failed_count, skipped_count, inputs_purged_at IS NOT NULL FROM processing.tasks WHERE task_id='<task_id>'\""
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT count(), countIf(rdap_lookup_status = '"'"'found'"'"'), countIf(rdap_lookup_status IN ('"'"'retryable_error'"'"', '"'"'terminal_error'"'"')), countIf(city_lookup_status = '"'"'found'"'"') FROM corpscout.ip_enrichment_current FORMAT PrettyCompact"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT count() FROM corpscout.ip_enrichment_current WHERE rdap_network_key IN (SELECT network_key FROM corpscout.rdap_network_registry_class_current WHERE registry_class != '"'"'reusable'"'"')"'
```

Expected: `completed | ≈49.3M | ≈49.3M − failed | failed | 0 | t`; the last count is **≥** the run's `registry_level_responses` (addresses whose registry returned only a registry-level block; each was looked up individually, none was served from such a block). It is not equal: `ip_enrichment_current` also holds addresses served from per-address markers stored by earlier runs (the Step 9/10 overlap, retry drafts), which the Step 11 run counts as cache hits, and `ip_registry_daily` may reclassify a network as non-reusable after the run stored it. A count **below** `registry_level_responses` means a non-reusable network lost its class row or an address was served from one through the trie — investigate. Queue the failed addresses later with `retry_failed_task_id` once their `retry_after` has passed.

- [ ] **Step 12: Mark the spec**

In `services/dagster_v3/docs/superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md` change line 3 to:

`Status: agreed 2026-09-24. Webtech implemented 2026-09-25 (plan 2026-09-24-webtech-queue-contract); crawl implemented 2026-09-25 (plan 2026-09-25-crawl-queue-contract); IP enrichment implemented <today's date> (plan 2026-09-25-ip-enrichment-queue-contract); Brave pending.`

```bash
git add services/dagster_v3/docs/superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md
git commit -m "docs(spec): IP enrichment is on the shared processing queue contract

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

## Decision coverage

| Decision | Task(s) |
| --- | --- |
| R1 no own registry-level classifier/migration; per-miss `classify_registration`, class row before segments, never reuse non-reusable, trie already excludes | 4 (resolver), 5 (storage assertion), 7 (docs) |
| R2 GeoLite2: no download/schedule/credentials; freshness check (14 days, build epoch from the .mmdb metadata), `.env.example:70` fix, manual procedure, no restart needed (verified: paths resolved and files opened per run) | 3, 5 (build dates per run), 7, 8 (Step 1, 7) |
| R3 clean re-run: cancel the 4 legacy tasks, wipe input/results (incl. legacy GeoIP rows)/RDAP cache/classes by `TRUNCATE`, keep `ip_registry_*`, effects on views/dictionary/consumers/legacy worker stated, backup check first, then the full inventory through the new execution with a smoke batch first | 8 (Steps 1–5, 9–11), 7 (docs) |
| R4 volume estimate per registry, `request_delay_seconds` kept, RIPE AUP (1,000 personal data sets/day/address): RIPE via the REST search and APNIC via whois `-r` without personal data (owner decisions 2026-09-26; NIR space falls back to RDAP), so nothing counts; optional per-registry request budget with deferral + wait; proxy lanes and bulk dumps considered and dropped; person entities measured per run | 4, 5, 8 (Steps 9–11), Risks |
| R5 everything else stays | 1–7 |
| D1 entry table layout (String task_id, required submission_id, gate, mutation-pool setting), input_id choice stated | 1 |
| D2 legacy tasks cancelled, entry table emptied in the deploy task | 8 (superseded by R3's wider wipe) |
| D3 draft queue import with receipts, queue_scope, submission-scoped retry, no manifest | 2 |
| D4 shared lifecycle, frozen profile vs transport keys, execution_id = original run, frozen cache window, live remaining, errors as outcomes, retry-failed mode, completion + purge | 4, 5 (retry mode in 2) |
| D5 per-page ClickHouse work, ResultBuffer, bounded-queries test, per-miss network writes stated | 4, 5 |
| D6 registry-level rule (now the deployed data-driven one), remediation replaced by the clean re-run | 4, 8 |
| D7 GeoLite2 (narrowed by R2) | 3 |
| D8 workflow removed, backoffice adds to the draft, queue page draft semantics, ordering, no FINAL | 5, 6 |
| D9 follow-ups listed below | — |
| D10 deploy with owner go-ahead, e2e incl. IPv6 and 103/8 | 8 |

## Risks and open questions for the owner

1. **RIPE and APNIC without personal data.** RIPE's limit is 1,000 personal data sets per day per source address (read from the published AUP on 2026-09-26; two live RIPE RDAP answers carried 3 and 8 person objects). The REST search with `no-referenced` returns none, and APNIC's whois `-r` returns contact handles only, so the full run stays within the policies with no budget at all; both smoke batches must show neither a `ripe` nor an `apnic` key in `rdap_person_entities_by_registry` — if one appears, a no-personal-data path is not in use and the run must stop. Proxies were considered and dropped: pooling a registry's allowance across addresses is the AUP's anti-avoidance case and the no-personal-data paths make it moot. The legacy bucket worker still uses RDAP for both registries and consumes personal data sets; do not run it in bulk. Residual exposure: APNIC's first `descr` line, taken as the holder name, is a natural person's name for some individual holders (the 2026-09-25 dump has e.g. `descr: John Strangio`); it is the registrant name of a network object, not a contact, and the owner accepted it.
2. **Duration.** One Dagster run of 6–12 days (≈200k–330k requests at one per second, added to the ClickHouse page work because the loop is single-threaded). A host restart or deploy stops it; the task resumes on re-run (budget window seeded from `rdap_networks`), but nobody re-runs it automatically — the job is tagged `dagster/max_retries: "0"` because Dagster's run retries would otherwise relaunch a failed or `max_requests`-stopped run twice (verified against Dagster 1.13.9 on 2026-09-26) — a resume sensor is a follow-up if the owner wants unattended recovery. Do not launch the legacy `commoncrawl_ip_rdap_networks` worker meanwhile.
3. **Data gap during the re-run.** `ip_enrichment_current` is empty at the start and fills over the run; backoffice IP pages, company IP views and `commoncrawl_ip_checks` show nothing for addresses not yet reached, and the 8.29M legacy GeoIP rows are gone for good (rollback = the B2 backup named in Step 2).
4. **GeoLite2 files are from 2026-07-10.** The check will fail until the owner replaces them by hand; replacing them before Step 11 means the 49M new rows carry current GeoIP, otherwise they carry July builds and a GeoIP-only refresh (follow-up) becomes necessary.
5. **Migration number.** Renumbered 000453 → **000456** on 2026-09-26: prod's ledger reached 454 from `codex/crawl-normalization-schema` (452–454, with an untracked 455 in the main checkout), none of it on `origin/main` (451). Step 1 requires that branch merged first, main to hold every applied file and the ledger at the highest file below 456; re-check at merge.
6. **Estimate uncertainty.** The per-registry densities come from the terminated run's first 1.08M IPv4 addresses (ordered by IP text, so APNIC/ARIN-heavy); RIPE IPv4 could need 60k–120k requests alone. Step 10's bucket is representative and refines the estimate before the owner commits to Step 11.
7. **Budget accounting.** The window is seeded from persisted networks (successful direct and parent fetches); failed requests (429, 5xx) are not seeded, so a resume right after a burst of errors undercounts by that burst. In-run accounting counts every request.
8. **6to4 and unmapped space.** Addresses outside whoisit's bootstrap prefixes (2002::/16 — 587 distinct /32s in the inventory) are never requested (`registry_for` returns `''`, since whoisit would pick a default server at random, possibly RIPE's or APNIC's RDAP). They get a **terminal** `no_registry` marker (since the final-fix pass; it was retryable), cached for `rdap_cache_days` like other terminal errors, so a `retry_failed_task_id` draft re-queues them but they are served from the marker without a request until the cache window passes.
9. **Checks under the backoffice launch.** `startQueueProcessing` launches `ip_enrichment_results_job` with `assetSelection: [asset]`; whether Dagster includes the asset's checks in that run depends on the GraphQL selection semantics, which is why every results run also reports the build dates itself and `geolite2_freshness_job` exists.
10. **REST and whois semantics.** RIPE's REST search answers unallocated or non-authoritative (RIPE-NONAUTH) space with the root object; APNIC's whois answers with `IANA-BLOCK`/`APNIC-AP` placeholders or an NIR's own object; in each case the resolver asks RDAP once, which the IANA bootstrap routes to the registry or NIR that holds the range. The RIPE answer stores `netname`, `country`, `status`, the `org` handle and dates; `descr` (which may contain a person's name) is dropped, so RIPE holders without an `org` object show only their `netname` — the on-click contact/detail path (follow-up) fetches the rest live. The APNIC path keeps NIR-managed space at the ISP allocation level where the NIR's database holds finer assignments (KRNIC, JPNIC ISP allocations are in APNIC's database and are used as they are); the review's NIR networks were ~6% of found IPv4.
11. **APNIC whois availability.** Port 43 must be reachable from the Dagster host (checked in Task 8, Step 1); APNIC's whois answers `%ERROR:201: access denied` when its query limit is hit — mapped to `rate_limited` (retryable, `rate_limit_retry_seconds`) and visible in the smoke bucket; the RDAP client's timeouts (10 s connect, 30 s read) apply.

## Follow-ups (out of scope, D9 and new)

- Bulk RIR delegation dumps as a range dictionary instead of per-network RDAP discovery.
- Honouring `Retry-After` on 429s (today a retryable outcome with `rate_limit_retry_seconds`).
- Proxy egress lanes with per-lane budgets if a registry's per-address rate limit ever binds (designed and dropped on 2026-09-26).
- Contact details on demand: one live RDAP call when a user opens a network in the backoffice (the batch path stores no personal data).
- No-contact paths for ARIN, LACNIC and AFRINIC (whois `-r` where their servers honour it) if their RDAP contact entities ever become a limit; LACNIC and AFRINIC whois `-r` were not verified.
- Parent lookups served from the cache, and `parent_depth` default 0.
- Owner-name fallback from `remarks` for APNIC-family and AFRINIC registrations.
- ARIN broad-allocation children (children of a `DIRECT ALLOCATION` block).
- whoisit's `User-Agent` override (`client.py:49` is never sent).
- Storing the HTTP status code and registry on `query_error` rows.
- GeoIP-only refresh of result rows after a GeoLite2 update (needed if the files are replaced after Step 11).
- A resume sensor for `ip_enrichment_results_job` (re-run a `selected` task whose last run stopped without completing), so a host restart does not need an operator.
- Backoffice controls for `retry_failed_task_id` and `registry_daily_budgets` (the Dagster config exists; the UI does not expose them).
- The legacy `commoncrawl_ip_rdap_networks` bucket worker keeps per-IP lookups; porting it to the page-batched resolver (or retiring it in favour of drafts) once it is next touched.
