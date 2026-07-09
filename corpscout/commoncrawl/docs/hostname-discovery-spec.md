# Durable Hostname Discovery, Provenance & CT Enrichment — Design Spec

Status: **spec only, not implemented.** Decisions recorded 2026-07-09. Supersedes the
earlier CT-only draft (this file was renamed from `ct-hostname-enrichment-spec.md`).

Give `cc-dns-worker` a **durable, monotonic per-domain hostname set** so every hostname we
ever discover — from Certificate Transparency, from an AXFR zone transfer, or from the
static guess list — keeps getting re-resolved every cycle, and record **how each hostname
was discovered**. Resolving IPs for those hostnames is the payoff (GeoIP / provider-map /
technology-inference downstream).

## The three gaps this closes

1. **Discovery isn't durable.** An AXFR-internal host (`jenkins.corp.com`, `vpn-gw.corp.com`)
   discovered this cycle is scanned once, then lost: it isn't in CT (no public cert) and the
   zone may close next cycle, so nothing re-finds it. It silently stops being scanned forever.
2. **No hostname-discovery provenance.** The records table can't say a hostname came from CT
   vs AXFR vs the static list. (The `source` column the AXFR work added is *how the record
   was obtained* — `query`|`axfr` — a different axis, and it isn't even in live ClickHouse yet.)
3. **The AXFR ClickHouse schema was never migrated.** The AXFR feature's `source` column and
   `axfr_*` summary columns live only in a README runbook — there is no migration file, and
   live `commoncrawl_domain_dns_records` / `commoncrawl_domain_dns_scan` don't have them, so
   the merged AXFR code can't even load. There's also no record of *which* NS IP allowed a
   transfer.

## 1. Durable hostname registry — new ClickHouse table

`commoncrawl_domain_hostnames` — the persistent, accumulating set of every hostname ever
discovered for a domain. This is the mechanism that makes discovery survive AXFR closing.

| Column | Type | Meaning |
| --- | --- | --- |
| `root_domain` | `String` | registered domain (join key) |
| `label` | `String` | hostname minus `.<root_domain>`, lowercased (`jenkins`, `api.eu`) |
| `discovery_source` | `LowCardinality(String)` | how **first** discovered: `ct` \| `axfr` \| `static` |
| `first_seen` | `DateTime64(3,'UTC')` | when we first discovered this label |
| `last_seen` | `DateTime64(3,'UTC')` | most recent cycle that re-discovered it (CT re-issue, AXFR, etc.) |
| `last_resolved` | `DateTime64(3,'UTC')` | most recent cycle its A/AAAA actually resolved (`0` if never) — powers a future decay policy |

Engine: `ReplacingMergeTree(last_seen)` keyed `ORDER BY (root_domain, label)` — re-discovery
updates the row; `discovery_source`/`first_seen` are preserved on the winning (earliest)
row via the write-back's `argMin` (see §4). One row per `(root_domain, label)`.

**Monotonic + capped, never decayed (this spec):** a host is never deleted. The registry's
*contribution to a scan* is capped at 100/domain by `last_seen` (see §3), which bounds query
volume; `last_resolved` is recorded so a later decay policy can deprioritize long-dead hosts
without losing the discovery record.

## 2. Two provenance axes on `commoncrawl_domain_dns_records`

Keep both — they answer different questions:

- **`source`** (`query` \| `axfr`) — how the *record* was obtained: actively queried vs pulled
  from a zone transfer. (Added by the AXFR work; migrated here in §5.)
- **`discovery`** (`static` \| `ct` \| `axfr`) *(new)* — how the *hostname* was found.
  A CT-found host resolved by a normal query → `source=query, discovery=ct`. A name straight
  out of a transfer → `source=axfr, discovery=axfr`. A host re-scanned from the registry
  carries the registry's original `discovery_source` (so an AXFR-first host keeps
  `discovery=axfr` on every future cycle, even after the zone closes) — there is no separate
  "known" value; provenance is always the original discovery channel.

## 3. Seed-load — union of all three sources, single per-domain cap

The per-cycle scan hostname set for a domain is:

```
static (5 labels, always)  ∪  top-100 of ( CT-current  ∪  registry )  ranked by liveness+recency
```

One cap (100) on the **union**, not per source — so overlapping CT/registry hosts don't
double the budget. Ranking: live-cert / recently-resolved first, then `last_seen` desc
(the CT live-first rule from the earlier draft, generalized across both sources).

Runs as a new **host-load phase** in `scanCycle`, after the domain seed and before dispatch,
gated by `--host-enrich` and skipped on resume via a `host_load_complete` marker:

1. Iterate seeded `root_domain`s in cursor batches of `--host-batch` (default 5000).
2. Per batch, query **both** ClickHouse sources scoped to the batch's domains:
   - `ctlogs.hostnames` — non-wildcard, non-apex, live-cert-first, `LIMIT 100 BY registered_domain`
     (the query from the earlier draft; every `fqdn` normalized to a label).
   - `corpscout.commoncrawl_domain_hostnames` — this domain's registry rows, `LIMIT 100 BY root_domain`
     by `last_seen` desc.
3. Merge the two per label (dedupe; `discovery_source` precedence `axfr > ct > static`; keep
   `live_cert` from CT), re-cap to `--host-cap` by liveness+recency, `INSERT OR IGNORE` into
   SQLite `scan_hostnames`, advance a persisted cursor (`host_load_state`).
4. Zero-domain batch → `MarkHostLoadComplete(scanID)`.

Resumable/idempotent, index-pruned on both CH tables' sort keys. The join-key alignment
(`root_domain` = `ctlogs.registered_domain` = `registry.root_domain`, all eTLD+1; 52% CT
hit-rate measured on a 50k sample) is re-confirmed as implementation step 1.

### CT selection query (per batch)

```sql
SELECT registered_domain, fqdn, (lna >= now()) AS live FROM (
  SELECT registered_domain, fqdn,
         max(is_wildcard) is_wc, max(last_seen) ls, max(last_not_after) lna
  FROM ctlogs.hostnames
  WHERE registered_domain IN (…5000 root_domains…)
  GROUP BY registered_domain, fqdn          -- merge AggregatingMergeTree SimpleAggregateFunction parts
)
WHERE is_wc = 0 AND fqdn != registered_domain
ORDER BY registered_domain, (lna >= now()) DESC, ls DESC   -- live-cert first, then recency
LIMIT 100 BY registered_domain
```

### SQLite `scan_hostnames` (per-cycle working set)

```sql
CREATE TABLE IF NOT EXISTS scan_hostnames (
  scan_id          TEXT NOT NULL,
  root_domain      TEXT NOT NULL,
  label            TEXT NOT NULL,
  discovery_source TEXT NOT NULL DEFAULT 'ct',   -- ct | axfr | static (winning source)
  live_cert        INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (scan_id, root_domain, label)
);
```

## 4. Scan-time union + write-back

- **Union into the plan.** The feeder bulk-loads each dispatch batch's labels via
  `HostnamesForBatch(scanID, domains) → map[domain][]labelInfo` (one SQLite read per 20000
  domains, not per worker), and sends `domainWork{domain, hosts []labelInfo}`. `resolveDomain`
  passes them to `records.Plan(domain, cfg, extraHosts)`, which unions with the static list
  (deduped, case-insensitive) and emits **A+AAAA** per host. Each resulting record carries
  `discovery` = the host's `discovery_source` (`static` for the built-in 5).
- **AXFR records** keep `source=axfr` and get `discovery=axfr`.
- **Write-back (after the cycle, in the load phase).** Upsert every hostname discovered this
  cycle into `commoncrawl_domain_hostnames`:
  - CT/registry-sourced labels that were scanned → refresh `last_seen`; set `last_resolved` if
    they resolved.
  - **AXFR-discovered hostnames** (distinct labels from this cycle's `source=axfr` records) →
    insert with `discovery_source=axfr`. **This is what makes them durable** — next cycle they
    come back from the registry and are re-resolved by normal query even if the zone closed.
  - `first_seen`/`discovery_source` preserved via `argMin(…, first_seen)` on merge.

## 5. Migrations (`000107+`, replacing the README DDL)

Proper golang-migrate files under `corpscout/clickhouse/migrations/` (next-free numbers at
implementation time; the ledger notes a prior `000101` collision from concurrent work, so
renumber to whatever's free), each with a matching `.down.sql`:

1. **records `source`** — `ADD COLUMN source SimpleAggregateFunction(anyLast, LowCardinality(String))`.
   NOT in the sort key: ClickHouse forbids a defaulted column in `ORDER BY` (and a new key column on
   a table with data must be defaulted), so `source` is a non-key aggregate like `ttl`/`rcode`/
   `last_run_id` — the `AggregatingMergeTree` merges it deterministically via `anyLast`. (An earlier
   attempt used `ADD COLUMN … DEFAULT 'query'` + `MODIFY ORDER BY … source`, which ClickHouse rejects
   with error 36; verified.) *Retrofits the already-merged AXFR code, which cannot load without it.*
   Also update `EXPECTED_MIGRATIONS` in `dagster_v3/tests/test_clickhouse_migrations.py`.
2. **records `discovery`** — `ADD COLUMN discovery SimpleAggregateFunction(anyLast, LowCardinality(String))`
   (same constraint as `source` — a non-key aggregate column, NOT added to the sort key).
3. **scan summary AXFR columns** — `ADD COLUMN axfr_open UInt8, axfr_records UInt32,
   axfr_truncated UInt8, axfr_server String` (all `DEFAULT`), the last being the NS IP that
   answered the transfer (`''` when closed). *Retrofits AXFR + adds the answering-IP info.*
4. **`commoncrawl_domain_hostnames`** — the registry table (§1).

The README "Schema migrations" runbook is replaced by a pointer to these files.

`ScanRow`/`DomainResult` gain `AXFRServer`; `RecordRow`/`DNSRecord` gain `Discovery`; the
SQLite stage + `store` read/write paths extend accordingly (mirrors the `source` plumbing the
AXFR work already established).

## 6. Flags (default-off, ships dark)

```
--host-enrich    bool  (default false — master switch for CT + registry hostname enrichment)
--host-cap       int   (default 100 — max discovered hosts per domain in a scan)
--host-batch     int   (default 5000 — domains per ClickHouse lookup)
```

`--axfr` (existing) still gates the transfer probe; when both are on, AXFR feeds the registry
via §4 write-back. Add matching `scanConfig`/`scanFlags` fields.

## 7. Cost

- **Seed:** a host-load phase — ~6,700 index-pruned CH queries/corpus against two tables, plus
  the per-cycle write-back. Longer, resumable seed (accepted — IP data is the goal).
- **Scan:** +~3 A/AAAA query-pairs per domain average (52% CT hit-rate, mean ~2.9 hosts), the
  registry adding durable AXFR/past-CT hosts on top, all bounded to +200 queries/domain by
  `--host-cap 100`.
- **ClickHouse:** one new table (`commoncrawl_domain_hostnames`, ~10^8 rows at full corpus,
  one per discovered (domain,label)) + two additive columns on records + four on the scan summary.

## 8. Rollout (phased)

- **Phase 0 — unblock AXFR:** migrations 1 & 3 (records `source`+re-key, scan `axfr_*`+`axfr_server`)
  so the already-merged AXFR code can load; wire `axfr_server` through the store. Independent,
  land first.
- **Phase 1 — provenance + registry:** migrations 2 & 4 (`discovery`, registry table); the
  `discovery` plumbing; the registry write-back.
- **Phase 2 — enrichment:** the seed-time host-load phase, `scan_hostnames`, the `Plan` union,
  behind `--host-enrich=false`. Tests: label normalization (fqdn→label, apex/deep), union+dedup+cap
  in `Plan`, source-precedence merge, store round-trip — against fixtures, not the 1.9 B-row table.
  Then a bounded `--limit` sample with `--host-enrich` on to read real added-host counts + seed
  duration before corpus-wide enablement.

## Interaction with AXFR

Symbiotic: AXFR discovers internal-only hosts → written to the registry → re-scanned durably
by normal query even after the zone closes; CT is the public workhorse for the other 52%. Both
land in the same records table, now tagged with both `source` (how obtained) and `discovery`
(how found).
