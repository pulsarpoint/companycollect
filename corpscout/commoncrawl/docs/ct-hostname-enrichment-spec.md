# CT-Hostname Enrichment for cc-dns-worker — Design Spec

Status: **spec only, not implemented.** Decisions recorded 2026-07-09.

Augment each scanned domain's hostname list with subdomains discovered from Certificate
Transparency logs, so `cc-dns-worker` resolves A/AAAA for the domain's *real* public
hostnames (`cpanel.`, `vpn.`, `api.`, `portal.` → PaaS CNAME, `asa-fw.`, …) instead of
only the 5 static guesses. IP data for those hostnames is the payoff — it feeds the
downstream GeoIP / provider-map / technology-inference work.

## Why / what changes

Today `records.DefaultConfig().Hostnames = {www, mail, webmail, smtp, autodiscover}` — a
fixed guess list, brute-forced for every domain. CT logs already record the hostnames a
domain actually issued certificates for. Taking the **union** of the static list and the
CT-discovered hostnames turns a blind guess into targeted enumeration.

**Measured (50k-domain sample):** 52% of scan domains have ≥1 CT hostname; of those,
median 1, p90 5, p99 34, mean 2.86 after filtering (capped at 100). Corpus-wide that is
≈ +3 A/AAAA query-pairs per domain on average — negligible — with the cap bounding the
fat tail (see [Cost](#cost)).

## Source

`ctlogs.hostnames` (ClickHouse on the same server as the corpscout tables) — CT-derived,
**1.9 B rows**, `AggregatingMergeTree`, sort key `(registered_domain, fqdn)`.

| Column | Type | Use |
| --- | --- | --- |
| `registered_domain` | `String` | join key = `root_domain` (eTLD+1) |
| `fqdn` | `String` | the discovered hostname |
| `is_wildcard` | `SimpleAggregateFunction(max, UInt8)` | drop `1` (`*.` certs) |
| `last_seen` | `SimpleAggregateFunction(max, DateTime64(3))` | recency ranking |
| `last_not_after` | `SimpleAggregateFunction(max, DateTime)` | cert expiry → "live" test |
| `source_logs` | `SimpleAggregateFunction(groupUniqArrayArray, Array(String))` | (unused) |

**Join-key alignment** (verified on a 50k sample, 52% match): both `root_domain`
(`commoncrawl_domains` → SQLite `scan_domains`) and `registered_domain` (`ctlogs`) are the
registered/registrable domain. The implementation's first step re-confirms the hit-rate
on the live corpus so a public-suffix mismatch (e.g. `bbc.co.uk` vs `co.uk`) can't
silently zero out matches.

## Selection & ranking (per domain, cap 100)

Rank **live-cert hostnames first, then backfill with expired-cert hostnames**, both by
recency, capped at 100. Rationale: a company that moves to a wildcard cert strands
perfectly valid, functional hostnames whose individual certs have lapsed — excluding
them by a hard live-cert filter would drop real hosts. So a domain with 30 live + 200
expired yields its 30 live plus the 70 most-recent expired; a domain with 150 live yields
100 live.

Wildcards (`is_wildcard=1`) and the apex (`fqdn == registered_domain`) are always dropped.

The ClickHouse query, run per batch (see below), is:

```sql
SELECT registered_domain, fqdn, (lna >= now()) AS live FROM (
  SELECT registered_domain, fqdn,
         max(is_wildcard) is_wc, max(last_seen) ls, max(last_not_after) lna
  FROM ctlogs.hostnames
  WHERE registered_domain IN (…batch…)
  GROUP BY registered_domain, fqdn          -- merge AggregatingMergeTree parts (SimpleAggregateFunction cols)
)
WHERE is_wc = 0 AND fqdn != registered_domain
ORDER BY registered_domain, (lna >= now()) DESC, ls DESC   -- live first, then recency
LIMIT 100 BY registered_domain
```

`LIMIT 100 BY registered_domain` is a ClickHouse extension (not SQLite). The `IN (…batch…)`
list uses the sort-key prefix `registered_domain`, so each query is index-pruned to the
batch's granules — it never scans the whole 1.9 B-row table.

**Normalization → label.** Each `fqdn` becomes a scan label by stripping the
`.<registered_domain>` suffix and lowercasing: `mail.example.com` → `mail`,
`api.eu.example.com` → `api.eu`. Rows equal to the apex (empty label) are skipped (already
filtered). The label is what the scan plan prepends to the domain, exactly like the static
hostnames.

## Cross-database flow

Two databases; the Go worker is the glue (mirroring the existing seed:
`input.StreamClickHouse` reads ClickHouse, `store.Seed` writes SQLite):

| Step | DB | Operation |
| --- | --- | --- |
| Read a batch of 5000 `root_domain`s | **SQLite** `scan_domains` | `SELECT root_domain … WHERE root_domain > ? ORDER BY root_domain LIMIT 5000` (cursor) |
| Fetch capped CT hostnames for those 5000 | **ClickHouse** `ctlogs.hostnames` | the query above, with the 5000 domains as `IN (…)` |
| Store labels | **SQLite** `scan_hostnames` | `INSERT OR IGNORE (root_domain, label, live_cert)` |

The "batch" is literally 5000 registered-domain strings from the scan queue. At scan time
nothing touches ClickHouse — labels are read purely from SQLite.

## 1. Seed-load phase — new, runs after domain seed, before scanning

`scanCycle` today: seed domains → build schedulers → dispatch. Insert a **CT-load phase**
between seed and dispatch, gated by `--ct-hostnames` and skipped on resume via a marker:

1. If `--ct-hostnames` off, or `ct_load_complete` already set for this scan-id → skip.
2. Else iterate seeded `root_domain`s in cursor batches of `--ct-batch` (default 5000):
   for each batch, open a ClickHouse conn (like `incLoad` does), run the selection query,
   normalize each `fqdn` → label, `INSERT OR IGNORE` into `scan_hostnames`, then advance a
   persisted cursor (`ct_load_state`, mirroring `load_state`).
3. When a batch returns zero domains → `MarkCTLoadComplete(scanID)`.

Resumable and idempotent: a crash resumes from the cursor; re-inserts are ignored by the
`(scan_id, root_domain, label)` primary key. ~6,700 index-pruned CH queries for the full
33.6 M corpus — a longer seed, explicitly accepted (the IP data is the goal).

## 2. SQLite storage — `scan_hostnames` (new table)

```sql
CREATE TABLE IF NOT EXISTS scan_hostnames (
  scan_id     TEXT NOT NULL,
  root_domain TEXT NOT NULL,
  label       TEXT NOT NULL,   -- fqdn minus ".<root_domain>", lowercased
  live_cert   INTEGER NOT NULL DEFAULT 0,  -- 1 = ranked from a still-valid cert
  PRIMARY KEY (scan_id, root_domain, label)
);
```

Added to `store.go`'s `schema` const and the additive `migrate()` list (idempotent
`ALTER`/`CREATE IF NOT EXISTS`). `live_cert` is a downstream confidence signal (a host
found via a live cert outranks one from an expired cert); it costs nothing at scan time.

New `store` methods: `SeededDomainsAfter(scanID, cursor, limit)` (batch-iterate the queue
for the CT-load phase), `InsertHostnames(scanID, rows)`, `HostnamesForBatch(scanID, domains)
→ map[domain][]label` (bulk-load labels for a dispatch batch — NOT per-worker, to avoid
SQLite contention), plus `CTLoadComplete`/`MarkCTLoadComplete` and a `ct_load_state`
cursor.

## 3. Scan-time union

- The **feeder** (in `scanCycle`) already pulls dispatch batches of 20000 via
  `PendingBatch`. After each, it calls `HostnamesForBatch` once and sends a
  `domainWork{domain string, labels []string}` (the work channel changes from
  `chan string` to `chan domainWork`) — one bulk SQLite read per 20000 domains, not 20000
  reads.
- `resolveDomain(…, work.labels)` passes the labels into
  `records.Plan(domain, cfg, extraLabels)`, which **unions** `cfg.Hostnames` with
  `extraLabels`, deduped case-insensitively, and emits **A + AAAA** for each (parity with
  the static hostnames; worst case +200 queries, accepted). A CT label that duplicates a
  static one (`www`) collapses to one pair.
- CT-derived records carry `source="query"` (they are actively resolved, not from a zone
  transfer). Their slot is the label. `scan_hostnames` is the record of CT provenance;
  if CT-origin is later wanted on the ClickHouse records themselves, that is a small
  additive change (a distinct slot prefix or a new source value), out of scope here.

## 4. Flags (default-off, ships dark)

```
--ct-hostnames   bool  (default false — master switch)
--ct-cap         int   (default 100 — max CT hostnames per domain)
--ct-batch       int   (default 5000 — domains per ClickHouse CT lookup)
```

Enabled by flipping `--ct-hostnames` on for the run. Add matching fields to `scanConfig`
and `scanFlags` in `scan.go`.

## 5. Rollout

1. Re-confirm the live-corpus hit-rate (the join-key alignment check) as the first
   implementation step.
2. Land the CT-load phase + `scan_hostnames` + the union behind `--ct-hostnames=false`,
   with tests: label normalization (fqdn→label, apex/deep cases), the union+dedup in
   `Plan`, and the store round-trip. Use a fake ClickHouse result / a small fixture rather
   than the 1.9 B-row table in unit tests.
3. Run a bounded `--limit` sample with `--ct-hostnames` on; read real added-hostname counts
   and seed-phase duration from stats before enabling corpus-wide.

## Cost

- **Seed:** +~6,700 index-pruned ClickHouse queries for the full corpus; a longer,
  resumable seed phase. No CH spike (batched, sort-key-pruned).
- **Scan:** +~3 A/AAAA query-pairs per domain on average (52% of domains matched, mean
  2.86 labels each), bounded to +200 queries for the fattest ~0.1% by the `--ct-cap`.
- **SQLite:** `scan_hostnames` holds ~one row per (domain, CT label) — order 10^8 rows at
  full corpus; indexed on the PK, bulk-read per dispatch batch.

## Interaction with AXFR

Complementary. CT is the public, low-risk subdomain workhorse (this spec); the AXFR probe
([axfr-zone-transfer-spec.md](axfr-zone-transfer-spec.md)) adds *internal-only* hostnames
for the minority of domains with open zone transfers. Both feed A/AAAA resolution into the
same record table; downstream inference consumes the union.
