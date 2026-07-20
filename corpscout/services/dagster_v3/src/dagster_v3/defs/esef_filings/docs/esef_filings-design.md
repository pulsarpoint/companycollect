# ESEF filings design doc

> Per `docs/source-design-doc-template.md` / `docs/data-source-guidelines.md`. As-built
> (2026-07-20). Supersedes `docs/esef-filings-research.md`, which now points here.

## 1. Source overview

- **Source / scope**: filings.xbrl.org (XBRL International's public repository of ESEF
  filings) — **cross-country**, not a single national register. Closes the "listed
  company" blind spot every per-country source shares (0 of 298 Finnish listed companies
  have financials in `fi_financial_statements`; the same gap exists everywhere, since
  listed issuers file ESEF to national OAMs, not to company registers).
- **Module**: `defs/esef_filings/` · DuckDB `data/esef_filings_source.duckdb` (dataset
  `esef_filings`) · pool `esef_filings_duckdb`.
- **ClickHouse tables** (migration `000149_corpscout_esef_filings`, **UNAPPLIED as of this
  writing — ledger at 148**; amended in place during Task 6, not a fresh migration):
  `corpscout.esef_filings`, `corpscout.esef_facts`, `corpscout.esef_financial_metrics`,
  `corpscout.esef_entity_registry_map`.
- **Datasets used**:

  | dataset | url | format | size | cadence | auth? |
  |---|---|---|---|---|---|
  | filing index | `https://filings.xbrl.org/api/filings` | JSON:API, paginated (`page[size]=200`) | `meta.count` = 25,061 filings (2026-07-19 snapshot; FI = 1,168) | rolling (new filings added continuously) | none |
  | per-filing facts | `json_url` (per filing, from the index) | OIM xBRL-JSON | varies/filing | fetched once, S3-cached | none |
  | per-filing rendered report | `report_url` (per filing) | XHTML | ~2-8 MB/filing | fetched once, S3-cached | none |

- **Entity key**: **LEI** (`lei`) everywhere in `esef_*` tables — national registry ids
  appear only in the match layer (`esef_entity_registry_map`), never as an ingestion
  filter. **`fxo_id`** is the filing-*version* key (see §9.3) — every table's `ORDER BY`
  includes it for exactly that reason.

## 2. Ingest mode — hybrid: full-sweep index + year-partitioned incremental downloads

- **Index** (`esef_filings_index_duckdb`): non-partitioned, full-replace every run. One
  paginated sweep (~125 pages @ 200/page) covers the entire ~25k-filing index in one run
  — there is no per-window bookkeeping benefit to partitioning it (§2's "why partition"
  test fails: the whole dataset already comes back cheaply). This is a deliberate
  deviation from the original research doc's monthly-partition sketch.
- **Facts + report-XHTML archive** (`esef_filing_facts_duckdb`, `esef_report_xhtml_s3`):
  year-partitioned (`StaticPartitionsDefinition` `str(y)` for `y` in `2019..2027`, keyed
  by `toYear(period_end)`), `BackfillPolicy.multi_run(max_partitions_per_run=1)`. This
  is a deliberate deviation from §4 ("API-only sources → partition by the API's natural
  window"): the partition key is **not** an API paging parameter — it's derived locally
  by grouping the *already-crawled* index rows by year (`_split_filings_by_partition_year`
  / `_split_report_filings_by_partition_year`). The expensive, partition-worthy part is
  the **per-filing download** (json_url / report_url), not the index crawl itself; year
  buckets give backfill a throttleable unit of work and give the weekly refresh a natural
  "current year only" selection, without needing the API to support date-windowed paging.
- Both partitioned assets independently re-read the full local index and re-derive their
  own year bucket on every run (cheap — one pass over already-fetched rows); neither
  depends on the other's output.

## 3. Loading

- **Client**: `EsefFilingsClient` (`client.py`) — `dlt.sources.helpers.requests` session
  (retry/backoff on connection errors and 429/5xx). `iter_filings()` does the whole
  JSON:API trick: entities arrive once per page in `included` (deduplicated by the API),
  each filing references its entity via `relationships.entity.data.id` — joined through a
  page-local `{entity_id: attributes}` map, **never by array position**.
  `download_json_facts(json_url, target)` streams to a local path with a whole-download
  retry loop (mirrors `latvia_ur/resources.py:_download_to_path`: truncates `target`
  before every attempt, verifies `Content-Length` when present, retries
  `ChunkedEncodingError`/`ConnectionError`/`Timeout`). This same method is reused, URL and
  content agnostic, for the report-XHTML archive download in `esef_report_xhtml_s3`.
- **Staging shape**: `esef_filings.filings_index` (one row per `EsefFilingRecord`, full
  replace every run) and `esef_filings.facts` (one row per parsed OIM fact,
  `period_end_year`-scoped delete+insert per partition). Both stage loosely-typed text
  columns (see `assets.py`'s `_FILINGS_INDEX_COLUMN_TYPES` / `_FACTS_COLUMN_TYPES`
  docstrings) — real typing/casting happens at ClickHouse export time (§5).
- No dlt row-resource, no CSV, no DuckDB bulk CSV reader: the source is JSON, not a bulk
  file, so none of `docs/data-source-guidelines.md` §3's CSV-reading guidance applies.
  `amount_original` is staged as **text**, not a DuckDB `DECIMAL` column — the `Decimal`
  value is already validated in Python at parse time (`facts.py`'s
  `EsefFact.amount_original`), and staging as text sidesteps DuckDB `DECIMAL` precision
  pitfalls; the ClickHouse export casts to `Decimal128(2)` explicitly.

## 4. Transform

- No SQL pivot/transform stage in DuckDB. The "transform" is Python parsing of OIM
  xBRL-JSON straight into a flat EAV-shaped fact table (`facts.py:parse_oim_facts`) — this
  is Level 1 of the XBRL ingest hierarchy (§5b): the source already publishes
  Arelle-extracted facts as JSON, so **no XBRL parser is needed in v1 at all** (unlike
  `sweden_financial`, which parses raw inline-XBRL XHTML itself).
- The only real "transform" logic lives in ClickHouse: `esef_financial_metrics` is
  derived entirely from `esef_facts` + `esef_filings` + `exchange_rates` via one
  CTE-chained `SELECT` (`metrics.py`) — see §9 for the selection rules.

## 5. ClickHouse schema — deviations

- **Grain**: `esef_filings` and `esef_financial_metrics` are 1 row per filing *version*
  (`fxo_id`); `esef_facts` is 1 row per fact; `esef_entity_registry_map` is 1 row per LEI.
- **`ORDER BY`**: `esef_filings`/`esef_facts`/`esef_financial_metrics` all key on
  `(lei, period_end, fxo_id[, fact_id])` — `fxo_id` is in the key, not just
  `lei + period_end`, because one `(lei, period_end)` can carry **multiple filing
  versions** (§9.3). `esef_entity_registry_map` keys on
  `(country_iso2, registry_id, lei)`.
- **`period_duration_end Nullable(Date32)`** on `esef_facts` — added to the migration
  *in place* during Task 6 (not part of the original Task 1 DDL), the module's single
  most important correctness decision. See §9.1.
- **Sentinel dates**: `esef_filings.period_end`/`date_added` and `esef_facts.period_end`
  are declared **non-nullable** `Date32` (all three sit in an `ORDER BY`, so they can
  never become `Nullable`). A missing/malformed source date string sentinels to
  `DATE '1970-01-01'` at export time rather than reaching `clickhouse_driver` as Python
  `None` (which crashes the Date32 writer). See §9.2.
- Non-nullable `String` columns (`json_url`, `package_url`, `report_url`, `viewer_url`,
  `package_sha256`) coalesce `NULL → ''` at export time (CLAUDE.md house rule —
  `clickhouse_driver` calls `.encode()` per value and dies on `None`).
- **No `raw_*` / `source_payload_hash` columns at all** — unlike sources that added and
  later dropped them (`latvia_ur`), this module never staged full raw JSON payloads or a
  per-row hash in the first place: `facts.py` parses and validates every field
  (`amount_original` as `Decimal`) before it ever reaches a DuckDB column, so there was
  nothing bulky/incompressible to strip later.
- **Export subset**: `tables.py`'s `*_EXPORT_COLUMNS` tuples, contract-tested against the
  migration file's literal column order
  (`tests/test_esef_filings_client.py::test_export_columns_match_migration_000149_column_order`).

## 6. Translation — N/A

`entity_name`, `country`, `concept_qname` and friends are proper nouns / XBRL taxonomy
identifiers, not translatable free text — and this source carries no legal-form/status
field at all (it's a financial-report index, not a company register). No translation
loader is wired for this module.

## 6b. Contacts (§8b) — N/A

filings.xbrl.org carries no contact fields (website/email/phone) for filers — this is a
scope boundary inherent to the source shape (a report index, not a register), not a gap
to close later. No contact-extraction asset exists for this module.

## 7. Currency

- Native currency is preserved end-to-end: every fact carries `amount_original` +
  `currency` (raw OIM `iso4217:<CCY>` unit), and `esef_facts` is never converted — only
  the derived `esef_financial_metrics` layer carries `_usd` columns.
- Unlike most sources, USD conversion is **not** a separate asset/pass over a DuckDB
  table — it's inlined directly into `metrics.py`'s single ClickHouse `SELECT`, because
  the metrics table has no DuckDB staging step to attach a second pass to (it's built
  entirely from already-exported ClickHouse tables). The *principle* (USD conversion is
  logically separate from native-currency ingestion) still holds; only the *mechanism*
  differs from the norm.
- Triangulates through `corpscout.exchange_rates` (EUR-based), generalizing
  `sweden_financial/metrics.py`'s nearest-date `argMaxIf`/`argMinIf` fallback from a fixed
  SEK/USD pair to an arbitrary filing currency: resolve the nearest EUR→USD rate and
  (unless the filing's currency IS EUR) the nearest EUR→`<currency>` rate around
  `period_end`, then `fx_rate_to_usd = eur_usd_rate / eur_ccy_rate`. EUR filings skip the
  second leg entirely (ratio reduces to `eur_usd_rate`); USD filings resolve both legs to
  the same EUR→USD lookup (ratio 1.0) with no special case. Facts span
  EUR/SEK/NOK/GBP/CHF/DKK/... per filer.

## 8. Jobs & schedule (Task 7)

Two jobs, one schedule:

| job | selection | partitioned? |
|---|---|---|
| `esef_filings_refresh_job` | `esef_filings_index_duckdb`, `esef_filing_facts_duckdb`, `esef_report_xhtml_s3`, `esef_filings_clickhouse`, `esef_facts_clickhouse`, `esef_entity_registry_map_clickhouse`, `esef_financial_metrics_clickhouse` | yes — resolves to `ESEF_FILING_FACTS_PARTITIONS` (shared by the 2 partitioned assets in the selection); the other 5 are unpartitioned and run every launch |
| `esef_filings_backfill_job` | `esef_filing_facts_duckdb`, `esef_report_xhtml_s3` only | yes — same partitions_def, exports excluded |

`esef_filings_refresh_weekly`: cron `10 5 * * 0` (moved from the originally-drafted
`50 5 * * 0`, which collided with `finland_verotax_schedule`'s `50 5 12 11 *` on
`(minute, hour)` — every schedule in `defs/` must claim a unique `(minute, hour)` pair,
enforced by `tests/test_schedule_cron_contracts.py`), `Europe/Belgrade`,
`DefaultScheduleStatus.STOPPED` (until Task 8 validates a live run and flips it on in the
UI). Both jobs carry `HEAVY_BULK_RUN_TAGS` (`corpscout/workload=heavy-bulk`) — like
`sweden_financial`'s backfill/weekly jobs, this pipeline downloads many per-filing
documents (facts JSON + report XHTML) per run.

**Partition ceiling is a silent dead-man's switch, not a hard stop.** The schedule's
`execution_fn` resolves `partition_key=str(now.year)` and only ever produces a
`RunRequest` for a year inside `[ESEF_FACTS_PARTITION_YEAR_MIN, ESEF_FACTS_PARTITION_YEAR_MAX]`
(currently 2019-2027, `assets.py`). From **2028** onward, every weekly tick resolves a
`partition_key` with no matching partition and `_esef_filings_refresh_run_request` returns
a `SkipReason` instead of a `RunRequest` — the schedule keeps firing on cron, keeps
"succeeding" (a skip is not a failure), and simply stops refreshing ESEF data, with no
alert. Bump `ESEF_FACTS_PARTITION_YEAR_MAX` (and the `ESEF_FILING_FACTS_PARTITIONS`
static list it drives) before 2028 to avoid this; see the code comment at its
declaration in `assets.py`.

**Why ONE job for the whole refresh chain, unlike `sweden_financial`'s split.**
`sweden_financial`'s weekly chain needed a 2026-07-20 de-partitioning redesign because it
keeps a **separate DuckDB file per year**: a partition-scoped incremental ClickHouse
export became order-dependent against the yearly backfill (the 2026-07-18 incident — see
`sweden_financial/docs/sweden_financial-design.md`), and the fix split the pipeline into
three jobs (partitioned backfill parse+export, unpartitioned reconciling weekly
parse+export, unpartitioned derived rebuild) plus reconciler-shaped exports. `esef_filings`
keeps its **entire dataset in ONE DuckDB file** (`esef_filings_source.duckdb`): the
year-partitioned facts/XHTML steps each delete-then-insert only their own
`period_end_year` scope within that single file, and the unpartitioned exports downstream
always full-replace ClickHouse from a read of the **whole** file (every year materialized
so far, including whatever the just-run partition step wrote). There is no split-file
order-independence hazard to design around here — a single job mirrors what
`sweden_financial`'s *pre-incident* shape looked like (see its commit `111704a5`, before
the 2026-07-19 incremental-export rework), where the weekly job's selection was a single
partitions_def shared across the whole chain including the exports.

**Mixing a partitioned selection with unpartitioned assets in one job is legal Dagster,
verified by reading the source** (not assumed): `JobDefinition._get_partitions_def`
(`dagster/_core/definitions/job_definition.py`) collects only the **non-None**
`partitions_def`s across a job's selected assets and requires exactly one unique value;
assets with `partitions_def=None` are simply excluded from that check and execute
unconditionally on every launch of the job — they are not gated by the run's
`partition_key`. Confirmed empirically too: `uv run dg check defs` passes, and a script
resolving the real repository's `esef_filings_refresh_job` shows
`job.partitions_def.get_partition_keys() == ['2019', ..., '2027']` with all 7 asset keys
present in the selection.

`esef_filings_refresh_weekly`'s `execution_fn` (`_esef_filings_refresh_run_request`)
mirrors `sweden_financial_current_year_weekly`'s pre-de-partition
`_current_year_run_request` resolver: at schedule-evaluation time it converts
`context.scheduled_execution_time` to `Europe/Belgrade` and hands the job
`RunRequest(partition_key=str(year))`; a year with no matching partition (should never
happen inside 2019-2027, but defensive) returns a `SkipReason` instead of crashing the
tick. Falls back to "now" in `Europe/Belgrade` when Dagster evaluates the schedule outside
a real tick (`scheduled_execution_time is None`).

`esef_filings_backfill_job` is the UI-launched backfill vehicle for 2019-2027 — exports
are deliberately excluded from it; run `esef_filings_refresh_job` (or the individual
export assets) once after all backfill partitions land, exactly like
`sweden_financial_backfill_job`'s exports-excluded shape.

## 9. Correctness decisions

### 9.1 `period_duration_end` anchor — the module's key correctness decision

`facts.py` stamps every fact row's `period_end` column with the **filing's own**
`period_end` (threaded through from the filing record), regardless of the fact's real OIM
period — this is necessary for joining, but means a prior-year **comparative** duration
fact (IFRS annual reports always carry last year's Revenue/ProfitLoss/... alongside this
year's) is *not* distinguishable from the current-year fact by `period_end` alone: both
would pass a `facts.period_end = filing.period_end` filter. This was Task 6's Finding 1
(Critical): the comparative fact could silently win the metrics tiebreak
(`argMaxIf(..., (decimals, fact_id))`), corrupting `revenue`/`operating_profit`/
`profit_loss` in `esef_financial_metrics`.

**Fix**: migration 000149 (amended in place — confirmed not yet applied anywhere, ledger
still at 148, so no backfill/re-migrate ordering hazard) adds
`period_duration_end Nullable(Date32)` to `corpscout.esef_facts`, immediately after
`period_instant`. `facts.py`'s `_split_period` now returns
`(period_start, period_instant, period_duration_end)`: for a duration period
(`"2022-01-01T00:00:00/2022-12-31T00:00:00"`), `period_duration_end` carries the fact's
own **true end date** (the second half of the OIM period string) — distinct from the
filing-level `period_end` stamped identically onto every fact. `metrics.py`'s
`current_facts` CTE anchors current-period selection as:

```sql
(facts.period_instant IS NOT NULL AND facts.period_instant = filing.period_end)
OR (facts.period_instant IS NULL AND facts.period_duration_end = filing.period_end)
```

This **structurally** excludes a prior-year comparative duration fact (same stamped
`period_end`, earlier true `period_duration_end`) before the `(decimals, fact_id)`
tiebreak ever sees it — exactly like the instant-fact branch already excluded a
non-matching instant. It is a structural guarantee, not a heuristic: IFRS annual reports
are guaranteed to carry undimensioned prior-year comparatives, so this anchor is load-
bearing for metrics correctness, not an edge case.

### 9.2 Sentinel dates (`1970-01-01`)

`esef_filings.period_end`/`date_added` and `esef_facts.period_end` are non-nullable
`Date32` (all three sit in an `ORDER BY`). A missing or malformed source date string
(`try_cast(... as date)` would `NULL`) is coalesced to `DATE '1970-01-01'` at export time
instead of reaching `clickhouse_driver` as Python `None` (which crashes the Date32
writer — confirmed via a direct DuckDB check:
`try_cast('2022-99-99' as date) → NULL`, `coalesce(try_cast(NULL as date), DATE
'1970-01-01') → 1970-01-01`). `esef_financial_metrics` **excludes sentinel filings
entirely** (`WHERE period_end != toDate32('1970-01-01')` in `filings_in_scope`) — "no
meaningful fiscal year" filings never contaminate metrics — and returns the excluded
count as `excluded_sentinel_period_end_count` materialization metadata so the gap is
queryable, never silent. Genuinely `Nullable` columns
(`processed_at`, `period_start`/`period_instant`/`period_duration_end` on facts) are left
plain `try_cast` — `NULL` there is semantically meaningful ("this fact has no duration
end date"), not an error to sentinel away.

### 9.3 `fxo_id` versioning

Verified live against the real API: amendments to a filing arrive as a **new filing**
under a **new `fxo_id`** (suffix `-0`, `-1`, ...) rather than mutating the original — so
one `(lei, period_end)` can carry multiple filing *versions* simultaneously in the index.
Every `esef_*` table's `ORDER BY` includes `fxo_id` for exactly this reason (not just
`lei, period_end`), and `esef_financial_metrics` deliberately does **not** dedup down to
one row per `(lei, period_end)` — each version gets its own metrics row; consumers that
want only the latest version should **prefer the highest `fxo_id` suffix themselves**
(no "latest" flag is derived in v1). Facts join by `fxo_id` too, so a given metrics row's
facts are already scoped to exactly that filing version — no cross-version contamination
even without a dedup step. Because `fxo_id` is version-stable upstream, S3 object keys
for both the facts-JSON cache and the report-XHTML archive are keyed by `fxo_id`
(`esef_filings/fact_json/fxo_id=<fxo_id>/facts.json`,
`esef_filings/report_xhtml/fxo_id=<fxo_id>/report.xhtml`) and are therefore
**version-stable and skip-existing safe**: an existing object is guaranteed to be the
same bytes the filing would produce again, so no amendment can silently serve stale
cached content under a shared key.

### 9.4 Entity map matching

`esef_entity_registry_map` is built entirely in ClickHouse from
`corpscout.gleif_lei_records` (`registered_as` + `primary_country_iso2`), scoped to LEIs
that actually appear in `corpscout.esef_filings` (this map is ESEF-driven, not a general
LEI→registry-id table). `match_source = 'gleif_registered_as'` on every row. Normalizers
(v1, extend as backoffice consumers appear):

- **FI**: lowercase, strip spaces; 8 bare digits → `NNNNNNN-N` (insert a dash before the
  last digit); already-dashed FI ids pass through unchanged.
- **SE**: digits only (10-digit org numbers expected, not enforced).
- **all other countries**: trimmed raw passthrough.

Some filings carry a **non-LEI national id** in the `lei` field by construction of the
source (e.g. Ukrainian filers using an EDRPOU code) — these simply never match a
`gleif_lei_records` row and are **kept unmatched by design**, not raised as an error; the
scope is "match what can be matched," never "filter ingestion to what's matchable."
`ORDER BY lei, resolved_at DESC` + `LIMIT 1 BY lei` guards against un-merged
`ReplacingMergeTree` duplicates in `gleif_lei_records`, even though that table is already
one-row-per-lei post-merge.

### 9.5 `scope = 'consolidated_ifrs'` — why ESEF never feeds `company_financials_latest` in v1

Every row `metrics.py`'s `native_metrics` CTE produces is stamped a literal, hardcoded
`scope`:

```sql
'consolidated_ifrs' AS scope,
```

This is not a placeholder for a future enum — it's a load-bearing correctness fence.
ESEF filings are **consolidated GROUP IFRS** figures (the parent's group-wide financials,
prepared under IFRS as EU law requires for listed issuers), whereas the per-country
national-register sources this repo otherwise ingests (`finland_ytj`, `norway_brreg`,
`estonia_ar`, `latvia_ur`, ...) report **standalone STATUTORY** financials for the single
legal entity registered in that country, prepared under local GAAP. The same company/year
can legitimately show *different, both-correct* numbers under these two scopes — a
Finnish subsidiary's standalone revenue is not comparable to its Nordic parent group's
consolidated revenue, and conflating the two is a silent, undetectable data-quality bug,
not a rounding difference.

`company_financials_latest` (the cross-source "pick one financials row per company/year"
consumer) **does not read `esef_financial_metrics` in v1**, deliberately (see §10) — the
scope-preference rule (when both a national-register standalone row and an ESEF
consolidated row exist for the same company/year, which should `company_financials_latest`
prefer, and does it ever need to expose both?) has not been agreed yet. Wiring ESEF in
before that decision is made would let a consolidated-group figure silently overwrite or
outrank a standalone statutory figure (or vice versa) with no signal to a consumer that the
number's scope just changed underneath them. `scope='consolidated_ifrs'` on every
`esef_financial_metrics` row is what makes that distinction machine-readable and enforced
at the source, so the eventual `company_financials_latest` integration (or the planned
"Group (IFRS consolidated)" backoffice section, §10) can filter/branch on it explicitly
rather than rediscovering the distinction ad hoc.

### 9.6 Two independent guards on `esef_financial_metrics_clickhouse`, and why only that asset has both

`replace_esef_financial_metrics_clickhouse` (`metrics.py`) runs **two** separate guards,
at two different points in its stage/insert/exchange sequence:

1. **Refuse-on-empty** — checked BEFORE the stage table is even created: `SELECT count()
   FROM (<scoped SELECT>)`; if 0, raises `ValueError` and the existing table is untouched.
   Same discipline as `publish.replace_esef_entity_registry_map_clickhouse`'s refuse-on-
   empty check and the shared DuckDB exporters' `allow_empty` guard.
2. **Shrink guard** (`guard_against_clickhouse_table_shrink`, `sweden_financial/clickhouse.py`,
   ratio `SHRINK_GUARD_MIN_RATIO = 0.5`) — checked AFTER the `INSERT ... SELECT` into the
   staged table but BEFORE `EXCHANGE TABLES`: compares the freshly-staged row count against
   the current live table's row count, and refuses the swap (raising `ValueError`) if the
   stage would leave the table with less than 50% of its current rows. Bypassable only via
   `EsefFinancialMetricsClickhouseExportConfig.allow_shrink` (default `False`, threaded
   through as explicit per-run config on `esef_financial_metrics_clickhouse` — never a
   standing default; see the config class's docstring in `assets.py`).

**Why this asset alone carries the shrink guard.** `esef_filings_clickhouse` and
`esef_facts_clickhouse` (the two DuckDB→ClickHouse exports) and
`esef_entity_registry_map_clickhouse` rely on refuse-on-empty ALONE — no shrink guard.
Those three tables' row counts are structurally tied 1:1 to their upstream input on every
run (the filings index, the accumulated facts across all backfilled years, the LEIs
present in `esef_filings`), so a *partial* shrink (fewer rows than before, but still
nonzero) isn't a distinct failure mode worth guarding beyond "not empty" — the ordinary
variance is upstream index churn, not a bug class. `esef_financial_metrics`, in contrast,
is a derived rebuild through a much longer chain (facts + filings + exchange rates, several
joins, an FX resolution step that can itself silently produce fewer matched rows if
`exchange_rates` is thin for a period), so a **partial** shrink — the SELECT still returns
rows, just meaningfully fewer than before — is a plausible, distinguishable failure mode
that refuse-on-empty alone would never catch. This mirrors exactly why
`sweden_financial_metrics_clickhouse` carries the same shrink guard while
`sweden_financial`'s simpler DuckDB-mirror exports don't (see
`sweden_financial/clickhouse.py`'s module docstring) — the asymmetry is deliberate, not an
inconsistency to "fix" by adding the guard everywhere.

## 10. Deferred (v1 skips)

Verbatim from the plan's "Explicitly deferred" ledger — not built, not being built as part
of this task, tracked for a future pass:

- **Embeddings + LLM search over the archived report XHTML corpus** — a separate,
  not-yet-brainstormed plan (text extraction from the S3 XHTML archive, embedding
  generation on the existing Qwen3-Embedding-8B infra, vector search + LLM answering over
  annual-report content). `esef_report_xhtml_s3` (Task 9) is this pass's data
  prerequisite, built now specifically so the corpus exists when that plan starts.
- **Arelle/package fallback** for filings without a usable `json_url` — decided after
  Task 8's real coverage numbers (what fraction of the ~25k filings actually ship
  `json_url`); until then those filings are skipped and counted
  (`has_json_facts = false`, `skipped_no_json` in materialization metadata), never
  silently dropped.
- **`company_financials_latest` consuming ESEF rows** — blocked on agreeing the
  scope-preference rule first (§9.5: ESEF is consolidated-group IFRS, national-register
  statements are usually standalone statutory, and the two can legitimately disagree for
  the same company/year — never silently mix them; `scope='consolidated_ifrs'` on every
  `esef_financial_metrics` row is the enforcement mechanism this decision will key off).
- **Backoffice "Group (IFRS consolidated)" section** — a separate follow-up plan once
  data is live in production, mirroring the generic public-contracts section pattern;
  needs a `consolidatedFinancialsQuery` per country joining `esef_financial_metrics` ×
  `esef_entity_registry_map`.
- **Dual-listed dedup rule** (one LEI/group filing in two countries) — v1 keeps both
  filings; metrics are per-filing (`fxo_id`), so nothing double-counts today, but there is
  no cross-country aggregate yet either.
- **Registry-id normalizers beyond FI/SE** — every other country's `registry_id` is a
  trimmed raw passthrough (§9.4); add a normalizer only when a real backoffice consumer
  needs it for that country.

## 11. Issues found during processing

- **JSON:API entity join must be by id, never array position** — `included` entities are
  deduplicated by the API itself (fewer entities than filings on a page with repeat
  filers), so `iter_filings()` builds a page-local `{entity_id: attributes}` map and joins
  via `relationships.entity.data.id` rather than zipping arrays.
- **Comparative-year duration facts silently winning the metrics tiebreak** — Task 6's
  Finding 1 (Critical), fixed via the `period_duration_end` structural anchor; see §9.1.
  This is the single most consequential correctness fix in the module and the reason
  migration 000149 was amended in place rather than shipped as originally drafted.
- **Non-nullable Date32 crash on missing/malformed source dates** — Task 5's Finding 1;
  `AttributeError: 'NoneType' object has no attribute 'year'` from `clickhouse_driver`
  when a `try_cast(... as date)` NULLs. Fixed with the `1970-01-01` sentinel (§9.2); a
  genuinely `Nullable` column (`processed_at`, `period_start`/`period_instant`/
  `period_duration_end`) must **not** get the same treatment — `NULL` there is meaningful.
- **`dagster_duckdb.DuckDBResource.get_connection()` leaks its connection on an
  exception** — its `@contextmanager` body has no `try`/`finally` around `yield`, so
  `conn.close()` never runs if an exception escapes the caller's `with` block. Fixed at
  the shared definition site, `read_only_duckdb_connection()` in
  `defs/common/duckdb_resources.py` (Task 9's fix pass) — every read-only DuckDB call site
  in this module (the index-non-empty check, both partitioned assets' scope reads, both
  DuckDB-sourced ClickHouse exports) goes through this one helper, so the fix covers all
  five without per-call-site changes, and benefits ~13 other country modules that share
  the same helper.
- **An injected fake `ObjectStoreResource` does not survive `dg.materialize`** — Dagster
  reconstructs a `ConfigurableResource` from its resolved pydantic config fields alone, so
  a fake S3 client set via a private attribute is silently replaced by a real
  `boto3.client(...)` inside the asset. `run_esef_filing_facts_partition` /
  `run_esef_report_xhtml_partition` are split out as plain functions specifically so
  tests can call them directly with duck-typed fakes, bypassing `dg.materialize` entirely
  for this scenario (mirrors `sweden_financial`'s
  `extract_sweden_financial_report_xhtml_catalog` pattern).
- **Mixing a partitioned + unpartitioned asset selection in one job** — not actually a
  Dagster limitation once verified against the source (§8); flagged here so a future
  reader doesn't assume it needs sweden_financial's job-splitting treatment by default.
  Originally only checked by inspecting `job.partitions_def`/`job.asset_layer` (the
  definition's *static* shape) — a code-review fix pass added
  `test_refresh_job_executes_all_assets_for_partition`
  (`tests/test_esef_filings_assets.py`), which actually calls
  `job.execute_in_process(partition_key="2026")` on a real, un-mocked-away
  `esef_filings_refresh_job` and asserts all 7 asset keys materialize. That test hits the
  same `ObjectStoreResource`/`dg.materialize` reconstruction gotcha above, plus the
  ClickHouse equivalent for `ClickhouseResource` — both resources are monkeypatched at the
  CLASS level (not instance-injected) for exactly that reason.
- **Two independent replace guards, asymmetric coverage** — see §9.6:
  `esef_financial_metrics_clickhouse` runs both refuse-on-empty AND the shrink guard;
  the three other exports (`esef_filings_clickhouse`, `esef_facts_clickhouse`,
  `esef_entity_registry_map_clickhouse`) run refuse-on-empty only. Documented as a
  finding here so a future reader doesn't read the asymmetry as an oversight.

## 12. Verification

- **Tests** (all against `FakeClickHouseClient`/`FakeObjectStore` stubs and real
  short-lived DuckDB files — no live ClickHouse or network in any test):
  `tests/test_esef_filings_client.py`, `tests/test_esef_filings_facts.py`,
  `tests/test_esef_filings_assets.py`, `tests/test_esef_filings_publish.py`,
  `tests/test_esef_filings_metrics.py` (mapping/pivot/export/migration-column-order/
  job+schedule contracts). `uv run pytest tests/test_esef_filings_assets.py -q` →
  passing (job/schedule assertions added in Task 7: job asset-key coverage, shared
  partitions_def resolution, schedule cron/timezone/status, the resolver's
  timezone-conversion and out-of-range-skip behavior; a code-review fix pass added
  `test_refresh_job_executes_all_assets_for_partition`, which actually EXECUTES
  `esef_filings_refresh_job` for `partition_key="2026"` rather than only inspecting its
  static definition — see §11). `uv run dg check defs` passes.
- **Cron slot**: `esef_filings_refresh_weekly` moved from `50 5 * * 0` (drafted) to
  `10 5 * * 0` (shipped) after a code-review fix pass caught a `(minute, hour)` collision
  with `finland_verotax_schedule`; `uv run pytest tests/test_schedule_cron_contracts.py -q`
  confirms no ESEF collision remains (2 unrelated pre-existing collisions —
  `companies_all`/`france_sirene` at `15 7`, `company_people`/`czech_ares` at `45 7` — are
  untouched, out of scope for this module).
- **Not yet done (Task 8, pending)**: migration 000149 has not been applied to live
  ClickHouse (ledger at 148); nothing in this module has been materialized against a real
  server. Task 8's plan (server backfill + validation) covers: confirming the migration
  applied cleanly, a live index materialization (compare `meta.count` vs stored rows,
  per-country distribution, `json_url` coverage fraction — the number the Arelle-fallback
  decision in §10 depends on), a UI-launched backfill of `esef_filing_facts_duckdb` +
  `esef_report_xhtml_s3` across 2019-2027, running the exports + entity map + metrics, and
  spot-checking real figures (e.g. Nokia, Ericsson) against published annual reports
  before flipping `esef_filings_refresh_weekly` on in the UI.
