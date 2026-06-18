# Finland PRH XBRL — Pipeline Analysis

> **Scope:** `src/dagster_v3/defs/finland_xbrl/`. A 7-asset pipeline that discovers Finnish companies' XBRL financial statements, downloads the XBRL XML to object storage, parses it with Arelle, and computes financial metrics in DuckDB.
>
> **Type:** Analysis only. No code changes proposed here.
>
> **Date:** 2026-06-18
>
> **Note:** Line references come from a structured mapping pass and are approximate; verify before editing.

---

## 0. Module inventory

| File | Lines | Responsibility |
|---|---|---|
| `assets.py` | 1599 | All 7 assets, dlt sources/pipelines, HTTP client, S3 catalog, DuckDB SQL transforms, config models |
| `arelle_parser.py` | 350 | Arelle-based XBRL parsing (with lxml fallback) |
| `parser.py` | 274 | XBRL parsing helpers |
| `tables.py` | 101 | Table-name constants + the hardcoded financial-metric map |
| `resources.py` | 55 | `XbrlApiResource` (PRH XBRL HTTP client) |
| `__init__.py` | 1 | docstring |

Shared resources used (imported from `defs/common/resources.py`): `LocalDuckDBResource` (default `data/finland_ytj.duckdb`) and `ObjectStoreResource` (S3 bucket `source-finland-prh-xbrl`).

---

## 1. Pipeline overview

```
finland_xbrl_financial_reports_duckdb        dlt: PRH report list → duckdb   [network, replace]
        │  + finland_ytj_all_companies_duckdb
        ▼
finland_xbrl_eligible_financial_reports      SQL join (active + has website) [cheap SQL]
        ▼
finland_xbrl_raw_xml_documents               download each XBRL XML → S3     [NETWORK; per-doc skip-if-exists ✅]
        ▼
fi_prh_xbrl_xml_documents                    parquet catalog of XML docs (S3)[cheap, read-only]
        ▼
fi_prh_xbrl_statement_documents + fi_prh_xbrl_facts_raw   Arelle parse → duckdb  [CPU-HEAVY, replace, NO checkpoint ❌]
        ▼
fi_prh_xbrl_financial_metrics                SQL pivot/aggregate → duckdb    [cheap SQL]
```

Two storage systems: **S3** (raw XML + parquet catalog) and the shared **DuckDB** file `data/finland_ytj.duckdb` (schema `finland_prh_xbrl`). The pipeline reads `finland_prhytj.all_companies` (owned by `finland_ytj`) for the eligibility join.

---

## 2. Asset-by-asset

### 2.1 `finland_xbrl_financial_reports_duckdb` (root)
- **What:** Paginated HTTP fetch of PRH financial-statement listings (`/all_financial_statements`, `registeredDateStart/End`, `page`) → dlt resource (`write_disposition="replace"`) → DuckDB `finland_prh_xbrl.financial_reports`.
- **Cost/risk:** Network-heavy, serial pagination with `request_delay_seconds` between pages; runs until an empty page (no hard cap).
- **Checkpoint:** None. `replace` truncates+reloads; a failure at page N of M loses all progress and re-fetches from page 1.
- **DuckDB:** WRITE to `data/finland_ytj.duckdb`.

### 2.2 `finland_xbrl_eligible_financial_reports`
- **What:** SQL inner join of `financial_reports` with `finland_prhytj.all_companies`, filtering `is_active = true` and non-empty `website_normalized_url` → `finland_prh_xbrl.eligible_financial_reports`.
- **Cost/risk:** Cheap SQL.
- **Checkpoint:** None (`create or replace`).
- **DuckDB:** WRITE `finland_prh_xbrl`; READ `finland_prhytj.all_companies` (cross-module, same file).

### 2.3 `finland_xbrl_raw_xml_documents` (the big download)
- **What:** Iterates eligible reports, downloads each XBRL XML from PRH, writes to S3 (`companies/{business_id}/{financial_date}.xml`); returns a Polars parquet catalog (`raw/fi_prh_xbrl_xml_documents.parquet`) with sha256/size/reused flags.
- **Cost/risk:** Network + I/O heavy; serial with `download_delay_seconds`; optional `max_reports` cap; `financial_start_date` defaults to ~2 years back.
- **Checkpoint:** **Per-document, good.** `object_store.exists(object_key)` skips already-downloaded docs unless `refresh_existing=True`; durable S3 writes; a mid-run failure keeps docs 1…N-1 and the re-run skips them.
- **DuckDB:** None (S3 only).

### 2.4 `fi_prh_xbrl_xml_documents` (catalog)
- **What:** Loads the parquet catalog from S3 and emits observability metadata (row count, bucket, key). Read-only.
- **Cost/risk:** Cheap. Fails if the catalog object is missing.
- **DuckDB:** None.

### 2.5 `fi_prh_xbrl_statement_documents` + `fi_prh_xbrl_facts_raw` (Arelle parse, multi-asset)
- **What:** Loads the XML catalog, reads each XML from S3, parses with Arelle (lxml fallback on zero facts), extracts statement metadata + individual facts → two dlt resources (`write_disposition="replace"`) into DuckDB (`fi_prh_xbrl_statement_documents`, `fi_prh_xbrl_facts_raw`).
- **Cost/risk:** **CPU-heavy** (Arelle model load + fact iteration per document); thousands of documents; progress logged every ~25 docs.
- **Checkpoint:** **None — and this is the worst spot.** All XMLs are loaded into memory, parsed sequentially, then **both tables are replaced**. A failure at document N of M loses *all* parse progress; large document counts risk OOM.
- **DuckDB:** WRITE `finland_prh_xbrl` (both tables replaced together in one dlt run).

### 2.6 `fi_prh_xbrl_financial_metrics`
- **What:** Joins `facts_raw` with a hardcoded metric map (`XBRL_FINANCIAL_METRIC_MAP` in `tables.py`), pivots numeric facts by metric code, joins `statement_documents` → `finland_prh_xbrl.fi_prh_xbrl_financial_metrics` (revenue, operating profit/loss, employees, ~12 metrics).
- **Cost/risk:** Cheap SQL (multi-CTE + pivot).
- **Checkpoint:** None (`create or replace`); requires `facts_raw` + `statement_documents` to exist.
- **DuckDB:** WRITE `finland_prh_xbrl`; READ the two parsed tables (same file).

---

## 3. What the pipeline does well

1. **Download is separated from processing, with a per-item checkpoint.** The XML download lands raw artifacts in S3 and skips already-present objects — exactly the "store-then-process, resume by object presence" design we wanted for the YTJ raw layer. A re-run after a partial download is cheap and correct.
2. **Real HTTP resilience.** Retry with exponential backoff (`max_retries=5`, initial 10s, max 120s), `respect_retry_after_header=True` (honors 429/503), per-request rate-limit delay, 120s timeout.
3. **Two-table parse split + observability.** Statements and facts are separate tables; assets emit row-count/observability metadata.
4. **Object-store provenance.** Catalog rows carry sha256, size, and a `reused` flag for downloaded documents.

---

## 4. Risks, issues, and smells

| # | Severity | Finding |
|---|---|---|
| **R1** | High | **Arelle parse has no checkpoint and buffers everything.** §2.5 loads all XMLs into memory, parses sequentially, then `replace`-writes both tables. Fail at document N of M → redo all N; thousands of docs → OOM risk. This is the most expensive stage with the *least* resilience — and it contradicts the per-doc checkpoint its own upstream download already has. By the project's own "don't redo expensive work on failure" rule, this is the standout. |
| **R2** | High | **Shared single-writer DuckDB, no concurrency pool.** Five `finland_xbrl` tables write to `data/finland_ytj.duckdb` and `eligible` reads `finland_ytj`'s `all_companies` — all on one single-writer file with **no `pool`**. Same lock / `LoadPackageNotFound` hazard fixed for `finland_ytj`/`finland_resolved`; the fix (`pool="finland_ytj_duckdb"`) has not been applied here. |
| **R3** | Med | **Root report list is full-`replace`, not incremental.** §2.1 re-fetches the entire PRH listing each run and keeps no progress on a mid-pagination failure. PRH supports `registeredDateStart/End` + `page`, so a partitioned/incremental load is feasible. |
| **R4** | Med | **Two pure-SQL transforms live as hand-written Python `create or replace` assets** (`eligible_financial_reports`, `financial_metrics`) rather than dbt-duckdb models — the same shape just migrated for `finland_resolved`. Harder to test in isolation; also part of the unpooled single-writer set. |
| **R5** | Med | **Arelle failure handling is coarse.** The lxml fallback only triggers when Arelle returns *zero* facts; an Arelle model-load crash fails the whole asset. Combined with R1 (no checkpoint), one bad document can sink the entire parse run. |
| **R6** | Low | **Catalog parquet is fully rewritten every run** (concat + `unique(keep="last")`), even when no new documents were downloaded — O(all-documents) work for a no-op; a catalog-write failure loses the catalog (S3 objects remain). |
| **R7** | Low | **Silent default scoping.** `financial_start_date` defaults to ~2 years back and `registered_date_*` default to a one-month window; `max_reports` silently caps the download. These bound coverage without surfacing it in metadata — easy to mistake partial coverage for complete. |
| **R8** | Low | **Hardcoded metric map.** `XBRL_FINANCIAL_METRIC_MAP` is embedded in `tables.py`; changing the XBRL→metric mapping requires a code change rather than a reference table. |

---

## 5. Improvement directions (mapped to established project principles)

These reuse the patterns already applied to `finland_ytj` / `finland_resolved`. Rough priority by value:

1. **Checkpoint the Arelle parse (addresses R1, R5) — highest value.** Process XMLs in batches, skip already-parsed documents (the catalog already tracks per-doc identity + sha256), and `merge`/append into DuckDB incrementally instead of replace-all-in-memory. Natural Dagster expression: partition the parse (by batch / registration-date window / company prefix) so a failure re-runs one partition, not the whole corpus. This puts the checkpoint boundary where the cost is — matching what the download already does.
2. **Single-writer pool (addresses R2) — cheap, do regardless.** Add `pool="finland_ytj_duckdb"` to the five DuckDB-touching assets (`financial_reports_duckdb`, `eligible_financial_reports`, the Arelle multi-asset, `financial_metrics`). Mirrors the `finland_ytj`/`finland_resolved` fix; serializes all writers on the shared file. The two S3-only assets don't need it.
3. **dbt-duckdb the SQL transforms (addresses R4).** Migrate `eligible_financial_reports` + `financial_metrics` to a dbt-duckdb project like `finland_resolved` (the metric map could become a dbt seed, addressing R8). Keep the dlt download, the XML→S3 download, and the Arelle parse as Python — they are not dbt-able.
4. **Incremental / partitioned report list (addresses R3).** Optional: partition §2.1 by `registeredDate` window so a failed pull resumes per-window; lower value than R1 since the list is comparatively small.

The downloads (§2.3) are already well-designed and need only minor polish (R6/R7): emit coverage metadata (what window/cap was applied) and make the catalog update incremental.

---

## 6. Summary

`finland_xbrl` is a more mature pipeline than `finland_ytj` was: it already separates the expensive XML download from processing, lands raw artifacts in S3 with a per-document skip-if-exists checkpoint, and has solid HTTP retry/backoff. The notable design flaw is that its **most expensive stage — the Arelle parse — has the *least* resilience** (loads everything in memory, replaces both tables, zero resumability), the opposite of the careful checkpointing its own upstream download uses. That is **R1**, the highest-value fix.

Beyond that, the pipeline carries the same two issues just resolved elsewhere: the **shared single-writer DuckDB without a concurrency pool (R2)** and **pure-SQL transforms that belong in dbt-duckdb (R4)**. Both are direct repeats of the `finland_ytj` / `finland_resolved` work.
