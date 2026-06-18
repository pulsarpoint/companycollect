# Finland PRH YTJ — Asset Analysis

> **Scope:** `src/dagster_v3/defs/finland_ytj/`. This module contains a **single** asset today. This document analyzes that asset in depth and notes where the "next" assets in this lineage actually live and what future assets *within* `finland_ytj` could be.
>
> **Type:** Analysis only. No code changes proposed here.
>
> **Date:** 2026-06-18

---

## 0. Module inventory

| File | Responsibility |
|---|---|
| `assets.py` | The dlt source/resource/pipeline + the one Dagster asset + all row-shaping helpers |
| `resources.py` | `LocalDuckDBResource` (used here) and `ObjectStoreResource` (defined here, **used by `finland_xbrl`, not by this module**) |
| `__init__.py` | Docstring only |

**Assets defined in this directory: 1**

- `finland_ytj_all_companies_duckdb` — root asset (no upstream deps), kinds `{python, dlt, duckdb}`, group `finland_ytj`.

**Downstream assets that depend on it (defined elsewhere, in `finland_resolved/`):**

- `finland_ytj_resolved_duckdb` ← depends on `finland_ytj_all_companies_duckdb`
- `finland_ytj_resolved_clickhouse` ← depends on `finland_ytj_resolved_duckdb`

So the lineage is `all_companies_duckdb → resolved_duckdb → resolved_clickhouse`, but only the **first** node lives in `finland_ytj/`. This is the key architectural fact to keep in mind: this module is the **raw ingestion landing** stage; normalization/export was split into `finland_resolved`.

How it gets registered: `definitions.py` calls `dg.load_from_defs_folder(...)`, which picks up the `defs = dg.Definitions(...)` object at the bottom of `assets.py`. The top-level `definitions.py` supplies the shared `dlt` (`DagsterDltResource`) and `clickhouse` resources; `assets.py` supplies the `ytj_duckdb` resource.

---

## Asset 1 of 1 — `finland_ytj_all_companies_duckdb`

### 1.1 What it is

A `@dlt_assets` asset (`assets.py:108-125`) that downloads the **entire** Finland PRH YTJ "all companies" open-data feed in one request and loads it into a local DuckDB table via [dlt](https://dlthub.com/). It is the root of the Finland company lineage — `deps=[]`.

- **Asset key:** `finland_ytj_all_companies_duckdb` (forced via the translator, see 1.4)
- **Group:** `finland_ytj`
- **Kinds:** `python`, `dlt`, `duckdb`
- **DuckDB target:** dataset `finland_prhytj`, table `all_companies`, file `data/finland_ytj.duckdb` (resource default)
- **Write disposition:** `replace` — the table is fully rebuilt on every materialization
- **Declared primary key:** `business_id`

### 1.2 Execution path (end to end)

```
finland_ytj_all_companies_duckdb_asset            (assets.py:114)
  └─ dlt.run(source=finland_ytj_source(run_id=context.run_id),
             pipeline=finland_ytj_pipeline(ytj_duckdb.path()))
        └─ finland_ytj_source                      (assets.py:49)
            └─ _all_companies_resource             (assets.py:67)  write_disposition="replace", primary_key="business_id"
                 ├─ _download_all_companies        (assets.py:180)  GET {base_url}/all_companies, raise_for_status, returns bytes
                 ├─ _json_bytes_from_response       (assets.py:216)  unzip first *.json if zip, else raw bytes
                 ├─ json.loads(...)                                full payload parsed in memory
                 ├─ _companies_from_payload         (assets.py:228)  accepts list[...] OR {"companies":[...]}
                 └─ build_dlt_company_rows          (assets.py:138)  -> _dlt_company_row per company
        └─ dlt writes rows to DuckDB dataset finland_prhytj / table all_companies
```

Source endpoint: `https://avoindata.prh.fi/opendata-ytj-api/v3/all_companies` (`YTJ_BASE_URL`, `assets.py:22`), 120s timeout, custom `User-Agent`.

### 1.3 Output schema (per `_dlt_company_row`, `assets.py:145-177`)

Three families of columns:

**Provenance / lineage**
| Column | Source | Notes |
|---|---|---|
| `country_iso2` | constant `"FI"` | `COUNTRY` |
| `source_slug` | constant `"finland_prhytj"` | `SOURCE` |
| `source_run_id` | `context.run_id` | Dagster run that produced the row |
| `source_line_number` | enumerate index, **1-based** | position in the feed |
| `source_record_id` | `business_id` | |
| `source_payload_hash` | `sha256` of sorted-key JSON of the raw company | `source_payload_hash`, `assets.py:201` — stable change-detection key |

**Core business fields (string-coerced via `_string`)**
`business_id`, `registration_date`, `end_date`, `last_modified`, `trade_register_status`, `status`, `primary_name`.

**Derived**
| Column | Logic | Reference |
|---|---|---|
| `lifecycle_status` | `"ceased"` if `endDate` present **or** `tradeRegisterStatus == "3"`, else `"active"` | `_lifecycle_status`, `assets.py:251` |
| `is_active` | `lifecycle_status == "active"` | |
| `primary_name` | first name with `type == "1"` and no `endDate`; fallback to first name in list | `_primary_name`, `assets.py:237` |
| `website_url` | `website.url` trimmed | |
| `website_normalized_url` / `website_host` / `website_path` | `https://` prepended if no scheme, netloc lower-cased, host+path split | `_stable_normalized_url_parts` / `normalized_url_parts`, `assets.py:194,206` |
| `website_registered_on` / `website_ended_on` | from `website` sub-object | |
| `raw_company` | verbatim compact JSON of the source record | full-fidelity escape hatch for downstream |

### 1.4 The translator override (`FinlandYtjDltTranslator`, `assets.py:35-46`)

`@dlt_assets` would normally derive asset keys/specs from dlt resource names. This translator intercepts the `all_companies` resource and forces a clean public contract:

- `key="finland_ytj_all_companies_duckdb"` (instead of a dlt-derived name)
- `deps=[]` (declares it as a lineage root)
- `group_name="finland_ytj"`, a human description, and `kinds={"python","dlt","duckdb"}`

This is a clean, deliberate pattern — the dlt internals stay hidden and the asset key is stable for downstream `finland_resolved` to reference.

### 1.5 Resources

- **`LocalDuckDBResource`** (`resources.py:85`) — default path `data/finland_ytj.duckdb`; provides a `connect()` context manager (not used by this asset, which lets dlt own the connection) and `path()` (used). Registered as `ytj_duckdb` in the module's `defs`.
- **`ObjectStoreResource`** (`resources.py:29`) — S3/MinIO wrapper. **Not referenced by this module's asset at all.** It exists here only because `finland_xbrl/assets.py` imports it from `finland_ytj.resources` (13 usages). See risk R7.

---

## 2. Design decisions worth calling out

1. **Full `replace` every run.** Simple and idempotent — the table always reflects the latest full feed. No history, no incremental cursor. `primary_key="business_id"` is essentially documentation here (replace doesn't merge).
2. **Raw + derived in one wide table.** `raw_company` keeps the untouched record, so `finland_resolved` can re-derive anything without re-fetching. Good separation: ingestion is "land it faithfully," normalization happens downstream.
3. **Stable provenance hash.** `source_payload_hash` over sorted-key JSON gives a deterministic per-record fingerprint for change detection in downstream models.
4. **Testability via injection.** `HttpSession` Protocol + `session` parameter threads through `finland_ytj_source → _all_companies_resource → _download_all_companies`, so the HTTP layer can be faked in tests without patching `requests`. Same for `LocalDuckDBResource` path injection.
5. **Zip-or-json tolerance.** `_json_bytes_from_response` transparently handles PRH serving either a `.zip` bundle or raw JSON.

---

## 3. Risks, issues, and smells

| # | Severity | Finding |
|---|---|---|
| **R1** | High | **Whole-dataset-in-memory.** `response.content` (full bytes) → `json.loads` (full object) → `build_dlt_company_rows` (full `list` of dict rows, each also carrying a `raw_company` JSON string). Finland PRH has on the order of 10⁶ businesses; the feed is large. Nothing streams or chunks. This is an OOM / long-pause risk and the most likely thing to break at scale. dlt's strength is generators — yet `_all_companies_resource` materializes the entire list before yielding (`yield from build_dlt_company_rows(...)`). |
| **R2** | High | **Empty payload silently wipes the table.** `_companies_from_payload` returns `[]` for any unexpected shape, and with `write_disposition="replace"` an empty/malformed download replaces a good table with zero rows. A transient PRH change or partial download could destroy the landing table that `finland_resolved` depends on. No min-row guard / abort-on-empty. |
| **R3** | Med | **Single 120s GET for a large ZIP.** No retry/backoff, no resumability. A flaky download fails the whole run. |
| **R4** | Med | **Magic strings.** `tradeRegisterStatus == "3"` (`assets.py:252`) and name `type == "1"` (`assets.py:242`) are undocumented PRH codes. A comment or named constant would prevent silent breakage if PRH changes encodings. |
| **R5** | Low | **No emitted run metadata.** The asset logs one line but emits no `MaterializeResult` metadata (row count, active count, download bytes). Operators can't see at a glance how many companies landed — and a row-count metric would also make R2 visible. |
| **R6** | Low | **Import-time construction with default path.** The decorator evaluates `finland_ytj_source()` and `finland_ytj_pipeline(LocalDuckDBResource().path())` at import (`assets.py:109-110`) using the **default** DuckDB path. The actual run rebuilds both with the injected `ytj_duckdb.path()` inside the body, so the import-time objects are only used for spec derivation. Harmless today, but the duplication is a footgun if someone later expects the decorator-level path to be authoritative. |
| **R7** | Low | **Cross-module resource coupling.** `finland_xbrl` imports `ObjectStoreResource` (and `LocalDuckDBResource`) from `finland_ytj.resources`. `finland_ytj` has become an accidental shared-resource home. A neutral `defs/shared/` (or `defs/common/resources.py`) would be a cleaner owner. |
| **R8** | Low | **Shared-session header mutation.** `_download_all_companies` does `http_session.headers["User-Agent"] = user_agent` on a caller-supplied session, mutating shared state. Minor, but a passed-in session gets silently rewritten. |

---

## 4. The "next assets" question

You framed the next step as **future assets within `finland_ytj`**. Two facts shape that:

1. The obvious downstream nodes (`resolved_duckdb`, `resolved_clickhouse`) **already exist**, but in `finland_resolved/`. So "normalize + export to ClickHouse" is already covered by a sibling module — duplicating it here would be wrong.
2. That leaves a clear lane for genuinely *new* assets that belong to the **ingestion** concern this module owns. Candidates, in rough priority:

   - **Asset checks on `all_companies_duckdb`** (directly addresses R2/R5): non-empty row count, `business_id` non-null/unique, active-count sanity floor. Cheapest, highest-value next addition.
   - **Incremental / `lastModified`-based load** (addresses R1/R3): PRH exposes per-company `lastModified`; a `merge` disposition keyed on `business_id` with a high-water mark would remove the full-replace memory pressure and the wipe risk.
   - **Child-table extraction** (e.g. `finland_ytj_names_duckdb`, `..._addresses_duckdb`): dlt can fan a nested array into separate tables, giving downstream typed name/address history instead of forcing every consumer to parse `raw_company`.

   These are observations, not a plan — confirm direction before any of them is scoped into tasks.

---

## 5. dlt idiomaticity & asset granularity

Two design questions came up about this asset: should it be split into smaller pieces, and is its HTTP download idiomatic for dlt? Both verified against dlt's own docs ([streaming resources](https://dlthub.com/docs), `RESTClient`/`rest_api`, `dlt.sources.helpers.requests`).

### 5.1 Should the asset be divided into smaller sections?

The answer differs by the kind of division:

- **Do NOT split into download → parse → transform Dagster assets.** A `@dlt_assets` asset *is* one dlt pipeline run, and extract→normalize→load is the exact unit dlt owns. Splitting those stages into separate Dagster assets forces materializing intermediate artifacts (raw bytes in S3, a staging table) just to hand them between assets — re-implementing what dlt already does internally. The correct *downstream* boundary already exists and is correctly placed: `finland_ytj_resolved_duckdb` in `finland_resolved/`.
- **DO consider the dlt-native division: one source, multiple resources.** The PRH record is deeply nested (`names[]`, `website{}`, addresses). Today everything is flattened into one wide row plus a `raw_company` blob. The idiomatic split is for the source to yield several resources, each landing its own typed table:

  ```python
  @dlt.source(name="finland_ytj")
  def finland_ytj_source(...):
      return [
          _all_companies_resource(...),   # -> all_companies
          _company_names_resource(...),   # -> company_names (one row per historical name)
          _company_addresses_resource(...),
      ]
  ```

  Same single asset run, multiple clean tables, and downstream consumers stop re-parsing `raw_company`. Worth doing **only if** `finland_resolved` currently pays a cost parsing those blobs (ties to the child-table idea in §4).
- **File-level splitting is cosmetic.** `assets.py` mixes dlt plumbing, the Dagster asset, and ~10 row-shaping helpers; they could move to `transforms.py`/`source.py`, but the functions are already small and well-named. Low priority.

### 5.2 Is the HTTP download idiomatic for dlt?

**The outer structure is idiomatic; the I/O internals are not.** Two specific deviations:

1. **Buffers the whole dataset instead of yielding (= R1).** Idiomatic dlt resources are generators that stream items/chunks (`for page in client.paginate(...): yield page`). Here `_download_all_companies` returns full `bytes` → `json.loads` parses the whole object → `build_dlt_company_rows` builds a complete `list` → `yield from`. By the time the first row reaches dlt the entire dataset is resident ~3× (bytes, parsed dict, row list with `raw_company` strings). This defeats dlt's streaming model.
2. **Raw `requests` with no retry (= R3).** dlt ships `dlt.sources.helpers.requests` (drop-in `requests` API with built-in retry/backoff) and `RESTClient`. A single bare 120 s GET with no backoff is fragile.

**Important nuance — it is not simply "wrong."** PRH's `all_companies` is a **bulk ZIP/JSON snapshot**, not a paginated JSON API, so `RESTClient.paginate` / the `rest_api` source (the textbook idiomatic path) genuinely don't fit. Fetching the file over plain HTTP is reasonable. (PRH *does* expose a paginated `/companies` endpoint — if per-page incremental loads were ever wanted, that's where `RESTClient.paginate` would become the idiomatic answer.)

**Idiomatic version of the current bulk approach** — stream-parse instead of buffer:

```python
import ijson
from dlt.sources.helpers import requests as dlt_requests

@dlt.resource(name=DLT_COMPANIES_TABLE, write_disposition="replace", primary_key="business_id")
def _all_companies_resource(*, base_url, run_id, ...):
    resp = dlt_requests.get(f"{base_url}/all_companies", timeout=..., stream=True)  # retry/backoff -> fixes R3
    resp.raise_for_status()
    stream = _json_stream(resp)                                # transparently unzip the member
    for i, company in enumerate(ijson.items(stream, "item"), start=1):  # or "companies.item"
        yield _dlt_company_row(company, line_number=i, run_id=run_id)   # one row at a time -> flat memory, fixes R1
```

**One wrinkle:** a ZIP's central directory sits at the *end* of the file, so true streaming-unzip needs either `stream-unzip` or a download-to-temp-file-then-stream-the-member step — trading RAM for disk, which is the right trade for a snapshot this size.

**Verdict:** the dlt *integration* (source → resource → pipeline, `@dlt_assets`, the translator key override) is clean and idiomatic. The *I/O internals* buffer the whole dataset and skip dlt's resilient HTTP helper — which is exactly why R1 and R3 are top findings.

---

## 6. Summary

`finland_ytj_all_companies_duckdb` is a clean, well-factored **raw landing** asset: faithful ingestion, strong provenance columns, an explicit asset-key contract via the translator, and good dependency injection for testing. Its design is sound for the role it plays (root of the Finland lineage; normalization deferred to `finland_resolved`).

The two findings worth acting on first are **R1 (whole-dataset-in-memory)** and **R2 (empty payload silently replaces the table)** — both are scale/correctness risks inherent to the full-`replace` + load-everything-in-memory approach. The single most valuable *new* asset-side addition would be **asset checks** that make R2 impossible to miss.
