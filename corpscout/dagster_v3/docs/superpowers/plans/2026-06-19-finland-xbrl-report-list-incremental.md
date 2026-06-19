# Finland XBRL Report List — Incremental by Registration Month (R3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make `finland_xbrl_financial_reports_duckdb` partitioned by registration month with `merge` (was full-`replace`), so the `financial_reports` table **accumulates complete history** one resumable month at a time instead of holding only the latest fetched window.

**Architecture:** Attach the existing monthly partition def (`fi_xbrl_parse_partitions`, registration-month, already used by the parse) to the report-list `@dlt_assets`. For partition month `M`, the asset derives `registeredDateStart/End` from `context.partition_time_window` (inclusive first/last day of `M`) and runs the existing paginated dlt source for that window. The resource's `write_disposition` flips `replace → merge` (keyed on `(business_id, financial_date, registration_date)`), so each month upserts. A registration date is fixed per filing, so a report never jumps partitions (same stability the parse relies on). The asset already carries `pool="finland_ytj_duckdb"`.

**Why:** Today the resource is `write_disposition="replace"` with a default 1-month window, so the `financial_reports` table only ever contains the most recent run's window — there is no way to build full history without one giant single-shot fetch, and a mid-pagination failure restarts from page 1. Partition+merge fixes all three: accumulate history (backfill the months), resume per-month, idempotent re-runs.

**Scope:** R3 only. Out of scope: per-checkout dlt `pipelines_dir` isolation for the xbrl pipelines (the `pool` already serializes within an instance — note it but don't fix here), R6, the `reparse_existing` flag.

**Tech Stack:** Dagster 1.13.9 (`MonthlyPartitionsDefinition`, `context.partition_time_window`), dlt (`merge`), DuckDB.

**Key existing facts (verified):**
- `_financial_reports_resource` (assets.py ~213-268): `@dlt.resource(name=XBRL_DLT_FINANCIAL_REPORTS_TABLE, write_disposition="replace", primary_key=("business_id","financial_date","registration_date"))`; paginates `_download_financial_reports_page` over `[registered_date_start, registered_date_end]` until an empty page.
- `finland_xbrl_financial_reports_duckdb_asset` (~308-337): `@dlt_assets(..., pool="finland_ytj_duckdb")`; body runs `dlt.run(dlt_source=finland_xbrl_financial_reports_source(registered_date_start=config.registered_date_start, registered_date_end=config.registered_date_end, ...), dlt_pipeline=finland_xbrl_financial_reports_pipeline(source_duckdb.path()))`.
- `run_finland_xbrl_financial_reports_dlt_pipeline(*, database_path, registered_date_start, registered_date_end, run_id, session=None, ...)` (~271-294) — the test entrypoint, takes explicit dates; **not changing its signature**.
- `XbrlFinancialReportsConfig` (~91-120): has `registered_date_start`/`registered_date_end` (defaults: last month / today) + operational fields (`request_delay_seconds`, `max_retries`, `retry_initial_delay_seconds`, `retry_max_delay_seconds`) + a `validate_required_iso_date` validator on the two date fields.
- `fi_xbrl_parse_partitions = dg.MonthlyPartitionsDefinition(start_date=FI_XBRL_PARSE_PARTITION_START)` already exists (defined for the parse).
- Downstream `eligible_financial_reports` (dbt model) reads the whole `financial_reports` table → unaffected by partitioning.

**Test command:** `uv run pytest tests/test_finland_xbrl_assets.py -v` (the report-list tests live here).

---

### Task 1: Partition the report-list asset + merge + window-driven dates

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets.py`
- Test: `tests/test_finland_xbrl_assets.py`

- [ ] **Step 1: Write the failing tests** (partition registration + merge-accumulation):

```python
from datetime import date

from dagster import AssetKey
from dagster_v3.definitions import defs as load_project_defs


def test_financial_reports_asset_is_monthly_partitioned():
    graph = load_project_defs().get_repository_def().asset_graph
    node = graph.get(AssetKey(["finland_xbrl_financial_reports_duckdb"]))
    assert node.partitions_def is not None
    assert type(node.partitions_def).__name__ == "MonthlyPartitionsDefinition"


def test_financial_reports_merge_accumulates_across_windows(tmp_path):
    # Two separate window loads into the same DuckDB must ACCUMULATE (merge), not replace.
    import duckdb
    import dagster_v3.defs.finland_xbrl.assets as xbrl

    db = tmp_path / "finland_ytj.duckdb"

    def _session(financials):
        # a fake HttpSession whose .get returns page 1 = financials, page 2 = empty
        class _Resp:
            def __init__(self, payload): self._p = payload
            def raise_for_status(self): pass
            def json(self): return self._p
        class _Sess:
            def __init__(self): self.calls = 0
            def get(self, url, params=None, timeout=120):
                self.calls += 1
                page = (params or {}).get("page", 1)
                return _Resp({"financials": financials if page == 1 else []})
        return _Sess()

    xbrl.run_finland_xbrl_financial_reports_dlt_pipeline(
        database_path=db, registered_date_start="2024-03-01", registered_date_end="2024-03-31",
        run_id="r1", session=_session([{"businessId": "a", "financialDate": "2023-12-31", "registrationDate": "2024-03-10"}]),
    )
    xbrl.run_finland_xbrl_financial_reports_dlt_pipeline(
        database_path=db, registered_date_start="2024-04-01", registered_date_end="2024-04-30",
        run_id="r2", session=_session([{"businessId": "b", "financialDate": "2023-12-31", "registrationDate": "2024-04-05"}]),
    )
    with duckdb.connect(str(db), read_only=True) as conn:
        ids = [r[0] for r in conn.execute(
            f"select business_id from {xbrl.XBRL_DLT_DATASET_NAME}.{xbrl.XBRL_DLT_FINANCIAL_REPORTS_TABLE} order by business_id"
        ).fetchall()]
    assert ids == ["a", "b"]  # merge accumulates; replace would yield only ["b"]
```
NOTE TO IMPLEMENTER: the project already has report-list tests in this file that call `run_finland_xbrl_financial_reports_dlt_pipeline` with a fake session — REUSE that file's existing fake-session/`_download_financial_reports_page` fixture rather than the inline `_session` above if it exists and is cleaner. Match the real `_download_financial_reports_page` HTTP shape (it calls `session.get(...)` and reads `payload["financials"]` via `_financials_from_payload`; confirm the param name for the page, e.g. `page`/`page_number`, and the URL). Make the fake faithful to the real request.

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/test_finland_xbrl_assets.py -k "monthly_partitioned or merge_accumulates" -v`
Expected: FAIL — asset not partitioned; resource is `replace` so the second window wipes the first (`ids == ["b"]`).

- [ ] **Step 3: Flip the resource to `merge`**

In `_financial_reports_resource`'s `@dlt.resource(...)` decorator, change `write_disposition="replace"` → `write_disposition="merge"` (keep `name` and `primary_key`).

- [ ] **Step 4: Partition the asset + derive the window**

Add `from datetime import timedelta` to the datetime import (it already imports `UTC, date, datetime` — extend it).

On the `@dlt_assets(...)` decorator for `finland_xbrl_financial_reports_duckdb_asset`, add `partitions_def=fi_xbrl_parse_partitions,` (keep the existing `dlt_source=`, `dlt_pipeline=`, `name=`, `dagster_dlt_translator=`, `pool=`).

Rewrite the body to derive the dates from the partition window:
```python
def finland_xbrl_financial_reports_duckdb_asset(
    context: dg.AssetExecutionContext,
    config: XbrlFinancialReportsConfig,
    dlt: DagsterDltResource,
    source_duckdb: LocalDuckDBResource,
) -> Iterator[Any]:
    """Load PRH XBRL financial report listings for the partition month to DuckDB with dlt."""
    window = context.partition_time_window
    registered_date_start = window.start.date().isoformat()
    registered_date_end = (window.end.date() - timedelta(days=1)).isoformat()  # inclusive last day of month
    context.log.info(
        "Loading XBRL financial reports registered %s..%s",
        registered_date_start, registered_date_end,
    )
    yield from dlt.run(
        context=context,
        dlt_source=finland_xbrl_financial_reports_source(
            registered_date_start=registered_date_start,
            registered_date_end=registered_date_end,
            request_delay_seconds=config.request_delay_seconds,
            max_retries=config.max_retries,
            retry_initial_delay_seconds=config.retry_initial_delay_seconds,
            retry_max_delay_seconds=config.retry_max_delay_seconds,
            run_id=context.run_id,
        ),
        dlt_pipeline=finland_xbrl_financial_reports_pipeline(source_duckdb.path()),
    )
```

- [ ] **Step 5: Remove the now-unused config date fields (with a fallout check)**

The asset no longer reads `config.registered_date_start`/`config.registered_date_end` (the partition window drives them). Remove those two fields from `XbrlFinancialReportsConfig` and the `validate_required_iso_date` validator (it only validated those two fields — confirm via reading the validator's `@field_validator(...)` targets). The constants `DEFAULT_XBRL_REGISTERED_DATE_START`/`DEFAULT_XBRL_REGISTERED_DATE_END` and the helper `resolve_registration_window` may become unused — **run `rg "DEFAULT_XBRL_REGISTERED_DATE_(START|END)|resolve_registration_window|registered_date_start|registered_date_end" src tests`** and remove only what is genuinely unused; KEEP anything still referenced (e.g. the runner's params, `_dlt_financial_report_row`'s `registered_date_start/end` args, `_download_financial_reports_page`). If removing the config fields breaks an existing test that set them, update that test to not set them (the partition window now controls the dates). If this cleanup balloons, leave the config fields in place (unused) and just stop reading them in the body — report which you did.

- [ ] **Step 6: Run the new tests + full file**

Run: `uv run pytest tests/test_finland_xbrl_assets.py -v`
Expected: PASS (partition test + merge-accumulation test + existing report-list tests). Update any existing test that materialized the asset without a `partition_key` (the asset is now partitioned — direct `materialize` needs `partition_key=`) or that relied on `replace` semantics.

- [ ] **Step 7: `dg check defs`**

Run: `uv run dg check defs`
Expected: no errors. The partitioned report-list feeds the dbt `eligible_financial_reports` (unpartitioned) — default `AllPartitionMapping`; if `dg check` complains, report and add the mapping only if required.

- [ ] **Step 8: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets.py tests/test_finland_xbrl_assets.py
git commit -m "feat(finland_xbrl): partition financial-reports list by registration month, merge-accumulate"
```

---

### Task 2: Verification

- [ ] **Step 1:** `uv run pytest tests/test_finland_xbrl_assets.py tests/test_finland_xbrl_parsed_assets.py tests/test_finland_xbrl_dbt.py -m "not integration" -v` → PASS.
- [ ] **Step 2:** `uv run dg check defs` → no errors.
- [ ] **Step 3: Lineage / partition spot-check:** `uv run dg list defs --json | python3 -c "import sys,json; d=json.load(sys.stdin); [print(a['asset_key'], a.get('dependency_keys')) for a in d['assets'] if 'xbrl' in a['asset_key'].lower()]"` — confirm `finland_xbrl_financial_reports_duckdb` is a root, `eligible_financial_reports` still depends on it + all_companies, and the rest of the lineage is unchanged.
- [ ] **Step 4:** Final commit if anything was adjusted.

---

## Self-Review

**Spec coverage:** partition by registration month → Task 1 Step 4; merge-accumulate → Step 3; window-driven dates → Step 4; config cleanup → Step 5. Resumability (per-month) + history-accumulation both fall out of partition+merge.

**Placeholder scan:** the fake-session in the Task 1 test is illustrative — the implementer is instructed to reuse the file's existing report-list fake fixture and match the real `_download_financial_reports_page` request shape. Not a placeholder for production code.

**Type/name consistency:** the runner `run_finland_xbrl_financial_reports_dlt_pipeline` signature is unchanged (explicit dates) so the merge-accumulation test drives it directly; the asset body derives dates from `context.partition_time_window` and passes them to the same `finland_xbrl_financial_reports_source`; `primary_key` unchanged so `merge` keys on `(business_id, financial_date, registration_date)`.

**Risks to verify during execution:**
1. **`context.partition_time_window` on `@dlt_assets`** — confirm it works for the dlt-assets op (the parse multi_asset uses it; `exchange_rates_v2`'s `@dlt_assets` uses `partition_key_range` instead — if `partition_time_window` raises here, switch to `context.partition_key_range` + `day_partition_range_window`-style helpers and report).
2. **Inclusive end date** — PRH's `registeredDateEnd` is treated inclusive; `(window.end.date() - 1 day)` gives the month's last day. Verify the real API semantics against `_download_financial_reports_page` (how it passes the dates) — if PRH wants an exclusive end, pass `window.end.date()` directly instead.
3. **Config-field removal fallout** — Step 5's grep; keep anything still referenced.
4. **Existing report-list tests** — some may set config dates or expect `replace`; update them (Task 1 Step 6).
5. **Shared dlt `pipelines_dir`** — `finland_xbrl_financial_reports_pipeline` uses the default `~/.dlt`; the `pool` (limit 1) serializes partition runs within the instance so they don't collide, but cross-worktree concurrency could (same latent issue fixed for finland_ytj). Out of scope here — note it.

---

## Execution Handoff

Plan complete and saved. Two options:
1. **Subagent-Driven (recommended)** — fresh subagent per task, two-stage review.
2. **Inline Execution** — execute here with checkpoints.

Which approach?
