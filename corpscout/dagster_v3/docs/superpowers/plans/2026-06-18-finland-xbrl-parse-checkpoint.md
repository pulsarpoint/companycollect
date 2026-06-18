# Finland XBRL Parse — Checkpoint via Monthly Partitions (R1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make the Arelle parse (`fi_prh_xbrl_statement_documents` + `fi_prh_xbrl_facts_raw`) resumable and memory-bounded by partitioning it **by registration month**, so a failure re-runs one month (not the whole corpus) and skips documents already parsed.

**Architecture:** Attach a `MonthlyPartitionsDefinition` to the parse multi-asset only (its neighbors stay unpartitioned). For partition month `M`, the asset: (1) filters the S3 XML catalog to docs whose `registration_date` ∈ `M`; (2) computes each doc's content-addressed `statement_key = statement_key_for(business_id, financial_date, xml_sha256)` from the catalog (no XML read) and **skips** docs whose key already exists in DuckDB; (3) for the remaining (new/corrected) docs, deletes any prior rows for their `(business_id, financial_date)` (so a corrected filing replaces the old version cleanly), parses them with Arelle, and **appends** to the two tables. Re-running a month skips everything already loaded → idempotent + resumable. Memory is bounded to one month's new docs.

**Why this is correct (the content-addressed key):** `statement_key` already includes `xml_sha256` (`arelle_parser.statement_key_for`), so identical content ⇒ identical key ⇒ idempotent `append` after the per-`(business_id, financial_date)` delete; changed content ⇒ new key, and the delete removes the stale prior version. No `merge`/orphan-fact complications. **No schema change needed** — `statement_key`, `xml_sha256`, `registration_date`, `business_id`, `financial_date` already exist on both the catalog and the statement table.

**Scope:** R1 only (the parse checkpoint). Rides along on the same asset: `pool="finland_ytj_duckdb"` (the R2 single-writer fix for this asset, free here since we're editing it). Out of scope: dbt migration of the SQL transforms (R4), incremental report list (R3), per-doc streaming below the month grain (note it as a future refinement if a peak month OOMs → `MultiPartitionsDefinition` month × hash-bucket).

**Tech Stack:** Dagster 1.13.9 (`MonthlyPartitionsDefinition`, `context.partition_time_window`), dlt (`write_disposition="append"`), DuckDB, Arelle. All present.

**Key existing facts (verified):**
- Parse asset: `finland_xbrl_parsed_tables` — a `@dg.multi_asset` (specs for `STATEMENT_DOCUMENTS_TABLE`, `FACTS_TABLE`) calling `run_finland_xbrl_arelle_dlt_pipeline` → `finland_xbrl_arelle_source` → `_finland_xbrl_arelle_resources` (assets.py ~431-696).
- `_finland_xbrl_arelle_resources` loads the **whole** manifest, parses **all** docs into in-memory lists, yields two `write_disposition="replace"` resources (assets.py ~485-568) — this is what we change.
- Catalog/manifest doc dict comes from `_normalize_xml_document_row` (assets.py ~1540), which currently **drops `xml_sha256`** — we must carry it through.
- `statement_key_for(business_id, financial_date, xml_sha256)` lives in `finland_xbrl/arelle_parser.py:133`.
- DuckDB dataset `finland_prh_xbrl` (`XBRL_DLT_DATASET_NAME`); tables from `tables.py`. `_duckdb_table_exists`, `_ensure_parsed_duckdb_tables`, `parsed_duckdb_row_counts` already exist.

**Test command:** `uv run pytest tests/test_finland_xbrl_parsed_assets.py -v`

---

### Task 1: Carry `xml_sha256` through the manifest + pure filter/skip helpers

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets.py` (`_normalize_xml_document_row`; add `documents_in_registration_window`, `unparsed_documents`; import `statement_key_for`)
- Test: `tests/test_finland_xbrl_parsed_assets.py`

- [ ] **Step 1: Write failing tests** (append to `tests/test_finland_xbrl_parsed_assets.py`; reuse its existing imports — confirm `import datetime`/`from datetime import date` and `import dagster_v3.defs.finland_xbrl.assets as xbrl` alias, adapt names to the file's conventions):

```python
from datetime import date

import dagster_v3.defs.finland_xbrl.assets as xbrl
from dagster_v3.defs.finland_xbrl.arelle_parser import statement_key_for


def _doc(business_id, financial_date, registration_date, sha):
    return {
        "business_id": business_id,
        "financial_date": financial_date,
        "registration_date": registration_date,
        "xml_object_key": f"companies/{business_id}/{financial_date}.xml",
        "xml_sha256": sha,
    }


def test_documents_in_registration_window_filters_by_month():
    docs = [
        _doc("a", "2023-12-31", "2024-03-10", "sha-a"),
        _doc("b", "2023-12-31", "2024-04-02", "sha-b"),
        _doc("c", "2023-12-31", "", "sha-c"),  # missing registration date -> excluded
    ]
    got = xbrl.documents_in_registration_window(
        docs, window_start=date(2024, 3, 1), window_end=date(2024, 4, 1)
    )
    assert [d["business_id"] for d in got] == ["a"]


def test_unparsed_documents_skips_already_parsed_by_content_key():
    docs = [
        _doc("a", "2023-12-31", "2024-03-10", "sha-a"),
        _doc("b", "2023-12-31", "2024-03-11", "sha-b"),
    ]
    already = {statement_key_for("a", "2023-12-31", "sha-a")}
    got = xbrl.unparsed_documents(docs, parsed_statement_keys=already)
    assert [d["business_id"] for d in got] == ["b"]
```

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/test_finland_xbrl_parsed_assets.py -k "registration_window or unparsed_documents" -v`
Expected: FAIL — helpers don't exist; `xml_sha256` not on normalized docs.

- [ ] **Step 3: Carry `xml_sha256` through `_normalize_xml_document_row`**

In `assets.py`, update `_normalize_xml_document_row` (~1540) to include `xml_sha256` in the returned dict (keep all existing keys):

```python
    return {
        "business_id": str(row.get("business_id") or ""),
        "financial_date": str(row.get("financial_date") or ""),
        "registration_date": row.get("registration_date"),
        "source_url": str(row.get("source_url") or ""),
        "xml_object_key": xml_object_key,
        "xml_sha256": str(row.get("xml_sha256") or ""),
    }
```

- [ ] **Step 4: Add the filter/skip helpers + import**

Add near the top imports: `from dagster_v3.defs.finland_xbrl.arelle_parser import statement_key_for` (confirm the existing arelle import line and extend it rather than duplicate). Add `from datetime import date` if not already imported.

Add these pure functions (place them near the other module-level helpers, e.g. just above `_finland_xbrl_arelle_resources`):

```python
def documents_in_registration_window(
    documents: list[dict[str, Any]],
    *,
    window_start: date,
    window_end: date,
) -> list[dict[str, Any]]:
    """Keep docs whose registration_date is in [window_start, window_end)."""
    selected: list[dict[str, Any]] = []
    for document in documents:
        raw = document.get("registration_date")
        if not raw:
            continue
        try:
            registered = date.fromisoformat(str(raw)[:10])
        except ValueError:
            continue
        if window_start <= registered < window_end:
            selected.append(document)
    return selected


def unparsed_documents(
    documents: list[dict[str, Any]],
    *,
    parsed_statement_keys: set[str],
) -> list[dict[str, Any]]:
    """Drop docs whose content-addressed statement_key is already parsed."""
    return [
        document
        for document in documents
        if statement_key_for(
            document["business_id"],
            document["financial_date"],
            document.get("xml_sha256", ""),
        )
        not in parsed_statement_keys
    ]
```

- [ ] **Step 5: Run, expect pass**

Run: `uv run pytest tests/test_finland_xbrl_parsed_assets.py -k "registration_window or unparsed_documents" -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets.py tests/test_finland_xbrl_parsed_assets.py
git commit -m "feat(finland_xbrl): carry xml_sha256 + add registration-window/skip helpers"
```

---

### Task 2: DuckDB read/delete helpers for incremental parse

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets.py` (add `load_parsed_statement_keys`, `delete_parsed_company_periods`)
- Test: `tests/test_finland_xbrl_parsed_assets.py`

- [ ] **Step 1: Write failing test** (uses `tmp_path` DuckDB + the existing `_ensure_parsed_duckdb_tables`):

```python
import duckdb
from dagster_v3.defs.common.resources import LocalDuckDBResource


def _resource(tmp_path):
    return LocalDuckDBResource(database_path=str(tmp_path / "finland_ytj.duckdb"))


def test_load_parsed_statement_keys_empty_when_no_table(tmp_path):
    res = _resource(tmp_path)
    assert xbrl.load_parsed_statement_keys(res) == set()


def test_delete_parsed_company_periods_removes_statement_and_facts(tmp_path):
    res = _resource(tmp_path)
    with res.connect() as conn:
        conn.execute(f"create schema if not exists {xbrl.XBRL_DLT_DATASET_NAME}")
        conn.execute(
            f"create table {xbrl.XBRL_DLT_DATASET_NAME}.fi_prh_xbrl_statement_documents "
            "(statement_key varchar, business_id varchar, financial_date varchar)"
        )
        conn.execute(
            f"create table {xbrl.XBRL_DLT_DATASET_NAME}.fi_prh_xbrl_facts_raw "
            "(statement_key varchar, business_id varchar, financial_date varchar)"
        )
        conn.execute(
            f"insert into {xbrl.XBRL_DLT_DATASET_NAME}.fi_prh_xbrl_statement_documents values "
            "('k1','a','2023-12-31'),('k2','b','2023-12-31')"
        )
        conn.execute(
            f"insert into {xbrl.XBRL_DLT_DATASET_NAME}.fi_prh_xbrl_facts_raw values "
            "('k1','a','2023-12-31'),('k2','b','2023-12-31')"
        )
    assert xbrl.load_parsed_statement_keys(res) == {"k1", "k2"}

    xbrl.delete_parsed_company_periods(res, company_periods=[("a", "2023-12-31")])

    with res.connect(read_only=True) as conn:
        s = conn.execute(
            f"select business_id from {xbrl.XBRL_DLT_DATASET_NAME}.fi_prh_xbrl_statement_documents"
        ).fetchall()
        f = conn.execute(
            f"select business_id from {xbrl.XBRL_DLT_DATASET_NAME}.fi_prh_xbrl_facts_raw"
        ).fetchall()
    assert s == [("b",)] and f == [("b",)]
```

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/test_finland_xbrl_parsed_assets.py -k "parsed_statement_keys or company_periods" -v`
Expected: FAIL — helpers missing.

- [ ] **Step 3: Implement the helpers** in `assets.py` (near `parsed_duckdb_row_counts`):

```python
def load_parsed_statement_keys(source_duckdb: LocalDuckDBResource) -> set[str]:
    with source_duckdb.connect(read_only=True) as connection:
        if not _duckdb_table_exists(connection, table=tables.STATEMENT_DOCUMENTS_TABLE):
            return set()
        rows = connection.execute(
            f"select statement_key from {XBRL_DLT_DATASET_NAME}.{tables.STATEMENT_DOCUMENTS_TABLE}"
        ).fetchall()
    return {row[0] for row in rows}


def delete_parsed_company_periods(
    source_duckdb: LocalDuckDBResource,
    *,
    company_periods: list[tuple[str, str]],
) -> None:
    """Remove existing statement+facts rows for these (business_id, financial_date)."""
    if not company_periods:
        return
    with source_duckdb.connect() as connection:
        if not _duckdb_table_exists(connection, table=tables.STATEMENT_DOCUMENTS_TABLE):
            return
        connection.execute(
            "create temp table _reparse_targets(business_id varchar, financial_date varchar)"
        )
        connection.executemany(
            "insert into _reparse_targets values (?, ?)", company_periods
        )
        for table in (tables.STATEMENT_DOCUMENTS_TABLE, tables.FACTS_TABLE):
            connection.execute(
                f"delete from {XBRL_DLT_DATASET_NAME}.{table} t "
                "where exists (select 1 from _reparse_targets r "
                "where r.business_id = t.business_id and r.financial_date = t.financial_date)"
            )
        connection.execute("drop table _reparse_targets")
```

- [ ] **Step 4: Run, expect pass**

Run: `uv run pytest tests/test_finland_xbrl_parsed_assets.py -k "parsed_statement_keys or company_periods" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets.py tests/test_finland_xbrl_parsed_assets.py
git commit -m "feat(finland_xbrl): add parsed-key read + company-period delete helpers"
```

---

### Task 3: Make the dlt parse source consume a pre-selected document list and append

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets.py` (`_finland_xbrl_arelle_resources`, `finland_xbrl_arelle_source`, `run_finland_xbrl_arelle_dlt_pipeline`)
- Test: `tests/test_finland_xbrl_parsed_assets.py`

The source currently loads the manifest itself and uses `replace`. Change it to accept an already-selected `documents` list and use `append` (selection/skip/delete now happen in the asset body, Task 4).

- [ ] **Step 1: Write/adjust failing test** — find the existing test that drives `run_finland_xbrl_arelle_dlt_pipeline` (the file `test_finland_xbrl_parsed_assets.py` already tests the parse). Add a focused test that the pipeline now takes `documents=` and appends across two calls (resumability):

```python
from datetime import UTC, datetime


def test_pipeline_appends_documents_across_calls(tmp_path, monkeypatch):
    res = _resource(tmp_path)

    # a fake object_store returning tiny XML bodies keyed by object key
    class FakeStore:
        def read_bytes(self, key, bucket=None):
            return b"<xbrl/>"

    def fake_parser(*, business_id, financial_date, registration_date, source_url,
                    xml_object_key, source_run_id, body, parsed_at):
        from dagster_v3.defs.finland_xbrl.parser import ParsedStatement  # adapt to real return type
        # build a minimal parsed result with statement_key_for + 1 fact
        ...  # IMPLEMENTER: construct using the real parser's dataclasses; assert shape from parser.py

    doc1 = _doc("a", "2023-12-31", "2024-03-10", "sha-a")
    doc2 = _doc("b", "2023-12-31", "2024-03-11", "sha-b")

    xbrl.run_finland_xbrl_arelle_dlt_pipeline(
        database_path=res.path(), object_store=FakeStore(),
        documents=[doc1], run_id="r1", parser=fake_parser,
    )
    xbrl.run_finland_xbrl_arelle_dlt_pipeline(
        database_path=res.path(), object_store=FakeStore(),
        documents=[doc2], run_id="r2", parser=fake_parser,
    )
    counts = xbrl.parsed_duckdb_row_counts(res)
    assert counts[xbrl.tables.STATEMENT_DOCUMENTS_TABLE] == 2
```

NOTE TO IMPLEMENTER: the file already has a working test that parses real/fixture XML through `run_finland_xbrl_arelle_dlt_pipeline`. Prefer adapting that existing fixture/parser rather than hand-rolling `fake_parser` — reuse whatever `ParsedStatement`/`StatementParseResult` type and fixture the current tests use, so the test is realistic. Drive the new behavior: two calls with different docs → 2 statement rows (append, not replace).

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/test_finland_xbrl_parsed_assets.py -k "appends_documents" -v`
Expected: FAIL — `run_finland_xbrl_arelle_dlt_pipeline` doesn't accept `documents=` / still uses `replace` (second call wipes the first).

- [ ] **Step 3: Refactor `_finland_xbrl_arelle_resources`** to take `documents` (and not load the manifest) and use `append`:

Replace its signature + manifest-loading head so it accepts `documents: list[dict[str, Any]]` instead of `object_store, documents_key` for the manifest load (it still needs `object_store` to read each XML body). Remove the `load_xbrl_document_manifest` call; iterate the passed `documents`. Change BOTH `dlt.resource(...)` calls from `write_disposition="replace"` to `write_disposition="append"` (keep the same `name`/`primary_key`). Keep the parse loop, progress logging, and `_table_row` mapping unchanged.

- [ ] **Step 4: Thread `documents` through `finland_xbrl_arelle_source` and `run_finland_xbrl_arelle_dlt_pipeline`**

- `finland_xbrl_arelle_source(*, object_store, documents, run_id, parser=..., log_info=None, progress_interval=25)` → passes `documents` to `_finland_xbrl_arelle_resources`.
- `run_finland_xbrl_arelle_dlt_pipeline(*, database_path, object_store, documents, run_id, parser=..., log_info=None, progress_interval=25)` → passes `documents`; keep the `pipeline.run(...)` + `_ensure_parsed_duckdb_tables` + log. Drop the `documents_key` param.

- [ ] **Step 5: Run, expect pass**

Run: `uv run pytest tests/test_finland_xbrl_parsed_assets.py -k "appends_documents" -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets.py tests/test_finland_xbrl_parsed_assets.py
git commit -m "feat(finland_xbrl): parse a pre-selected document list with append disposition"
```

---

### Task 4: Partition the parse multi-asset (month filter + skip + delete + pool)

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets.py` (partition def; `finland_xbrl_parsed_tables` body)
- Test: `tests/test_finland_xbrl_parsed_assets.py`

- [ ] **Step 1: Write failing test for registration + lineage**

```python
from dagster_v3.definitions import defs as load_project_defs


def test_parse_asset_is_monthly_partitioned():
    repo = load_project_defs().get_repository_def()
    graph = repo.asset_graph
    from dagster import AssetKey
    node = graph.get(AssetKey(["fi_prh_xbrl_statement_documents"]))
    assert node.partitions_def is not None
    assert type(node.partitions_def).__name__ == "MonthlyPartitionsDefinition"
    # downstream metrics stays unpartitioned
    metrics = graph.get(AssetKey(["fi_prh_xbrl_financial_metrics"]))
    assert metrics.partitions_def is None
```
(Adapt the `partitions_def`/`node` accessor to the Dagster 1.13.9 asset-graph API if it differs; report what you used.)

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/test_finland_xbrl_parsed_assets.py -k "monthly_partitioned" -v`
Expected: FAIL — no partitions_def.

- [ ] **Step 3: Define the partition set** (near the top of `assets.py`, after the config constants):

```python
FI_XBRL_PARSE_PARTITION_START = "2014-01-01"  # earliest registration month in scope; widen if needed
fi_xbrl_parse_partitions = dg.MonthlyPartitionsDefinition(start_date=FI_XBRL_PARSE_PARTITION_START)
```

- [ ] **Step 4: Partition the multi-asset + add the pool, and rewrite its body**

On the `@dg.multi_asset(...)` decorator for `finland_xbrl_parsed_tables`, add `partitions_def=fi_xbrl_parse_partitions` and `pool="finland_ytj_duckdb"`. Add `dg.AssetSpec(...)` unchanged; the partitions_def + pool go on the decorator (not per-spec).

Replace the body:

```python
def finland_xbrl_parsed_tables(
    context: dg.AssetExecutionContext,
    config: XbrlParsedConfig,
    object_store: ObjectStoreResource,
    source_duckdb: LocalDuckDBResource,
) -> Iterator[dg.MaterializeResult]:
    documents_key = resolve_xbrl_documents_key(config=config)
    window = context.partition_time_window
    window_start = window.start.date()
    window_end = window.end.date()

    documents, _meta = load_xbrl_document_manifest(
        object_store=object_store, documents_key=documents_key
    )
    in_window = documents_in_registration_window(
        documents, window_start=window_start, window_end=window_end
    )
    to_parse = unparsed_documents(
        in_window, parsed_statement_keys=load_parsed_statement_keys(source_duckdb)
    )
    context.log.info(
        "XBRL parse partition %s: %d catalog docs, %d in window, %d to parse",
        context.partition_key, len(documents), len(in_window), len(to_parse),
    )

    if to_parse:
        delete_parsed_company_periods(
            source_duckdb,
            company_periods=[(d["business_id"], d["financial_date"]) for d in to_parse],
        )
        run_finland_xbrl_arelle_dlt_pipeline(
            database_path=source_duckdb.path(),
            object_store=object_store,
            documents=to_parse,
            run_id=context.run_id,
            log_info=context.log.info,
        )

    row_counts = parsed_duckdb_row_counts(source_duckdb)
    for table in (tables.STATEMENT_DOCUMENTS_TABLE, tables.FACTS_TABLE):
        yield dg.MaterializeResult(
            asset_key=table,
            metadata={
                "duckdb_schema": XBRL_DLT_DATASET_NAME,
                "duckdb_table": table,
                "partition": context.partition_key,
                "documents_in_window": len(in_window),
                "documents_parsed_this_run": len(to_parse),
                "row_count": row_counts[table],
                "xml_documents_object_key": documents_key,
            },
        )
```

Notes: the observability call `parsed_duckdb_observability_metadata` was per-run over the whole catalog; replace with the per-partition metadata above (counts of in-window/parsed). If other code references `parsed_duckdb_observability_metadata`, leave that function in place (it may still be used elsewhere) — just stop calling it here. Confirm with `rg "parsed_duckdb_observability_metadata"`.

- [ ] **Step 5: Run the partition test + full parse suite**

Run: `uv run pytest tests/test_finland_xbrl_parsed_assets.py -v`
Expected: PASS. If existing tests materialized the asset without a partition, they must now pass a `partition_key` (use `dg.materialize([...], partition_key="2024-03-01", resources=...)` or the file's existing materialization helper). Update those call sites; the parse asset is now partitioned.

- [ ] **Step 6: `dg check defs`**

Run: `uv run dg check defs`
Expected: no errors. The partitioned parse feeding the unpartitioned `financial_metrics` is allowed (default `AllPartitionMapping`); if `dg check` complains about the partition mapping at that boundary, add an explicit `AssetDep(..., partition_mapping=dg.AllPartitionMapping())` on `financial_metrics`'s deps and report it.

- [ ] **Step 7: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets.py tests/test_finland_xbrl_parsed_assets.py
git commit -m "feat(finland_xbrl): partition Arelle parse by registration month, skip already-parsed"
```

---

### Task 5: End-to-end resumability + verification

**Files:**
- Test: `tests/test_finland_xbrl_parsed_assets.py`
- Verify only.

- [ ] **Step 1: Add an end-to-end resumability test** (seed an S3 catalog + fake store with docs across two months; materialize partition `2024-03-01`, assert only March docs parsed; materialize again, assert 0 re-parsed; materialize `2024-04-01`, assert April docs added and March untouched). Adapt to the file's existing object-store fake + materialization helpers:

```python
def test_partition_materialization_is_incremental_and_resumable(tmp_path, monkeypatch):
    # IMPLEMENTER: build a fake ObjectStoreResource holding a parquet catalog with
    # March docs (registration 2024-03-xx) and April docs (2024-04-xx), and XML bodies.
    # Materialize fi_prh_xbrl_statement_documents+facts for partition "2024-03-01":
    #   assert only March statement rows exist.
    # Re-materialize "2024-03-01": assert row count unchanged (all skipped).
    # Materialize "2024-04-01": assert April rows added, March rows still present.
    ...
```

- [ ] **Step 2: Run the parse suite**

Run: `uv run pytest tests/test_finland_xbrl_parsed_assets.py tests/test_finland_xbrl_assets.py -m "not integration" -v`
Expected: PASS.

- [ ] **Step 3: Validate definitions + lineage**

Run: `uv run dg check defs`
Run: `uv run dg list defs --json | python3 -c "import sys,json; d=json.load(sys.stdin); [print(a['asset_key'], a.get('kinds')) for a in d['assets'] if 'xbrl' in a['asset_key'].lower()]"`
Expected: the two parse assets present (now partitioned); the rest unchanged.

- [ ] **Step 4: Final commit if anything adjusted**

```bash
git add -A && git commit -m "test(finland_xbrl): verify incremental monthly parse" || echo "nothing to commit"
```

---

## Self-Review

**Spec coverage:**
- Monthly partition on the parse only → Task 3-4 (`fi_xbrl_parse_partitions`, `partitions_def` on the multi-asset; neighbors untouched).
- Select docs by registration month → `documents_in_registration_window` (Task 1).
- Skip already-parsed by content-addressed key (no XML read) → `unparsed_documents` + `load_parsed_statement_keys` (Task 1-2).
- Clean replacement of corrected filings → `delete_parsed_company_periods` + `append` (Task 2-3).
- Memory bounded to one month → falls out of filtering to the window before parsing.
- R2 pool on this asset → Task 4 (`pool="finland_ytj_duckdb"`).

**Placeholder scan:** Tasks 1-2 + 4 have complete code. Tasks 3 and 5 intentionally defer the fake-parser/object-store fixture construction to the implementer **with explicit instructions to reuse the existing test fixtures** in `test_finland_xbrl_parsed_assets.py` rather than invent them — because the real `ParsedStatement`/parser return types must match and are best copied from the working tests. Flag if the existing tests don't provide a reusable fixture.

**Type/name consistency:** `documents` (list[dict]) threads identically through `run_finland_xbrl_arelle_dlt_pipeline` → `finland_xbrl_arelle_source` → `_finland_xbrl_arelle_resources`; `statement_key_for(business_id, financial_date, xml_sha256)` is used identically in `unparsed_documents` and the parser; `company_periods` is `list[tuple[str,str]]` in both producer (asset body) and consumer (`delete_parsed_company_periods`).

**Risks to verify during execution:**
1. **`context.partition_time_window` semantics** — for `MonthlyPartitionsDefinition`, `window.start`/`window.end` are tz-aware datetimes spanning the month; `.date()` gives `[month-start, next-month-start)`. Confirm against one partition in a test (Task 4 Step 1).
2. **`registration_date` format in the catalog** — assumed ISO `YYYY-MM-DD...`; `documents_in_registration_window` parses the first 10 chars. If PRH uses another format, adjust the parse and report.
3. **Existing parse tests pass a partition_key now** — the asset is partitioned, so any direct `materialize` of it needs `partition_key=` (Task 4 Step 5).
4. **`backfill_policy`** — left default (one run per partition). If a single-run backfill over many months is wanted later, add `backfill_policy=dg.BackfillPolicy.single_run()` and have the body read `context.partition_time_window` over the whole range — out of scope here.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-18-finland-xbrl-parse-checkpoint.md`. Two options:

1. **Subagent-Driven (recommended)** — fresh subagent per task, two-stage review.
2. **Inline Execution** — execute here with checkpoints.

Which approach?
