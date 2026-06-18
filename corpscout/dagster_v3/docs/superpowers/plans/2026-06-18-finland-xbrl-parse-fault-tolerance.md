# Finland XBRL — Parse Fault Tolerance (R5) + Pool Completion (R2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make the monthly-partitioned Arelle parse tolerant of individual bad documents (one un-parseable XML must not fail its whole month), and finish applying the single-writer pool to the remaining DuckDB-touching finland_xbrl assets.

**Architecture (R5):** Today `parse_statement_xml_with_arelle` raises on malformed XML (`etree.fromstring`) or an Arelle model-load crash, and that exception propagates out of the parse loop → the dlt extract fails → **the entire month partition fails**. With monthly partitions, one poison document blocks that month indefinitely. Fix: wrap each document's parse in a try/except — on failure, record the doc, log a warning, and continue. Because a failed doc is never written to `fi_prh_xbrl_statement_documents`, the existing object-key skip (`unparsed_documents`) **automatically retries it on the next run** — no separate retry machinery needed. Surface the failed count in asset metadata + a warning so the gap is observable. The parse loop is extracted into a pure, independently-testable `parse_xbrl_documents(...)` helper that returns `(statement_rows, fact_rows, failed_docs)`; the dlt source is built from the pre-parsed rows; the runner returns the counts.

**Architecture (R2):** Add `pool="finland_ytj_duckdb"` to the three finland_xbrl assets that write the shared `data/finland_ytj.duckdb` and currently lack it: `finland_xbrl_financial_reports_duckdb` (`@dlt_assets`), `finland_xbrl_eligible_financial_reports` (`@dg.asset`), `finland_xbrl_financial_metrics` (`@dg.asset`). The parse asset already has it; the two S3-only assets (`raw_xml_documents`, `xml_documents`) don't touch DuckDB and don't need it. The pool limit is already 1 in the instance.

**Scope:** R5 + R2 only. Out of scope: a `reparse_existing` flag (corrected filings), recording permanently-failed docs to stop infinite retries (auto-retry is acceptable for v1; failures are logged each run), R3/R4.

**Tech Stack:** Dagster 1.13.9, dlt, DuckDB, Arelle/lxml. All present.

**Key existing facts (verified):**
- Parse loop lives in `_finland_xbrl_arelle_resources` (assets.py ~485-568): eagerly parses all `documents` into `statement_rows`/`fact_rows` lists, then returns two `dlt.resource(..., write_disposition="append")`. `run_finland_xbrl_arelle_dlt_pipeline` (~431) → `finland_xbrl_arelle_source` (~465) → `_finland_xbrl_arelle_resources`. The runner currently returns `load_info`; **no test asserts the return** (tests query DuckDB).
- `parse_statement_xml_with_arelle(*, business_id, financial_date, registration_date, source_url, xml_object_key, source_run_id, body, parsed_at) -> ArelleParsedStatement(statement_document, facts, warnings)`. Raises on bad XML / Arelle crash.
- `_table_row(table, row)`, `_should_log_parse_progress`, `_log_parse_progress` exist and stay.
- Asset body `finland_xbrl_parsed_tables` calls `run_finland_xbrl_arelle_dlt_pipeline(..., documents=to_parse, ...)` then builds metadata.
- The 3 pool targets: `@dlt_assets` at assets.py ~294 (financial_reports); `@dg.asset` `finland_xbrl_eligible_financial_reports` (~589) and `finland_xbrl_financial_metrics` (~699). `@dlt_assets` and `@dg.asset` both accept `pool=`.

**Test command:** `uv run pytest tests/test_finland_xbrl_parsed_assets.py tests/test_finland_xbrl_assets.py -m "not integration" -v`

---

### Task 1: R2 — pool the three remaining DuckDB-touching xbrl assets

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets.py` (3 decorators)
- Test: `tests/test_finland_xbrl_assets.py` (or the parsed-assets test file)

- [ ] **Step 1: Write the failing test**

Add (to `tests/test_finland_xbrl_assets.py` — confirm its imports; it likely already loads project defs):

```python
from dagster import AssetKey
from dagster_v3.definitions import defs as load_project_defs


def test_duckdb_xbrl_assets_share_the_finland_ytj_duckdb_pool():
    graph = load_project_defs().get_repository_def().asset_graph
    for key in (
        "finland_xbrl_financial_reports_duckdb",
        "finland_xbrl_eligible_financial_reports",
        "fi_prh_xbrl_statement_documents",
        "fi_prh_xbrl_financial_metrics",
    ):
        node = graph.get(AssetKey([key]))
        assert node.is_executable
        assert "finland_ytj_duckdb" in node.pools  # node.pools is a set[str] of the op's pools
```
If `node.pools` is not the right accessor in 1.13.9, adapt to the correct way to read an asset op's pool(s) and report what you used (do not weaken the intent: these 4 assets are in pool `finland_ytj_duckdb`). The parse asset (`fi_prh_xbrl_statement_documents`) already has the pool — include it to confirm.

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/test_finland_xbrl_assets.py -k "share_the_finland_ytj_duckdb_pool" -v`
Expected: FAIL — financial_reports/eligible/metrics not in the pool.

- [ ] **Step 3: Add `pool="finland_ytj_duckdb"` to the three decorators**

- `@dlt_assets(... name="finland_xbrl_financial_reports_duckdb", dagster_dlt_translator=FinlandXbrlDltTranslator())` → add `pool="finland_ytj_duckdb",`.
- `@dg.asset(name="finland_xbrl_eligible_financial_reports", group_name="finland_xbrl", deps=[...], kinds={...})` → add `pool="finland_ytj_duckdb",`.
- `@dg.asset(name=tables.FINANCIAL_METRICS_TABLE, group_name="finland_xbrl", deps=[...], kinds={...})` → add `pool="finland_ytj_duckdb",`.

- [ ] **Step 4: Run the test + dg check**

Run: `uv run pytest tests/test_finland_xbrl_assets.py -k "share_the_finland_ytj_duckdb_pool" -v` → PASS
Run: `uv run dg check defs` → no errors.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets.py tests/test_finland_xbrl_assets.py
git commit -m "fix(finland_xbrl): pool all duckdb-writing xbrl assets on finland_ytj_duckdb"
```

---

### Task 2: R5 — extract `parse_xbrl_documents` with per-document fault tolerance

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets.py` (`_finland_xbrl_arelle_resources`, `finland_xbrl_arelle_source`, `run_finland_xbrl_arelle_dlt_pipeline`; add `parse_xbrl_documents` + a result type)
- Test: `tests/test_finland_xbrl_parsed_assets.py`

- [ ] **Step 1: Write the failing test (one good doc, one poison doc)**

Add to `tests/test_finland_xbrl_parsed_assets.py` (reuse `FakeS3Client`, `ObjectStoreResource`, `SAMPLE_XML`, `_xbrl_resource`, `_fake_arelle_parser`):

```python
def _raising_parser(**kwargs):
    if kwargs["business_id"] == "bad":
        raise ValueError("boom: unparseable XBRL")
    return _fake_arelle_parser(**kwargs)


def test_parse_skips_bad_document_and_keeps_good_ones(tmp_path):
    s3 = FakeS3Client()
    object_store = ObjectStoreResource(bucket="source-finland-prh-xbrl", s3_client=s3)
    s3.objects[("source-finland-prh-xbrl", "companies/good/2023-12-31.xml")] = SAMPLE_XML
    s3.objects[("source-finland-prh-xbrl", "companies/bad/2023-12-31.xml")] = SAMPLE_XML
    good = {"business_id": "good", "financial_date": "2023-12-31", "registration_date": "2024-03-10",
            "source_url": "", "xml_object_key": "companies/good/2023-12-31.xml"}
    bad = {"business_id": "bad", "financial_date": "2023-12-31", "registration_date": "2024-03-11",
           "source_url": "", "xml_object_key": "companies/bad/2023-12-31.xml"}
    db = tmp_path / "source.duckdb"

    result = run_finland_xbrl_arelle_dlt_pipeline(
        database_path=db, object_store=object_store,
        documents=[good, bad], run_id="r1", parser=_raising_parser,
    )

    assert result.parsed == 1
    assert result.failed == 1
    with duckdb.connect(str(db), read_only=True) as conn:
        keys = [r[0] for r in conn.execute(
            f"select xml_object_key from {XBRL_DLT_DATASET_NAME}.{STATEMENT_DOCUMENTS_TABLE}"
        ).fetchall()]
    assert keys == ["companies/good/2023-12-31.xml"]  # bad doc skipped, not in table → auto-retried next run
```
(Import `run_finland_xbrl_arelle_dlt_pipeline` already imported. Adapt `result.parsed`/`result.failed` to the result type you define in Step 3.)

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/test_finland_xbrl_parsed_assets.py -k "skips_bad_document" -v`
Expected: FAIL — the raise propagates and fails the whole run (no result object / pipeline raises).

- [ ] **Step 3: Add the result type + `parse_xbrl_documents` helper**

Add near the top (after imports) or with the other dataclasses:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class XbrlParseRunResult:
    load_info: Any
    parsed: int
    failed: int
```

Add the pure parse helper (replace the body of `_finland_xbrl_arelle_resources`'s parse loop by moving it here):

```python
def parse_xbrl_documents(
    documents: list[dict[str, Any]],
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    parser: ArelleStatementParser = parse_statement_xml_with_arelle,
    log_info: Callable[[str], None] | None = None,
    progress_interval: int = 25,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Parse each document; on a per-doc error, record it and continue (don't fail the batch)."""
    parsed_at = datetime.now(UTC)
    total_documents = len(documents)
    statement_rows: list[dict[str, Any]] = []
    fact_rows: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    warning_count = 0
    started_at = datetime.now(UTC)
    for document_index, document in enumerate(documents, start=1):
        xml_object_key = document["xml_object_key"]
        try:
            body = object_store.read_bytes(xml_object_key, bucket=XBRL_BUCKET)
            parsed = parser(
                business_id=document["business_id"],
                financial_date=document["financial_date"],
                registration_date=document.get("registration_date"),
                source_url=document.get("source_url", ""),
                xml_object_key=xml_object_key,
                source_run_id=run_id,
                body=body,
                parsed_at=parsed_at,
            )
        except Exception as exc:  # noqa: BLE001 - one bad doc must not fail the month
            failed.append(
                {
                    "xml_object_key": xml_object_key,
                    "business_id": document.get("business_id", ""),
                    "financial_date": document.get("financial_date", ""),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            _log_parse_progress(
                log_info,
                f"Skipping unparseable XBRL document {xml_object_key}: {type(exc).__name__}: {exc}",
            )
            continue
        warning_count += len(parsed.warnings)
        statement_rows.append(_table_row(tables.STATEMENT_DOCUMENTS_TABLE, parsed.statement_document))
        fact_rows.extend(_table_row(tables.FACTS_TABLE, fact) for fact in parsed.facts)
        if _should_log_parse_progress(
            document_index=document_index,
            total_documents=total_documents,
            progress_interval=progress_interval,
        ):
            elapsed = (datetime.now(UTC) - started_at).total_seconds()
            _log_parse_progress(
                log_info,
                f"Parsed XBRL XML document {document_index}/{total_documents}: "
                f"business_id={document['business_id']} facts={len(parsed.facts)} "
                f"elapsed_seconds={elapsed:.1f}",
            )
    elapsed = (datetime.now(UTC) - started_at).total_seconds()
    _log_parse_progress(
        log_info,
        "Parsed XBRL XML documents complete: "
        f"documents={total_documents} statement_rows={len(statement_rows)} "
        f"fact_rows={len(fact_rows)} failed={len(failed)} "
        f"parser_warnings={warning_count} elapsed_seconds={elapsed:.1f}",
    )
    return statement_rows, fact_rows, failed
```
(Preserve the exact progress/complete log substrings the existing tests assert: `"Parsed XBRL XML document {i}/{n}"`, `"Parsed XBRL XML documents complete"`. Match the current wording — read the current loop and keep its log strings; the snippet above is illustrative.)

- [ ] **Step 4: Make the dlt source build from pre-parsed rows**

Replace `finland_xbrl_arelle_source` and `_finland_xbrl_arelle_resources` to take `statement_rows`/`fact_rows` instead of `documents`:

```python
@dlt.source(name="finland_xbrl_arelle")
def finland_xbrl_arelle_source(
    *,
    statement_rows: list[dict[str, Any]],
    fact_rows: list[dict[str, Any]],
) -> list[DltResource]:
    return _finland_xbrl_arelle_resources(statement_rows=statement_rows, fact_rows=fact_rows)


def _finland_xbrl_arelle_resources(
    *,
    statement_rows: list[dict[str, Any]],
    fact_rows: list[dict[str, Any]],
) -> list[DltResource]:
    return [
        dlt.resource(
            statement_rows,
            name=tables.STATEMENT_DOCUMENTS_TABLE,
            write_disposition="append",
            primary_key="statement_key",
        ),
        dlt.resource(
            fact_rows,
            name=tables.FACTS_TABLE,
            write_disposition="append",
            primary_key=("statement_key", "fact_ordinal"),
        ),
    ]
```

- [ ] **Step 5: Orchestrate in the runner + return the result**

Rewrite `run_finland_xbrl_arelle_dlt_pipeline`:

```python
def run_finland_xbrl_arelle_dlt_pipeline(
    *,
    database_path: str | Path,
    object_store: ObjectStoreResource,
    documents: list[dict[str, Any]],
    run_id: str,
    parser: ArelleStatementParser = parse_statement_xml_with_arelle,
    log_info: Callable[[str], None] | None = None,
    progress_interval: int = 25,
) -> XbrlParseRunResult:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    _log_parse_progress(log_info, f"Parsing {len(documents)} XBRL XML documents")
    statement_rows, fact_rows, failed = parse_xbrl_documents(
        documents,
        object_store=object_store,
        run_id=run_id,
        parser=parser,
        log_info=log_info,
        progress_interval=progress_interval,
    )
    pipeline = dlt.pipeline(
        pipeline_name="finland_xbrl_arelle_parsed_tables",
        destination=dlt.destinations.duckdb(str(database_file)),
        dataset_name=XBRL_DLT_DATASET_NAME,
        dev_mode=False,
    )
    load_info = pipeline.run(
        finland_xbrl_arelle_source(statement_rows=statement_rows, fact_rows=fact_rows)
    )
    _ensure_parsed_duckdb_tables(database_file)
    if log_info is not None:
        log_info("dlt loaded parsed XBRL tables into DuckDB")
    return XbrlParseRunResult(load_info=load_info, parsed=len(statement_rows), failed=len(failed))
```

- [ ] **Step 6: Run the parse suite**

Run: `uv run pytest tests/test_finland_xbrl_parsed_assets.py -v`
Expected: PASS — the new bad-doc test plus all existing tests (which call the runner and query DuckDB; the return type changed but they don't assert it). If a test asserted the old `load_info` return, update it to `.load_info`.

- [ ] **Step 7: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets.py tests/test_finland_xbrl_parsed_assets.py
git commit -m "feat(finland_xbrl): tolerate per-document parse failures (skip + record, auto-retry)"
```

---

### Task 3: Surface failed-doc count in the asset metadata + warning

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets.py` (`finland_xbrl_parsed_tables` body)
- Test: `tests/test_finland_xbrl_parsed_assets.py`

- [ ] **Step 1: Update the asset body to consume the result**

In `finland_xbrl_parsed_tables`, capture the run result and surface `documents_failed_this_run`:

```python
    failed_this_run = 0
    if to_parse:
        result = run_finland_xbrl_arelle_dlt_pipeline(
            database_path=source_duckdb.path(),
            object_store=object_store,
            documents=to_parse,
            run_id=context.run_id,
            log_info=context.log.info,
        )
        failed_this_run = result.failed
        if failed_this_run:
            context.log.warning(
                "XBRL parse partition %s: %d documents failed to parse and were skipped "
                "(will retry next run)",
                context.partition_key, failed_this_run,
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
                "documents_failed_this_run": failed_this_run,
                "documents_missing_registration_date": missing_registration,
                "total_table_row_count": row_counts[table],
                "xml_documents_object_key": documents_key,
            },
        )
```
(Keep the existing `missing_registration` computation and the window/skip lines above unchanged.)

- [ ] **Step 2: Add a test that the asset metadata reports failures** — OR (simpler, since materializing the partitioned asset is heavy) assert via the composition the body uses, mirroring the existing E2E test: run with a poison doc and assert `result.failed == 1` and the good rows landed. If the bad-doc test from Task 2 already covers the runner result, add only a light assertion that the asset body path compiles/works (e.g. `dg check defs`). Prefer extending the existing E2E-style helper test with a poison doc rather than spinning up a Dagster materialization.

Run: `uv run pytest tests/test_finland_xbrl_parsed_assets.py -v` → PASS.

- [ ] **Step 3: `dg check defs`** → no errors.

- [ ] **Step 4: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets.py tests/test_finland_xbrl_parsed_assets.py
git commit -m "feat(finland_xbrl): surface documents_failed_this_run in parse metadata"
```

---

### Task 4: Verification

- [ ] **Step 1:** `uv run pytest tests/test_finland_xbrl_parsed_assets.py tests/test_finland_xbrl_assets.py -m "not integration" -v` → PASS.
- [ ] **Step 2:** `uv run dg check defs` → no errors.
- [ ] **Step 3:** Confirm pools: `uv run dg list defs --json | python3 -c "import sys,json; d=json.load(sys.stdin); [print(a['asset_key'], a.get('pools')) for a in d['assets'] if 'xbrl' in a['asset_key'].lower()]"` (or however pools surface) — the 4 duckdb assets carry `finland_ytj_duckdb`.
- [ ] **Step 4:** Final commit if anything adjusted.

---

## Self-Review

**Spec coverage:**
- R5 per-doc fault tolerance → Task 2 (`parse_xbrl_documents` try/except; failed docs recorded, not written → auto-retried via the object-key skip).
- R5 observability → Task 2 (per-doc + summary logs) + Task 3 (`documents_failed_this_run` metadata + warning).
- R2 pool completion → Task 1 (3 assets).

**Placeholder scan:** Task 2's `parse_xbrl_documents` log strings are illustrative — the implementer is told to preserve the EXACT current progress/complete log substrings the existing tests assert. That's a real instruction, not a placeholder, because the current wording must be read from the live code.

**Type/name consistency:** `XbrlParseRunResult(load_info, parsed, failed)` returned by the runner and consumed in the asset body (`result.failed`) and the Task 2 test (`result.parsed`/`result.failed`); `finland_xbrl_arelle_source(*, statement_rows, fact_rows)` matches `_finland_xbrl_arelle_resources`; the runner still takes `documents=` (callers/tests unchanged).

**Risks to verify during execution:**
1. **Existing parse tests' log assertions** — Task 2 must preserve `"Parsed XBRL XML document i/n"`, `"...complete"`, `"Parsing N XBRL XML documents"` exactly (read current strings).
2. **`node.pools` accessor** for the pool test (Task 1) — adapt to the real 1.13.9 API; report what was used.
3. **Result-type change** — confirm no existing test asserts the old `load_info` return directly; update to `.load_info` if so.
4. **Auto-retry of a persistently-bad doc** — it will be re-attempted (and re-logged) every run forever; acceptable for v1, noted as a future enhancement (record permanent failures to stop retrying).

---

## Execution Handoff

Plan complete and saved. Two options:

1. **Subagent-Driven (recommended)** — fresh subagent per task, two-stage review.
2. **Inline Execution** — execute here with checkpoints.

Which approach?
