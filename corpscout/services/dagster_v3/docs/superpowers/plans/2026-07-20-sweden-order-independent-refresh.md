# Sweden Order-Independent Weekly Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Sweden financial weekly refresh and the yearly (per-year) backfill fully order-independent: the user can materialize either, in any interleaving, and every run succeeds — no clash class exists.

**Architecture:** The weekly "current" chain loses its weekly partitions (they exist only to give each week an identity for export-scope bookkeeping — the exact state the yearly rebuild wipes). The five current assets become non-partitioned: sync → catalog → parse stay run-correlated (unchanged mechanics), and the two ClickHouse exports become **reconcilers** — they diff the local 2026 DuckDB year file against ClickHouse per `source_archive_key` (row counts, not just presence) and upsert exactly the missing/mismatched archives; an empty diff is a clean no-op. The yearly backfill chain (partitions 2020–2026, full re-parse + full-year-scope upsert) is untouched. Order-independence becomes structural: yearly-after-weekly is an idempotent superset upsert (already true); weekly-after-yearly finds either nothing to do or exactly the gap, never a missing ledger.

**Tech Stack:** dagster_v3 (Python 3.14, `uv run`), DuckDB per-year files, clickhouse-driver, pytest with the existing `StatefulFakeClickHouseClient` fake.

## Global Constraints

- All work in `corpscout/services/dagster_v3/`. Always `uv run` for `dg`/`pytest`.
- Commit by explicit path only (working tree carries unrelated WIP); never `git add -A`. Commit trailer: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- Gates before each commit: the named test files green + `uv run dg check defs` clean.
- Assets stay SEPARATE (user requirement) — no folding steps into one asset; composition happens via job selection.
- The backfill (yearly) chain — assets, partitions `2020`–`2026`, `upsert_*_partition` backfill branch, `sweden_financial_backfill_job`, `sweden_financial_backfill_clickhouse_job` — is UNTOUCHED except where noted (removing the now-dead weekly branch from shared helpers).
- ClickHouse `IN` scopes: archive keys (≤ ~300 strings) bind as `Array(String)` params; statement keys (up to ~500k) go through the existing Memory stage-table mechanism. Never interpolate keys into SQL sent to ClickHouse.
- Empty-file guard stays: exporting from a year file with zero reports rows must still raise (that is corruption, not an ordering artifact).
- `pool=SWEDEN_FINANCIAL_CURRENT_DUCKDB_POOL` ("sweden_financial_current_2026_duckdb") stays on every current asset that touches the 2026 file (catalog, parse, both exports) — single-writer DuckDB discipline.
- No `from __future__ import annotations` in asset modules.
- Ops gotcha (Task 4 documents it): cancel any in-flight backfills targeting the current assets BEFORE deploying a `partitions_def` removal (CLAUDE.md: stale queued partition runs fail with `RUN_EXCEPTION` and can leak pool slots).

## Ground truth (verified 2026-07-20)

- `archive_sync_catalog` (local DuckDB, `archive_state.py:173-194`): 14 columns, NO timestamp column; created via `create table if not exists`; inserts are positional `values (?, ... ?)`.
- Current-chain run correlation: `sweden_financial_current_report_xhtml_catalog_duckdb` records the sync with `source_run_id=context.run_id` and parse resolves `changed_sweden_financial_archive_keys_for_run(source_run_id=context.run_id)` / `catalog_source_run_id=context.run_id` — all steps of one job run share `run_id`, so in-run correlation works and is kept.
- Manifest S3 keys (`archive_state.py:16-22`) embed `load_partition_key`; the current chain passes `context.partition_key` today.
- `resolve_sweden_financial_partition_archive_keys` (`clickhouse.py:163+`) has a weekly branch (catalog lookup by `load_partition_key`) that becomes dead; the backfill branch (whole-file distinct) stays.
- `_facts_scope_where_sql(partition_key, archive_keys)` (`clickhouse.py:423`) branches on `_is_backfill_partition_key`; the weekly branch body (filter facts through local reports by archive keys) is exactly what the reconciler needs.
- Reports table has `statement_key`, `source_archive_key`, `facts_count` columns both locally and in CH; facts link to reports via `statement_key` only.
- `StatefulFakeClickHouseClient` (`tests/test_sweden_financial_clickhouse.py:65+`) keeps real per-table row state and supports the count/delete/stage/insert SQL shapes; new query shapes used by the reconciler must be added to it (Task 1 does this).
- `_seed_year_file` test helper (`tests/test_sweden_financial_clickhouse.py:897+`) seeds reports/facts/catalog rows.
- The schedule `sweden_financial_current_year_weekly` uses `execution_fn=_current_year_run_request` solely to map the run date to a weekly partition key — with no partitions it becomes a plain `ScheduleDefinition`.

---

### Task 1: Reconciliation scope resolvers in `clickhouse.py`

**Files:**
- Modify: `src/dagster_v3/defs/sweden_financial/clickhouse.py`
- Test: `tests/test_sweden_financial_clickhouse.py`

**Interfaces:**
- Produces: `resolve_unreconciled_report_archive_keys(duckdb_connection, clickhouse_client, *, log=None) -> list[str]` — archives whose per-archive **reports** row count differs between the local year file and CH (including absent-in-CH). Sorted, deduped.
- Produces: `resolve_unreconciled_facts_archive_keys(duckdb_connection, clickhouse_client, *, log=None) -> list[str]` — same, for **facts** counted per archive via the `statement_key -> reports` join on both sides.
- Both return `[]` when everything matches; both raise `ValueError("Sweden financial year file has zero report rows ...")` if the local reports table is empty (corruption guard, mirrors the existing empty-scope rule).
- The fake CH client learns two new query shapes (regex-matched like the existing ones):
  - `SELECT source_archive_key, count() FROM corpscout.se_financial_reports WHERE source_archive_key IN %(keys)s GROUP BY source_archive_key`
  - `SELECT r.source_archive_key, count() FROM corpscout.se_financial_facts AS f INNER JOIN corpscout.se_financial_reports AS r ON f.statement_key = r.statement_key WHERE r.source_archive_key IN %(keys)s GROUP BY r.source_archive_key`

- [ ] **Step 1: Write the failing tests**

Append to the "scope resolution" section of `tests/test_sweden_financial_clickhouse.py` (imports: add `resolve_unreconciled_report_archive_keys`, `resolve_unreconciled_facts_archive_keys` to the existing `from dagster_v3.defs.sweden_financial.clickhouse import (...)` block):

```python
def test_reconcile_reports_scope_is_missing_and_mismatched_archives(
    tmp_path: Path,
) -> None:
    # W1 fully in CH (match), W2 absent from CH, 2025_A present with a
    # WRONG count (1 local row seeded twice in CH) -> scope is W2 + 2025_A.
    _seed_year_file(
        tmp_path,
        "2026",
        reports=(
            ("5560000001", _ARCHIVE_KEY_2026_W1),
            ("5560000002", _ARCHIVE_KEY_2026_W2),
            ("5560000003", _ARCHIVE_KEY_2025_A),
        ),
    )
    client = _reports_facts_fake_client()
    client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE].extend(
        [
            _clickhouse_report_row(
                company_id="5560000001",
                year="2026",
                archive_key=_ARCHIVE_KEY_2026_W1,
            ),
            _clickhouse_report_row(
                company_id="5560000003",
                year="2026",
                archive_key=_ARCHIVE_KEY_2025_A,
            ),
            _clickhouse_report_row(
                company_id="5560000003",
                year="2026",
                archive_key=_ARCHIVE_KEY_2025_A,
            ),
        ]
    )

    with sweden_financial_year_duckdb_connection("2026", root=tmp_path) as connection:
        keys = resolve_unreconciled_report_archive_keys(connection, client)

    assert keys == sorted([_ARCHIVE_KEY_2025_A, _ARCHIVE_KEY_2026_W2])


def test_reconcile_reports_scope_empty_when_clickhouse_matches(
    tmp_path: Path,
) -> None:
    _seed_year_file(
        tmp_path, "2026", reports=(("5560000001", _ARCHIVE_KEY_2026_W1),)
    )
    client = _reports_facts_fake_client()
    client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE].append(
        _clickhouse_report_row(
            company_id="5560000001",
            year="2026",
            archive_key=_ARCHIVE_KEY_2026_W1,
        )
    )

    with sweden_financial_year_duckdb_connection("2026", root=tmp_path) as connection:
        keys = resolve_unreconciled_report_archive_keys(connection, client)

    assert keys == []


def test_reconcile_reports_scope_raises_on_empty_year_file(
    tmp_path: Path,
) -> None:
    _seed_year_file(tmp_path, "2026", reports=())
    client = _reports_facts_fake_client()

    with sweden_financial_year_duckdb_connection("2026", root=tmp_path) as connection:
        with pytest.raises(ValueError, match="zero report rows"):
            resolve_unreconciled_report_archive_keys(connection, client)


def test_reconcile_facts_scope_counts_facts_per_archive(
    tmp_path: Path,
) -> None:
    # Local: one fact for W1's report and one for W2's. CH: W1's fact
    # present, W2's absent -> facts scope is W2 only, even though CH's
    # REPORTS table already has both (reports diff would be empty).
    _seed_year_file(
        tmp_path,
        "2026",
        reports=(
            ("5560000001", _ARCHIVE_KEY_2026_W1),
            ("5560000002", _ARCHIVE_KEY_2026_W2),
        ),
        facts=("5560000001", "5560000002"),
    )
    client = _reports_facts_fake_client()
    for company_id, archive_key in (
        ("5560000001", _ARCHIVE_KEY_2026_W1),
        ("5560000002", _ARCHIVE_KEY_2026_W2),
    ):
        client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE].append(
            _clickhouse_report_row(
                company_id=company_id, year="2026", archive_key=archive_key
            )
        )
    client.rows[QUALIFIED_SE_FINANCIAL_FACTS_TABLE].append(
        _clickhouse_fact_row(company_id="5560000001", year="2026")
    )

    with sweden_financial_year_duckdb_connection("2026", root=tmp_path) as connection:
        keys = resolve_unreconciled_facts_archive_keys(connection, client)

    assert keys == [_ARCHIVE_KEY_2026_W2]
```

Implementation notes for the test author:
- `_clickhouse_report_row` / `_clickhouse_fact_row` already exist in this test file (used by the upsert tests); reuse them. `_clickhouse_report_row` must produce the `statement_key` the seeded local report generates for the same `(company_id, year)` — check `_insert_report` in the helper section; if the statement keys are derived deterministically from company_id+year (they are — read `_insert_report`), reuse that derivation. If `_clickhouse_fact_row` does not currently exist under that exact name, find the fact-row builder used by the facts upsert tests and use its actual name (adjust the tests above accordingly — the name in this plan is the expected one, verify at implementation time).

- [ ] **Step 2: Extend the fake CH client to answer the two GROUP BY shapes**

In `StatefulFakeClickHouseClient.execute`, add two regexes at module level:

```python
_REPORTS_ARCHIVE_COUNT_SQL_RE = re.compile(
    r"^SELECT source_archive_key, count\(\) FROM (\S+) "
    r"WHERE source_archive_key IN %\(keys\)s GROUP BY source_archive_key$"
)
_FACTS_ARCHIVE_COUNT_SQL_RE = re.compile(
    r"^SELECT r\.source_archive_key, count\(\) FROM (\S+) AS f "
    r"INNER JOIN (\S+) AS r ON f\.statement_key = r\.statement_key "
    r"WHERE r\.source_archive_key IN %\(keys\)s GROUP BY r\.source_archive_key$"
)
```

and matching branches in `execute` (before the final `raise AssertionError`):

```python
        reports_count_match = _REPORTS_ARCHIVE_COUNT_SQL_RE.match(normalized)
        if reports_count_match is not None:
            table = reports_count_match.group(1)
            key_index = self.columns[table].index("source_archive_key")
            counts: dict[str, int] = {}
            for row in self.rows[table]:
                key = row[key_index]
                if key in params["keys"]:
                    counts[key] = counts.get(key, 0) + 1
            return sorted(counts.items())
        facts_count_match = _FACTS_ARCHIVE_COUNT_SQL_RE.match(normalized)
        if facts_count_match is not None:
            facts_table, reports_table = facts_count_match.groups()
            statement_index = self.columns[reports_table].index("statement_key")
            archive_index = self.columns[reports_table].index("source_archive_key")
            statement_to_archive = {
                row[statement_index]: row[archive_index]
                for row in self.rows[reports_table]
                if row[archive_index] in params["keys"]
            }
            fact_statement_index = self.columns[facts_table].index("statement_key")
            counts = {}
            for row in self.rows[facts_table]:
                archive = statement_to_archive.get(row[fact_statement_index])
                if archive is not None:
                    counts[archive] = counts.get(archive, 0) + 1
            return sorted(counts.items())
```

- [ ] **Step 3: Run the new tests, verify they fail on the missing functions**

Run: `uv run pytest tests/test_sweden_financial_clickhouse.py -k reconcile -v`
Expected: FAIL / ERROR with `ImportError` (`resolve_unreconciled_report_archive_keys` not defined).

- [ ] **Step 4: Implement the resolvers in `clickhouse.py`**

Place after `resolve_sweden_financial_partition_archive_keys`:

```python
def _local_report_counts_by_archive(duckdb_connection: Any) -> dict[str, int]:
    rows = duckdb_connection.execute(
        f"select source_archive_key, count(*) "
        f"from {SWEDEN_FINANCIAL_DATASET_NAME}.reports group by 1"
    ).fetchall()
    return {str(key): int(count) for key, count in rows}


def _local_facts_counts_by_archive(duckdb_connection: Any) -> dict[str, int]:
    rows = duckdb_connection.execute(
        f"""
        select r.source_archive_key, count(*)
        from {SWEDEN_FINANCIAL_DATASET_NAME}.facts f
        join {SWEDEN_FINANCIAL_DATASET_NAME}.reports r using (statement_key)
        group by 1
        """
    ).fetchall()
    return {str(key): int(count) for key, count in rows}


def _diff_against_clickhouse_counts(
    *,
    local_counts: dict[str, int],
    clickhouse_rows: Sequence[tuple[Any, Any]],
) -> list[str]:
    clickhouse_counts = {str(key): int(count) for key, count in clickhouse_rows}
    return sorted(
        key
        for key, count in local_counts.items()
        if clickhouse_counts.get(key, 0) != count
    )


def _require_nonempty_local_reports(local_counts: dict[str, int]) -> None:
    if not local_counts:
        raise ValueError(
            "Sweden financial year file has zero report rows; refusing to "
            "reconcile from an empty local file -- re-materialize the parse "
            "assets on this host first."
        )


def resolve_unreconciled_report_archive_keys(
    duckdb_connection: Any,
    clickhouse_client: Any,
    *,
    log: Callable[..., object] | None = None,
) -> list[str]:
    """Archives whose local ``reports`` row count differs from ClickHouse.

    The order-independence mechanism (2026-07-20 design): the weekly export
    carries NO bookkeeping of its own -- it diffs the local year file
    against the target table and upserts exactly the difference, so a
    yearly rebuild (which replaces the whole year file) can never strand
    it. An empty diff is the legitimate "ClickHouse already has
    everything" no-op.
    """
    local_counts = _local_report_counts_by_archive(duckdb_connection)
    _require_nonempty_local_reports(local_counts)
    clickhouse_rows = clickhouse_client.execute(
        f"SELECT source_archive_key, count() "
        f"FROM {QUALIFIED_SE_FINANCIAL_REPORTS_TABLE} "
        f"WHERE source_archive_key IN %(keys)s GROUP BY source_archive_key",
        {"keys": tuple(sorted(local_counts))},
    )
    keys = _diff_against_clickhouse_counts(
        local_counts=local_counts, clickhouse_rows=clickhouse_rows
    )
    if log is not None:
        log(
            "Sweden reports reconcile scope: local_archives=%s unreconciled=%s",
            len(local_counts),
            len(keys),
        )
    return keys


def resolve_unreconciled_facts_archive_keys(
    duckdb_connection: Any,
    clickhouse_client: Any,
    *,
    log: Callable[..., object] | None = None,
) -> list[str]:
    """Archives whose per-archive FACTS count (via the ``statement_key`` ->
    ``reports`` join on both sides) differs from ClickHouse. Computed
    independently of the reports diff so a facts-only gap (e.g. a reports
    export that succeeded while the facts export failed) is still found.
    """
    report_counts = _local_report_counts_by_archive(duckdb_connection)
    _require_nonempty_local_reports(report_counts)
    local_counts = _local_facts_counts_by_archive(duckdb_connection)
    clickhouse_rows = clickhouse_client.execute(
        f"SELECT r.source_archive_key, count() "
        f"FROM {QUALIFIED_SE_FINANCIAL_FACTS_TABLE} AS f "
        f"INNER JOIN {QUALIFIED_SE_FINANCIAL_REPORTS_TABLE} AS r "
        f"ON f.statement_key = r.statement_key "
        f"WHERE r.source_archive_key IN %(keys)s "
        f"GROUP BY r.source_archive_key",
        {"keys": tuple(sorted(report_counts))},
    )
    keys = _diff_against_clickhouse_counts(
        local_counts=local_counts, clickhouse_rows=clickhouse_rows
    )
    if log is not None:
        log(
            "Sweden facts reconcile scope: local_archives=%s unreconciled=%s",
            len(local_counts),
            len(keys),
        )
    return keys
```

Note the facts diff iterates `local_counts` (facts side) — an archive with reports but zero local facts is legal (no facts extracted) and must NOT appear in the scope just because CH also has zero.

- [ ] **Step 5: Run the tests, verify pass**

Run: `uv run pytest tests/test_sweden_financial_clickhouse.py -k reconcile -v`
Expected: 4 PASS. Then the whole file: `uv run pytest tests/test_sweden_financial_clickhouse.py -q` — all pass.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/sweden_financial/clickhouse.py tests/test_sweden_financial_clickhouse.py
git commit -m "feat(dagster): sweden reconciliation scope resolvers (local vs clickhouse diff)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Reconciling export functions; retire the weekly ledger branch

**Files:**
- Modify: `src/dagster_v3/defs/sweden_financial/clickhouse.py`
- Test: `tests/test_sweden_financial_clickhouse.py`

**Interfaces:**
- Consumes: Task 1's `resolve_unreconciled_report_archive_keys` / `resolve_unreconciled_facts_archive_keys`.
- Produces: `reconcile_sweden_financial_reports_clickhouse(duckdb_connection, clickhouse, log=None) -> dict[str, str | int]` and `reconcile_sweden_financial_facts_clickhouse(...)` — metadata `{"archives": int, "deleted": int, "inserted": int}` plus `"skipped_reason": "clickhouse already matches the local year file"` when the diff is empty.
- Removes: the weekly (`else`) branch of `resolve_sweden_financial_partition_archive_keys`, `_is_backfill_partition_key`, `_BACKFILL_PARTITION_KEY_PATTERN`, `_quiet_week_skip_metadata`, and the quiet-week early-returns in the two `upsert_*_partition` functions. `resolve_sweden_financial_partition_archive_keys(duckdb_connection)` loses its `partition_key` parameter entirely (rename to `resolve_sweden_financial_year_archive_keys(duckdb_connection)` — it is now "the whole year file's distinct archives", used only by the backfill path). `_facts_scope_where_sql(partition_key, archive_keys)` becomes `_facts_scope_where_sql(archive_keys)` returning the reports-join filter, and the backfill upserts pass `where_sql=None` explicitly.

- [ ] **Step 1: Write the failing tests**

```python
def test_reconcile_reports_upserts_only_the_diff(
    tmp_path: Path,
    monkeypatch,
) -> None:
    # W1 already in CH and matching -> untouched; W2 missing -> upserted.
    _seed_year_file(
        tmp_path,
        "2026",
        reports=(
            ("5560000001", _ARCHIVE_KEY_2026_W1),
            ("5560000002", _ARCHIVE_KEY_2026_W2),
        ),
    )
    client = _reports_facts_fake_client()
    w1_row = _clickhouse_report_row(
        company_id="5560000001", year="2026", archive_key=_ARCHIVE_KEY_2026_W1
    )
    client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE].append(w1_row)

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2026", root=tmp_path
        ) as connection:
            metadata = reconcile_sweden_financial_reports_clickhouse(
                duckdb_connection=connection,
                clickhouse=resource,
            )

    assert metadata == {"archives": 1, "deleted": 0, "inserted": 1}
    assert len(client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE]) == 2
    assert w1_row in client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE]
    # W1 was never deleted or re-sent.
    delete_ops = [
        params
        for sql, params in client.operations
        if sql.startswith("ALTER TABLE")
    ]
    assert delete_ops == []  # W2 was absent -> pre-count 0 -> pure insert


def test_reconcile_reports_noop_when_clickhouse_matches(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_year_file(
        tmp_path, "2026", reports=(("5560000001", _ARCHIVE_KEY_2026_W1),)
    )
    client = _reports_facts_fake_client()
    client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE].append(
        _clickhouse_report_row(
            company_id="5560000001", year="2026", archive_key=_ARCHIVE_KEY_2026_W1
        )
    )

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2026", root=tmp_path
        ) as connection:
            metadata = reconcile_sweden_financial_reports_clickhouse(
                duckdb_connection=connection,
                clickhouse=resource,
            )

    assert metadata == {
        "archives": 0,
        "deleted": 0,
        "inserted": 0,
        "skipped_reason": "clickhouse already matches the local year file",
    }
    assert not any(
        sql.startswith(("ALTER TABLE", "INSERT INTO"))
        for sql in client.statements()
    )


def test_reconcile_facts_upserts_only_the_diff(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_year_file(
        tmp_path,
        "2026",
        reports=(
            ("5560000001", _ARCHIVE_KEY_2026_W1),
            ("5560000002", _ARCHIVE_KEY_2026_W2),
        ),
        facts=("5560000001", "5560000002"),
    )
    client = _reports_facts_fake_client()
    for company_id, archive_key in (
        ("5560000001", _ARCHIVE_KEY_2026_W1),
        ("5560000002", _ARCHIVE_KEY_2026_W2),
    ):
        client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE].append(
            _clickhouse_report_row(
                company_id=company_id, year="2026", archive_key=archive_key
            )
        )
    w1_fact = _clickhouse_fact_row(company_id="5560000001", year="2026")
    client.rows[QUALIFIED_SE_FINANCIAL_FACTS_TABLE].append(w1_fact)

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2026", root=tmp_path
        ) as connection:
            metadata = reconcile_sweden_financial_facts_clickhouse(
                duckdb_connection=connection,
                clickhouse=resource,
            )

    assert metadata == {"archives": 1, "deleted": 0, "inserted": 1}
    assert w1_fact in client.rows[QUALIFIED_SE_FINANCIAL_FACTS_TABLE]
    assert len(client.rows[QUALIFIED_SE_FINANCIAL_FACTS_TABLE]) == 2


def test_reconcile_is_idempotent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_year_file(
        tmp_path, "2026", reports=(("5560000001", _ARCHIVE_KEY_2026_W1),)
    )
    client = _reports_facts_fake_client()

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2026", root=tmp_path
        ) as connection:
            first = reconcile_sweden_financial_reports_clickhouse(
                duckdb_connection=connection,
                clickhouse=resource,
            )
            second = reconcile_sweden_financial_reports_clickhouse(
                duckdb_connection=connection,
                clickhouse=resource,
            )

    assert first["inserted"] == 1
    assert second["skipped_reason"] == (
        "clickhouse already matches the local year file"
    )
    assert len(client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE]) == 1
```

Also UPDATE existing tests in this file that are now obsolete:
- `test_resolve_current_partition_archive_keys_uses_archive_sync_catalog`, `test_resolve_current_partition_returns_empty_when_synced_but_nothing_new`, `test_resolve_partition_archive_keys_raises_when_weekly_partition_not_synced`, `test_upsert_reports_partition_skips_cleanly_on_quiet_week`, `test_upsert_facts_partition_skips_cleanly_on_quiet_week`: DELETE (the ledger mechanism they pin no longer exists).
- Backfill resolve tests: change calls from `resolve_sweden_financial_partition_archive_keys(connection, "2025")` to `resolve_sweden_financial_year_archive_keys(connection)` and keep the assertions; the raises-when-empty test keeps `pytest.raises(ValueError, match="No Sweden financial archive keys")`.
- Any `upsert_*_partition(..., partition_key="2025")` calls keep working — the backfill upserts keep their `partition_key` parameter for metadata/labeling but stop consulting `_is_backfill_partition_key` (see Step 3).

- [ ] **Step 2: Run tests, verify the new ones fail on ImportError**

Run: `uv run pytest tests/test_sweden_financial_clickhouse.py -k reconcile -v`
Expected: FAIL/ERROR (`reconcile_sweden_financial_reports_clickhouse` not defined).

- [ ] **Step 3: Implement**

In `clickhouse.py`:

1. Delete `_BACKFILL_PARTITION_KEY_PATTERN`, `_is_backfill_partition_key`, `_quiet_week_skip_metadata`.
2. Replace `resolve_sweden_financial_partition_archive_keys(duckdb_connection, partition_key)` with:

```python
def resolve_sweden_financial_year_archive_keys(
    duckdb_connection: Any,
) -> list[str]:
    """Every distinct ``source_archive_key`` in the local year file's
    ``reports`` table -- the backfill export scope ("the whole file"; the
    parse layer replaces the entire year file for backfill partitions).

    Raises ``ValueError`` if the file holds no rows at all (corruption /
    parse never ran -- never export nothing silently).
    """
    rows = duckdb_connection.execute(
        f"select distinct source_archive_key "
        f"from {SWEDEN_FINANCIAL_DATASET_NAME}.reports"
    ).fetchall()
    archive_keys = sorted({str(row[0]) for row in rows})
    if not archive_keys:
        raise ValueError(
            "No Sweden financial archive keys found in the local year file. "
            "The reports table is empty -- materialize the matching parse "
            "asset on this host before exporting."
        )
    return archive_keys
```

3. In `upsert_sweden_financial_reports_partition` / `upsert_sweden_financial_facts_partition`: replace the `resolve_sweden_financial_partition_archive_keys(duckdb_connection, partition_key)` call with `resolve_sweden_financial_year_archive_keys(duckdb_connection)`; delete the `if not archive_keys: return _quiet_week_skip_metadata(...)` lines; in the facts upsert change `_facts_scope_where_sql(partition_key, archive_keys)` to `where_sql=None` (backfill scope is the whole file — that is what the old backfill branch produced). Update both docstrings: they are now backfill-year-only exporters.
4. Change `_facts_scope_where_sql` to:

```python
def _facts_scope_where_sql(archive_keys: Sequence[str]) -> str:
    """DuckDB-side facts filter for an archive-scoped (reconcile) export:
    facts joined through the local ``reports`` table by archive keys (at
    most a few hundred inlined literals) -- never by inlined statement
    keys."""
    return (
        "statement_key in (select statement_key "
        f"from {SWEDEN_FINANCIAL_DATASET_NAME}.reports "
        f"where source_archive_key in ({_archive_keys_in_sql(archive_keys)}))"
    )
```

5. Add the two reconcilers (after the upserts). They follow the exact delete-then-insert shape of the upserts but take their scope from the Task 1 resolvers and share one connection block:

```python
def reconcile_sweden_financial_reports_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, str | int]:
    """Order-independent weekly export: upsert exactly the archives whose
    local ``reports`` rows are missing or count-mismatched in ClickHouse.
    A yearly rebuild cannot strand this -- there is no bookkeeping to
    lose; the diff is recomputed from data every run."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(SE_FINANCIAL_REPORTS_TABLE,),
    )
    with clickhouse.get_connection() as client:
        archive_keys = resolve_unreconciled_report_archive_keys(
            duckdb_connection, client, log=log
        )
        if not archive_keys:
            return {
                "archives": 0,
                "deleted": 0,
                "inserted": 0,
                "skipped_reason": (
                    "clickhouse already matches the local year file"
                ),
            }
        deleted = _count_and_delete_clickhouse_rows_by_key(
            client,
            qualified_table=QUALIFIED_SE_FINANCIAL_REPORTS_TABLE,
            column="source_archive_key",
            keys=archive_keys,
        )
        inserted = _insert_partition_scope_rows(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_table="reports",
            where_sql=_reports_scope_where_sql(archive_keys),
            clickhouse_table=SE_FINANCIAL_REPORTS_TABLE,
            columns=SE_FINANCIAL_REPORTS_EXPORT_COLUMNS,
            log=log,
        )
    if log is not None:
        log(
            "Reconciled Sweden financial reports: archives=%s deleted=%s "
            "inserted=%s",
            len(archive_keys),
            deleted,
            inserted,
        )
    return {"archives": len(archive_keys), "deleted": deleted, "inserted": inserted}


def reconcile_sweden_financial_facts_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, str | int]:
    """Facts twin of ``reconcile_sweden_financial_reports_clickhouse``,
    diffing facts counts per archive (via ``statement_key``) and deleting
    through the Memory stage-table mechanism (statement-key scopes can
    exceed ``max_query_size`` as Array params)."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(SE_FINANCIAL_FACTS_TABLE,),
    )
    with clickhouse.get_connection() as client:
        archive_keys = resolve_unreconciled_facts_archive_keys(
            duckdb_connection, client, log=log
        )
        if not archive_keys:
            return {
                "archives": 0,
                "deleted": 0,
                "inserted": 0,
                "skipped_reason": (
                    "clickhouse already matches the local year file"
                ),
            }
        statement_keys = _statement_keys_for_archive_keys(
            duckdb_connection, archive_keys
        )
        deleted = _count_and_delete_facts_rows_via_scope_stage(
            client, statement_keys
        )
        inserted = _insert_partition_scope_rows(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_table="facts",
            where_sql=_facts_scope_where_sql(archive_keys),
            clickhouse_table=SE_FINANCIAL_FACTS_TABLE,
            columns=SE_FINANCIAL_FACTS_EXPORT_COLUMNS,
            log=log,
        )
    if log is not None:
        log(
            "Reconciled Sweden financial facts: archives=%s deleted=%s "
            "inserted=%s",
            len(archive_keys),
            deleted,
            inserted,
        )
    return {"archives": len(archive_keys), "deleted": deleted, "inserted": inserted}
```

(Check how the existing upserts call `_upsert_*` vs the facts stage helper for the exact existing facts upsert body and mirror it; `_statement_keys_for_archive_keys` docstring's "never empty" note stays true — reconcilers return early on empty scope.)

- [ ] **Step 4: Run the whole test file, verify green**

Run: `uv run pytest tests/test_sweden_financial_clickhouse.py -q`
Expected: all pass (including the reworked backfill tests, none of the deleted tests remain).

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/sweden_financial/clickhouse.py tests/test_sweden_financial_clickhouse.py
git commit -m "feat(dagster): reconciling sweden weekly exports; retire weekly export ledger

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: De-partition the current chain; wire assets, jobs, schedule

**Files:**
- Modify: `src/dagster_v3/defs/sweden_financial/assets.py`
- Test: `tests/test_sweden_financial_assets.py`, `tests/test_sweden_financial_archive_state.py` (only if it asserts on `load_partition_key` semantics — check)

**Interfaces:**
- Consumes: Task 2's `reconcile_sweden_financial_reports_clickhouse` / `reconcile_sweden_financial_facts_clickhouse` (imported instead of nothing new for backfill; the `upsert_*_partition` imports stay for the backfill assets).
- Produces: the five current assets non-partitioned; `SWEDEN_FINANCIAL_CURRENT_PARTITIONS`, `SWEDEN_FINANCIAL_CURRENT_PARTITION_KEYS`, `SWEDEN_FINANCIAL_CURRENT_START_DATE`, `SWEDEN_FINANCIAL_CURRENT_END_DATE`, and `_current_year_run_request` deleted; `sweden_financial_current_year_weekly` becomes a plain `ScheduleDefinition` (same name/cron/timezone/`default_status`).
- The current sync passes `load_partition_key="current"` (a constant — the manifest S3 key becomes `sync_kind=current/load_partition_key=current/manifest.json`, overwritten each weekly run; the catalog asset reads the same constant). The backfill chain keeps `context.partition_key`.

- [ ] **Step 1: Update the assets test (failing first)**

In `tests/test_sweden_financial_assets.py::test_sweden_financial_backfill_and_current_assets_are_separate`:
- `current_raw_node`: replace the `StaticPartitionsDefinition` + partition-keys assertions with `assert current_raw_node.partitions_def is None`.
- Same for `current_catalog_node` / `current_parsed_node` (`partitions_def is None`; keep the pool and parent-key assertions).
- Add the two current export nodes: `partitions_def is None`, pool `{"sweden_financial_current_2026_duckdb"}`, parents `{dg.AssetKey("sweden_financial_current_parsed_reports_duckdb")}`.
- Job selections: `sweden_financial_current_year_job` and `sweden_financial_current_clickhouse_job` keep their current expected sets (5 and 2 asset names — unchanged names).
- Schedule: keep asserting name/cron/job-name; if the test asserts anything about partitioned run requests, drop it.

Run: `uv run pytest tests/test_sweden_financial_assets.py -q` — expected FAIL (assets still partitioned).

- [ ] **Step 2: De-partition in `assets.py`**

- Delete `SWEDEN_FINANCIAL_CURRENT_START_DATE`, `SWEDEN_FINANCIAL_CURRENT_END_DATE`, `SWEDEN_FINANCIAL_CURRENT_PARTITION_KEYS`, `SWEDEN_FINANCIAL_CURRENT_PARTITIONS` (keep `SWEDEN_FINANCIAL_CURRENT_YEAR` and `SWEDEN_FINANCIAL_TIMEZONE`).
- `sweden_financial_current_raw_archives_s3`: remove `partitions_def` and `backfill_policy`; body calls `_sync_raw_archives(..., sync_kind="current", archive_year=SWEDEN_FINANCIAL_CURRENT_YEAR)`. `_sync_raw_archives` gains an explicit `load_partition_key: str` parameter (backfill assets pass `context.partition_key`, the current asset passes `"current"`) — replace both internal uses of `context.partition_key` with the parameter.
- `sweden_financial_current_report_xhtml_catalog_duckdb`: remove `partitions_def`/`backfill_policy`; `read_sweden_financial_archive_sync_manifest(..., load_partition_key="current")`; `record_sweden_financial_archive_sync(..., load_partition_key="current")`; metadata `"load_partition_key": "current"`. Everything else (run correlation via `context.run_id`, `replace_scope="archive"`) unchanged.
- `sweden_financial_current_parsed_reports_duckdb`: remove `partitions_def`/`backfill_policy`; drop the `"load_partition_key"` metadata entry; body unchanged.
- `sweden_financial_current_reports_clickhouse` / `sweden_financial_current_facts_clickhouse`: remove `partitions_def`/`backfill_policy`; bodies become:

```python
@dg.asset(
    deps=["sweden_financial_current_parsed_reports_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "xbrl"},
    pool=SWEDEN_FINANCIAL_CURRENT_DUCKDB_POOL,
    metadata={"table": QUALIFIED_SE_FINANCIAL_REPORTS_TABLE},
    description=(
        "Reconciles the active-year Sweden financial reports into "
        "ClickHouse: diffs the local year DuckDB against the target per "
        "source_archive_key and upserts exactly the missing/mismatched "
        "archives. Stateless -- safe to run in any order relative to the "
        "yearly backfill."
    ),
)
def sweden_financial_current_reports_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    duckdb_path = sweden_financial_source_duckdb_path(SWEDEN_FINANCIAL_CURRENT_YEAR)
    with sweden_financial_year_duckdb_connection(
        SWEDEN_FINANCIAL_CURRENT_YEAR
    ) as connection:
        metadata = reconcile_sweden_financial_reports_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **metadata,
            "duckdb_table": "sweden_financial.reports",
            "duckdb_path": str(duckdb_path),
            "clickhouse_table": QUALIFIED_SE_FINANCIAL_REPORTS_TABLE,
        }
    )
```

and the facts twin calling `reconcile_sweden_financial_facts_clickhouse` with `"duckdb_table": "sweden_financial.facts"` / `QUALIFIED_SE_FINANCIAL_FACTS_TABLE`. Import the two reconcilers in the import block at the top.
- `_upsert_reports_partition_result` / `_upsert_facts_partition_result` stay for the backfill assets only.
- Delete `_current_year_run_request`; the schedule becomes:

```python
sweden_financial_current_year_weekly = dg.ScheduleDefinition(
    name="sweden_financial_current_year_weekly",
    job=sweden_financial_current_year_job,
    cron_schedule="45 6 * * 6",
    execution_timezone=SWEDEN_FINANCIAL_TIMEZONE,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
```

- `_all_partition_deps` and the derived assets' deps: `AllPartitionMapping` on a dep that is no longer partitioned is invalid — split `SWEDEN_FINANCIAL_EXPORT_ASSET_KEYS`/`SWEDEN_FINANCIAL_FACTS_EXPORT_ASSET_KEYS` into backfill (partitioned, keep `_all_partition_deps`) and current (plain string deps):

```python
SWEDEN_FINANCIAL_EXPORT_DEPS = [
    *_all_partition_deps(
        "sweden_financial_backfill_reports_clickhouse",
        "sweden_financial_backfill_facts_clickhouse",
    ),
    "sweden_financial_current_reports_clickhouse",
    "sweden_financial_current_facts_clickhouse",
]
SWEDEN_FINANCIAL_FACTS_EXPORT_DEPS = [
    *_all_partition_deps("sweden_financial_backfill_facts_clickhouse"),
    "sweden_financial_current_facts_clickhouse",
]
```

and use these in `sweden_financial_metrics_clickhouse`, `se_financial_history_clickhouse` (which also keeps its `"sweden_financial_metrics_clickhouse"` ordering dep), `se_company_officers_clickhouse`, `se_company_audits_clickhouse`.
- Comments on `SWEDEN_FINANCIAL_CURRENT_SELECTION` updated: the selection membership itself is unchanged.

- [ ] **Step 3: Gates**

Run: `uv run pytest tests/test_sweden_financial_assets.py tests/test_sweden_financial_clickhouse.py tests/test_sweden_financial_archive_state.py -q`
Expected: all pass.
Run: `uv run dg check defs`
Expected: `All definitions loaded successfully.`

- [ ] **Step 4: Commit**

```bash
git add src/dagster_v3/defs/sweden_financial/assets.py tests/test_sweden_financial_assets.py
git commit -m "feat(dagster): de-partition sweden weekly chain -- order-independent refresh

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Docs + deploy/ops notes

**Files:**
- Modify: `src/dagster_v3/defs/sweden_financial/docs/sweden_financial-design.md` (the "Job And Schedule" section written 2026-07-20)
- Modify: `docs/sweden-data-sources.md` (jobs table + the weekly-partition language in section 2)

**Interfaces:** none (docs only).

- [ ] **Step 1: Rewrite the design doc's weekly section**

Replace the paragraph block starting "`sweden_financial_current_year_job` selects the full weekly chain..." (added 2026-07-20) with:

```markdown
`sweden_financial_current_year_job` selects the full weekly chain as separate
non-partitioned assets in one run: `sweden_financial_current_raw_archives_s3`,
`sweden_financial_current_report_xhtml_catalog_duckdb`,
`sweden_financial_current_parsed_reports_duckdb`, then the
`sweden_financial_current_reports_clickhouse` /
`sweden_financial_current_facts_clickhouse` export pair.

The weekly chain is deliberately unpartitioned (2026-07-20 design): weekly
partition identities existed only to give each week's export a bookkeeping
scope (`archive_sync_catalog.load_partition_key`), and that bookkeeping was
exactly what a yearly re-parse destroyed (the 2026-07-18 incident -- the
backfill replaces the entire year DuckDB file). Instead, the weekly exports
are **reconcilers**: they diff the local year file against ClickHouse per
`source_archive_key` (row counts on both sides; facts counted through the
`statement_key` -> reports join) and upsert exactly the missing/mismatched
archives. No state can be lost because no state is kept.

**Order-independence invariant:** the weekly job and the yearly backfill
(parse + export) may be materialized in ANY order, any number of times, and
every run succeeds -- yearly-after-weekly is an idempotent superset upsert;
weekly-after-yearly reconciles to a no-op (metadata `skipped_reason`) or
fills exactly the remaining gap. The only remaining export error is the
corruption guard: a local year file with zero report rows refuses to export.

`sweden_financial_current_year_weekly` runs at `45 6 * * 6` in
`Europe/Belgrade` and is enabled by default. Each weekly run discovers
upstream `LastModified` changes, downloads only changed archives, parses
them, and reconciles ClickHouse.
```

Also delete the now-stale "A completed year backfill plus its backfill export subsumes every earlier weekly partition..." paragraph (there are no weekly partitions to subsume).

- [ ] **Step 2: Update `docs/sweden-data-sources.md`**

- Jobs table row: `sweden_financial_current_year_job` | `full weekly chain: sync + catalog + parse + reconciling reports/facts exports (non-partitioned)` | schedule cell unchanged.
- `sweden_financial_current_clickhouse_job` row: `reconciling current export pair (manual; safe any time -- stateless diff vs ClickHouse)` | `manual`.
- In section 2's prose, replace any "7-day partitions" description of the current chain with "non-partitioned weekly refresh (reconciling exports)".

- [ ] **Step 3: Add the deploy note to the design doc (same section, at the end)**

```markdown
**Deploy note (one-time, 2026-07-20):** before deploying the de-partitioned
current chain, cancel any in-flight/queued backfills or runs targeting the
old weekly partitions (`bulk_actions` / `run_tags key='dagster/backfill'`);
a queued partition run that starts after the partitions are gone fails with
`RUN_EXCEPTION` and can leak its `sweden_financial_current_2026_duckdb` pool
slot (see CLAUDE.md Troubleshooting). Historical weekly-partition
materializations remain in the event log as orphans; that is cosmetic.
```

- [ ] **Step 4: Gates + commit**

Run: `uv run dg check defs` (defs untouched — sanity only).

```bash
git add src/dagster_v3/defs/sweden_financial/docs/sweden_financial-design.md docs/sweden-data-sources.md
git commit -m "docs(dagster): sweden order-independent weekly refresh -- reconciling exports

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Full-suite gate + prod validation checklist

**Files:** none (verification only).

- [ ] **Step 1: Full Sweden suite + defs**

Run:
```bash
uv run pytest tests/test_sweden_financial_archive_state.py tests/test_sweden_financial_assets.py tests/test_sweden_financial_audits.py tests/test_sweden_financial_clickhouse.py tests/test_sweden_financial_clickhouse_migrations.py tests/test_sweden_financial_history.py tests/test_sweden_financial_metrics.py tests/test_sweden_financial_metrics_migration.py tests/test_sweden_financial_officers.py tests/test_sweden_financial_resources.py -q
uv run dg check defs
```
Expected: all pass; definitions load.

- [ ] **Step 2: Report the prod rollout checklist to the user (do NOT execute it unprompted)**

1. On prod: check for queued/in-flight runs or backfills targeting `sweden_financial_current_*` (UI or `bulk_actions`); cancel stragglers.
2. Deploy (`ansible-playbook site.yml` from `ansible/`).
3. Validate order-independence live, in this order (each must be green):
   a. materialize `sweden_financial_current_year_job` (weekly refresh — likely reconciles to no-op or a small diff),
   b. materialize the yearly `2026` backfill chain + `sweden_financial_backfill_clickhouse_job` partition `2026`,
   c. materialize `sweden_financial_current_year_job` again (must be green, `skipped_reason` no-op).
4. Confirm `archive_ingest_complete` check still passes on the next metrics wave.
