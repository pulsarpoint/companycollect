"""Tests for the two esef_facts_clickhouse data-correctness asset checks in
partitioned_publish.py.

No live ClickHouse -- a small keyed-dispatch FakeClickHouseClient per check
answers each query's SQL with canned rows, mirroring
tests/test_clickhouse_leaf_checks.py's FakeClient and
tests/test_esef_filings_publish.py's FakeClickHouseClient patterns. Each
check is invoked directly via `.node_def.compute_fn.decorated_fn(...)`
(mirrors test_esef_filings_publish.py's translation-coverage test), bypassing
Dagster's op execution machinery entirely.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import dagster as dg

from dagster_v3.defs.esef_filings.partitioned_publish import (
    fxo_id_single_processed_week,
    per_week_facts_coverage,
)


# --- fxo_id_single_processed_week (Guard 1) ----------------------------------


class _FakeFxoWeekClient:
    """Answers the offending-count query and, only when the count is
    non-zero, the LIMIT-10 sample query -- mirrors the check's own
    short-circuit (no sample query is issued when nothing is offending)."""

    def __init__(
        self,
        *,
        offending_count: int,
        sample_rows: list[tuple[str, list[str]]] | None = None,
    ) -> None:
        self.offending_count = offending_count
        self.sample_rows = sample_rows or []
        self.statements: list[str] = []

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        self.statements.append(sql)
        stripped = sql.strip()
        if stripped.startswith("SELECT count() FROM ("):
            return [(self.offending_count,)]
        if "groupUniqArray(processed_week)" in stripped:
            return self.sample_rows
        raise AssertionError(f"unexpected SQL: {sql}")


class _FakeClickHouseResource:
    def __init__(self, client: Any) -> None:
        self._client = client

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        yield self._client


def _run_fxo_check(client: _FakeFxoWeekClient) -> dg.AssetCheckResult:
    return fxo_id_single_processed_week.node_def.compute_fn.decorated_fn(
        _FakeClickHouseResource(client)
    )


def test_fxo_id_single_processed_week_passes_when_every_fxo_stays_in_one_week() -> (
    None
):
    client = _FakeFxoWeekClient(offending_count=0)

    result = _run_fxo_check(client)

    assert result.passed is True
    assert result.severity == dg.AssetCheckSeverity.ERROR
    assert result.metadata["offending_fxo_count"].value == 0
    assert result.metadata["sample"].value == []
    # No sample query when nothing is offending.
    assert len(client.statements) == 1


def test_fxo_id_single_processed_week_fails_with_sample_when_fxo_spans_two_weeks() -> (
    None
):
    client = _FakeFxoWeekClient(
        offending_count=1,
        sample_rows=[("542900ABCDEF-01", ["2025-11-16", "2025-11-23"])],
    )

    result = _run_fxo_check(client)

    assert result.passed is False
    assert result.severity == dg.AssetCheckSeverity.ERROR
    assert result.metadata["offending_fxo_count"].value == 1
    assert result.metadata["sample"].value == [
        {
            "fxo_id": "542900ABCDEF-01",
            "processed_weeks": ["2025-11-16", "2025-11-23"],
        }
    ]
    assert len(client.statements) == 2


# --- per_week_facts_coverage (Guard 2) ---------------------------------------


class _FakeCoverageClient:
    def __init__(
        self,
        *,
        week_rows: list[tuple[str, int, int]],
        facts_fxo_total: int,
        index_fxo_total: int,
    ) -> None:
        self.week_rows = week_rows
        self.facts_fxo_total = facts_fxo_total
        self.index_fxo_total = index_fxo_total
        self.statements: list[str] = []

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        self.statements.append(sql)
        stripped = sql.strip()
        if "index_weeks" in stripped:
            return self.week_rows
        if "facts_fxo_total" in stripped:
            return [(self.facts_fxo_total, self.index_fxo_total)]
        raise AssertionError(f"unexpected SQL: {sql}")


def _run_coverage_check(client: _FakeCoverageClient) -> dg.AssetCheckResult:
    return per_week_facts_coverage.node_def.compute_fn.decorated_fn(
        _FakeClickHouseResource(client)
    )


def test_per_week_facts_coverage_passes_with_covered_and_never_run_weeks() -> None:
    # One fully-covered week and one week that has never run (zero facts) --
    # the never-run week must not itself fail the check.
    client = _FakeCoverageClient(
        week_rows=[
            ("2025-11-23", 100, 95),  # covered: 95%
            ("2025-11-30", 1821, 0),  # never run
        ],
        facts_fxo_total=95,
        index_fxo_total=1921,
    )

    result = _run_coverage_check(client)

    assert result.passed is True
    assert result.severity == dg.AssetCheckSeverity.WARN
    assert result.metadata["weeks_with_index_filings"].value == 2
    assert result.metadata["weeks_covered"].value == 1
    assert result.metadata["weeks_under_covered"].value == 0
    assert result.metadata["weeks_never_run"].value == 1
    assert result.metadata["worst_under_covered_weeks"].value == []
    assert result.metadata["facts_fxo_total"].value == 95
    assert result.metadata["index_fxo_total"].value == 1921


def test_per_week_facts_coverage_warns_on_under_covered_week() -> None:
    # Reproduces the two prod-observed bounded runs: a week that ran but only
    # parsed a max_documents-capped subset must fail (WARN), distinct from a
    # week that never ran at all.
    client = _FakeCoverageClient(
        week_rows=[
            ("2023-11-12", 2889, 82),  # under-covered: ~2.8%
            ("2025-11-30", 1821, 24),  # under-covered: ~1.3%
            ("2025-12-07", 500, 480),  # covered: 96%
            ("2025-12-14", 300, 0),  # never run
        ],
        facts_fxo_total=586,
        index_fxo_total=5510,
    )

    result = _run_coverage_check(client)

    assert result.passed is False
    assert result.severity == dg.AssetCheckSeverity.WARN
    assert result.metadata["weeks_with_index_filings"].value == 4
    assert result.metadata["weeks_covered"].value == 1
    assert result.metadata["weeks_under_covered"].value == 2
    assert result.metadata["weeks_never_run"].value == 1

    worst = result.metadata["worst_under_covered_weeks"].value
    assert [row["week"] for row in worst] == ["2025-11-30", "2023-11-12"]
    assert worst[0] == {
        "week": "2025-11-30",
        "index_filings": 1821,
        "facts_filings": 24,
        "pct": round(24 / 1821 * 100, 1),
    }
    assert worst[1] == {
        "week": "2023-11-12",
        "index_filings": 2889,
        "facts_filings": 82,
        "pct": round(82 / 2889 * 100, 1),
    }

    assert result.metadata["facts_fxo_total"].value == 586
    assert result.metadata["index_fxo_total"].value == 5510
    assert result.metadata["overall_filing_coverage_pct"].value == round(
        586 / 5510 * 100, 1
    )


def test_per_week_facts_coverage_worst_list_is_capped_at_ten() -> None:
    # 12 distinct under-covered weeks (10% coverage each) to exceed the cap
    # of 10 worst weeks in the metadata.
    week_rows = [(f"2024-{month:02d}-07", 100, 10) for month in range(1, 13)]

    client = _FakeCoverageClient(
        week_rows=week_rows,
        facts_fxo_total=120,
        index_fxo_total=1200,
    )

    result = _run_coverage_check(client)

    assert result.passed is False
    assert result.metadata["weeks_under_covered"].value == 12
    assert len(result.metadata["worst_under_covered_weeks"].value) == 10


def test_per_week_facts_coverage_totals_come_from_totals_query_not_summed_weeks() -> (
    None
):
    # A cross-week duplicate fxo_id (Guard 1's concern) would inflate a
    # summed-per-week total; the totals query is a distinct global count, so
    # it must be wired straight through rather than derived from week_rows.
    client = _FakeCoverageClient(
        week_rows=[
            ("2025-11-16", 50, 50),
            ("2025-11-23", 50, 50),
        ],
        facts_fxo_total=99,  # one fxo_id shared across both weeks -> not 100
        index_fxo_total=100,
    )

    result = _run_coverage_check(client)

    assert result.metadata["facts_fxo_total"].value == 99
    assert result.metadata["overall_filing_coverage_pct"].value == 99.0


def test_per_week_facts_coverage_handles_zero_index_total() -> None:
    client = _FakeCoverageClient(
        week_rows=[],
        facts_fxo_total=0,
        index_fxo_total=0,
    )

    result = _run_coverage_check(client)

    assert result.passed is True
    assert result.metadata["overall_filing_coverage_pct"].value is None
