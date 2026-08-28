"""Task 2b: the asset that force-refreshes corpscout.se_companies_current, and its check.

`sweden_companies_current_clickhouse` issues SYSTEM REFRESH VIEW then SYSTEM WAIT VIEW against
`corpscout.se_companies_current` (after asserting the view exists) so the companies serving view is
fresh the moment the weekly geocoding run finishes rather than at the next hourly auto-refresh.
These tests pin, against a FakeClient:

- the two SYSTEM statements are issued, in order, against the qualified view name -- and NOTHING
  else write-mode (no INSERT/CREATE/ALTER/DROP/EXCHANGE/TRUNCATE/RENAME);
- the view's existence is asserted first (a pre-migration run fails clearly on the name);
- the refresh-health check reads system.view_refreshes for this view and reports the shared
  predicate.

The predicate `companies_current_refresh_is_healthy` itself is covered in
`test_se_companies_current_mv.py`; here it is exercised only through the check wrapper.
"""

import re
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Iterator

import dagster as dg

from dagster_v3.defs.sweden_company import companies_current
from dagster_v3.defs.sweden_company.companies_current_asset import (
    QUALIFIED_SE_COMPANIES_SERVING_VIEW,
    REFRESH_VIEW_SQL,
    ROW_COUNT_SQL,
    WAIT_VIEW_SQL,
    sweden_companies_current_clickhouse,
    sweden_companies_current_refresh_check,
)

_WRITE_MODE_KEYWORDS = (
    "INSERT",
    "CREATE",
    "ALTER",
    "DROP",
    "EXCHANGE",
    "TRUNCATE",
    "RENAME",
    "REPLACE",
)


class _FakeResource:
    def __init__(self, client: Any) -> None:
        self._client = client

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        yield self._client


class _FakeCompaniesCurrentClient:
    """Answers the asset's four statements and records every (sql, params) pair it saw.

    The four are distinct by shape: the existence probe hits ``system.tables`` (and carries the
    requested table set in its params), the two SYSTEM statements are matched as whole constants,
    and the row count is its own constant. Anything unrecognised raises rather than answering the
    wrong query, so a statement the asset should not issue cannot pass silently.
    """

    def __init__(self, *, view_exists: bool = True, row_count: int = 4_321) -> None:
        self._view_exists = view_exists
        self._row_count = row_count
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        self.executed.append((sql, params))
        if "system.tables" in sql:
            return (
                [(companies_current.SE_COMPANIES_SERVING_VIEW,)]
                if self._view_exists
                else []
            )
        if sql == REFRESH_VIEW_SQL:
            return []
        if sql == WAIT_VIEW_SQL:
            return []
        if sql == ROW_COUNT_SQL:
            return [(self._row_count,)]
        raise AssertionError(f"unexpected statement: {sql!r}")

    @property
    def statements(self) -> list[str]:
        return [sql for sql, _ in self.executed]


def _run_asset(client: _FakeCompaniesCurrentClient) -> dg.MaterializeResult:
    return sweden_companies_current_clickhouse.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(),
        _FakeResource(client),
    )


def test_the_asset_forces_a_refresh_and_waits_on_this_view() -> None:
    client = _FakeCompaniesCurrentClient(row_count=4_321)
    result = _run_asset(client)

    # The two SYSTEM statements are issued, in order, against the qualified serving view.
    system_statements = [sql for sql in client.statements if sql.startswith("SYSTEM ")]
    assert system_statements == [REFRESH_VIEW_SQL, WAIT_VIEW_SQL]
    assert (
        REFRESH_VIEW_SQL == f"SYSTEM REFRESH VIEW {QUALIFIED_SE_COMPANIES_SERVING_VIEW}"
    )
    assert WAIT_VIEW_SQL == f"SYSTEM WAIT VIEW {QUALIFIED_SE_COMPANIES_SERVING_VIEW}"
    assert QUALIFIED_SE_COMPANIES_SERVING_VIEW == "corpscout.se_companies_serving"

    metadata = result.metadata
    assert metadata["view"] == QUALIFIED_SE_COMPANIES_SERVING_VIEW
    assert metadata["row_count"] == 4_321


def test_the_asset_asserts_the_view_exists_before_refreshing() -> None:
    client = _FakeCompaniesCurrentClient()
    _run_asset(client)

    # The existence probe runs, and it names THIS view in the serving database.
    probe = [(sql, params) for sql, params in client.executed if "system.tables" in sql]
    assert len(probe) == 1
    [(_, probe_params)] = probe
    assert probe_params["database"] == companies_current.CLICKHOUSE_DATABASE
    assert companies_current.SE_COMPANIES_SERVING_VIEW in probe_params["tables"]
    # It runs BEFORE either SYSTEM statement -- a pre-migration run must fail on the name.
    system_index = min(
        i for i, sql in enumerate(client.statements) if sql.startswith("SYSTEM ")
    )
    probe_index = next(
        i for i, sql in enumerate(client.statements) if "system.tables" in sql
    )
    assert probe_index < system_index


def test_a_pre_migration_run_fails_clearly_on_the_missing_view() -> None:
    client = _FakeCompaniesCurrentClient(view_exists=False)
    try:
        _run_asset(client)
    except ValueError as exc:
        assert companies_current.SE_COMPANIES_SERVING_VIEW in str(exc)
    else:
        raise AssertionError("a missing view must raise")
    # It failed on the existence probe, before touching the view with any SYSTEM statement.
    assert not any(sql.startswith("SYSTEM ") for sql in client.statements)


def test_the_asset_issues_nothing_else_write_mode() -> None:
    client = _FakeCompaniesCurrentClient()
    _run_asset(client)

    for sql in client.statements:
        if sql in (REFRESH_VIEW_SQL, WAIT_VIEW_SQL):
            continue
        upper = sql.upper()
        for keyword in _WRITE_MODE_KEYWORDS:
            assert not re.search(rf"\b{keyword}\b", upper), (keyword, sql)
    # Exactly one refresh and one wait -- the view is not refreshed twice.
    assert client.statements.count(REFRESH_VIEW_SQL) == 1
    assert client.statements.count(WAIT_VIEW_SQL) == 1


# --- the refresh-health check ---------------------------------------------------------------


class _FakeRefreshClient:
    """Answers the one system.view_refreshes read the check issues, and records it."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows
        self.executed: list[str] = []

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        self.executed.append(sql)
        assert sql == companies_current.SE_COMPANIES_SERVING_REFRESH_SQL
        return self._rows


def _run_check(rows: list[tuple[Any, ...]]) -> dict[str, Any]:
    client = _FakeRefreshClient(rows)
    result = sweden_companies_current_refresh_check.node_def.compute_fn.decorated_fn(
        _FakeResource(client)
    )
    [executed] = client.executed
    assert executed == companies_current.SE_COMPANIES_SERVING_REFRESH_SQL
    return {
        "passed": result.passed,
        **{key: value.value for key, value in result.metadata.items()},
    }


def _recent_epoch(hours_ago: float) -> int:
    return int((datetime.now(UTC) - timedelta(hours=hours_ago)).timestamp())


def test_the_check_reads_system_view_refreshes_for_this_view() -> None:
    assert "system.view_refreshes" in companies_current.SE_COMPANIES_SERVING_REFRESH_SQL
    assert (
        "database = 'corpscout'" in companies_current.SE_COMPANIES_SERVING_REFRESH_SQL
    )
    assert (
        "view = 'se_companies_serving'"
        in companies_current.SE_COMPANIES_SERVING_REFRESH_SQL
    )
    assert (
        "toUnixTimestamp(last_success_time)"
        in companies_current.SE_COMPANIES_SERVING_REFRESH_SQL
    )


def test_a_recent_success_passes_the_check() -> None:
    result = _run_check([("Scheduled", "", _recent_epoch(0.5))])
    assert result["passed"]
    assert result["refresh_row_found"] is True


def test_a_missing_refresh_row_fails_the_check() -> None:
    result = _run_check([])
    assert not result["passed"]
    assert result["refresh_row_found"] is False


def test_an_exception_fails_the_check() -> None:
    result = _run_check([("Scheduled", "definer lost SELECT", _recent_epoch(0.1))])
    assert not result["passed"]


def test_a_stale_success_fails_the_check() -> None:
    result = _run_check([("Scheduled", "", _recent_epoch(4))])
    assert not result["passed"]


def test_the_check_is_registered_on_the_asset() -> None:
    assert sweden_companies_current_refresh_check.check_keys == {
        dg.AssetCheckKey(
            dg.AssetKey("sweden_companies_current_clickhouse"),
            "companies_current_view_is_being_refreshed",
        )
    }
    assert sweden_companies_current_clickhouse.key == dg.AssetKey(
        "sweden_companies_current_clickhouse"
    )
