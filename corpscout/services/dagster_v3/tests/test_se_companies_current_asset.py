"""The asset that force-refreshes corpscout.se_companies_serving, and its check.

`sweden_companies_current_clickhouse` issues SYSTEM REFRESH VIEW against
`corpscout.se_companies_serving` (after asserting the view exists) and then POLLS
`system.view_refreshes` until a refresh that started at or after the request has succeeded.
It never issues SYSTEM WAIT VIEW: that statement holds a client socket silent for the whole
rebuild, and the rebuild (13-16 minutes on prod as of 2026-09-21) outlasts the driver's
300-second read timeout, so every run died with `TimeoutError: timed out` while ClickHouse
finished the refresh anyway. Each poll is its own short query, so the wait is immune to how
long the rebuild takes. These tests pin, against a FakeClient:

- REFRESH VIEW is issued exactly once, against the qualified view name, and NOTHING else
  write-mode (no WAIT/INSERT/CREATE/ALTER/DROP/EXCHANGE/TRUNCATE/RENAME);
- the view's existence is asserted first (a pre-migration run fails clearly on the name);
- a STOPPED view fails before the refresh is requested (REFRESH VIEW is a no-op on it, so
  polling would wait for nothing);
- the verdict on a system.view_refreshes row: only a success that STARTED at or after the
  request counts, an in-flight refresh that started earlier does not, a failure after the
  request raises, a failure before the request is ignored;
- the poll loop sleeps between pending polls and gives up past its budget;
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
import pytest

from dagster_v3.defs.sweden_company import companies_current
from dagster_v3.defs.sweden_company.companies_current_asset import (
    MAX_REFRESH_WAIT,
    QUALIFIED_SE_COMPANIES_SERVING_VIEW,
    REFRESH_POLL_INTERVAL,
    REFRESH_STATE_SQL,
    REFRESH_VIEW_SQL,
    ROW_COUNT_SQL,
    SERVER_NOW_SQL,
    RefreshOutcome,
    RefreshState,
    judge_refresh,
    sweden_companies_current_clickhouse,
    sweden_companies_current_refresh_check,
    wait_for_refresh,
)

_WRITE_MODE_KEYWORDS = (
    "WAIT",
    "INSERT",
    "CREATE",
    "ALTER",
    "DROP",
    "EXCHANGE",
    "TRUNCATE",
    "RENAME",
    "REPLACE",
)

REQUESTED_AT = 1_800_000_000  # the server's now() when the refresh was requested


def _state_row(
    *,
    status: str = "Scheduled",
    exception: str = "",
    last_success_epoch: int | None = None,
    last_refresh_epoch: int | None = None,
    duration_ms: int | None = None,
    progress: float = 0.0,
) -> tuple[Any, ...]:
    return (status, exception, last_success_epoch, last_refresh_epoch, duration_ms, progress)


_DONE_ROW = _state_row(
    last_success_epoch=REQUESTED_AT + 3,
    last_refresh_epoch=REQUESTED_AT + 900,
    duration_ms=897_000,
    progress=1.0,
)
_RUNNING_ROW = _state_row(
    status="Running",
    last_success_epoch=REQUESTED_AT - 3_600,
    last_refresh_epoch=REQUESTED_AT - 2_700,
    duration_ms=900_000,
    progress=0.4,
)


class _FakeResource:
    def __init__(self, client: Any) -> None:
        self._client = client
        self.connections = 0

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        self.connections += 1
        yield self._client


class _FakeCompaniesCurrentClient:
    """Answers the asset's statements and records every (sql, params) pair it saw.

    The existence probe hits ``system.tables`` (and carries the requested table set in its
    params); the server-time read, the REFRESH statement, the state poll and the row count are
    matched as whole constants. State polls are answered from a script -- one row per poll,
    the last row repeating -- so a test can stage "running, then done". Anything unrecognised
    raises rather than answering the wrong query, so a statement the asset should not issue
    cannot pass silently.
    """

    def __init__(
        self,
        *,
        view_exists: bool = True,
        row_count: int = 4_321,
        states: list[list[tuple[Any, ...]]] | None = None,
    ) -> None:
        self._view_exists = view_exists
        self._row_count = row_count
        self._states = list(states) if states is not None else [[_DONE_ROW]]
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        self.executed.append((sql, params))
        if "system.tables" in sql:
            return (
                [(companies_current.SE_COMPANIES_SERVING_VIEW,)]
                if self._view_exists
                else []
            )
        if sql == SERVER_NOW_SQL:
            return [(REQUESTED_AT,)]
        if sql == REFRESH_VIEW_SQL:
            return []
        if sql == REFRESH_STATE_SQL:
            rows = self._states[0]
            if len(self._states) > 1:
                self._states.pop(0)
            return rows
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


# --- the asset ------------------------------------------------------------------------------


def test_the_asset_forces_a_refresh_and_polls_until_it_lands() -> None:
    client = _FakeCompaniesCurrentClient(row_count=4_321)
    result = _run_asset(client)

    assert REFRESH_VIEW_SQL == f"SYSTEM REFRESH VIEW {QUALIFIED_SE_COMPANIES_SERVING_VIEW}"
    assert QUALIFIED_SE_COMPANIES_SERVING_VIEW == "corpscout.se_companies_serving"
    # Exactly one SYSTEM statement, the refresh request; the wait is done by polling.
    assert [sql for sql in client.statements if sql.startswith("SYSTEM ")] == [REFRESH_VIEW_SQL]
    assert not any("WAIT VIEW" in sql for sql in client.statements)
    # The server clock is read BEFORE the refresh is requested, and the state is polled after.
    assert client.statements.index(SERVER_NOW_SQL) < client.statements.index(REFRESH_VIEW_SQL)
    polls_after_request = [
        i
        for i, sql in enumerate(client.statements)
        if sql == REFRESH_STATE_SQL and i > client.statements.index(REFRESH_VIEW_SQL)
    ]
    assert polls_after_request, "the refresh state must be polled after the request"
    # The row count is read only once the refresh has landed.
    assert client.statements.index(ROW_COUNT_SQL) > polls_after_request[-1]

    metadata = result.metadata
    assert metadata["view"] == QUALIFIED_SE_COMPANIES_SERVING_VIEW
    assert metadata["row_count"] == 4_321
    assert metadata["refresh_duration_seconds"] == 897.0
    assert metadata["refresh_started_at"] == datetime.fromtimestamp(
        REQUESTED_AT + 3, tz=UTC
    ).isoformat()


def test_the_asset_asserts_the_view_exists_before_refreshing() -> None:
    client = _FakeCompaniesCurrentClient()
    _run_asset(client)

    probe = [(sql, params) for sql, params in client.executed if "system.tables" in sql]
    assert len(probe) == 1
    [(_, probe_params)] = probe
    assert probe_params["database"] == companies_current.CLICKHOUSE_DATABASE
    assert companies_current.SE_COMPANIES_SERVING_VIEW in probe_params["tables"]
    probe_index = next(i for i, sql in enumerate(client.statements) if "system.tables" in sql)
    assert probe_index < client.statements.index(REFRESH_VIEW_SQL)


def test_a_pre_migration_run_fails_clearly_on_the_missing_view() -> None:
    client = _FakeCompaniesCurrentClient(view_exists=False)
    with pytest.raises(ValueError, match=companies_current.SE_COMPANIES_SERVING_VIEW):
        _run_asset(client)
    assert not any(sql.startswith("SYSTEM ") for sql in client.statements)


def test_a_stopped_view_fails_before_the_refresh_is_requested() -> None:
    """SYSTEM STOP VIEW leaves the view Disabled; REFRESH VIEW is then a no-op, so polling
    would wait the whole budget for a refresh that never starts. Fail up front instead."""
    client = _FakeCompaniesCurrentClient(states=[[_state_row(status="Disabled")]])
    with pytest.raises(RuntimeError, match="Disabled"):
        _run_asset(client)
    assert REFRESH_VIEW_SQL not in client.statements


def test_a_missing_refresh_row_fails_before_the_refresh_is_requested() -> None:
    client = _FakeCompaniesCurrentClient(states=[[]])
    with pytest.raises(RuntimeError, match="refreshable"):
        _run_asset(client)
    assert REFRESH_VIEW_SQL not in client.statements


def test_the_asset_issues_nothing_else_write_mode() -> None:
    client = _FakeCompaniesCurrentClient()
    _run_asset(client)

    for sql in client.statements:
        if sql == REFRESH_VIEW_SQL:
            continue
        upper = sql.upper()
        for keyword in _WRITE_MODE_KEYWORDS:
            assert not re.search(rf"\b{keyword}\b", upper), (keyword, sql)
    assert client.statements.count(REFRESH_VIEW_SQL) == 1


def test_the_state_poll_reads_system_view_refreshes_for_this_view() -> None:
    assert "system.view_refreshes" in REFRESH_STATE_SQL
    assert "database = 'corpscout'" in REFRESH_STATE_SQL
    assert "view = 'se_companies_serving'" in REFRESH_STATE_SQL
    assert "toUnixTimestamp(last_success_time)" in REFRESH_STATE_SQL
    assert "toUnixTimestamp(last_refresh_time)" in REFRESH_STATE_SQL
    assert SERVER_NOW_SQL == "SELECT toUnixTimestamp(now())"


# --- the verdict on one system.view_refreshes row ---------------------------------------------


def _judge(row: tuple[Any, ...] | None) -> tuple[RefreshOutcome, str]:
    state = RefreshState.from_rows([] if row is None else [row])
    return judge_refresh(state, requested_at=REQUESTED_AT)


def test_a_success_that_started_at_or_after_the_request_is_done() -> None:
    outcome, _ = _judge(_state_row(last_success_epoch=REQUESTED_AT + 3, progress=1.0))
    assert outcome is RefreshOutcome.DONE
    outcome, _ = _judge(_state_row(last_success_epoch=REQUESTED_AT, progress=1.0))
    assert outcome is RefreshOutcome.DONE, "same-second start counts: >= not >"


def test_a_running_refresh_is_pending() -> None:
    outcome, detail = _judge(_RUNNING_ROW)
    assert outcome is RefreshOutcome.PENDING
    assert "Running" in detail


def test_an_in_flight_refresh_that_started_before_the_request_does_not_count() -> None:
    """Overlap case: a scheduled refresh was already running when we asked. ClickHouse queues
    ours behind it. When the earlier one lands, last_success_time is ITS start -- before our
    request -- so we keep waiting for the queued refresh that reflects our upstream data."""
    outcome, _ = _judge(
        _state_row(
            status="Scheduled",
            last_success_epoch=REQUESTED_AT - 600,
            last_refresh_epoch=REQUESTED_AT + 300,
            progress=1.0,
        )
    )
    assert outcome is RefreshOutcome.PENDING


def test_a_failure_after_the_request_fails_with_the_servers_message() -> None:
    outcome, detail = _judge(
        _state_row(
            status="Scheduled",
            exception="Code: 241. DB::Exception: Memory limit exceeded",
            last_success_epoch=REQUESTED_AT - 3_600,
            last_refresh_epoch=REQUESTED_AT + 400,
        )
    )
    assert outcome is RefreshOutcome.FAILED
    assert "Memory limit exceeded" in detail


def test_a_failure_before_the_request_is_still_pending() -> None:
    """The exception column keeps the last attempt's error; one that predates our request says
    nothing about OUR refresh, which has not run yet."""
    outcome, _ = _judge(
        _state_row(
            status="Scheduled",
            exception="Code: 241. DB::Exception: Memory limit exceeded",
            last_success_epoch=REQUESTED_AT - 7_200,
            last_refresh_epoch=REQUESTED_AT - 100,
        )
    )
    assert outcome is RefreshOutcome.PENDING


def test_a_disabled_view_and_a_missing_row_fail() -> None:
    outcome, detail = _judge(_state_row(status="Disabled"))
    assert outcome is RefreshOutcome.FAILED
    assert "Disabled" in detail
    outcome, detail = _judge(None)
    assert outcome is RefreshOutcome.FAILED
    assert "refreshable" in detail


# --- the poll loop ------------------------------------------------------------------------------


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_the_poll_loop_sleeps_between_pending_polls_and_returns_the_landed_state() -> None:
    client = _FakeCompaniesCurrentClient(states=[[_RUNNING_ROW], [_RUNNING_ROW], [_DONE_ROW]])
    resource = _FakeResource(client)
    clock = _Clock()

    state = wait_for_refresh(
        resource,
        requested_at=REQUESTED_AT,
        log=dg.build_asset_context().log,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )

    assert state.last_success_epoch == REQUESTED_AT + 3
    assert client.statements.count(REFRESH_STATE_SQL) == 3
    assert clock.sleeps == [REFRESH_POLL_INTERVAL.total_seconds()] * 2
    # Each poll is its own short connection: no socket is held open across the wait.
    assert resource.connections == 3


def test_the_poll_loop_gives_up_past_its_budget() -> None:
    client = _FakeCompaniesCurrentClient(states=[[_RUNNING_ROW]])
    clock = _Clock()

    with pytest.raises(TimeoutError, match=QUALIFIED_SE_COMPANIES_SERVING_VIEW):
        wait_for_refresh(
            _FakeResource(client),
            requested_at=REQUESTED_AT,
            log=dg.build_asset_context().log,
            sleep=clock.sleep,
            monotonic=clock.monotonic,
        )
    assert clock.now >= MAX_REFRESH_WAIT.total_seconds()
    assert MAX_REFRESH_WAIT >= timedelta(minutes=60), "prod refreshes take 13-16 min; leave headroom"


def test_the_poll_loop_raises_a_refresh_failure() -> None:
    failed = _state_row(
        exception="Code: 241. DB::Exception: Memory limit exceeded",
        last_success_epoch=REQUESTED_AT - 3_600,
        last_refresh_epoch=REQUESTED_AT + 400,
    )
    client = _FakeCompaniesCurrentClient(states=[[_RUNNING_ROW], [failed]])
    clock = _Clock()

    with pytest.raises(RuntimeError, match="Memory limit exceeded"):
        wait_for_refresh(
            _FakeResource(client),
            requested_at=REQUESTED_AT,
            log=dg.build_asset_context().log,
            sleep=clock.sleep,
            monotonic=clock.monotonic,
        )


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
    assert "database = 'corpscout'" in companies_current.SE_COMPANIES_SERVING_REFRESH_SQL
    assert "view = 'se_companies_serving'" in companies_current.SE_COMPANIES_SERVING_REFRESH_SQL
    assert "toUnixTimestamp(last_success_time)" in companies_current.SE_COMPANIES_SERVING_REFRESH_SQL


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


def test_the_refresh_asset_runs_after_geocoding_publication_and_the_fold() -> None:
    """The serving view reads se_company_address, which only the fold rewrites. Without the
    fold in its dependencies the weekly could refresh the view before the fold landed and
    serve last week's coordinates for another week. The other three dependencies are the
    centroids, the geocode-cache warm and the domain publication (has_domains)."""
    assert {key.path[-1] for key in sweden_companies_current_clickhouse.dependency_keys} == {
        "sweden_geocode_centroids_clickhouse",
        "se_address_geocodes_warm",
        "se_company_domain_publish",
        "se_company_address_publish",
    }


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
