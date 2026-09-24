"""Refresh `corpscout.se_companies_serving` after geocoding or domain publication.

Migration 000335 makes `se_companies_serving` a REFRESHABLE MATERIALIZED VIEW that ClickHouse
rebuilds on its own; 000366 moved that cadence to HOURLY, OFFSET 45 MINUTE (restated by 000393's
MODIFY QUERY, which carried the same schedule forward), so nothing here computes its contents.
What this asset does is tighten the timing: the weekly job has just republished the coarse
centroids and warmed the address entity's geocode cache against the new OSM extract. Left to the
hourly schedule, the view would keep serving the PREVIOUS week's join for up to an hour after
that data landed. Issuing `SYSTEM REFRESH VIEW` forces an immediate rebuild, and this asset then
waits for it to land so the admin surfaces read fresh rows the moment the run finishes rather
than at the next auto-refresh.

HOW IT WAITS. Not with `SYSTEM WAIT VIEW`. That statement holds one client socket silent until
the rebuild finishes, and the rebuild takes 13-16 minutes on prod (2026-09-21: 47 refreshes,
median 14.9 min) while the driver's default read timeout is 300 s -- so every run died with
`TimeoutError: timed out` although ClickHouse completed the refresh regardless, and Dagster's
run retries then queued two more full rebuilds. Instead the asset reads the server clock, asks
for the refresh, and polls `system.view_refreshes` with a short query every
REFRESH_POLL_INTERVAL until `last_success_time` -- the START of the latest successful refresh
-- is at or after the request. Only a refresh that started after the request can include the
upstream data this run just landed; an in-flight refresh that started earlier does not count,
and ClickHouse queues ours behind it. A refresh attempt that fails after the request raises with
the server's message, as SYSTEM WAIT VIEW would have. A view left Disabled by SYSTEM STOP VIEW
fails before the request, because REFRESH VIEW is a no-op on it. MAX_REFRESH_WAIT bounds the
whole wait so a refresh that never starts cannot hang the run forever.

Both statements run against a name ClickHouse owns -- there is no DuckDB work and no pool to
take. `deps` are the centroids, the address entity's geocode-cache warm, the address fold
(`se_company_address_publish`, which rewrites `corpscout.se_company_address` -- the table the
serving view actually reads for addresses and coordinates) and domain publication, which
updates the current domains counted by `has_domains`. The fold dependency is what makes this
asset run LAST in the weekly chain (spec
docs/superpowers/specs/2026-09-24-address-weekly-chain-design.md): without it the view could
refresh before the fold landed and serve last week's coordinates for another week. Selecting
this asset downstream of domain publication refreshes the company list in the same run.

The refresh-health check hangs off THIS asset: the view is never materialized, so its only
observable health is its refresh state in `system.view_refreshes`, and a
refresh that quietly stops (throws, never succeeds, or falls three intervals behind) leaves the
serving name answering fast with contents that stop advancing. The predicate and its SQL live in
`companies_current.py` beside the builder so both stay importable and unit-testable.
"""

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.sweden_company import companies_current
from dagster_v3.defs.sweden_company.centroid_assets import CENTROIDS_ASSET_KEY

GROUP_NAME = "sweden_company"

COMPANIES_CURRENT_ASSET_KEY = "sweden_companies_current_clickhouse"
WARM_ASSET_KEY = "se_address_geocodes_warm"
ADDRESS_PUBLISH_ASSET_KEY = "se_company_address_publish"

QUALIFIED_SE_COMPANIES_SERVING_VIEW = f"{companies_current.CLICKHOUSE_DATABASE}.{companies_current.SE_COMPANIES_SERVING_VIEW}"

REFRESH_VIEW_SQL = f"SYSTEM REFRESH VIEW {QUALIFIED_SE_COMPANIES_SERVING_VIEW}"
ROW_COUNT_SQL = f"SELECT count() FROM {QUALIFIED_SE_COMPANIES_SERVING_VIEW}"
# The server's clock, not the worker's: it is compared against system.view_refreshes times.
SERVER_NOW_SQL = "SELECT toUnixTimestamp(now())"
# last_success_time is when the latest SUCCESSFUL refresh STARTED; last_refresh_time is when the
# latest attempt (success or failure) finished. Both are Nullable until the view has run once.
REFRESH_STATE_SQL = f"""SELECT
    status,
    exception,
    toUnixTimestamp(last_success_time),
    toUnixTimestamp(last_refresh_time),
    last_success_duration_ms,
    progress
FROM system.view_refreshes
WHERE database = '{companies_current.CLICKHOUSE_DATABASE}'
  AND view = '{companies_current.SE_COMPANIES_SERVING_VIEW}'"""

REFRESH_POLL_INTERVAL = timedelta(seconds=30)
# Prod refreshes take 13-16 min; the 2026-09-08 post-refold render took 27 min. In the overlap
# case (a scheduled refresh already running when we ask) ours is queued behind it, so budget two.
MAX_REFRESH_WAIT = timedelta(minutes=90)


@dataclass(frozen=True)
class RefreshState:
    """One row of system.view_refreshes for the serving view, or its absence."""

    status: str
    exception: str
    last_success_epoch: int | None
    last_refresh_epoch: int | None
    last_success_duration_ms: int | None
    progress: float

    @classmethod
    def from_rows(cls, rows: Sequence[tuple[Any, ...]]) -> "RefreshState | None":
        if not rows:
            return None
        [(status, exception, success_epoch, refresh_epoch, duration_ms, progress)] = rows
        return cls(
            status=str(status),
            exception=str(exception or ""),
            last_success_epoch=int(success_epoch) if success_epoch is not None else None,
            last_refresh_epoch=int(refresh_epoch) if refresh_epoch is not None else None,
            last_success_duration_ms=int(duration_ms) if duration_ms is not None else None,
            progress=float(progress),
        )


class RefreshOutcome(Enum):
    PENDING = "pending"
    DONE = "done"
    FAILED = "failed"


def judge_refresh(
    state: RefreshState | None, *, requested_at: int
) -> tuple[RefreshOutcome, str]:
    """Decide what one system.view_refreshes reading says about the refresh requested at
    `requested_at` (server epoch seconds). Returns the outcome and a human-readable detail."""
    if state is None:
        return (
            RefreshOutcome.FAILED,
            f"{QUALIFIED_SE_COMPANIES_SERVING_VIEW} is not a refreshable materialized view on "
            "this server -- system.view_refreshes has no row for it",
        )
    if state.status == "Disabled":
        return (
            RefreshOutcome.FAILED,
            f"{QUALIFIED_SE_COMPANIES_SERVING_VIEW} is Disabled (SYSTEM STOP VIEW); "
            "SYSTEM REFRESH VIEW is a no-op until SYSTEM START VIEW",
        )
    if state.last_success_epoch is not None and state.last_success_epoch >= requested_at:
        return (
            RefreshOutcome.DONE,
            f"refresh started at {_iso(state.last_success_epoch)} succeeded",
        )
    if (
        state.exception
        and state.status != "Running"
        and state.last_refresh_epoch is not None
        and state.last_refresh_epoch >= requested_at
    ):
        return (
            RefreshOutcome.FAILED,
            f"refresh attempt finished at {_iso(state.last_refresh_epoch)} failed: "
            f"{state.exception}",
        )
    return (
        RefreshOutcome.PENDING,
        f"status={state.status} progress={state.progress:.2f} "
        f"last_success_started={_iso(state.last_success_epoch)}",
    )


def wait_for_refresh(
    clickhouse: Any,
    *,
    requested_at: int,
    log: Any,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    poll_interval: timedelta = REFRESH_POLL_INTERVAL,
    max_wait: timedelta = MAX_REFRESH_WAIT,
) -> RefreshState:
    """Poll system.view_refreshes until a refresh started at/after `requested_at` succeeds.

    Each poll opens its own connection and runs one instant query, so no socket is ever held
    open long enough for the driver's read timeout to matter. Raises RuntimeError when the
    refresh fails or the view is not refreshable, TimeoutError past `max_wait`.
    """
    started = monotonic()
    while True:
        with clickhouse.get_connection() as client:
            state = RefreshState.from_rows(client.execute(REFRESH_STATE_SQL))
        outcome, detail = judge_refresh(state, requested_at=requested_at)
        elapsed = monotonic() - started
        if outcome is RefreshOutcome.DONE:
            log.info("%s: %s after %.0fs", QUALIFIED_SE_COMPANIES_SERVING_VIEW, detail, elapsed)
            assert state is not None
            return state
        if outcome is RefreshOutcome.FAILED:
            raise RuntimeError(f"{QUALIFIED_SE_COMPANIES_SERVING_VIEW}: {detail}")
        if elapsed >= max_wait.total_seconds():
            raise TimeoutError(
                f"{QUALIFIED_SE_COMPANIES_SERVING_VIEW}: no refresh started at or after "
                f"{_iso(requested_at)} succeeded within {max_wait} ({detail})"
            )
        log.info(
            "%s: waiting for refresh (%s, %.0fs elapsed)",
            QUALIFIED_SE_COMPANIES_SERVING_VIEW,
            detail,
            elapsed,
        )
        sleep(poll_interval.total_seconds())


def _iso(epoch_seconds: int | None) -> str:
    if epoch_seconds is None:
        return "never"
    return datetime.fromtimestamp(epoch_seconds, tz=UTC).isoformat()


@dg.asset(
    name=COMPANIES_CURRENT_ASSET_KEY,
    deps=[
        dg.AssetKey(CENTROIDS_ASSET_KEY),
        dg.AssetKey(WARM_ASSET_KEY),
        dg.AssetKey(ADDRESS_PUBLISH_ASSET_KEY),
        dg.AssetKey("se_company_domain_publish"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    metadata={"view": QUALIFIED_SE_COMPANIES_SERVING_VIEW},
    description=(
        "Refresh corpscout.se_companies_serving after geocoding or domain publication "
        "and poll system.view_refreshes until that refresh lands. Updates company-list "
        "data, including has_domains for current unverified source candidates. Select "
        "this downstream asset with se_company_domain_publish to update the company list "
        "in the same run."
    ),
)
def sweden_companies_current_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    """Request a refresh of the companies serving view now, and poll until it lands.

    `assert_clickhouse_tables_exist` runs first so a pre-migration run fails clearly on the
    view's name rather than deep inside a SYSTEM statement. The view's refresh state is read
    before the request so a stopped view fails up front, then the server clock is read and the
    refresh requested on one short connection. The wait itself is `wait_for_refresh`, a poll
    loop of instant queries -- see the module docstring for why not SYSTEM WAIT VIEW. The row
    count is read only after a refresh that STARTED at or after the request has succeeded, so
    it reflects this run's upstream data.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=companies_current.CLICKHOUSE_DATABASE,
        tables=(companies_current.SE_COMPANIES_SERVING_VIEW,),
    )
    with clickhouse.get_connection() as client:
        before = RefreshState.from_rows(client.execute(REFRESH_STATE_SQL))
        if before is None or before.status == "Disabled":
            _, detail = judge_refresh(before, requested_at=0)
            raise RuntimeError(f"{QUALIFIED_SE_COMPANIES_SERVING_VIEW}: {detail}")
        [(requested_at,)] = client.execute(SERVER_NOW_SQL)
        requested_at = int(requested_at)
        client.execute(REFRESH_VIEW_SQL)
    if before.status == "Running":
        context.log.info(
            "%s: a refresh is already running (progress %.2f); ClickHouse queues ours after it",
            QUALIFIED_SE_COMPANIES_SERVING_VIEW,
            before.progress,
        )
    landed = wait_for_refresh(clickhouse, requested_at=requested_at, log=context.log)
    with clickhouse.get_connection() as client:
        [(row_count,)] = client.execute(ROW_COUNT_SQL)
    refresh_seconds = (
        landed.last_success_duration_ms / 1000.0
        if landed.last_success_duration_ms is not None
        else None
    )
    context.log.info(
        "Refreshed %s: %s rows (refresh took %ss)",
        QUALIFIED_SE_COMPANIES_SERVING_VIEW,
        row_count,
        refresh_seconds,
    )
    return dg.MaterializeResult(
        metadata={
            "view": QUALIFIED_SE_COMPANIES_SERVING_VIEW,
            "row_count": int(row_count),
            "refresh_started_at": _iso(landed.last_success_epoch),
            "refresh_duration_seconds": refresh_seconds,
        }
    )


@dg.asset_check(
    asset=sweden_companies_current_clickhouse,
    name="companies_current_view_is_being_refreshed",
    description=(
        "Fails when the Sweden companies serving view has stopped refreshing: its last "
        "refresh raised, or it has not succeeded within three times its refresh interval, "
        "or it has never succeeded at all, or it is not a refreshable view on this server."
    ),
)
def sweden_companies_current_refresh_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """The only thing watching corpscout.se_companies_serving, mirroring the store view's check.

    Like se_address_geocodes_current (migration 000320), this view is never materialized by an
    asset that recomputes it -- migration 000335 hands it to ClickHouse's 15-minute refresh -- so its
    health is not a freshness signal but its refresh state in system.view_refreshes. This asset's
    forced refresh runs once a week; the check reports whether the 15-minute refresh in between is
    still landing.

    A MISSING ROW IS A FAILURE, not "no data yet": the WHERE names one database and one view, and
    ClickHouse lists every refreshable view it knows. No row means se_companies_serving is not a
    refreshable view on this server -- migration 000335 not applied, or the view replaced by a
    table -- and a check that shrugged at an empty result would leave the serving name unwatched.
    """
    checked_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        rows = client.execute(companies_current.SE_COMPANIES_SERVING_REFRESH_SQL)
    if not rows:
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "view": QUALIFIED_SE_COMPANIES_SERVING_VIEW,
                "refresh_row_found": False,
                "detail": (
                    f"{QUALIFIED_SE_COMPANIES_SERVING_VIEW} is not a refreshable materialized "
                    "view on this server -- system.view_refreshes has no row for it (migration "
                    "000335 not applied, or the view was replaced)"
                ),
            },
        )
    [(status, exception, last_success_epoch_seconds)] = rows
    last_success = (
        datetime.fromtimestamp(int(last_success_epoch_seconds), tz=UTC)
        if last_success_epoch_seconds is not None
        else None
    )
    return dg.AssetCheckResult(
        passed=companies_current.companies_current_refresh_is_healthy(
            row_found=True,
            exception=str(exception),
            last_success_epoch_seconds=last_success_epoch_seconds,
            now=checked_at,
        ),
        metadata={
            "view": QUALIFIED_SE_COMPANIES_SERVING_VIEW,
            "refresh_row_found": True,
            "status": str(status),
            "exception": str(exception),
            "last_success_at": (
                last_success.isoformat() if last_success is not None else None
            ),
            "refresh_age_hours": (
                (checked_at - last_success).total_seconds() / 3600
                if last_success is not None
                else None
            ),
            "maximum_refresh_age_hours": (
                companies_current.MAX_COMPANIES_SERVING_REFRESH_AGE.total_seconds()
                / 3600
            ),
        },
    )
