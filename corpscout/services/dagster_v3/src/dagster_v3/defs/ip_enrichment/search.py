"""The IP search table: refresh requests and its freshness check.

corpscout.ip_enrichment_search (migration 000470) is a refreshable materialized view with
one row per inventory IP and its current enrichment. ClickHouse rebuilds it daily at 03:00
UTC; ip_enrichment_results also asks for a rebuild when a task completes. The request is
fire-and-forget: SYSTEM REFRESH VIEW returns at once, a request during a running rebuild is
coalesced into one follow-up rebuild, and the run never waits (a full build takes tens of
minutes, and a SYSTEM WAIT VIEW with a client timeout already broke the se_companies_serving
asset). A failed request is a warning, never a failed run: the daily refresh catches up.
"""

from datetime import UTC, datetime, timedelta

import dagster as dg
from dagster_clickhouse import ClickhouseResource

SEARCH_DATABASE = "corpscout"
SEARCH_VIEW = "ip_enrichment_search"
SEARCH_RELATION = f"{SEARCH_DATABASE}.{SEARCH_VIEW}"
REFRESH_SQL = f"SYSTEM REFRESH VIEW {SEARCH_RELATION}"
# The daily refresh plus a rebuild's duration and slack: two missed days are a problem,
# one late rebuild on a busy host is not.
MAX_AGE = timedelta(hours=36)
REFRESH_STATE_SQL = f"""SELECT status, exception,
    if(isNull(last_success_time), NULL, toUnixTimestamp(last_success_time))
FROM system.view_refreshes
WHERE database = '{SEARCH_DATABASE}' AND view = '{SEARCH_VIEW}'"""
RESULTS_ASSET = dg.AssetKey("ip_enrichment_results")
CHECK_KEY = dg.AssetCheckKey(RESULTS_ASSET, "ip_enrichment_search_fresh")


def request_search_refresh(client, log) -> bool:
    """Ask ClickHouse to rebuild the search table now; never wait, never raise."""
    try:
        client.execute(REFRESH_SQL)
    except Exception as error:  # noqa: BLE001 - a missed refresh must not fail the run
        log.warning(
            "Could not request a refresh of %s (%s); the daily refresh will pick up "
            "these results.",
            SEARCH_RELATION,
            error,
        )
        return False
    log.info("Requested a refresh of %s (not waiting).", SEARCH_RELATION)
    return True


def search_freshness(
    rows: list[tuple], now: datetime, *, max_age: timedelta = MAX_AGE
) -> dg.AssetCheckResult:
    """Judge one system.view_refreshes reading of the search view.

    Fails (as a warning) when the view is unknown to the server, its last refresh raised,
    it never succeeded, or its last success is older than ``max_age``.
    """
    if not rows:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            description=f"{SEARCH_RELATION} is not a refreshable view on this server "
            "(migration 000470 not applied).",
            metadata={"view": SEARCH_RELATION, "refresh_row_found": False},
        )
    [(status, exception, last_success_epoch)] = rows
    last_success = (
        datetime.fromtimestamp(int(last_success_epoch), UTC)
        if last_success_epoch is not None
        else None
    )
    problems = []
    if exception:
        problems.append(f"the last refresh failed: {exception}")
    if last_success is None:
        problems.append("it has never refreshed successfully")
    elif now - last_success > max_age:
        problems.append(
            f"its last successful refresh was {last_success.isoformat()}, "
            f"more than {int(max_age.total_seconds() // 3600)} hours ago"
        )
    return dg.AssetCheckResult(
        passed=not problems,
        severity=dg.AssetCheckSeverity.WARN,
        description=(
            f"{SEARCH_RELATION} is fresh."
            if not problems
            else f"{SEARCH_RELATION} is stale: " + "; ".join(problems) + "."
        ),
        metadata={
            "view": SEARCH_RELATION,
            "refresh_row_found": True,
            "status": str(status),
            "exception": str(exception or ""),
            "last_success_time": last_success.isoformat() if last_success else "",
            "max_age_hours": int(max_age.total_seconds() // 3600),
        },
    )


@dg.asset_check(
    asset=RESULTS_ASSET,
    name=CHECK_KEY.name,
    description="Warns when corpscout.ip_enrichment_search (the backoffice IP list and the "
    "queue selection) has not refreshed successfully within 36 hours, its last refresh "
    "failed, or the view is missing. Reads system.view_refreshes.",
    blocking=False,
)
def ip_enrichment_search_fresh(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        rows = client.execute(REFRESH_STATE_SQL)
    return search_freshness(rows, datetime.now(UTC))


# Runs the check alone (Dagster UI or dg launch); it also runs with every
# ip_enrichment_results_job run.
ip_enrichment_search_freshness_job = dg.define_asset_job(
    "ip_enrichment_search_freshness_job", selection=dg.AssetSelection.checks(CHECK_KEY)
)
defs = dg.Definitions(
    asset_checks=[ip_enrichment_search_fresh],
    jobs=[ip_enrichment_search_freshness_job],
)
