"""Force-refresh `corpscout.se_companies_current` at the end of the weekly geocoding run.

Migration 000326 makes `se_companies_current` a REFRESHABLE MATERIALIZED VIEW that ClickHouse
rebuilds on its own EVERY 1 HOUR, so nothing here computes its contents. What this asset does is
tighten the timing: the weekly job has just republished the coarse centroids and appended the
week's geocode outcomes to the store (which `se_address_geocodes_current`, and through it
`se_address_geocodes_served`, refreshes from). Left to the hourly schedule, the companies view
would keep serving the PREVIOUS week's join for up to an hour after that data landed. Issuing
`SYSTEM REFRESH VIEW` forces an immediate rebuild and `SYSTEM WAIT VIEW` blocks until it lands, so
the admin surfaces read fresh rows the moment the run finishes rather than at the next auto-refresh.

Both are SYSTEM statements against a name ClickHouse owns -- there is no DuckDB work and no pool to
take. `deps` are the two assets that produce its freshest inputs (the centroids and the store
append), so this asset is ordered LAST in the weekly, after everything the view reads is current.

The refresh-health check hangs off THIS asset, mirroring
`sweden_address_geocodes_serving_view_refresh_check` on the store asset: the view is never
materialized, so its only observable health is its refresh state in `system.view_refreshes`, and a
refresh that quietly stops (throws, never succeeds, or falls three intervals behind) leaves the
serving name answering fast with contents that stop advancing. The predicate and its SQL live in
`companies_current.py` beside the builder so both stay importable and unit-testable.
"""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.sweden_company import companies_current
from dagster_v3.defs.sweden_company.centroid_assets import CENTROIDS_ASSET_KEY

GROUP_NAME = "sweden_company"

COMPANIES_CURRENT_ASSET_KEY = "sweden_companies_current_clickhouse"
GEOCODE_STORE_ASSET_KEY = "sweden_address_geocode_store_clickhouse"

QUALIFIED_SE_COMPANIES_CURRENT_VIEW = (
    f"{companies_current.CLICKHOUSE_DATABASE}.{companies_current.SE_COMPANIES_CURRENT_VIEW}"
)

REFRESH_VIEW_SQL = f"SYSTEM REFRESH VIEW {QUALIFIED_SE_COMPANIES_CURRENT_VIEW}"
WAIT_VIEW_SQL = f"SYSTEM WAIT VIEW {QUALIFIED_SE_COMPANIES_CURRENT_VIEW}"
ROW_COUNT_SQL = f"SELECT count() FROM {QUALIFIED_SE_COMPANIES_CURRENT_VIEW}"


@dg.asset(
    name=COMPANIES_CURRENT_ASSET_KEY,
    deps=[dg.AssetKey(CENTROIDS_ASSET_KEY), dg.AssetKey(GEOCODE_STORE_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    metadata={"view": QUALIFIED_SE_COMPANIES_CURRENT_VIEW},
    description=(
        "Forces an immediate refresh of the corpscout.se_companies_current refreshable "
        "materialized view at the end of the weekly Sweden geocoding run -- SYSTEM REFRESH "
        "VIEW then SYSTEM WAIT VIEW -- so the companies/geocoding admin surfaces read the "
        "week's fresh centroid and geocode data at once rather than waiting up to the "
        "hourly auto-refresh."
    ),
)
def sweden_companies_current_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    """Refresh the companies serving view now, and block until the rebuild lands.

    `assert_clickhouse_tables_exist` runs first so a pre-migration run fails clearly on the
    view's name rather than deep inside a SYSTEM statement. The refresh is forced and then
    WAITED on -- SYSTEM WAIT VIEW returns only once the refreshable view has finished
    materializing -- so the row count read afterwards reflects THIS refresh, and downstream
    readers are guaranteed fresh rows the moment this asset completes.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=companies_current.CLICKHOUSE_DATABASE,
        tables=(companies_current.SE_COMPANIES_CURRENT_VIEW,),
    )
    with clickhouse.get_connection() as client:
        client.execute(REFRESH_VIEW_SQL)
        client.execute(WAIT_VIEW_SQL)
        [(row_count,)] = client.execute(ROW_COUNT_SQL)
    context.log.info(
        "Refreshed %s: %s rows",
        QUALIFIED_SE_COMPANIES_CURRENT_VIEW,
        row_count,
    )
    return dg.MaterializeResult(
        metadata={
            "view": QUALIFIED_SE_COMPANIES_CURRENT_VIEW,
            "row_count": int(row_count),
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
    """The only thing watching corpscout.se_companies_current, mirroring the store view's check.

    Like se_address_geocodes_current (migration 000320), this view is never materialized by an
    asset that recomputes it -- migration 000326 hands it to ClickHouse's hourly refresh -- so its
    health is not a freshness signal but its refresh state in system.view_refreshes. This asset's
    forced refresh runs once a week; the check reports whether the HOURLY refresh in between is
    still landing.

    A MISSING ROW IS A FAILURE, not "no data yet": the WHERE names one database and one view, and
    ClickHouse lists every refreshable view it knows. No row means se_companies_current is not a
    refreshable view on this server -- migration 000326 not applied, or the view replaced by a
    table -- and a check that shrugged at an empty result would leave the serving name unwatched.
    """
    checked_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        rows = client.execute(companies_current.SE_COMPANIES_CURRENT_REFRESH_SQL)
    if not rows:
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "view": QUALIFIED_SE_COMPANIES_CURRENT_VIEW,
                "refresh_row_found": False,
                "detail": (
                    f"{QUALIFIED_SE_COMPANIES_CURRENT_VIEW} is not a refreshable materialized "
                    "view on this server -- system.view_refreshes has no row for it (migration "
                    "000326 not applied, or the view was replaced)"
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
            "view": QUALIFIED_SE_COMPANIES_CURRENT_VIEW,
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
                companies_current.MAX_COMPANIES_CURRENT_REFRESH_AGE.total_seconds() / 3600
            ),
        },
    )
