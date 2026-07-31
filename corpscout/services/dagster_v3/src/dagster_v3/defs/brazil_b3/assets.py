"""B3 listed issuers: the Brazilian company-to-ticker bridge.

Daily, because listings and delistings happen. Everything is a small JSON API
read plus one atomic table swap -- no download staging, no DuckDB.
"""

from datetime import UTC, datetime
import uuid

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.brazil_b3 import tables
from dagster_v3.defs.brazil_b3.source import build_b3_listing_rows, fetch_b3_listings
from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist

GROUP_NAME = "brazil_b3"


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "reference"},
    description=(
        "B3's listed issuers: CNPJ, trading-code root and CVM code, the bridge "
        "from a Brazilian company to its ticker."
    ),
)
def brazil_b3_listings_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(tables.BR_B3_LISTINGS_TABLE,),
    )
    entries = list(fetch_b3_listings())
    rows = build_b3_listing_rows(
        entries,
        source_run_id=context.run_id,
        retrieved_at=datetime.now(UTC).replace(tzinfo=None),
    )
    if len(rows) < tables.MIN_B3_LISTINGS:
        # Refuse to replace on a short read rather than silently unlist Brazil.
        raise ValueError(
            f"B3 returned {len(rows)} listings, below the "
            f"{tables.MIN_B3_LISTINGS} floor"
        )

    qualified = f"`{tables.CLICKHOUSE_DATABASE}`.`{tables.BR_B3_LISTINGS_TABLE}`"
    stage = (
        f"`{tables.CLICKHOUSE_DATABASE}`."
        f"`_tmp_{tables.BR_B3_LISTINGS_TABLE}_{uuid.uuid4().hex}`"
    )
    columns = ", ".join(tables.BR_B3_LISTINGS_COLUMNS)
    with clickhouse.get_connection() as client:
        try:
            client.execute(f"CREATE TABLE {stage} AS {qualified}")
            client.execute(f"INSERT INTO {stage} ({columns}) VALUES", rows)
            client.execute(f"EXCHANGE TABLES {stage} AND {qualified}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")

    with_cnpj = sum(1 for row in rows if row[1])
    traded = sum(1 for row in rows if row[6])
    return dg.MaterializeResult(
        metadata={
            "rows": len(rows),
            "with_cnpj": with_cnpj,
            # A market segment is what separates a listed company from an
            # issuer that merely registered a debenture.
            "with_market_segment": traded,
        }
    )


brazil_b3_job = dg.define_asset_job(
    name="brazil_b3_job",
    selection=dg.AssetSelection.assets(brazil_b3_listings_clickhouse),
)

brazil_b3_daily = dg.ScheduleDefinition(
    name="brazil_b3_daily",
    job=brazil_b3_job,
    cron_schedule="15 6 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
