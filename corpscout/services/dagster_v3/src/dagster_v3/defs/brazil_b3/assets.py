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
from dagster_v3.defs.brazil_b3.source import (
    build_b3_instrument_rows,
    build_b3_listing_rows,
    fetch_b3_company_detail,
    fetch_b3_listings,
    parse_b3_instruments,
)
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


def _replace_table(client, table: str, columns, rows) -> None:
    qualified = f"`{tables.CLICKHOUSE_DATABASE}`.`{table}`"
    stage = f"`{tables.CLICKHOUSE_DATABASE}`.`_tmp_{table}_{uuid.uuid4().hex}`"
    try:
        client.execute(f"CREATE TABLE {stage} AS {qualified}")
        client.execute(f"INSERT INTO {stage} ({', '.join(columns)}) VALUES", rows)
        client.execute(f"EXCHANGE TABLES {stage} AND {qualified}")
    finally:
        client.execute(f"DROP TABLE IF EXISTS {stage}")


@dg.asset(
    deps=[dg.AssetKey("brazil_b3_listings_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "reference"},
    description=(
        "Every B3 trading code with its ISIN, per company — the authoritative "
        "mapping the ticker-root prefix was standing in for."
    ),
)
def brazil_b3_instruments_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(tables.BR_B3_INSTRUMENTS_TABLE,),
    )
    # One detail call per issuer, so only issuers that actually carry a trading
    # code are asked for — the ETFs and code-less registrations have nothing to
    # return and would just be requests.
    with clickhouse.get_connection() as client:
        codes = [
            str(row[0])
            for row in client.execute(
                f"SELECT DISTINCT cvm_code FROM "
                f"`{tables.CLICKHOUSE_DATABASE}`.`{tables.BR_B3_LISTINGS_TABLE}` "
                f"WHERE cvm_code != '' AND ticker_root != '' ORDER BY cvm_code"
            )
        ]
    context.log.info("fetching instrument detail for %d issuers", len(codes))

    instruments = []
    failures = 0
    for index, code in enumerate(codes, start=1):
        try:
            instruments.extend(parse_b3_instruments(fetch_b3_company_detail(code)))
        except Exception as exc:  # noqa: BLE001 - one issuer must not fail the load
            failures += 1
            context.log.warning("B3 detail failed for codeCVM=%s: %s", code, exc)
        if index % 250 == 0:
            context.log.info("  %d/%d issuers, %d instruments", index, len(codes), len(instruments))

    rows = build_b3_instrument_rows(instruments, source_run_id=context.run_id)
    if len(rows) < tables.MIN_B3_INSTRUMENTS:
        raise ValueError(
            f"B3 detail yielded {len(rows)} instruments, below the "
            f"{tables.MIN_B3_INSTRUMENTS} floor"
        )
    with clickhouse.get_connection() as client:
        _replace_table(
            client, tables.BR_B3_INSTRUMENTS_TABLE, tables.BR_B3_INSTRUMENTS_COLUMNS, rows
        )

    with_isin = sum(1 for row in rows if row[4])
    return dg.MaterializeResult(
        metadata={
            "issuers_requested": len(codes),
            "instruments": len(rows),
            "with_isin": with_isin,
            "issuer_detail_failures": failures,
        }
    )


brazil_b3_job = dg.define_asset_job(
    name="brazil_b3_job",
    selection=dg.AssetSelection.assets(
        brazil_b3_listings_clickhouse, brazil_b3_instruments_clickhouse
    ),
)

brazil_b3_daily = dg.ScheduleDefinition(
    name="brazil_b3_daily",
    job=brazil_b3_job,
    cron_schedule="15 6 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
