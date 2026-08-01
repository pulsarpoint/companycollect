"""Resolve every country's government contracts once a day, into one table.

The per-country `*_government_contracts` views join a national register to
resolve a TED winner, and the contracts pages read them directly -- so a single
page load pays for that join several times: a count, a page of rows, and one
aggregate per facet. Migration 000233 stopped the join exhausting memory, but
not running: Sweden streams 3.4M register rows per facet query at 8.6s a page,
and Brazil, which joins no register at all, takes 35s on volume alone.

Same move company_market_summary already made for markets -- do the work on a
schedule, let the request read a table.

The views stay the source. They hold each country's identity rules, and a
second copy in Python would drift from them silently, which is how this
codebase has been bitten before.
"""

import uuid
from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
)
from dagster_v3.defs.company_contracts import tables
from dagster_v3.defs.company_contracts.rollup_sql import build_rollup_select

GROUP_NAME = "company_contracts"

# Stored, not derived: every query in contracts.server groups by this exact
# expression, so making it a column puts it in the sort key.
CONTRACT_REF = "if(contract_key != '', contract_key, contract_id)"


def facts_insert_sql(
    *, target: str, view: str, resolved_at: str, columns: tuple[str, ...]
) -> str:
    """One country's contracts, shaped for the facts table.

    The SELECT list is built from CONTRACT_FACTS_COLUMNS so it cannot drift out
    of order from the INSERT list beside it -- the two are the same tuple,
    with contract_ref and resolved_at computed where they sit in it.
    """
    selected = ", ".join(
        f"{CONTRACT_REF} AS contract_ref"
        if column == "contract_ref"
        else f"toDateTime('{resolved_at}') AS resolved_at"
        if column == "resolved_at"
        else column
        for column in columns
    )
    return f"""
INSERT INTO {target} ({', '.join(columns)})
SELECT {selected}
FROM {view}
"""


@dg.asset(
    name="company_contract_facts",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql"},
    metadata={"tables": [f"{RESOLVED_DATABASE}.{tables.CONTRACT_FACTS_TABLE}"]},
    description=(
        "Every country's government contracts resolved into one table, so a "
        "contracts page reads rows instead of re-running a register join."
    ),
)
def company_contract_facts(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.CONTRACT_FACTS_TABLES,
    )
    qualified = f"`{RESOLVED_DATABASE}`.`{tables.CONTRACT_FACTS_TABLE}`"
    stage = (
        f"`{RESOLVED_DATABASE}`."
        f"`_tmp_{tables.CONTRACT_FACTS_TABLE}_{uuid.uuid4().hex}`"
    )
    resolved_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    per_country: dict[str, int] = {}
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {stage} AS {qualified}")
        try:
            for code in tables.CONTRACT_COUNTRIES:
                # The _live view holds the join. Migration 000235 renamed it
                # there and gave the public name to a read of this table, so
                # reading the public name would make this asset its own source.
                view = f"{RESOLVED_DATABASE}.{code}_government_contracts_live"
                client.execute(
                    facts_insert_sql(
                        target=stage,
                        view=view,
                        resolved_at=resolved_at,
                        columns=tables.CONTRACT_FACTS_COLUMNS,
                    )
                )
                rows = client.execute(
                    f"SELECT count() FROM {stage} "
                    f"WHERE country_code = '{code.upper()}'"
                )[0][0]
                per_country[code] = int(rows)
                context.log.info("%s: %d contract rows", code.upper(), rows)

            total = sum(per_country.values())
            if total < tables.MIN_CONTRACT_FACTS_ROWS:
                # Refuse to replace a populated table from a source that has
                # gone empty -- a broken upstream view looks exactly like this,
                # and the pages would show a country with no procurement at all
                # rather than fail.
                raise ValueError(
                    f"contract facts produced {total} rows, below the "
                    f"{tables.MIN_CONTRACT_FACTS_ROWS} floor"
                )

            # EXCHANGE swaps atomically, so a reader never sees the table
            # mid-replacement -- unlike truncate-then-insert.
            client.execute(f"EXCHANGE TABLES {stage} AND {qualified}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")

    return dg.MaterializeResult(
        metadata={"rows": sum(per_country.values()), **per_country}
    )


@dg.asset(
    name="company_contract_award_facts",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql"},
    metadata={"tables": [f"{RESOLVED_DATABASE}.{tables.AWARD_FACTS_TABLE}"]},
    description=(
        "Brazil's and Norway's award-shaped contracts, with the supplier "
        "columns those pages show, resolved into one table."
    ),
)
def company_contract_award_facts(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.AWARD_FACTS_TABLES,
    )
    qualified = f"`{RESOLVED_DATABASE}`.`{tables.AWARD_FACTS_TABLE}`"
    stage = (
        f"`{RESOLVED_DATABASE}`."
        f"`_tmp_{tables.AWARD_FACTS_TABLE}_{uuid.uuid4().hex}`"
    )
    resolved_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    per_country: dict[str, int] = {}
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {stage} AS {qualified}")
        try:
            for code in tables.AWARD_COUNTRIES:
                view = (
                    f"{RESOLVED_DATABASE}.{code}_government_contract_awards_live"
                )
                client.execute(
                    facts_insert_sql(
                        target=stage,
                        view=view,
                        resolved_at=resolved_at,
                        columns=tables.AWARD_FACTS_COLUMNS,
                    )
                )
                rows = client.execute(
                    f"SELECT count() FROM {stage} "
                    f"WHERE country_code = '{code.upper()}'"
                )[0][0]
                per_country[code] = int(rows)
                context.log.info("%s: %d award rows", code.upper(), rows)

            total = sum(per_country.values())
            if total < tables.MIN_AWARD_FACTS_ROWS:
                raise ValueError(
                    f"award facts produced {total} rows, below the "
                    f"{tables.MIN_AWARD_FACTS_ROWS} floor"
                )
            client.execute(f"EXCHANGE TABLES {stage} AND {qualified}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")

    return dg.MaterializeResult(
        metadata={"rows": sum(per_country.values()), **per_country}
    )


def rollup_insert_sql(
    *,
    target: str,
    source: str,
    country_code: str,
    resolved_at: str,
    has_supplier_detail: bool,
) -> str:
    """One country's contracts, rolled up to one row per contract.

    The source is filtered to the country before the aggregation rather than
    after: a rollup of every country at once would group 5.5M rows to answer a
    question each page asks about one of them.

    country_code and resolved_at are not produced by the aggregation -- they
    are selected as literals at their own positions in ROLLUP_COLUMNS, the same
    way facts_insert_sql handles contract_ref and resolved_at. Everything
    between them comes from build_rollup_select, which projects in that same
    order.
    """
    selected = ", ".join(
        f"'{country_code.upper()}' AS country_code"
        if column == "country_code"
        else f"toDateTime('{resolved_at}') AS resolved_at"
        if column == "resolved_at"
        else column
        for column in tables.ROLLUP_COLUMNS
    )
    inner = build_rollup_select(
        source_table=f"(SELECT * FROM {source} WHERE country_code = "
        f"'{country_code.upper()}')",
        has_supplier_detail=has_supplier_detail,
    )
    return f"""
INSERT INTO {target} ({', '.join(tables.ROLLUP_COLUMNS)})
SELECT {selected}
FROM ({inner})
"""


@dg.asset(
    name="company_contract_rollup",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql"},
    deps=[
        dg.AssetKey("company_contract_facts"),
        dg.AssetKey("company_contract_award_facts"),
    ],
    metadata={"tables": [f"{RESOLVED_DATABASE}.{tables.ROLLUP_TABLE}"]},
    description=(
        "One row per contract, the shape the contracts list renders, so a page "
        "filters and sorts an ordered read instead of aggregating every winner "
        "row in the country first."
    ),
)
def company_contract_rollup(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.ROLLUP_TABLES,
    )
    qualified = f"`{RESOLVED_DATABASE}`.`{tables.ROLLUP_TABLE}`"
    stage = f"`{RESOLVED_DATABASE}`.`_tmp_{tables.ROLLUP_TABLE}_{uuid.uuid4().hex}`"
    resolved_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    per_country: dict[str, int] = {}
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {stage} AS {qualified}")
        try:
            for code in tables.CONTRACT_COUNTRIES:
                # Which facts table by SHAPE, not preference: br and no publish
                # the supplier columns their pages show, and the plain table
                # does not carry them at all.
                awards = code in tables.AWARD_COUNTRIES
                source = (
                    f"{RESOLVED_DATABASE}.{tables.AWARD_FACTS_TABLE}"
                    if awards
                    else f"{RESOLVED_DATABASE}.{tables.CONTRACT_FACTS_TABLE}"
                )
                client.execute(
                    rollup_insert_sql(
                        target=stage,
                        source=source,
                        country_code=code,
                        resolved_at=resolved_at,
                        has_supplier_detail=awards,
                    )
                )
                rows = client.execute(
                    f"SELECT count() FROM {stage} "
                    f"WHERE country_code = '{code.upper()}'"
                )[0][0]
                per_country[code] = int(rows)
                context.log.info("%s: %d contracts", code.upper(), rows)

            total = sum(per_country.values())
            if total < tables.MIN_ROLLUP_ROWS:
                # Same refusal as the facts assets: an emptied upstream would
                # otherwise publish "this country has no procurement at all"
                # rather than fail.
                raise ValueError(
                    f"contract rollup produced {total} rows, below the "
                    f"{tables.MIN_ROLLUP_ROWS} floor"
                )
            client.execute(f"EXCHANGE TABLES {stage} AND {qualified}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")

    return dg.MaterializeResult(
        metadata={"rows": sum(per_country.values()), **per_country}
    )


company_contract_facts_job = dg.define_asset_job(
    name="company_contract_facts_job",
    # The rollup last, and after BOTH facts assets, so the two grains cannot
    # disagree: a rollup built from yesterday's facts would show a contract
    # count the same page's detail view contradicts.
    selection=dg.AssetSelection.assets(
        company_contract_facts, company_contract_award_facts, company_contract_rollup
    ),
)

company_contract_facts_schedule = dg.ScheduleDefinition(
    name="company_contract_facts_daily",
    job=company_contract_facts_job,
    # 04:50, after the register and procurement chains have landed.
    cron_schedule="50 4 * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[
        company_contract_facts,
        company_contract_award_facts,
        company_contract_rollup,
    ],
    jobs=[company_contract_facts_job],
    schedules=[company_contract_facts_schedule],
)
