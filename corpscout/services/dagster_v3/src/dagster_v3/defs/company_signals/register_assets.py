"""Publish the register descriptions to ClickHouse.

Reference data, not fetched data. It is declared in ``registers.py`` and copied
here so the UI can read it from the warehouse instead of carrying its own copy
-- which is the point of §3.3: the source panel and the source pages should not
each hold a private map of what TED is.

The whole table is replaced each run. It is five rows of prose; there is nothing
to preserve incrementally, and replacing wholesale means a register removed from
the declaration actually disappears rather than lingering.
"""

import uuid
from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_signals import tables
from dagster_v3.defs.company_signals.registers import PROCUREMENT_REGISTERS

REGISTERS_TABLE = "procurement_registers"
REGISTER_COLUMNS = (
    "source_slug",
    "register_name",
    "operator",
    "country_codes",
    "homepage_url",
    "api_or_download_url",
    "retrieval_method",
    "documentation_url",
    "licence",
    "coverage_description",
    "open_tenders_url",
    "grain_description",
    "source_tables",
    "notice_table",
    "notice_key_column",
    "notes",
    "resolved_at",
)


@dg.asset(
    name="procurement_registers_clickhouse",
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql"},
    metadata={"tables": [f"{tables.CLICKHOUSE_DATABASE}.{REGISTERS_TABLE}"]},
    description=(
        "What each procurement register is: operator, licence, what it covers, "
        "its own grain, and where open tenders are advertised. One row per "
        "source rather than per country, because TED is one register serving "
        "three. Deliberately not coverage -- how much of it we hold is per "
        "country and lives in company_signal_coverage."
    ),
)
def procurement_registers_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(REGISTERS_TABLE,),
    )
    qualified = f"`{tables.CLICKHOUSE_DATABASE}`.`{REGISTERS_TABLE}`"
    stage = f"`{tables.CLICKHOUSE_DATABASE}`.`_tmp_{REGISTERS_TABLE}_{uuid.uuid4().hex}`"

    rows = [
        (
            register.source_slug,
            register.register_name,
            register.operator,
            list(register.country_codes),
            register.homepage_url,
            register.api_or_download_url,
            register.retrieval_method,
            register.documentation_url,
            register.licence,
            register.coverage_description,
            register.open_tenders_url,
            register.grain_description,
            list(register.source_tables),
            register.notice_table,
            register.notice_key_column,
            register.notes,
        )
        for register in PROCUREMENT_REGISTERS
    ]
    resolved_at = datetime.now(UTC)
    if not rows:
        raise ValueError(
            "No procurement registers declared; refusing to blank the table"
        )

    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {stage} AS {qualified}")
        try:
            client.execute(
                f"INSERT INTO {stage} "
                f"({', '.join(c for c in REGISTER_COLUMNS if c != 'resolved_at')}, "
                f"resolved_at) VALUES",
                [(*row, resolved_at) for row in rows],
            )
            # EXCHANGE swaps the two atomically, so a reader never sees the
            # table mid-replacement -- unlike a truncate-then-insert.
            client.execute(f"EXCHANGE TABLES {stage} AND {qualified}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")

    context.log.info(
        "Published %s procurement registers: %s",
        len(rows),
        ", ".join(register.source_slug for register in PROCUREMENT_REGISTERS),
    )
    return dg.MaterializeResult(
        metadata={
            "registers": len(rows),
            "sources": [r.source_slug for r in PROCUREMENT_REGISTERS],
            "countries": sorted(
                {code for r in PROCUREMENT_REGISTERS for code in r.country_codes}
            ),
        }
    )
