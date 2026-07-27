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
from dagster_v3.defs.company_signals.entity_types import (
    LEGAL_FORM_MAPPINGS,
    is_public_sector,
    label_for,
)
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


ENTITY_TYPES_TABLE = "company_entity_types"
ENTITY_TYPE_COLUMNS = (
    "country_code",
    "legal_form_code",
    "entity_type",
    "entity_type_label",
    "source_label",
    "is_public_sector",
    "resolved_at",
)


@dg.asset(
    name="company_entity_types_clickhouse",
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql"},
    metadata={"tables": [f"{tables.CLICKHOUSE_DATABASE}.{ENTITY_TYPES_TABLE}"]},
    description=(
        "Normalised entity type per (country, legal form), so a municipality "
        "in a company register can be labelled as one and excluded from a "
        "company count deliberately rather than silently. Reference data, "
        "replaced wholesale."
    ),
)
def company_entity_types_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(ENTITY_TYPES_TABLE,),
    )
    qualified = f"`{tables.CLICKHOUSE_DATABASE}`.`{ENTITY_TYPES_TABLE}`"
    stage = (
        f"`{tables.CLICKHOUSE_DATABASE}`.`_tmp_{ENTITY_TYPES_TABLE}_{uuid.uuid4().hex}`"
    )

    resolved_at = datetime.now(UTC)
    rows = [
        (
            mapping.country_code,
            mapping.legal_form_code,
            mapping.entity_type,
            label_for(mapping.entity_type),
            mapping.source_label,
            1 if is_public_sector(mapping.entity_type) else 0,
            resolved_at,
        )
        for mapping in LEGAL_FORM_MAPPINGS
    ]
    if not rows:
        raise ValueError("No legal form mappings declared; refusing to blank the table")

    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {stage} AS {qualified}")
        try:
            client.execute(
                f"INSERT INTO {stage} ({', '.join(ENTITY_TYPE_COLUMNS)}) VALUES", rows
            )
            client.execute(f"EXCHANGE TABLES {stage} AND {qualified}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")

    public = sum(1 for row in rows if row[5] == 1)
    context.log.info(
        "Published %s legal form mappings across %s countries; %s are public sector",
        len(rows),
        len({row[0] for row in rows}),
        public,
    )
    return dg.MaterializeResult(
        metadata={
            "mappings": len(rows),
            "countries": sorted({row[0] for row in rows}),
            "public_sector_forms": public,
        }
    )
