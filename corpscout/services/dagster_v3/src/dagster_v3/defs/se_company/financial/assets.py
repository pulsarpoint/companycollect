"""Dagster assets of the financial entity. Slice 1 ships the precedence export; the
extractors and the extract job (slice 2), the fold (slice 3) follow in their own modules."""

from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.precedence import precedence_rows

GROUP_NAME = "se_company_financial"


def export_precedence(client: Any, exported_at: datetime) -> tuple[int, int]:
    """Insert every (field, source, precedence) pair as a global rule (company_id '',
    period_key '', decided_by 'code'). Returns (pairs inserted, stale pairs): stale is the
    count of global (field, source) pairs present in ClickHouse that the dictionary no
    longer names at all -- they stay until removed by hand (the export never deletes). A
    pair whose number merely changed is not stale; the insert corrects it. Never touches a
    company-scoped row.

    Read the stored global rows first: when they already equal `precedence_rows()`, insert
    nothing and report 0 pairs (the caller reads that as `unchanged`); otherwise insert every
    pair."""
    wanted = precedence_rows()
    stored = {
        tuple(row)
        for row in client.execute(
            f"SELECT field, source, precedence FROM {tables.QUALIFIED_PRECEDENCE_TABLE} "
            "FINAL WHERE company_id = '' AND period_key = '' AND removed = 0"
        )
    }
    pairs = 0
    if stored != set(wanted):
        rows = [
            ("", "", field, source, precedence, 0, "code", "", exported_at)
            for field, source, precedence in wanted
        ]
        client.execute(
            f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
            f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES",
            rows,
        )
        pairs = len(rows)
    stale = len(
        {(field, source) for field, source, _ in stored}
        - {(field, source) for field, source, _ in wanted}
    )
    return pairs, stale


@dg.asset(
    name="se_company_financial_precedence_clickhouse",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_PRECEDENCE_TABLE},
    description=(
        "Exports FINANCIAL_PRECEDENCE to se_company_financial_precedence as global rules "
        "(company_id '', period_key '') for the fold and the backoffice to read. The Python "
        "dictionary is the only source for these rows; re-run after changing it, then "
        "re-fold every bucket with changed_only false (a precedence change is not a "
        "per-company change). Idle re-materialisation is a no-op: when the stored rows "
        "already match the dictionary, nothing is written and the watermark does not move. "
        "Never touches a company-scoped row."
    ),
)
def se_company_financial_precedence_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE, tables=(tables.PRECEDENCE_TABLE,)
    )
    exported_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        pairs, stale = export_precedence(client, exported_at)
    if stale:
        context.log.warning(
            "%d precedence pairs exist in ClickHouse that the dictionary no longer names; "
            "they stay until removed by hand",
            stale,
        )
    return dg.MaterializeResult(
        metadata={
            "pairs": pairs, "stale_pairs": stale, "unchanged": pairs == 0,
            "table": tables.QUALIFIED_PRECEDENCE_TABLE,
        }
    )
