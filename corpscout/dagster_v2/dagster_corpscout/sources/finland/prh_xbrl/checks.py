"""Quality gates for the parsed XBRL tables."""

import dagster as dg

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.sources.finland.prh_xbrl import tables
from dagster_corpscout.sources.finland.prh_xbrl.assets.parsed import statement_tables


@dg.asset_check(asset=statement_tables, name="statement_documents_have_facts")
def statement_documents_have_facts(
    context: dg.AssetCheckExecutionContext, clickhouse: ClickHouseResource
) -> dg.AssetCheckResult:
    """A document with zero facts is a parse problem worth surfacing, not hiding."""
    client = clickhouse.client()
    result = client.query(
        f"SELECT countIf(facts_count = 0), count() FROM {tables.STATEMENT_DOCUMENTS_TABLE} FINAL"
    )
    empty_documents, total_documents = result.result_rows[0]
    return dg.AssetCheckResult(
        passed=int(empty_documents) == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "empty_documents": int(empty_documents),
            "total_documents": int(total_documents),
        },
    )
