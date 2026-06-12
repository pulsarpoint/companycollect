"""Parsed layer: re-parse the partition's raw XML from RustFS into ClickHouse.

Rebuildable from object storage forever — never touches the PRH API.
Re-runs are safe: all target tables are ReplacingMergeTree keyed on statement
identity, versioned by parsed_at.
"""

from datetime import datetime, timezone

import dagster as dg

from dagster_corpscout.lib.automation import eager_partition_cascade
from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec, tables
from dagster_corpscout.sources.finland.prh_xbrl.assets.raw import raw_xml_documents
from dagster_corpscout.sources.finland.prh_xbrl.importer import load_rows
from dagster_corpscout.sources.finland.prh_xbrl.parser import parse_statement_xml
from dagster_corpscout.sources.finland.prh_xbrl.partitions import registration_month_partitions


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="statement_tables",
    partitions_def=registration_month_partitions,
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "parsed"},
    deps=[raw_xml_documents],
    automation_condition=eager_partition_cascade(),
    retry_policy=dg.RetryPolicy(max_retries=2, delay=120, backoff=dg.Backoff.EXPONENTIAL),
    op_tags={"dagster/concurrency_key": f"{spec.SOURCE_NAME}:clickhouse"},
)
def statement_tables(
    context: dg.AssetExecutionContext,
    rustfs: RustFSResource,
    clickhouse: ClickHouseResource,
) -> dg.MaterializeResult:
    listing = rustfs.get_json(spec.BUCKET, spec.window_listing_object_key(context.partition_key))
    parsed_at = datetime.now(timezone.utc)

    rows_by_table: dict[str, list[dict]] = {
        tables.STATEMENT_DOCUMENTS_TABLE: [],
        tables.CONTEXTS_TABLE: [],
        tables.UNITS_TABLE: [],
        tables.FACTS_TABLE: [],
    }
    warnings: list[str] = []
    for entry in listing["documents"]:
        body = rustfs.get_bytes(spec.BUCKET, entry["object_key"])
        parsed = parse_statement_xml(
            business_id=entry["business_id"],
            financial_date=entry["financial_date"],
            registration_date=entry.get("registration_date"),
            source_url=entry["source_url"],
            xml_object_key=entry["object_key"],
            source_run_id=context.run.run_id,
            body=body,
            parsed_at=parsed_at,
        )
        for table, rows in parsed.rows_by_table.items():
            rows_by_table[table].extend(rows)
        warnings.extend(parsed.warnings)

    counts = load_rows(clickhouse, rows_by_table)
    for warning in warnings:
        context.log.warning(warning)

    return dg.MaterializeResult(
        metadata={
            "documents_count": len(listing["documents"]),
            "warnings_count": len(warnings),
            **{f"rows_{table}": count for table, count in counts.items()},
        }
    )
