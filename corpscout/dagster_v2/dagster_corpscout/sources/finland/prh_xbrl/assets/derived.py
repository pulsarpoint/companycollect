"""Normalized layer: curated long-format financial metrics from raw facts.

Rebuildable from ClickHouse alone (never re-downloads, never re-parses XML).
Unpartitioned: it derives over all facts each run; the metrics table is
ReplacingMergeTree keyed on (business_id, financial_date, metric_key,
period_reference), so re-derivation supersedes in place. Move to
INSERT...SELECT inside ClickHouse if full-registry volume makes the
Python round-trip slow.
"""

from datetime import datetime, timezone

import dagster as dg

from dagster_corpscout.lib.automation import eager_rollup_cascade
from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.sources.finland.prh_xbrl import spec, tables
from dagster_corpscout.sources.finland.prh_xbrl.assets.parsed import statement_tables
from dagster_corpscout.sources.finland.prh_xbrl.metrics import (
    METRIC_MAPPINGS,
    derive_metric_rows,
)

# ClickHouse requires: FROM table AS alias FINAL (alias before FINAL).
_FACTS_QUERY = f"""
SELECT
    f.statement_key,
    f.business_id,
    f.financial_date,
    f.concept_qname,
    f.mcy_member_code,
    f.ref_member_code,
    f.fact_ordinal,
    f.numeric_value,
    d.reported_period_start,
    d.reported_period_end
FROM {tables.FACTS_TABLE} AS f FINAL
LEFT JOIN {tables.STATEMENT_DOCUMENTS_TABLE} AS d FINAL USING (statement_key)
WHERE f.value_kind = 'numeric' AND f.numeric_value IS NOT NULL
  AND (f.concept_qname, f.mcy_member_code) IN ({{pairs}})
"""


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="financial_metrics",
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "normalized"},
    deps=[statement_tables],
    automation_condition=eager_rollup_cascade(),
    op_tags={"dagster/concurrency_key": f"{spec.SOURCE_NAME}:clickhouse"},
)
def financial_metrics(
    context: dg.AssetExecutionContext, clickhouse: ClickHouseResource
) -> dg.MaterializeResult:
    client = clickhouse.client()
    # Mapping pairs are static module constants — safe to inline.
    pairs = ", ".join(
        f"('{concept}', '{mcy}')" for _key, _label, concept, mcy in METRIC_MAPPINGS
    )
    result = client.query(_FACTS_QUERY.format(pairs=pairs))
    facts = [dict(zip(result.column_names, row)) for row in result.result_rows]

    rows = derive_metric_rows(facts, derived_at=datetime.now(timezone.utc))
    clickhouse.insert_rows(
        client, tables.METRICS_TABLE, tables.TABLE_COLUMNS[tables.METRICS_TABLE], rows
    )
    return dg.MaterializeResult(
        metadata={"facts_considered": len(facts), "metric_rows": len(rows)}
    )
