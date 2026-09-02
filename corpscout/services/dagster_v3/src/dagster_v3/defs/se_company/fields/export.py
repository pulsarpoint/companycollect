"""Exports the registry and its generated SQL to corpscout.se_company_field_registry.

One row per field plus one ``field = '*'`` row carrying the wide projection statement, so
both runners -- the Dagster resolve asset and the backoffice's per-company resolve after a
decision -- read every statement they need from this one table with
``argMax(resolve_sql, version)``. Every export is a NEW version of each row
(ReplacingMergeTree(version)), so a registry change becomes effective without deletes,
exactly like se_code_labels_clickhouse re-seeds the label dictionary.

Assets
  se_company_field_registry_clickhouse -> corpscout.se_company_field_registry
"""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.fields.policies import policy_for
from dagster_v3.defs.se_company.fields.registry import INFO_REGISTRY, DatatypeRegistry
from dagster_v3.defs.se_company.fields.sql import render_projection_sql, render_resolve_sql
from dagster_v3.defs.se_company.fields.tables import (
    SE_COMPANY_FIELD_REGISTRY,
    SE_COMPANY_FIELD_REGISTRY_COLUMNS,
)

GROUP_NAME = "se_company_fields"
DATABASE = "corpscout"
PROJECTION_FIELD = "*"
PROJECTION_VALUE_TYPE = "projection"


def registry_rows(registry: DatatypeRegistry, *, rendered_at: datetime) -> list[dict[str, object]]:
    """One dict per field in SE_COMPANY_FIELD_REGISTRY_COLUMNS key order, then the
    projection row. ``rendered_at`` is the row version shared by the whole export."""
    rows: list[dict[str, object]] = []
    for field in registry.fields:
        policy = policy_for(field)
        rows.append({
            "datatype": registry.datatype,
            "country": registry.country,
            "field": field.name,
            "value_type": field.value_type,
            "display_group": field.display_group,
            "structured": field.structured,
            "python_only": field.python_only,
            "sources": list(field.sources),
            "policy_name": policy.name,
            "policy_version": policy.version,
            "resolve_sql": render_resolve_sql(registry, field),
            "registry_version": registry.version,
            "version": rendered_at,
        })
    rows.append({
        "datatype": registry.datatype,
        "country": registry.country,
        "field": PROJECTION_FIELD,
        "value_type": PROJECTION_VALUE_TYPE,
        "display_group": "",
        "structured": False,
        "python_only": False,
        "sources": [],
        "policy_name": "",
        "policy_version": "",
        "resolve_sql": render_projection_sql(registry),
        "registry_version": registry.version,
        "version": rendered_at,
    })
    return rows


@dg.asset(
    name="se_company_field_registry_clickhouse",
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    metadata={"table": SE_COMPANY_FIELD_REGISTRY},
    description=(
        "Exports the SE info field registry (fields, sources in precedence order, policy "
        "bindings) and every generated resolve statement plus the wide projection statement "
        "to corpscout.se_company_field_registry. ReplacingMergeTree(version) + argMax in the "
        "readers make a re-export effective without deletes."
    ),
)
def se_company_field_registry_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=DATABASE, tables=("se_company_field_registry",))
    rows = registry_rows(INFO_REGISTRY, rendered_at=datetime.now(UTC))
    values = [tuple(row[column] for column in SE_COMPANY_FIELD_REGISTRY_COLUMNS) for row in rows]
    columns = ", ".join(SE_COMPANY_FIELD_REGISTRY_COLUMNS)
    with clickhouse.get_connection() as client:
        client.execute(f"INSERT INTO {SE_COMPANY_FIELD_REGISTRY} ({columns}) VALUES", values)
    context.log.info("exported %d registry rows for %s", len(rows), INFO_REGISTRY.version)
    return dg.MaterializeResult(
        metadata={
            "rows": len(rows),
            "fields": len(INFO_REGISTRY.fields),
            "registry_version": INFO_REGISTRY.version,
            "table": SE_COMPANY_FIELD_REGISTRY,
        }
    )


defs = dg.Definitions(assets=[se_company_field_registry_clickhouse])
