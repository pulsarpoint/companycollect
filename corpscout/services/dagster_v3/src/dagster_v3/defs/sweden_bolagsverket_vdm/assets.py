from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from pydantic import Field

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_bolagsverket_vdm import normalize, source, tables
from dagster_v3.defs.sweden_bolagsverket_vdm.clickhouse import (
    append_observations_to_clickhouse,
)
from dagster_v3.defs.sweden_bolagsverket_vdm.resources import (
    BolagsverketVdmResource,
)

GROUP_NAME = "sweden_bolagsverket_vdm"
SOURCE_TAGS = {"country": "se", "source": "bolagsverket_vdm"}
RAW_ASSET_KEY = "sweden_bolagsverket_vdm_raw_responses_s3"
DUCKDB_ASSET_KEY = "sweden_bolagsverket_vdm_observations_duckdb"
CLICKHOUSE_ASSET_KEY = "sweden_bolagsverket_vdm_clickhouse"


class BolagsverketTargetedRefreshConfig(dg.Config):
    company_ids: list[str]
    request_delay_seconds: float = Field(default=0.25, ge=0, le=30)


@dg.asset(
    name=RAW_ASSET_KEY,
    group_name=GROUP_NAME,
    kinds={"python", "s3", "json", "bolagsverket"},
    tags={**SOURCE_TAGS, "layer": "raw"},
    description=(
        "On-demand, bounded Bolagsverket VDM organisation and document-list "
        "responses persisted immutably for configured company identities."
    ),
)
def sweden_bolagsverket_vdm_raw_responses_s3(
    context: dg.AssetExecutionContext,
    config: BolagsverketTargetedRefreshConfig,
    sweden_bolagsverket_vdm_api: BolagsverketVdmResource,
    sweden_bolagsverket_vdm_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    result = source.sync_selected_companies(
        object_store=sweden_bolagsverket_vdm_object_store,
        api=sweden_bolagsverket_vdm_api,
        company_ids=config.company_ids,
        run_id=context.run_id,
        request_delay_seconds=config.request_delay_seconds,
        observed_at=datetime.now(UTC),
    )
    context.log.info(
        "Stored targeted Bolagsverket VDM responses: companies=%s responses=%s",
        result.requested_company_count,
        result.raw_response_count,
    )
    return dg.MaterializeResult(metadata=result.metadata())


@dg.asset(
    name=DUCKDB_ASSET_KEY,
    deps=[dg.AssetKey(RAW_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "json", "bolagsverket"},
    tags={**SOURCE_TAGS, "layer": "normalized"},
    pool=tables.DUCKDB_POOL,
    description=(
        "Normalizes the current targeted refresh's persisted raw responses into "
        "company-state and annual-report-document observations."
    ),
)
def sweden_bolagsverket_vdm_observations_duckdb(
    context: dg.AssetExecutionContext,
    sweden_bolagsverket_vdm_object_store: ObjectStoreResource,
    sweden_bolagsverket_vdm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    tables.DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sweden_bolagsverket_vdm_duckdb.get_connection() as connection:
        counts = normalize.load_observations_from_object_store(
            connection=connection,
            object_store=sweden_bolagsverket_vdm_object_store,
            run_id=context.run_id,
        )
    return dg.MaterializeResult(
        metadata={**counts, "duckdb_path": str(tables.DUCKDB_PATH)}
    )


@dg.asset(
    name=CLICKHOUSE_ASSET_KEY,
    deps=[dg.AssetKey(DUCKDB_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "bolagsverket"},
    tags={**SOURCE_TAGS, "layer": "published"},
    pool=tables.DUCKDB_POOL,
    metadata={
        "company_table": tables.QUALIFIED_COMPANY_OBSERVATIONS_TABLE,
        "document_table": tables.QUALIFIED_DOCUMENT_OBSERVATIONS_TABLE,
    },
    description=(
        "Appends a selected Bolagsverket VDM refresh to source-specific "
        "ClickHouse observation tables."
    ),
)
def sweden_bolagsverket_vdm_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_bolagsverket_vdm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with sweden_bolagsverket_vdm_duckdb.get_connection() as connection:
        counts = append_observations_to_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "company_table": tables.QUALIFIED_COMPANY_OBSERVATIONS_TABLE,
            "document_table": tables.QUALIFIED_DOCUMENT_OBSERVATIONS_TABLE,
        }
    )


# Intentionally on-demand: launch with selected company_ids after a stale/missing
# profile is identified. It is not scheduled and never runs in a page request.
sweden_bolagsverket_vdm_targeted_refresh_job = dg.define_asset_job(
    "sweden_bolagsverket_vdm_targeted_refresh_job",
    selection=dg.AssetSelection.assets(CLICKHOUSE_ASSET_KEY).upstream(),
)


defs = dg.Definitions(
    assets=[
        sweden_bolagsverket_vdm_raw_responses_s3,
        sweden_bolagsverket_vdm_observations_duckdb,
        sweden_bolagsverket_vdm_clickhouse,
    ],
    jobs=[sweden_bolagsverket_vdm_targeted_refresh_job],
    resources={
        "sweden_bolagsverket_vdm_api": BolagsverketVdmResource(),
        "sweden_bolagsverket_vdm_object_store": ObjectStoreResource(
            bucket=source.RAW_BUCKET
        ),
        "sweden_bolagsverket_vdm_duckdb": duckdb_resource(tables.DUCKDB_PATH),
    },
)
