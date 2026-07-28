import tempfile
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.eur_usd import apply_eur_usd_conversion
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.france_decp_procurement import tables
from dagster_v3.defs.france_decp_procurement.clickhouse import (
    export_decp_contract_holders,
)
from dagster_v3.defs.france_decp_procurement.normalize import (
    build_contract_holder_candidates,
    replace_raw_table,
)
from dagster_v3.defs.france_decp_procurement.resources import (
    latest_snapshot_manifest,
    sync_decp_snapshot,
)

DUCKDB_PATH = Path("data") / tables.DUCKDB_FILE_NAME


@dg.asset(
    name="france_decp_procurement_raw_snapshot_s3",
    group_name=tables.GROUP_NAME,
    kinds={"python", "csv", "s3"},
    description=(
        "Downloads the complete official DECP contract CSV and stores an "
        "immutable content-addressed S3 snapshot with an audit manifest."
    ),
)
def france_decp_procurement_raw_snapshot_s3(
    context: dg.AssetExecutionContext,
    france_decp_procurement_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    snapshot = sync_decp_snapshot(
        object_store=france_decp_procurement_object_store,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
    )
    return dg.MaterializeResult(
        metadata={
            "source_url": tables.SOURCE_URL,
            "object_key": snapshot.object_key,
            "manifest_key": snapshot.manifest_key,
            "sha256": snapshot.sha256,
            "size_bytes": snapshot.size_bytes,
            "source_rows": snapshot.row_count,
            "downloaded": snapshot.downloaded,
        }
    )


@dg.asset(
    name="france_decp_contract_holders_duckdb",
    deps=[dg.AssetKey("france_decp_procurement_raw_snapshot_s3")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "csv", "s3", "duckdb", "sql"},
    pool=tables.DUCKDB_POOL,
    description=(
        "Expands each DECP contract's three holder slots to one row per "
        "(contract, holder), preserving raw identifiers and contract-level "
        "amounts while normalizing SIRET and French VAT identifiers to SIREN."
    ),
)
def france_decp_contract_holders_duckdb(
    context: dg.AssetExecutionContext,
    france_decp_procurement_duckdb: DuckDBResource,
    france_decp_procurement_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    manifest = latest_snapshot_manifest(france_decp_procurement_object_store)
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="france_decp_parse_") as temp_dir:
        csv_path = Path(temp_dir) / "decp.csv"
        france_decp_procurement_object_store.download_file(
            str(manifest["object_key"]),
            csv_path,
            bucket=tables.S3_BUCKET,
        )
        with france_decp_procurement_duckdb.get_connection() as connection:
            raw_rows = replace_raw_table(
                connection=connection,
                csv_path=csv_path,
                source_run_id=str(manifest["source_run_id"]),
                source_object_key=str(manifest["object_key"]),
                source_retrieved_at=datetime.fromisoformat(
                    str(manifest["retrieved_at"])
                ),
            )
            counts = build_contract_holder_candidates(
                connection=connection,
                source_run_id=context.run_id,
                resolved_at=datetime.now(UTC),
                log=context.log.info,
            )
    return dg.MaterializeResult(metadata={"raw_contracts": raw_rows, **counts})


@dg.asset(
    name="france_decp_contract_holders_usd",
    deps=[dg.AssetKey("france_decp_contract_holders_duckdb")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=tables.DUCKDB_POOL,
    description=(
        "Converts DECP's contract-level EUR montant to USD on notification "
        "date, falling back to publication date. It remains non-attributable "
        "to any individual holder."
    ),
)
def france_decp_contract_holders_usd(
    context: dg.AssetExecutionContext,
    france_decp_procurement_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    with france_decp_procurement_duckdb.get_connection() as connection:
        counts = apply_eur_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            qualified_table=(f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"),
            rate_date_columns=("notification_date", "publication_date"),
            amount_columns=(("contract_amount_eur", "contract_amount_usd"),),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="france_decp_contract_holders_clickhouse",
    deps=[
        dg.AssetKey("france_decp_contract_holders_usd"),
        dg.AssetKey("france_sirene_clickhouse_companies"),
    ],
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=tables.DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_CONTRACT_HOLDERS_TABLE},
    description=(
        "Atomically replaces France's DECP contract-holder table and annotates "
        "exact fr_companies SIREN matches. The published montant remains a "
        "contract-level amount and is never represented as holder spend."
    ),
)
def france_decp_contract_holders_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    france_decp_procurement_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(france_decp_procurement_duckdb) as connection:
        counts = export_decp_contract_holders(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


france_decp_procurement_job = dg.define_asset_job(
    "france_decp_procurement_job",
    selection=dg.AssetSelection.assets(
        "france_decp_contract_holders_clickhouse"
    ).upstream(),
)

france_decp_procurement_schedule = dg.ScheduleDefinition(
    name="france_decp_procurement_schedule",
    job=france_decp_procurement_job,
    cron_schedule="20 5 10 * *",
    execution_timezone="Europe/Paris",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[
        france_decp_procurement_raw_snapshot_s3,
        france_decp_contract_holders_duckdb,
        france_decp_contract_holders_usd,
        france_decp_contract_holders_clickhouse,
    ],
    jobs=[france_decp_procurement_job],
    schedules=[france_decp_procurement_schedule],
    resources={
        "france_decp_procurement_duckdb": duckdb_resource(DUCKDB_PATH),
        "france_decp_procurement_object_store": ObjectStoreResource(
            bucket=tables.S3_BUCKET
        ),
    },
)
