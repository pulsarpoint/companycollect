import json
import tempfile
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_clickhouse import ClickhouseResource
from exchange_rates import ExchangeRateClient, ExchangeRateRequest

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.norway_brreg_financial import (
    financial_fetches,
    financial_normalize,
)
from dagster_v3.defs.norway_brreg.assets.entity_clickhouse import (
    _drop_affected_stage_table,
    _insert_clickhouse_rows,
    _quote_clickhouse_identifier,
    _quote_clickhouse_qualified_table,
    _quote_duckdb_identifier,
)
from dagster_v3.defs.norway_brreg_financial.constants import (
    GROUP_NAME,
    NORWAY_BRREG_FINANCIAL_BUCKET,
)
from dagster_v3.defs.norway_brreg_financial.assets.financial_fetches import (
    NORWAY_BRREG_FINANCIAL_UPDATE_PARTITIONS,
)
from dagster_v3.defs.norway_brreg.entity_storage import (
    ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS,
    NorwayBrregEntityParquetStorageResource,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
)
from dagster_v3.defs.norway_brreg import resolved_tables as no_tables

FINANCIAL_STATEMENTS_PARQUET_KINDS = {"python", "s3", "parquet", "brreg"}
FINANCIAL_STATEMENTS_CLICKHOUSE_KINDS = {
    "python",
    "parquet",
    "duckdb",
    "clickhouse",
    "brreg",
}
FINANCIAL_STATEMENTS_TABLE = no_tables.NO_FINANCIAL_STATEMENTS_TABLE
FINANCIAL_STATEMENT_COLUMNS = no_tables.RESOLVED_EXPORT_COLUMNS[
    FINANCIAL_STATEMENTS_TABLE
]
FINANCIAL_STATEMENTS_DUCKDB_SCHEMA = "norway_brreg_financial_publish"
FINANCIAL_STATEMENT_REPLACEMENT_KEY_COLUMNS = (
    "source_system",
    "source_record_id",
)
RESPONSE_READ_BATCH_SIZE = 1_000
RESPONSE_READ_WORKERS = 8
STATEMENT_WRITE_BATCH_SIZE = 50_000
_RESPONSE_INDEX_RAW_TABLE = "_norway_response_index_raw"
_RESPONSE_INDEX_TABLE = "_norway_response_index"
_EMPTY_RESPONSE_INDEX_RELATION = "_norway_empty_response_index"
_STATEMENT_STREAM_ORDINAL = "_statement_stream_ordinal"
_STATEMENT_OUTPUT_ORDINAL = "_statement_output_ordinal"
_STATEMENT_DEDUP_RANK = "_statement_dedup_rank"
_FX_STAGE_TABLE = "_norway_financial_statement_fx"
_FX_BATCH_RELATION = "_norway_financial_statement_fx_batch"
_FX_ARROW_SCHEMA = pa.schema(
    [
        pa.field("currency", pa.string(), nullable=False),
        pa.field("period_end_date", pa.date32(), nullable=False),
        pa.field("fx_rate", pa.string(), nullable=False),
        pa.field("fx_rate_date", pa.date32(), nullable=False),
        pa.field("fx_source", pa.string()),
    ]
)

FINANCIAL_STATEMENT_SCHEMA = {
    "country_iso2": pl.Utf8,
    "source_system": pl.Utf8,
    "source_run_id": pl.Utf8,
    "source_record_id": pl.Utf8,
    "org_number": pl.Utf8,
    "legal_name": pl.Utf8,
    "last_submitted_accounts_year": pl.Utf8,
    "filing_id": pl.Int64,
    "journal_number": pl.Utf8,
    "accounts_type": pl.Utf8,
    "legal_form_code": pl.Utf8,
    "is_parent_company": pl.Boolean,
    "period_start_date": pl.Date,
    "period_end_date": pl.Date,
    "fiscal_year": pl.Int64,
    "currency": pl.Utf8,
    "liquidation_accounts": pl.Boolean,
    "statement_layout": pl.Utf8,
    "is_not_audited": pl.Boolean,
    "opted_out_audit": pl.Boolean,
    "is_small_enterprise": pl.Boolean,
    "accounting_rules": pl.Utf8,
    "quality_flag": pl.Utf8,
    "operating_revenue_amount_original": pl.Decimal(38, 6),
    "operating_revenue_amount_usd": pl.Decimal(38, 6),
    "operating_costs_amount_original": pl.Decimal(38, 6),
    "operating_costs_amount_usd": pl.Decimal(38, 6),
    "operating_result_amount_original": pl.Decimal(38, 6),
    "operating_result_amount_usd": pl.Decimal(38, 6),
    "net_financial_items_amount_original": pl.Decimal(38, 6),
    "net_financial_items_amount_usd": pl.Decimal(38, 6),
    "pretax_result_amount_original": pl.Decimal(38, 6),
    "pretax_result_amount_usd": pl.Decimal(38, 6),
    "net_result_amount_original": pl.Decimal(38, 6),
    "net_result_amount_usd": pl.Decimal(38, 6),
    "total_assets_amount_original": pl.Decimal(38, 6),
    "total_assets_amount_usd": pl.Decimal(38, 6),
    "current_assets_amount_original": pl.Decimal(38, 6),
    "current_assets_amount_usd": pl.Decimal(38, 6),
    "fixed_assets_amount_original": pl.Decimal(38, 6),
    "fixed_assets_amount_usd": pl.Decimal(38, 6),
    "equity_amount_original": pl.Decimal(38, 6),
    "equity_amount_usd": pl.Decimal(38, 6),
    "total_debt_amount_original": pl.Decimal(38, 6),
    "total_debt_amount_usd": pl.Decimal(38, 6),
    "current_liabilities_amount_original": pl.Decimal(38, 6),
    "current_liabilities_amount_usd": pl.Decimal(38, 6),
    "long_term_liabilities_amount_original": pl.Decimal(38, 6),
    "long_term_liabilities_amount_usd": pl.Decimal(38, 6),
    "fx_rate_to_usd": pl.Decimal(38, 12),
    "fx_rate_date": pl.Date,
    "fx_source": pl.Utf8,
    "source_url": pl.Utf8,
    "resolved_at": pl.Datetime(time_unit="ms", time_zone="UTC"),
}


@dg.asset(
    name="norway_brreg_financial_statements_snapshot_parquet",
    deps=[dg.AssetKey("norway_brreg_financial_bootstrap_responses_parquet")],
    group_name=GROUP_NAME,
    kinds=FINANCIAL_STATEMENTS_PARQUET_KINDS,
    description=(
        "Builds native-currency Norway Brreg resolved financial statement parquet "
        "from the verified historical response index and source JSON objects."
    ),
)
def norway_brreg_financial_statements_snapshot_parquet(
    context,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Building Norway Brreg snapshot financial statements parquet from the "
        "historical response index"
    )
    with tempfile.TemporaryDirectory(
        prefix="norway-brreg-financial-statements-snapshot-"
    ) as temporary_directory:
        work_directory = Path(temporary_directory)
        response_index_paths = _download_historical_response_indexes(
            norway_brreg_financial_storage,
            target_directory=work_directory / "response-indexes",
            log=context.log.info,
        )
        output_path = work_directory / "financial_statements.parquet"
        counts = _build_original_statement_parquet(
            response_index_paths=response_index_paths,
            output_path=output_path,
            database_path=work_directory / "response-index.duckdb",
            storage=norway_brreg_financial_storage,
            log=context.log.info,
        )
        output_key = norway_brreg_financial_storage.upload_snapshot_statements(
            output_path,
            log=context.log.info,
        )
    context.log.info(
        "Completed Norway Brreg snapshot financial statements parquet: "
        "response_index_rows=%d successful_responses=%d statement_rows=%d s3_key=%s",
        counts["fetch_row_count"],
        counts["successful_fetch_count"],
        counts["statement_row_count"],
        output_key,
    )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
            "s3_key": output_key,
        }
    )


@dg.asset(
    name="norway_brreg_financial_statements_updates_parquet",
    deps=[dg.AssetKey("norway_brreg_financial_responses_updates_parquet")],
    group_name=GROUP_NAME,
    kinds=FINANCIAL_STATEMENTS_PARQUET_KINDS,
    partitions_def=NORWAY_BRREG_FINANCIAL_UPDATE_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description=(
        "Builds native-currency Norway Brreg resolved financial statement parquet "
        "for one update fetch partition."
    ),
)
def norway_brreg_financial_statements_updates_parquet(
    context,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    partition_date = context.partition_key
    context.log.info(
        "Building Norway Brreg update financial statements parquet: partition=%s",
        partition_date,
    )
    with tempfile.TemporaryDirectory(
        prefix="norway-brreg-financial-statements-update-"
    ) as temporary_directory:
        work_directory = Path(temporary_directory)
        response_index_path = work_directory / "responses.parquet"
        norway_brreg_financial_storage.download_update_response_index(
            partition_date,
            response_index_path,
        )
        output_path = work_directory / "financial_statements.parquet"
        counts = _build_original_statement_parquet(
            response_index_paths=[response_index_path],
            output_path=output_path,
            database_path=work_directory / "response-index.duckdb",
            storage=norway_brreg_financial_storage,
            log=context.log.info,
        )
        output_key = norway_brreg_financial_storage.upload_update_statements(
            partition_date,
            output_path,
        )
    context.log.info(
        "Completed Norway Brreg update financial statements parquet: "
        "partition=%s fetch_rows=%d successful_fetches=%d statement_rows=%d s3_key=%s",
        partition_date,
        counts["fetch_row_count"],
        counts["successful_fetch_count"],
        counts["statement_row_count"],
        output_key,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            **counts,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
            "s3_key": output_key,
        }
    )


@dg.asset(
    name="norway_brreg_financial_statements_snapshot_usd_parquet",
    deps=[dg.AssetKey("norway_brreg_financial_statements_snapshot_parquet")],
    group_name=GROUP_NAME,
    kinds=FINANCIAL_STATEMENTS_PARQUET_KINDS,
    description=(
        "Enriches Norway Brreg snapshot financial statement parquet with USD "
        "amounts and FX fields."
    ),
)
def norway_brreg_financial_statements_snapshot_usd_parquet(
    context,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    context.log.info("Building Norway Brreg snapshot financial statements USD parquet")
    with tempfile.TemporaryDirectory(
        prefix="norway-brreg-financial-statements-snapshot-usd-"
    ) as temporary_directory:
        work_directory = Path(temporary_directory)
        original_path = work_directory / "financial_statements.parquet"
        output_path = work_directory / "financial_statements_usd.parquet"
        norway_brreg_financial_storage.download_snapshot_statements(original_path)
        counts = _build_usd_statement_parquet(
            original_path=original_path,
            output_path=output_path,
            database_path=work_directory / "financial-statements.duckdb",
            exchange_rates=ExchangeRateClient.from_env(),
        )
        output_key = (
            norway_brreg_financial_storage.upload_snapshot_usd_statements(output_path)
        )
    context.log.info(
        "Completed Norway Brreg snapshot financial statements USD parquet: "
        "original_rows=%d usd_rows=%d rate_dates=%d s3_key=%s",
        counts["original_row_count"],
        counts["usd_row_count"],
        counts["rate_date_count"],
        output_key,
    )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
            "s3_key": output_key,
        }
    )


@dg.asset(
    name="norway_brreg_financial_statements_updates_usd_parquet",
    deps=[dg.AssetKey("norway_brreg_financial_statements_updates_parquet")],
    group_name=GROUP_NAME,
    kinds=FINANCIAL_STATEMENTS_PARQUET_KINDS,
    partitions_def=NORWAY_BRREG_FINANCIAL_UPDATE_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description=(
        "Enriches one Norway Brreg update financial statement parquet partition "
        "with USD amounts and FX fields."
    ),
)
def norway_brreg_financial_statements_updates_usd_parquet(
    context,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    partition_date = context.partition_key
    context.log.info(
        "Building Norway Brreg update financial statements USD parquet: partition=%s",
        partition_date,
    )
    with tempfile.TemporaryDirectory(
        prefix="norway-brreg-financial-statements-update-usd-"
    ) as temporary_directory:
        work_directory = Path(temporary_directory)
        original_path = work_directory / "financial_statements.parquet"
        output_path = work_directory / "financial_statements_usd.parquet"
        norway_brreg_financial_storage.download_update_statements(
            partition_date,
            original_path,
        )
        counts = _build_usd_statement_parquet(
            original_path=original_path,
            output_path=output_path,
            database_path=work_directory / "financial-statements.duckdb",
            exchange_rates=ExchangeRateClient.from_env(),
        )
        output_key = (
            norway_brreg_financial_storage.upload_update_usd_statements(
                partition_date,
                output_path,
            )
        )
    context.log.info(
        "Completed Norway Brreg update financial statements USD parquet: "
        "partition=%s original_rows=%d usd_rows=%d rate_dates=%d s3_key=%s",
        partition_date,
        counts["original_row_count"],
        counts["usd_row_count"],
        counts["rate_date_count"],
        output_key,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            **counts,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
            "s3_key": output_key,
        }
    )


@dg.asset(
    name="norway_brreg_financial_statements_snapshot_clickhouse",
    deps=[
        dg.AssetKey("norway_brreg_financial_statements_snapshot_usd_parquet"),
        dg.AssetKey("norway_brreg_entities_snapshot_clickhouse"),
    ],
    group_name=GROUP_NAME,
    kinds=FINANCIAL_STATEMENTS_CLICKHOUSE_KINDS,
    description=(
        "Replaces corpscout.no_financial_statements from Norway Brreg snapshot "
        "financial statement USD parquet."
    ),
)
def norway_brreg_financial_statements_snapshot_clickhouse(
    context,
    clickhouse: ClickhouseResource,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Publishing Norway Brreg snapshot financial statements to ClickHouse"
    )
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=(FINANCIAL_STATEMENTS_TABLE,),
    )
    with clickhouse.get_connection() as client:
        row_count = replace_financial_statement_snapshot_parquet_in_clickhouse(
            storage=norway_brreg_financial_storage,
            clickhouse_client=client,
            log=context.log.info,
        )
    context.log.info(
        "Completed Norway Brreg snapshot financial statements ClickHouse publish: rows=%d",
        row_count,
    )
    return dg.MaterializeResult(metadata={"row_count": row_count})


@dg.asset(
    name="norway_brreg_financial_statements_updates_clickhouse",
    deps=[
        dg.AssetKey("norway_brreg_financial_statements_updates_usd_parquet"),
        dg.AssetKey("norway_brreg_entity_updates_affected_orgs_parquet"),
        dg.AssetKey("norway_brreg_entity_updates_clickhouse"),
    ],
    group_name=GROUP_NAME,
    kinds=FINANCIAL_STATEMENTS_CLICKHOUSE_KINDS,
    partitions_def=NORWAY_BRREG_FINANCIAL_UPDATE_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description=(
        "Replaces only the Norway financial statement records present in one "
        "update USD parquet partition and preserves all other filings."
    ),
)
def norway_brreg_financial_statements_updates_clickhouse(
    context,
    clickhouse: ClickhouseResource,
    norway_brreg_entity_storage: NorwayBrregEntityParquetStorageResource,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    partition_date = context.partition_key
    context.log.info(
        "Publishing Norway Brreg update financial statements to ClickHouse: partition=%s",
        partition_date,
    )
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=(FINANCIAL_STATEMENTS_TABLE,),
    )
    with clickhouse.get_connection() as client:
        counts = apply_financial_statement_update_parquet_to_clickhouse(
            entity_storage=norway_brreg_entity_storage,
            financial_storage=norway_brreg_financial_storage,
            clickhouse_client=client,
            partition_date=partition_date,
            log=context.log.info,
        )
    context.log.info(
        "Completed Norway Brreg update financial statements ClickHouse publish: "
        "partition=%s affected_orgs=%d replacement_keys=%d rows=%d",
        partition_date,
        counts["affected_orgs"],
        counts["replacement_statement_keys"],
        counts[FINANCIAL_STATEMENTS_TABLE],
    )
    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "affected_org_count": counts["affected_orgs"],
            "replacement_statement_key_count": counts["replacement_statement_keys"],
            "row_count": counts[FINANCIAL_STATEMENTS_TABLE],
        }
    )


def replace_financial_statement_snapshot_parquet_in_clickhouse(
    *,
    storage: NorwayBrregFinancialParquetStorageResource,
    clickhouse_client: Any,
    log: Callable[..., None] | None = None,
) -> int:
    with tempfile.TemporaryDirectory(
        prefix="norway-brreg-financial-snapshot-publish-"
    ) as temporary_directory:
        work_directory = Path(temporary_directory)
        parquet_path = work_directory / "financial_statements_usd.parquet"
        storage.download_snapshot_usd_statements(parquet_path)
        with duckdb.connect(
            str(work_directory / "financial-statements.duckdb")
        ) as connection:
            row_count = _load_financial_parquet_into_duckdb(
                connection,
                parquet_path,
            )
            _log(
                log,
                "Loaded Norway Brreg snapshot financial statement USD parquet "
                "into DuckDB: rows=%s",
                row_count,
            )
            return export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=clickhouse_client,
                duckdb_schema=FINANCIAL_STATEMENTS_DUCKDB_SCHEMA,
                duckdb_table=FINANCIAL_STATEMENTS_TABLE,
                clickhouse_database=RESOLVED_DATABASE,
                clickhouse_table=FINANCIAL_STATEMENTS_TABLE,
                columns=FINANCIAL_STATEMENT_COLUMNS,
                truncate=True,
            )


def apply_financial_statement_update_parquet_to_clickhouse(
    *,
    entity_storage: NorwayBrregEntityParquetStorageResource,
    financial_storage: NorwayBrregFinancialParquetStorageResource,
    clickhouse_client: Any,
    partition_date: str,
    log: Callable[..., None] | None = None,
) -> dict[str, int]:
    affected_orgs = entity_storage.read_normalized_update_table(
        partition_date,
        ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS,
    )
    with tempfile.TemporaryDirectory(
        prefix="norway-brreg-financial-update-publish-"
    ) as temporary_directory:
        work_directory = Path(temporary_directory)
        parquet_path = work_directory / "financial_statements_usd.parquet"
        financial_storage.download_update_usd_statements(
            partition_date,
            parquet_path,
        )
        with duckdb.connect(
            str(work_directory / "financial-statements.duckdb")
        ) as connection:
            statement_count = _load_financial_parquet_into_duckdb(
                connection,
                parquet_path,
            )
            replacement_key_rows = (
                _financial_statement_replacement_key_rows_from_duckdb(connection)
            )
            row_counts = {
                "affected_orgs": affected_orgs.height,
                "replacement_statement_keys": len(replacement_key_rows),
                FINANCIAL_STATEMENTS_TABLE: statement_count,
            }
            if not replacement_key_rows:
                _log(
                    log,
                    "Norway Brreg financial update partition %s has no replacement "
                    "statements; preserving existing ClickHouse rows",
                    partition_date,
                )
                return row_counts

            if affected_orgs.height < 1:
                _validate_no_financial_replacements_without_affected_orgs(
                    statement_count
                )

            replacement_stage_table = (
                f"_tmp_no_financial_statement_keys_{uuid.uuid4().hex}"
            )
            replacement_stage_qualified = _quote_clickhouse_qualified_table(
                RESOLVED_DATABASE,
                replacement_stage_table,
            )
            quoted_key_columns = ", ".join(
                _quote_clickhouse_identifier(column)
                for column in FINANCIAL_STATEMENT_REPLACEMENT_KEY_COLUMNS
            )
            replacement_key_columns_ddl = ", ".join(
                f"{_quote_clickhouse_identifier(column)} String"
                for column in FINANCIAL_STATEMENT_REPLACEMENT_KEY_COLUMNS
            )
            clickhouse_client.execute(
                f"CREATE TABLE {replacement_stage_qualified} "
                f"({replacement_key_columns_ddl}) ENGINE = Memory"
            )
            primary_error: Exception | None = None
            try:
                _insert_clickhouse_rows(
                    clickhouse_client,
                    database=RESOLVED_DATABASE,
                    table=replacement_stage_table,
                    qualified_table=replacement_stage_qualified,
                    columns=FINANCIAL_STATEMENT_REPLACEMENT_KEY_COLUMNS,
                    rows=replacement_key_rows,
                )
                clickhouse_client.execute(
                    f"ALTER TABLE {_quote_clickhouse_qualified_table(RESOLVED_DATABASE, FINANCIAL_STATEMENTS_TABLE)} "
                    f"DELETE WHERE ({quoted_key_columns}) IN "
                    f"(SELECT {quoted_key_columns} FROM {replacement_stage_qualified}) "
                    "SETTINGS mutations_sync = 1"
                )

                row_counts[FINANCIAL_STATEMENTS_TABLE] = (
                    export_duckdb_connection_table_to_clickhouse(
                        duckdb_connection=connection,
                        clickhouse_client=clickhouse_client,
                        duckdb_schema=FINANCIAL_STATEMENTS_DUCKDB_SCHEMA,
                        duckdb_table=FINANCIAL_STATEMENTS_TABLE,
                        clickhouse_database=RESOLVED_DATABASE,
                        clickhouse_table=FINANCIAL_STATEMENTS_TABLE,
                        columns=FINANCIAL_STATEMENT_COLUMNS,
                        truncate=False,
                    )
                )
            except Exception as exc:
                primary_error = exc
                raise
            finally:
                _drop_affected_stage_table(
                    clickhouse_client,
                    replacement_stage_qualified,
                    suppress_errors=primary_error is not None,
                )
            return row_counts


def _download_historical_response_indexes(
    storage: NorwayBrregFinancialParquetStorageResource,
    *,
    target_directory: Path,
    log: Callable[..., object] | None = None,
) -> list[Path]:
    keys = storage.list_all_response_index_keys()
    target_directory.mkdir(parents=True, exist_ok=True)
    _log(
        log,
        "Downloading Norway Brreg financial response index parquet files: count=%d",
        len(keys),
    )
    paths: list[Path] = []
    for index, key in enumerate(keys):
        path = target_directory / f"response-index-{index:06d}.parquet"
        storage.download_response_index(key, path)
        paths.append(path)
    return paths


def _build_original_statement_parquet(
    *,
    response_index_paths: list[Path],
    output_path: Path,
    database_path: Path,
    storage: NorwayBrregFinancialParquetStorageResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    with duckdb.connect(str(database_path)) as connection:
        index_counts = _load_response_index_parquet_files(
            connection,
            response_index_paths,
            log=log,
        )
        statement_count = _write_original_statement_parquet(
            connection,
            output_path=output_path,
            storage=storage,
            log=log,
        )
    return {
        "fetch_row_count": index_counts["fetch_row_count"],
        "successful_fetch_count": index_counts["successful_fetch_count"],
        "statement_row_count": statement_count,
    }


def _load_response_index_parquet_files(
    connection: duckdb.DuckDBPyConnection,
    paths: list[Path],
    *,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    empty_arrow = financial_fetches.financial_fetches_frame([]).to_arrow()
    connection.register(_EMPTY_RESPONSE_INDEX_RELATION, empty_arrow)
    try:
        connection.execute(
            f"""
            create or replace table {_RESPONSE_INDEX_RAW_TABLE} as
            select *, cast(null as bigint) as _response_index_ordinal
            from {_EMPTY_RESPONSE_INDEX_RELATION}
            where false
            """
        )
    finally:
        connection.unregister(_EMPTY_RESPONSE_INDEX_RELATION)

    input_rows = 0
    for file_index, path in enumerate(paths, start=1):
        file_rows = int(
            connection.execute(
                "select count(*) from read_parquet(?)",
                [str(path)],
            ).fetchone()[0]
        )
        connection.execute(
            f"""
            insert into {_RESPONSE_INDEX_RAW_TABLE}
            select *,
                   cast(? as bigint) + row_number() over ()
                       as _response_index_ordinal
            from read_parquet(?)
            """,
            [input_rows, str(path)],
        )
        input_rows += file_rows
        _log(
            log,
            "Loaded Norway Brreg response index parquet into DuckDB: "
            "file=%d total_files=%d file_rows=%d total_rows=%d",
            file_index,
            len(paths),
            file_rows,
            input_rows,
        )

    connection.execute(
        f"""
        create or replace table {_RESPONSE_INDEX_TABLE} as
        select * exclude ({_STATEMENT_DEDUP_RANK})
        from (
            select *,
                   row_number() over (
                       partition by org_number, fetch_status, source_object_key
                       order by _response_index_ordinal desc
                   ) as {_STATEMENT_DEDUP_RANK}
            from {_RESPONSE_INDEX_RAW_TABLE}
        )
        where {_STATEMENT_DEDUP_RANK} = 1
        order by _response_index_ordinal
        """
    )
    output_rows, successful_rows = connection.execute(
        f"""
        select
            count(*),
            count(*) filter (
                where fetch_status = ?
            )
        from {_RESPONSE_INDEX_TABLE}
        """,
        [financial_fetches.FINANCIAL_FETCH_STATUS_SUCCESS],
    ).fetchone()
    _log(
        log,
        "Deduplicated Norway Brreg response indexes set-wise: "
        "input_rows=%d output_rows=%d successful_rows=%d",
        input_rows,
        output_rows,
        successful_rows,
    )
    return {
        "fetch_row_count": int(output_rows),
        "successful_fetch_count": int(successful_rows),
    }


def _write_original_statement_parquet(
    connection: duckdb.DuckDBPyConnection,
    *,
    output_path: Path,
    storage: NorwayBrregFinancialParquetStorageResource,
    log: Callable[..., object] | None = None,
) -> int:
    spool_path = output_path.with_name(f"{output_path.stem}-spool.parquet")
    statement_schema = _financial_statement_frame([]).to_arrow().schema
    spool_schema = statement_schema.append(
        pa.field(_STATEMENT_STREAM_ORDINAL, pa.int64(), nullable=False)
    )
    resolved_at = datetime.now(UTC)
    parsed_rows = 0
    processed_fetches = 0
    pending_rows: list[dict[str, Any]] = []
    result = connection.execute(
        f"""
        select * exclude (_response_index_ordinal)
        from {_RESPONSE_INDEX_TABLE}
        where fetch_status = ?
        order by _response_index_ordinal
        """,
        [financial_fetches.FINANCIAL_FETCH_STATUS_SUCCESS],
    )
    with pq.ParquetWriter(spool_path, spool_schema, compression="zstd") as writer:
        for record_batch in result.to_arrow_reader(
            batch_size=RESPONSE_READ_BATCH_SIZE
        ):
            index_rows = record_batch.to_pylist()
            response_rows = _response_rows_with_verified_payloads(
                index_rows,
                storage=storage,
            )
            normalized_rows = (
                financial_normalize.iter_resolved_financial_statement_original_rows_from_fetch_rows(
                    response_rows,
                    resolved_at=resolved_at,
                    log=log,
                    total_fetch_rows=len(response_rows),
                )
            )
            for normalized_row in normalized_rows:
                pending_rows.append(normalized_row)
                if len(pending_rows) == STATEMENT_WRITE_BATCH_SIZE:
                    _write_statement_parquet_batch(
                        writer,
                        pending_rows,
                        first_ordinal=parsed_rows,
                        spool_schema=spool_schema,
                    )
                    parsed_rows += len(pending_rows)
                    pending_rows.clear()
            processed_fetches += len(index_rows)
            _log(
                log,
                "Parsed Norway Brreg response JSON record batch: "
                "processed_fetches=%d parsed_statement_rows=%d",
                processed_fetches,
                parsed_rows + len(pending_rows),
            )
        _write_statement_parquet_batch(
            writer,
            pending_rows,
            first_ordinal=parsed_rows,
            spool_schema=spool_schema,
        )
        parsed_rows += len(pending_rows)

    columns = ", ".join(
        _quote_duckdb_identifier(column) for column in FINANCIAL_STATEMENT_COLUMNS
    )
    connection.execute(
        f"""
        copy (
            select {columns}
            from (
                select *,
                       row_number() over (
                           partition by source_system, source_record_id
                           order by {_STATEMENT_STREAM_ORDINAL} desc
                       ) as {_STATEMENT_DEDUP_RANK}
                from read_parquet(?)
            )
            where {_STATEMENT_DEDUP_RANK} = 1
            order by {_STATEMENT_STREAM_ORDINAL}
        ) to {_duckdb_string_literal(output_path)}
        (format parquet, compression zstd)
        """,
        [str(spool_path)],
    )
    statement_count = int(
        connection.execute(
            "select count(*) from read_parquet(?)",
            [str(output_path)],
        ).fetchone()[0]
    )
    _log(
        log,
        "Completed bounded Norway Brreg statement parsing: "
        "parsed_rows=%d deduplicated_rows=%d",
        parsed_rows,
        statement_count,
    )
    return statement_count


def _write_statement_parquet_batch(
    writer: pq.ParquetWriter,
    rows: list[dict[str, Any]],
    *,
    first_ordinal: int,
    spool_schema: pa.Schema,
) -> None:
    if not rows:
        return
    table = _financial_statement_frame(rows).to_arrow()
    ordinal_array = pa.array(
        range(first_ordinal, first_ordinal + len(rows)),
        type=pa.int64(),
    )
    writer.write_table(
        pa.Table.from_arrays(
            [*table.columns, ordinal_array],
            schema=spool_schema,
        )
    )


def _response_rows_with_verified_payloads(
    response_index_rows: list[dict[str, Any]],
    *,
    storage: NorwayBrregFinancialParquetStorageResource,
) -> list[dict[str, Any]]:
    def load(index_row: dict[str, Any]) -> dict[str, Any]:
        response_key = _string(index_row.get("source_object_key"))
        expected_hash = _string(index_row.get("source_payload_hash"))
        if response_key == "" or expected_hash == "":
            raise RuntimeError(
                "Successful Norway BRREG response index row has no object key or "
                f"hash: org={index_row.get('org_number')}"
            )
        try:
            response_body = storage.read_response(response_key)
        except Exception as error:
            raise RuntimeError(
                f"Norway BRREG response JSON object is missing: {response_key}"
            ) from error
        actual_hash = financial_fetches.sha256_hex(response_body)
        if actual_hash != expected_hash:
            raise RuntimeError(
                "Norway BRREG response JSON hash mismatch while parsing statements: "
                f"key={response_key} expected={expected_hash} actual={actual_hash}"
            )
        try:
            payload = json.loads(response_body)
        except Exception as error:
            raise RuntimeError(
                f"Norway BRREG response object is invalid JSON: {response_key}"
            ) from error
        if not isinstance(payload, list) or not all(
            isinstance(record, dict) for record in payload
        ):
            raise RuntimeError(
                "Norway BRREG response object must contain a list of objects: "
                f"{response_key}"
            )
        return {**index_row, "response_payload": payload}

    with ThreadPoolExecutor(max_workers=RESPONSE_READ_WORKERS) as executor:
        return list(executor.map(load, response_index_rows))


def _build_usd_statement_parquet(
    *,
    original_path: Path,
    output_path: Path,
    database_path: Path,
    exchange_rates: Any,
) -> dict[str, int]:
    with duckdb.connect(str(database_path)) as connection:
        available_columns = _parquet_columns(connection, original_path)
        _validate_financial_columns(available_columns)
        columns = ", ".join(
            _quote_duckdb_identifier(column) for column in FINANCIAL_STATEMENT_COLUMNS
        )
        connection.execute(
            f"""
            create table _norway_financial_statements as
            select {columns},
                   row_number() over () as {_STATEMENT_OUTPUT_ORDINAL}
            from read_parquet(?)
            """,
            [str(original_path)],
        )
        original_row_count = int(
            connection.execute(
                "select count(*) from _norway_financial_statements"
            ).fetchone()[0]
        )
        pairs = connection.execute(
            """
            select distinct upper(currency), cast(period_end_date as varchar)
            from _norway_financial_statements
            where currency <> '' and period_end_date is not null
            order by 1, 2
            """
        ).fetchall()
        requests = [
            ExchangeRateRequest(currency=currency, rate_date=rate_date)
            for currency, rate_date in pairs
        ]
        rates = _load_available_usd_rates(exchange_rates, requests)
        fx_rows = [
            {
                "currency": currency,
                "period_end_date": date.fromisoformat(rate_date),
                "fx_rate": str(rate.rate),
                "fx_rate_date": date.fromisoformat(str(rate.rate_date)),
                "fx_source": rate.source,
            }
            for currency, rate_date in pairs
            if (rate := rates.get((currency, rate_date))) is not None
        ]
        _replace_fx_stage(connection, fx_rows)

        reset_assignments = [
            "quality_flag = coalesce(quality_flag, '')",
            *(f"{name}_amount_usd = null" for name in financial_normalize.FINANCIAL_AMOUNT_NAMES),
            "fx_rate_to_usd = null",
            "fx_rate_date = null",
            "fx_source = null",
        ]
        connection.execute(
            "update _norway_financial_statements set "
            + ", ".join(reset_assignments)
        )
        if fx_rows:
            amount_assignments = ", ".join(
                f"{name}_amount_usd = cast("
                f"statements.{name}_amount_original * fx.fx_rate "
                "as decimal(38, 6))"
                for name in financial_normalize.FINANCIAL_AMOUNT_NAMES
            )
            connection.execute(
                f"""
                update _norway_financial_statements as statements
                set {amount_assignments},
                    fx_rate_to_usd = fx.fx_rate,
                    fx_rate_date = fx.fx_rate_date,
                    fx_source = fx.fx_source
                from {_FX_STAGE_TABLE} as fx
                where upper(statements.currency) = fx.currency
                  and statements.period_end_date = fx.period_end_date
                """
            )
        connection.execute(
            f"""
            copy (
                select {columns}
                from _norway_financial_statements
                order by {_STATEMENT_OUTPUT_ORDINAL}
            ) to {_duckdb_string_literal(output_path)}
            (format parquet, compression zstd)
            """
        )
        usd_row_count, rate_date_count = connection.execute(
            """
            select count(*), count(distinct fx_rate_date)
            from _norway_financial_statements
            """
        ).fetchone()
    return {
        "original_row_count": original_row_count,
        "usd_row_count": int(usd_row_count),
        "rate_date_count": int(rate_date_count),
    }


def _load_available_usd_rates(
    exchange_rates: Any,
    requests: list[ExchangeRateRequest],
) -> dict[tuple[str, str], Any]:
    if not requests:
        return {}
    try:
        return exchange_rates.usd_rates(requests)
    except LookupError:
        rates: dict[tuple[str, str], Any] = {}
        for request in requests:
            try:
                rates.update(exchange_rates.usd_rates([request]))
            except LookupError:
                continue
        return rates


def _replace_fx_stage(
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
) -> None:
    connection.execute(
        f"""
        create or replace temp table {_FX_STAGE_TABLE} (
            currency varchar not null,
            period_end_date date not null,
            fx_rate decimal(38, 12) not null,
            fx_rate_date date not null,
            fx_source varchar
        )
        """
    )
    if not rows:
        return
    arrow_table = pa.Table.from_pylist(rows, schema=_FX_ARROW_SCHEMA)
    connection.register(_FX_BATCH_RELATION, arrow_table)
    try:
        connection.execute(
            f"""
            insert into {_FX_STAGE_TABLE}
            select
                currency,
                period_end_date,
                cast(fx_rate as decimal(38, 12)),
                fx_rate_date,
                fx_source
            from {_FX_BATCH_RELATION}
            """
        )
    finally:
        connection.unregister(_FX_BATCH_RELATION)


def _financial_statement_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=FINANCIAL_STATEMENT_SCHEMA)
    return _coerce_financial_statement_frame(pl.DataFrame(rows))


def _coerce_financial_statement_frame(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.select(
        [
            _financial_statement_column_expression(frame, column_name, data_type)
            for column_name, data_type in FINANCIAL_STATEMENT_SCHEMA.items()
        ]
    )


def _financial_statement_column_expression(
    frame: pl.DataFrame,
    column_name: str,
    data_type: pl.DataType,
) -> pl.Expr:
    if column_name not in frame.columns:
        return pl.lit(None, dtype=data_type).alias(column_name)
    return pl.col(column_name).cast(data_type, strict=False).alias(column_name)


def _load_financial_parquet_into_duckdb(
    connection: duckdb.DuckDBPyConnection,
    parquet_path: Path,
) -> int:
    _validate_financial_columns(_parquet_columns(connection, parquet_path))
    connection.execute(
        f"create schema {_quote_duckdb_identifier(FINANCIAL_STATEMENTS_DUCKDB_SCHEMA)}"
    )
    columns = ", ".join(
        _quote_duckdb_identifier(column) for column in FINANCIAL_STATEMENT_COLUMNS
    )
    connection.execute(
        f"create table {_quote_duckdb_identifier(FINANCIAL_STATEMENTS_DUCKDB_SCHEMA)}."
        f"{_quote_duckdb_identifier(FINANCIAL_STATEMENTS_TABLE)} as "
        f"select {columns} from read_parquet(?)",
        [str(parquet_path)],
    )
    return int(
        connection.execute(
            f"select count(*) from "
            f"{_quote_duckdb_identifier(FINANCIAL_STATEMENTS_DUCKDB_SCHEMA)}."
            f"{_quote_duckdb_identifier(FINANCIAL_STATEMENTS_TABLE)}"
        ).fetchone()[0]
    )


def _parquet_columns(
    connection: duckdb.DuckDBPyConnection,
    parquet_path: Path,
) -> list[str]:
    result = connection.execute(
        "select * from read_parquet(?) limit 0",
        [str(parquet_path)],
    )
    return [description[0] for description in result.description]


def _validate_financial_columns(columns: list[str]) -> None:
    missing_columns = [
        column for column in FINANCIAL_STATEMENT_COLUMNS if column not in columns
    ]
    if missing_columns:
        raise ValueError(
            "Norway Brreg financial statement parquet is missing columns: "
            + ", ".join(missing_columns)
        )


def _financial_statement_replacement_key_rows_from_duckdb(
    connection: duckdb.DuckDBPyConnection,
) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    source_system_column, source_record_id_column = (
        _quote_duckdb_identifier(column)
        for column in FINANCIAL_STATEMENT_REPLACEMENT_KEY_COLUMNS
    )
    unique_keys = connection.execute(
        f"""
        select distinct {source_system_column}, {source_record_id_column}
        from {_quote_duckdb_identifier(FINANCIAL_STATEMENTS_DUCKDB_SCHEMA)}.
             {_quote_duckdb_identifier(FINANCIAL_STATEMENTS_TABLE)}
        """
    ).fetchall()
    for source_system, source_record_id in unique_keys:
        if (
            not isinstance(source_system, str)
            or source_system.strip() == ""
            or not isinstance(source_record_id, str)
            or source_record_id.strip() == ""
        ):
            raise ValueError(
                "Norway Brreg financial statement replacement keys must contain "
                "non-empty source_system and source_record_id values"
            )
        rows.append((source_system, source_record_id))
    return rows


def _validate_no_financial_replacements_without_affected_orgs(
    statement_count: int,
) -> None:
    if statement_count > 0:
        raise ValueError(
            "Norway Brreg update partition has replacement financial statement rows but no affected orgs"
        )


def _duckdb_string_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _log(log: Callable[..., None] | None, message: str, *args: Any) -> None:
    if log is not None:
        log(message, *args)


def _string(value: Any) -> str:
    return "" if value is None else str(value)
