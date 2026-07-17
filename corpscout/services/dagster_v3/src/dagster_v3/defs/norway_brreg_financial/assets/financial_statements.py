import json
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC
from datetime import datetime
from typing import Any

import dagster as dg
import duckdb
import polars as pl
from dagster_clickhouse import ClickhouseResource
from exchange_rates import ExchangeRateClient

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
    response_index = (
        norway_brreg_financial_storage.read_consolidated_historical_response_index(
            log=context.log.info,
        )
    )
    successful_fetch_count = _successful_fetch_count(response_index)
    context.log.info(
        "Loaded Norway Brreg historical response index for statement normalization: "
        "response_rows=%d successful_responses=%d",
        response_index.height,
        successful_fetch_count,
    )
    statement_frame = _original_statement_frame(
        response_index,
        storage=norway_brreg_financial_storage,
        log=context.log.info,
    )
    context.log.info(
        "Writing Norway Brreg snapshot financial statements parquet: "
        "statement_rows=%d bucket=%s",
        statement_frame.height,
        NORWAY_BRREG_FINANCIAL_BUCKET,
    )
    output_key = norway_brreg_financial_storage.write_snapshot_statements(
        statement_frame,
        log=context.log.info,
    )
    context.log.info(
        "Completed Norway Brreg snapshot financial statements parquet: "
        "response_index_rows=%d successful_responses=%d statement_rows=%d s3_key=%s",
        response_index.height,
        successful_fetch_count,
        statement_frame.height,
        output_key,
    )
    return dg.MaterializeResult(
        metadata={
            "fetch_row_count": response_index.height,
            "successful_fetch_count": successful_fetch_count,
            "statement_row_count": statement_frame.height,
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
    fetch_frame = norway_brreg_financial_storage.read_update_response_index(
        partition_date
    )
    statement_frame = _original_statement_frame(
        fetch_frame,
        storage=norway_brreg_financial_storage,
        log=context.log.info,
    )
    output_key = norway_brreg_financial_storage.write_update_statements(
        partition_date,
        statement_frame,
    )
    context.log.info(
        "Completed Norway Brreg update financial statements parquet: "
        "partition=%s fetch_rows=%d successful_fetches=%d statement_rows=%d s3_key=%s",
        partition_date,
        fetch_frame.height,
        _successful_fetch_count(fetch_frame),
        statement_frame.height,
        output_key,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "fetch_row_count": fetch_frame.height,
            "successful_fetch_count": _successful_fetch_count(fetch_frame),
            "statement_row_count": statement_frame.height,
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
    original_frame = norway_brreg_financial_storage.read_snapshot_statements()
    usd_frame = _usd_statement_frame(original_frame)
    output_key = norway_brreg_financial_storage.write_snapshot_usd_statements(usd_frame)
    context.log.info(
        "Completed Norway Brreg snapshot financial statements USD parquet: "
        "original_rows=%d usd_rows=%d rate_dates=%d s3_key=%s",
        original_frame.height,
        usd_frame.height,
        _rate_date_count(usd_frame),
        output_key,
    )
    return dg.MaterializeResult(
        metadata={
            "original_row_count": original_frame.height,
            "usd_row_count": usd_frame.height,
            "rate_date_count": _rate_date_count(usd_frame),
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
    original_frame = norway_brreg_financial_storage.read_update_statements(
        partition_date
    )
    usd_frame = _usd_statement_frame(original_frame)
    output_key = norway_brreg_financial_storage.write_update_usd_statements(
        partition_date,
        usd_frame,
    )
    context.log.info(
        "Completed Norway Brreg update financial statements USD parquet: "
        "partition=%s original_rows=%d usd_rows=%d rate_dates=%d s3_key=%s",
        partition_date,
        original_frame.height,
        usd_frame.height,
        _rate_date_count(usd_frame),
        output_key,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "original_row_count": original_frame.height,
            "usd_row_count": usd_frame.height,
            "rate_date_count": _rate_date_count(usd_frame),
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
    frame = storage.read_snapshot_usd_statements()
    _log(
        log,
        "Loaded Norway Brreg snapshot financial statement USD parquet: rows=%s",
        frame.height,
    )
    with duckdb.connect(":memory:") as connection:
        _load_financial_frame_into_duckdb(connection, frame)
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
    frame = financial_storage.read_update_usd_statements(partition_date)
    _validate_financial_frame_columns(frame)
    replacement_key_rows = _financial_statement_replacement_key_rows(frame)
    row_counts = {
        "affected_orgs": affected_orgs.height,
        "replacement_statement_keys": len(replacement_key_rows),
        FINANCIAL_STATEMENTS_TABLE: frame.height,
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
        _validate_no_financial_replacements_without_affected_orgs(frame)

    replacement_stage_table = f"_tmp_no_financial_statement_keys_{uuid.uuid4().hex}"
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

        with duckdb.connect(":memory:") as connection:
            _load_financial_frame_into_duckdb(connection, frame)
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


def _original_statement_frame(
    response_index: pl.DataFrame,
    *,
    storage: NorwayBrregFinancialParquetStorageResource,
    log: Callable[..., object] | None = None,
) -> pl.DataFrame:
    _log(
        log,
        "Converting Norway Brreg response index to Python rows: response_rows=%d",
        response_index.height,
    )
    if response_index.is_empty():
        return _financial_statement_frame([])
    successful_fetch_rows = response_index.filter(
        pl.col("fetch_status") == financial_fetches.FINANCIAL_FETCH_STATUS_SUCCESS
    ).to_dicts()
    _log(
        log,
        "Reading verified Norway Brreg financial response JSON: successful_fetches=%d "
        "batch_size=%d workers=%d",
        len(successful_fetch_rows),
        RESPONSE_READ_BATCH_SIZE,
        RESPONSE_READ_WORKERS,
    )
    resolved_at = datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    for batch_start in range(0, len(successful_fetch_rows), RESPONSE_READ_BATCH_SIZE):
        response_rows = _response_rows_with_verified_payloads(
            successful_fetch_rows[
                batch_start : batch_start + RESPONSE_READ_BATCH_SIZE
            ],
            storage=storage,
        )
        rows.extend(
            financial_normalize.build_resolved_financial_statement_original_rows_from_fetch_rows(
                response_rows,
                resolved_at=resolved_at,
                log=log,
            )
        )
        _log(
            log,
            "Parsed Norway Brreg financial response JSON batch: processed=%d "
            "total=%d statement_rows=%d",
            min(batch_start + RESPONSE_READ_BATCH_SIZE, len(successful_fetch_rows)),
            len(successful_fetch_rows),
            len(rows),
        )
    _log(
        log,
        "Coercing Norway Brreg snapshot financial statement rows to parquet schema: "
        "statement_rows=%d columns=%d",
        len(rows),
        len(FINANCIAL_STATEMENT_SCHEMA),
    )
    statement_frame = _financial_statement_frame(rows)
    if statement_frame.is_empty():
        return statement_frame
    return statement_frame.unique(
        subset=["source_system", "source_record_id"],
        keep="last",
        maintain_order=True,
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


def _usd_statement_frame(original_frame: pl.DataFrame) -> pl.DataFrame:
    rows = financial_normalize.build_resolved_financial_statement_usd_rows(
        original_frame.to_dicts(),
        exchange_rates=ExchangeRateClient.from_env(),
    )
    return _financial_statement_frame(rows)


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


def _load_financial_frame_into_duckdb(
    connection: duckdb.DuckDBPyConnection,
    frame: pl.DataFrame,
) -> None:
    _validate_financial_frame_columns(frame)
    frame = _coerce_financial_statement_frame(frame)
    connection.execute(
        f"create schema {_quote_duckdb_identifier(FINANCIAL_STATEMENTS_DUCKDB_SCHEMA)}"
    )
    registered_name = "_frame_no_financial_statements"
    connection.register(registered_name, frame.to_arrow())
    try:
        connection.execute(
            f"create table {_quote_duckdb_identifier(FINANCIAL_STATEMENTS_DUCKDB_SCHEMA)}."
            f"{_quote_duckdb_identifier(FINANCIAL_STATEMENTS_TABLE)} as "
            f"select * from {_quote_duckdb_identifier(registered_name)}"
        )
    finally:
        connection.unregister(registered_name)


def _validate_financial_frame_columns(frame: pl.DataFrame) -> None:
    missing_columns = [
        column for column in FINANCIAL_STATEMENT_COLUMNS if column not in frame.columns
    ]
    if missing_columns:
        raise ValueError(
            "Norway Brreg financial statement parquet is missing columns: "
            + ", ".join(missing_columns)
        )


def _financial_statement_replacement_key_rows(
    frame: pl.DataFrame,
) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    unique_keys = frame.select(FINANCIAL_STATEMENT_REPLACEMENT_KEY_COLUMNS).unique(
        maintain_order=True
    )
    for source_system, source_record_id in unique_keys.iter_rows():
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
    frame: pl.DataFrame,
) -> None:
    if frame.height > 0:
        raise ValueError(
            "Norway Brreg update partition has replacement financial statement rows but no affected orgs"
        )


def _successful_fetch_count(fetch_frame: pl.DataFrame) -> int:
    if fetch_frame.is_empty() or "fetch_status" not in fetch_frame.columns:
        return 0
    return fetch_frame.filter(pl.col("fetch_status") == "success").height


def _rate_date_count(frame: pl.DataFrame) -> int:
    if frame.is_empty() or "fx_rate_date" not in frame.columns:
        return 0
    return frame.get_column("fx_rate_date").drop_nulls().n_unique()


def _log(log: Callable[..., None] | None, message: str, *args: Any) -> None:
    if log is not None:
        log(message, *args)


def _string(value: Any) -> str:
    return "" if value is None else str(value)
