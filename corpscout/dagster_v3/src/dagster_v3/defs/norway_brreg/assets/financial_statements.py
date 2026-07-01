import uuid
from collections.abc import Callable
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
from dagster_v3.defs.norway_brreg import financial_normalize
from dagster_v3.defs.norway_brreg.assets.entity_clickhouse import (
    _affected_org_rows,
    _drop_affected_stage_table,
    _insert_clickhouse_rows,
    _quote_clickhouse_identifier,
    _quote_clickhouse_qualified_table,
    _quote_duckdb_identifier,
)
from dagster_v3.defs.norway_brreg.assets.entity_snapshot import (
    GROUP_NAME,
    NORWAY_BRREG_ENTITY_BUCKET,
)
from dagster_v3.defs.norway_brreg.assets.entity_updates import (
    NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
)
from dagster_v3.defs.norway_brreg.entity_storage import (
    ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS,
    NorwayBrregEntityParquetStorageResource,
)
from dagster_v3.defs.norway_brreg.financial_storage import (
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
    deps=[dg.AssetKey("norway_brreg_financial_fetches_snapshot_parquet")],
    group_name=GROUP_NAME,
    kinds=FINANCIAL_STATEMENTS_PARQUET_KINDS,
    description=(
        "Builds native-currency Norway Brreg resolved financial statement parquet "
        "from successful historical raw fetch rows."
    ),
)
def norway_brreg_financial_statements_snapshot_parquet(
    context,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Building Norway Brreg snapshot financial statements parquet from historical raw fetches"
    )
    fetch_frame = norway_brreg_financial_storage.read_historical_raw_fetches()
    statement_frame = _original_statement_frame(fetch_frame)
    output_key = norway_brreg_financial_storage.write_snapshot_statements(
        statement_frame
    )
    context.log.info(
        "Completed Norway Brreg snapshot financial statements parquet: "
        "historical_raw_fetch_rows=%d successful_fetches=%d statement_rows=%d s3_key=%s",
        fetch_frame.height,
        _successful_fetch_count(fetch_frame),
        statement_frame.height,
        output_key,
    )
    return dg.MaterializeResult(
        metadata={
            "fetch_row_count": fetch_frame.height,
            "successful_fetch_count": _successful_fetch_count(fetch_frame),
            "statement_row_count": statement_frame.height,
            "s3_bucket": NORWAY_BRREG_ENTITY_BUCKET,
            "s3_key": output_key,
        }
    )


@dg.asset(
    name="norway_brreg_financial_statements_updates_parquet",
    deps=[dg.AssetKey("norway_brreg_financial_fetches_updates_parquet")],
    group_name=GROUP_NAME,
    kinds=FINANCIAL_STATEMENTS_PARQUET_KINDS,
    partitions_def=NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
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
    fetch_frame = norway_brreg_financial_storage.read_update_fetches(partition_date)
    statement_frame = _original_statement_frame(fetch_frame)
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
            "s3_bucket": NORWAY_BRREG_ENTITY_BUCKET,
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
            "s3_bucket": NORWAY_BRREG_ENTITY_BUCKET,
            "s3_key": output_key,
        }
    )


@dg.asset(
    name="norway_brreg_financial_statements_updates_usd_parquet",
    deps=[dg.AssetKey("norway_brreg_financial_statements_updates_parquet")],
    group_name=GROUP_NAME,
    kinds=FINANCIAL_STATEMENTS_PARQUET_KINDS,
    partitions_def=NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
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
            "s3_bucket": NORWAY_BRREG_ENTITY_BUCKET,
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
    context.log.info("Publishing Norway Brreg snapshot financial statements to ClickHouse")
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
    partitions_def=NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
    description=(
        "Deletes affected Norway org financial rows from ClickHouse and appends "
        "replacement rows from one update USD parquet partition."
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
        "partition=%s affected_orgs=%d rows=%d",
        partition_date,
        counts["affected_orgs"],
        counts[FINANCIAL_STATEMENTS_TABLE],
    )
    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "affected_org_count": counts["affected_orgs"],
            "row_count": counts[FINANCIAL_STATEMENTS_TABLE],
        }
    )


norway_brreg_financial_snapshot_job = dg.define_asset_job(
    "norway_brreg_financial_snapshot_job",
    selection=dg.AssetSelection.assets(
        "norway_brreg_financial_fetches_snapshot_parquet",
        "norway_brreg_financial_statements_snapshot_parquet",
        "norway_brreg_financial_statements_snapshot_usd_parquet",
        "norway_brreg_financial_statements_snapshot_clickhouse",
    ),
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
    row_counts = {
        "affected_orgs": affected_orgs.height,
        FINANCIAL_STATEMENTS_TABLE: frame.height,
    }
    if affected_orgs.height < 1:
        _validate_no_financial_replacements_without_affected_orgs(frame)
        _log(
            log,
            "Norway Brreg financial update partition %s has no affected orgs; "
            "skipping ClickHouse changes",
            partition_date,
        )
        return row_counts

    affected_stage_table = f"_tmp_no_financial_affected_orgs_{uuid.uuid4().hex}"
    affected_stage_qualified = _quote_clickhouse_qualified_table(
        RESOLVED_DATABASE,
        affected_stage_table,
    )
    clickhouse_client.execute(
        f"CREATE TABLE {affected_stage_qualified} "
        f"({_quote_clickhouse_identifier('org_number')} String) ENGINE = Memory"
    )
    primary_error: Exception | None = None
    try:
        _insert_clickhouse_rows(
            clickhouse_client,
            database=RESOLVED_DATABASE,
            table=affected_stage_table,
            qualified_table=affected_stage_qualified,
            columns=("org_number",),
            rows=_affected_org_rows(affected_orgs),
        )
        clickhouse_client.execute(
            f"ALTER TABLE {_quote_clickhouse_qualified_table(RESOLVED_DATABASE, FINANCIAL_STATEMENTS_TABLE)} "
            f"DELETE WHERE {_quote_clickhouse_identifier('org_number')} IN "
            f"(SELECT {_quote_clickhouse_identifier('org_number')} FROM {affected_stage_qualified}) "
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
            affected_stage_qualified,
            suppress_errors=primary_error is not None,
        )
    return row_counts


def _original_statement_frame(fetch_frame: pl.DataFrame) -> pl.DataFrame:
    rows = financial_normalize.build_resolved_financial_statement_original_rows_from_fetch_rows(
        fetch_frame.to_dicts(),
        resolved_at=datetime.now(UTC),
    )
    return _financial_statement_frame(rows)


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
