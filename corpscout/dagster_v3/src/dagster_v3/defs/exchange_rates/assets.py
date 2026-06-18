from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import dagster as dg
import dlt as dlt_lib
import duckdb
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from dagster_v3.defs.clickhouse.resolved import export_duckdb_table_to_clickhouse
from dagster_v3.defs.duckdb.schema_contract import (
    create_duckdb_table_from_contract,
    dagster_table_schema_from_contract,
    validate_duckdb_table_contract,
)
from dagster_v3.defs.exchange_rates import tables
from dagster_v3.defs.exchange_rates.source import (
    DEFAULT_CLICKHOUSE_NATIVE_PORT,
    EXCHANGE_RATES_DUCKDB_PIPELINE_NAME,
    EXCHANGE_RATES_DUCKDB_DATASET_NAME,
    EXCHANGE_RATES_RAW_DLT_TABLE,
    ecb_rate_rows_from_range_payload,
    exchange_rates_raw_range_source,
    identity_eur_row,
)

GROUP_NAME = "exchange_rates"
EXCHANGE_RATES_DUCKDB_PATH = Path("data/exchange_rates_source.duckdb")
ECB_RATES_TABLE = "ecb_rates"
IDENTITY_RATES_TABLE = "identity_rates"
CLICKHOUSE_EXPORT_TABLE = "clickhouse_exchange_rates"
EXCHANGE_RATES_PARTITIONS = dg.DailyPartitionsDefinition(start_date="2023-01-01")


class ExchangeRatesDltTranslator(DagsterDltTranslator):
    def __init__(self, *, asset_key: str, description: str) -> None:
        super().__init__()
        self._asset_key = asset_key
        self._description = description

    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.table_name != EXCHANGE_RATES_RAW_DLT_TABLE:
            return spec
        return spec.replace_attributes(
            key=self._asset_key,
            deps=[],
            group_name=GROUP_NAME,
            description=self._description,
            kinds={"python", "dlt", "duckdb", "reference", "fx"},
        )


class ExchangeRatesConfig(dg.Config):
    currencies: list[str] = ["NOK", "USD", "EUR", "GBP", "SEK", "DKK"]


def month_partition_window(partition_key: str, *, today: date | None = None) -> tuple[str, str]:
    start = date.fromisoformat(partition_key)
    next_month = date(start.year + (start.month // 12), (start.month % 12) + 1, 1)
    end = next_month - timedelta(days=1)
    effective_today = today or date.today()
    return start.isoformat(), min(end, effective_today).isoformat()


def month_partition_range_window(
    *,
    start_partition_key: str,
    end_partition_key: str,
    today: date | None = None,
) -> tuple[str, str]:
    start_date, _ = month_partition_window(start_partition_key, today=today)
    _, end_date = month_partition_window(end_partition_key, today=today)
    return start_date, end_date


def day_partition_window(partition_key: str) -> tuple[str, str]:
    day = date.fromisoformat(partition_key)
    return day.isoformat(), day.isoformat()


def day_partition_range_window(
    *,
    start_partition_key: str,
    end_partition_key: str,
) -> tuple[str, str]:
    start_date, _ = day_partition_window(start_partition_key)
    _, end_date = day_partition_window(end_partition_key)
    return start_date, end_date


# dagster-dlt needs a concrete source and pipeline when definitions are loaded.
# These fixed dates are only the definition-time shape; runtime partitions,
# currencies, and run id are passed to dlt.run inside the asset function.
@dlt_assets(
    dlt_source=exchange_rates_raw_range_source(
        start_date="2023-01-01",
        end_date="2023-01-01",
        currencies=[],
    ),
    dlt_pipeline=dlt_lib.pipeline(
        pipeline_name=EXCHANGE_RATES_DUCKDB_PIPELINE_NAME,
        destination=dlt_lib.destinations.duckdb(str(EXCHANGE_RATES_DUCKDB_PATH)),
        dataset_name=EXCHANGE_RATES_DUCKDB_DATASET_NAME,
        dev_mode=False,
    ),
    name="exchange_rates_raw_duckdb",
    dagster_dlt_translator=ExchangeRatesDltTranslator(
        asset_key="exchange_rates_raw_duckdb",
        description="Raw ECB exchange-rate API payloads stored in DuckDB.",
    ),
    partitions_def=EXCHANGE_RATES_PARTITIONS,
)
def exchange_rates_raw_duckdb_asset(
    context: AssetExecutionContext,
    config: ExchangeRatesConfig,
    dlt: DagsterDltResource,
) -> Iterator[Any]:
    start_date, end_date = _context_partition_range(context)
    EXCHANGE_RATES_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    context.log.info(
        "Loading raw ECB exchange-rate payload to DuckDB: duckdb_path=%s, start_date=%s, "
        "end_date=%s, currencies=%s",
        EXCHANGE_RATES_DUCKDB_PATH,
        start_date,
        end_date,
        config.currencies,
    )
    yield from dlt.run(
        context=context,
        dlt_source=exchange_rates_raw_range_source(
            start_date=start_date,
            end_date=end_date,
            currencies=config.currencies,
            source_run_id=context.run_id,
        ),
        dlt_pipeline=dlt_lib.pipeline(
            pipeline_name=EXCHANGE_RATES_DUCKDB_PIPELINE_NAME,
            destination=dlt_lib.destinations.duckdb(str(EXCHANGE_RATES_DUCKDB_PATH)),
            dataset_name=EXCHANGE_RATES_DUCKDB_DATASET_NAME,
            dev_mode=False,
        ),
    )


@dg.asset(
    deps=[dg.AssetKey("exchange_rates_raw_duckdb")],
    partitions_def=EXCHANGE_RATES_PARTITIONS,
    group_name=GROUP_NAME,
    kinds={"duckdb"},
    metadata={
        "dagster/column_schema": dagster_table_schema_from_contract(
            tables.EXCHANGE_RATES_DUCKDB_CONTRACT
        )
    },
    description="Normalized ECB exchange-rate rows parsed from raw ECB payloads in DuckDB.",
)
def exchange_rates_ecb_rates_duckdb(context: AssetExecutionContext) -> dg.MaterializeResult:
    start_date, end_date = _context_partition_range(context)
    counts = normalize_exchange_rates_ecb_duckdb(
        duckdb_path=EXCHANGE_RATES_DUCKDB_PATH,
        start_date=start_date,
        end_date=end_date,
    )
    context.log.info("Normalized ECB exchange-rate rows in DuckDB", extra=counts)
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("exchange_rates_ecb_rates_duckdb")],
    partitions_def=EXCHANGE_RATES_PARTITIONS,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    metadata={
        "dagster/column_schema": dagster_table_schema_from_contract(
            tables.EXCHANGE_RATES_DUCKDB_CONTRACT
        )
    },
    description="Generated EUR/EUR identity exchange-rate rows in DuckDB.",
)
def exchange_rates_identity_rates_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    start_date, end_date = _context_partition_range(context)
    counts = generate_exchange_rates_identity_duckdb(
        duckdb_path=EXCHANGE_RATES_DUCKDB_PATH,
        start_date=start_date,
        end_date=end_date,
    )
    context.log.info("Generated identity exchange-rate rows in DuckDB", extra=counts)
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[
        dg.AssetKey("exchange_rates_ecb_rates_duckdb"),
        dg.AssetKey("exchange_rates_identity_rates_duckdb"),
    ],
    partitions_def=EXCHANGE_RATES_PARTITIONS,
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse"},
    description="Exchange-rate reference rows exported from DuckDB to migrated ClickHouse table.",
)
def exchange_rates_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    start_date, end_date = _context_partition_range(context)
    counts = export_exchange_rates_clickhouse(
        duckdb_path=EXCHANGE_RATES_DUCKDB_PATH,
        clickhouse=clickhouse,
        start_date=start_date,
        end_date=end_date,
    )
    context.log.info("Exported exchange-rate rows to ClickHouse", extra=counts)
    return dg.MaterializeResult(metadata=counts)


def normalize_exchange_rates_ecb_duckdb(
    *,
    duckdb_path: str | Path,
    start_date: str,
    end_date: str,
) -> dict[str, int | str]:
    with duckdb.connect(str(duckdb_path)) as connection:
        _ensure_exchange_rates_duckdb_schema(connection)
        connection.execute(
            f"""
            delete from {EXCHANGE_RATES_DUCKDB_DATASET_NAME}.{ECB_RATES_TABLE}
            where rate_date >= ? and rate_date <= ?
            """,
            [start_date, end_date],
        )
        raw_rows = connection.execute(
            f"""
            select
              source_payload_json,
              quote_currencies_json,
              source_url,
              source_run_id,
              pulled_at
            from {EXCHANGE_RATES_DUCKDB_DATASET_NAME}.{EXCHANGE_RATES_RAW_DLT_TABLE}
            where start_date >= ? and end_date <= ?
            """,
            [start_date, end_date],
        ).fetchall()
        normalized_rows: list[tuple[str, str, str, str, str, str, str, str, str, str, str]] = []
        for payload_json, quote_currencies_json, source_url, source_run_id, pulled_at in raw_rows:
            rows = ecb_rate_rows_from_range_payload(
                json.loads(payload_json),
                quote_currencies=json.loads(quote_currencies_json),
                source_url=source_url,
                source_run_id=source_run_id,
                pulled_at=_pulled_at_utc_string(pulled_at),
            )
            normalized_rows.extend(_exchange_rate_row_tuple(row) for row in rows)
        if normalized_rows:
            connection.executemany(
                f"""
                insert into {EXCHANGE_RATES_DUCKDB_DATASET_NAME}.{ECB_RATES_TABLE}
                ({", ".join(tables.EXCHANGE_RATES_COLUMNS)})
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                normalized_rows,
            )
    return {"raw_payloads": len(raw_rows), "ecb_rates": len(normalized_rows)}


def generate_exchange_rates_identity_duckdb(
    *,
    duckdb_path: str | Path,
    start_date: str,
    end_date: str,
) -> dict[str, int]:
    with duckdb.connect(str(duckdb_path)) as connection:
        _ensure_exchange_rates_duckdb_schema(connection)
        connection.execute(
            f"""
            delete from {EXCHANGE_RATES_DUCKDB_DATASET_NAME}.{IDENTITY_RATES_TABLE}
            where rate_date >= ? and rate_date <= ?
            """,
            [start_date, end_date],
        )
        source_dates = connection.execute(
            f"""
            select rate_date, min(source_run_id), min(pulled_at)
            from {EXCHANGE_RATES_DUCKDB_DATASET_NAME}.{ECB_RATES_TABLE}
            where rate_date >= ? and rate_date <= ?
            group by rate_date
            order by rate_date
            """,
            [start_date, end_date],
        ).fetchall()
        identity_rows = [
            _exchange_rate_row_tuple(
                identity_eur_row(
                    rate_date=str(rate_date),
                    source_run_id=str(source_run_id),
                    pulled_at=_pulled_at_utc_string(pulled_at),
                )
            )
            for rate_date, source_run_id, pulled_at in source_dates
        ]
        if identity_rows:
            connection.executemany(
                f"""
                insert into {EXCHANGE_RATES_DUCKDB_DATASET_NAME}.{IDENTITY_RATES_TABLE}
                ({", ".join(tables.EXCHANGE_RATES_COLUMNS)})
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                identity_rows,
            )
    return {"identity_rates": len(identity_rows)}


def export_exchange_rates_clickhouse(
    *,
    duckdb_path: str | Path,
    clickhouse: ClickhouseResource,
    start_date: str,
    end_date: str,
) -> dict[str, int | str]:
    with duckdb.connect(str(duckdb_path)) as connection:
        _validate_exchange_rates_duckdb_table(connection, ECB_RATES_TABLE)
        _validate_exchange_rates_duckdb_table(connection, IDENTITY_RATES_TABLE)
        connection.execute(
            f"""
            create or replace table {EXCHANGE_RATES_DUCKDB_DATASET_NAME}.{CLICKHOUSE_EXPORT_TABLE}
            as
            select {", ".join(tables.EXCHANGE_RATES_COLUMNS)}
            from {EXCHANGE_RATES_DUCKDB_DATASET_NAME}.{ECB_RATES_TABLE}
            where rate_date >= '{_duckdb_string(start_date)}' and rate_date <= '{_duckdb_string(end_date)}'
            union all
            select {", ".join(tables.EXCHANGE_RATES_COLUMNS)}
            from {EXCHANGE_RATES_DUCKDB_DATASET_NAME}.{IDENTITY_RATES_TABLE}
            where rate_date >= '{_duckdb_string(start_date)}' and rate_date <= '{_duckdb_string(end_date)}'
            """
        )
    with clickhouse.get_connection() as client:
        delete_exchange_rates_window(client, start_date=start_date, end_date=end_date)
        row_count = export_duckdb_table_to_clickhouse(
            duckdb_path=duckdb_path,
            clickhouse_client=client,
            duckdb_schema=EXCHANGE_RATES_DUCKDB_DATASET_NAME,
            duckdb_table=CLICKHOUSE_EXPORT_TABLE,
            clickhouse_database=tables.EXCHANGE_RATES_DATABASE,
            clickhouse_table=tables.EXCHANGE_RATES_TABLE,
            columns=tables.EXCHANGE_RATES_COLUMNS,
            truncate=False,
        )
    return {
        "rows": row_count,
        "table": tables.QUALIFIED_EXCHANGE_RATES_TABLE,
        "start_date": start_date,
        "end_date": end_date,
    }


def delete_exchange_rates_window(
    client: Any,
    *,
    start_date: str,
    end_date: str,
) -> None:
    client.execute(
        "ALTER TABLE reference.exchange_rates DELETE WHERE "
        "source IN ('ECB EXR', 'identity') "
        f"AND rate_date >= '{_sql_string(start_date)}' "
        f"AND rate_date <= '{_sql_string(end_date)}'"
    )


exchange_rates_selection = dg.AssetSelection.assets("exchange_rates_clickhouse").upstream()

exchange_rates_backfill_job = dg.define_asset_job(
    "exchange_rates_backfill_job",
    selection=exchange_rates_selection,
)
exchange_rates_daily_job = dg.define_asset_job(
    "exchange_rates_daily_job",
    selection=exchange_rates_selection,
)


@dg.schedule(
    name="exchange_rates_daily_schedule",
    cron_schedule="30 18 * * 1-5",
    execution_timezone="Europe/Belgrade",
    job=exchange_rates_daily_job,
)
def exchange_rates_daily_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.RunRequest:
    scheduled_time = context.scheduled_execution_time
    partition_key = (
        scheduled_time.date().isoformat()
        if scheduled_time is not None
        else date.today().isoformat()
    )
    return dg.RunRequest(partition_key=partition_key)


def clickhouse_resource_from_env() -> ClickhouseResource:
    return ClickhouseResource(
        host=dg.EnvVar("CLICKHOUSE_HOST"),
        port=_int_env("CLICKHOUSE_NATIVE_PORT", DEFAULT_CLICKHOUSE_NATIVE_PORT),
        user=dg.EnvVar("CLICKHOUSE_USER"),
        password=dg.EnvVar("CLICKHOUSE_PASSWORD"),
        database=dg.EnvVar("CLICKHOUSE_DATABASE"),
        secure=_bool_env("CLICKHOUSE_SECURE", False),
    )


def _ensure_exchange_rates_duckdb_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema if not exists {EXCHANGE_RATES_DUCKDB_DATASET_NAME}")
    for table in (ECB_RATES_TABLE, IDENTITY_RATES_TABLE):
        _ensure_exchange_rates_derived_table(connection, table)


def _ensure_exchange_rates_derived_table(
    connection: duckdb.DuckDBPyConnection,
    table: str,
) -> None:
    try:
        create_duckdb_table_from_contract(
            connection,
            schema=EXCHANGE_RATES_DUCKDB_DATASET_NAME,
            table=table,
            contract=tables.EXCHANGE_RATES_DUCKDB_CONTRACT,
        )
    except ValueError:
        _repair_exchange_rates_derived_table(connection, table)
        validate_duckdb_table_contract(
            connection,
            schema=EXCHANGE_RATES_DUCKDB_DATASET_NAME,
            table=table,
            contract=tables.EXCHANGE_RATES_DUCKDB_CONTRACT,
        )


def _repair_exchange_rates_derived_table(
    connection: duckdb.DuckDBPyConnection,
    table: str,
) -> None:
    repaired_table = f"{table}__contract_repair"
    connection.execute(
        f"drop table if exists {EXCHANGE_RATES_DUCKDB_DATASET_NAME}.{repaired_table}"
    )
    create_duckdb_table_from_contract(
        connection,
        schema=EXCHANGE_RATES_DUCKDB_DATASET_NAME,
        table=repaired_table,
        contract=tables.EXCHANGE_RATES_DUCKDB_CONTRACT,
    )
    select_columns = ", ".join(
        _exchange_rates_contract_cast_expression(column.name, column.duckdb_type)
        for column in tables.EXCHANGE_RATES_DUCKDB_CONTRACT.columns
    )
    connection.execute(
        f"""
        insert into {EXCHANGE_RATES_DUCKDB_DATASET_NAME}.{repaired_table}
        ({", ".join(tables.EXCHANGE_RATES_COLUMNS)})
        select {select_columns}
        from {EXCHANGE_RATES_DUCKDB_DATASET_NAME}.{table}
        """
    )
    connection.execute(f"drop table {EXCHANGE_RATES_DUCKDB_DATASET_NAME}.{table}")
    connection.execute(
        f"""
        alter table {EXCHANGE_RATES_DUCKDB_DATASET_NAME}.{repaired_table}
        rename to {table}
        """
    )


def _exchange_rates_contract_cast_expression(column_name: str, duckdb_type: str) -> str:
    quoted_column = f'"{column_name}"'
    if column_name == "pulled_at":
        return (
            f"cast(cast({quoted_column} as timestamp with time zone) "
            "at time zone 'UTC' as timestamp)"
        )
    return f"cast({quoted_column} as {duckdb_type})"


def _validate_exchange_rates_duckdb_table(
    connection: duckdb.DuckDBPyConnection,
    table: str,
) -> None:
    validate_duckdb_table_contract(
        connection,
        schema=EXCHANGE_RATES_DUCKDB_DATASET_NAME,
        table=table,
        contract=tables.EXCHANGE_RATES_DUCKDB_CONTRACT,
    )


def _exchange_rate_row_tuple(row: dict[str, Any]) -> tuple[str, str, str, str, str, str, str, str, str, str, str]:
    dlt_id_source = "|".join(
        str(row.get(column, "")) for column in tables.EXCHANGE_RATES_COLUMNS[:-2]
    )
    row_with_dlt = {
        **row,
        "_dlt_load_id": "",
        "_dlt_id": hashlib.sha256(dlt_id_source.encode("utf-8")).hexdigest(),
    }
    return tuple(str(row_with_dlt.get(column, "")) for column in tables.EXCHANGE_RATES_COLUMNS)


def _context_partition_range(context: AssetExecutionContext) -> tuple[str, str]:
    partition_key_range = context.partition_key_range
    return day_partition_range_window(
        start_partition_key=partition_key_range.start,
        end_partition_key=partition_key_range.end,
    )


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else int(value)


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def _duckdb_string(value: str) -> str:
    return value.replace("'", "''")


def _pulled_at_utc_string(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(UTC).replace(tzinfo=None).isoformat(
                timespec="milliseconds"
            )
        return value.isoformat(timespec="milliseconds")
    return str(value)


defs = dg.Definitions(
    assets=[
        exchange_rates_raw_duckdb_asset,
        exchange_rates_ecb_rates_duckdb,
        exchange_rates_identity_rates_duckdb,
        exchange_rates_clickhouse,
    ],
    jobs=[exchange_rates_backfill_job, exchange_rates_daily_job],
    schedules=[exchange_rates_daily_schedule],
)
