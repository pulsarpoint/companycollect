from collections.abc import Iterator
from datetime import date, timedelta
import os
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from dagster_v3.defs.exchange_rates import tables
from dagster_v3.defs.exchange_rates.clickhouse import prepare_exchange_rates_table
from dagster_v3.defs.exchange_rates.source import (
    DEFAULT_CLICKHOUSE_NATIVE_PORT,
    EXCHANGE_RATES_DLT_TABLE,
    exchange_rates_clickhouse_pipeline,
    exchange_rates_range_source,
)


class ExchangeRatesDltTranslator(DagsterDltTranslator):
    def __init__(self, *, asset_key: str, description: str) -> None:
        super().__init__()
        self._asset_key = asset_key
        self._description = description

    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.table_name != EXCHANGE_RATES_DLT_TABLE:
            return spec
        return spec.replace_attributes(
            key=self._asset_key,
            deps=[],
            group_name="exchange_rates",
            description=self._description,
            kinds={"python", "dlt", "clickhouse", "reference", "fx"},
        )


class ExchangeRatesConfig(dg.Config):
    currencies: list[str] = ["NOK", "USD", "EUR", "GBP", "SEK", "DKK"]


EXCHANGE_RATES_BACKFILL_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2023-01-01",
    end_date=date.today().isoformat(),
)
EXCHANGE_RATES_DAILY_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date=date.today().isoformat(),
)


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


def delete_exchange_rates_window(
    client: Any,
    *,
    start_date: str,
    end_date: str,
    currencies: list[str],
) -> None:
    quote_currencies = sorted({currency.upper() for currency in currencies} | {"EUR", "NOK", "USD"})
    quoted_currencies = ", ".join(f"'{_sql_string(currency)}'" for currency in quote_currencies)
    client.execute(
        "ALTER TABLE reference.exchange_rates DELETE WHERE "
        "source IN ('ECB EXR', 'identity') "
        f"AND rate_date >= '{_sql_string(start_date)}' "
        f"AND rate_date <= '{_sql_string(end_date)}' "
        f"AND quote_currency IN ({quoted_currencies})"
    )


@dlt_assets(
    dlt_source=exchange_rates_range_source(
        start_date="2023-01-01",
        end_date="2023-01-01",
        currencies=[],
    ),
    dlt_pipeline=exchange_rates_clickhouse_pipeline(),
    name="exchange_rates_backfill",
    dagster_dlt_translator=ExchangeRatesDltTranslator(
        asset_key="exchange_rates_backfill",
        description="Monthly backfill of shared exchange rates loaded to ClickHouse from ECB.",
    ),
    partitions_def=EXCHANGE_RATES_BACKFILL_PARTITIONS,
)
def exchange_rates_backfill_asset(
    context: AssetExecutionContext,
    config: ExchangeRatesConfig,
    dlt: DagsterDltResource,
    clickhouse: ClickhouseResource,
) -> Iterator[Any]:
    partition_key_range = context.partition_key_range
    start_date, end_date = month_partition_range_window(
        start_partition_key=partition_key_range.start,
        end_partition_key=partition_key_range.end,
    )
    yield from _run_exchange_rates_partition(
        context=context,
        config=config,
        dlt=dlt,
        clickhouse=clickhouse,
        start_date=start_date,
        end_date=end_date,
        asset_label="backfill",
    )


@dlt_assets(
    dlt_source=exchange_rates_range_source(
        start_date=date.today().isoformat(),
        end_date=date.today().isoformat(),
        currencies=[],
    ),
    dlt_pipeline=exchange_rates_clickhouse_pipeline(),
    name="exchange_rates_daily",
    dagster_dlt_translator=ExchangeRatesDltTranslator(
        asset_key="exchange_rates_daily",
        description="Daily refresh of shared exchange rates loaded to ClickHouse from ECB.",
    ),
    partitions_def=EXCHANGE_RATES_DAILY_PARTITIONS,
)
def exchange_rates_daily_asset(
    context: AssetExecutionContext,
    config: ExchangeRatesConfig,
    dlt: DagsterDltResource,
    clickhouse: ClickhouseResource,
) -> Iterator[Any]:
    partition_key_range = context.partition_key_range
    start_date, end_date = day_partition_range_window(
        start_partition_key=partition_key_range.start,
        end_partition_key=partition_key_range.end,
    )
    yield from _run_exchange_rates_partition(
        context=context,
        config=config,
        dlt=dlt,
        clickhouse=clickhouse,
        start_date=start_date,
        end_date=end_date,
        asset_label="daily",
    )


def _run_exchange_rates_partition(
    *,
    context: AssetExecutionContext,
    config: ExchangeRatesConfig,
    dlt: DagsterDltResource,
    clickhouse: ClickhouseResource,
    start_date: str,
    end_date: str,
    asset_label: str,
) -> Iterator[Any]:
    context.log.info(
        "Preparing ClickHouse table %s for exchange-rate %s partition",
        tables.QUALIFIED_EXCHANGE_RATES_TABLE,
        asset_label,
    )
    prepare_exchange_rates_table(clickhouse)
    context.log.info(
        "Deleting existing exchange-rate rows: table=%s, start_date=%s, end_date=%s, "
        "currencies=%s",
        tables.QUALIFIED_EXCHANGE_RATES_TABLE,
        start_date,
        end_date,
        sorted({currency.upper() for currency in config.currencies} | {"EUR", "NOK", "USD"}),
    )
    with clickhouse.get_connection() as client:
        delete_exchange_rates_window(
            client,
            start_date=start_date,
            end_date=end_date,
            currencies=config.currencies,
        )
    context.log.info(
        "Loading ECB exchange rates into ClickHouse: start_date=%s, end_date=%s, "
        "currencies=%s",
        start_date,
        end_date,
        config.currencies,
    )
    yield from dlt.run(
        context=context,
        dlt_source=exchange_rates_range_source(
            start_date=start_date,
            end_date=end_date,
            currencies=config.currencies,
            source_run_id=context.run_id,
        ),
        dlt_pipeline=exchange_rates_clickhouse_pipeline(),
    )
    context.log.info(
        "Completed exchange-rate %s partition: start_date=%s, end_date=%s",
        asset_label,
        start_date,
        end_date,
    )


exchange_rates_daily_job = dg.define_asset_job(
    "exchange_rates_daily_job",
    selection=dg.AssetSelection.assets("exchange_rates_daily"),
)
exchange_rates_daily_schedule = dg.build_schedule_from_partitioned_job(
    exchange_rates_daily_job,
    name="exchange_rates_daily_schedule",
)


def clickhouse_resource_from_env() -> ClickhouseResource:
    return ClickhouseResource(
        host=dg.EnvVar("CLICKHOUSE_HOST"),
        port=_int_env("CLICKHOUSE_NATIVE_PORT", DEFAULT_CLICKHOUSE_NATIVE_PORT),
        user=dg.EnvVar("CLICKHOUSE_USER"),
        password=dg.EnvVar("CLICKHOUSE_PASSWORD"),
        database=dg.EnvVar("CLICKHOUSE_DATABASE"),
        secure=_bool_env("CLICKHOUSE_SECURE", False),
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


defs = dg.Definitions(
    assets=[exchange_rates_backfill_asset, exchange_rates_daily_asset],
    schedules=[exchange_rates_daily_schedule],
)
