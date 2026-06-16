from collections.abc import Iterator
import os
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from dagster_v3.defs.exchange_rates.clickhouse import prepare_exchange_rates_table
from dagster_v3.defs.exchange_rates.source import (
    DEFAULT_CLICKHOUSE_NATIVE_PORT,
    EXCHANGE_RATES_DLT_TABLE,
    exchange_rates_clickhouse_pipeline,
    exchange_rates_source,
)


class ExchangeRatesDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.table_name != EXCHANGE_RATES_DLT_TABLE:
            return spec
        return spec.replace_attributes(
            key="exchange_rates",
            deps=[],
            group_name="exchange_rates",
            description="Shared exchange rates loaded to ClickHouse from ECB reference rates.",
            kinds={"python", "dlt", "clickhouse", "reference", "fx"},
        )


class ExchangeRatesConfig(dg.Config):
    rate_dates: list[str] = []
    currencies: list[str] = ["NOK", "USD", "EUR", "GBP", "SEK", "DKK"]


@dlt_assets(
    dlt_source=exchange_rates_source(rate_dates=[], currencies=[]),
    dlt_pipeline=exchange_rates_clickhouse_pipeline(),
    name="exchange_rates",
    dagster_dlt_translator=ExchangeRatesDltTranslator(),
)
def exchange_rates_asset(
    context: AssetExecutionContext,
    config: ExchangeRatesConfig,
    dlt: DagsterDltResource,
    clickhouse: ClickhouseResource,
) -> Iterator[Any]:
    context.log.info("Preparing ClickHouse table reference.exchange_rates")
    prepare_exchange_rates_table(clickhouse)
    context.log.info("Loading ECB exchange rates into ClickHouse with dlt")
    yield from dlt.run(
        context=context,
        dlt_source=exchange_rates_source(
            rate_dates=config.rate_dates,
            currencies=config.currencies,
            source_run_id=context.run_id,
        ),
        dlt_pipeline=exchange_rates_clickhouse_pipeline(),
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


defs = dg.Definitions(
    assets=[exchange_rates_asset],
)
