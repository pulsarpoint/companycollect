from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import dlt
import requests
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline

from dagster_v3.defs.exchange_rates import tables

ECB_EXR_BASE_URL = "https://data-api.ecb.europa.eu/service/data/EXR"
EXCHANGE_RATES_DLT_PIPELINE_NAME = "reference_exchange_rates"
EXCHANGE_RATES_DLT_TABLE = "exchange_rates"
EXCHANGE_RATES_DLT_DATASET_NAME: str | None = None
FX_SOURCE_NAME = "ECB EXR"
DEFAULT_CLICKHOUSE_NATIVE_PORT = 9002
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
DEFAULT_ECB_TIMEOUT_SECONDS = 30
EXCHANGE_RATES_PRIMARY_KEY = ["rate_date", "base_currency", "quote_currency", "source"]


@dlt.source(name="exchange_rates")
def exchange_rates_source(
    *,
    rate_dates: list[str],
    currencies: list[str],
    source_run_id: str = "",
    pulled_at: str | None = None,
) -> list[DltResource]:
    effective_pulled_at = pulled_at or _utc_now_iso()
    return [
        *[
            ecb_exchange_rates_resource(
                rate_date=rate_date,
                quote_currency=currency,
                source_run_id=source_run_id,
                pulled_at=effective_pulled_at,
            )
            for rate_date in sorted(set(rate_dates))
            for currency in _quote_currencies(currencies)
        ],
        identity_exchange_rates_resource(
            rate_dates=rate_dates,
            source_run_id=source_run_id,
            pulled_at=effective_pulled_at,
        ),
    ]


@dlt.source(name="exchange_rates")
def exchange_rates_range_source(
    *,
    start_date: str,
    end_date: str,
    currencies: list[str],
    source_run_id: str = "",
    pulled_at: str | None = None,
) -> list[DltResource]:
    effective_pulled_at = pulled_at or _utc_now_iso()
    return [
        ecb_exchange_rates_range_resource(
            start_date=start_date,
            end_date=end_date,
            currencies=currencies,
            source_run_id=source_run_id,
            pulled_at=effective_pulled_at,
        ),
        identity_exchange_rates_for_range_resource(
            start_date=start_date,
            end_date=end_date,
            source_run_id=source_run_id,
            pulled_at=effective_pulled_at,
        ),
    ]


def ecb_exchange_rates_resource(
    *,
    rate_date: str,
    quote_currency: str,
    source_run_id: str,
    pulled_at: str,
) -> DltResource:
    quote_currency = quote_currency.upper()
    source_url = f"{ECB_EXR_BASE_URL}/D.{quote_currency}.EUR.SP00.A"
    resource_name = f"exchange_rates_ecb_{quote_currency.lower()}_{rate_date.replace('-', '_')}"

    def rows() -> Iterator[dict[str, Any]]:
        response = requests.get(
            source_url,
            params={
                "format": "jsondata",
                "startPeriod": rate_date,
                "endPeriod": rate_date,
            },
            headers={"User-Agent": DEFAULT_USER_AGENT},
            timeout=DEFAULT_ECB_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        yield ecb_rate_row_from_payload(
            response.json(),
            quote_currency=quote_currency,
            rate_date=rate_date,
            source_url=source_url,
            source_run_id=source_run_id,
            pulled_at=pulled_at,
        )

    return dlt.resource(
        rows,
        name=resource_name,
        table_name=EXCHANGE_RATES_DLT_TABLE,
        write_disposition="append",
        primary_key=EXCHANGE_RATES_PRIMARY_KEY,
    )


def ecb_exchange_rates_range_resource(
    *,
    start_date: str,
    end_date: str,
    currencies: list[str],
    source_run_id: str,
    pulled_at: str,
) -> DltResource:
    quote_currencies = _quote_currencies(currencies)
    currency_key = "+".join(quote_currencies)
    source_url = f"{ECB_EXR_BASE_URL}/D.{currency_key}.EUR.SP00.A"
    resource_name = (
        f"exchange_rates_ecb_{start_date.replace('-', '_')}_{end_date.replace('-', '_')}"
    )

    def rows() -> Iterator[dict[str, Any]]:
        response = requests.get(
            source_url,
            params={
                "format": "jsondata",
                "startPeriod": start_date,
                "endPeriod": end_date,
            },
            headers={"User-Agent": DEFAULT_USER_AGENT},
            timeout=DEFAULT_ECB_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        yield from ecb_rate_rows_from_range_payload(
            response.json(),
            quote_currencies=quote_currencies,
            source_url=source_url,
            source_run_id=source_run_id,
            pulled_at=pulled_at,
        )

    return dlt.resource(
        rows,
        name=resource_name,
        table_name=EXCHANGE_RATES_DLT_TABLE,
        write_disposition="append",
        primary_key=EXCHANGE_RATES_PRIMARY_KEY,
    )


def identity_exchange_rates_resource(
    *,
    rate_dates: list[str],
    source_run_id: str,
    pulled_at: str,
) -> DltResource:
    return identity_exchange_rates_for_dates_resource(
        rate_dates=rate_dates,
        source_run_id=source_run_id,
        pulled_at=pulled_at,
    )


def identity_exchange_rates_for_dates_resource(
    *,
    rate_dates: list[str],
    source_run_id: str,
    pulled_at: str,
) -> DltResource:
    return dlt.resource(
        [
            identity_eur_row(
                rate_date=rate_date,
                source_run_id=source_run_id,
                pulled_at=pulled_at,
            )
            for rate_date in sorted(set(rate_dates))
        ],
        name="exchange_rates_identity",
        table_name=EXCHANGE_RATES_DLT_TABLE,
        write_disposition="append",
        primary_key=EXCHANGE_RATES_PRIMARY_KEY,
    )


def identity_exchange_rates_for_range_resource(
    *,
    start_date: str,
    end_date: str,
    source_run_id: str,
    pulled_at: str,
) -> DltResource:
    return identity_exchange_rates_for_dates_resource(
        rate_dates=_date_range(start_date=start_date, end_date=end_date),
        source_run_id=source_run_id,
        pulled_at=pulled_at,
    )


def ecb_rate_row_from_payload(
    payload: dict[str, Any],
    *,
    quote_currency: str,
    rate_date: str,
    source_url: str,
    source_run_id: str,
    pulled_at: str,
) -> dict[str, Any]:
    values = _extract_observation_values(payload)
    if not values:
        raise ValueError(f"ECB returned no EUR/{quote_currency} rate for {rate_date}")
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return {
        "rate_date": rate_date,
        "base_currency": "EUR",
        "quote_currency": quote_currency.upper(),
        "rate": str(values[-1]),
        "source": FX_SOURCE_NAME,
        "source_url": source_url,
        "source_payload_hash": hashlib.sha256(body.encode("utf-8")).hexdigest(),
        "source_run_id": source_run_id,
        "pulled_at": pulled_at,
    }


def ecb_rate_rows_from_range_payload(
    payload: dict[str, Any],
    *,
    quote_currencies: list[str],
    source_url: str,
    source_run_id: str,
    pulled_at: str,
) -> list[dict[str, Any]]:
    currency_dimension_index, currency_ids = _series_dimension_ids(
        payload,
        dimension_id="CURRENCY",
    )
    rate_dates = _observation_dates(payload)
    series = _payload_series(payload)
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()

    rows: list[dict[str, Any]] = []
    allowed_currencies = {currency.upper() for currency in quote_currencies}
    for series_key, series_payload in series.items():
        series_indexes = str(series_key).split(":")
        if len(series_indexes) <= currency_dimension_index:
            continue
        currency_index = int(series_indexes[currency_dimension_index])
        quote_currency = currency_ids[currency_index].upper()
        if quote_currency not in allowed_currencies:
            continue
        observations = series_payload.get("observations", {})
        for observation_key in sorted(observations, key=_observation_sort_key):
            observation = observations[observation_key]
            if not observation:
                continue
            rate_date = rate_dates[int(observation_key)]
            rows.append(
                {
                    "rate_date": rate_date,
                    "base_currency": "EUR",
                    "quote_currency": quote_currency,
                    "rate": str(Decimal(str(observation[0]))),
                    "source": FX_SOURCE_NAME,
                    "source_url": source_url,
                    "source_payload_hash": payload_hash,
                    "source_run_id": source_run_id,
                    "pulled_at": pulled_at,
                }
            )
    return rows


def identity_eur_row(*, rate_date: str, source_run_id: str, pulled_at: str) -> dict[str, Any]:
    return {
        "rate_date": rate_date,
        "base_currency": "EUR",
        "quote_currency": "EUR",
        "rate": "1",
        "source": "identity",
        "source_url": "",
        "source_payload_hash": "0" * 64,
        "source_run_id": source_run_id,
        "pulled_at": pulled_at,
    }


def exchange_rates_clickhouse_pipeline(credentials: dict[str, Any] | None = None) -> Pipeline:
    return dlt.pipeline(
        pipeline_name=EXCHANGE_RATES_DLT_PIPELINE_NAME,
        destination=dlt.destinations.clickhouse(
            credentials=credentials or clickhouse_destination_credentials_from_env()
        ),
        dataset_name=EXCHANGE_RATES_DLT_DATASET_NAME,
        dev_mode=False,
    )


def clickhouse_destination_credentials_from_env() -> dict[str, Any]:
    return {
        "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
        "port": _int_env("CLICKHOUSE_NATIVE_PORT", DEFAULT_CLICKHOUSE_NATIVE_PORT),
        "http_port": _int_env(
            "CLICKHOUSE_HTTP_PORT",
            _int_env("CLICKHOUSE_PORT", DEFAULT_CLICKHOUSE_HTTP_PORT),
        ),
        "username": os.getenv("CLICKHOUSE_USER", "default"),
        "password": os.getenv("CLICKHOUSE_PASSWORD") or None,
        "database": tables.EXCHANGE_RATES_DATABASE,
        "secure": 1 if _bool_env("CLICKHOUSE_SECURE", False) else 0,
    }


def _extract_observation_values(payload: dict[str, Any]) -> list[Decimal]:
    if "series" in payload:
        series = payload["series"]
    else:
        data_sets = payload.get("dataSets", [])
        if not data_sets:
            return []
        series = data_sets[0].get("series", {})
    values: list[Decimal] = []
    for series_payload in series.values():
        observations = series_payload.get("observations", {})
        for observation_key in sorted(observations, key=_observation_sort_key):
            observation = observations[observation_key]
            if observation:
                values.append(Decimal(str(observation[0])))
    return values


def _payload_series(payload: dict[str, Any]) -> dict[str, Any]:
    data_sets = payload.get("dataSets", [])
    if not data_sets:
        return {}
    return data_sets[0].get("series", {})


def _series_dimension_ids(payload: dict[str, Any], *, dimension_id: str) -> tuple[int, list[str]]:
    dimensions = payload.get("structure", {}).get("dimensions", {})
    series_dimensions = dimensions.get("series", [])
    if not series_dimensions:
        return 0, []
    for dimension_index, dimension in enumerate(series_dimensions):
        if str(dimension.get("id", "")).upper() == dimension_id.upper():
            return dimension_index, [str(value["id"]) for value in dimension.get("values", [])]
    return 0, [str(value["id"]) for value in series_dimensions[0].get("values", [])]


def _observation_dates(payload: dict[str, Any]) -> list[str]:
    dimensions = payload.get("structure", {}).get("dimensions", {})
    observation_dimensions = dimensions.get("observation", [])
    if not observation_dimensions:
        return []
    return [str(value["id"]) for value in observation_dimensions[0].get("values", [])]


def _date_range(*, start_date: str, end_date: str) -> list[str]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    dates: list[str] = []
    current = start
    while current <= end:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


def _quote_currencies(currencies: list[str]) -> list[str]:
    return [
        currency
        for currency in sorted({currency.upper() for currency in currencies} | {"USD", "NOK"})
        if currency != "EUR"
    ]


def _observation_sort_key(value: str) -> tuple[int, str]:
    return (int(value), value) if value.isdigit() else (0, value)


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else int(value)


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
