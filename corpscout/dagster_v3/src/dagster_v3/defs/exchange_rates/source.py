from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import dlt
import requests
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline

ECB_EXR_BASE_URL = "https://data-api.ecb.europa.eu/service/data/EXR"
EXCHANGE_RATES_DUCKDB_PIPELINE_NAME = "exchange_rates_raw"
EXCHANGE_RATES_DUCKDB_DATASET_NAME = "exchange_rates_stage"
EXCHANGE_RATES_RAW_DLT_TABLE = "ecb_raw_payloads"
FX_SOURCE_NAME = "ECB EXR"
DEFAULT_CLICKHOUSE_NATIVE_PORT = 9002
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
DEFAULT_ECB_TIMEOUT_SECONDS = 30


@dlt.source(name="exchange_rates_raw")
def exchange_rates_raw_range_source(
    *,
    start_date: str,
    end_date: str,
    currencies: list[str],
    source_run_id: str = "",
    pulled_at: str | None = None,
) -> list[DltResource]:
    effective_pulled_at = pulled_at or _utc_now_iso()
    return [
        ecb_exchange_rate_raw_resource(
            start_date=start_date,
            end_date=end_date,
            currencies=currencies,
            source_run_id=source_run_id,
            pulled_at=effective_pulled_at,
        )
    ]


def ecb_exchange_rate_raw_resource(
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
    request_params = {
        "format": "jsondata",
        "startPeriod": start_date,
        "endPeriod": end_date,
    }

    def rows() -> Iterator[dict[str, Any]]:
        response = requests.get(
            source_url,
            params=request_params,
            headers={"User-Agent": DEFAULT_USER_AGENT},
            timeout=DEFAULT_ECB_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        yield {
            "start_date": start_date,
            "end_date": end_date,
            "quote_currencies_json": json.dumps(quote_currencies, separators=(",", ":")),
            "source_url": source_url,
            "request_params_json": json.dumps(request_params, sort_keys=True, separators=(",", ":")),
            "source_payload_json": payload_json,
            "source_payload_hash": hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
            "source_run_id": source_run_id,
            "pulled_at": pulled_at,
        }

    return dlt.resource(
        rows,
        name=EXCHANGE_RATES_RAW_DLT_TABLE,
        table_name=EXCHANGE_RATES_RAW_DLT_TABLE,
        write_disposition="append",
        primary_key=["start_date", "end_date", "source_payload_hash"],
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


def exchange_rates_duckdb_pipeline(*, destination_path: str) -> Pipeline:
    return dlt.pipeline(
        pipeline_name=EXCHANGE_RATES_DUCKDB_PIPELINE_NAME,
        destination=dlt.destinations.duckdb(destination_path),
        dataset_name=EXCHANGE_RATES_DUCKDB_DATASET_NAME,
        dev_mode=False,
    )


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


def _quote_currencies(currencies: list[str]) -> list[str]:
    return [
        currency
        for currency in sorted({currency.upper() for currency in currencies} | {"USD", "NOK"})
        if currency != "EUR"
    ]


def _observation_sort_key(value: str) -> tuple[int, str]:
    return (int(value), value) if value.isdigit() else (0, value)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
