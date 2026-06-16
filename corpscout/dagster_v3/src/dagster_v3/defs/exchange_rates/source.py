from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import dlt
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig

from dagster_v3.defs.exchange_rates import tables

ECB_EXR_BASE_URL = "https://data-api.ecb.europa.eu/service/data/EXR"
EXCHANGE_RATES_DLT_PIPELINE_NAME = "reference_exchange_rates"
EXCHANGE_RATES_DLT_TABLE = "exchange_rates"
EXCHANGE_RATES_DLT_DATASET_NAME: str | None = None
FX_SOURCE_NAME = "ECB EXR"
DEFAULT_CLICKHOUSE_NATIVE_PORT = 9002
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"


@dlt.source(name="exchange_rates")
def exchange_rates_source(
    *,
    rate_dates: list[str],
    currencies: list[str],
    source_run_id: str = "",
    pulled_at: str | None = None,
) -> list[DltResource]:
    effective_pulled_at = pulled_at or _utc_now_iso()
    ecb_source = rest_api_source(
        config=exchange_rate_rest_api_config(
            rate_dates=rate_dates,
            currencies=currencies,
            source_run_id=source_run_id,
            pulled_at=effective_pulled_at,
        ),
        name="exchange_rates_ecb",
    )
    return [
        *ecb_source.resources.values(),
        identity_exchange_rates_resource(
            rate_dates=rate_dates,
            source_run_id=source_run_id,
            pulled_at=effective_pulled_at,
        ),
    ]


def exchange_rate_rest_api_config(
    *,
    rate_dates: list[str],
    currencies: list[str],
    source_run_id: str,
    pulled_at: str,
    user_agent: str = DEFAULT_USER_AGENT,
) -> RESTAPIConfig:
    resources: list[Any] = []
    for rate_date in sorted(set(rate_dates)):
        for currency in sorted({currency.upper() for currency in currencies} | {"USD", "NOK"}):
            if currency == "EUR":
                continue
            resource_name = f"exchange_rates_{currency.lower()}_{rate_date.replace('-', '_')}"
            source_url = f"{ECB_EXR_BASE_URL}/D.{currency}.EUR.SP00.A"
            resources.append(
                {
                    "name": resource_name,
                    "table_name": EXCHANGE_RATES_DLT_TABLE,
                    "write_disposition": "append",
                    "primary_key": ["rate_date", "base_currency", "quote_currency", "source"],
                    "endpoint": {
                        "path": f"D.{currency}.EUR.SP00.A",
                        "params": {
                            "format": "jsondata",
                            "startPeriod": rate_date,
                            "endPeriod": rate_date,
                        },
                        "paginator": "single_page",
                    },
                    "processing_steps": [
                        {
                            "yield_map": _ecb_mapper(
                                quote_currency=currency,
                                rate_date=rate_date,
                                source_url=source_url,
                                source_run_id=source_run_id,
                                pulled_at=pulled_at,
                            )
                        }
                    ],
                }
            )
    return {
        "client": {
            "base_url": f"{ECB_EXR_BASE_URL}/",
            "headers": {"User-Agent": user_agent},
        },
        "resources": resources,
    }


def identity_exchange_rates_resource(
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
        primary_key=["rate_date", "base_currency", "quote_currency", "source"],
    )


def _ecb_mapper(
    *,
    quote_currency: str,
    rate_date: str,
    source_url: str,
    source_run_id: str,
    pulled_at: str,
) -> Any:
    def mapper(payload: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield ecb_rate_row_from_payload(
            payload,
            quote_currency=quote_currency,
            rate_date=rate_date,
            source_url=source_url,
            source_run_id=source_run_id,
            pulled_at=pulled_at,
        )

    return mapper


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
