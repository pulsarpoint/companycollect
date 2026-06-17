from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import dlt
import requests
from dlt.extract.resource import DltResource

ECB_EXR_BASE_URL = "https://data-api.ecb.europa.eu/service/data/EXR"
EXCHANGE_RATES_V2_DUCKDB_PIPELINE_NAME = "exchange_rates_v2_raw"
EXCHANGE_RATES_V2_DUCKDB_DATASET_NAME = "exchange_rates_v2_stage"
EXCHANGE_RATES_V2_RAW_DLT_TABLE = "ecb_raw_payloads"
DEFAULT_CLICKHOUSE_NATIVE_PORT = 9002
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
DEFAULT_ECB_TIMEOUT_SECONDS = 30


@dlt.source(name="exchange_rates_v2_raw")
def exchange_rates_v2_raw_range_source(
    *,
    start_date: str,
    end_date: str,
    currencies: list[str],
    source_run_id: str = "",
    pulled_at: str | None = None,
) -> list[DltResource]:
    effective_pulled_at = pulled_at or _utc_now_iso()
    return [
        ecb_exchange_rate_v2_raw_resource(
            start_date=start_date,
            end_date=end_date,
            currencies=currencies,
            source_run_id=source_run_id,
            pulled_at=effective_pulled_at,
        )
    ]


def ecb_exchange_rate_v2_raw_resource(
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
        name=EXCHANGE_RATES_V2_RAW_DLT_TABLE,
        table_name=EXCHANGE_RATES_V2_RAW_DLT_TABLE,
        write_disposition="append",
        primary_key=["start_date", "end_date", "source_payload_hash"],
    )


def _quote_currencies(currencies: list[str]) -> list[str]:
    return [
        currency
        for currency in sorted({currency.upper() for currency in currencies} | {"USD", "NOK"})
        if currency != "EUR"
    ]


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
