import gzip
import json
import time
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime
from hashlib import sha256
from typing import Any
from urllib.parse import quote

import dagster as dg
from pydantic import field_validator

from dagster_v3.defs.common.resources import ObjectStoreResource

EODHD_RAW_BUCKET = "source-eodhd"
EODHD_SOURCE_SYSTEM = "eodhd"
DEFAULT_REFERENCE_REQUEST_DELAY_SECONDS = 0.1
DEFAULT_PRICE_REQUEST_DELAY_SECONDS = 0.05
DEFAULT_PRICE_PROGRESS_INTERVAL = 100
NON_COMPANY_EXCHANGE_CODES = frozenset({"CC", "FOREX", "GBOND", "MONEY", "EUFUND"})
DEFAULT_EQUITY_INSTRUMENT_TYPES = (
    "Common Stock",
    "Preferred Stock",
    "Stock",
)
REPORTED_EXCHANGE_MIC_ALIASES = {
    "NASDAQ": "XNAS",
    "NYSE": "XNYS",
    "NYSE ARCA": "ARCX",
    "NYSE MKT": "XASE",
    "AMEX": "XASE",
    "LSE": "XLON",
    "XETRA": "XETR",
}


class EodhdReferenceConfig(dg.Config):
    exchange_codes_csv: str | None = None
    max_exchanges: int | None = None
    include_delisted: bool = True
    include_non_company_exchanges: bool = False
    request_delay_seconds: float = DEFAULT_REFERENCE_REQUEST_DELAY_SECONDS

    @field_validator("exchange_codes_csv")
    @classmethod
    def validate_exchange_codes_csv(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("exchange_codes_csv must not be blank when provided")
        return stripped

    @field_validator("max_exchanges")
    @classmethod
    def validate_max_exchanges(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("max_exchanges must be greater than zero")
        return value

    @field_validator("request_delay_seconds")
    @classmethod
    def validate_request_delay(cls, value: float) -> float:
        if value < 0:
            raise ValueError("request_delay_seconds must be zero or greater")
        return value

    def configured_exchange_codes(self) -> tuple[str, ...] | None:
        if self.exchange_codes_csv is None:
            return None
        return tuple(
            dict.fromkeys(
                code.strip().upper()
                for code in self.exchange_codes_csv.split(",")
                if code.strip()
            )
        )


class EodhdRawRunConfig(dg.Config):
    raw_run_id: str | None = None

    @field_validator("raw_run_id")
    @classmethod
    def validate_raw_run_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("raw_run_id must not be blank when provided")
        return stripped


class EodhdPriceBackfillConfig(dg.Config):
    instrument_types_csv: str = ",".join(DEFAULT_EQUITY_INSTRUMENT_TYPES)
    include_delisted: bool = True
    max_symbols: int | None = None
    request_delay_seconds: float = DEFAULT_PRICE_REQUEST_DELAY_SECONDS
    progress_interval: int = DEFAULT_PRICE_PROGRESS_INTERVAL

    @field_validator("progress_interval")
    @classmethod
    def validate_positive_int(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("value must be greater than zero")
        return value

    @field_validator("max_symbols")
    @classmethod
    def validate_max_symbols(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("max_symbols must be greater than zero")
        return value

    @field_validator("request_delay_seconds")
    @classmethod
    def validate_price_request_delay(cls, value: float) -> float:
        if value < 0:
            raise ValueError("request_delay_seconds must be zero or greater")
        return value

    @field_validator("instrument_types_csv")
    @classmethod
    def validate_instrument_types(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("instrument_types_csv must not be blank")
        return stripped

    def instrument_types(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                value.strip()
                for value in self.instrument_types_csv.split(",")
                if value.strip()
            )
        )


def write_eodhd_reference_snapshot(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    config: EodhdReferenceConfig,
    run_id: str,
    retrieved_at: str,
    log: Callable[..., object],
) -> dict[str, Any]:
    object_store.ensure_bucket(EODHD_RAW_BUCKET)
    source_exchanges = client.exchanges()
    selected_exchanges = _select_exchanges(source_exchanges, config=config)
    if not selected_exchanges:
        raise ValueError("EODHD exchange list produced no selected exchanges")

    exchange_object_key = reference_exchanges_object_key(run_id)
    exchange_bytes = write_json_gzip(selected_exchanges)
    object_store.write_bytes(
        exchange_object_key,
        exchange_bytes,
        bucket=EODHD_RAW_BUCKET,
    )
    objects: list[dict[str, Any]] = [
        _snapshot_object(
            kind="exchanges",
            object_key=exchange_object_key,
            row_count=len(selected_exchanges),
            content=exchange_bytes,
            exchange_code=None,
            is_delisted=None,
        )
    ]
    active_symbol_count = 0
    delisted_symbol_count = 0

    log("Selected EODHD exchanges: count=%s", len(selected_exchanges))
    for exchange_number, exchange in enumerate(selected_exchanges, start=1):
        exchange_code = required_text(exchange, "Code")
        active_payload = client.symbols(exchange_code, delisted=False)
        active_object_key = reference_symbols_object_key(
            run_id,
            exchange_code=exchange_code,
            is_delisted=False,
        )
        active_bytes = write_json_gzip(active_payload)
        object_store.write_bytes(
            active_object_key,
            active_bytes,
            bucket=EODHD_RAW_BUCKET,
        )
        objects.append(
            _snapshot_object(
                kind="symbols_active",
                object_key=active_object_key,
                row_count=len(active_payload),
                content=active_bytes,
                exchange_code=exchange_code,
                is_delisted=False,
            )
        )
        active_symbol_count += len(active_payload)

        delisted_count = 0
        if config.include_delisted:
            if config.request_delay_seconds > 0:
                time.sleep(config.request_delay_seconds)
            delisted_payload = client.symbols(exchange_code, delisted=True)
            delisted_object_key = reference_symbols_object_key(
                run_id,
                exchange_code=exchange_code,
                is_delisted=True,
            )
            delisted_bytes = write_json_gzip(delisted_payload)
            object_store.write_bytes(
                delisted_object_key,
                delisted_bytes,
                bucket=EODHD_RAW_BUCKET,
            )
            objects.append(
                _snapshot_object(
                    kind="symbols_delisted",
                    object_key=delisted_object_key,
                    row_count=len(delisted_payload),
                    content=delisted_bytes,
                    exchange_code=exchange_code,
                    is_delisted=True,
                )
            )
            delisted_count = len(delisted_payload)
            delisted_symbol_count += delisted_count

        log(
            "Downloaded EODHD symbols: exchange=%s exchange_progress=%s/%s "
            "active=%s delisted=%s total_downloaded=%s",
            exchange_code,
            exchange_number,
            len(selected_exchanges),
            len(active_payload),
            delisted_count,
            active_symbol_count + delisted_symbol_count,
        )
        if config.request_delay_seconds > 0 and exchange_number < len(
            selected_exchanges
        ):
            time.sleep(config.request_delay_seconds)

    snapshot = {
        "schema_version": 1,
        "source_system": EODHD_SOURCE_SYSTEM,
        "source_run_id": run_id,
        "retrieved_at": retrieved_at,
        "completed": True,
        "exchange_count": len(selected_exchanges),
        "active_symbol_count": active_symbol_count,
        "delisted_symbol_count": delisted_symbol_count,
        "objects": objects,
    }
    object_store.write_json(
        reference_snapshot_object_key(run_id),
        json.dumps(snapshot, sort_keys=True, separators=(",", ":")),
        bucket=EODHD_RAW_BUCKET,
    )
    return {
        "source_run_id": run_id,
        "exchange_count": len(selected_exchanges),
        "active_symbol_count": active_symbol_count,
        "delisted_symbol_count": delisted_symbol_count,
        "object_count": len(objects) + 1,
        "snapshot_object_key": reference_snapshot_object_key(run_id),
    }


def read_reference_snapshot(
    object_store: ObjectStoreResource,
    *,
    run_id: str,
) -> dict[str, Any]:
    key = reference_snapshot_object_key(run_id)
    if not object_store.exists(key, bucket=EODHD_RAW_BUCKET):
        raise ValueError(
            f"No completed EODHD reference snapshot found for run_id={run_id}; "
            "materialize eodhd_reference_raw_objects first"
        )
    snapshot = json.loads(object_store.read_bytes(key, bucket=EODHD_RAW_BUCKET))
    if snapshot.get("completed") is not True:
        raise ValueError(f"EODHD reference snapshot is incomplete for run_id={run_id}")
    if snapshot.get("source_run_id") != run_id:
        raise ValueError(
            f"EODHD reference snapshot run ID mismatch for run_id={run_id}"
        )
    objects = snapshot.get("objects")
    if not isinstance(objects, list) or not objects:
        raise ValueError(f"EODHD reference snapshot has no objects for run_id={run_id}")
    return snapshot


def exchange_rows_from_payload(
    payload: Iterable[dict[str, Any]],
    *,
    source_run_id: str,
    retrieved_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    exchanges: list[dict[str, Any]] = []
    exchange_mics: list[dict[str, Any]] = []
    for raw_row in payload:
        exchange_code = required_text(raw_row, "Code").upper()
        operating_mic_raw = optional_text(raw_row.get("OperatingMIC"))
        payload_hash = payload_sha256(raw_row)
        exchanges.append(
            {
                "exchange_code": exchange_code,
                "exchange_name": required_text(raw_row, "Name"),
                "country_name": optional_text(raw_row.get("Country")),
                "country_iso2": optional_upper_text(raw_row.get("CountryISO2")),
                "country_iso3": optional_upper_text(raw_row.get("CountryISO3")),
                "currency": optional_upper_text(raw_row.get("Currency")),
                "operating_mic_raw": operating_mic_raw,
                "source_system": EODHD_SOURCE_SYSTEM,
                "source_run_id": source_run_id,
                "source_record_id": exchange_code,
                "source_payload_hash": payload_hash,
                "retrieved_at": retrieved_at,
            }
        )
        for position, mic in enumerate(split_mics(operating_mic_raw), start=1):
            exchange_mics.append(
                {
                    "exchange_code": exchange_code,
                    "mic": mic,
                    "mic_position": position,
                    "source_system": EODHD_SOURCE_SYSTEM,
                    "source_run_id": source_run_id,
                    "source_record_id": f"{exchange_code}:{mic}",
                    "source_payload_hash": payload_hash,
                    "retrieved_at": retrieved_at,
                }
            )
    return exchanges, exchange_mics


def symbol_rows_from_payload(
    payload: Iterable[dict[str, Any]],
    *,
    exchange_code: str,
    is_delisted: bool,
    source_run_id: str,
    retrieved_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    normalized_exchange_code = exchange_code.strip().upper()
    for raw_row in payload:
        ticker = required_text(raw_row, "Code").upper()
        symbol_key = f"{ticker}.{normalized_exchange_code}"
        rows.append(
            {
                "eodhd_symbol_key": symbol_key,
                "exchange_code": normalized_exchange_code,
                "reported_exchange_code": optional_upper_text(raw_row.get("Exchange")),
                "ticker": ticker,
                "symbol_name": optional_text(raw_row.get("Name")) or ticker,
                "country_name": optional_text(raw_row.get("Country")),
                "currency": optional_upper_text(raw_row.get("Currency")),
                "instrument_type": optional_text(raw_row.get("Type")) or "Unknown",
                "isin": optional_upper_text(raw_row.get("Isin")),
                "is_delisted": 1 if is_delisted else 0,
                "source_system": EODHD_SOURCE_SYSTEM,
                "source_run_id": source_run_id,
                "source_record_id": symbol_key,
                "source_payload_hash": payload_sha256(raw_row),
                "retrieved_at": retrieved_at,
            }
        )
    return rows


def resolve_symbol_mic_rows(
    *,
    symbols: Iterable[dict[str, Any]],
    exchange_mics: dict[str, tuple[str, ...]],
    source_run_id: str,
    resolved_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        symbol_key = str(symbol["eodhd_symbol_key"])
        exchange_code = str(symbol["exchange_code"])
        candidates = exchange_mics.get(exchange_code, ())
        reported_exchange = optional_upper_text(symbol.get("reported_exchange_code"))
        alias_mic = (
            REPORTED_EXCHANGE_MIC_ALIASES.get(reported_exchange)
            if reported_exchange is not None
            else None
        )
        if alias_mic is not None and (not candidates or alias_mic in candidates):
            resolved = ((alias_mic, 1, "reported_exchange_alias", "high"),)
        elif len(candidates) == 1:
            resolved = ((candidates[0], 1, "exchange_code_unique_mic", "high"),)
        else:
            resolved = tuple(
                (mic, 0, "exchange_code_candidate", "low") for mic in candidates
            )
        for mic, is_primary, method, confidence in resolved:
            evidence = {
                "eodhd_symbol_key": symbol_key,
                "exchange_code": exchange_code,
                "reported_exchange_code": reported_exchange,
                "mic": mic,
                "resolution_method": method,
            }
            rows.append(
                {
                    "eodhd_symbol_key": symbol_key,
                    "mic": mic,
                    "is_primary": is_primary,
                    "resolution_method": method,
                    "resolution_confidence": confidence,
                    "source_system": EODHD_SOURCE_SYSTEM,
                    "source_run_id": source_run_id,
                    "source_record_id": f"{symbol_key}:{mic}",
                    "source_payload_hash": payload_sha256(evidence),
                    "resolved_at": resolved_at,
                }
            )
    return rows


def price_rows_from_payload(
    payload: Iterable[dict[str, Any]],
    *,
    symbol: dict[str, Any],
    source_run_id: str,
    source_object_key: str,
    retrieved_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    symbol_key = str(symbol["eodhd_symbol_key"])
    for raw_row in payload:
        price_date = required_text(raw_row, "date")
        date.fromisoformat(price_date)
        rows.append(
            {
                "eodhd_symbol_key": symbol_key,
                "exchange_code": str(symbol["exchange_code"]),
                "ticker": str(symbol["ticker"]),
                "price_date": price_date,
                "open": optional_decimal_text(raw_row.get("open")),
                "high": optional_decimal_text(raw_row.get("high")),
                "low": optional_decimal_text(raw_row.get("low")),
                "close": optional_decimal_text(raw_row.get("close")),
                "adjusted_close": optional_decimal_text(raw_row.get("adjusted_close")),
                "volume": optional_non_negative_int(raw_row.get("volume")),
                "currency": optional_upper_text(symbol.get("currency")),
                "source_system": EODHD_SOURCE_SYSTEM,
                "source_run_id": source_run_id,
                "source_record_id": f"{symbol_key}:{price_date}",
                "source_payload_hash": payload_sha256(raw_row),
                "source_object_key": source_object_key,
                "retrieved_at": retrieved_at,
            }
        )
    return rows


def reference_snapshot_object_key(run_id: str) -> str:
    return f"reference/run_id={run_id}/snapshot.json"


def reference_exchanges_object_key(run_id: str) -> str:
    return f"reference/run_id={run_id}/exchanges.json.gz"


def reference_symbols_object_key(
    run_id: str,
    *,
    exchange_code: str,
    is_delisted: bool,
) -> str:
    status = "delisted" if is_delisted else "active"
    return (
        f"reference/run_id={run_id}/symbols/exchange={quote(exchange_code, safe='')}/"
        f"{status}.json.gz"
    )


def write_json_gzip(payload: Any) -> bytes:
    return gzip.compress(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )


def read_json_gzip(content: bytes) -> Any:
    return json.loads(gzip.decompress(content))


def payload_sha256(payload: Any) -> str:
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def split_mics(value: str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    return tuple(
        dict.fromkeys(mic.strip().upper() for mic in value.split(",") if mic.strip())
    )


def required_text(row: dict[str, Any], key: str) -> str:
    value = optional_text(row.get(key))
    if value is None:
        raise ValueError(f"EODHD row is missing required field {key}")
    return value


def optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def optional_upper_text(value: Any) -> str | None:
    text = optional_text(value)
    return text.upper() if text is not None else None


def optional_decimal_text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def optional_non_negative_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    parsed = int(value)
    if parsed < 0:
        raise ValueError("EODHD volume must not be negative")
    return parsed


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _select_exchanges(
    payload: Iterable[dict[str, Any]],
    *,
    config: EodhdReferenceConfig,
) -> list[dict[str, Any]]:
    configured_codes = config.configured_exchange_codes()
    configured_set = set(configured_codes) if configured_codes is not None else None
    selected = []
    for row in payload:
        code = required_text(row, "Code").upper()
        if configured_set is not None and code not in configured_set:
            continue
        if (
            configured_set is None
            and not config.include_non_company_exchanges
            and code in NON_COMPANY_EXCHANGE_CODES
        ):
            continue
        selected.append(row)
    selected.sort(key=lambda row: required_text(row, "Code"))
    if config.max_exchanges is not None:
        return selected[: config.max_exchanges]
    return selected


def _snapshot_object(
    *,
    kind: str,
    object_key: str,
    row_count: int,
    content: bytes,
    exchange_code: str | None,
    is_delisted: bool | None,
) -> dict[str, Any]:
    return {
        "kind": kind,
        "object_key": object_key,
        "row_count": row_count,
        "content_sha256": sha256(content).hexdigest(),
        "content_length_bytes": len(content),
        "exchange_code": exchange_code,
        "is_delisted": is_delisted,
    }
