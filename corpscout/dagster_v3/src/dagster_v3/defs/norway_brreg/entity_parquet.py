from __future__ import annotations

import json
from decimal import Decimal
from io import BytesIO
from typing import Any

import polars as pl

ENTITY_PARQUET_SCHEMA = {
    "org_number": pl.Utf8,
    "change_type": pl.Utf8,
    "source_change_type": pl.Utf8,
    "updated_at": pl.Utf8,
    "update_id": pl.Int64,
    "entity_url": pl.Utf8,
    "entity_json": pl.Utf8,
    "raw_update_json": pl.Utf8,
}


def entity_records_parquet_bytes(
    records: list[dict[str, Any]],
    *,
    allow_empty: bool = False,
    empty_error_message: str = "Norway Brreg entity records produced no rows",
) -> bytes:
    rows = [_entity_parquet_row(record) for record in records]
    if not rows and not allow_empty:
        raise ValueError(empty_error_message)

    buffer = BytesIO()
    pl.DataFrame(rows, schema=ENTITY_PARQUET_SCHEMA).write_parquet(buffer)
    return buffer.getvalue()


def _entity_parquet_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "org_number": _string(record.get("org_number")),
        "change_type": _string(record.get("change_type")),
        "source_change_type": _string(record.get("source_change_type")),
        "updated_at": _string(record.get("updated_at")),
        "update_id": record.get("update_id"),
        "entity_url": _string(record.get("entity_url")),
        "entity_json": _json_string(record.get("entity")),
        "raw_update_json": _json_string(record.get("raw_update")),
    }


def _json_string(value: Any) -> str:
    if value is None:
        return ""
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=_json_default,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            return str(value)
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _string(value: Any) -> str:
    return "" if value is None else str(value)
