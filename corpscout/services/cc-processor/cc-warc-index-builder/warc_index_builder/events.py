"""Structured command events for the WARC index builder."""

import json
import sys
from datetime import datetime, timezone
from typing import Literal, TextIO

import humanize


EventLevel = Literal["INFO", "WARN", "ERROR"]


def binary_size(byte_count: int) -> str:
    if byte_count < 0:
        raise ValueError("byte count must not be negative")
    return humanize.naturalsize(byte_count, binary=True, format="%.1f")


def emit_event(
    message: str,
    *,
    level: EventLevel = "INFO",
    stream: TextIO | None = None,
    **fields: object,
) -> None:
    event: dict[str, object] = {
        "time": datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z"),
        "level": level,
        "msg": message,
    }
    for key, value in fields.items():
        if key in event:
            raise ValueError(f"event field {key!r} is reserved")
        event[key] = value
    print(
        json.dumps(event, ensure_ascii=False, separators=(",", ":")),
        file=stream if stream is not None else sys.stdout,
        flush=True,
    )
