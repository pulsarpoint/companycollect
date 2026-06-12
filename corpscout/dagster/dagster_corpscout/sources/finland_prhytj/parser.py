"""Streaming parser for PRH YTJ snapshot artifacts."""

import hashlib
import json
from dataclasses import dataclass
from typing import Any, BinaryIO, Iterator


@dataclass(frozen=True)
class ParsedRecord:
    line_number: int
    payload_hash: str
    payload: dict[str, Any]


def parse_snapshot(stream: BinaryIO) -> Iterator[ParsedRecord]:
    for line_number, raw_line in enumerate(stream, start=1):
        raw_line = raw_line.rstrip(b"\n")
        if raw_line.endswith(b"\r"):
            raw_line = raw_line[:-1]
        if not raw_line.strip():
            continue
        try:
            payload = json.loads(raw_line.strip())
        except json.JSONDecodeError as exc:
            raise ValueError(f"malformed PRH YTJ snapshot JSON on line {line_number}") from exc
        yield ParsedRecord(
            line_number=line_number,
            payload_hash=hashlib.sha256(raw_line).hexdigest(),
            payload=payload,
        )
