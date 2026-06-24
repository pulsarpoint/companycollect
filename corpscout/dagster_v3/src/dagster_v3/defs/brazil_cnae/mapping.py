from __future__ import annotations

import csv
import hashlib
import re
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dagster_v3.defs.brazil_cnae import tables


REQUIRED_FIXTURE_COLUMNS = (
    "cnae_version",
    "cnae_code",
    "cnae_description_pt",
    "cnae_description_en",
    "nace_revision",
    "nace_code",
    "nace_description_en",
    "mapping_source",
    "source_url",
)

NormalizedNaceTarget = tuple[str, str]
MappingRow = tuple[Any, ...]


def normalize_cnae_code(value: str) -> str:
    return re.sub(r"\D+", "", value.strip())


def normalize_nace_code(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "", value.strip())


def build_br_cnae_to_nace_rows(
    *,
    fixture_path: str | Path,
    source_run_id: str,
    valid_nace_targets: set[NormalizedNaceTarget],
    pulled_at: datetime | None = None,
) -> list[MappingRow]:
    resolved_fixture_path = Path(fixture_path)
    payload = resolved_fixture_path.read_bytes()
    source_payload_hash = hashlib.sha256(payload).hexdigest()
    resolved_pulled_at = pulled_at if pulled_at is not None else datetime.now(timezone.utc)

    rows: list[MappingRow] = []
    seen_edges: set[tuple[str, str, str, str]] = set()

    with resolved_fixture_path.open(newline="", encoding="utf-8") as fixture_file:
        reader = csv.DictReader(fixture_file)
        _validate_fixture_header(reader.fieldnames)

        for line_number, raw_row in enumerate(reader, start=2):
            row = _normalized_fixture_row(raw_row, line_number=line_number)
            cnae_normalized_code = normalize_cnae_code(row["cnae_code"])
            nace_normalized_code = normalize_nace_code(row["nace_code"])

            if not cnae_normalized_code:
                raise ValueError(
                    f"Missing normalized CNAE code for fixture row at line {line_number}"
                )
            if not nace_normalized_code:
                raise ValueError(
                    f"Missing normalized NACE code for fixture row at line {line_number}"
                )

            edge = (
                row["cnae_version"],
                cnae_normalized_code,
                row["nace_revision"],
                nace_normalized_code,
            )
            if edge in seen_edges:
                raise ValueError(
                    "Duplicate Brazil CNAE to NACE mapping edge: "
                    f"{row['cnae_version']}/{row['cnae_code']} -> "
                    f"{row['nace_revision']}/{row['nace_code']} at line {line_number}"
                )
            seen_edges.add(edge)

            nace_target = (row["nace_revision"], nace_normalized_code)
            if nace_target not in valid_nace_targets:
                raise ValueError(
                    "Unknown NACE target: "
                    f"{row['nace_revision']}/{row['nace_code']} "
                    f"normalized as {nace_normalized_code} at line {line_number}"
                )

            rows.append(
                (
                    row["cnae_version"],
                    row["cnae_code"],
                    cnae_normalized_code,
                    row["cnae_description_pt"],
                    row["cnae_description_en"],
                    row["nace_revision"],
                    row["nace_code"],
                    nace_normalized_code,
                    row["nace_description_en"],
                    row["mapping_source"],
                    row["source_url"],
                    source_payload_hash,
                    source_run_id,
                    resolved_pulled_at,
                )
            )

    if not rows:
        raise ValueError("Brazil CNAE to NACE fixture produced no rows")

    return rows


def _validate_fixture_header(fieldnames: Sequence[str] | None) -> None:
    if fieldnames is None:
        raise ValueError(
            "Missing required fixture columns: "
            + ", ".join(REQUIRED_FIXTURE_COLUMNS)
        )

    missing_columns = [
        column for column in REQUIRED_FIXTURE_COLUMNS if column not in fieldnames
    ]
    if missing_columns:
        raise ValueError(
            "Missing required fixture columns: " + ", ".join(missing_columns)
        )


def _normalized_fixture_row(
    raw_row: dict[str, str | None],
    *,
    line_number: int,
) -> dict[str, str]:
    row: dict[str, str] = {}
    for column in REQUIRED_FIXTURE_COLUMNS:
        value = raw_row.get(column)
        if value is None or not value.strip():
            raise ValueError(
                f"Missing required fixture value: {column} at line {line_number}"
            )
        row[column] = value.strip()
    return row
