from __future__ import annotations

import csv
import hashlib
import re
import uuid
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dagster_v3.defs.brazil_companies.cnae import tables


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
    resolved_pulled_at = (
        pulled_at if pulled_at is not None else datetime.now(timezone.utc)
    )

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

            output_row: dict[str, Any] = {
                "cnae_version": row["cnae_version"],
                "cnae_code": row["cnae_code"],
                "cnae_normalized_code": cnae_normalized_code,
                "cnae_description_pt": row["cnae_description_pt"],
                "cnae_description_en": row["cnae_description_en"],
                "nace_revision": row["nace_revision"],
                "nace_code": row["nace_code"],
                "nace_normalized_code": nace_normalized_code,
                "nace_description_en": row["nace_description_en"],
                "mapping_source": row["mapping_source"],
                "source_url": row["source_url"],
                "source_payload_hash": source_payload_hash,
                "source_run_id": source_run_id,
                "pulled_at": resolved_pulled_at,
            }
            rows.append(
                tuple(output_row[column] for column in tables.BR_CNAE_TO_NACE_COLUMNS)
            )

    if not rows:
        raise ValueError("Brazil CNAE to NACE fixture produced no rows")

    return rows


def replace_br_cnae_to_nace_clickhouse(
    *,
    clickhouse_client: Any,
    fixture_path: str | Path,
    source_run_id: str,
    pulled_at: datetime | None = None,
) -> dict[str, int]:
    valid_nace_targets = _load_valid_nace_targets(clickhouse_client)
    rows = build_br_cnae_to_nace_rows(
        fixture_path=fixture_path,
        source_run_id=source_run_id,
        valid_nace_targets=valid_nace_targets,
        pulled_at=pulled_at,
    )

    stage_table = f"_tmp_{tables.BR_CNAE_TO_NACE_TABLE}_{uuid.uuid4().hex}"
    qualified_stage_table = _qualified_clickhouse_table(stage_table)
    qualified_target_table = _qualified_clickhouse_table(tables.BR_CNAE_TO_NACE_TABLE)
    columns = ", ".join(tables.BR_CNAE_TO_NACE_COLUMNS)
    primary_error: Exception | None = None

    try:
        clickhouse_client.execute(
            f"CREATE TABLE {qualified_stage_table} AS {qualified_target_table}"
        )
        clickhouse_client.execute(
            f"INSERT INTO {qualified_stage_table} ({columns}) VALUES",
            rows,
        )
        clickhouse_client.execute(
            f"EXCHANGE TABLES {qualified_stage_table} AND {qualified_target_table}"
        )
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        try:
            clickhouse_client.execute(f"DROP TABLE IF EXISTS {qualified_stage_table}")
        except Exception:
            if primary_error is None:
                raise

    return {
        "rows": len(rows),
        "cnae_codes": len({str(row[2]) for row in rows}),
        "nace_targets": len({(str(row[5]), str(row[7])) for row in rows}),
    }


def _load_valid_nace_targets(clickhouse_client: Any) -> set[NormalizedNaceTarget]:
    rows = clickhouse_client.execute(
        "SELECT classification_version, normalized_code, description_en "
        f"FROM {_qualified_clickhouse_table('nace_categories')}"
    )
    valid_nace_targets = {(str(row[0]), str(row[1])) for row in rows}
    if not valid_nace_targets:
        raise ValueError(
            "No NACE categories available for Brazil CNAE mapping validation"
        )
    return valid_nace_targets


def _qualified_clickhouse_table(table: str) -> str:
    return (
        f"{_quote_clickhouse_identifier(tables.BRAZIL_COMP_CNAE_DATABASE)}."
        f"{_quote_clickhouse_identifier(table)}"
    )


def _quote_clickhouse_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def _validate_fixture_header(fieldnames: Sequence[str] | None) -> None:
    if fieldnames is None:
        raise ValueError(
            "Missing required fixture columns: " + ", ".join(REQUIRED_FIXTURE_COLUMNS)
        )

    missing_columns = [
        column for column in REQUIRED_FIXTURE_COLUMNS if column not in fieldnames
    ]
    if missing_columns:
        raise ValueError(
            "Missing required fixture columns: " + ", ".join(missing_columns)
        )


def _normalized_fixture_row(
    raw_row: dict[str | None, str | list[str] | None],
    *,
    line_number: int,
) -> dict[str, str]:
    if None in raw_row:
        raise ValueError(
            f"Unexpected extra fixture values at line {line_number}: {raw_row[None]}"
        )

    row: dict[str, str] = {}
    for column in REQUIRED_FIXTURE_COLUMNS:
        value = raw_row.get(column)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"Missing required fixture value: {column} at line {line_number}"
            )
        row[column] = value.strip()
    return row
