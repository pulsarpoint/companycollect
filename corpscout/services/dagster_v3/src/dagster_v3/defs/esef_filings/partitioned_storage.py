"""Per-processed-week DuckDB storage for canonical ESEF parsing outputs."""

from __future__ import annotations

import tempfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from dagster_v3.defs.common.partition_duckdb import partition_duckdb_path
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.segment_assets import (
    esef_document_result_metadata,
    iter_esef_document_result_rows,
    local_esef_document_result,
    processed_week_bounds,
)


PARTITION_STATUS_TABLE = "_partition_status"
QUALIFIED_PARTITION_STATUS_TABLE = f"{tables.DLT_DATASET_NAME}.{PARTITION_STATUS_TABLE}"
ROW_BATCH_SIZE = 10_000

FACTS_STORAGE = "esef_filing_facts"
CONTACT_CANDIDATES_STORAGE = "esef_document_contact_candidates"
CONCEPT_LABELS_STORAGE = "esef_document_concept_labels"
DISCLOSURES_STORAGE = "esef_disclosures"


@dataclass(frozen=True)
class ResultProjection:
    dataset_name: str
    storage_source: str
    property_name: str
    expected_row_count_property: str
    table: str
    columns: tuple[str, ...]
    integer_columns: frozenset[str]
    boolean_columns: frozenset[str] = frozenset()

    @property
    def qualified_table(self) -> str:
        return f"{tables.DLT_DATASET_NAME}.{self.table}"


CONTACT_CANDIDATES_PROJECTION = ResultProjection(
    dataset_name="contact_candidates",
    storage_source=CONTACT_CANDIDATES_STORAGE,
    property_name="candidate_rows",
    expected_row_count_property="contact_candidate_row_count",
    table=tables.ESEF_DOCUMENT_CONTACT_CANDIDATES_TABLE,
    columns=tables.ESEF_DOCUMENT_CONTACT_CANDIDATES_PARTITION_EXPORT_COLUMNS,
    integer_columns=frozenset({"fiscal_year", "evidence_count"}),
)
CONCEPT_LABELS_PROJECTION = ResultProjection(
    dataset_name="taxonomy_labels",
    storage_source=CONCEPT_LABELS_STORAGE,
    property_name="concept_label_rows",
    expected_row_count_property="concept_label_row_count",
    table=tables.ESEF_DOCUMENT_CONCEPT_LABELS_TABLE,
    columns=tables.ESEF_DOCUMENT_CONCEPT_LABELS_PARTITION_EXPORT_COLUMNS,
    integer_columns=frozenset({"fiscal_year"}),
    boolean_columns=frozenset({"is_extension", "is_report_language"}),
)


def esef_partition_duckdb_path(*, storage_source: str, partition_key: str) -> Path:
    processed_week_bounds(partition_key)
    return partition_duckdb_path(source=storage_source, partition=partition_key)


def write_result_projection_partition(
    *,
    object_store: Any,
    partition_key: str,
    projection: ResultProjection,
    target_path: Path | None = None,
) -> dict[str, object]:
    """Stream one artifact-result array into an atomic partition database."""
    processed_week = date.fromisoformat(partition_key)
    destination = target_path or esef_partition_duckdb_path(
        storage_source=projection.storage_source,
        partition_key=partition_key,
    )

    def build(database_path: Path) -> dict[str, object]:
        with local_esef_document_result(
            object_store,
            partition_key=partition_key,
        ) as result_path:
            result_metadata = esef_document_result_metadata(result_path)
            expected_row_count = int(
                result_metadata[projection.expected_row_count_property]
            )
            source_document_count = sum(
                1
                for _row in iter_esef_document_result_rows(
                    result_path,
                    property_name="document_rows",
                )
            )
            rows = (
                {**row, "processed_week": processed_week}
                for row in iter_esef_document_result_rows(
                    result_path,
                    property_name=projection.property_name,
                )
            )
            actual_row_count = write_rows(
                database_path=database_path,
                table=projection.table,
                columns=projection.columns,
                integer_columns=projection.integer_columns,
                boolean_columns=projection.boolean_columns,
                rows=rows,
            )
        write_partition_status(
            database_path=database_path,
            dataset_name=projection.dataset_name,
            processed_week=processed_week,
            source_document_count=source_document_count,
            expected_row_count=expected_row_count,
            actual_row_count=actual_row_count,
        )
        return {
            "dataset_name": projection.dataset_name,
            "partition_key": partition_key,
            "source_document_count": source_document_count,
            "expected_row_count": expected_row_count,
            "row_count": actual_row_count,
            "table": projection.qualified_table,
        }

    metadata = atomic_partition_database(destination, build)
    return {**metadata, "duckdb_path": str(destination)}


def write_rows(
    *,
    database_path: Path,
    table: str,
    columns: Sequence[str],
    integer_columns: frozenset[str],
    boolean_columns: frozenset[str],
    rows: Iterable[Mapping[str, object]],
) -> int:
    """Bulk-load mapping rows through Arrow without Python executemany."""
    schema = _arrow_schema(
        columns,
        integer_columns=integer_columns,
        boolean_columns=boolean_columns,
    )
    column_sql = ", ".join(
        f'"{column}" {_duckdb_type(column, integer_columns, boolean_columns)}'
        for column in columns
    )
    row_count = 0
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"create table {tables.DLT_DATASET_NAME}.{table} ({column_sql})"
        )
        batch: list[Mapping[str, object]] = []
        for row in rows:
            _require_columns(row, columns)
            batch.append(row)
            if len(batch) >= ROW_BATCH_SIZE:
                _insert_arrow_batch(
                    connection,
                    qualified_table=f"{tables.DLT_DATASET_NAME}.{table}",
                    columns=columns,
                    schema=schema,
                    rows=batch,
                )
                row_count += len(batch)
                batch.clear()
        if batch:
            _insert_arrow_batch(
                connection,
                qualified_table=f"{tables.DLT_DATASET_NAME}.{table}",
                columns=columns,
                schema=schema,
                rows=batch,
            )
            row_count += len(batch)
    return row_count


def write_partition_status(
    *,
    database_path: Path,
    dataset_name: str,
    processed_week: date,
    source_document_count: int,
    expected_row_count: int,
    actual_row_count: int,
) -> None:
    if expected_row_count != actual_row_count:
        raise ValueError(
            f"ESEF {dataset_name} partition is incomplete: "
            f"expected={expected_row_count} actual={actual_row_count}"
        )
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"create table {QUALIFIED_PARTITION_STATUS_TABLE} ("
            "dataset_name varchar, processed_week date, "
            "source_document_count ubigint, expected_row_count ubigint, "
            "actual_row_count ubigint, completed_at timestamptz)"
        )
        connection.execute(
            f"insert into {QUALIFIED_PARTITION_STATUS_TABLE} values (?, ?, ?, ?, ?, ?)",
            [
                dataset_name,
                processed_week,
                source_document_count,
                expected_row_count,
                actual_row_count,
                datetime.now(UTC),
            ],
        )


def require_completed_partition(
    connection: Any,
    *,
    dataset_name: str,
    table: str,
    partition_key: str,
) -> int:
    """Refuse to publish a missing, incomplete, or cross-partition database."""
    processed_week = date.fromisoformat(partition_key)
    status = connection.execute(
        f"select source_document_count, expected_row_count, actual_row_count "
        f"from {QUALIFIED_PARTITION_STATUS_TABLE} "
        "where dataset_name = ? and processed_week = ?",
        [dataset_name, processed_week],
    ).fetchall()
    if len(status) != 1:
        raise ValueError(
            f"ESEF {dataset_name} DuckDB has no single completed status row for "
            f"processed_week={partition_key}"
        )
    _source_document_count, expected_row_count, recorded_row_count = status[0]
    [(actual_row_count, partition_count, minimum_week, maximum_week)] = (
        connection.execute(
            f"select count(*), count(distinct processed_week), "
            f"min(processed_week), max(processed_week) "
            f"from {tables.DLT_DATASET_NAME}.{table}"
        ).fetchall()
    )
    if int(expected_row_count) != int(recorded_row_count):
        raise ValueError(f"ESEF {dataset_name} status row is internally inconsistent")
    if int(actual_row_count) != int(recorded_row_count):
        raise ValueError(
            f"ESEF {dataset_name} row count changed after completion: "
            f"status={recorded_row_count} actual={actual_row_count}"
        )
    if int(actual_row_count) > 0 and (
        int(partition_count) != 1
        or minimum_week != processed_week
        or maximum_week != processed_week
    ):
        raise ValueError(
            f"ESEF {dataset_name} DuckDB contains rows outside "
            f"processed_week={partition_key}"
        )
    return int(actual_row_count)


def atomic_partition_database(
    target_path: Path,
    build: Callable[[Path], dict[str, object]],
) -> dict[str, object]:
    """Build beside the target and replace only after all validation succeeds."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="esef_partition_",
        dir=target_path.parent,
    ) as temp_name:
        temporary_path = Path(temp_name) / "data.duckdb"
        metadata = build(temporary_path)
        temporary_path.replace(target_path)
    return metadata


def _arrow_schema(
    columns: Sequence[str],
    *,
    integer_columns: frozenset[str],
    boolean_columns: frozenset[str],
) -> pa.Schema:
    fields = []
    for column in columns:
        if column == "processed_week":
            data_type = pa.date32()
        elif column in integer_columns:
            data_type = pa.int64()
        elif column in boolean_columns:
            data_type = pa.bool_()
        else:
            data_type = pa.string()
        fields.append(pa.field(column, data_type))
    return pa.schema(fields)


def _duckdb_type(
    column: str,
    integer_columns: frozenset[str],
    boolean_columns: frozenset[str],
) -> str:
    if column == "processed_week":
        return "date"
    if column in integer_columns:
        return "bigint"
    if column in boolean_columns:
        return "boolean"
    return "varchar"


def _insert_arrow_batch(
    connection: Any,
    *,
    qualified_table: str,
    columns: Sequence[str],
    schema: pa.Schema,
    rows: Sequence[Mapping[str, object]],
) -> None:
    arrow_table = pa.Table.from_pylist(list(rows), schema=schema)
    relation_name = "_esef_partition_batch"
    connection.register(relation_name, arrow_table)
    try:
        column_list = ", ".join(f'"{column}"' for column in columns)
        connection.execute(
            f"insert into {qualified_table} ({column_list}) "
            f"select {column_list} from {relation_name}"
        )
    finally:
        connection.unregister(relation_name)


def _require_columns(row: Mapping[str, object], columns: Sequence[str]) -> None:
    missing = [column for column in columns if column not in row]
    if missing:
        raise ValueError(
            "ESEF partition row is missing required columns: " + ", ".join(missing)
        )
