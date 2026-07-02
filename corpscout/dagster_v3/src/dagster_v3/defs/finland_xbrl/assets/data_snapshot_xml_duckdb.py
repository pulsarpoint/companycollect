import json
import shutil
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import polars as pl

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.assets.common import XBRL_BUCKET
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml import (
    XML_SNAPSHOT_PARTITIONS,
    data_snapshot_xml,
    xml_snapshot_manifest_key,
    xml_snapshot_partition_prefix,
    xml_snapshot_success_key,
)
from dagster_v3.defs.finland_xbrl.assets.parse import StatementParser
from dagster_v3.defs.finland_xbrl.parser import parse_statement_xml

FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH = Path(
    "data/finland_xbrl/duckdb/xml_snapshot_parse"
)
FINLAND_XBRL_XML_SNAPSHOT_PARSE_TEMP_PATH = Path(
    "data/finland_xbrl/tmp/xml_snapshot_parse"
)
REQUIRED_XML_SNAPSHOT_MANIFEST_FIELDS = (
    "business_id",
    "financial_date",
    "registration_date",
    "source_url",
    "xml_object_key",
)


def xml_snapshot_parse_duckdb_path(partition_key: str) -> Path:
    return (
        FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH
        / f"partition_key={partition_key}"
        / "data.duckdb"
    )


def xml_snapshot_parse_temp_dir(partition_key: str) -> Path:
    return FINLAND_XBRL_XML_SNAPSHOT_PARSE_TEMP_PATH / f"partition_key={partition_key}"


def read_xml_snapshot_manifest_rows(
    *,
    object_store: ObjectStoreResource,
    manifest_key: str,
) -> list[dict[str, str]]:
    body = object_store.read_bytes(manifest_key, bucket=XBRL_BUCKET)
    rows: list[dict[str, str]] = []
    for line_number, raw_line in enumerate(body.decode("utf-8").splitlines(), start=1):
        if not raw_line.strip():
            continue
        payload = json.loads(raw_line)
        if not isinstance(payload, dict):
            raise ValueError(f"Manifest line {line_number} is not an object: {manifest_key}")
        missing = [
            field
            for field in REQUIRED_XML_SNAPSHOT_MANIFEST_FIELDS
            if not str(payload.get(field) or "").strip()
        ]
        if missing:
            raise ValueError(
                f"Manifest line {line_number} is missing required fields {missing}: "
                f"{manifest_key}"
            )
        rows.append(
            {
                field: str(payload[field]).strip()
                for field in REQUIRED_XML_SNAPSHOT_MANIFEST_FIELDS
            }
        )
    return rows


def _statement_document_row(row: dict[str, Any]) -> dict[str, Any]:
    return {column: row.get(column) for column in tables.STATEMENT_DOCUMENTS_COLUMNS}


def _fact_row(row: dict[str, Any]) -> dict[str, Any]:
    return {column: row.get(column) for column in tables.FACTS_COLUMNS}


def _write_partition_parquet(
    *,
    path: Path,
    rows: list[dict[str, Any]],
    columns: list[str],
    schema: dict[str, pl.DataType],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [{column: row.get(column) for column in columns} for row in rows],
        schema=schema,
    ).write_parquet(path)


def _create_duckdb_table_from_parquet(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    parquet_dir: Path,
    columns: list[str],
    schema: dict[str, pl.DataType],
) -> int:
    parquet_files = sorted(parquet_dir.glob("*.parquet"))
    if parquet_files:
        files = [str(path) for path in parquet_files]
        connection.execute(
            f"create or replace table {table_name} as select * from read_parquet(?)",
            [files],
        )
    else:
        empty_path = parquet_dir / "_empty.parquet"
        _write_partition_parquet(
            path=empty_path,
            rows=[],
            columns=columns,
            schema=schema,
        )
        connection.execute(
            f"create or replace table {table_name} as select * from read_parquet(?)",
            [[str(empty_path)]],
        )
        empty_path.unlink()
    return connection.execute(f"select count(*) from {table_name}").fetchone()[0]


def materialize_data_snapshot_xml_duckdb(
    *,
    partition_key: str,
    registered_date_start: str,
    registered_date_end: str,
    object_store: ObjectStoreResource,
    duckdb_path: Path,
    temp_dir: Path,
    run_id: str,
    log_info: Callable[[str], None] | None = None,
    parser: StatementParser = parse_statement_xml,
) -> dg.MaterializeResult:
    prefix = xml_snapshot_partition_prefix(registered_date_start, registered_date_end)
    manifest_key = xml_snapshot_manifest_key(registered_date_start, registered_date_end)
    success_key = xml_snapshot_success_key(registered_date_start, registered_date_end)
    if not object_store.exists(success_key, bucket=XBRL_BUCKET):
        raise FileNotFoundError(
            "Finland XBRL XML snapshot success marker is missing: "
            f"bucket={XBRL_BUCKET} key={success_key}"
        )
    if not object_store.exists(manifest_key, bucket=XBRL_BUCKET):
        raise FileNotFoundError(
            "Finland XBRL XML snapshot manifest is missing: "
            f"bucket={XBRL_BUCKET} key={manifest_key}"
        )
    rows = read_xml_snapshot_manifest_rows(
        object_store=object_store,
        manifest_key=manifest_key,
    )
    if log_info is not None:
        log_info(
            "Finland XBRL XML snapshot parse manifest loaded: "
            f"partition={partition_key} rows={len(rows)} manifest_key={manifest_key}"
        )
    parsed_at = datetime.now(UTC)
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    statement_dir = temp_dir / "statement_documents"
    facts_dir = temp_dir / "facts"
    statement_parquet_count = 0
    facts_parquet_count = 0
    documents_parsed = 0
    failed_rows: list[dict[str, str]] = []

    for index, row in enumerate(rows, start=1):
        body = object_store.read_bytes(row["xml_object_key"], bucket=XBRL_BUCKET)
        try:
            parsed = parser(
                business_id=row["business_id"],
                financial_date=row["financial_date"],
                registration_date=row["registration_date"],
                source_url=row["source_url"],
                xml_object_key=row["xml_object_key"],
                source_run_id=run_id,
                body=body,
                parsed_at=parsed_at,
            )
        except Exception as exc:  # noqa: BLE001 - record bad XML and continue
            failed_rows.append(
                {
                    "business_id": row["business_id"],
                    "financial_date": row["financial_date"],
                    "xml_object_key": row["xml_object_key"],
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            if log_info is not None:
                log_info(
                    "Finland XBRL XML snapshot parse failed: "
                    f"partition={partition_key} business_id={row['business_id']} "
                    f"financial_date={row['financial_date']} "
                    f"xml_key={row['xml_object_key']} "
                    f"error={type(exc).__name__}: {exc}"
                )
            continue

        statement_rows = [
            _statement_document_row(statement)
            for statement in parsed.rows_by_table[tables.STATEMENT_DOCUMENTS_TABLE]
        ]
        fact_rows = [
            _fact_row(fact)
            for fact in parsed.rows_by_table[tables.FACTS_TABLE]
        ]
        if statement_rows:
            statement_parquet_count += 1
            _write_partition_parquet(
                path=statement_dir / f"part-{index:06d}.parquet",
                rows=statement_rows,
                columns=tables.STATEMENT_DOCUMENTS_COLUMNS,
                schema=tables.STATEMENT_DOCUMENTS_POLARS_SCHEMA,
            )
        if fact_rows:
            facts_parquet_count += 1
            _write_partition_parquet(
                path=facts_dir / f"part-{index:06d}.parquet",
                rows=fact_rows,
                columns=tables.FACTS_COLUMNS,
                schema=tables.FACTS_POLARS_SCHEMA,
            )
        documents_parsed += 1
        if log_info is not None and (
            index == 1 or index == len(rows) or index % 25 == 0
        ):
            log_info(
                "Finland XBRL XML snapshot parse progress: "
                f"partition={partition_key} {index}/{len(rows)} "
                f"business_id={row['business_id']} financial_date={row['financial_date']}"
            )

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(duckdb_path)) as connection:
        statement_count = _create_duckdb_table_from_parquet(
            connection=connection,
            table_name="statement_documents",
            parquet_dir=statement_dir,
            columns=tables.STATEMENT_DOCUMENTS_COLUMNS,
            schema=tables.STATEMENT_DOCUMENTS_POLARS_SCHEMA,
        )
        facts_count = _create_duckdb_table_from_parquet(
            connection=connection,
            table_name="facts",
            parquet_dir=facts_dir,
            columns=tables.FACTS_COLUMNS,
            schema=tables.FACTS_POLARS_SCHEMA,
        )

    temporary_directory_removed = False
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
        temporary_directory_removed = True

    return dg.MaterializeResult(
        metadata={
            "partition": partition_key,
            "registered_date_start": registered_date_start,
            "registered_date_end": registered_date_end,
            "s3_bucket": XBRL_BUCKET,
            "s3_prefix": prefix,
            "manifest_key": manifest_key,
            "success_key": success_key,
            "duckdb_path": str(duckdb_path),
            "documents_in_manifest": len(rows),
            "documents_parsed_this_run": documents_parsed,
            "documents_failed_this_run": len(failed_rows),
            "statement_documents_row_count": statement_count,
            "facts_row_count": facts_count,
            "temporary_statement_parquet_count": statement_parquet_count,
            "temporary_facts_parquet_count": facts_parquet_count,
            "temporary_directory_removed": temporary_directory_removed,
        }
    )


@dg.asset(
    name="data_snapshot_xml_duckdb",
    group_name="finland_xbrl",
    deps=[data_snapshot_xml],
    partitions_def=XML_SNAPSHOT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"python", "duckdb", "parquet", "xml"},
    description=(
        "Parses monthly historical Finland XBRL XML snapshot files from S3 "
        "into a partition-scoped DuckDB database."
    ),
)
def data_snapshot_xml_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    window = context.partition_time_window
    start = window.start.date().isoformat()
    end = (window.end.date() - timedelta(days=1)).isoformat()
    return materialize_data_snapshot_xml_duckdb(
        partition_key=context.partition_key,
        registered_date_start=start,
        registered_date_end=end,
        object_store=object_store,
        duckdb_path=xml_snapshot_parse_duckdb_path(context.partition_key),
        temp_dir=xml_snapshot_parse_temp_dir(context.partition_key),
        run_id=context.run.run_id,
        log_info=context.log.info,
    )
