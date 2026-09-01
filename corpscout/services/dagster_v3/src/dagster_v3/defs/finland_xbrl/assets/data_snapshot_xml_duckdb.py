import json
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import polars as pl

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.assets.common import (
    FINLAND_XBRL_DUCKDB_POOL,
    XBRL_BUCKET,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml import (
    XML_SNAPSHOT_PARTITIONS,
    data_snapshot_xml,
    xml_snapshot_manifest_key,
    xml_snapshot_partition_prefix,
    xml_snapshot_success_key,
)
from dagster_v3.defs.finland_xbrl.parser import ParsedStatement, parse_statement_xml
from dagster_v3.defs.finland_xbrl.unified_adapter import (
    FINLAND_UNIFIED_CONTRACT,
    parse_statement_xml_unified,
)
from dagster_v3.defs.xbrl_common.tables import TableContract, XbrlRowContract

StatementParser = Callable[..., ParsedStatement]

FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH = Path(
    "data/finland_xbrl/duckdb/xml_snapshot_parse"
)
FINLAND_XBRL_XML_SNAPSHOT_PARSE_TEMP_PATH = Path(
    "data/finland_xbrl/tmp/xml_snapshot_parse"
)
FINLAND_XBRL_XML_DAILY_PARSE_DUCKDB_PATH = Path(
    "data/finland_xbrl/duckdb/xml_daily_parse"
)
FINLAND_XBRL_XML_DAILY_PARSE_TEMP_PATH = Path("data/finland_xbrl/tmp/xml_daily_parse")
REQUIRED_XML_SNAPSHOT_MANIFEST_FIELDS = (
    "business_id",
    "financial_date",
    "registration_date",
    "source_url",
    "xml_object_key",
)


@dataclass(frozen=True)
class ParsedXmlDuckdbRows:
    statement_documents: list[dict[str, Any]]
    contexts: list[dict[str, Any]]
    units: list[dict[str, Any]]
    facts: list[dict[str, Any]]
    duckdb_path_count: int

    @property
    def statement_documents_count(self) -> int:
        return len(self.statement_documents)

    @property
    def facts_count(self) -> int:
        return len(self.facts)

    @property
    def contexts_count(self) -> int:
        return len(self.contexts)

    @property
    def units_count(self) -> int:
        return len(self.units)


@dataclass(frozen=True)
class FinlandXbrlParseRun:
    statement_rows: list[dict[str, Any]]
    context_rows: list[dict[str, Any]]
    unit_rows: list[dict[str, Any]]
    fact_rows: list[dict[str, Any]]
    failed_rows: list[dict[str, str]]


def run_finland_xbrl_parse(
    *,
    object_store: ObjectStoreResource,
    documents: list[dict[str, Any]],
    run_id: str,
    parsed_at: datetime | None = None,
    parser: StatementParser = parse_statement_xml,
) -> FinlandXbrlParseRun:
    statement_rows: list[dict[str, Any]] = []
    context_rows: list[dict[str, Any]] = []
    unit_rows: list[dict[str, Any]] = []
    fact_rows: list[dict[str, Any]] = []
    failed_rows: list[dict[str, str]] = []
    parse_time = parsed_at or datetime.now(UTC)
    for document in documents:
        try:
            body = object_store.read_bytes(
                str(document["xml_object_key"]),
                bucket=XBRL_BUCKET,
            )
            parsed = parser(
                business_id=str(document["business_id"]),
                financial_date=str(document["financial_date"]),
                registration_date=str(document.get("registration_date") or ""),
                source_url=str(document.get("source_url") or ""),
                xml_object_key=str(document["xml_object_key"]),
                source_run_id=run_id,
                body=body,
                parsed_at=parse_time,
            )
        except Exception as exc:  # noqa: BLE001 - caller decides whether failures are fatal
            failed_rows.append(
                {
                    "business_id": str(document.get("business_id") or ""),
                    "financial_date": str(document.get("financial_date") or ""),
                    "xml_object_key": str(document.get("xml_object_key") or ""),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue
        statement_rows.extend(parsed.rows_by_table[tables.STATEMENT_DOCUMENTS_TABLE])
        context_rows.extend(parsed.rows_by_table[tables.CONTEXTS_TABLE])
        unit_rows.extend(parsed.rows_by_table[tables.UNITS_TABLE])
        fact_rows.extend(parsed.rows_by_table[tables.FACTS_TABLE])
    return FinlandXbrlParseRun(
        statement_rows=statement_rows,
        context_rows=context_rows,
        unit_rows=unit_rows,
        fact_rows=fact_rows,
        failed_rows=failed_rows,
    )


def xml_snapshot_parse_duckdb_path(partition_key: str) -> Path:
    return (
        FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH
        / f"partition_key={partition_key}"
        / "data.duckdb"
    )


def xml_snapshot_parse_temp_dir(partition_key: str) -> Path:
    return FINLAND_XBRL_XML_SNAPSHOT_PARSE_TEMP_PATH / f"partition_key={partition_key}"


def xml_daily_parse_duckdb_path(partition_key: str) -> Path:
    return (
        FINLAND_XBRL_XML_DAILY_PARSE_DUCKDB_PATH
        / f"partition_key={partition_key}"
        / "data.duckdb"
    )


def xml_daily_parse_temp_dir(partition_key: str) -> Path:
    return FINLAND_XBRL_XML_DAILY_PARSE_TEMP_PATH / f"partition_key={partition_key}"


def list_xml_parse_duckdb_paths(
    *,
    snapshot_base_path: Path = FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH,
    daily_base_path: Path = FINLAND_XBRL_XML_DAILY_PARSE_DUCKDB_PATH,
) -> list[Path]:
    return [
        *sorted(snapshot_base_path.glob("partition_key=*/data.duckdb")),
        *sorted(daily_base_path.glob("partition_key=*/data.duckdb")),
    ]


FINLAND_XBRL_XML_SNAPSHOT_UNIFIED_DUCKDB_PATH = Path(
    "data/finland_xbrl/xml_snapshot_unified_duckdb"
)
FINLAND_XBRL_XML_SNAPSHOT_UNIFIED_PARSE_TEMP_PATH = Path(
    "data/finland_xbrl/xml_snapshot_unified_parse_tmp"
)
FINLAND_XBRL_XML_DAILY_UNIFIED_DUCKDB_PATH = Path(
    "data/finland_xbrl/xml_daily_unified_duckdb"
)
FINLAND_XBRL_XML_DAILY_UNIFIED_PARSE_TEMP_PATH = Path(
    "data/finland_xbrl/xml_daily_unified_parse_tmp"
)


def xml_snapshot_unified_duckdb_path(partition_key: str) -> Path:
    return (
        FINLAND_XBRL_XML_SNAPSHOT_UNIFIED_DUCKDB_PATH
        / f"partition_key={partition_key}"
        / "data.duckdb"
    )


def xml_snapshot_unified_parse_temp_dir(partition_key: str) -> Path:
    return (
        FINLAND_XBRL_XML_SNAPSHOT_UNIFIED_PARSE_TEMP_PATH
        / f"partition_key={partition_key}"
    )


def xml_daily_unified_duckdb_path(partition_key: str) -> Path:
    return (
        FINLAND_XBRL_XML_DAILY_UNIFIED_DUCKDB_PATH
        / f"partition_key={partition_key}"
        / "data.duckdb"
    )


def xml_daily_unified_parse_temp_dir(partition_key: str) -> Path:
    return (
        FINLAND_XBRL_XML_DAILY_UNIFIED_PARSE_TEMP_PATH
        / f"partition_key={partition_key}"
    )


def list_xml_unified_duckdb_paths(
    *,
    snapshot_base_path: Path = FINLAND_XBRL_XML_SNAPSHOT_UNIFIED_DUCKDB_PATH,
    daily_base_path: Path = FINLAND_XBRL_XML_DAILY_UNIFIED_DUCKDB_PATH,
) -> list[Path]:
    return [
        *sorted(snapshot_base_path.glob("partition_key=*/data.duckdb")),
        *sorted(daily_base_path.glob("partition_key=*/data.duckdb")),
    ]


def read_xml_parse_duckdb_rows(*, duckdb_paths: list[Path]) -> ParsedXmlDuckdbRows:
    statement_documents: list[dict[str, Any]] = []
    contexts: list[dict[str, Any]] = []
    units: list[dict[str, Any]] = []
    facts: list[dict[str, Any]] = []
    for path in duckdb_paths:
        if not path.exists():
            raise FileNotFoundError(f"Finland XBRL parsed DuckDB is missing: {path}")
        statement_documents.extend(
            _read_duckdb_table_rows(
                path=path,
                table_name="statement_documents",
                columns=tables.STATEMENT_DOCUMENTS_COLUMNS,
            )
        )
        contexts.extend(
            _read_optional_duckdb_table_rows(
                path=path,
                table_name="contexts",
                columns=tables.CONTEXTS_COLUMNS,
            )
        )
        units.extend(
            _read_optional_duckdb_table_rows(
                path=path,
                table_name="units",
                columns=tables.UNITS_COLUMNS,
            )
        )
        facts.extend(
            _read_duckdb_table_rows(
                path=path,
                table_name="facts",
                columns=tables.FACTS_COLUMNS,
            )
        )
    return ParsedXmlDuckdbRows(
        statement_documents=statement_documents,
        contexts=contexts,
        units=units,
        facts=facts,
        duckdb_path_count=len(duckdb_paths),
    )


def _read_duckdb_table_rows(
    *,
    path: Path,
    table_name: str,
    columns: list[str],
) -> list[dict[str, Any]]:
    column_sql = ", ".join(f'"{column}"' for column in columns)
    with duckdb.connect(str(path), read_only=True) as connection:
        rows = connection.execute(f"select {column_sql} from {table_name}").fetchall()
    return [
        {column: value for column, value in zip(columns, row, strict=True)}
        for row in rows
    ]


def _read_optional_duckdb_table_rows(
    *,
    path: Path,
    table_name: str,
    columns: list[str],
) -> list[dict[str, Any]]:
    with duckdb.connect(str(path), read_only=True) as connection:
        exists = connection.execute(
            "select count(*) from information_schema.tables where table_name = ?",
            [table_name],
        ).fetchone()[0]
    if not exists:
        return []
    return _read_duckdb_table_rows(path=path, table_name=table_name, columns=columns)


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


def _projected_row(row: dict[str, Any], *, columns: list[str]) -> dict[str, Any]:
    return {column: row.get(column) for column in columns}


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
    row_contract: XbrlRowContract | None = None,
) -> dg.MaterializeResult:
    documents_contract = (
        row_contract.documents
        if row_contract
        else TableContract(
            columns=tables.STATEMENT_DOCUMENTS_COLUMNS,
            schema=tables.STATEMENT_DOCUMENTS_POLARS_SCHEMA,
        )
    )
    contexts_contract = (
        row_contract.contexts
        if row_contract
        else TableContract(
            columns=tables.CONTEXTS_COLUMNS, schema=tables.CONTEXTS_POLARS_SCHEMA
        )
    )
    units_contract = (
        row_contract.units
        if row_contract
        else TableContract(columns=tables.UNITS_COLUMNS, schema=tables.UNITS_POLARS_SCHEMA)
    )
    facts_contract = (
        row_contract.facts
        if row_contract
        else TableContract(columns=tables.FACTS_COLUMNS, schema=tables.FACTS_POLARS_SCHEMA)
    )
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
    contexts_dir = temp_dir / "contexts"
    units_dir = temp_dir / "units"
    facts_dir = temp_dir / "facts"
    statement_parquet_count = 0
    contexts_parquet_count = 0
    units_parquet_count = 0
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
            _projected_row(statement, columns=documents_contract.columns)
            for statement in parsed.rows_by_table[tables.STATEMENT_DOCUMENTS_TABLE]
        ]
        fact_rows = [
            _projected_row(fact, columns=facts_contract.columns)
            for fact in parsed.rows_by_table[tables.FACTS_TABLE]
        ]
        context_rows = [
            _projected_row(item, columns=contexts_contract.columns)
            for item in parsed.rows_by_table.get(tables.CONTEXTS_TABLE, [])
        ]
        unit_rows = [
            _projected_row(item, columns=units_contract.columns)
            for item in parsed.rows_by_table.get(tables.UNITS_TABLE, [])
        ]
        if statement_rows:
            statement_parquet_count += 1
            _write_partition_parquet(
                path=statement_dir / f"part-{index:06d}.parquet",
                rows=statement_rows,
                columns=documents_contract.columns,
                schema=documents_contract.schema,
            )
        if fact_rows:
            facts_parquet_count += 1
            _write_partition_parquet(
                path=facts_dir / f"part-{index:06d}.parquet",
                rows=fact_rows,
                columns=facts_contract.columns,
                schema=facts_contract.schema,
            )
        if context_rows:
            contexts_parquet_count += 1
            _write_partition_parquet(
                path=contexts_dir / f"part-{index:06d}.parquet",
                rows=context_rows,
                columns=contexts_contract.columns,
                schema=contexts_contract.schema,
            )
        if unit_rows:
            units_parquet_count += 1
            _write_partition_parquet(
                path=units_dir / f"part-{index:06d}.parquet",
                rows=unit_rows,
                columns=units_contract.columns,
                schema=units_contract.schema,
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

    if failed_rows:
        first_failure = failed_rows[0]
        raise ValueError(
            "Finland XBRL partition is incomplete: "
            f"partition={partition_key} failed={len(failed_rows)} "
            f"first_business_id={first_failure['business_id']} "
            f"first_error={first_failure['error']}"
        )

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(duckdb_path)) as connection:
        statement_count = _create_duckdb_table_from_parquet(
            connection=connection,
            table_name="statement_documents",
            parquet_dir=statement_dir,
            columns=documents_contract.columns,
            schema=documents_contract.schema,
        )
        contexts_count = _create_duckdb_table_from_parquet(
            connection=connection,
            table_name="contexts",
            parquet_dir=contexts_dir,
            columns=contexts_contract.columns,
            schema=contexts_contract.schema,
        )
        units_count = _create_duckdb_table_from_parquet(
            connection=connection,
            table_name="units",
            parquet_dir=units_dir,
            columns=units_contract.columns,
            schema=units_contract.schema,
        )
        facts_count = _create_duckdb_table_from_parquet(
            connection=connection,
            table_name="facts",
            parquet_dir=facts_dir,
            columns=facts_contract.columns,
            schema=facts_contract.schema,
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
            "contexts_row_count": contexts_count,
            "units_row_count": units_count,
            "facts_row_count": facts_count,
            "temporary_statement_parquet_count": statement_parquet_count,
            "temporary_facts_parquet_count": facts_parquet_count,
            "temporary_contexts_parquet_count": contexts_parquet_count,
            "temporary_units_parquet_count": units_parquet_count,
            "temporary_directory_removed": temporary_directory_removed,
        }
    )


@dg.asset(
    name="data_snapshot_xml_duckdb",
    group_name="finland_xbrl",
    pool=FINLAND_XBRL_DUCKDB_POOL,
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


@dg.asset(
    name="data_snapshot_xml_unified_duckdb",
    group_name="finland_xbrl",
    pool=FINLAND_XBRL_DUCKDB_POOL,
    deps=[data_snapshot_xml],
    partitions_def=XML_SNAPSHOT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"python", "duckdb", "parquet", "xml"},
    description=(
        "Parses monthly historical Finland XBRL XML snapshot files from S3 "
        "into a partition-scoped DuckDB database using the unified XBRL row "
        "contract."
    ),
)
def data_snapshot_xml_unified_duckdb(
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
        duckdb_path=xml_snapshot_unified_duckdb_path(context.partition_key),
        temp_dir=xml_snapshot_unified_parse_temp_dir(context.partition_key),
        run_id=context.run.run_id,
        log_info=context.log.info,
        parser=parse_statement_xml_unified,
        row_contract=FINLAND_UNIFIED_CONTRACT,
    )
