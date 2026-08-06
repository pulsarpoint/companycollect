"""Processed-week persistence for deterministic ESEF narrative disclosures.

The three stages deliberately release the shared DuckDB pool while XHTML is
parsed. A compact Parquet snapshot is read under the DuckDB pool, parsing runs
in bounded worker processes from S3, and publication takes one short write lock.

No ``from __future__ import annotations``: Dagster inspects asset annotations.
"""

import json
import tempfile
from collections.abc import Mapping
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_duckdb import DuckDBResource
from pydantic import Field

from dagster_v3.defs.common.duckdb_resources import safe_duckdb_connection
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.disclosure_parser import (
    DISCLOSURE_PARSER_NAME,
    DISCLOSURE_PARSER_VERSION,
    disclosure_row,
    ensure_disclosure_table,
)
from dagster_v3.defs.esef_filings.segment_assets import (
    ESEF_DOCUMENT_BUCKET,
    ESEF_PROCESSED_WEEK_PARTITIONS,
    processed_week_bounds,
)


GROUP_NAME = "esef_filings"
ESEF_DISCLOSURE_PARSE_POOL = "esef_disclosure_parse"
ESEF_DISCLOSURE_INPUT_PREFIX = "esef_filings/fact_disclosure_inputs/"
ESEF_DISCLOSURE_RESULT_PREFIX = "esef_filings/fact_disclosure_results/"
_DISCLOSURE_STAGE_SCHEMA_VERSION = 1
_DEFAULT_PARSE_WORKERS = 4
_MAX_PARSE_WORKERS = 8
_PARQUET_BATCH_SIZE = 100

_DISCLOSURE_ARROW_SCHEMA = pa.schema(
    [
        pa.field(
            column,
            pa.int32()
            if column in {"fiscal_year", "block_count", "table_count"}
            else pa.string(),
        )
        for column in tables.ESEF_FACT_DISCLOSURES_EXPORT_COLUMNS
    ]
)


class EsefDisclosureParseConfig(dg.Config):
    parse_workers: int = Field(
        default=_DEFAULT_PARSE_WORKERS,
        ge=1,
        le=_MAX_PARSE_WORKERS,
    )


def disclosure_input_object_key(partition_key: str) -> str:
    processed_week_bounds(partition_key)
    return f"{ESEF_DISCLOSURE_INPUT_PREFIX}processed_week={partition_key}/facts.parquet"


def disclosure_input_manifest_object_key(partition_key: str) -> str:
    processed_week_bounds(partition_key)
    return f"{ESEF_DISCLOSURE_INPUT_PREFIX}processed_week={partition_key}/manifest.json"


def disclosure_result_object_key(partition_key: str) -> str:
    processed_week_bounds(partition_key)
    return (
        f"{ESEF_DISCLOSURE_RESULT_PREFIX}parser={DISCLOSURE_PARSER_NAME}-"
        f"v{DISCLOSURE_PARSER_VERSION}/processed_week={partition_key}/"
        "disclosures.parquet"
    )


def disclosure_result_manifest_object_key(partition_key: str) -> str:
    processed_week_bounds(partition_key)
    return (
        f"{ESEF_DISCLOSURE_RESULT_PREFIX}parser={DISCLOSURE_PARSER_NAME}-"
        f"v{DISCLOSURE_PARSER_VERSION}/processed_week={partition_key}/result.json"
    )


def snapshot_disclosure_inputs(
    *,
    esef_filings_duckdb: DuckDBResource,
    object_store: Any,
    partition_key: str,
    source_run_id: str,
) -> dict[str, object]:
    """Snapshot one processed week's text facts under a short read lock."""
    processed_from, processed_until = processed_week_bounds(partition_key)
    with tempfile.TemporaryDirectory(prefix="esef_disclosure_input_") as temp_name:
        parquet_path = Path(temp_name) / "facts.parquet"
        with safe_duckdb_connection(esef_filings_duckdb) as connection:
            _require_source_table(connection, tables.ESEF_SOURCE_DOCUMENTS_TABLE)
            _require_source_table(connection, tables.FACTS_TABLE)
            source_document_ids = [
                str(row[0])
                for row in connection.execute(
                    "select source_document_id from "
                    f"{tables.DLT_DATASET_NAME}.{tables.ESEF_SOURCE_DOCUMENTS_TABLE} "
                    "where try_cast(source_processed_at as timestamptz) >= ? "
                    "and try_cast(source_processed_at as timestamptz) < ? "
                    "and extraction_status in ('parsed', 'reused') "
                    "and package_sha256 != '' order by source_document_id",
                    [processed_from, processed_until],
                ).fetchall()
            ]
            query = _disclosure_input_query(processed_from, processed_until)
            connection.execute(
                f"copy ({query}) to {_sql_string(parquet_path)} "
                "(format parquet, compression zstd, row_group_size 1000)"
            )
        input_parquet = pq.ParquetFile(parquet_path)
        input_row_count = input_parquet.metadata.num_rows
        fact_document_ids = set(
            pq.read_table(parquet_path, columns=["source_document_id"])
            .column("source_document_id")
            .to_pylist()
        )
        source_fact_document_count = len(fact_document_ids)
        if source_document_ids and int(source_fact_document_count) == 0:
            raise ValueError(
                "ESEF source documents exist for the processed week but no parsed text "
                "facts are available; materialize esef_filing_facts_duckdb first"
            )
        input_key = disclosure_input_object_key(partition_key)
        object_store.upload_file(
            input_key,
            parquet_path,
            bucket=ESEF_DOCUMENT_BUCKET,
        )
        input_size_bytes = parquet_path.stat().st_size
        manifest = {
            "schema_version": _DISCLOSURE_STAGE_SCHEMA_VERSION,
            "processed_week": partition_key,
            "processed_window_start": processed_from.isoformat(),
            "processed_window_end": processed_until.isoformat(),
            "source_run_id": source_run_id,
            "source_document_ids": source_document_ids,
            "input_object_key": input_key,
            "input_row_count": int(input_row_count),
            "source_fact_document_count": int(source_fact_document_count),
        }
        manifest_key = disclosure_input_manifest_object_key(partition_key)
        object_store.write_bytes(
            manifest_key,
            _json_bytes(manifest),
            bucket=ESEF_DOCUMENT_BUCKET,
        )
    return {
        **manifest,
        "manifest_object_key": manifest_key,
        "input_size_bytes": input_size_bytes,
    }


def parse_disclosure_inputs(
    *,
    object_store: Any,
    partition_key: str,
    source_run_id: str,
    parse_workers: int,
) -> dict[str, object]:
    """Parse the Parquet snapshot outside DuckDB in bounded processes."""
    if parse_workers < 1 or parse_workers > _MAX_PARSE_WORKERS:
        raise ValueError(
            f"ESEF disclosure parse_workers must be between 1 and {_MAX_PARSE_WORKERS}"
        )
    manifest = _read_stage_manifest(
        object_store,
        key=disclosure_input_manifest_object_key(partition_key),
        partition_key=partition_key,
    )
    extracted_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    table_count = 0
    block_count = 0
    output_row_count = 0
    with tempfile.TemporaryDirectory(prefix="esef_disclosure_parse_") as temp_name:
        temp_root = Path(temp_name)
        input_path = temp_root / "facts.parquet"
        output_path = temp_root / "disclosures.parquet"
        object_store.download_file(
            str(manifest["input_object_key"]),
            input_path,
            bucket=ESEF_DOCUMENT_BUCKET,
        )
        parquet_file = pq.ParquetFile(input_path)
        with (
            ProcessPoolExecutor(max_workers=parse_workers) as executor,
            pq.ParquetWriter(
                output_path,
                _DISCLOSURE_ARROW_SCHEMA,
                compression="zstd",
            ) as writer,
        ):
            for batch in parquet_file.iter_batches(batch_size=_PARQUET_BATCH_SIZE):
                sources = batch.to_pylist()
                if parse_workers == 1:
                    parsed_rows = [
                        disclosure_row(
                            source,
                            source_run_id=source_run_id,
                            extracted_at=extracted_at,
                        )
                        for source in sources
                    ]
                else:
                    tasks = [
                        (source, source_run_id, extracted_at) for source in sources
                    ]
                    parsed_rows = list(executor.map(_disclosure_row_worker, tasks))
                writer.write_table(
                    pa.Table.from_pylist(parsed_rows, schema=_DISCLOSURE_ARROW_SCHEMA)
                )
                output_row_count += len(parsed_rows)
                block_count += sum(int(row["block_count"]) for row in parsed_rows)
                table_count += sum(int(row["table_count"]) for row in parsed_rows)
        result_key = disclosure_result_object_key(partition_key)
        object_store.upload_file(
            result_key,
            output_path,
            bucket=ESEF_DOCUMENT_BUCKET,
        )
        result = {
            "schema_version": _DISCLOSURE_STAGE_SCHEMA_VERSION,
            "processed_week": partition_key,
            "source_run_id": source_run_id,
            "extracted_at": extracted_at,
            "source_document_ids": manifest["source_document_ids"],
            "result_object_key": result_key,
            "input_row_count": int(manifest["input_row_count"]),
            "output_row_count": output_row_count,
            "block_count": block_count,
            "table_count": table_count,
            "parse_worker_count": parse_workers,
            "parser_name": DISCLOSURE_PARSER_NAME,
            "parser_version": DISCLOSURE_PARSER_VERSION,
        }
        result_manifest_key = disclosure_result_manifest_object_key(partition_key)
        object_store.write_bytes(
            result_manifest_key,
            _json_bytes(result),
            bucket=ESEF_DOCUMENT_BUCKET,
        )
        output_size_bytes = output_path.stat().st_size
    return {
        **result,
        "result_manifest_object_key": result_manifest_key,
        "output_size_bytes": output_size_bytes,
    }


def publish_disclosure_partition(
    *,
    esef_filings_duckdb: DuckDBResource,
    object_store: Any,
    partition_key: str,
) -> dict[str, object]:
    """Publish one parsed processed week with one short DuckDB transaction."""
    result = _read_stage_manifest(
        object_store,
        key=disclosure_result_manifest_object_key(partition_key),
        partition_key=partition_key,
    )
    source_document_ids = [str(value) for value in result["source_document_ids"]]
    with tempfile.TemporaryDirectory(prefix="esef_disclosure_publish_") as temp_name:
        parquet_path = Path(temp_name) / "disclosures.parquet"
        object_store.download_file(
            str(result["result_object_key"]),
            parquet_path,
            bucket=ESEF_DOCUMENT_BUCKET,
        )
        with safe_duckdb_connection(esef_filings_duckdb) as connection:
            ensure_disclosure_table(connection)
            connection.execute("begin transaction")
            try:
                connection.execute(
                    "create or replace temp table selected_esef_disclosure_documents "
                    "(source_document_id varchar)"
                )
                if source_document_ids:
                    connection.executemany(
                        "insert into selected_esef_disclosure_documents values (?)",
                        [(value,) for value in source_document_ids],
                    )
                    connection.execute(
                        f"delete from {tables.QUALIFIED_FACT_DISCLOSURES_TABLE} "
                        "where source_document_id in (select source_document_id "
                        "from selected_esef_disclosure_documents)"
                    )
                columns = ", ".join(tables.ESEF_FACT_DISCLOSURES_EXPORT_COLUMNS)
                connection.execute(
                    f"insert into {tables.QUALIFIED_FACT_DISCLOSURES_TABLE} "
                    f"({columns}) select {columns} from read_parquet("
                    f"{_sql_string(parquet_path)})"
                )
                connection.execute("commit")
            except Exception:
                connection.execute("rollback")
                raise
    return {
        **result,
        "table": tables.QUALIFIED_FACT_DISCLOSURES_TABLE,
    }


def _disclosure_row_worker(
    task: tuple[Mapping[str, object], str, str],
) -> dict[str, object]:
    source, source_run_id, extracted_at = task
    return disclosure_row(
        source,
        source_run_id=source_run_id,
        extracted_at=extracted_at,
    )


def _disclosure_input_query(processed_from: datetime, processed_until: datetime) -> str:
    return f"""
        select
            documents.source_document_id,
            documents.package_sha256,
            documents.lei,
            documents.country_iso2,
            documents.company_id,
            documents.period_end,
            documents.fiscal_year,
            facts.fact_id,
            facts.concept_qname,
            facts.concept_local_name,
            coalesce(facts.language, '') as language,
            facts.raw_value
        from {tables.DLT_DATASET_NAME}.{tables.ESEF_SOURCE_DOCUMENTS_TABLE} documents
        inner join {tables.QUALIFIED_FACTS_TABLE} facts
            on facts.fxo_id = documents.source_document_id
        where try_cast(documents.source_processed_at as timestamptz)
                  >= try_cast({_sql_string(processed_from.isoformat())} as timestamptz)
          and try_cast(documents.source_processed_at as timestamptz)
                  < try_cast({_sql_string(processed_until.isoformat())} as timestamptz)
          and documents.extraction_status in ('parsed', 'reused')
          and documents.package_sha256 != ''
          and facts.value_kind = 'text'
          and length(trim(coalesce(facts.raw_value, ''))) > 0
        order by documents.source_document_id, facts.fact_id
    """


def _read_stage_manifest(
    object_store: Any,
    *,
    key: str,
    partition_key: str,
) -> Mapping[str, Any]:
    value = json.loads(object_store.read_bytes(key, bucket=ESEF_DOCUMENT_BUCKET))
    if not isinstance(value, Mapping):
        raise ValueError("ESEF disclosure stage manifest must be an object")
    if value.get("schema_version") != _DISCLOSURE_STAGE_SCHEMA_VERSION:
        raise ValueError("ESEF disclosure stage manifest has an unsupported version")
    if value.get("processed_week") != partition_key:
        raise ValueError("ESEF disclosure stage processed week does not match its key")
    source_document_ids = value.get("source_document_ids")
    if not isinstance(source_document_ids, list):
        raise ValueError("ESEF disclosure source_document_ids must be a list")
    return value


def _require_source_table(connection: Any, table: str) -> None:
    exists = bool(
        connection.execute(
            "select count(*) from information_schema.tables "
            "where table_schema = ? and table_name = ?",
            [tables.DLT_DATASET_NAME, table],
        ).fetchone()[0]
    )
    if not exists:
        raise ValueError(
            f"Missing ESEF DuckDB source table: {tables.DLT_DATASET_NAME}.{table}"
        )


def _sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


_DISCLOSURE_INPUT_DEPENDENCIES = [
    dg.AssetDep(dg.AssetKey("esef_filing_facts_duckdb")),
    dg.AssetDep(dg.AssetKey("esef_source_documents_duckdb")),
]


@dg.asset(
    name="esef_fact_disclosure_inputs_s3",
    deps=_DISCLOSURE_INPUT_DEPENDENCIES,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "s3", "ixbrl"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="esef_filings_duckdb",
    description=(
        "Snapshots source-linked narrative ESEF facts for one processed week to "
        "Parquet, releasing DuckDB before XHTML parsing begins."
    ),
)
def esef_fact_disclosure_inputs_s3(
    context: dg.AssetExecutionContext,
    esef_filings_duckdb: DuckDBResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    metadata = snapshot_disclosure_inputs(
        esef_filings_duckdb=esef_filings_duckdb,
        object_store=object_store,
        partition_key=context.partition_key,
        source_run_id=context.run_id,
    )
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name="esef_fact_disclosure_artifacts_s3",
    deps=[dg.AssetKey("esef_fact_disclosure_inputs_s3")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "xhtml"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=ESEF_DISCLOSURE_PARSE_POOL,
    description=(
        "Parses narrative facts into headings, paragraphs, and tables in "
        "bounded worker processes without holding DuckDB."
    ),
)
def esef_fact_disclosure_artifacts_s3(
    context: dg.AssetExecutionContext,
    config: EsefDisclosureParseConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    metadata = parse_disclosure_inputs(
        object_store=object_store,
        partition_key=context.partition_key,
        source_run_id=context.run_id,
        parse_workers=config.parse_workers,
    )
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name="esef_fact_disclosures_duckdb",
    deps=[dg.AssetKey("esef_fact_disclosure_artifacts_s3")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb", "xhtml"},
    metadata={"table": tables.QUALIFIED_FACT_DISCLOSURES_TABLE},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="esef_filings_duckdb",
    description=(
        "Publishes source-preserving structured disclosure blocks for one "
        "processed week with a short transactional DuckDB write."
    ),
)
def esef_fact_disclosures_duckdb(
    context: dg.AssetExecutionContext,
    esef_filings_duckdb: DuckDBResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    metadata = publish_disclosure_partition(
        esef_filings_duckdb=esef_filings_duckdb,
        object_store=object_store,
        partition_key=context.partition_key,
    )
    return dg.MaterializeResult(metadata=metadata)


defs = dg.Definitions(
    assets=[
        esef_fact_disclosure_inputs_s3,
        esef_fact_disclosure_artifacts_s3,
        esef_fact_disclosures_duckdb,
    ]
)
