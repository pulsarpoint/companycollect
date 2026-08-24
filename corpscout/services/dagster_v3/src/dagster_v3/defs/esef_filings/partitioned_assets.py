"""Independent weekly DuckDB outputs for the ESEF parsing v2 shadow path.

No ``from __future__ import annotations``: Dagster inspects asset annotations.
"""

import json
from collections.abc import Mapping
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import Field

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.esef_filings import facts, tables
from dagster_v3.defs.esef_filings.assets import run_esef_artifact_facts_partition
from dagster_v3.defs.esef_filings.disclosure_parser import disclosure_row
from dagster_v3.defs.esef_filings.partitioned_storage import (
    CONCEPT_LABELS_PROJECTION,
    DISCLOSURES_STORAGE,
    FACTS_STORAGE,
    CONTACT_CANDIDATES_PROJECTION,
    SOURCE_DOCUMENTS_PROJECTION,
    atomic_partition_database,
    esef_partition_duckdb_path,
    write_partition_status,
    write_result_projection_partition,
    write_rows,
)
from dagster_v3.defs.esef_filings.segment_assets import (
    ESEF_DOCUMENT_BUCKET,
    ESEF_PROCESSED_WEEK_PARTITIONS,
    iter_esef_document_result_rows,
    local_esef_document_result,
)


GROUP_NAME = "esef_filings"
ARTIFACT_DEPENDENCY = [dg.AssetKey("esef_document_artifacts_s3")]
DEFAULT_DISCLOSURE_PARSE_WORKERS = 4
MAX_DISCLOSURE_PARSE_WORKERS = 8
DISCLOSURE_BATCH_SIZE = 100


class EsefPartitionedDisclosureConfig(dg.Config):
    parse_workers: int = Field(
        default=DEFAULT_DISCLOSURE_PARSE_WORKERS,
        ge=1,
        le=MAX_DISCLOSURE_PARSE_WORKERS,
    )


@dataclass(frozen=True)
class _DisclosureDocumentTask:
    artifact_path: str
    output_path: str
    source_document_id: str
    package_sha256: str
    lei: str
    country_iso2: str
    company_id: str
    period_end: str
    fiscal_year: int
    source_run_id: str
    extracted_at: str
    processed_week: date


@dataclass(frozen=True)
class _DisclosureParseResult:
    output_path: str
    row_count: int
    block_count: int
    table_count: int


_DISCLOSURE_INTEGER_COLUMNS = frozenset(
    {"fiscal_year", "block_count", "table_count"}
)
_DISCLOSURE_ARROW_SCHEMA = pa.schema(
    [
        pa.field(
            column,
            pa.date32()
            if column == "processed_week"
            else pa.int64()
            if column in _DISCLOSURE_INTEGER_COLUMNS
            else pa.string(),
        )
        for column in tables.ESEF_FACT_DISCLOSURES_V2_EXPORT_COLUMNS
    ]
)


def build_facts_partition_database(
    *,
    object_store: Any,
    partition_key: str,
    source_run_id: str,
    log_info: Any,
    log_warning: Any,
    target_path: Path | None = None,
) -> dict[str, object]:
    """Reuse the production artifact fact parser in an isolated weekly file."""
    destination = target_path or esef_partition_duckdb_path(
        storage_source=FACTS_STORAGE,
        partition_key=partition_key,
    )
    processed_week = date.fromisoformat(partition_key)

    def build(database_path: Path) -> dict[str, object]:
        resource = duckdb_resource(database_path)
        metadata = run_esef_artifact_facts_partition(
            esef_filings_duckdb=resource,
            object_store=object_store,
            partition_key=partition_key,
            source_run_id=source_run_id,
            log_info=log_info,
            log_warning=log_warning,
        )
        parse_failed_count = int(metadata["parse_failed_count"])
        missing_artifact_count = int(metadata["skipped_missing_artifact_object"])
        if parse_failed_count or missing_artifact_count:
            raise ValueError(
                "ESEF facts partition is incomplete: "
                f"parse_failed={parse_failed_count} "
                f"missing_artifacts={missing_artifact_count}"
            )
        with duckdb.connect(str(database_path)) as connection:
            connection.execute(
                f"alter table {tables.QUALIFIED_FACTS_TABLE} "
                "add column processed_week date"
            )
            connection.execute(
                f"update {tables.QUALIFIED_FACTS_TABLE} set processed_week = ?",
                [processed_week],
            )
            actual_row_count = int(
                connection.execute(
                    f"select count(*) from {tables.QUALIFIED_FACTS_TABLE}"
                ).fetchone()[0]
            )
        expected_row_count = int(metadata["fact_row_count"])
        write_partition_status(
            database_path=database_path,
            dataset_name="facts",
            processed_week=processed_week,
            source_document_count=int(metadata["filings_in_scope"]),
            expected_row_count=expected_row_count,
            actual_row_count=actual_row_count,
        )
        return {
            **metadata,
            "dataset_name": "facts",
            "row_count": actual_row_count,
            "table": tables.QUALIFIED_FACTS_TABLE,
        }

    metadata = atomic_partition_database(destination, build)
    return {**metadata, "duckdb_path": str(destination)}


def build_disclosures_partition_database(
    *,
    object_store: Any,
    partition_key: str,
    source_run_id: str,
    parse_workers: int,
    target_path: Path | None = None,
) -> dict[str, object]:
    """Parse narrative facts directly from artifacts into one weekly file."""
    if parse_workers < 1 or parse_workers > MAX_DISCLOSURE_PARSE_WORKERS:
        raise ValueError(
            "ESEF disclosure parse_workers must be between 1 and "
            f"{MAX_DISCLOSURE_PARSE_WORKERS}"
        )
    destination = target_path or esef_partition_duckdb_path(
        storage_source=DISCLOSURES_STORAGE,
        partition_key=partition_key,
    )
    processed_week = date.fromisoformat(partition_key)

    def build(database_path: Path) -> dict[str, object]:
        work_directory = database_path.parent / "disclosure_work"
        artifact_directory = work_directory / "artifacts"
        output_directory = work_directory / "output"
        artifact_directory.mkdir(parents=True)
        output_directory.mkdir()
        extracted_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        source_document_count = 0
        tasks: list[_DisclosureDocumentTask] = []
        artifact_paths: dict[str, Path] = {}
        seen_document_ids: set[str] = set()

        with local_esef_document_result(
            object_store,
            partition_key=partition_key,
        ) as result_path:
            for document in iter_esef_document_result_rows(
                result_path,
                property_name="document_rows",
            ):
                source_document_count += 1
                source_document_id = str(document.get("source_document_id", ""))
                if source_document_id == "":
                    raise ValueError("ESEF disclosure document has no source_document_id")
                if source_document_id in seen_document_ids:
                    raise ValueError(
                        "ESEF disclosure result repeats source_document_id="
                        f"{source_document_id}"
                    )
                seen_document_ids.add(source_document_id)
                if str(document.get("extraction_status", "")) not in {
                    "parsed",
                    "reused",
                }:
                    continue
                artifact_key = str(document.get("parsed_artifact_object_key", ""))
                if artifact_key == "":
                    raise ValueError(
                        "Parsed ESEF disclosure document has no artifact object key: "
                        f"source_document_id={source_document_id}"
                    )
                artifact_path = artifact_paths.get(artifact_key)
                if artifact_path is None:
                    if not object_store.exists(
                        artifact_key,
                        bucket=ESEF_DOCUMENT_BUCKET,
                    ):
                        raise ValueError(
                            f"ESEF disclosure artifact is missing: {artifact_key}"
                        )
                    artifact_path = artifact_directory / (
                        sha256(artifact_key.encode()).hexdigest() + ".json"
                    )
                    object_store.download_file(
                        artifact_key,
                        artifact_path,
                        bucket=ESEF_DOCUMENT_BUCKET,
                    )
                    artifact_paths[artifact_key] = artifact_path
                output_path = output_directory / (
                    sha256(source_document_id.encode()).hexdigest() + ".parquet"
                )
                tasks.append(
                    _DisclosureDocumentTask(
                        artifact_path=str(artifact_path),
                        output_path=str(output_path),
                        source_document_id=source_document_id,
                        package_sha256=str(document.get("package_sha256", "")),
                        lei=str(document.get("lei", "")),
                        country_iso2=str(document.get("country_iso2", "")),
                        company_id=str(document.get("company_id", "")),
                        period_end=str(document.get("period_end", "")),
                        fiscal_year=int(document.get("fiscal_year", 0)),
                        source_run_id=source_run_id,
                        extracted_at=extracted_at,
                        processed_week=processed_week,
                    )
                )

        if parse_workers == 1:
            parsed = [_parse_disclosure_document(task) for task in tasks]
        else:
            with ProcessPoolExecutor(max_workers=parse_workers) as executor:
                parsed = list(executor.map(_parse_disclosure_document, tasks))

        write_rows(
            database_path=database_path,
            table=tables.ESEF_FACT_DISCLOSURES_TABLE,
            columns=tables.ESEF_FACT_DISCLOSURES_V2_EXPORT_COLUMNS,
            integer_columns=_DISCLOSURE_INTEGER_COLUMNS,
            boolean_columns=frozenset(),
            rows=(),
        )
        columns = ", ".join(tables.ESEF_FACT_DISCLOSURES_V2_EXPORT_COLUMNS)
        with duckdb.connect(str(database_path)) as connection:
            for result in parsed:
                connection.execute(
                    f"insert into {tables.QUALIFIED_FACT_DISCLOSURES_TABLE} "
                    f"({columns}) select {columns} from read_parquet(?)",
                    [result.output_path],
                )

        row_count = sum(result.row_count for result in parsed)
        block_count = sum(result.block_count for result in parsed)
        table_count = sum(result.table_count for result in parsed)
        write_partition_status(
            database_path=database_path,
            dataset_name="disclosures",
            processed_week=processed_week,
            source_document_count=source_document_count,
            expected_row_count=row_count,
            actual_row_count=row_count,
        )
        return {
            "dataset_name": "disclosures",
            "partition_key": partition_key,
            "source_document_count": source_document_count,
            "parsed_document_count": len(tasks),
            "unique_artifact_count": len(artifact_paths),
            "row_count": row_count,
            "block_count": block_count,
            "table_count": table_count,
            "table": tables.QUALIFIED_FACT_DISCLOSURES_TABLE,
        }

    metadata = atomic_partition_database(destination, build)
    return {**metadata, "duckdb_path": str(destination)}


def _parse_disclosure_document(
    task: _DisclosureDocumentTask,
) -> _DisclosureParseResult:
    with Path(task.artifact_path).open("r", encoding="utf-8") as handle:
        artifact = json.load(handle)
    rows: list[dict[str, object]] = []
    row_count = 0
    block_count = 0
    table_count = 0
    output_path = Path(task.output_path)
    with pq.ParquetWriter(
        output_path,
        _DISCLOSURE_ARROW_SCHEMA,
        compression="zstd",
    ) as writer:
        for fact in facts.iter_artifact_facts(
            artifact,
            lei=task.lei,
            fxo_id=task.source_document_id,
            period_end=task.period_end,
        ):
            if fact.value_kind != "text" or fact.raw_value.strip() == "":
                continue
            source: Mapping[str, object] = {
                "source_document_id": task.source_document_id,
                "package_sha256": task.package_sha256,
                "lei": task.lei,
                "country_iso2": task.country_iso2,
                "company_id": task.company_id,
                "period_end": task.period_end,
                "fiscal_year": task.fiscal_year,
                "fact_id": fact.fact_id,
                "concept_qname": fact.concept_qname,
                "concept_local_name": fact.concept_local_name,
                "language": fact.language,
                "raw_value": fact.raw_value,
            }
            row = disclosure_row(
                source,
                source_run_id=task.source_run_id,
                extracted_at=task.extracted_at,
            )
            row["processed_week"] = task.processed_week
            rows.append(row)
            row_count += 1
            block_count += int(row["block_count"])
            table_count += int(row["table_count"])
            if len(rows) >= DISCLOSURE_BATCH_SIZE:
                writer.write_table(pa.Table.from_pylist(rows, schema=_DISCLOSURE_ARROW_SCHEMA))
                rows.clear()
        if rows:
            writer.write_table(pa.Table.from_pylist(rows, schema=_DISCLOSURE_ARROW_SCHEMA))
    return _DisclosureParseResult(
        output_path=str(output_path),
        row_count=row_count,
        block_count=block_count,
        table_count=table_count,
    )


@dg.asset(
    name="esef_source_documents_duckdb_v2",
    deps=ARTIFACT_DEPENDENCY,
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="esef_source_documents_v2_duckdb",
    description="Builds one isolated source-document DuckDB per processed week.",
)
def esef_source_documents_duckdb_v2(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata=write_result_projection_partition(
            object_store=object_store,
            partition_key=context.partition_key,
            projection=SOURCE_DOCUMENTS_PROJECTION,
        )
    )


@dg.asset(
    name="esef_filing_facts_duckdb_v2",
    deps=ARTIFACT_DEPENDENCY,
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb", "xbrl"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="esef_filing_facts_v2_duckdb",
    description="Builds one isolated XBRL-fact DuckDB per processed week.",
)
def esef_filing_facts_duckdb_v2(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata=build_facts_partition_database(
            object_store=object_store,
            partition_key=context.partition_key,
            source_run_id=context.run_id,
            log_info=context.log.info,
            log_warning=context.log.warning,
        )
    )


@dg.asset(
    name="esef_document_contact_candidates_duckdb_v2",
    deps=ARTIFACT_DEPENDENCY,
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="esef_document_contact_candidates_v2_duckdb",
    description="Builds one isolated contact-candidate DuckDB per processed week.",
)
def esef_document_contact_candidates_duckdb_v2(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata=write_result_projection_partition(
            object_store=object_store,
            partition_key=context.partition_key,
            projection=CONTACT_CANDIDATES_PROJECTION,
        )
    )


@dg.asset(
    name="esef_document_concept_labels_duckdb_v2",
    deps=ARTIFACT_DEPENDENCY,
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb", "taxonomy"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="esef_document_concept_labels_v2_duckdb",
    description="Builds one isolated taxonomy-label DuckDB per processed week.",
)
def esef_document_concept_labels_duckdb_v2(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata=write_result_projection_partition(
            object_store=object_store,
            partition_key=context.partition_key,
            projection=CONCEPT_LABELS_PROJECTION,
        )
    )


@dg.asset(
    name="esef_fact_disclosures_duckdb_v2",
    deps=ARTIFACT_DEPENDENCY,
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb", "xhtml"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="esef_fact_disclosures_v2_duckdb",
    description="Builds disclosures directly from artifacts in an isolated weekly DuckDB.",
)
def esef_fact_disclosures_duckdb_v2(
    context: dg.AssetExecutionContext,
    config: EsefPartitionedDisclosureConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata=build_disclosures_partition_database(
            object_store=object_store,
            partition_key=context.partition_key,
            source_run_id=context.run_id,
            parse_workers=config.parse_workers,
        )
    )


ESEF_PARSING_V2_ASSETS = (
    esef_source_documents_duckdb_v2,
    esef_filing_facts_duckdb_v2,
    esef_document_contact_candidates_duckdb_v2,
    esef_document_concept_labels_duckdb_v2,
    esef_fact_disclosures_duckdb_v2,
)


defs = dg.Definitions(assets=list(ESEF_PARSING_V2_ASSETS))
