from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
import zipfile

import dlt as dlt_lib
import duckdb
from dlt.sources.filesystem import filesystem, read_csv_duckdb

GLEIF_DLT_PIPELINE_NAME = "gleif_raw_csv_duckdb"
GLEIF_DLT_RAW_DATASET_NAME = "gleif_raw"
GLEIF_RAW_LEI_RECORDS_TABLE = "gleif_raw_lei_records"
GLEIF_RAW_RELATIONSHIPS_TABLE = "gleif_raw_relationships"
GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE = "gleif_raw_reporting_exceptions"

FileKind = Literal["lei_records", "relationships", "reporting_exceptions"]

RAW_TABLE_BY_FILE_KIND: dict[FileKind, str] = {
    "lei_records": GLEIF_RAW_LEI_RECORDS_TABLE,
    "relationships": GLEIF_RAW_RELATIONSHIPS_TABLE,
    "reporting_exceptions": GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
}


@dataclass(frozen=True)
class ExtractedGleifCsv:
    file_kind: FileKind
    csv_path: Path
    source_url: str
    s3_key: str
    source_sha256: str
    publish_date: str
    load_mode: Literal["full", "delta"]
    run_id: str


def extract_single_csv_member(
    *,
    zip_path: str | Path,
    output_dir: str | Path,
    file_kind: str,
) -> Path:
    archive_path = Path(zip_path)
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(archive_path) as archive:
        csv_members = [
            info
            for info in archive.infolist()
            if not info.is_dir() and info.filename.lower().endswith(".csv")
        ]
        if not csv_members:
            raise ValueError(f"GLEIF ZIP {archive_path} contains no CSV members")
        if len(csv_members) > 1:
            names = [info.filename for info in csv_members]
            raise ValueError(f"GLEIF ZIP {archive_path} contains multiple CSV members: {names}")

        output_path = target_dir / f"{file_kind}.csv"
        with archive.open(csv_members[0]) as source, output_path.open("wb") as target:
            target.write(source.read())
        return output_path


def gleif_csv_dlt_pipeline(database_path: str | Path) -> Any:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    working_dir = database_file.parent / ".dlt" / "gleif"
    working_dir.mkdir(parents=True, exist_ok=True)
    return dlt_lib.pipeline(
        pipeline_name=GLEIF_DLT_PIPELINE_NAME,
        destination=dlt_lib.destinations.duckdb(str(database_file)),
        dataset_name=GLEIF_DLT_RAW_DATASET_NAME,
        dev_mode=False,
        pipelines_dir=str(working_dir),
    )


def load_gleif_csv_raw_tables(
    *,
    database_path: str | Path,
    extracted_files: Iterable[ExtractedGleifCsv],
) -> None:
    files = list(extracted_files)
    pipeline = gleif_csv_dlt_pipeline(database_path)
    pipeline.drop_pending_packages()
    for item in files:
        load_info = pipeline.run(gleif_csv_dlt_source([item]))
        load_info.raise_on_failed_jobs()


def raw_table_row_counts(connection: duckdb.DuckDBPyConnection) -> dict[str, int]:
    return {
        table_name: _raw_table_row_count(connection, table_name)
        for table_name in RAW_TABLE_BY_FILE_KIND.values()
    }


def _raw_table_row_count(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
) -> int:
    exists = connection.execute(
        """
        select 1
        from information_schema.tables
        where table_schema = ?
          and table_name = ?
        limit 1
        """,
        [GLEIF_DLT_RAW_DATASET_NAME, table_name],
    ).fetchone()
    if exists is None:
        return 0
    return int(
        connection.execute(
            f'select count(*) from "{GLEIF_DLT_RAW_DATASET_NAME}"."{table_name}"'
        ).fetchone()[0]
    )


def gleif_definition_time_csv_files() -> list[ExtractedGleifCsv]:
    base = Path("/tmp/gleif-dlt-definition-shape")
    return [
        ExtractedGleifCsv(
            file_kind=file_kind,
            csv_path=base / f"{file_kind}.csv",
            source_url=f"https://example.invalid/{file_kind}.csv",
            s3_key=f"definition-shape/{file_kind}/source.csv.zip",
            source_sha256="0" * 64,
            publish_date="1970-01-01T00:00:00+00:00",
            load_mode="full",
            run_id="definition-shape",
        )
        for file_kind in RAW_TABLE_BY_FILE_KIND
    ]


@dlt_lib.source(name="gleif_csv")
def gleif_csv_dlt_source(
    extracted_files: Iterable[ExtractedGleifCsv],
) -> list[Any]:
    resources: list[Any] = []
    for item in extracted_files:
        table_name = RAW_TABLE_BY_FILE_KIND[item.file_kind]
        resource = filesystem(
            bucket_url=str(item.csv_path.parent),
            file_glob=item.csv_path.name,
        ) | read_csv_duckdb(use_pyarrow=True, header=True, all_varchar=True)
        resource = resource.with_name(table_name)
        resource.apply_hints(write_disposition="replace")
        resources.append(resource)
    return resources
