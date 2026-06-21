from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import shutil
import zipfile

import dlt as dlt_lib
import duckdb
from dlt.sources.filesystem import filesystem, read_csv_duckdb

OPEN_PAGE_RANK_DLT_PIPELINE_NAME = "open_page_rank_raw_csv_duckdb"
OPEN_PAGE_RANK_DLT_DATASET_NAME = "open_page_rank_raw"
OPEN_PAGE_RANK_RAW_TABLE = "open_page_rank_raw_domains"


@dataclass(frozen=True)
class ExtractedOpenPageRankCsv:
    csv_path: Path


def extract_single_csv_member(*, zip_path: str | Path, output_dir: str | Path) -> Path:
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
            raise ValueError(f"Open PageRank ZIP {archive_path} contains no CSV members")
        if len(csv_members) > 1:
            names = [info.filename for info in csv_members]
            raise ValueError(
                f"Open PageRank ZIP {archive_path} contains multiple CSV members: {names}"
            )

        output_path = target_dir / "open_page_rank_domains.csv"
        with archive.open(csv_members[0]) as source, output_path.open("wb") as target:
            shutil.copyfileobj(source, target, length=8 * 1024 * 1024)
        return output_path


def open_page_rank_csv_dlt_pipeline(database_path: str | Path) -> Any:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    working_dir = database_file.parent / ".dlt" / "open_page_rank"
    working_dir.mkdir(parents=True, exist_ok=True)
    return dlt_lib.pipeline(
        pipeline_name=OPEN_PAGE_RANK_DLT_PIPELINE_NAME,
        destination=dlt_lib.destinations.duckdb(str(database_file)),
        dataset_name=OPEN_PAGE_RANK_DLT_DATASET_NAME,
        dev_mode=False,
        pipelines_dir=str(working_dir),
    )


@dlt_lib.source(name="open_page_rank_csv")
def open_page_rank_csv_dlt_source(
    extracted_file: ExtractedOpenPageRankCsv,
) -> list[Any]:
    resource = filesystem(
        bucket_url=str(extracted_file.csv_path.parent),
        file_glob=extracted_file.csv_path.name,
    ) | read_csv_duckdb(use_pyarrow=True, header=True, all_varchar=True)
    resource = resource.with_name(OPEN_PAGE_RANK_RAW_TABLE)
    resource.apply_hints(write_disposition="replace")
    return [resource]


def load_open_page_rank_raw_table(
    *,
    database_path: str | Path,
    extracted_file: ExtractedOpenPageRankCsv,
) -> dict[str, int]:
    pipeline = open_page_rank_csv_dlt_pipeline(database_path)
    pipeline.drop_pending_packages()
    load_info = pipeline.run(open_page_rank_csv_dlt_source(extracted_file))
    load_info.raise_on_failed_jobs()
    return {OPEN_PAGE_RANK_RAW_TABLE: raw_table_row_count(database_path)}


def raw_table_row_count(database_path: str | Path) -> int:
    database_file = Path(database_path)
    if not database_file.exists():
        return 0

    with duckdb.connect(str(database_file), read_only=True) as connection:
        exists = connection.execute(
            """
            select 1
            from information_schema.tables
            where table_schema = ?
              and table_name = ?
            limit 1
            """,
            [OPEN_PAGE_RANK_DLT_DATASET_NAME, OPEN_PAGE_RANK_RAW_TABLE],
        ).fetchone()
        if exists is None:
            return 0
        return int(
            connection.execute(
                f'select count(*) from "{OPEN_PAGE_RANK_DLT_DATASET_NAME}"."{OPEN_PAGE_RANK_RAW_TABLE}"'
            ).fetchone()[0]
        )
