import re
import tempfile
import zipfile
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_financial.resources import SWEDEN_FINANCIAL_RAW_BUCKET

SWEDEN_FINANCIAL_DATASET_NAME = "sweden_financial"
SWEDEN_FINANCIAL_DUCKDB_PATH = Path("data/sweden_financial_source.duckdb")
RAW_ARCHIVE_PREFIX = "sweden_financial/raw_archives/"
REPORT_XHTML_PREFIX = "sweden_financial/report_xhtml/"

_DATE_PATTERN = re.compile(r"(?P<year>\d{4})[-_]? (?P<month>\d{2})[-_]? (?P<day>\d{2})", re.X)
_COMPANY_ID_PATTERN = re.compile(r"(?<!\d)(\d{10,12})(?!\d)")


def extract_sweden_financial_report_xhtml_catalog(
    *,
    connection: Any,
    object_store: ObjectStoreResource,
    source_run_id: str,
    partition_year: str,
    source_archive_keys: list[str] | None = None,
    replace_scope: str = "partition",
) -> dict[str, int]:
    object_store.ensure_bucket(SWEDEN_FINANCIAL_RAW_BUCKET)
    if source_archive_keys is None:
        source_archive_keys = object_store.list_keys(
            f"{RAW_ARCHIVE_PREFIX}year={partition_year}/",
            bucket=SWEDEN_FINANCIAL_RAW_BUCKET,
        )
    else:
        source_archive_keys = sorted(source_archive_keys)
    rows: list[tuple[Any, ...]] = []
    nested_zip_count = 0
    downloaded_report_count = 0
    reused_report_count = 0

    with tempfile.TemporaryDirectory(prefix="sweden_financial_parse_") as tmpdir:
        temp_dir = Path(tmpdir)
        for source_archive_key in sorted(source_archive_keys):
            archive_path = temp_dir / f"{sha256(source_archive_key.encode()).hexdigest()}.zip"
            object_store.download_file(
                source_archive_key,
                archive_path,
                bucket=SWEDEN_FINANCIAL_RAW_BUCKET,
            )
            with zipfile.ZipFile(archive_path) as outer_zip:
                for nested_member in _zip_file_members(outer_zip):
                    if not nested_member.filename.lower().endswith(".zip"):
                        continue
                    nested_zip_count += 1
                    nested_zip_body = outer_zip.read(nested_member)
                    nested_reports = _extract_nested_reports(
                        nested_zip_body=nested_zip_body,
                        nested_zip_member=nested_member.filename,
                        source_archive_key=source_archive_key,
                        source_run_id=source_run_id,
                        partition_year=partition_year,
                        object_store=object_store,
                    )
                    for row, downloaded in nested_reports:
                        rows.append(row)
                        if downloaded:
                            downloaded_report_count += 1
                        else:
                            reused_report_count += 1

    _replace_report_xhtml_catalog(
        connection=connection,
        partition_year=partition_year,
        rows=rows,
        replace_scope=replace_scope,
        source_archive_names=sorted(
            {
                _archive_name_from_object_key(source_archive_key)
                for source_archive_key in source_archive_keys
            }
        ),
    )
    return {
        "partition_year": partition_year,
        "source_archive_count": len(source_archive_keys),
        "nested_zip_count": nested_zip_count,
        "report_xhtml_count": len(rows),
        "downloaded_report_xhtml_count": downloaded_report_count,
        "reused_report_xhtml_count": reused_report_count,
        "catalog_row_count": len(rows),
    }


def report_xhtml_object_key(
    *,
    partition_year: str,
    source_archive_key: str,
    nested_zip_member: str,
    company_id: str,
    report_period_end: str,
    xhtml_member: str,
) -> str:
    source_archive = _safe_path_segment(_archive_name_from_object_key(source_archive_key))
    source_archive_hash = sha256(source_archive_key.encode()).hexdigest()[:16]
    nested_zip = _safe_path_segment(Path(nested_zip_member).stem)
    xhtml_name = _safe_path_segment(Path(xhtml_member).name)
    return (
        f"{REPORT_XHTML_PREFIX}"
        f"year={partition_year}/"
        f"company_id={company_id or 'unknown'}/"
        f"report_period_end={report_period_end or 'unknown'}/"
        f"source_archive_hash={source_archive_hash}/"
        f"source_archive={source_archive}/"
        f"nested_zip={nested_zip}/"
        f"{xhtml_name}"
    )


def _extract_nested_reports(
    *,
    nested_zip_body: bytes,
    nested_zip_member: str,
    source_archive_key: str,
    source_run_id: str,
    partition_year: str,
    object_store: ObjectStoreResource,
) -> list[tuple[tuple[Any, ...], bool]]:
    company_id, report_period_end = _metadata_from_nested_zip_name(nested_zip_member)
    source_archive_year = _year_from_archive_object_key(source_archive_key)
    source_archive_name = _archive_name_from_object_key(source_archive_key)
    reports: list[tuple[tuple[Any, ...], bool]] = []

    with zipfile.ZipFile(BytesIO(nested_zip_body)) as nested_zip:
        xhtml_members = [
            member
            for member in _zip_file_members(nested_zip)
            if member.filename.lower().endswith((".xhtml", ".html"))
        ]
        if not xhtml_members:
            raise ValueError(
                f"Expected at least one XHTML file in nested archive {nested_zip_member}"
            )

        for xhtml_member in xhtml_members:
            xhtml_body = nested_zip.read(xhtml_member)
            xhtml_s3_key = report_xhtml_object_key(
                partition_year=partition_year,
                source_archive_key=source_archive_key,
                nested_zip_member=nested_zip_member,
                company_id=company_id,
                report_period_end=report_period_end,
                xhtml_member=xhtml_member.filename,
            )
            downloaded = not object_store.exists(
                xhtml_s3_key,
                bucket=SWEDEN_FINANCIAL_RAW_BUCKET,
            )
            if downloaded:
                object_store.write_bytes(
                    xhtml_s3_key,
                    xhtml_body,
                    bucket=SWEDEN_FINANCIAL_RAW_BUCKET,
                )
            reports.append(
                (
                    (
                        source_run_id,
                        partition_year,
                        source_archive_key,
                        nested_zip_member,
                        xhtml_member.filename,
                        SWEDEN_FINANCIAL_RAW_BUCKET,
                        xhtml_s3_key,
                        company_id,
                        report_period_end,
                        source_archive_year,
                        source_archive_name,
                        len(xhtml_body),
                        sha256(xhtml_body).hexdigest(),
                    ),
                    downloaded,
                )
            )

    return reports


def _replace_report_xhtml_catalog(
    *,
    connection: Any,
    partition_year: str,
    rows: list[tuple[Any, ...]],
    replace_scope: str,
    source_archive_names: list[str],
) -> None:
    connection.execute(f"create schema if not exists {SWEDEN_FINANCIAL_DATASET_NAME}")
    connection.execute(
        f"""
        create table if not exists {SWEDEN_FINANCIAL_DATASET_NAME}.report_xhtml_catalog (
            source_run_id varchar,
            partition_year varchar,
            source_archive_key varchar,
            nested_zip_member varchar,
            xhtml_member varchar,
            xhtml_s3_bucket varchar,
            xhtml_s3_key varchar,
            company_id varchar,
            report_period_end varchar,
            source_archive_year varchar,
            source_archive_name varchar,
            size_bytes bigint,
            sha256 varchar
        )
        """
    )
    if replace_scope == "partition":
        connection.execute(
            f"""
            delete from {SWEDEN_FINANCIAL_DATASET_NAME}.report_xhtml_catalog
            where partition_year = ?
            """,
            [partition_year],
        )
    elif replace_scope == "archive":
        if source_archive_names:
            placeholders = ", ".join("?" for _ in source_archive_names)
            connection.execute(
                f"""
                delete from {SWEDEN_FINANCIAL_DATASET_NAME}.report_xhtml_catalog
                where partition_year = ?
                  and source_archive_name in ({placeholders})
                """,
                [partition_year, *source_archive_names],
            )
    else:
        raise ValueError(f"Unknown Sweden financial XHTML replace scope: {replace_scope}")
    if rows:
        connection.executemany(
            f"""
            insert into {SWEDEN_FINANCIAL_DATASET_NAME}.report_xhtml_catalog
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def _zip_file_members(archive: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    return [
        member
        for member in archive.infolist()
        if not member.is_dir() and not Path(member.filename).name.startswith(".")
    ]


def _metadata_from_nested_zip_name(nested_zip_member: str) -> tuple[str, str]:
    name = Path(nested_zip_member).stem
    company_match = _COMPANY_ID_PATTERN.search(name)
    date_match = None
    for match in _DATE_PATTERN.finditer(name):
        date_match = match
    company_id = company_match.group(1) if company_match is not None else ""
    report_period_end = ""
    if date_match is not None:
        report_period_end = (
            f"{date_match.group('year')}-{date_match.group('month')}-{date_match.group('day')}"
        )
    return company_id, report_period_end


def _year_from_archive_object_key(source_archive_key: str) -> str:
    for part in source_archive_key.split("/"):
        if part.startswith("year="):
            return part.removeprefix("year=")
    return "unknown"


def _archive_name_from_object_key(source_archive_key: str) -> str:
    for part in source_archive_key.split("/"):
        if part.startswith("archive="):
            return part.removeprefix("archive=")
    return Path(source_archive_key).name


def _safe_path_segment(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")
    return cleaned or "unknown"
