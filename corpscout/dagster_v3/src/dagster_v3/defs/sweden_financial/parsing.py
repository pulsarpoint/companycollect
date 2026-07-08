import json
import re
import tempfile
import zipfile
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from time import monotonic
from typing import Any

from lxml import etree

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_financial.resources import SWEDEN_FINANCIAL_RAW_BUCKET

SWEDEN_FINANCIAL_DATASET_NAME = "sweden_financial"
SWEDEN_FINANCIAL_DUCKDB_ROOT = Path("data/sweden_finacial")
SWEDEN_FINANCIAL_DUCKDB_PATH = (
    SWEDEN_FINANCIAL_DUCKDB_ROOT / "sweden_financial_source_2026.duckdb"
)
RAW_ARCHIVE_PREFIX = "sweden_financial/raw_archives/"
REPORT_XHTML_PREFIX = "sweden_financial/report_xhtml/"
LOG_EVERY_NESTED_ZIPS = 1_000
SWEDEN_FINANCIAL_XHTML_PARSER_VERSION = "sweden-financial-ixbrl-v1"
PARSED_REPORT_INSERT_BATCH_SIZE = 50_000
IX_NS = "http://www.xbrl.org/2013/inlineXBRL"
XBRLI_NS = "http://www.xbrl.org/2003/instance"
XBRLDI_NS = "http://xbrl.org/2006/xbrldi"
LINK_NS = "http://www.xbrl.org/2003/linkbase"
XLINK_NS = "http://www.w3.org/1999/xlink"

_DATE_PATTERN = re.compile(
    r"(?P<year>\d{4})[-_]? (?P<month>\d{2})[-_]? (?P<day>\d{2})", re.X
)
_COMPANY_ID_PATTERN = re.compile(r"(?<!\d)(\d{10,12})(?!\d)")
_NUMERIC_CLEAN_RE = re.compile(r"[^0-9.\-]")
_XML_PARSER = etree.XMLParser(recover=True, huge_tree=True, resolve_entities=False)


def sweden_financial_source_duckdb_path(
    year: str | int,
    *,
    root: str | Path = SWEDEN_FINANCIAL_DUCKDB_ROOT,
) -> Path:
    normalized_year = _normalize_year(year)
    return Path(root) / f"sweden_financial_source_{normalized_year}.duckdb"


def extract_sweden_financial_report_xhtml_catalog(
    *,
    connection: Any,
    object_store: ObjectStoreResource,
    source_run_id: str,
    partition_year: str,
    source_archive_keys: list[str] | None = None,
    replace_scope: str = "partition",
    log_info: Callable[..., object] | None = None,
) -> dict[str, int]:
    started_at = monotonic()
    object_store.ensure_bucket(SWEDEN_FINANCIAL_RAW_BUCKET)
    if source_archive_keys is None:
        source_archive_keys = object_store.list_keys(
            f"{RAW_ARCHIVE_PREFIX}year={partition_year}/",
            bucket=SWEDEN_FINANCIAL_RAW_BUCKET,
        )
    else:
        source_archive_keys = sorted(source_archive_keys)
    archive_count = len(source_archive_keys)
    if log_info is not None:
        log_info(
            "Starting Sweden financial XHTML catalog extraction: "
            "partition_year=%s archives=%s replace_scope=%s",
            partition_year,
            archive_count,
            replace_scope,
        )
    rows: list[tuple[Any, ...]] = []
    nested_zip_count = 0
    skipped_nested_zip_count = 0
    downloaded_report_count = 0
    reused_report_count = 0

    with tempfile.TemporaryDirectory(prefix="sweden_financial_parse_") as tmpdir:
        temp_dir = Path(tmpdir)
        for archive_index, source_archive_key in enumerate(
            sorted(source_archive_keys),
            start=1,
        ):
            archive_started_at = monotonic()
            archive_nested_zip_count = 0
            archive_report_count = 0
            archive_skipped_nested_zip_count = 0
            archive_downloaded_report_count = 0
            archive_reused_report_count = 0
            if log_info is not None:
                log_info(
                    "Processing Sweden financial archive: partition_year=%s "
                    "archive_index=%s archive_count=%s source_archive_key=%s",
                    partition_year,
                    archive_index,
                    archive_count,
                    source_archive_key,
                )
            archive_path = (
                temp_dir / f"{sha256(source_archive_key.encode()).hexdigest()}.zip"
            )
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
                    archive_nested_zip_count += 1
                    nested_zip_body = outer_zip.read(nested_member)
                    nested_reports = _extract_nested_reports(
                        nested_zip_body=nested_zip_body,
                        nested_zip_member=nested_member.filename,
                        source_archive_key=source_archive_key,
                        source_run_id=source_run_id,
                        partition_year=partition_year,
                        object_store=object_store,
                    )
                    if not nested_reports:
                        skipped_nested_zip_count += 1
                        archive_skipped_nested_zip_count += 1
                    for row, downloaded in nested_reports:
                        rows.append(row)
                        archive_report_count += 1
                        if downloaded:
                            downloaded_report_count += 1
                            archive_downloaded_report_count += 1
                        else:
                            reused_report_count += 1
                            archive_reused_report_count += 1
                    if log_info is not None and (
                        archive_nested_zip_count == 1
                        or archive_nested_zip_count % LOG_EVERY_NESTED_ZIPS == 0
                    ):
                        log_info(
                            "Processing Sweden financial nested ZIPs: "
                            "partition_year=%s archive_index=%s archive_count=%s "
                            "nested_zips_in_archive=%s reports_in_archive=%s "
                            "skipped_nested_zips_in_archive=%s total_reports=%s",
                            partition_year,
                            archive_index,
                            archive_count,
                            archive_nested_zip_count,
                            archive_report_count,
                            archive_skipped_nested_zip_count,
                            len(rows),
                        )
            if log_info is not None:
                log_info(
                    "Processed Sweden financial archive: partition_year=%s "
                    "archive_index=%s archive_count=%s nested_zips=%s reports=%s "
                    "skipped_nested_zips=%s downloaded_reports=%s reused_reports=%s "
                    "total_reports=%s elapsed_seconds=%.1f source_archive_key=%s",
                    partition_year,
                    archive_index,
                    archive_count,
                    archive_nested_zip_count,
                    archive_report_count,
                    archive_skipped_nested_zip_count,
                    archive_downloaded_report_count,
                    archive_reused_report_count,
                    len(rows),
                    monotonic() - archive_started_at,
                    source_archive_key,
                )

    if log_info is not None:
        log_info(
            "Replacing Sweden financial XHTML catalog rows: partition_year=%s "
            "replace_scope=%s rows=%s",
            partition_year,
            replace_scope,
            len(rows),
        )
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
    if log_info is not None:
        log_info(
            "Finished Sweden financial XHTML catalog extraction: partition_year=%s "
            "archives=%s nested_zips=%s skipped_nested_zips=%s reports=%s "
            "downloaded_reports=%s reused_reports=%s elapsed_seconds=%.1f",
            partition_year,
            archive_count,
            nested_zip_count,
            skipped_nested_zip_count,
            len(rows),
            downloaded_report_count,
            reused_report_count,
            monotonic() - started_at,
        )
    return {
        "partition_year": partition_year,
        "source_archive_count": len(source_archive_keys),
        "nested_zip_count": nested_zip_count,
        "skipped_nested_zip_count": skipped_nested_zip_count,
        "report_xhtml_count": len(rows),
        "downloaded_report_xhtml_count": downloaded_report_count,
        "reused_report_xhtml_count": reused_report_count,
        "catalog_row_count": len(rows),
    }


def parse_sweden_financial_report_xhtml_catalog(
    *,
    connection: Any,
    object_store: ObjectStoreResource,
    source_run_id: str,
    partition_year: str,
    replace_scope: str = "partition",
    catalog_source_run_id: str | None = None,
    insert_batch_size: int = PARSED_REPORT_INSERT_BATCH_SIZE,
    log_info: Callable[..., object] | None = None,
) -> dict[str, int]:
    if insert_batch_size < 1:
        raise ValueError("Sweden financial XHTML insert batch size must be positive")
    _ensure_parsed_report_tables(connection)
    catalog_rows = _report_xhtml_catalog_rows(
        connection=connection,
        partition_year=partition_year,
        catalog_source_run_id=catalog_source_run_id,
    )
    source_archive_names = sorted(
        {
            str(row["source_archive_name"])
            for row in catalog_rows
            if str(row["source_archive_name"]).strip()
        }
    )
    _delete_parsed_report_scope(
        connection=connection,
        replace_scope=replace_scope,
        partition_year=partition_year,
        source_archive_names=source_archive_names,
    )

    if log_info is not None:
        log_info(
            "Starting Sweden financial XHTML parse: partition_year=%s catalog_rows=%s replace_scope=%s",
            partition_year,
            len(catalog_rows),
            replace_scope,
        )

    resolved_at = datetime.now(UTC)
    report_rows: list[tuple[Any, ...]] = []
    fact_rows: list[tuple[Any, ...]] = []
    error_rows: list[tuple[Any, ...]] = []
    report_row_count = 0
    fact_row_count = 0
    error_row_count = 0

    for index, catalog_row in enumerate(catalog_rows, start=1):
        try:
            body = object_store.read_bytes(
                str(catalog_row["xhtml_s3_key"]),
                bucket=str(catalog_row["xhtml_s3_bucket"]),
            )
            report_row, report_fact_rows = _parse_report_xhtml(
                catalog_row=catalog_row,
                body=body,
                source_run_id=source_run_id,
                resolved_at=resolved_at,
            )
            report_rows.append(report_row)
            fact_rows.extend(report_fact_rows)
        except Exception as exc:  # noqa: BLE001 - persist bad XHTML and keep parsing.
            error_rows.append(
                (
                    source_run_id,
                    partition_year,
                    catalog_row["source_archive_name"],
                    catalog_row["company_id"],
                    catalog_row["report_period_end"],
                    catalog_row["xhtml_s3_key"],
                    f"{type(exc).__name__}: {exc}",
                    resolved_at,
                )
            )
            if log_info is not None:
                log_info(
                    "Sweden financial XHTML parse failed: partition_year=%s index=%s/%s xhtml_key=%s error=%s: %s",
                    partition_year,
                    index,
                    len(catalog_rows),
                    catalog_row["xhtml_s3_key"],
                    type(exc).__name__,
                    exc,
                )
            if len(error_rows) >= insert_batch_size:
                inserted_reports, inserted_facts, inserted_errors = (
                    _flush_parsed_report_rows(
                        connection=connection,
                        report_rows=report_rows,
                        fact_rows=fact_rows,
                        error_rows=error_rows,
                    )
                )
                report_row_count += inserted_reports
                fact_row_count += inserted_facts
                error_row_count += inserted_errors
                if log_info is not None:
                    log_info(
                        "Inserted Sweden financial XHTML parse batch: partition_year=%s "
                        "batch_reports=%s batch_facts=%s batch_errors=%s "
                        "total_reports=%s total_facts=%s total_errors=%s",
                        partition_year,
                        inserted_reports,
                        inserted_facts,
                        inserted_errors,
                        report_row_count,
                        fact_row_count,
                        error_row_count,
                    )
            continue

        if (
            len(fact_rows) >= insert_batch_size
            or len(report_rows) >= insert_batch_size
            or len(error_rows) >= insert_batch_size
        ):
            inserted_reports, inserted_facts, inserted_errors = (
                _flush_parsed_report_rows(
                    connection=connection,
                    report_rows=report_rows,
                    fact_rows=fact_rows,
                    error_rows=error_rows,
                )
            )
            report_row_count += inserted_reports
            fact_row_count += inserted_facts
            error_row_count += inserted_errors
            if log_info is not None:
                log_info(
                    "Inserted Sweden financial XHTML parse batch: partition_year=%s "
                    "batch_reports=%s batch_facts=%s batch_errors=%s "
                    "total_reports=%s total_facts=%s total_errors=%s",
                    partition_year,
                    inserted_reports,
                    inserted_facts,
                    inserted_errors,
                    report_row_count,
                    fact_row_count,
                    error_row_count,
                )

        if log_info is not None and (
            index == 1 or index == len(catalog_rows) or index % 100 == 0
        ):
            log_info(
                "Sweden financial XHTML parse progress: partition_year=%s parsed=%s/%s facts=%s",
                partition_year,
                index,
                len(catalog_rows),
                fact_row_count + len(fact_rows),
            )

    inserted_reports, inserted_facts, inserted_errors = _flush_parsed_report_rows(
        connection=connection,
        report_rows=report_rows,
        fact_rows=fact_rows,
        error_rows=error_rows,
    )
    report_row_count += inserted_reports
    fact_row_count += inserted_facts
    error_row_count += inserted_errors

    if log_info is not None:
        log_info(
            "Finished Sweden financial XHTML parse: partition_year=%s reports=%s facts=%s errors=%s",
            partition_year,
            report_row_count,
            fact_row_count,
            error_row_count,
        )

    return {
        "partition_year": partition_year,
        "catalog_row_count": len(catalog_rows),
        "reports_parsed_count": report_row_count,
        "reports_failed_count": error_row_count,
        "report_row_count": report_row_count,
        "fact_row_count": fact_row_count,
        "parse_error_row_count": error_row_count,
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
    source_archive = _safe_path_segment(
        _archive_name_from_object_key(source_archive_key)
    )
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
            return []

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
        raise ValueError(
            f"Unknown Sweden financial XHTML replace scope: {replace_scope}"
        )
    if rows:
        connection.executemany(
            f"""
            insert into {SWEDEN_FINANCIAL_DATASET_NAME}.report_xhtml_catalog
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def _ensure_parsed_report_tables(connection: Any) -> None:
    connection.execute(f"create schema if not exists {SWEDEN_FINANCIAL_DATASET_NAME}")
    connection.execute(
        f"""
        create table if not exists {SWEDEN_FINANCIAL_DATASET_NAME}.reports (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            statement_key varchar,
            company_id varchar,
            report_period_start date,
            report_period_end date,
            fiscal_year integer,
            reported_company_name varchar,
            report_language varchar,
            source_archive_key varchar,
            source_archive_name varchar,
            nested_zip_name varchar,
            xhtml_object_key varchar,
            xhtml_sha256 varchar,
            xhtml_size_bytes bigint,
            taxonomy_entrypoint varchar,
            schema_refs varchar,
            contexts_count bigint,
            units_count bigint,
            facts_count bigint,
            parser_version varchar,
            source_payload_hash varchar,
            resolved_at timestamp
        )
        """
    )
    connection.execute(
        f"""
        create table if not exists {SWEDEN_FINANCIAL_DATASET_NAME}.facts (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            statement_key varchar,
            company_id varchar,
            report_period_end date,
            fact_ordinal bigint,
            concept_qname varchar,
            concept_namespace varchar,
            concept_local_name varchar,
            context_id varchar,
            unit_id varchar,
            decimals varchar,
            precision varchar,
            value_kind varchar,
            raw_value varchar,
            amount_original decimal(38, 10),
            amount_usd decimal(38, 10),
            date_value date,
            text_value varchar,
            currency varchar,
            dimensions varchar,
            fx_rate_to_usd decimal(38, 12),
            fx_rate_date date,
            fx_source varchar,
            parser_version varchar,
            source_payload_hash varchar,
            resolved_at timestamp
        )
        """
    )
    connection.execute(
        f"""
        create table if not exists {SWEDEN_FINANCIAL_DATASET_NAME}.parse_errors (
            source_run_id varchar,
            partition_year varchar,
            source_archive_name varchar,
            company_id varchar,
            report_period_end varchar,
            xhtml_object_key varchar,
            error varchar,
            resolved_at timestamp
        )
        """
    )


def _report_xhtml_catalog_rows(
    *,
    connection: Any,
    partition_year: str,
    catalog_source_run_id: str | None,
) -> list[dict[str, Any]]:
    where = "where partition_year = ?"
    params: list[Any] = [partition_year]
    if catalog_source_run_id is not None:
        where += " and source_run_id = ?"
        params.append(catalog_source_run_id)
    rows = connection.execute(
        f"""
        select
            source_run_id,
            partition_year,
            source_archive_key,
            nested_zip_member,
            xhtml_member,
            xhtml_s3_bucket,
            xhtml_s3_key,
            company_id,
            report_period_end,
            source_archive_year,
            source_archive_name,
            size_bytes,
            sha256
        from {SWEDEN_FINANCIAL_DATASET_NAME}.report_xhtml_catalog
        {where}
        order by source_archive_key, nested_zip_member, xhtml_member
        """,
        params,
    ).fetchall()
    columns = (
        "source_run_id",
        "partition_year",
        "source_archive_key",
        "nested_zip_member",
        "xhtml_member",
        "xhtml_s3_bucket",
        "xhtml_s3_key",
        "company_id",
        "report_period_end",
        "source_archive_year",
        "source_archive_name",
        "size_bytes",
        "sha256",
    )
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _delete_parsed_report_scope(
    *,
    connection: Any,
    replace_scope: str,
    partition_year: str,
    source_archive_names: list[str],
) -> None:
    source_archive_year_prefix = f"{RAW_ARCHIVE_PREFIX}year={partition_year}/%"
    if replace_scope == "partition":
        _delete_parsed_reports_for_filter(
            connection=connection,
            report_filter_sql="source_archive_key like ?",
            report_filter_params=[source_archive_year_prefix],
        )
        connection.execute(
            f"""
            delete from {SWEDEN_FINANCIAL_DATASET_NAME}.parse_errors
            where partition_year = ?
            """,
            [partition_year],
        )
        return
    if replace_scope != "archive":
        raise ValueError(
            f"Unknown Sweden financial parsed replace scope: {replace_scope}"
        )
    if not source_archive_names:
        return
    placeholders = ", ".join("?" for _ in source_archive_names)
    report_filter_params = [source_archive_year_prefix, *source_archive_names]
    _delete_parsed_reports_for_filter(
        connection=connection,
        report_filter_sql=(
            f"source_archive_key like ? and source_archive_name in ({placeholders})"
        ),
        report_filter_params=report_filter_params,
    )
    connection.execute(
        f"""
        delete from {SWEDEN_FINANCIAL_DATASET_NAME}.parse_errors
        where partition_year = ?
          and source_archive_name in ({placeholders})
        """,
        [partition_year, *source_archive_names],
    )


def _delete_parsed_reports_for_filter(
    *,
    connection: Any,
    report_filter_sql: str,
    report_filter_params: list[Any],
) -> None:
    connection.execute(
        f"""
        delete from {SWEDEN_FINANCIAL_DATASET_NAME}.facts
        where statement_key in (
            select statement_key
            from {SWEDEN_FINANCIAL_DATASET_NAME}.reports
            where {report_filter_sql}
        )
        """,
        report_filter_params,
    )
    connection.execute(
        f"""
        delete from {SWEDEN_FINANCIAL_DATASET_NAME}.reports
        where {report_filter_sql}
        """,
        report_filter_params,
    )


def _flush_parsed_report_rows(
    *,
    connection: Any,
    report_rows: list[tuple[Any, ...]],
    fact_rows: list[tuple[Any, ...]],
    error_rows: list[tuple[Any, ...]],
) -> tuple[int, int, int]:
    report_row_count = len(report_rows)
    fact_row_count = len(fact_rows)
    error_row_count = len(error_rows)
    if report_rows:
        connection.executemany(
            f"""
            insert into {SWEDEN_FINANCIAL_DATASET_NAME}.reports
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            report_rows,
        )
        report_rows.clear()
    if fact_rows:
        connection.executemany(
            f"""
            insert into {SWEDEN_FINANCIAL_DATASET_NAME}.facts
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            fact_rows,
        )
        fact_rows.clear()
    if error_rows:
        connection.executemany(
            f"""
            insert into {SWEDEN_FINANCIAL_DATASET_NAME}.parse_errors
            values (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            error_rows,
        )
        error_rows.clear()
    return report_row_count, fact_row_count, error_row_count


def _parse_report_xhtml(
    *,
    catalog_row: dict[str, Any],
    body: bytes,
    source_run_id: str,
    resolved_at: datetime,
) -> tuple[tuple[Any, ...], list[tuple[Any, ...]]]:
    root = etree.fromstring(body, parser=_XML_PARSER)
    schema_refs = _schema_refs(root)
    contexts = _contexts_by_id(root)
    units = _units_by_id(root)
    facts = list(_fact_elements(root))
    source_payload_hash = str(catalog_row["sha256"])
    statement_key = sha256(
        f"{catalog_row['xhtml_s3_key']}:{source_payload_hash}".encode()
    ).hexdigest()
    report_period_end = _parse_date(
        str(catalog_row["report_period_end"])
    ) or _latest_period_end(
        contexts,
    )
    report_period_start = _period_start_for_report_end(contexts, report_period_end)
    fiscal_year = report_period_end.year if report_period_end is not None else None
    report_language = root.get(
        "{http://www.w3.org/XML/1998/namespace}lang"
    ) or root.get("lang")

    report_row = (
        "SE",
        SWEDEN_FINANCIAL_DATASET_NAME,
        source_run_id,
        statement_key,
        statement_key,
        catalog_row["company_id"],
        report_period_start,
        report_period_end,
        fiscal_year,
        _reported_company_name(facts),
        report_language,
        catalog_row["source_archive_key"],
        catalog_row["source_archive_name"],
        catalog_row["nested_zip_member"],
        catalog_row["xhtml_s3_key"],
        source_payload_hash,
        catalog_row["size_bytes"],
        schema_refs[0] if schema_refs else None,
        json.dumps(schema_refs, ensure_ascii=False),
        len(contexts),
        len(units),
        len(facts),
        SWEDEN_FINANCIAL_XHTML_PARSER_VERSION,
        source_payload_hash,
        resolved_at,
    )
    fact_rows = [
        _parsed_fact_row(
            fact=fact,
            fact_ordinal=ordinal,
            statement_key=statement_key,
            company_id=str(catalog_row["company_id"]),
            report_period_end=report_period_end,
            contexts=contexts,
            units=units,
            source_run_id=source_run_id,
            source_payload_hash=source_payload_hash,
            resolved_at=resolved_at,
        )
        for ordinal, fact in enumerate(facts, start=1)
    ]
    return report_row, fact_rows


def _schema_refs(root: etree._Element) -> list[str]:
    return [
        href
        for element in root.findall(f".//{{{LINK_NS}}}schemaRef")
        if (href := element.get(f"{{{XLINK_NS}}}href") or element.get("href"))
    ]


def _contexts_by_id(root: etree._Element) -> dict[str, dict[str, Any]]:
    return {
        context["context_id"]: context
        for element in root.findall(f".//{{{XBRLI_NS}}}context")
        if (context := _context_row(element))["context_id"]
    }


def _context_row(element: etree._Element) -> dict[str, Any]:
    period = element.find(f"{{{XBRLI_NS}}}period")
    instant = (
        _parse_date(period.findtext(f"{{{XBRLI_NS}}}instant"))
        if period is not None
        else None
    )
    start = (
        _parse_date(period.findtext(f"{{{XBRLI_NS}}}startDate"))
        if period is not None
        else None
    )
    end = (
        _parse_date(period.findtext(f"{{{XBRLI_NS}}}endDate"))
        if period is not None
        else None
    )
    dimensions = {
        str(member.get("dimension")): (member.text or "").strip()
        for member in element.findall(f".//{{{XBRLDI_NS}}}explicitMember")
        if member.get("dimension") and (member.text or "").strip()
    }
    return {
        "context_id": element.get("id", ""),
        "period_start": start,
        "period_end": instant or end,
        "dimensions": dimensions,
    }


def _units_by_id(root: etree._Element) -> dict[str, str]:
    units: dict[str, str] = {}
    for element in root.findall(f".//{{{XBRLI_NS}}}unit"):
        unit_id = element.get("id")
        measure = element.findtext(f".//{{{XBRLI_NS}}}measure")
        if unit_id:
            units[unit_id] = (measure or "").strip()
    return units


def _fact_elements(root: etree._Element) -> list[etree._Element]:
    return [
        element
        for element in root.iter()
        if element.tag in {f"{{{IX_NS}}}nonFraction", f"{{{IX_NS}}}nonNumeric"}
        and element.get("name")
        and element.get("contextRef")
    ]


def _parsed_fact_row(
    *,
    fact: etree._Element,
    fact_ordinal: int,
    statement_key: str,
    company_id: str,
    report_period_end: date | None,
    contexts: dict[str, dict[str, Any]],
    units: dict[str, str],
    source_run_id: str,
    source_payload_hash: str,
    resolved_at: datetime,
) -> tuple[Any, ...]:
    concept_qname = fact.get("name", "")
    concept_namespace, concept_local_name = _resolve_qname(concept_qname, fact.nsmap)
    context_id = fact.get("contextRef", "")
    context = contexts.get(context_id, {"dimensions": {}})
    unit_id = fact.get("unitRef")
    raw_value = " ".join("".join(fact.itertext()).split())
    amount_original = None
    date_value = None
    text_value = None
    if fact.tag == f"{{{IX_NS}}}nonFraction":
        amount_original = _parse_decimal(
            raw_value,
            scale=fact.get("scale"),
            sign=fact.get("sign"),
        )
        value_kind = "numeric" if amount_original is not None else "text"
        if amount_original is None:
            text_value = raw_value or None
    else:
        date_value = _parse_date(raw_value)
        if date_value is not None:
            value_kind = "date"
        elif raw_value:
            value_kind = "text"
            text_value = raw_value
        else:
            value_kind = "empty"
    return (
        "SE",
        SWEDEN_FINANCIAL_DATASET_NAME,
        source_run_id,
        f"{statement_key}:{fact_ordinal}",
        statement_key,
        company_id,
        report_period_end,
        fact_ordinal,
        concept_qname,
        concept_namespace,
        concept_local_name,
        context_id,
        unit_id,
        fact.get("decimals"),
        fact.get("precision"),
        value_kind,
        raw_value,
        amount_original,
        None,
        date_value,
        text_value,
        _currency_from_unit(unit_id=unit_id, units=units),
        json.dumps(context.get("dimensions", {}), ensure_ascii=False),
        None,
        None,
        "",
        SWEDEN_FINANCIAL_XHTML_PARSER_VERSION,
        source_payload_hash,
        resolved_at,
    )


def _resolve_qname(qname: str, nsmap: dict[str | None, str]) -> tuple[str, str]:
    if ":" not in qname:
        return "", qname
    prefix, local_name = qname.split(":", 1)
    return nsmap.get(prefix, ""), local_name


def _parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value.strip()[:10])
    except ValueError:
        return None


def _parse_decimal(
    raw_value: str,
    *,
    scale: str | None,
    sign: str | None,
) -> Decimal | None:
    if not raw_value:
        return None
    negative = sign == "-" or raw_value.startswith("(")
    cleaned = _NUMERIC_CLEAN_RE.sub("", raw_value.replace(",", ""))
    cleaned = cleaned.replace("-", "")
    if cleaned in {"", "."}:
        return None
    try:
        value = Decimal(cleaned)
        if scale:
            value *= Decimal(10) ** int(scale)
    except InvalidOperation, ValueError:
        return None
    return -value if negative else value


def _currency_from_unit(
    *,
    unit_id: str | None,
    units: dict[str, str],
) -> str | None:
    if unit_id is None:
        return None
    measure = units.get(unit_id, "")
    candidate = (
        measure.rsplit(":", 1)[-1].strip().upper()
        if measure
        else unit_id.strip().upper()
    )
    if len(candidate) == 3 and candidate.isalpha():
        return candidate
    return None


def _latest_period_end(contexts: dict[str, dict[str, Any]]) -> date | None:
    dates = [
        context["period_end"]
        for context in contexts.values()
        if context.get("period_end") is not None
    ]
    return max(dates) if dates else None


def _period_start_for_report_end(
    contexts: dict[str, dict[str, Any]],
    report_period_end: date | None,
) -> date | None:
    for context in contexts.values():
        if (
            context.get("period_end") == report_period_end
            and context.get("period_start") is not None
        ):
            return context["period_start"]
    return None


def _reported_company_name(facts: list[etree._Element]) -> str | None:
    for fact in facts:
        concept_name = fact.get("name", "").rsplit(":", 1)[-1].lower()
        if concept_name in {
            "companyname",
            "entityname",
            "foretagsnamn",
            "företagsnamn",
        }:
            value = " ".join("".join(fact.itertext()).split())
            if value:
                return value
    return None


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
        report_period_end = f"{date_match.group('year')}-{date_match.group('month')}-{date_match.group('day')}"
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


def _normalize_year(year: str | int) -> str:
    normalized = str(year).strip()
    if not normalized.isdigit() or len(normalized) != 4:
        raise ValueError(
            f"Sweden financial source year must be a four-digit year: {year!r}"
        )
    return normalized


def _safe_path_segment(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")
    return cleaned or "unknown"
