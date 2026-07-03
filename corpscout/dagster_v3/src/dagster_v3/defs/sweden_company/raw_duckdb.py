import tempfile
import zipfile
from shutil import copyfileobj
from pathlib import Path
from typing import Any

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_company import tables
from dagster_v3.defs.sweden_company.resources import SWEDEN_COMPANY_RAW_BUCKET

EXPECTED_SOURCE_SLUGS = frozenset({"bolagsverket_bulkfil", "scb_bulkfil"})


def load_sweden_company_raw_manifest(
    *,
    connection: Any,
    object_store: ObjectStoreResource,
    manifest: dict[str, Any],
    source_run_id: str,
) -> dict[str, int]:
    _validate_manifest_source_slugs(manifest)
    connection.execute("begin transaction")
    try:
        connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
        _replace_raw_files_table(connection=connection, manifest=manifest)

        counts = {"raw_files": _table_count(connection, "raw_files")}
        with tempfile.TemporaryDirectory(prefix="sweden_company_raw_") as tmpdir:
            temp_dir = Path(tmpdir)
            for file_entry in manifest["files"]:
                source_slug = str(file_entry["source_slug"])
                s3_key = str(file_entry["s3_key"])
                zip_path = temp_dir / f"{source_slug}.zip"
                object_store.download_file(
                    s3_key,
                    zip_path,
                    bucket=SWEDEN_COMPANY_RAW_BUCKET,
                )
                text_path = _extract_single_member(zip_path=zip_path, output_dir=temp_dir)
                if source_slug == "bolagsverket_bulkfil":
                    _replace_bolagsverket_raw_table(
                        connection=connection,
                        csv_path=text_path,
                        source_run_id=source_run_id,
                        source_s3_key=s3_key,
                    )
                    counts["bolagsverket_raw"] = _table_count(
                        connection, "bolagsverket_raw"
                    )
                elif source_slug == "scb_bulkfil":
                    _replace_scb_raw_table(
                        connection=connection,
                        csv_path=text_path,
                        source_run_id=source_run_id,
                        source_s3_key=s3_key,
                    )
                    counts["scb_raw"] = _table_count(connection, "scb_raw")
                else:
                    raise ValueError(
                        f"Unsupported Sweden company source slug: {source_slug}"
                    )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise
    return counts


def _validate_manifest_source_slugs(manifest: dict[str, Any]) -> None:
    source_slugs = [
        str(file_entry["source_slug"]) for file_entry in manifest["files"]
    ]
    actual_source_slugs = set(source_slugs)
    duplicate_source_slugs = sorted(
        source_slug
        for source_slug in actual_source_slugs
        if source_slugs.count(source_slug) > 1
    )
    missing_source_slugs = sorted(EXPECTED_SOURCE_SLUGS - actual_source_slugs)
    unexpected_source_slugs = sorted(actual_source_slugs - EXPECTED_SOURCE_SLUGS)
    if (
        not duplicate_source_slugs
        and not missing_source_slugs
        and not unexpected_source_slugs
    ):
        return

    actual = ", ".join(sorted(actual_source_slugs)) or "<none>"
    expected = ", ".join(sorted(EXPECTED_SOURCE_SLUGS))
    details = [
        f"expected source slugs: {expected}",
        f"actual source slugs: {actual}",
    ]
    if missing_source_slugs:
        details.append(f"missing source slug(s): {', '.join(missing_source_slugs)}")
    if unexpected_source_slugs:
        details.append(
            f"unexpected source slug(s): {', '.join(unexpected_source_slugs)}"
        )
    if duplicate_source_slugs:
        details.append(f"duplicate source slug(s): {', '.join(duplicate_source_slugs)}")
    raise ValueError(
        f"Invalid Sweden company manifest source slugs; {'; '.join(details)}"
    )


def _replace_raw_files_table(*, connection: Any, manifest: dict[str, Any]) -> None:
    rows = [
        (
            str(file_entry["source_slug"]),
            str(file_entry["source_url"]),
            SWEDEN_COMPANY_RAW_BUCKET,
            str(file_entry["s3_key"]),
            str(file_entry["source_last_modified"]),
            str(manifest["retrieved_date"]),
            str(manifest["run_id"]),
            _optional_int(file_entry.get("size_bytes")),
            str(file_entry.get("sha256") or ""),
        )
        for file_entry in manifest["files"]
    ]
    connection.execute(
        f"""
        create or replace table {tables.DLT_DATASET_NAME}.raw_files (
            source_slug varchar,
            source_url varchar,
            s3_bucket varchar,
            s3_key varchar,
            source_last_modified varchar,
            retrieved_date varchar,
            source_run_id varchar,
            size_bytes bigint,
            sha256 varchar
        )
        """
    )
    if rows:
        connection.executemany(
            f"insert into {tables.DLT_DATASET_NAME}.raw_files values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )


def _replace_bolagsverket_raw_table(
    *,
    connection: Any,
    csv_path: Path,
    source_run_id: str,
    source_s3_key: str,
) -> None:
    column_select = ",\n                ".join(
        _quoted(column) for column in tables.BOLAGSVERKET_SOURCE_COLUMNS
    )
    raw_record_sql = _raw_record_sql(tables.BOLAGSVERKET_SOURCE_COLUMNS)
    connection.execute(
        f"""
        create or replace table {tables.DLT_DATASET_NAME}.bolagsverket_raw as
        with source_rows as (
            select
                row_number() over ()::bigint as source_line_number,
                {column_select}
            from read_csv(?, delim=';', header=true, all_varchar=true, quote='"', escape='"')
        ),
        with_raw_record as (
            select
                *,
                {raw_record_sql} as raw_record
            from source_rows
        )
        select
            ?::varchar as source_run_id,
            source_line_number,
            "organisationsidentitet"::varchar as source_record_id,
            sha256(raw_record)::varchar as source_payload_hash,
            ?::varchar as source_s3_key,
            raw_record,
            {column_select}
        from with_raw_record
        """,
        [str(csv_path), source_run_id, source_s3_key],
    )


def _replace_scb_raw_table(
    *,
    connection: Any,
    csv_path: Path,
    source_run_id: str,
    source_s3_key: str,
) -> None:
    utf8_path = csv_path.with_suffix(".utf8.txt")
    _transcode_latin1_to_utf8(source_path=csv_path, target_path=utf8_path)
    column_select = ",\n                ".join(
        _quoted(column) for column in tables.SCB_SOURCE_COLUMNS
    )
    raw_record_sql = _raw_record_sql(tables.SCB_SOURCE_COLUMNS)
    connection.execute(
        f"""
        create or replace table {tables.DLT_DATASET_NAME}.scb_raw as
        with source_rows as (
            select
                row_number() over ()::bigint as source_line_number,
                {column_select}
            from read_csv(
                ?,
                delim='\t',
                header=true,
                all_varchar=true,
                quote='"',
                escape='"',
                null_padding=true,
                strict_mode=false
            )
        ),
        with_raw_record as (
            select
                *,
                {raw_record_sql} as raw_record
            from source_rows
        )
        select
            ?::varchar as source_run_id,
            source_line_number,
            "PeOrgNr"::varchar as source_record_id,
            sha256(raw_record)::varchar as source_payload_hash,
            ?::varchar as source_s3_key,
            raw_record,
            {column_select}
        from with_raw_record
        """,
        [str(utf8_path), source_run_id, source_s3_key],
    )


def _transcode_latin1_to_utf8(*, source_path: Path, target_path: Path) -> None:
    with (
        source_path.open("r", encoding="latin-1", newline="") as source,
        target_path.open("w", encoding="utf-8", newline="") as target,
    ):
        copyfileobj(source, target)


def _raw_record_sql(source_columns: tuple[str, ...]) -> str:
    keys = ", ".join(f"'{column}'" for column in source_columns)
    values = ", ".join(_quoted(column) for column in source_columns)
    return f"to_json(map([{keys}], [{values}]))"


def _extract_single_member(*, zip_path: Path, output_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path) as archive:
        members = [
            member
            for member in archive.infolist()
            if not member.is_dir() and not Path(member.filename).name.startswith(".")
        ]
        if len(members) != 1:
            names = [member.filename for member in members]
            raise ValueError(f"Expected one file in {zip_path.name}, got {names}")
        member = members[0]
        output_path = output_dir / Path(member.filename).name
        with archive.open(member) as source, output_path.open("wb") as target:
            copyfileobj(source, target)
        return output_path


def _table_count(connection: Any, table_name: str) -> int:
    value = connection.execute(
        f"select count(*) from {tables.DLT_DATASET_NAME}.{table_name}"
    ).fetchone()[0]
    return int(value)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'
