import tempfile
import zipfile
from pathlib import Path
from typing import Any

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_company import tables
from dagster_v3.defs.sweden_company.resources import SWEDEN_COMPANY_RAW_BUCKET


def load_sweden_company_raw_manifest(
    *,
    connection: Any,
    object_store: ObjectStoreResource,
    manifest: dict[str, Any],
    source_run_id: str,
) -> dict[str, int]:
    connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
    _replace_raw_files_table(connection=connection, manifest=manifest)

    counts = {tables.RAW_FILES_TABLE: _table_count(connection, tables.RAW_FILES_TABLE)}
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
                counts[tables.BOLAGSVERKET_RAW_TABLE] = _table_count(
                    connection, tables.BOLAGSVERKET_RAW_TABLE
                )
            elif source_slug == "scb_bulkfil":
                _replace_scb_raw_table(
                    connection=connection,
                    csv_path=text_path,
                    source_run_id=source_run_id,
                    source_s3_key=s3_key,
                )
                counts[tables.SCB_RAW_TABLE] = _table_count(connection, tables.SCB_RAW_TABLE)
            else:
                raise ValueError(f"Unsupported Sweden company source slug: {source_slug}")
    return counts


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
        create or replace table {tables.DLT_DATASET_NAME}.{tables.RAW_FILES_TABLE} (
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
            f"insert into {tables.DLT_DATASET_NAME}.{tables.RAW_FILES_TABLE} values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )


def _replace_bolagsverket_raw_table(
    *,
    connection: Any,
    csv_path: Path,
    source_run_id: str,
    source_s3_key: str,
) -> None:
    _replace_source_table(
        connection=connection,
        table_name=tables.BOLAGSVERKET_RAW_TABLE,
        csv_path=csv_path,
        source_columns=tables.BOLAGSVERKET_SOURCE_COLUMNS,
        source_record_id_column="organisationsidentitet",
        source_run_id=source_run_id,
        source_s3_key=source_s3_key,
        read_csv_options="delim=';', header=true, all_varchar=true, quote='\"', escape='\"'",
    )


def _replace_scb_raw_table(
    *,
    connection: Any,
    csv_path: Path,
    source_run_id: str,
    source_s3_key: str,
) -> None:
    utf8_path = csv_path.with_suffix(".utf8.txt")
    utf8_path.write_text(csv_path.read_text(encoding="latin-1"), encoding="utf-8")
    _replace_source_table(
        connection=connection,
        table_name=tables.SCB_RAW_TABLE,
        csv_path=utf8_path,
        source_columns=tables.SCB_SOURCE_COLUMNS,
        source_record_id_column="PeOrgNr",
        source_run_id=source_run_id,
        source_s3_key=source_s3_key,
        read_csv_options=(
            "delim='\\t', header=true, all_varchar=true, quote='\"', escape='\"', "
            "null_padding=true, strict_mode=false"
        ),
    )


def _replace_source_table(
    *,
    connection: Any,
    table_name: str,
    csv_path: Path,
    source_columns: tuple[str, ...],
    source_record_id_column: str,
    source_run_id: str,
    source_s3_key: str,
    read_csv_options: str,
) -> None:
    column_select = ",\n                ".join(_quoted(column) for column in source_columns)
    raw_record_sql = _raw_record_sql(source_columns)
    connection.execute(
        f"""
        create or replace table {tables.DLT_DATASET_NAME}.{table_name} as
        with source_rows as (
            select
                row_number() over ()::bigint as source_line_number,
                {column_select}
            from read_csv(?, {read_csv_options})
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
            {_quoted(source_record_id_column)}::varchar as source_record_id,
            sha256(raw_record)::varchar as source_payload_hash,
            ?::varchar as source_s3_key,
            raw_record,
            {column_select}
        from with_raw_record
        """,
        [str(csv_path), source_run_id, source_s3_key],
    )


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
            target.write(source.read())
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
