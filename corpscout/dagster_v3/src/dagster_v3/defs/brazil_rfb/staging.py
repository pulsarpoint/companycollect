from __future__ import annotations

from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_rfb import source, tables
from dagster_v3.defs.brazil_rfb.duckdb_attach import (
    attached_read_only_database,
    sql_literal,
)


def _sql_literal(value: object) -> str:
    return sql_literal(value)


def _list_literal(values: tuple[str, ...]) -> str:
    return "[" + ", ".join(_sql_literal(value) for value in values) + "]"


def load_raw_family_from_manifest(
    *,
    connection: duckdb.DuckDBPyConnection,
    manifest_database_path: str | Path,
    family: str,
    source_run_id: str,
) -> int:
    if family not in tables.RAW_TABLE_BY_FAMILY:
        raise ValueError(f"unknown Brazil RFB file family: {family}")

    table_name = tables.RAW_TABLE_BY_FAMILY[family]
    column_names = tables.RAW_COLUMNS_BY_FAMILY[family]
    dataset = tables.DLT_DATASET_NAME
    qualified = f"{dataset}.{table_name}"

    connection.execute(f"create schema if not exists {dataset}")
    with attached_read_only_database(
        connection,
        database_path=manifest_database_path,
        alias="manifest_db",
    ) as manifest_alias:
        manifest = f"{manifest_alias}.{dataset}.{tables.SNAPSHOT_FILES_TABLE}"
        manifest_rows = connection.execute(
            f"""
            select csv_path, archive_url, csv_member_name
            from {manifest}
            where family = ? and source_run_id = ?
            order by archive_name, csv_member_name
            """,
            [family, source_run_id],
        ).fetchall()
    if not manifest_rows:
        raise ValueError(f"No Brazil RFB manifest rows found for family {family}")

    raw_csv_paths = tuple(Path(str(row[0])) for row in manifest_rows)
    if all(csv_path.stat().st_size == 0 for csv_path in raw_csv_paths):
        raise ValueError(f"Brazil RFB family {family} produced no rows")

    csv_paths = tuple(
        str(source.normalize_csv_for_duckdb(csv_path))
        for csv_path in raw_csv_paths
    )
    read_csv_paths = _list_literal(csv_paths)
    read_csv_columns = _list_literal(column_names)
    manifest_values = ", ".join(
        "("
        + ", ".join(
            (
                _sql_literal(csv_path),
                _sql_literal(family),
                _sql_literal(str(row[1])),
                _sql_literal(str(row[2])),
            )
        )
        + ")"
        for row, csv_path in zip(manifest_rows, csv_paths, strict=True)
    )
    connection.execute(
        f"""
        create or replace table {qualified} as
        with file_manifest(csv_path, source_file_family, source_archive_url, source_csv_member) as (
            values {manifest_values}
        ),
        loaded as (
            select *
            from read_csv(
                {read_csv_paths},
                names = {read_csv_columns},
                header = false,
                all_varchar = true,
                delim = ';',
                quote = '"',
                escape = '"',
                encoding = 'utf-8',
                filename = true
            )
        )
        select
            {", ".join(column_names)},
            fm.source_file_family,
            fm.source_archive_url,
            fm.source_csv_member,
            {_sql_literal(source_run_id)} as source_run_id,
            now() as loaded_at
        from loaded
        join file_manifest fm on loaded.filename = fm.csv_path
        """
    )
    count = int(connection.execute(f"select count(*) from {qualified}").fetchone()[0])

    if count == 0:
        raise ValueError(f"Brazil RFB family {family} produced no rows")
    return count


def load_all_raw_families_from_manifest(
    *,
    connection: duckdb.DuckDBPyConnection,
    manifest_database_path: str | Path,
    source_run_id: str,
) -> dict[str, int]:
    return {
        family: load_raw_family_from_manifest(
            connection=connection,
            manifest_database_path=manifest_database_path,
            family=family,
            source_run_id=source_run_id,
        )
        for family in tables.RAW_TABLE_BY_FAMILY
    }


def load_raw_families_from_manifest(
    *,
    connection: duckdb.DuckDBPyConnection,
    manifest_database_path: str | Path,
    families: tuple[str, ...],
    source_run_id: str,
) -> dict[str, int]:
    return {
        family: load_raw_family_from_manifest(
            connection=connection,
            manifest_database_path=manifest_database_path,
            family=family,
            source_run_id=source_run_id,
        )
        for family in families
    }
