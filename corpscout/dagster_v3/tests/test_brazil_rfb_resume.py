from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_rfb import resume, source, tables


def _create_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")


def test_stage_table_counts_require_existing_nonempty_tables(tmp_path: Path) -> None:
    database_path = tmp_path / "stage.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        _create_schema(connection)
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.companies as
            select 1 as is_active
            """
        )

    assert resume.stage_table_counts(database_path, ("companies",)) == {"companies": 1}
    assert (
        resume.stage_table_counts(database_path, ("companies", "establishments"))
        is None
    )

    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            f"create table {tables.DLT_DATASET_NAME}.establishments as select * from (select 1) where false"
        )

    assert (
        resume.stage_table_counts(database_path, ("companies", "establishments"))
        is None
    )


def test_existing_manifest_rows_are_reused_with_current_run_id(tmp_path: Path) -> None:
    database_path = tmp_path / "manifest.duckdb"
    csv_paths = {}
    for family in source.DEFAULT_FAMILIES:
        csv_path = tmp_path / family / f"{family}.csv"
        csv_path.parent.mkdir()
        csv_path.write_text("row\n")
        csv_paths[family] = csv_path

    with duckdb.connect(str(database_path)) as connection:
        _create_schema(connection)
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.SNAPSHOT_FILES_TABLE} (
                family varchar,
                archive_url varchar,
                archive_name varchar,
                archive_sha256 varchar,
                csv_member_name varchar,
                csv_path varchar,
                source_run_id varchar,
                retrieved_at timestamp
            )
            """
        )
        for family, csv_path in csv_paths.items():
            connection.execute(
                f"""
                insert into {tables.DLT_DATASET_NAME}.{tables.SNAPSHOT_FILES_TABLE}
                values (?, ?, ?, 'hash', ?, ?, 'old-run', now())
                """,
                [
                    family,
                    f"https://example.test/{family}.zip",
                    f"{family}.zip",
                    f"{family}.csv",
                    str(csv_path),
                ],
            )

    rows = resume.existing_snapshot_manifest_rows(
        database_path,
        source_run_id="new-run",
        required_families=source.DEFAULT_FAMILIES,
    )

    assert rows is not None
    assert {row["family"] for row in rows} == set(source.DEFAULT_FAMILIES)
    assert {row["source_run_id"] for row in rows} == {"new-run"}

    next(iter(csv_paths.values())).unlink()

    assert (
        resume.existing_snapshot_manifest_rows(
            database_path,
            source_run_id="new-run",
            required_families=source.DEFAULT_FAMILIES,
        )
        is None
    )
