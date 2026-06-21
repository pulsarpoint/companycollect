from pathlib import Path
import zipfile

import duckdb
import pytest

from dagster_v3.defs.gleif.dlt_csv import (
    ExtractedGleifCsv,
    GLEIF_DLT_RAW_DATASET_NAME,
    GLEIF_RAW_LEI_RECORDS_TABLE,
    GLEIF_RAW_RELATIONSHIPS_TABLE,
    GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
    extract_single_csv_member,
    gleif_csv_dlt_source,
    gleif_definition_time_csv_files,
    load_gleif_csv_raw_tables,
)


def test_extract_single_csv_member_writes_csv(tmp_path: Path) -> None:
    archive_path = tmp_path / "source.csv.zip"
    write_zip(archive_path, {"lei2.csv": "LEI,Entity.LegalName\n123,Acme\n"})

    csv_path = extract_single_csv_member(
        zip_path=archive_path,
        output_dir=tmp_path / "out",
        file_kind="lei_records",
    )

    assert csv_path.name == "lei_records.csv"
    assert csv_path.read_text(encoding="utf-8") == "LEI,Entity.LegalName\n123,Acme\n"


def test_extract_single_csv_member_rejects_empty_zip(tmp_path: Path) -> None:
    archive_path = tmp_path / "source.csv.zip"
    write_zip(archive_path, {"readme.txt": "not csv"})

    with pytest.raises(ValueError, match="contains no CSV members"):
        extract_single_csv_member(
            zip_path=archive_path,
            output_dir=tmp_path / "out",
            file_kind="lei_records",
        )


def test_extract_single_csv_member_rejects_multiple_csv_members(tmp_path: Path) -> None:
    archive_path = tmp_path / "source.csv.zip"
    write_zip(archive_path, {"a.csv": "a\n", "b.csv": "b\n"})

    with pytest.raises(ValueError, match="contains multiple CSV members"):
        extract_single_csv_member(
            zip_path=archive_path,
            output_dir=tmp_path / "out",
            file_kind="relationships",
        )


def test_definition_time_source_has_three_resources() -> None:
    dlt_source = gleif_csv_dlt_source(gleif_definition_time_csv_files())

    assert set(dlt_source.resources.keys()) == {
        GLEIF_RAW_LEI_RECORDS_TABLE,
        GLEIF_RAW_RELATIONSHIPS_TABLE,
        GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
    }


def test_gleif_csv_dlt_pipeline_loads_duckdb_raw_table(tmp_path: Path) -> None:
    csv_path = tmp_path / "lei_records.csv"
    csv_path.write_text(
        "LEI,Entity.LegalName,Entity.LegalName.xmllang\n"
        "5493001KJTIIGC8Y1R12,ACME PLC,en\n",
        encoding="utf-8",
    )
    database_path = tmp_path / "gleif_reference.duckdb"

    row_counts = load_gleif_csv_raw_tables(
        database_path=database_path,
        extracted_files=[
            ExtractedGleifCsv(
                file_kind="lei_records",
                csv_path=csv_path,
                source_url="https://example.test/lei2/latest.csv",
                s3_key="gleif/raw/run_id=run-1/file_kind=lei_records/source.csv.zip",
                source_sha256="a" * 64,
                publish_date="2026-06-20T16:00:00+00:00",
                load_mode="full",
                run_id="run-1",
            )
        ],
    )

    assert row_counts[GLEIF_RAW_LEI_RECORDS_TABLE] == 1
    with duckdb.connect(str(database_path), read_only=True) as connection:
        assert (
            connection.execute(
                f"select count(*) from {GLEIF_DLT_RAW_DATASET_NAME}.{GLEIF_RAW_LEI_RECORDS_TABLE}"
            ).fetchone()[0]
            == 1
        )
        assert connection.execute(
            f"""
            select lei, entity_legal_name, entity_legal_name_xmllang
            from {GLEIF_DLT_RAW_DATASET_NAME}.{GLEIF_RAW_LEI_RECORDS_TABLE}
            """
        ).fetchall() == [("5493001KJTIIGC8Y1R12", "ACME PLC", "en")]


def test_gleif_csv_dlt_pipeline_loads_more_than_one_csv_chunk(tmp_path: Path) -> None:
    row_count = 5_001
    csv_path = tmp_path / "lei_records.csv"
    csv_path.write_text(
        "LEI,Entity.LegalName,Entity.LegalName.xmllang\n"
        + "".join(
            f"5493001KJTIIGC8Y{i:08d},Company {i},en\n"
            for i in range(row_count)
        ),
        encoding="utf-8",
    )
    database_path = tmp_path / "gleif_reference.duckdb"

    row_counts = load_gleif_csv_raw_tables(
        database_path=database_path,
        extracted_files=[
            ExtractedGleifCsv(
                file_kind="lei_records",
                csv_path=csv_path,
                source_url="https://example.test/lei2/latest.csv",
                s3_key="gleif/raw/run_id=run-1/file_kind=lei_records/source.csv.zip",
                source_sha256="a" * 64,
                publish_date="2026-06-20T16:00:00+00:00",
                load_mode="full",
                run_id="run-1",
            )
        ],
    )

    assert row_counts[GLEIF_RAW_LEI_RECORDS_TABLE] == row_count
    with duckdb.connect(str(database_path), read_only=True) as connection:
        assert (
            connection.execute(
                f"select count(*) from {GLEIF_DLT_RAW_DATASET_NAME}.{GLEIF_RAW_LEI_RECORDS_TABLE}"
            ).fetchone()[0]
            == row_count
        )


def write_zip(path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, body in members.items():
            archive.writestr(name, body)
