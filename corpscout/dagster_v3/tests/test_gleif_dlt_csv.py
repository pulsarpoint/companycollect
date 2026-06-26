from collections.abc import Iterable
from pathlib import Path
import zipfile

import duckdb
import pytest

from dagster_v3.defs.gleif.dlt_csv import (
    ExtractedGleifCsv,
    FileKind,
    GLEIF_DLT_RAW_DATASET_NAME,
    GLEIF_RAW_LEI_RECORDS_TABLE,
    GLEIF_RAW_RELATIONSHIPS_TABLE,
    GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
    extract_single_csv_member,
    gleif_csv_dlt_source,
    gleif_definition_time_csv_files,
    load_gleif_csv_raw_tables,
    raw_table_row_counts,
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

    load_gleif_csv_raw_tables(
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

    with duckdb.connect(str(database_path), read_only=True) as connection:
        row_counts = raw_table_row_counts(connection)
        assert row_counts[GLEIF_RAW_LEI_RECORDS_TABLE] == 1
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

    load_gleif_csv_raw_tables(
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

    with duckdb.connect(str(database_path), read_only=True) as connection:
        row_counts = raw_table_row_counts(connection)
        assert row_counts[GLEIF_RAW_LEI_RECORDS_TABLE] == row_count
        assert (
            connection.execute(
                f"select count(*) from {GLEIF_DLT_RAW_DATASET_NAME}.{GLEIF_RAW_LEI_RECORDS_TABLE}"
            ).fetchone()[0]
            == row_count
        )


def test_gleif_csv_raw_loader_loads_all_chunks_for_multiple_resources(
    tmp_path: Path,
) -> None:
    row_count = 5_001
    extracted_files = [
        _write_extracted_csv(
            tmp_path,
            file_kind="lei_records",
            header="LEI,Entity.LegalName,Entity.LegalName.xmllang\n",
            rows=(
                f"5493001KJTIIGC8Y{i:08d},Company {i},en\n"
                for i in range(row_count)
            ),
        ),
        _write_extracted_csv(
            tmp_path,
            file_kind="relationships",
            header=(
                "Relationship.StartNode.NodeID,Relationship.EndNode.NodeID,"
                "Relationship.RelationshipType\n"
            ),
            rows=(
                f"5493001KJTIIGC8Y{i:08d},5493001KJTIIGC8P{i:08d},"
                "IS_DIRECTLY_CONSOLIDATED_BY\n"
                for i in range(row_count)
            ),
        ),
        _write_extracted_csv(
            tmp_path,
            file_kind="reporting_exceptions",
            header="LEI,Exception.Category,Exception.Reason.1\n",
            rows=(
                "5493001KJTIIGC8Y"
                f"{i:08d},DIRECT_ACCOUNTING_CONSOLIDATION_PARENT,NON_CONSOLIDATING\n"
                for i in range(row_count)
            ),
        ),
    ]

    database_path = tmp_path / "gleif_reference.duckdb"
    load_gleif_csv_raw_tables(
        database_path=database_path,
        extracted_files=extracted_files,
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        assert raw_table_row_counts(connection) == {
            GLEIF_RAW_LEI_RECORDS_TABLE: row_count,
            GLEIF_RAW_RELATIONSHIPS_TABLE: row_count,
            GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE: row_count,
        }


def _write_extracted_csv(
    tmp_path: Path,
    *,
    file_kind: FileKind,
    header: str,
    rows: Iterable[str],
) -> ExtractedGleifCsv:
    csv_path = tmp_path / f"{file_kind}.csv"
    csv_path.write_text(header + "".join(rows), encoding="utf-8")
    return ExtractedGleifCsv(
        file_kind=file_kind,
        csv_path=csv_path,
        source_url=f"https://example.test/{file_kind}/latest.csv",
        s3_key=f"gleif/raw/run_id=run-1/file_kind={file_kind}/source.csv.zip",
        source_sha256="a" * 64,
        publish_date="2026-06-20T16:00:00+00:00",
        load_mode="full",
        run_id="run-1",
    )


def write_zip(path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, body in members.items():
            archive.writestr(name, body)
