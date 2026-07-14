from __future__ import annotations

from pathlib import Path
import zipfile

import duckdb

from dagster_v3.defs.open_page_rank.dlt_csv import (
    OPEN_PAGE_RANK_RAW_TABLE,
    ExtractedOpenPageRankCsv,
    extract_single_csv_member,
    load_open_page_rank_raw_table,
    raw_table_row_count,
)


def test_extract_single_csv_member_rejects_zip_without_csv(tmp_path: Path) -> None:
    archive_path = tmp_path / "bad.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("notes.txt", "not csv")

    try:
        extract_single_csv_member(zip_path=archive_path, output_dir=tmp_path)
    except ValueError as exc:
        assert "contains no CSV members" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_dlt_loads_open_page_rank_csv_with_arrow(tmp_path: Path) -> None:
    csv_path = tmp_path / "top10milliondomains.csv"
    csv_path.write_text(
        "Rank,Domain,Open Page Rank,Extension\n"
        "1,google.com,10.00,com\n"
        "2,example.co.uk,7.50,uk\n"
    )
    database_path = tmp_path / "open_page_rank_source.duckdb"

    load_open_page_rank_raw_table(
        database_path=database_path,
        extracted_file=ExtractedOpenPageRankCsv(csv_path=csv_path),
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        counts = {OPEN_PAGE_RANK_RAW_TABLE: raw_table_row_count(connection)}

    assert counts == {OPEN_PAGE_RANK_RAW_TABLE: 2}
