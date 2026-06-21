from __future__ import annotations

from pathlib import Path

import duckdb

from dagster_v3.defs.open_page_rank.dlt_csv import (
    OPEN_PAGE_RANK_DLT_DATASET_NAME,
    OPEN_PAGE_RANK_RAW_TABLE,
)
from dagster_v3.defs.open_page_rank.tables import (
    OPEN_PAGE_RANK_DOMAINS_COLUMNS,
    OPEN_PAGE_RANK_DOMAINS_TABLE,
)
from dagster_v3.defs.open_page_rank.transforms import (
    replace_current_open_page_rank_domains,
)


def test_replace_current_open_page_rank_domains_normalizes_raw_rows(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "open_page_rank_source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f'create schema "{OPEN_PAGE_RANK_DLT_DATASET_NAME}"')
        connection.execute(
            f'''
            create table "{OPEN_PAGE_RANK_DLT_DATASET_NAME}"."{OPEN_PAGE_RANK_RAW_TABLE}"
            (
                rank varchar,
                domain varchar,
                open_page_rank varchar,
                extension varchar
            )
            '''
        )
        connection.execute(
            f'''
            insert into "{OPEN_PAGE_RANK_DLT_DATASET_NAME}"."{OPEN_PAGE_RANK_RAW_TABLE}"
            values ('1', ' Google.COM ', '10.00', 'COM'), ('bad', '', 'x', '')
            '''
        )

    row_count = replace_current_open_page_rank_domains(
        database_path=database_path,
        source_url="https://www.domcop.com/files/top/top10milliondomains.csv.zip",
        source_run_id="run-1",
        retrieved_date="2026-06-21",
        retrieved_at="2026-06-21T10:30:00+00:00",
    )

    assert row_count == 1
    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            f'''
            select {", ".join(OPEN_PAGE_RANK_DOMAINS_COLUMNS)}
            from open_page_rank.{OPEN_PAGE_RANK_DOMAINS_TABLE}
            '''
        ).fetchall()

    assert rows[0][0:9] == (
        "open_page_rank",
        "domcop_top_10m_domains",
        "run-1",
        "open_page_rank:1:google.com",
        1,
        "google.com",
        "google.com",
        "com",
        10.0,
    )


def test_replace_current_open_page_rank_domains_derives_extension_when_missing(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "open_page_rank_source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f'create schema "{OPEN_PAGE_RANK_DLT_DATASET_NAME}"')
        connection.execute(
            f'''
            create table "{OPEN_PAGE_RANK_DLT_DATASET_NAME}"."{OPEN_PAGE_RANK_RAW_TABLE}"
            (
                rank varchar,
                domain varchar,
                open_page_rank varchar
            )
            '''
        )
        connection.execute(
            f'''
            insert into "{OPEN_PAGE_RANK_DLT_DATASET_NAME}"."{OPEN_PAGE_RANK_RAW_TABLE}"
            values ('2', 'Example.CO.UK', '7.50')
            '''
        )

    row_count = replace_current_open_page_rank_domains(
        database_path=database_path,
        source_url="https://www.domcop.com/files/top/top10milliondomains.csv.zip",
        source_run_id="run-1",
        retrieved_date="2026-06-21",
        retrieved_at="2026-06-21T10:30:00+00:00",
    )

    assert row_count == 1
    with duckdb.connect(str(database_path), read_only=True) as connection:
        row = connection.execute(
            f'''
            select domain, domain_extension
            from open_page_rank.{OPEN_PAGE_RANK_DOMAINS_TABLE}
            '''
        ).fetchone()

    assert row == ("example.co.uk", "uk")
