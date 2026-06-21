from __future__ import annotations

from pathlib import Path

import duckdb

from dagster_v3.defs.open_page_rank.dlt_csv import (
    OPEN_PAGE_RANK_DLT_DATASET_NAME,
    OPEN_PAGE_RANK_RAW_TABLE,
)
from dagster_v3.defs.open_page_rank.tables import OPEN_PAGE_RANK_DOMAINS_TABLE

OPEN_PAGE_RANK_DUCKDB_SCHEMA = "open_page_rank"


def replace_current_open_page_rank_domains(
    *,
    database_path: str | Path,
    source_url: str,
    source_run_id: str,
    retrieved_date: str,
    retrieved_at: str,
) -> int:
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f'create schema if not exists "{OPEN_PAGE_RANK_DUCKDB_SCHEMA}"')
        connection.execute(
            f'''
            create or replace table "{OPEN_PAGE_RANK_DUCKDB_SCHEMA}"."{OPEN_PAGE_RANK_DOMAINS_TABLE}" as
            with raw as (
                select
                    try_cast(nullif(trim("rank"), '') as uinteger) as source_rank,
                    lower(trim("domain")) as domain,
                    lower(trim("extension")) as domain_extension,
                    try_cast(nullif(trim("open_page_rank"), '') as double) as open_page_rank
                from "{OPEN_PAGE_RANK_DLT_DATASET_NAME}"."{OPEN_PAGE_RANK_RAW_TABLE}"
            )
            select
                'open_page_rank' as source_system,
                'domcop_top_10m_domains' as source_list_name,
                ? as source_run_id,
                concat('open_page_rank:', cast(source_rank as varchar), ':', domain)
                    as source_record_id,
                source_rank,
                domain,
                domain as root_domain,
                domain_extension,
                open_page_rank,
                ? as source_url,
                cast(? as date) as retrieved_date,
                cast(? as timestamp) as retrieved_at,
                now() as resolved_at
            from raw
            where source_rank is not null
              and domain != ''
            ''',
            [source_run_id, source_url, retrieved_date, retrieved_at],
        )
        return int(
            connection.execute(
                f'select count(*) from "{OPEN_PAGE_RANK_DUCKDB_SCHEMA}"."{OPEN_PAGE_RANK_DOMAINS_TABLE}"'
            ).fetchone()[0]
        )
