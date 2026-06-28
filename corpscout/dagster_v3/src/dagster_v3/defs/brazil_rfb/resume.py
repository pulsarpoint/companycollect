from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb

from dagster_v3.defs.brazil_rfb import tables


def _qualified_table_name(table_name: str) -> str:
    return f"{tables.DLT_DATASET_NAME}.{table_name}"


def _table_exists(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    exists = connection.execute(
        """
        select count(*)
        from information_schema.tables
        where table_schema = ?
          and table_name = ?
          and table_type = 'BASE TABLE'
        """,
        [tables.DLT_DATASET_NAME, table_name],
    ).fetchone()[0]
    return int(exists) == 1


def stage_table_counts(
    database_path: str | Path,
    table_names: Sequence[str],
) -> dict[str, int] | None:
    path = Path(database_path)
    if not path.exists():
        return None

    try:
        with duckdb.connect(str(path), read_only=True) as connection:
            counts: dict[str, int] = {}
            for table_name in table_names:
                if not _table_exists(connection, table_name):
                    return None
                count = int(
                    connection.execute(
                        f"select count(*) from {_qualified_table_name(table_name)}"
                    ).fetchone()[0]
                )
                if count == 0:
                    return None
                counts[table_name] = count
            return counts
    except duckdb.Error:
        return None


def existing_snapshot_manifest_rows(
    database_path: str | Path,
    *,
    source_run_id: str,
    required_families: Sequence[str],
) -> list[dict[str, Any]] | None:
    table_counts = stage_table_counts(database_path, (tables.SNAPSHOT_FILES_TABLE,))
    if table_counts is None:
        return None

    path = Path(database_path)
    try:
        with duckdb.connect(str(path), read_only=True) as connection:
            rows = connection.execute(
                f"""
                select
                    family,
                    archive_url,
                    archive_name,
                    archive_sha256,
                    csv_member_name,
                    csv_path,
                    retrieved_at
                from {_qualified_table_name(tables.SNAPSHOT_FILES_TABLE)}
                order by archive_name, csv_member_name
                """
            ).fetchall()
    except duckdb.Error:
        return None

    required_family_set = set(required_families)
    row_family_set = {str(row[0]) for row in rows}
    if not required_family_set.issubset(row_family_set):
        return None

    manifest_rows: list[dict[str, Any]] = []
    for row in rows:
        csv_path = Path(str(row[5]))
        if not csv_path.exists() or csv_path.stat().st_size == 0:
            return None
        manifest_rows.append(
            {
                "family": row[0],
                "archive_url": row[1],
                "archive_name": row[2],
                "archive_sha256": row[3],
                "csv_member_name": row[4],
                "csv_path": row[5],
                "source_run_id": source_run_id,
                "retrieved_at": row[6],
            }
        )
    return manifest_rows


def existing_companies_counts(database_path: str | Path) -> dict[str, int] | None:
    table_counts = stage_table_counts(
        database_path,
        (tables.COMPANIES_TABLE, tables.ESTABLISHMENTS_TABLE),
    )
    if table_counts is None:
        return None

    try:
        with duckdb.connect(str(database_path), read_only=True) as connection:
            active_companies = int(
                connection.execute(
                    f"""
                    select count(*)
                    from {_qualified_table_name(tables.COMPANIES_TABLE)}
                    where is_active = 1
                    """
                ).fetchone()[0]
            )
    except duckdb.Error:
        return None

    return {
        "companies": table_counts[tables.COMPANIES_TABLE],
        "establishments": table_counts[tables.ESTABLISHMENTS_TABLE],
        "active_companies": active_companies,
    }


def existing_contact_info_counts(database_path: str | Path) -> dict[str, int] | None:
    table_counts = stage_table_counts(
        database_path, (tables.COMPANY_CONTACT_INFO_TABLE,)
    )
    if table_counts is None:
        return None

    try:
        with duckdb.connect(str(database_path), read_only=True) as connection:
            email_domains = int(
                connection.execute(
                    f"""
                    select count(*)
                    from {_qualified_table_name(tables.COMPANY_CONTACT_INFO_TABLE)}
                    where domain_source = 'email'
                    """
                ).fetchone()[0]
            )
            companies_with_contacts = int(
                connection.execute(
                    f"""
                    select count(distinct cnpj_basico)
                    from {_qualified_table_name(tables.COMPANY_CONTACT_INFO_TABLE)}
                    """
                ).fetchone()[0]
            )
    except duckdb.Error:
        return None

    return {
        "contacts": table_counts[tables.COMPANY_CONTACT_INFO_TABLE],
        "email_domains": email_domains,
        "companies_with_contacts": companies_with_contacts,
    }


def existing_websites_counts(database_path: str | Path) -> dict[str, int] | None:
    table_counts = stage_table_counts(database_path, (tables.WEBSITES_TABLE,))
    if table_counts is None:
        return None

    try:
        with duckdb.connect(str(database_path), read_only=True) as connection:
            companies_with_websites = int(
                connection.execute(
                    f"""
                    select count(distinct cnpj_basico)
                    from {_qualified_table_name(tables.WEBSITES_TABLE)}
                    """
                ).fetchone()[0]
            )
    except duckdb.Error:
        return None

    return {
        "websites": table_counts[tables.WEBSITES_TABLE],
        "companies_with_websites": companies_with_websites,
    }
