"""Load and transform the Verohallinto public tax CSVs in DuckDB."""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection

from dagster_v3.defs.finland_verotax import tables
from dagster_v3.defs.finland_verotax.resources import (
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_USER_AGENT,
    HttpSession,
    download_to_path,
)

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
CSV_ENCODING = "latin-1"


def load_finland_verotax_csv(
    *,
    duckdb_connection: DuckDBPyConnection,
    download_url: str,
    raw_table: str,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
) -> int:
    """Download one tax-year CSV and replace its DuckDB raw staging table."""
    with tempfile.TemporaryDirectory(prefix="finland_verotax_") as tmpdir:
        csv_path = Path(tmpdir) / f"{raw_table}.csv"
        download_to_path(
            url=download_url,
            dest=csv_path,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            session=session,
        )
        return load_csv_path_into_raw_table(
            duckdb_connection=duckdb_connection,
            csv_path=csv_path,
            raw_table=raw_table,
            source_url=download_url,
        )


def load_csv_path_into_raw_table(
    *,
    duckdb_connection: DuckDBPyConnection,
    csv_path: Path,
    raw_table: str,
    source_url: str,
) -> int:
    """Replace a raw staging table from a downloaded Latin-1 CSV file.

    The bilingual FI|SV header row is skipped and positional names are applied
    instead; the column count decides between the 8-column (2023+) and 9-column
    (2020-2022, extra prepayments) shapes.
    """
    names = _raw_columns_for_header(csv_path)
    names_sql = ", ".join(f"'{name}'" for name in names)
    duckdb_connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    try:
        # quote='' because the files never CSV-quote fields, but ~150 company
        # names contain literal '"' (e.g. 'UAB "Tokajus"') which breaks the
        # sniffer in quoted mode.
        duckdb_connection.execute(
            f"create or replace table {DLT_DATASET_NAME}.{raw_table} as "
            f"select *, cast(? as varchar) as source_url from read_csv(?, delim=';', "
            f"header=false, skip=1, all_varchar=true, quote='', escape='', "
            f"encoding='{CSV_ENCODING}', names=[{names_sql}])",
            [source_url, str(csv_path)],
        )
    except duckdb.InvalidInputException as exc:
        # A header-only file fails the CSV sniffer instead of yielding 0 rows.
        raise ValueError(
            f"Finland verotax CSV produced zero rows: url={source_url} "
            f"table={DLT_DATASET_NAME}.{raw_table}"
        ) from exc
    count = int(
        duckdb_connection.execute(
            f"select count(*) from {DLT_DATASET_NAME}.{raw_table}"
        ).fetchone()[0]
    )
    if count == 0:
        raise ValueError(
            f"Finland verotax CSV produced zero rows: url={source_url} "
            f"table={DLT_DATASET_NAME}.{raw_table}"
        )
    return count


def _raw_columns_for_header(csv_path: Path) -> tuple[str, ...]:
    with csv_path.open(encoding=CSV_ENCODING) as handle:
        header = handle.readline()
    column_count = header.count(";") + 1
    if column_count == len(tables.RAW_COLUMNS_8):
        return tables.RAW_COLUMNS_8
    if column_count == len(tables.RAW_COLUMNS_9):
        return tables.RAW_COLUMNS_9
    raise ValueError(
        f"Finland verotax CSV has unexpected column count {column_count} "
        f"(expected 8 or 9): {csv_path.name}"
    )


def _amount_expr(column: str, *, present: bool) -> str:
    if not present:
        return f"cast(null as decimal(38, 2)) as {column}_amount_original"
    return (
        f"try_cast(replace(replace(replace({column}, ' ', ''), chr(160), ''), "
        f"',', '.') as decimal(38, 2)) as {column}_amount_original"
    )


def _year_select(raw_table: str, raw_columns: tuple[str, ...]) -> str:
    metric_exprs = ",\n            ".join(
        expr
        for metric in tables.TAX_METRIC_NAMES
        for expr in (
            _amount_expr(metric, present=metric in raw_columns),
            f"cast(null as decimal(38, 2)) as {metric}_amount_usd",
        )
    )
    json_pairs = ", ".join(f"'{col}', {col}" for col in raw_columns)
    return f"""
        select
            '{tables.COUNTRY_ISO2}' as country_iso2,
            '{tables.SOURCE_SLUG}' as source_slug,
            cast(? as varchar) as source_run_id,
            trim(business_id) || ':' || trim(tax_year) as source_record_id,
            trim(business_id) as business_id,
            coalesce(trim(taxpayer_name), '') as taxpayer_name,
            coalesce(regexp_extract(trim(municipality_raw), '^([0-9]+)\\s+', 1), '')
                as municipality_code,
            coalesce(nullif(regexp_extract(trim(municipality_raw), '^[0-9]+\\s+(.*)$', 1), ''),
                     trim(municipality_raw), '') as municipality_name,
            try_cast(tax_year as integer) as tax_year,
            make_date(try_cast(tax_year as integer), 12, 31) as period_end_date,
            'EUR' as currency,
            {metric_exprs},
            cast(null as decimal(38, 12)) as fx_rate_to_usd,
            cast(null as date) as fx_rate_date,
            '' as fx_source,
            source_url,
            cast(now() as timestamp) as resolved_at,
            json_object({json_pairs})::varchar as raw_tax_record,
            sha256(json_object({json_pairs})::varchar) as source_payload_hash
        from {DLT_DATASET_NAME}.{raw_table}
        where coalesce(trim(business_id), '') <> ''
    """


def build_finland_verotax_tax_records(
    *,
    duckdb_connection: DuckDBPyConnection,
    source_run_id: str,
    years: tuple[int, ...] = tables.EXPECTED_YEARS,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Union the per-year raw tables into the typed tax_records table."""
    selects = []
    for year in years:
        raw_table = tables.raw_table_for_year(year)
        raw_columns = _raw_table_columns(duckdb_connection, raw_table)
        selects.append(_year_select(raw_table, raw_columns))
    union_sql = "\n        union all by name\n".join(selects)
    qualified = f"{DLT_DATASET_NAME}.{tables.TAX_RECORDS_TABLE}"
    duckdb_connection.execute(
        f"create or replace table {qualified} as {union_sql}",
        [source_run_id] * len(selects),
    )
    records = int(
        duckdb_connection.execute(f"select count(*) from {qualified}").fetchone()[0]
    )
    duplicate_keys = int(
        duckdb_connection.execute(
            f"""
            select count(*) from (
                select business_id, tax_year
                from {qualified}
                group by business_id, tax_year
                having count(*) > 1
            )
            """
        ).fetchone()[0]
    )
    counts = {"tax_records": records, "duplicate_business_year_keys": duplicate_keys}
    if log is not None:
        log(
            "Built Finland verotax tax records: records=%s duplicate_keys=%s",
            records,
            duplicate_keys,
        )
    return counts


def _raw_table_columns(
    duckdb_connection: DuckDBPyConnection, raw_table: str
) -> tuple[str, ...]:
    rows = duckdb_connection.execute(
        """
        select column_name from information_schema.columns
        where table_schema = ? and table_name = ?
        order by ordinal_position
        """,
        [DLT_DATASET_NAME, raw_table],
    ).fetchall()
    if not rows:
        raise ValueError(
            f"Finland verotax raw table missing: {DLT_DATASET_NAME}.{raw_table} "
            "(materialize the raw download assets first)"
        )
    return tuple(row[0] for row in rows)
