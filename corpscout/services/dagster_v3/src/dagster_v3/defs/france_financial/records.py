"""Load and normalize France BCE/INPI financial ratios with DuckDB."""

import tempfile
from collections.abc import Callable
from pathlib import Path

from duckdb import DuckDBPyConnection

from dagster_v3.defs.france_financial import tables
from dagster_v3.defs.france_financial.resources import (
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_USER_AGENT,
    HttpSession,
    download_to_path,
)


def load_france_financial_parquet(
    *,
    duckdb_connection: DuckDBPyConnection,
    download_url: str = tables.PARQUET_EXPORT_URL,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    log: Callable[..., None] | None = None,
) -> int:
    """Download the full Parquet export and replace the raw DuckDB table."""
    with tempfile.TemporaryDirectory(prefix="france_financial_") as tmpdir:
        parquet_path = Path(tmpdir) / "ratios_inpi_bce.parquet"
        download_to_path(
            url=download_url,
            dest=parquet_path,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            session=session,
            log=log,
        )
        return load_parquet_path_into_raw_table(
            duckdb_connection=duckdb_connection,
            parquet_path=parquet_path,
            source_url=download_url,
        )


def load_parquet_path_into_raw_table(
    *,
    duckdb_connection: DuckDBPyConnection,
    parquet_path: Path,
    source_url: str,
) -> int:
    """Replace raw staging from a local Parquet file, preserving source values."""
    duckdb_connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
    selections = ", ".join(
        f"cast({column} as varchar) as {column}" for column in tables.RAW_SOURCE_COLUMNS
    )
    duckdb_connection.execute(
        f"""
        create or replace table {tables.DLT_DATASET_NAME}.{tables.RAW_TABLE} as
        select {selections}, cast(? as varchar) as source_url
        from read_parquet(?)
        """,
        [source_url, str(parquet_path)],
    )
    rows = int(
        duckdb_connection.execute(
            f"select count(*) from {tables.DLT_DATASET_NAME}.{tables.RAW_TABLE}"
        ).fetchone()[0]
    )
    if rows == 0:
        raise ValueError(
            "France financial Parquet produced zero rows; refusing to replace "
            f"{tables.DLT_DATASET_NAME}.{tables.RAW_TABLE}"
        )
    return rows


def build_france_financial_metrics(
    *,
    duckdb_connection: DuckDBPyConnection,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Build typed monetary metrics and ratios from the raw Parquet staging table."""
    raw_json_pairs = ", ".join(
        f"'{column}', {column}" for column in tables.RAW_SOURCE_COLUMNS
    )
    amount_select = ",\n            ".join(
        expression
        for metric, source_column in tables.MONETARY_SOURCE_COLUMNS.items()
        for expression in (
            f"try_cast({source_column} as decimal(38, 2)) as {metric}_amount_original",
            f"cast(null as decimal(38, 2)) as {metric}_amount_usd",
        )
    )
    ratio_select = ",\n            ".join(
        f"try_cast({source_column} as decimal(38, 6)) as {metric}"
        for metric, source_column in tables.RATIO_SOURCE_COLUMNS.items()
    )
    qualified_raw = f"{tables.DLT_DATASET_NAME}.{tables.RAW_TABLE}"
    qualified_metrics = f"{tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}"
    duckdb_connection.execute(
        f"""
        create or replace table {qualified_metrics} as
        with source as (
            select
                *,
                json_object({raw_json_pairs})::varchar as raw_financial_record
            from {qualified_raw}
        )
        select
            '{tables.COUNTRY_ISO2}' as country_iso2,
            '{tables.SOURCE_SLUG}' as source_slug,
            cast(? as varchar) as source_run_id,
            concat_ws(
                ':',
                coalesce(siren, ''),
                coalesce(date_cloture_exercice, ''),
                coalesce(type_bilan, '')
            ) as source_record_id,
            coalesce(siren, '') as siren,
            try_cast(date_cloture_exercice as date) as period_end_date,
            year(try_cast(date_cloture_exercice as date))::integer as fiscal_year,
            coalesce(type_bilan, '') as balance_type_code,
            'EUR' as currency,
            {amount_select},
            {ratio_select},
            coalesce(confidentiality, '') as confidentiality_status,
            cast(null as decimal(38, 12)) as fx_rate_to_usd,
            cast(null as date) as fx_rate_date,
            '' as fx_source,
            coalesce(source_url, '') as source_url,
            cast(now() as timestamp) as resolved_at,
            raw_financial_record,
            sha256(raw_financial_record) as source_payload_hash
        from source
        """,
        [source_run_id],
    )
    rows = int(
        duckdb_connection.execute(
            f"select count(*) from {qualified_metrics}"
        ).fetchone()[0]
    )
    if rows == 0:
        raise ValueError(
            "France financial normalization produced zero rows; refusing to "
            "replace downstream data"
        )
    duplicates = int(
        duckdb_connection.execute(
            f"""
            select count(*)
            from (
                select siren, period_end_date, balance_type_code
                from {qualified_metrics}
                group by all
                having count(*) > 1
            )
            """
        ).fetchone()[0]
    )
    counts = {
        "financial_metrics": rows,
        "duplicate_statement_keys": duplicates,
    }
    if log is not None:
        log(
            "Built France financial metrics: rows=%s duplicate_statement_keys=%s",
            rows,
            duplicates,
        )
    return counts
