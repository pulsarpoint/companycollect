import tempfile
from collections.abc import Callable
from pathlib import Path

import duckdb

from dagster_v3.defs.latvia_ur import tables
from dagster_v3.defs.latvia_ur.resources import (
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_USER_AGENT,
    HttpSession,
    _download_to_path,
)

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
WIDE_TABLE = tables.FINANCIAL_STATEMENTS_WIDE_TABLE
FINANCIALS_SOURCE_SLUG = "latvia_ur_financials"
FINANCIALS_SOURCE_URL = "https://data.gov.lv/dati/lv/dataset/gada-parskatu-finansu-dati"

# Raw financial_statements.csv metadata columns kept in the JSON evidence record.
_FS_RAW_COLUMNS = (
    "id",
    "file_id",
    "legal_entity_registration_number",
    "source_schema",
    "source_type",
    "year",
    "year_started_on",
    "year_ended_on",
    "employees",
    "rounded_to_nearest",
    "currency",
    "created_at",
)


def load_latvia_ur_financial_csv(
    *,
    database_path: str | Path,
    download_url: str,
    raw_table: str,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
) -> int:
    """Download one financial CSV and (re)load it into a DuckDB raw staging table.

    Uses DuckDB read_csv with all_varchar so the large flat files load fast and
    losslessly; the pivot step casts numerics. Each raw table is an independent
    checkpoint, so a failure in one file never re-downloads the others.
    """
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="latvia_ur_fin_") as tmpdir:
        csv_path = Path(tmpdir) / f"{raw_table}.csv"
        _download_to_path(
            url=download_url,
            dest=csv_path,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            session=session,
        )
        with duckdb.connect(str(database_path)) as connection:
            connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
            connection.execute(
                f"create or replace table {DLT_DATASET_NAME}.{raw_table} as "
                "select * from read_csv(?, delim=';', header=true, all_varchar=true, "
                "quote='\"', escape='\"')",
                [str(csv_path)],
            )
            count = connection.execute(
                f"select count(*) from {DLT_DATASET_NAME}.{raw_table}"
            ).fetchone()[0]
    return int(count)


def _numeric_casts(alias: str, columns: tuple[str, ...]) -> str:
    return ",\n        ".join(
        f"try_cast({alias}.{col} as decimal(38, 2)) as {col}" for col in columns
    )


def _orphan_count(connection: duckdb.DuckDBPyConnection, raw_table: str) -> int:
    return int(
        connection.execute(
            f"""
            select count(*)
            from (select distinct statement_id from {DLT_DATASET_NAME}.{raw_table}) part
            left join {DLT_DATASET_NAME}.{tables.FINANCIAL_STATEMENTS_RAW_TABLE} fs
                on fs.id = part.statement_id
            where fs.id is null
            """
        ).fetchone()[0]
    )


def build_latvia_ur_financial_statements(
    *,
    database_path: str | Path,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Pivot the four raw financial tables into the wide latvia_ur.financial_statements.

    financial_statements_raw is the spine; the other three are LEFT JOINed on
    statement_id. Orphan statement_ids present in a part but missing from the
    spine are counted (not dropped silently) and reported in the metadata.
    """
    json_pairs = ", ".join(f"'{col}', fs.{col}" for col in _FS_RAW_COLUMNS)
    balance = _numeric_casts("bs", tables.BALANCE_NUMERIC_COLUMNS)
    income = _numeric_casts("inc", tables.INCOME_NUMERIC_COLUMNS)
    cashflow = _numeric_casts("cf", tables.CASHFLOW_NUMERIC_COLUMNS)

    sql = f"""
    create or replace table {DLT_DATASET_NAME}.{WIDE_TABLE} as
    with spine as (
        select fs.*, json_object({json_pairs})::varchar as raw_financial_record
        from {DLT_DATASET_NAME}.{tables.FINANCIAL_STATEMENTS_RAW_TABLE} fs
    )
    select
        'LV' as country_iso2,
        '{FINANCIALS_SOURCE_SLUG}' as source_slug,
        ? as source_run_id,
        row_number() over (order by spine.id) as source_line_number,
        spine.id as source_record_id,
        sha256(spine.raw_financial_record) as source_payload_hash,
        spine.id as statement_id,
        spine.file_id as file_id,
        spine.legal_entity_registration_number as regcode,
        spine.source_schema as source_schema,
        -- source_type is NULL for ~35% of statements (pre-euro filings); the
        -- ClickHouse column is a non-nullable LowCardinality(String), so empty
        -- must be '' not NULL or the native-protocol insert hits None.encode().
        coalesce(spine.source_type, '') as source_type,
        try_cast(spine.year as integer) as fiscal_year,
        try_cast(spine.year_started_on as date) as period_start_date,
        try_cast(spine.year_ended_on as date) as period_end_date,
        try_cast(spine.employees as bigint) as employees,
        spine.rounded_to_nearest as rounded_to_nearest,
        spine.currency as currency,
        spine.created_at as created_at,
        {balance},
        {income},
        {cashflow},
        ? as source_url,
        spine.raw_financial_record as raw_financial_record
    from spine
    left join {DLT_DATASET_NAME}.{tables.BALANCE_SHEETS_RAW_TABLE} bs
        on bs.statement_id = spine.id
    left join {DLT_DATASET_NAME}.{tables.INCOME_STATEMENTS_RAW_TABLE} inc
        on inc.statement_id = spine.id
    left join {DLT_DATASET_NAME}.{tables.CASH_FLOW_STATEMENTS_RAW_TABLE} cf
        on cf.statement_id = spine.id
    """

    with duckdb.connect(str(database_path)) as connection:
        connection.execute(sql, [source_run_id, FINANCIALS_SOURCE_URL])
        statements = int(
            connection.execute(
                f"select count(*) from {DLT_DATASET_NAME}.{WIDE_TABLE}"
            ).fetchone()[0]
        )
        counts = {
            "financial_statements": statements,
            "balance_orphans": _orphan_count(connection, tables.BALANCE_SHEETS_RAW_TABLE),
            "income_orphans": _orphan_count(connection, tables.INCOME_STATEMENTS_RAW_TABLE),
            "cash_flow_orphans": _orphan_count(
                connection, tables.CASH_FLOW_STATEMENTS_RAW_TABLE
            ),
        }
    if log is not None:
        log(
            "Built Latvia UR wide financial statements: statements=%s, "
            "balance_orphans=%s, income_orphans=%s, cash_flow_orphans=%s",
            counts["financial_statements"],
            counts["balance_orphans"],
            counts["income_orphans"],
            counts["cash_flow_orphans"],
        )
    return counts
