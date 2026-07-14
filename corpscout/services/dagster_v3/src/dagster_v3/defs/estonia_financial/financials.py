from __future__ import annotations

import logging
import tempfile
from collections.abc import Callable
from pathlib import Path

from duckdb import DuckDBPyConnection

from dagster_v3.defs.estonia_ar import resources as ar_resources
from dagster_v3.defs.estonia_ar import tables
from dagster_v3.defs.estonia_financial.resources import EstoniaFinancialResource

LOGGER = logging.getLogger(__name__)

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
WIDE_TABLE = tables.FINANCIAL_STATEMENTS_WIDE_TABLE
REPORT_GENERAL_RAW_TABLE = tables.REPORT_GENERAL_RAW_TABLE
FINANCIALS_SOURCE_SLUG = "estonia_ar_financials"
DEFAULT_TIMEOUT_SECONDS = ar_resources.DEFAULT_TIMEOUT_SECONDS

# Report-general columns captured into raw_financial_record (DuckDB-staging-only).
# Spaced/`?` names are double-quoted for the SQL reference.
_RG_JSON_COLUMNS = (
    "report_id",
    "registrikood",
    '"õiguslik vorm"',
    "staatus",
    "aruandeaasta",
    '"kas konsolideeritud?"',
    "period_start",
    "period_end",
    "esitatud_kpv",
    '"kas auditeeritud?"',
    '"valitud aruanne kategooria"',
)


def _sql_literal(value: str) -> str:
    # Inlined (not a ? param) because the report-general column names contain a
    # literal '?' (e.g. "kas konsolideeritud?") that would collide with DuckDB
    # positional parameter markers.
    return "'" + str(value).replace("'", "''") + "'"


def load_estonia_ar_financial_csv(
    *,
    duckdb_connection: DuckDBPyConnection,
    download_url: str,
    raw_table: str,
    financial_resource: EstoniaFinancialResource | None = None,
    session: ar_resources.HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> int:
    """Download one zipped financial CSV, unzip, and (re)load it raw into DuckDB.

    all_varchar keeps every value as text losslessly; the pivot casts later.
    """
    with tempfile.TemporaryDirectory(prefix="estonia_ar_fin_") as tmpdir:
        tmp = Path(tmpdir)
        zip_path = tmp / "data.zip"
        source = financial_resource or EstoniaFinancialResource(
            session=session,
            timeout_seconds=timeout_seconds,
        )
        source.download_financial_zip(
            download_url=download_url,
            dest=zip_path,
        )
        csv_path = ar_resources._extract_single_csv(zip_path, tmp)
        duckdb_connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
        duckdb_connection.execute(
            f"create or replace table {DLT_DATASET_NAME}.{raw_table} as "
            "select * from read_csv(?, delim=';', header=true, all_varchar=true, "
            "quote='\"', escape='\"')",
            [str(csv_path)],
        )
        rows = duckdb_connection.execute(
            f"select count(*) from {DLT_DATASET_NAME}.{raw_table}"
        ).fetchone()[0]
    return int(rows)


def build_estonia_ar_financial_statements(
    *,
    duckdb_connection: DuckDBPyConnection,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Pivot the per-year EAV element tables into the wide financial_statements.

    Each report's headline elements (elemendi_nimetus) are conditional-aggregated
    into native-EUR columns keyed by report_id and joined to the report-general
    spine. Refuses to replace on empty input (a bad fetch can't blank the table).
    """
    elements_union = "\n            union all\n            ".join(
        f"select report_id, elemendi_nimetus, vaartus from {DLT_DATASET_NAME}.{raw}"
        for raw in tables.KEY_INDICATORS_RAW_TABLES
    )
    metric_pivot = ",\n            ".join(
        f"cast(null as decimal(38, 2)) as {name}"
        if element is None
        else (
            f"max(case when elemendi_nimetus = '{element}' "
            f"then try_cast(vaartus as decimal(38, 2)) end) as {name}"
        )
        for name, element in tables.EE_FINANCIAL_METRIC_ELEMENTS.items()
    )
    metric_select = ",\n        ".join(
        f"pivoted.{name}" for name in tables.FINANCIAL_METRIC_NAMES
    )
    json_pairs = ", ".join(
        f"'{col.strip(chr(34))}', {col}" for col in _RG_JSON_COLUMNS
    )
    report_category_en = "\n            ".join(
        f"when '{name}' then '{en}'"
        for name, en in ar_resources.EE_REPORT_CATEGORY_EN_BY_NAME.items()
    )

    sql = f"""
        create or replace table {DLT_DATASET_NAME}.{WIDE_TABLE} as
        with elements as (
            {elements_union}
        ),
        pivoted as (
            select
                report_id,
                {metric_pivot}
            from elements
            group by report_id
        ),
        spine as (
            select
                report_id,
                registrikood as reg_code,
                try_cast(aruandeaasta as integer) as fiscal_year,
                try_strptime(period_start, '%d.%m.%Y')::date as period_start_date,
                try_strptime(period_end, '%d.%m.%Y')::date as period_end_date,
                try_strptime(esitatud_kpv, '%d.%m.%Y')::date as submitted_date,
                case when "kas konsolideeritud?" = 'Jah' then 1 else 0 end as is_consolidated,
                case when "kas auditeeritud?" = 'Jah' then 1 else 0 end as is_audited,
                coalesce("valitud aruanne kategooria", '') as report_category_original,
                json_object({json_pairs})::varchar as raw_financial_record
            from {DLT_DATASET_NAME}.{REPORT_GENERAL_RAW_TABLE}
        )
        select
            'EE' as country_iso2,
            '{FINANCIALS_SOURCE_SLUG}' as source_slug,
            {_sql_literal(source_run_id)} as source_run_id,
            row_number() over (order by spine.report_id) as source_line_number,
            spine.report_id as source_record_id,
            sha256(spine.raw_financial_record) as source_payload_hash,
            spine.report_id,
            spine.reg_code,
            spine.fiscal_year,
            spine.period_start_date,
            spine.period_end_date,
            spine.submitted_date,
            spine.is_consolidated,
            spine.is_audited,
            spine.report_category_original,
            case spine.report_category_original
            {report_category_en}
            else '' end as report_category_en,
            'EUR' as currency,
            {metric_select},
            {_sql_literal(tables.REPORT_GENERAL_URL)} as source_url,
            spine.raw_financial_record
        from spine
        left join pivoted on pivoted.report_id = spine.report_id
    """

    spine_count = int(
        duckdb_connection.execute(
            f"select count(*) from {DLT_DATASET_NAME}.{REPORT_GENERAL_RAW_TABLE}"
        ).fetchone()[0]
    )
    if spine_count == 0:
        raise ValueError(
            "Estonia AR report_general_raw produced no rows; refusing to replace "
            "the wide financial_statements table"
        )
    duckdb_connection.execute(sql)
    statements = int(
        duckdb_connection.execute(
            f"select count(*) from {DLT_DATASET_NAME}.{WIDE_TABLE}"
        ).fetchone()[0]
    )
    with_metrics = int(
        duckdb_connection.execute(
            f"select count(*) from {DLT_DATASET_NAME}.{WIDE_TABLE} "
            "where net_result is not null or total_assets is not null"
        ).fetchone()[0]
    )

    counts = {"financial_statements": statements, "with_metrics": with_metrics}
    if log is not None:
        log(
            "Built Estonia AR wide financial statements: financial_statements=%s with_metrics=%s",
            counts["financial_statements"],
            counts["with_metrics"],
        )
    return counts
