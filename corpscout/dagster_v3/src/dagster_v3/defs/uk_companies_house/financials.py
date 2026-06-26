import re
import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

import duckdb

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.uk_companies_house import resources, tables
from dagster_v3.defs.xbrl_common.parser import parse_ixbrl

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
METRICS_TABLE = tables.FINANCIAL_METRICS_TABLE
METRIC_NAMES = tables.UK_FINANCIAL_METRIC_NAMES
FINANCIALS_SOURCE_SLUG = "uk_companies_house_accounts"
DEFAULT_CURRENCY = "GBP"
_RATE_REQUEST_BATCH = 50
_IXBRL_SUFFIXES = (".html", ".xhtml", ".xml")


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]: ...


def _request(currency: str, rate_date: str) -> Any:
    from exchange_rates import ExchangeRateRequest

    return ExchangeRateRequest(currency=currency, rate_date=rate_date)


def resolve_latest_accounts_archive_url(
    *,
    session: resources.HttpSession | None = None,
    index_url: str = tables.ACCOUNTS_INDEX_URL,
    base_url: str = tables.DOWNLOAD_BASE_URL,
    timeout_seconds: int = 60,
) -> str:
    """Resolve the most recent daily Accounts_Bulk_Data zip URL from the index."""
    from dlt.sources.helpers import requests as dlt_requests

    http_session = session or dlt_requests.Session()
    response = http_session.get(index_url, timeout=timeout_seconds)
    response.raise_for_status()
    matches = re.findall(tables.ACCOUNTS_FILENAME_RE, response.text)
    if not matches:
        raise LookupError(f"could not find Accounts_Bulk_Data zip on {index_url}")
    return base_url + sorted(set(matches))[-1]


def _extract_metrics(content: bytes) -> tuple[str, Any, dict[str, Any]] | None:
    """Parse one iXBRL filing → (company_number, period_end, {metric: Decimal|None})."""
    try:
        doc = parse_ixbrl(content)
    except Exception:
        return None
    company_number = doc.entity_id
    period_end = doc.reporting_period_end
    if not company_number or period_end is None:
        return None
    metrics = {
        metric: doc.metric(concepts)
        for metric, concepts in tables.UK_METRIC_CONCEPTS.items()
    }
    if all(value is None for value in metrics.values()):
        return None
    return company_number.strip(), period_end, metrics


def build_uk_financials_from_archive(
    *,
    connection: duckdb.DuckDBPyConnection,
    archive_path: str | Path,
    source_run_id: str,
    source_url: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Parse every iXBRL filing in an accounts archive into native-GBP metrics."""
    rows, parsed = parse_archive_rows(archive_path)
    counts = write_metrics_table(
        connection=connection,
        rows=rows,
        source_run_id=source_run_id,
        source_slug=FINANCIALS_SOURCE_SLUG,
    )
    counts["filings_parsed"] = parsed
    if log is not None:
        log(
            "Built UK financial metrics: filings=%s companies=%s with_revenue=%s",
            parsed,
            counts["companies"],
            counts["with_revenue"],
        )
    return counts


def parse_archive_rows(archive_path: str | Path) -> tuple[list[tuple[Any, ...]], int]:
    """Parse every iXBRL filing in one accounts archive → (metric rows, parsed count)."""
    rows: list[tuple[Any, ...]] = []
    parsed = 0
    with zipfile.ZipFile(archive_path) as archive:
        members = [
            n for n in archive.namelist() if n.lower().endswith(_IXBRL_SUFFIXES)
        ]
        for name in members:
            extracted = _extract_metrics(archive.read(name))
            if extracted is None:
                continue
            parsed += 1
            company_number, period_end, metrics = extracted
            rows.append(metrics_row(company_number, period_end, metrics))
    return rows, parsed


def metrics_row(company_number: str, period_end: Any, metrics: dict[str, Any]) -> tuple[Any, ...]:
    """One staging row: (company_number, period_end, fiscal_year, currency, *originals)."""
    return (
        company_number,
        period_end,
        period_end.year,
        DEFAULT_CURRENCY,
        *(metrics[m] for m in METRIC_NAMES),
    )


def write_metrics_table(
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
    database_path: str | Path | None = None,
    rows: list[tuple[Any, ...]],
    source_run_id: str,
    source_slug: str,
    allow_empty: bool = False,
) -> dict[str, int]:
    """Write staging metric rows into the DuckDB metrics table (latest period/company)."""
    if connection is None:
        if database_path is None:
            raise ValueError("write_metrics_table requires connection or database_path")
        database_file = Path(database_path)
        database_file.parent.mkdir(parents=True, exist_ok=True)
        with duckdb_resource(database_file).get_connection() as managed_connection:
            return write_metrics_table(
                connection=managed_connection,
                rows=rows,
                source_run_id=source_run_id,
                source_slug=source_slug,
                allow_empty=allow_empty,
            )

    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    amount_cols = ", ".join(f"{m}_amount_original decimal(38, 2)" for m in METRIC_NAMES)
    placeholders = ", ".join(["?"] * (4 + len(METRIC_NAMES)))
    metric_cols_select = ", ".join(
        f"{m}_amount_original, cast(null as decimal(38, 2)) as {m}_amount_usd"
        for m in METRIC_NAMES
    )
    connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    connection.execute(
        f"create or replace temp table _gb_stage ("
        f"company_number varchar, period_end date, fiscal_year integer, "
        f"currency varchar, {amount_cols})"
    )
    if rows:
        connection.executemany(
            f"insert into _gb_stage values ({placeholders})", rows
        )
    connection.execute(
        f"""
        create or replace table {qualified} as
        with ranked as (
            select *, row_number() over (
                partition by company_number order by period_end desc
            ) as rn
            from _gb_stage
        )
        select
            'GB' as country_iso2,
            {resources._sql_literal(source_slug)} as source_slug,
            {resources._sql_literal(source_run_id)} as source_run_id,
            company_number as source_record_id,
            company_number,
            period_end as period_end_date,
            fiscal_year,
            currency,
            {metric_cols_select},
            cast(null as decimal(38, 12)) as fx_rate_to_usd,
            cast(null as date) as fx_rate_date,
            '' as fx_source,
            now() as resolved_at
        from ranked where rn = 1
        """
    )
    company_rows = int(
        connection.execute(f"select count(*) from {qualified}").fetchone()[0]
    )
    with_revenue = int(
        connection.execute(
            f"select count(*) from {qualified} where revenue_amount_original is not null"
        ).fetchone()[0]
    )
    if company_rows == 0 and not allow_empty:
        raise ValueError(
            "UK Companies House accounts produced no metrics; refusing to replace the table"
        )
    return {"companies": company_rows, "with_revenue": with_revenue}


def build_uk_companies_house_financials(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    download_url: str | None = None,
    session: resources.HttpSession | None = None,
    timeout_seconds: int = resources.DEFAULT_TIMEOUT_SECONDS,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Download the latest accounts archive and build native-GBP metrics."""
    url = download_url or resolve_latest_accounts_archive_url(session=session)
    with tempfile.TemporaryDirectory(prefix="uk_accounts_") as tmpdir:
        archive_path = Path(tmpdir) / "accounts.zip"
        resources._download_to_path(
            url=url,
            dest=archive_path,
            timeout_seconds=timeout_seconds,
            session=session,
            log=log if callable(log) else None,
        )
        return build_uk_financials_from_archive(
            connection=connection,
            archive_path=archive_path,
            source_run_id=source_run_id,
            source_url=url,
            log=log,
        )


def _load_rates(
    exchange_rates: ExchangeRates, requests: list[Any]
) -> dict[tuple[str, str], Any]:
    rates: dict[tuple[str, str], Any] = {}
    for start in range(0, len(requests), _RATE_REQUEST_BATCH):
        batch = requests[start : start + _RATE_REQUEST_BATCH]
        try:
            rates.update(exchange_rates.usd_rates(batch))
        except LookupError:
            for request in batch:
                try:
                    rates.update(exchange_rates.usd_rates([request]))
                except LookupError:
                    continue
    return rates


def apply_uk_usd_conversion(
    *,
    connection: duckdb.DuckDBPyConnection,
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Fill *_usd + fx_* columns by GBP→USD at each filing's period_end_date."""
    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    pairs = connection.execute(
        f"""
        select distinct upper(currency) as currency,
                        cast(period_end_date as varchar) as period_end
        from {qualified}
        where coalesce(currency, '') <> '' and period_end_date is not null
        """
    ).fetchall()

    requests = [_request(currency, end) for currency, end in pairs]
    rates = _load_rates(exchange_rates, requests)
    fx_rows = [
        (currency, end, rate.rate, str(rate.rate_date), rate.source)
        for currency, end in pairs
        if (rate := rates.get((currency, end))) is not None
    ]

    reset_usd = ", ".join(f"{m}_amount_usd = NULL" for m in METRIC_NAMES)
    set_usd = ", ".join(
        f"{m}_amount_usd = cast({m}_amount_original * fx.fx_rate as decimal(38, 2))"
        for m in METRIC_NAMES
    )
    connection.execute(
        "create or replace temp table _gb_fx ("
        "currency varchar, period_end date, fx_rate decimal(38, 12), "
        "fx_rate_date date, fx_source varchar)"
    )
    if fx_rows:
        connection.executemany(
            "insert into _gb_fx values "
            "(?, cast(? as date), cast(? as decimal(38, 12)), cast(? as date), ?)",
            fx_rows,
        )
    connection.execute(
        f"update {qualified} set fx_rate_to_usd = NULL, fx_rate_date = NULL, "
        f"fx_source = '', {reset_usd}"
    )
    connection.execute(
        f"""
        update {qualified} as mt
        set fx_rate_to_usd = fx.fx_rate,
            fx_rate_date = fx.fx_rate_date,
            fx_source = fx.fx_source,
            {set_usd}
        from _gb_fx as fx
        where upper(mt.currency) = fx.currency
          and mt.period_end_date = fx.period_end
        """
    )
    converted = int(
        connection.execute(
            f"select count(*) from {qualified} where fx_rate_to_usd is not null"
        ).fetchone()[0]
    )
    counts = {"rate_pairs": len(pairs), "rates_found": len(fx_rows), "rows_converted": converted}
    if log is not None:
        log(
            "Applied UK USD conversion: rate_pairs=%s rates_found=%s rows_converted=%s",
            counts["rate_pairs"],
            counts["rates_found"],
            counts["rows_converted"],
        )
    return counts
