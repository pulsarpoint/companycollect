import re
import tempfile
import zipfile
from collections.abc import Callable, Iterable, Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

import duckdb
import pyarrow as pa

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.uk_companies_house import raw_archives, resources, tables
from dagster_v3.defs.xbrl_common.parser import parse_ixbrl

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
METRICS_TABLE = tables.FINANCIAL_METRICS_TABLE
METRIC_NAMES = tables.UK_FINANCIAL_METRIC_NAMES
FINANCIALS_SOURCE_SLUG = "uk_companies_house_accounts"
DEFAULT_CURRENCY = "GBP"
_RATE_REQUEST_BATCH = 50
_IXBRL_SUFFIXES = (".html", ".xhtml", ".xml")
DEFAULT_INSERT_BATCH_ROWS = 50_000
_METRICS_STAGE_TABLE = "_gb_stage"
_METRICS_BATCH_RELATION = "_gb_metrics_batch"
_FX_BATCH_RELATION = "_gb_fx_batch"
_METRICS_STAGE_COLUMNS = (
    "company_number",
    "period_end",
    "fiscal_year",
    "currency",
    *(f"{metric}_amount_original" for metric in METRIC_NAMES),
)
_METRICS_ARROW_SCHEMA = pa.schema(
    [
        pa.field("company_number", pa.string(), nullable=False),
        pa.field("period_end", pa.date32(), nullable=False),
        pa.field("fiscal_year", pa.int32(), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        *(
            pa.field(f"{metric}_amount_original", pa.string())
            for metric in METRIC_NAMES
        ),
    ]
)
_FX_ARROW_SCHEMA = pa.schema(
    [
        pa.field("currency", pa.string(), nullable=False),
        pa.field("period_end", pa.date32(), nullable=False),
        pa.field("fx_rate", pa.decimal128(38, 12), nullable=False),
        pa.field("fx_rate_date", pa.date32(), nullable=False),
        pa.field("fx_source", pa.string(), nullable=False),
    ]
)


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
    create_metrics_stage_table(connection)
    parsed = append_metrics_rows(
        connection=connection,
        rows=iter_archive_rows(archive_path),
    )
    counts = replace_metrics_table_from_stage(
        connection=connection,
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


def iter_archive_rows(archive_path: str | Path) -> Iterator[tuple[Any, ...]]:
    """Yield native-currency metric rows from one iXBRL accounts archive."""
    with zipfile.ZipFile(archive_path) as archive:
        for member in archive.infolist():
            if member.is_dir() or not member.filename.lower().endswith(_IXBRL_SUFFIXES):
                continue
            extracted = _extract_metrics(archive.read(member))
            if extracted is None:
                continue
            company_number, period_end, metrics = extracted
            yield metrics_row(company_number, period_end, metrics)


def metrics_row(
    company_number: str, period_end: Any, metrics: dict[str, Any]
) -> tuple[Any, ...]:
    """One staging row: (company_number, period_end, fiscal_year, currency, *originals)."""
    return (
        company_number,
        period_end,
        period_end.year,
        DEFAULT_CURRENCY,
        *(metrics[m] for m in METRIC_NAMES),
    )


def create_metrics_stage_table(connection: duckdb.DuckDBPyConnection) -> None:
    amount_cols = ", ".join(
        f"{metric}_amount_original decimal(38, 2)" for metric in METRIC_NAMES
    )
    connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    connection.execute(
        f"create or replace temp table {_METRICS_STAGE_TABLE} ("
        f"company_number varchar, period_end date, fiscal_year integer, "
        f"currency varchar, {amount_cols})"
    )


def append_metrics_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    rows: Iterable[tuple[Any, ...]],
    batch_rows: int = DEFAULT_INSERT_BATCH_ROWS,
) -> int:
    """Stream metric rows into the current staging table in bounded Arrow batches."""
    if batch_rows < 1:
        raise ValueError("UK accounts insert batch size must be greater than zero")

    batch: list[tuple[Any, ...]] = []
    inserted = 0
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_rows:
            _insert_metrics_batch(connection, batch)
            inserted += len(batch)
            batch.clear()
    if batch:
        _insert_metrics_batch(connection, batch)
        inserted += len(batch)
    return inserted


def _insert_metrics_batch(
    connection: duckdb.DuckDBPyConnection,
    rows: list[tuple[Any, ...]],
) -> None:
    expected_columns = len(_METRICS_STAGE_COLUMNS)
    if any(len(row) != expected_columns for row in rows):
        raise ValueError(
            f"UK accounts metric rows must contain {expected_columns} columns"
        )

    arrow_table = pa.Table.from_arrays(
        [
            pa.array((row[0] for row in rows), type=pa.string()),
            pa.array((row[1] for row in rows), type=pa.date32()),
            pa.array((row[2] for row in rows), type=pa.int32()),
            pa.array((row[3] for row in rows), type=pa.string()),
            *(
                pa.array(
                    (None if row[index] is None else str(row[index]) for row in rows),
                    type=pa.string(),
                )
                for index in range(4, expected_columns)
            ),
        ],
        schema=_METRICS_ARROW_SCHEMA,
    )
    column_list = ", ".join(_METRICS_STAGE_COLUMNS)
    select_list = ", ".join(
        (
            *_METRICS_STAGE_COLUMNS[:4],
            *(
                f"cast({column} as decimal(38, 2)) as {column}"
                for column in _METRICS_STAGE_COLUMNS[4:]
            ),
        )
    )
    connection.register(_METRICS_BATCH_RELATION, arrow_table)
    try:
        connection.execute(
            f"insert into {_METRICS_STAGE_TABLE} ({column_list}) "
            f"select {select_list} from {_METRICS_BATCH_RELATION}"
        )
    finally:
        connection.unregister(_METRICS_BATCH_RELATION)


def replace_metrics_table_from_stage(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    source_slug: str,
    allow_empty: bool = False,
) -> dict[str, int]:
    """Atomically replace metrics with the latest staged period per company."""
    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    metric_cols_select = ", ".join(
        f"{m}_amount_original, cast(null as decimal(38, 2)) as {m}_amount_usd"
        for m in METRIC_NAMES
    )
    staged_rows = int(
        connection.execute(f"select count(*) from {_METRICS_STAGE_TABLE}").fetchone()[0]
    )
    if staged_rows == 0 and not allow_empty:
        raise ValueError(
            "UK Companies House accounts produced no metrics; refusing to replace the table"
        )

    connection.execute("begin transaction")
    try:
        connection.execute(
            f"""
            create or replace table {qualified} as
            with ranked as (
                select *, row_number() over (
                    partition by company_number order by period_end desc
                ) as rn
                from {_METRICS_STAGE_TABLE}
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
                f"select count(*) from {qualified} "
                "where revenue_amount_original is not null"
            ).fetchone()[0]
        )
    except Exception:
        connection.execute("rollback")
        raise
    connection.execute("commit")
    return {"companies": company_rows, "with_revenue": with_revenue}


def write_metrics_table(
    *,
    connection: duckdb.DuckDBPyConnection,
    rows: Iterable[tuple[Any, ...]],
    source_run_id: str,
    source_slug: str,
    allow_empty: bool = False,
    batch_rows: int = DEFAULT_INSERT_BATCH_ROWS,
) -> dict[str, int]:
    """Stream rows into staging and atomically publish the latest company periods."""
    create_metrics_stage_table(connection)
    append_metrics_rows(
        connection=connection,
        rows=rows,
        batch_rows=batch_rows,
    )
    return replace_metrics_table_from_stage(
        connection=connection,
        source_run_id=source_run_id,
        source_slug=source_slug,
        allow_empty=allow_empty,
    )


def build_uk_companies_house_financials(
    *,
    connection: duckdb.DuckDBPyConnection,
    object_store: ObjectStoreResource,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Build native-GBP metrics from the latest accounts archive in object storage."""
    archive = raw_archives.latest_stored_archive(
        object_store,
        kind=raw_archives.ACCOUNTS_KIND,
    )
    with tempfile.TemporaryDirectory(prefix="uk_accounts_") as tmpdir:
        archive_path = Path(tmpdir) / archive.filename
        object_store.download_file(
            archive.object_key,
            archive_path,
            bucket=raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET,
        )
        return build_uk_financials_from_archive(
            connection=connection,
            archive_path=archive_path,
            source_run_id=source_run_id,
            source_url=archive.source_url,
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
        (
            currency,
            date.fromisoformat(end),
            Decimal(str(rate.rate)),
            date.fromisoformat(str(rate.rate_date)),
            rate.source,
        )
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
        fx_table = pa.Table.from_pylist(
            [dict(zip(_FX_ARROW_SCHEMA.names, row, strict=True)) for row in fx_rows],
            schema=_FX_ARROW_SCHEMA,
        )
        connection.register(_FX_BATCH_RELATION, fx_table)
        try:
            connection.execute(
                "insert into _gb_fx "
                "select currency, period_end, fx_rate, fx_rate_date, fx_source "
                f"from {_FX_BATCH_RELATION}"
            )
        finally:
            connection.unregister(_FX_BATCH_RELATION)
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
    counts = {
        "rate_pairs": len(pairs),
        "rates_found": len(fx_rows),
        "rows_converted": converted,
    }
    if log is not None:
        log(
            "Applied UK USD conversion: rate_pairs=%s rates_found=%s rows_converted=%s",
            counts["rate_pairs"],
            counts["rates_found"],
            counts["rows_converted"],
        )
    return counts
