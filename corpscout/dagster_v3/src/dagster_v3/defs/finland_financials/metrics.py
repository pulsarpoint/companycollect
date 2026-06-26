import csv
import hashlib
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

import duckdb

from dagster_v3.defs.finland_financials import tables
from dagster_v3.defs.xbrl_common.parser import parse_xbrl

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
METRICS_TABLE = tables.METRICS_TABLE
METRIC_NAMES = tables.FI_METRIC_NAMES
_RATE_REQUEST_BATCH = 50


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]: ...


def _request(currency: str, rate_date: str) -> Any:
    from exchange_rates import ExchangeRateRequest

    return ExchangeRateRequest(currency=currency, rate_date=rate_date)


def load_metric_map(path: Path = tables.METRIC_MAP_PATH) -> dict[tuple[str, str], str]:
    """The dimensional map: (concept_qname, mcy_member_code) -> canonical metric."""
    mapping: dict[tuple[str, str], str] = {}
    with open(path, encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            mapping[(row["concept_qname"], row["mcy_member_code"])] = row["metric_code"]
    return mapping


def discover_reports(
    *,
    registered_date_start: str,
    registered_date_end: str,
    max_reports: int,
    session: Any = None,
    base_url: str = tables.XBRL_BASE_URL,
    max_pages: int = 100,
) -> list[dict[str, str]]:
    """List eligible (businessId, financialDate) reports in a registration window."""
    from dlt.sources.helpers import requests as dlt_requests

    http_session = session or dlt_requests.Session()
    reports: list[dict[str, str]] = []
    for page in range(1, max_pages + 1):
        response = http_session.get(
            f"{base_url}/all_financial_statements",
            params={
                "registeredDateStart": registered_date_start,
                "registeredDateEnd": registered_date_end,
                "page": page,
            },
            timeout=120,
        )
        response.raise_for_status()
        items = (response.json() or {}).get("financials") or []
        if not items:
            break
        reports.extend(items)
        if len(reports) >= max_reports:
            break
    return reports[:max_reports]


def extract_statement_metrics(
    body: bytes, metric_map: dict[tuple[str, str], str]
) -> dict[str, Any] | None:
    """Parse one statement XML and map its dimensional facts to canonical metrics."""
    doc = parse_xbrl(body)
    period_end = doc.reporting_period_end
    if not doc.entity_id or period_end is None:
        return None
    period_start = None
    for ctx in doc.contexts.values():
        if ctx.end == period_end and ctx.start is not None:
            period_start = ctx.start
            break

    metrics: dict[str, Any] = {m: None for m in METRIC_NAMES}
    currency = ""
    numeric = 0
    mapped = 0
    for fact in doc.facts:
        ctx = doc.contexts.get(fact.context_ref)
        if ctx is None or fact.value is None or ctx.period_end != period_end:
            continue
        numeric += 1
        for member in ctx.dimensions.values():
            metric = metric_map.get((fact.qname, member))
            if metric and metric in metrics and metrics[metric] is None:
                metrics[metric] = fact.value
                mapped += 1
                if fact.unit_ref:
                    currency = "EUR" if "EUR" in fact.unit_ref else fact.unit_ref.split("_")[-1]
    if all(value is None for value in metrics.values()):
        return None
    return {
        "period_start": period_start,
        "period_end": period_end,
        "currency": currency or "EUR",
        "metrics": metrics,
        "numeric": numeric,
        "mapped": mapped,
    }


def build_finland_financials(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    registered_date_start: str,
    registered_date_end: str,
    max_reports: int = 200,
    session: Any = None,
    request_delay_seconds: float = 0.3,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Discover + fetch recent statements, parse with xbrl_common, build metrics."""
    import time

    from dlt.sources.helpers import requests as dlt_requests

    http_session = session or dlt_requests.Session()
    metric_map = load_metric_map()
    reports = discover_reports(
        registered_date_start=registered_date_start,
        registered_date_end=registered_date_end,
        max_reports=max_reports,
        session=http_session,
    )

    rows: list[tuple[Any, ...]] = []
    parsed = 0
    fetch_failed = 0
    no_metrics = 0
    for index, report in enumerate(reports):
        business_id = report.get("businessId")
        financial_date = report.get("financialDate")
        if not business_id or not financial_date:
            continue
        if index and request_delay_seconds:
            time.sleep(request_delay_seconds)  # be polite; avoid rate-limiting
        try:
            response = http_session.get(
                f"{tables.XBRL_BASE_URL}/financial",
                params={"businessId": business_id, "financialDate": financial_date},
                timeout=120,
            )
            response.raise_for_status()
            body = response.content
        except Exception as exc:  # noqa: BLE001 - count, don't hide
            fetch_failed += 1
            if log is not None:
                log("Finland statement fetch failed for %s/%s: %s", business_id, financial_date, exc)
            continue
        extracted = extract_statement_metrics(body, metric_map)
        if extracted is None:
            no_metrics += 1
            continue
        parsed += 1
        sha = hashlib.sha256(body).hexdigest()
        statement_key = hashlib.sha256(
            f"{business_id}|{financial_date}|{sha}".encode()
        ).hexdigest()
        rows.append((
            statement_key, business_id, financial_date,
            extracted["period_start"], extracted["period_end"], extracted["currency"],
            *(extracted["metrics"][m] for m in METRIC_NAMES),
            extracted["numeric"], extracted["mapped"],
            max(extracted["numeric"] - extracted["mapped"], 0),
            sha,
        ))

    counts = _write_metrics_table(connection, rows, source_run_id, log=log)
    counts["reports"] = len(reports)
    counts["parsed"] = parsed
    counts["fetch_failed"] = fetch_failed
    counts["no_metrics"] = no_metrics
    if log is not None:
        log(
            "Built Finland financial metrics: reports=%s parsed=%s fetch_failed=%s no_metrics=%s",
            len(reports), parsed, fetch_failed, no_metrics,
        )
    return counts


def _write_metrics_table(
    connection: duckdb.DuckDBPyConnection,
    rows: list[tuple[Any, ...]],
    source_run_id: str,
    *,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    amount_cols = ", ".join(f"{m}_amount_original decimal(38, 6)" for m in METRIC_NAMES)
    placeholders = ", ".join(["?"] * (6 + len(METRIC_NAMES) + 4))
    metric_cols_select = ", ".join(
        f"{m}_amount_original, cast(null as decimal(38, 6)) as {m}_amount_usd"
        for m in METRIC_NAMES
    )
    connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    connection.execute(
        f"create or replace temp table _fi_stage ("
        f"statement_key varchar, business_id varchar, financial_date date, "
        f"period_start date, period_end date, currency_original varchar, "
        f"{amount_cols}, source_fact_count bigint, mapped_fact_count bigint, "
        f"unmapped_numeric_fact_count bigint, source_payload_hash varchar)"
    )
    if rows:
        connection.executemany(f"insert into _fi_stage values ({placeholders})", rows)
    connection.execute(
        f"""
        create or replace table {qualified} as
        select
            statement_key, business_id, financial_date, period_start, period_end,
            currency_original,
            {metric_cols_select},
            cast(null as decimal(38, 6)) as employees,
            source_fact_count, mapped_fact_count, unmapped_numeric_fact_count,
            '' as metric_warnings,
            '{tables.MAPPING_VERSION}' as mapping_version,
            cast(null as decimal(38, 12)) as fx_rate_to_usd,
            cast(null as date) as fx_rate_date,
            cast(null as timestamp) as fx_converted_at,
            '{tables.SOURCE_SLUG}' as source_system,
            '{source_run_id}' as source_run_id,
            business_id as source_record_id,
            source_payload_hash,
            now() as resolved_at
        from _fi_stage
        """
    )
    statements = int(connection.execute(f"select count(*) from {qualified}").fetchone()[0])
    return {"statements": statements}


def _load_rates(exchange_rates: ExchangeRates, requests: list[Any]) -> dict[tuple[str, str], Any]:
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


def apply_finland_usd_conversion(
    *,
    connection: duckdb.DuckDBPyConnection,
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Fill *_usd + fx_* columns by currency→USD at each statement's period_end."""
    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    pairs = connection.execute(
        f"""
        select distinct upper(currency_original) as currency,
                        cast(period_end as varchar) as period_end
        from {qualified}
        where coalesce(currency_original, '') <> '' and period_end is not null
        """
    ).fetchall()
    requests = [_request(currency, end) for currency, end in pairs]
    rates = _load_rates(exchange_rates, requests)
    fx_rows = [
        (currency, end, rate.rate, str(rate.rate_date))
        for currency, end in pairs
        if (rate := rates.get((currency, end))) is not None
    ]
    reset_usd = ", ".join(f"{m}_amount_usd = NULL" for m in METRIC_NAMES)
    set_usd = ", ".join(
        f"{m}_amount_usd = cast({m}_amount_original * fx.fx_rate as decimal(38, 6))"
        for m in METRIC_NAMES
    )
    connection.execute(
        "create or replace temp table _fi_fx ("
        "currency varchar, period_end date, fx_rate decimal(38, 12), fx_rate_date date)"
    )
    if fx_rows:
        connection.executemany(
            "insert into _fi_fx values "
            "(?, cast(? as date), cast(? as decimal(38, 12)), cast(? as date))",
            fx_rows,
        )
    connection.execute(
        f"update {qualified} set fx_rate_to_usd = NULL, fx_rate_date = NULL, "
        f"fx_converted_at = NULL, {reset_usd}"
    )
    connection.execute(
        f"""
        update {qualified} as mt
        set fx_rate_to_usd = fx.fx_rate,
            fx_rate_date = fx.fx_rate_date,
            fx_converted_at = now(),
            {set_usd}
        from _fi_fx as fx
        where upper(mt.currency_original) = fx.currency and mt.period_end = fx.period_end
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
            "Applied Finland USD conversion: rate_pairs=%s rates_found=%s rows_converted=%s",
            counts["rate_pairs"], counts["rates_found"], counts["rows_converted"],
        )
    return counts
