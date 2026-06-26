import time
from collections.abc import Callable
from typing import Any, Protocol

import duckdb

from dagster_v3.defs.slovakia_financials import tables
from dagster_v3.defs.slovakia_financials.client import RuzClient

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
METRICS_TABLE = tables.METRICS_TABLE
METRIC_NAMES = tables.SK_METRIC_NAMES
_RATE_REQUEST_BATCH = 50

# Metric → (table index, cisloRiadku) per statutory template family. table 0 =
# Strana aktív (assets), 1 = Strana pasív (equity+liabilities), 2 = Výkaz ziskov
# a strát (P&L). Row numbers (cisloRiadku) are stable within a form family.
# Verified against live Úč MUJ (idSablony 687) and Úč POD (699) templates.
TEMPLATE_METRIC_ROWS: dict[str, dict[str, tuple[int, int]]] = {
    "Úč MUJ": {  # micro entity
        "total_assets": (0, 1),
        "equity": (1, 25),
        "liabilities": (1, 34),
        "revenue": (2, 1),  # Výnosy z hospodárskej činnosti spolu
        "pretax_result": (2, 35),
        "net_result": (2, 38),
    },
    "Úč POD": {  # businesses (double-entry)
        "total_assets": (0, 1),
        "equity": (1, 80),
        "liabilities": (1, 101),
        "revenue": (2, 1),  # Čistý obrat
        "pretax_result": (2, 56),
        "net_result": (2, 61),
    },
}


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]: ...


def _request(currency: str, rate_date: str) -> Any:
    from exchange_rates import ExchangeRateRequest

    return ExchangeRateRequest(currency=currency, rate_date=rate_date)


def template_family(template_name: str | None) -> str | None:
    """Map a template `nazov` (e.g. 'Úč POD v.2014') to a known family prefix."""
    if not template_name:
        return None
    for family in TEMPLATE_METRIC_ROWS:
        if template_name.startswith(family):
            return family
    return None


def _to_number(raw: Any) -> float | None:
    text = str(raw or "").strip()
    if text == "":
        return None
    try:
        return float(text.replace(" ", "").replace(",", "."))
    except ValueError:
        return None


def _cell(
    report_tabulky: list[dict[str, Any]],
    template_tabulky: list[dict[str, Any]],
    table_idx: int,
    cislo_riadku: int,
) -> float | None:
    """Current-period net value at (table, cisloRiadku).

    `data[]` length = rows × columns; the last-but-one column is the current
    period's net value (2-col tables → index 0; the Úč POD 4-col assets table →
    index 2, i.e. ncol-2 in both cases). The previous period is ncol-1.
    """
    if table_idx >= len(report_tabulky) or table_idx >= len(template_tabulky):
        return None
    data = report_tabulky[table_idx].get("data") or []
    rows = template_tabulky[table_idx].get("riadky") or []
    if not rows:
        return None
    ncol = len(data) // len(rows)
    if ncol < 2:
        return None
    for row_index, row in enumerate(rows):
        if row.get("cisloRiadku") == cislo_riadku:
            cell_index = row_index * ncol + (ncol - 2)
            if 0 <= cell_index < len(data):
                return _to_number(data[cell_index])
            return None
    return None


def extract_report_metrics(
    report_obsah: dict[str, Any], template: dict[str, Any]
) -> tuple[dict[str, float | None] | None, str | None]:
    """Extract canonical metrics from one report's tables. Returns (metrics, family)."""
    family = template_family(template.get("nazov"))
    if family is None:
        return None, None
    report_tabulky = report_obsah.get("tabulky") or []
    template_tabulky = template.get("tabulky") or []
    if not report_tabulky:
        return None, family
    metrics = {
        metric: _cell(report_tabulky, template_tabulky, table_idx, cislo)
        for metric, (table_idx, cislo) in TEMPLATE_METRIC_ROWS[family].items()
    }
    return metrics, family


def _period_dates(statement: dict[str, Any]) -> tuple[str | None, str | None, int | None]:
    period_end = statement.get("datumZostaveniaK") or None
    obdobie_od = statement.get("obdobieOd")
    period_start = f"{obdobie_od}-01" if obdobie_od else None
    fiscal_year = None
    if period_end and len(period_end) >= 4 and period_end[:4].isdigit():
        fiscal_year = int(period_end[:4])
    elif statement.get("obdobieDo"):
        head = str(statement["obdobieDo"])[:4]
        fiscal_year = int(head) if head.isdigit() else None
    return period_start, period_end, fiscal_year


def process_statement(
    client: RuzClient,
    statement_id: int,
    *,
    entity_cache: dict[int, dict[str, Any]],
    template_cache: dict[int, dict[str, Any]],
) -> dict[str, Any] | None:
    """Fetch a statement + its reports, resolve entity, extract merged metrics.

    Returns a flat row dict, or None to skip (deleted / no usable financial data).
    """
    statement = client.statement(statement_id)
    if statement.get("stav") == "ZMAZANÉ":
        return None
    entity_id = statement.get("idUJ")
    ico = ""
    if entity_id is not None:
        entity = entity_cache.get(entity_id)
        if entity is None:
            entity = client.entity(entity_id)
            entity_cache[entity_id] = entity
        ico = str(entity.get("ico") or "")

    merged: dict[str, float | None] = {metric: None for metric in METRIC_NAMES}
    template_name = ""
    mapped = False
    for report_id in statement.get("idUctovnychVykazov") or []:
        report = client.report(report_id)
        if report.get("pristupnostDat") != "Verejné":
            continue
        obsah = report.get("obsah") or {}
        if "tabulky" not in obsah:
            continue
        template_id = report.get("idSablony")
        if template_id is None:
            continue
        template = template_cache.get(template_id)
        if template is None:
            template = client.template(template_id)
            template_cache[template_id] = template
        metrics, family = extract_report_metrics(obsah, template)
        if metrics is None:
            continue
        mapped = True
        template_name = str(template.get("nazov") or family or "")
        for metric, value in metrics.items():
            if merged[metric] is None and value is not None:
                merged[metric] = value

    period_start, period_end, fiscal_year = _period_dates(statement)
    return {
        "statement_id": statement_id,
        "ruz_entity_id": str(entity_id) if entity_id is not None else "",
        "ico": ico,
        "template_name": template_name,
        "statement_type": str(statement.get("typ") or ""),
        "fiscal_year": fiscal_year,
        "period_start": period_start,
        "period_end": period_end,
        "filed_date": statement.get("datumPodania"),
        "approved_date": statement.get("datumSchvalenia"),
        "currency": "EUR",
        "metrics": merged,
        "mapped_metric_count": sum(1 for v in merged.values() if v is not None),
        "template_mapped": 1 if mapped else 0,
    }


def build_slovakia_financials(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    after_id: int,
    max_statements: int,
    changed_since: str = tables.RUZ_CHANGED_SINCE,
    client: RuzClient | None = None,
    request_delay_seconds: float = 0.05,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Sweep up to `max_statements` statements after `after_id`, build metric rows."""
    ruz = client or RuzClient()
    statement_ids, _ = ruz.statement_ids(
        changed_since=changed_since, after_id=after_id, max_records=max_statements
    )
    entity_cache: dict[int, dict[str, Any]] = {}
    template_cache: dict[int, dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    fetch_failed = 0
    skipped = 0
    last_id = after_id
    for index, statement_id in enumerate(statement_ids):
        if index and request_delay_seconds:
            time.sleep(request_delay_seconds)
        try:
            row = process_statement(
                ruz, statement_id, entity_cache=entity_cache, template_cache=template_cache
            )
        except Exception as exc:  # noqa: BLE001 - count, don't hide
            fetch_failed += 1
            if log is not None:
                log("RÚZ statement %s failed: %s", statement_id, exc)
            continue
        last_id = statement_id
        if row is None:
            skipped += 1
            continue
        rows.append(row)

    counts = _write_metrics_table(connection, rows, source_run_id, log=log)
    counts.update(
        {
            "fetched_ids": len(statement_ids),
            "fetch_failed": fetch_failed,
            "skipped": skipped,
            "last_id": last_id,
        }
    )
    if log is not None:
        log(
            "Built Slovak RÚZ metrics: ids=%s rows=%s mapped=%s skipped=%s failed=%s last_id=%s",
            len(statement_ids), counts.get("statements", 0), counts.get("mapped_statements", 0),
            skipped, fetch_failed, last_id,
        )
    return counts


_STAGE_COLUMNS = (
    "statement_id",
    "ruz_entity_id",
    "ico",
    "template_name",
    "statement_type",
    "fiscal_year",
    "period_start",
    "period_end",
    "filed_date",
    "approved_date",
    "currency",
    *(f"{metric}_amount_original" for metric in METRIC_NAMES),
    "mapped_metric_count",
    "template_mapped",
)


def _stage_row(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        str(row["statement_id"]),
        row["ruz_entity_id"],
        row["ico"],
        row["template_name"],
        row["statement_type"],
        row["fiscal_year"],
        row["period_start"],
        row["period_end"],
        row["filed_date"],
        row["approved_date"],
        row["currency"],
        *(row["metrics"][metric] for metric in METRIC_NAMES),
        row["mapped_metric_count"],
        row["template_mapped"],
    )


def _write_metrics_table(
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
    source_run_id: str,
    *,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    amount_cols = ", ".join(f"{m}_amount_original decimal(38, 2)" for m in METRIC_NAMES)
    placeholders = ", ".join(["?"] * len(_STAGE_COLUMNS))
    metric_cols_select = ", ".join(
        f"{m}_amount_original, cast(null as decimal(38, 2)) as {m}_amount_usd"
        for m in METRIC_NAMES
    )
    connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    connection.execute(
        f"create or replace temp table _sk_stage ("
        f"statement_id varchar, ruz_entity_id varchar, ico varchar, "
        f"template_name varchar, statement_type varchar, fiscal_year integer, "
        f"period_start date, period_end date, filed_date date, approved_date date, "
        f"currency_original varchar, {amount_cols}, "
        f"mapped_metric_count integer, template_mapped integer)"
    )
    if rows:
        connection.executemany(
            f"insert into _sk_stage values ({placeholders})",
            [_stage_row(row) for row in rows],
        )
    connection.execute(
        f"""
        create or replace table {qualified} as
        select
            'SK' as country_iso2,
            '{tables.SOURCE_SLUG}' as source_slug,
            '{source_run_id}' as source_run_id,
            statement_id as source_record_id,
            ico,
            ruz_entity_id,
            statement_id,
            template_name,
            statement_type,
            fiscal_year,
            period_start as period_start_date,
            period_end as period_end_date,
            filed_date,
            approved_date,
            currency_original,
            {metric_cols_select},
            mapped_metric_count,
            template_mapped,
            '{tables.MAPPING_VERSION}' as mapping_version,
            cast(null as decimal(38, 12)) as fx_rate_to_usd,
            cast(null as date) as fx_rate_date,
            cast(null as timestamp) as fx_converted_at,
            '{tables.RUZ_BASE_URL}' as source_url,
            now() as resolved_at
        from _sk_stage
        """
    )
    statements = int(connection.execute(f"select count(*) from {qualified}").fetchone()[0])
    mapped = int(
        connection.execute(
            f"select count(*) from {qualified} where template_mapped = 1"
        ).fetchone()[0]
    )
    return {"statements": statements, "mapped_statements": mapped}


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


def apply_slovakia_usd_conversion(
    *,
    connection: duckdb.DuckDBPyConnection,
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Fill *_usd + fx_* columns by EUR→USD at each statement's period_end."""
    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    pairs = connection.execute(
        f"""
        select distinct upper(currency_original) as currency,
                        cast(period_end_date as varchar) as period_end
        from {qualified}
        where coalesce(currency_original, '') <> '' and period_end_date is not null
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
        f"{m}_amount_usd = cast({m}_amount_original * fx.fx_rate as decimal(38, 2))"
        for m in METRIC_NAMES
    )
    connection.execute(
        "create or replace temp table _sk_fx ("
        "currency varchar, period_end date, fx_rate decimal(38, 12), fx_rate_date date)"
    )
    if fx_rows:
        connection.executemany(
            "insert into _sk_fx values "
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
        from _sk_fx as fx
        where upper(mt.currency_original) = fx.currency
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
            "Applied Slovak USD conversion: rate_pairs=%s rates_found=%s rows_converted=%s",
            counts["rate_pairs"], counts["rates_found"], counts["rows_converted"],
        )
    return counts
