from collections.abc import Callable, Iterable
from datetime import date
from decimal import Decimal
from typing import Any, Protocol

import duckdb
import pyarrow as pa

from dagster_v3.defs.slovakia_financials import raw_store, tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
METRICS_TABLE = tables.METRICS_TABLE
METRIC_NAMES = tables.SK_METRIC_NAMES
_RATE_REQUEST_BATCH = 50
DEFAULT_INSERT_BATCH_ROWS = 50_000
_METRICS_STAGE_TABLE = "_sk_stage"
_METRICS_BATCH_RELATION = "_sk_metrics_batch"
_FX_BATCH_RELATION = "_sk_fx_batch"

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


def decode_statement_bundle(
    bundle: dict[str, Any],
    template_lookup: Callable[[int], dict[str, Any]],
) -> dict[str, Any] | None:
    """Decode one raw S3 statement bundle into a flat metrics row.

    Pure: reads the already-fetched raw JSON (statement + entity + reports)
    and resolves templates via `template_lookup` — no API calls. Returns None
    for deleted statements.
    """
    statement = bundle.get("statement") or {}
    if statement.get("stav") == "ZMAZANÉ":
        return None
    statement_id = int(bundle["statement_id"])
    entity = bundle.get("entity") or {}
    entity_id = statement.get("idUJ")
    ico = str(entity.get("ico") or "")

    merged: dict[str, float | None] = {metric: None for metric in METRIC_NAMES}
    template_name = ""
    mapped = False
    for report in bundle.get("reports") or []:
        if report.get("pristupnostDat") != "Verejné":
            continue
        obsah = report.get("obsah") or {}
        if "tabulky" not in obsah:
            continue
        template_id = report.get("idSablony")
        if template_id is None:
            continue
        template = template_lookup(int(template_id))
        extracted, family = extract_report_metrics(obsah, template)
        if extracted is None:
            continue
        mapped = True
        template_name = str(template.get("nazov") or family or "")
        for metric, value in extracted.items():
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


PROCESSED_BATCHES_TABLE = f"{DLT_DATASET_NAME}.processed_batches"


def _ensure_metrics_tables(connection: duckdb.DuckDBPyConnection) -> None:
    amount_cols = ", ".join(
        f"{metric}_amount_original decimal(38, 2), {metric}_amount_usd decimal(38, 2)"
        for metric in METRIC_NAMES
    )
    connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    connection.execute(
        f"create table if not exists {DLT_DATASET_NAME}.{METRICS_TABLE} ("
        f"country_iso2 varchar, source_slug varchar, source_run_id varchar, "
        f"source_record_id varchar, ico varchar, ruz_entity_id varchar, "
        f"statement_id varchar, template_name varchar, statement_type varchar, "
        f"fiscal_year integer, period_start_date date, period_end_date date, "
        f"filed_date date, approved_date date, currency_original varchar, "
        f"{amount_cols}, mapped_metric_count integer, template_mapped integer, "
        f"mapping_version varchar, fx_rate_to_usd decimal(38, 12), "
        f"fx_rate_date date, fx_converted_at timestamp, source_url varchar, "
        f"resolved_at timestamp, source_batch_key varchar)"
    )
    # Pre-split deployments created the table without the batch column.
    connection.execute(
        f"alter table {DLT_DATASET_NAME}.{METRICS_TABLE} "
        "add column if not exists source_batch_key varchar"
    )
    connection.execute(
        f"create table if not exists {PROCESSED_BATCHES_TABLE} ("
        "batch_key varchar primary key, source_run_id varchar, "
        "statements integer, processed_at timestamp)"
    )


def build_metrics_from_batches(
    *,
    connection: duckdb.DuckDBPyConnection,
    object_store: Any,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Decode every not-yet-processed S3 raw batch into the metrics table.

    Appends per batch with delete-then-insert on source_batch_key, so
    reprocessing a batch (or wiping processed_batches to force a full
    re-decode) is idempotent.
    """
    _ensure_metrics_tables(connection)
    all_keys = raw_store.list_statement_batch_keys(object_store)
    processed = {
        row[0]
        for row in connection.execute(
            f"select batch_key from {PROCESSED_BATCHES_TABLE}"
        ).fetchall()
    }
    pending = [key for key in all_keys if key not in processed]

    template_cache: dict[int, dict[str, Any]] = {}

    def template_lookup(template_id: int) -> dict[str, Any]:
        template = template_cache.get(template_id)
        if template is None:
            template = raw_store.read_template(object_store, template_id)
            template_cache[template_id] = template
        return template

    statements = 0
    skipped = 0
    for batch_key in pending:
        rows: list[dict[str, Any]] = []
        for bundle in raw_store.read_statement_batch(object_store, batch_key):
            row = decode_statement_bundle(bundle, template_lookup)
            if row is None:
                skipped += 1
                continue
            rows.append(row)
        statements += _append_metrics_batch(
            connection,
            rows,
            source_run_id,
            batch_key,
        )

    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    table_statements = int(
        connection.execute(f"select count(*) from {qualified}").fetchone()[0]
    )
    table_mapped = int(
        connection.execute(
            f"select count(*) from {qualified} where template_mapped = 1"
        ).fetchone()[0]
    )
    counts = {
        "batches_processed": len(pending),
        "statements": statements,
        "skipped": skipped,
        "table_statements": table_statements,
        "table_mapped": table_mapped,
    }
    if log is not None:
        log(
            "Built Slovak RÚZ metrics from raw batches: batches=%s rows=%s skipped=%s "
            "table_rows=%s table_mapped=%s",
            len(pending), statements, skipped, table_statements, table_mapped,
        )
    return counts


_STAGE_ARROW_SCHEMA = pa.schema(
    [
        pa.field("statement_id", pa.string(), nullable=False),
        pa.field("ruz_entity_id", pa.string(), nullable=False),
        pa.field("ico", pa.string(), nullable=False),
        pa.field("template_name", pa.string(), nullable=False),
        pa.field("statement_type", pa.string(), nullable=False),
        pa.field("fiscal_year", pa.int32()),
        pa.field("period_start", pa.string()),
        pa.field("period_end", pa.string()),
        pa.field("filed_date", pa.string()),
        pa.field("approved_date", pa.string()),
        pa.field("currency", pa.string(), nullable=False),
        *(
            pa.field(f"{metric}_amount_original", pa.float64())
            for metric in METRIC_NAMES
        ),
        pa.field("mapped_metric_count", pa.int32(), nullable=False),
        pa.field("template_mapped", pa.int32(), nullable=False),
    ]
)
_FX_ARROW_SCHEMA = pa.schema(
    [
        pa.field("currency", pa.string(), nullable=False),
        pa.field("period_end", pa.date32(), nullable=False),
        pa.field("fx_rate", pa.decimal128(38, 12), nullable=False),
        pa.field("fx_rate_date", pa.date32(), nullable=False),
    ]
)


def _stage_arrow_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "statement_id": str(row["statement_id"]),
        "ruz_entity_id": row["ruz_entity_id"],
        "ico": row["ico"],
        "template_name": row["template_name"],
        "statement_type": row["statement_type"],
        "fiscal_year": row["fiscal_year"],
        "period_start": row["period_start"],
        "period_end": row["period_end"],
        "filed_date": row["filed_date"],
        "approved_date": row["approved_date"],
        "currency": row["currency"],
        **{
            f"{metric}_amount_original": row["metrics"][metric]
            for metric in METRIC_NAMES
        },
        "mapped_metric_count": row["mapped_metric_count"],
        "template_mapped": row["template_mapped"],
    }


def _append_metrics_batch(
    connection: duckdb.DuckDBPyConnection,
    rows: Iterable[dict[str, Any]],
    source_run_id: str,
    batch_key: str,
    batch_rows: int = DEFAULT_INSERT_BATCH_ROWS,
) -> int:
    if batch_rows < 1:
        raise ValueError("Slovakia financial insert batch size must be positive")

    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    amount_cols = ", ".join(f"{m}_amount_original decimal(38, 2)" for m in METRIC_NAMES)
    metric_cols_select = ", ".join(
        f"{m}_amount_original, cast(null as decimal(38, 2)) as {m}_amount_usd"
        for m in METRIC_NAMES
    )
    connection.execute(
        f"create or replace temp table {_METRICS_STAGE_TABLE} ("
        f"statement_id varchar, ruz_entity_id varchar, ico varchar, "
        f"template_name varchar, statement_type varchar, fiscal_year integer, "
        f"period_start date, period_end date, filed_date date, approved_date date, "
        f"currency_original varchar, {amount_cols}, "
        f"mapped_metric_count integer, template_mapped integer)"
    )
    row_batch: list[dict[str, Any]] = []
    row_count = 0
    for row in rows:
        row_batch.append(row)
        if len(row_batch) >= batch_rows:
            _insert_metrics_stage_batch(connection, row_batch)
            row_count += len(row_batch)
            row_batch.clear()
    if row_batch:
        _insert_metrics_stage_batch(connection, row_batch)
        row_count += len(row_batch)

    connection.execute("begin transaction")
    try:
        connection.execute(f"delete from {qualified} where source_batch_key = ?", [batch_key])
        connection.execute(
            f"""
            insert into {qualified} by name
            select
                'SK' as country_iso2,
                '{tables.SOURCE_SLUG}' as source_slug,
                ? as source_run_id,
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
                now() as resolved_at,
                ? as source_batch_key
            from {_METRICS_STAGE_TABLE}
            """,
            [source_run_id, batch_key],
        )
        connection.execute(
            f"delete from {PROCESSED_BATCHES_TABLE} where batch_key = ?", [batch_key]
        )
        connection.execute(
            f"insert into {PROCESSED_BATCHES_TABLE} values (?, ?, ?, now())",
            [batch_key, source_run_id, row_count],
        )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise
    return row_count


def _insert_metrics_stage_batch(
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
) -> None:
    arrow_table = pa.Table.from_pylist(
        [_stage_arrow_row(row) for row in rows],
        schema=_STAGE_ARROW_SCHEMA,
    )
    metric_select = ", ".join(
        f"cast({metric}_amount_original as decimal(38, 2)) "
        f"as {metric}_amount_original"
        for metric in METRIC_NAMES
    )
    connection.register(_METRICS_BATCH_RELATION, arrow_table)
    try:
        connection.execute(
            f"""
            insert into {_METRICS_STAGE_TABLE}
            select
                statement_id,
                ruz_entity_id,
                ico,
                template_name,
                statement_type,
                fiscal_year,
                cast(period_start as date),
                cast(period_end as date),
                cast(filed_date as date),
                cast(approved_date as date),
                currency,
                {metric_select},
                mapped_metric_count,
                template_mapped
            from {_METRICS_BATCH_RELATION}
            """
        )
    finally:
        connection.unregister(_METRICS_BATCH_RELATION)


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
    _ensure_metrics_tables(connection)
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
        {
            "currency": currency,
            "period_end": date.fromisoformat(end),
            "fx_rate": Decimal(str(rate.rate)),
            "fx_rate_date": date.fromisoformat(str(rate.rate_date)),
        }
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
        fx_table = pa.Table.from_pylist(
            fx_rows,
            schema=_FX_ARROW_SCHEMA,
        )
        connection.register(_FX_BATCH_RELATION, fx_table)
        try:
            connection.execute(
                "insert into _sk_fx "
                "select currency, period_end, fx_rate, fx_rate_date "
                f"from {_FX_BATCH_RELATION}"
            )
        finally:
            connection.unregister(_FX_BATCH_RELATION)
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
