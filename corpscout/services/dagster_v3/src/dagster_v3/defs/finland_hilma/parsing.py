"""Parse and transform the manually exported Hilma search-results CSVs."""

from __future__ import annotations

import csv
import io
import re
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from duckdb import DuckDBPyConnection

from dagster_v3.defs.finland_hilma import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
CSV_SOURCE_ENCODING = "cp1252"

_WHITESPACE = re.compile(r"\s+")
_RATE_REQUEST_BATCH = 50


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]: ...


def _normalize_title(title: str) -> str:
    return _WHITESPACE.sub(" ", title).strip()


def validate_export_header(csv_text: str) -> None:
    """Refuse exports made with a different portal column selection.

    The portal lets users pick arbitrary columns; only the full 58-column
    export is supported (see design doc §3).
    """
    header = next(csv.reader(io.StringIO(csv_text), delimiter=";"))
    titles = tuple(_normalize_title(t) for t in header)
    expected = tables.EXPECTED_HEADER_TITLES
    if titles == expected:
        return
    if len(titles) != len(expected):
        raise ValueError(
            f"Hilma export has {len(titles)} columns, expected {len(expected)}: "
            "export from the portal with the full column set"
        )
    mismatches = [
        f"col {i}: got {got!r}, expected {want!r}"
        for i, (got, want) in enumerate(zip(titles, expected))
        if got != want
    ]
    raise ValueError(
        "Hilma export header does not match the supported shape: "
        + "; ".join(mismatches[:5])
    )


def load_export_bytes_into_raw_table(
    *,
    duckdb_connection: DuckDBPyConnection,
    csv_bytes: bytes,
    source_key: str,
    replace: bool,
) -> int:
    """Transcode one cp1252 export to UTF-8 and append it to the raw table.

    The header row contains quoted embedded newlines, so it must be consumed
    as a CSV record (header=true) — never skipped by line count.
    """
    csv_text = csv_bytes.decode(CSV_SOURCE_ENCODING)
    validate_export_header(csv_text)
    # Explicit columns disable the CSV sniffer entirely — it chokes on the
    # 58-column all-quoted shape with multiline cells.
    columns_sql = ", ".join(f"'{name}': 'VARCHAR'" for name in tables.RAW_COLUMNS)
    duckdb_connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    qualified = f"{DLT_DATASET_NAME}.{tables.RAW_EXPORTS_TABLE}"
    with tempfile.TemporaryDirectory(prefix="finland_hilma_") as tmpdir:
        utf8_path = Path(tmpdir) / "export.csv"
        utf8_path.write_text(csv_text, encoding="utf-8")
        verb = "create or replace table" if replace else "insert into"
        duckdb_connection.execute(
            f"{verb} {qualified} "
            f"{'as ' if replace else ''}select *, cast(? as varchar) as source_key "
            f"from read_csv(?, delim=';', header=true, strict_mode=false, "
            f"quote='\"', escape='\"', columns={{{columns_sql}}})",
            [source_key, str(utf8_path)],
        )
        rows = int(
            duckdb_connection.execute(
                f"select count(*) from {qualified} where source_key = ?",
                [source_key],
            ).fetchone()[0]
        )
    if rows == 0:
        raise ValueError(f"Hilma export produced zero rows: {source_key}")
    return rows


def _amount_exprs() -> str:
    return ",\n            ".join(
        expr
        for name, (value_col, currency_col) in tables.AMOUNT_SOURCE_COLUMNS.items()
        for expr in (
            f"try_cast(replace(replace(nullif(trim({value_col}), ''), ' ', ''), "
            f"',', '.') as decimal(38, 2)) as {name}_amount_original",
            f"cast(null as decimal(38, 2)) as {name}_amount_usd",
            f"upper(coalesce(nullif(trim({currency_col}), ''), '')) as {name}_currency",
        )
    )


def build_finland_hilma_notices(
    *,
    duckdb_connection: DuckDBPyConnection,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Dedup + type the raw export rows into the notices and winners tables."""
    notices = f"{DLT_DATASET_NAME}.{tables.NOTICES_TABLE}"
    duckdb_connection.execute(
        f"""
        create or replace table {notices} as
        with deduped as (
            select *
            from {DLT_DATASET_NAME}.{tables.RAW_EXPORTS_TABLE}
            qualify row_number() over (
                partition by notice_number, coalesce(lot_id, '')
                order by try_cast(published_utc as timestamp) desc nulls last,
                         source_key desc
            ) = 1
        )
        select
            '{tables.COUNTRY_ISO2}' as country_iso2,
            '{tables.SOURCE_SLUG}' as source_slug,
            cast(? as varchar) as source_run_id,
            notice_number || ':' || coalesce(lot_id, '') as source_record_id,
            notice_number,
            coalesce(trim(ted_number), '') as ted_number,
            coalesce(trim(lot_id), '') as lot_id,
            try_cast(published_utc as timestamp) as published_at,
            replace(coalesce(notice_type, ''), '%u2013', '–') as notice_type,
            cast(lower(coalesce(notice_type, '')) like '%award%' as tinyint) as is_award,
            coalesce(trim(procurement_project_id), '') as procurement_project_id,
            coalesce(trim(procedure_id), '') as procedure_id,
            coalesce(notice_name_fi, '') as notice_name_fi,
            coalesce(notice_name_en, '') as notice_name_en,
            coalesce(notice_name_sv, '') as notice_name_sv,
            coalesce(lot_name_fi, '') as lot_name_fi,
            coalesce(lot_name_en, '') as lot_name_en,
            coalesce(lot_name_sv, '') as lot_name_sv,
            coalesce(trim(notice_cpv_code), '') as notice_cpv_code,
            coalesce(trim(lot_cpv_code), '') as lot_cpv_code,
            coalesce(organisation_name_fi, '') as buyer_name_fi,
            coalesce(organisation_name_en, '') as buyer_name_en,
            coalesce(organisation_name_sv, '') as buyer_name_sv,
            coalesce(organisation_department, '') as buyer_department,
            coalesce(trim(organisation_registration_number), '') as buyer_business_id,
            coalesce(organisation_address, '') as buyer_address,
            coalesce(trim(nuts_code), '') as nuts_code,
            try_cast(deadline_utc as timestamp) as deadline_at,
            coalesce(trim(dynamic_purchasing_system), '') as dynamic_purchasing_system,
            coalesce(trim(framework_agreement), '') as framework_agreement,
            coalesce(trim(procedure_type), '') as procedure_type,
            coalesce(trim(main_nature), '') as main_nature,
            coalesce(trim(lot_main_nature), '') as lot_main_nature,
            coalesce(lot_winner, '') as winners_raw,
            {_amount_exprs()},
            cast(null as decimal(38, 12)) as fx_rate_to_usd,
            cast(null as date) as fx_rate_date,
            '' as fx_source,
            source_key,
            cast(now() as timestamp) as resolved_at
        from deduped
        where coalesce(trim(notice_number), '') <> ''
        """,
        [source_run_id],
    )

    winners = f"{DLT_DATASET_NAME}.{tables.NOTICE_WINNERS_TABLE}"
    duckdb_connection.execute(
        f"""
        create or replace table {winners} as
        with exploded as (
            select
                notice_number, lot_id, is_award, published_at, buyer_business_id,
                string_split(winners_raw, '//') as parts
            from {notices}
            where winners_raw <> ''
        )
        select
            '{tables.COUNTRY_ISO2}' as country_iso2,
            '{tables.SOURCE_SLUG}' as source_slug,
            cast(? as varchar) as source_run_id,
            notice_number,
            lot_id,
            cast(part_index as integer) as winner_ordinal,
            trim(regexp_replace(parts[part_index], '\\s*\\(\\d{{7}}-\\d\\)\\s*$', ''))
                as winner_name,
            coalesce(regexp_extract(parts[part_index], '\\((\\d{{7}}-\\d)\\)\\s*$', 1), '')
                as winner_business_id,
            buyer_business_id,
            is_award,
            published_at,
            cast(now() as timestamp) as resolved_at
        from exploded
        cross join generate_series(1, len(parts)) as t(part_index)
        where trim(parts[part_index]) <> ''
        """,
        [source_run_id],
    )

    counts = {
        "notices": int(
            duckdb_connection.execute(f"select count(*) from {notices}").fetchone()[0]
        ),
        "winners": int(
            duckdb_connection.execute(f"select count(*) from {winners}").fetchone()[0]
        ),
        "winners_with_business_id": int(
            duckdb_connection.execute(
                f"select count(*) from {winners} where winner_business_id <> ''"
            ).fetchone()[0]
        ),
    }
    if log is not None:
        log(
            "Built Finland Hilma notices: notices=%s winners=%s with_business_id=%s",
            counts["notices"],
            counts["winners"],
            counts["winners_with_business_id"],
        )
    return counts


def _request(currency: str, rate_date: str) -> Any:
    from exchange_rates import ExchangeRateRequest

    return ExchangeRateRequest(currency=currency, rate_date=rate_date)


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


def apply_finland_hilma_usd_conversion(
    *,
    duckdb_connection: DuckDBPyConnection,
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Fill each amount's *_usd using its own currency, keyed on published date.

    The row-level fx trio reflects the procurement-value currency (design §5).
    """
    qualified = f"{DLT_DATASET_NAME}.{tables.NOTICES_TABLE}"
    currency_cols = [f"{name}_currency" for name in tables.AMOUNT_NAMES]
    pair_sql = " union ".join(
        f"select distinct {col} as currency, cast(published_at as date) as rate_date "
        f"from {qualified} where {col} <> '' and published_at is not null"
        for col in currency_cols
    )
    pairs = duckdb_connection.execute(pair_sql).fetchall()
    requests = [_request(currency, str(rate_date)) for currency, rate_date in pairs]
    rates = _load_rates(exchange_rates, requests)
    fx_rows = [
        (currency, str(rate_date), rate.rate, str(rate.rate_date), rate.source)
        for currency, rate_date in pairs
        if (rate := rates.get((currency, str(rate_date)))) is not None
    ]

    duckdb_connection.execute(
        "create or replace temp table _fi_hilma_fx ("
        "currency varchar, rate_date date, fx_rate decimal(38, 12), "
        "fx_rate_date date, fx_source varchar)"
    )
    if fx_rows:
        duckdb_connection.executemany(
            "insert into _fi_hilma_fx values "
            "(?, cast(? as date), cast(? as decimal(38, 12)), cast(? as date), ?)",
            fx_rows,
        )

    set_usd = ", ".join(
        f"{name}_amount_usd = ("
        f"select cast(records.{name}_amount_original * fx.fx_rate as decimal(38, 2)) "
        f"from _fi_hilma_fx fx "
        f"where fx.currency = records.{name}_currency "
        f"and fx.rate_date = cast(records.published_at as date))"
        for name in tables.AMOUNT_NAMES
    )
    duckdb_connection.execute(
        f"""
        update {qualified} as records
        set {set_usd},
            fx_rate_to_usd = (
                select fx.fx_rate from _fi_hilma_fx fx
                where fx.currency = records.procurement_value_currency
                  and fx.rate_date = cast(records.published_at as date)),
            fx_rate_date = (
                select fx.fx_rate_date from _fi_hilma_fx fx
                where fx.currency = records.procurement_value_currency
                  and fx.rate_date = cast(records.published_at as date)),
            fx_source = coalesce((
                select fx.fx_source from _fi_hilma_fx fx
                where fx.currency = records.procurement_value_currency
                  and fx.rate_date = cast(records.published_at as date)), '')
        """
    )
    converted = int(
        duckdb_connection.execute(
            f"select count(*) from {qualified} "
            f"where procurement_value_amount_usd is not null"
        ).fetchone()[0]
    )
    counts = {
        "rate_pairs": len(pairs),
        "rates_found": len(fx_rows),
        "procurement_values_converted": converted,
    }
    if log is not None:
        log(
            "Applied Finland Hilma USD conversion: rate_pairs=%s rates_found=%s "
            "procurement_values_converted=%s",
            counts["rate_pairs"],
            counts["rates_found"],
            counts["procurement_values_converted"],
        )
    return counts
