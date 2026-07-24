"""Build, convert and export the cross-partition TED tables."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, Protocol

from duckdb import DuckDBPyConnection

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.sweden_company.identity import normalize_sweden_identity
from dagster_v3.defs.ted_procurement import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
PARTITION_DUCKDB_ROOT = Path("data/ted_procurement/duckdb")
_RATE_REQUEST_BATCH = 50


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]: ...


def partition_duckdb_path(partition_key: str) -> Path:
    return PARTITION_DUCKDB_ROOT / f"partition_key={partition_key}" / "data.duckdb"


def list_parsed_partitions() -> list[tuple[str, Path]]:
    if not PARTITION_DUCKDB_ROOT.exists():
        return []
    result = []
    for entry in sorted(PARTITION_DUCKDB_ROOT.glob("partition_key=*/data.duckdb")):
        key = entry.parent.name.removeprefix("partition_key=")
        result.append((key, entry))
    return result


def normalize_national_id(country: str, raw: str) -> str:
    country_code = country.upper()
    value = raw.strip()
    if country_code in {"SE", "SWE"}:
        return normalize_sweden_identity(value)
    rule = tables.NATIONAL_ID_NORMALIZATION.get(country_code)
    if rule is None or value == "":
        return value
    pattern, replacement = rule
    return re.sub(pattern, replacement, value)


def build_publish_tables(
    *,
    duckdb_connection: DuckDBPyConnection,
    partitions: Iterable[tuple[str, Path]],
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Union all parsed partition DuckDBs into the publish notices/winners tables."""
    partitions = list(partitions)
    if not partitions:
        raise ValueError(
            "No parsed TED partitions found — materialize ted_monthly_duckdb first"
        )
    duckdb_connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    first = True
    for key, path in partitions:
        alias = "part_db"
        duckdb_connection.execute(f"attach '{path}' as {alias} (read_only)")
        for temp, source in (
            ("_ted_listing_all", "listing"),
            ("_ted_docs_all", "notice_docs"),
            ("_ted_orgs_all", "organizations"),
            ("_ted_winners_all", "winner_links"),
        ):
            verb = (
                f"create or replace temp table {temp} as"
                if first
                else f"insert into {temp}"
            )
            duckdb_connection.execute(
                f"{verb} select *, '{key}' as partition_key from {alias}.{source}"
            )
        duckdb_connection.execute(f"detach {alias}")
        first = False

    notices = f"{DLT_DATASET_NAME}.{tables.NOTICES_TABLE}"
    duckdb_connection.execute(
        f"""
        create or replace table {notices} as
        with deduped as (
            select * from _ted_listing_all
            qualify row_number() over (
                partition by country_iso2, publication_number
                order by partition_key desc
            ) = 1
        ),
        docs as (
            select * from _ted_docs_all
            qualify row_number() over (
                partition by publication_number order by partition_key desc
            ) = 1
        )
        select
            l.country_iso2,
            '{tables.SOURCE_SLUG}' as source_slug,
            cast(? as varchar) as source_run_id,
            l.publication_number,
            try_cast(substr(l.publication_date, 1, 10) as date) as publication_date,
            l.notice_type,
            l.place_country,
            l.buyer_name,
            coalesce(nd.buyer_org_ref, '') as buyer_org_ref,
            coalesce(b.national_id_raw, '') as buyer_national_id_raw,
            coalesce(b.national_id, '') as buyer_national_id,
            coalesce(b.country, '') as buyer_country,
            l.notice_title,
            try_cast(l.total_value as decimal(38, 2)) as total_value_amount_original,
            cast(null as decimal(38, 2)) as total_value_amount_usd,
            upper(coalesce(l.total_value_currency, '')) as total_value_currency,
            cast(null as decimal(38, 12)) as fx_rate_to_usd,
            cast(null as date) as fx_rate_date,
            '' as fx_source,
            l.partition_key,
            cast(now() as timestamp) as resolved_at
        from deduped l
        left join docs nd on nd.publication_number = l.publication_number
        left join (
            select distinct publication_number, org_ref, name, national_id_raw,
                   national_id, country
            from _ted_orgs_all
        ) b on b.publication_number = l.publication_number
           and b.org_ref = nd.buyer_org_ref
        """,
        [source_run_id],
    )

    winners = f"{DLT_DATASET_NAME}.{tables.NOTICE_WINNERS_TABLE}"
    duckdb_connection.execute(
        f"""
        create or replace table {winners} as
        with deduped as (
            select * from _ted_winners_all
            qualify row_number() over (
                partition by publication_number, lot_id, tender_id, winner_ordinal
                order by partition_key desc
            ) = 1
        )
        select
            n.country_iso2,
            '{tables.SOURCE_SLUG}' as source_slug,
            cast(? as varchar) as source_run_id,
            w.publication_number,
            w.lot_id,
            w.tender_id,
            w.winner_ordinal,
            coalesce(o.name, '') as winner_name,
            coalesce(o.national_id_raw, '') as winner_national_id_raw,
            coalesce(o.national_id, '') as winner_national_id,
            coalesce(o.country, '') as winner_country,
            try_cast(w.awarded_amount as decimal(38, 2)) as awarded_amount_original,
            cast(null as decimal(38, 2)) as awarded_amount_usd,
            upper(coalesce(w.awarded_currency, '')) as awarded_currency,
            n.buyer_national_id,
            n.place_country,
            n.publication_date,
            cast(null as decimal(38, 12)) as fx_rate_to_usd,
            cast(null as date) as fx_rate_date,
            '' as fx_source,
            w.partition_key,
            cast(now() as timestamp) as resolved_at
        from deduped w
        join {notices} n on n.publication_number = w.publication_number
        left join (
            select distinct publication_number, org_ref, name, national_id_raw,
                   national_id, country
            from _ted_orgs_all
        ) o on o.publication_number = w.publication_number
           and o.org_ref = w.org_ref
        """,
        [source_run_id],
    )

    counts = {
        "partitions": len(partitions),
        "notices": int(
            duckdb_connection.execute(f"select count(*) from {notices}").fetchone()[0]
        ),
        "winners": int(
            duckdb_connection.execute(f"select count(*) from {winners}").fetchone()[0]
        ),
        "winners_with_national_id": int(
            duckdb_connection.execute(
                f"select count(*) from {winners} where winner_national_id <> ''"
            ).fetchone()[0]
        ),
    }
    if log is not None:
        log("Built TED publish tables: %s", counts)
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


def apply_ted_usd_conversion(
    *,
    duckdb_connection: DuckDBPyConnection,
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Fill *_usd + fx columns on both publish tables, keyed on publication date."""
    notices = f"{DLT_DATASET_NAME}.{tables.NOTICES_TABLE}"
    winners = f"{DLT_DATASET_NAME}.{tables.NOTICE_WINNERS_TABLE}"
    pairs = duckdb_connection.execute(
        f"""
        select distinct total_value_currency as currency,
               cast(publication_date as varchar) as rate_date
        from {notices}
        where total_value_currency <> '' and publication_date is not null
        union
        select distinct awarded_currency, cast(publication_date as varchar)
        from {winners}
        where awarded_currency <> '' and publication_date is not null
        """
    ).fetchall()
    requests = [_request(currency, rate_date) for currency, rate_date in pairs]
    rates = _load_rates(exchange_rates, requests)
    fx_rows = [
        (currency, rate_date, rate.rate, str(rate.rate_date), rate.source)
        for currency, rate_date in pairs
        if (rate := rates.get((currency, rate_date))) is not None
    ]
    duckdb_connection.execute(
        "create or replace temp table _ted_fx ("
        "currency varchar, rate_date date, fx_rate decimal(38, 12), "
        "fx_rate_date date, fx_source varchar)"
    )
    if fx_rows:
        duckdb_connection.executemany(
            "insert into _ted_fx values "
            "(?, cast(? as date), cast(? as decimal(38, 12)), cast(? as date), ?)",
            fx_rows,
        )
    for qualified, amount, currency_col in (
        (notices, "total_value", "total_value_currency"),
        (winners, "awarded", "awarded_currency"),
    ):
        original = (
            "total_value_amount_original"
            if amount == "total_value"
            else "awarded_amount_original"
        )
        usd = (
            "total_value_amount_usd"
            if amount == "total_value"
            else "awarded_amount_usd"
        )
        duckdb_connection.execute(
            f"""
            update {qualified} as records
            set {usd} = cast(records.{original} * fx.fx_rate as decimal(38, 2)),
                fx_rate_to_usd = fx.fx_rate,
                fx_rate_date = fx.fx_rate_date,
                fx_source = fx.fx_source
            from _ted_fx as fx
            where fx.currency = records.{currency_col}
              and fx.rate_date = records.publication_date
            """
        )
    converted = int(
        duckdb_connection.execute(
            f"select count(*) from {winners} where awarded_amount_usd is not null"
        ).fetchone()[0]
    )
    counts = {
        "rate_pairs": len(pairs),
        "rates_found": len(fx_rows),
        "winner_amounts_converted": converted,
    }
    if log is not None:
        log("Applied TED USD conversion: %s", counts)
    return counts


def export_ted_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.TED_PROCUREMENT_DATABASE,
        tables=(tables.TED_NOTICES_TABLE, tables.TED_NOTICE_WINNERS_TABLE),
    )
    counts: dict[str, int] = {}
    with clickhouse.get_connection() as client:
        for duckdb_table, ch_table, columns in (
            (
                tables.NOTICES_TABLE,
                tables.TED_NOTICES_TABLE,
                tables.TED_NOTICES_COLUMNS,
            ),
            (
                tables.NOTICE_WINNERS_TABLE,
                tables.TED_NOTICE_WINNERS_TABLE,
                tables.TED_NOTICE_WINNERS_COLUMNS,
            ),
        ):
            if log is not None:
                log("Exporting TED table to ClickHouse: %s", ch_table)
            counts[ch_table] = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=duckdb_connection,
                clickhouse_client=client,
                duckdb_schema=DLT_DATASET_NAME,
                duckdb_table=duckdb_table,
                clickhouse_database=tables.TED_PROCUREMENT_DATABASE,
                clickhouse_table=ch_table,
                columns=columns,
                truncate=True,
            )
    if log is not None:
        log("Finished TED ClickHouse export: %s", counts)
    return counts
