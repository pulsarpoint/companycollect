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


def partition_duckdb_path(*, country_iso2: str, month: str) -> Path:
    """One DuckDB file per (country, month) partition.

    Country leads the path for the same reason it leads the object prefix: a
    country must be removable or re-fetchable on its own.
    """
    return (
        PARTITION_DUCKDB_ROOT
        / f"country={country_iso2}"
        / f"partition_key={month}"
        / "data.duckdb"
    )


def list_parsed_partitions() -> list[tuple[str, str, Path]]:
    """Every parsed partition as (country_iso2, month, path), sorted.

    The publish step is deliberately unpartitioned and unions all of these, so
    it picks up a newly backfilled country without any change of its own.
    """
    if not PARTITION_DUCKDB_ROOT.exists():
        return []
    result = []
    for entry in sorted(
        PARTITION_DUCKDB_ROOT.glob("country=*/partition_key=*/data.duckdb")
    ):
        month = entry.parent.name.removeprefix("partition_key=")
        country_iso2 = entry.parent.parent.name.removeprefix("country=")
        result.append((country_iso2, month, entry))
    return result


def _normalize_france_identity(value: str) -> str:
    compact = re.sub(r"\s+", "", value)
    if re.fullmatch(r"\d{14}", compact):
        return compact[:9]
    if re.fullmatch(r"\d{9}", compact):
        return compact
    vat_match = re.fullmatch(r"FR[A-Z0-9]{2}(\d{9})", compact.upper())
    if vat_match is not None:
        return vat_match.group(1)
    return value


def _normalize_slovakia_identity(value: str) -> str:
    compact = re.sub(r"\s+", "", value)
    if re.fullmatch(r"\d{8}", compact):
        return compact
    return value


def _normalize_latvia_identity(value: str) -> str:
    compact = re.sub(r"\s+", "", value)
    match = re.fullmatch(r"(?:LV)?(\d{11})", compact.upper())
    if match is not None:
        return match.group(1)
    return value


def _normalize_denmark_identity(value: str) -> str:
    compact = re.sub(r"\s+", "", value)
    match = re.fullmatch(r"(?:DK)?(\d{8})", compact.upper())
    if match is not None:
        return match.group(1)
    return value


def _normalize_estonia_identity(value: str) -> str:
    compact = re.sub(r"\s+", "", value)
    if re.fullmatch(r"\d{8}", compact):
        return compact
    return value


def normalize_national_id(country: str, raw: str) -> str:
    country_code = country.upper()
    value = raw.strip()
    if country_code in {"SE", "SWE"}:
        return normalize_sweden_identity(value)
    if country_code in {"FR", "FRA"}:
        return _normalize_france_identity(value)
    if country_code in {"SK", "SVK"}:
        return _normalize_slovakia_identity(value)
    if country_code in {"LV", "LVA"}:
        return _normalize_latvia_identity(value)
    if country_code in {"DK", "DNK"}:
        return _normalize_denmark_identity(value)
    if country_code in {"EE", "EST"}:
        return _normalize_estonia_identity(value)
    rule = tables.NATIONAL_ID_NORMALIZATION.get(country_code)
    if rule is None or value == "":
        return value
    pattern, replacement = rule
    return re.sub(pattern, replacement, value)


def build_publish_tables(
    *,
    duckdb_connection: DuckDBPyConnection,
    partitions: Iterable[tuple[str, str, Path]],
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
    for _country_iso2, month, path in partitions:
        alias = "part_db"
        duckdb_connection.execute(f"attach '{path}' as {alias} (read_only)")
        for temp, source in (
            ("_ted_listing_all", "listing"),
            ("_ted_docs_all", "notice_docs"),
            ("_ted_orgs_all", "organizations"),
            ("_ted_lots_all", "lots"),
            ("_ted_winners_all", "winner_links"),
        ):
            verb = (
                f"create or replace temp table {temp} as"
                if first
                else f"insert into {temp}"
            )
            duckdb_connection.execute(
                f"{verb} select *, '{month}' as partition_key from {alias}.{source}"
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
            -- BT-27 / BT-709 / BT-118 / BT-1118, each in its own column. An
            -- estimate, a ceiling and a realized award are different claims;
            -- one column holding whichever was present makes all of them
            -- unreadable, and a ceiling summed as spend overstates it wildly.
            try_cast(nd.estimated_value as decimal(38, 2))
                as estimated_value_amount_original,
            cast(null as decimal(38, 2)) as estimated_value_amount_usd,
            upper(coalesce(nd.estimated_value_currency, '')) as estimated_value_currency,
            try_cast(nd.framework_maximum as decimal(38, 2))
                as framework_maximum_amount_original,
            cast(null as decimal(38, 2)) as framework_maximum_amount_usd,
            upper(coalesce(nd.framework_maximum_currency, ''))
                as framework_maximum_currency,
            try_cast(nd.framework_total_maximum as decimal(38, 2))
                as framework_total_maximum_amount_original,
            cast(null as decimal(38, 2)) as framework_total_maximum_amount_usd,
            upper(coalesce(nd.framework_total_maximum_currency, ''))
                as framework_total_maximum_currency,
            try_cast(nd.framework_total_approximate as decimal(38, 2))
                as framework_total_approximate_amount_original,
            cast(null as decimal(38, 2)) as framework_total_approximate_amount_usd,
            upper(coalesce(nd.framework_total_approximate_currency, ''))
                as framework_total_approximate_currency,
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

    lots = f"{DLT_DATASET_NAME}.{tables.NOTICE_LOTS_TABLE}"
    duckdb_connection.execute(
        f"""
        create or replace table {lots} as
        with deduped as (
            select * from _ted_lots_all
            qualify row_number() over (
                partition by publication_number, lot_id
                order by partition_key desc
            ) = 1
        )
        select
            n.country_iso2,
            '{tables.SOURCE_SLUG}' as source_slug,
            cast(? as varchar) as source_run_id,
            t.publication_number,
            t.lot_id,
            t.lot_title,
            try_cast(t.estimated_value as decimal(38, 2))
                as estimated_value_amount_original,
            cast(null as decimal(38, 2)) as estimated_value_amount_usd,
            upper(coalesce(t.estimated_value_currency, '')) as estimated_value_currency,
            try_cast(t.framework_maximum as decimal(38, 2))
                as framework_maximum_amount_original,
            cast(null as decimal(38, 2)) as framework_maximum_amount_usd,
            upper(coalesce(t.framework_maximum_currency, ''))
                as framework_maximum_currency,
            try_cast(t.framework_value_maximum as decimal(38, 2))
                as framework_value_maximum_amount_original,
            cast(null as decimal(38, 2)) as framework_value_maximum_amount_usd,
            upper(coalesce(t.framework_value_maximum_currency, ''))
                as framework_value_maximum_currency,
            try_cast(t.framework_value_reestimated as decimal(38, 2))
                as framework_value_reestimated_amount_original,
            cast(null as decimal(38, 2)) as framework_value_reestimated_amount_usd,
            upper(coalesce(t.framework_value_reestimated_currency, ''))
                as framework_value_reestimated_currency,
            try_cast(t.lower_tender as decimal(38, 2)) as lower_tender_amount_original,
            cast(null as decimal(38, 2)) as lower_tender_amount_usd,
            upper(coalesce(t.lower_tender_currency, '')) as lower_tender_currency,
            try_cast(t.higher_tender as decimal(38, 2)) as higher_tender_amount_original,
            cast(null as decimal(38, 2)) as higher_tender_amount_usd,
            upper(coalesce(t.higher_tender_currency, '')) as higher_tender_currency,
            cast(null as decimal(38, 12)) as fx_rate_to_usd,
            cast(null as date) as fx_rate_date,
            '' as fx_source,
            n.publication_date,
            t.partition_key,
            cast(now() as timestamp) as resolved_at
        from deduped t
        join {notices} n on n.publication_number = t.publication_number
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
            -- BT-553: the share this winner subcontracts away. Its own column,
            -- never netted off the award -- the winner was still paid the whole
            -- amount.
            try_cast(w.subcontracting_amount as decimal(38, 2))
                as subcontracting_amount_original,
            cast(null as decimal(38, 2)) as subcontracting_amount_usd,
            upper(coalesce(w.subcontracting_currency, '')) as subcontracting_currency,
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
        "lots": int(
            duckdb_connection.execute(f"select count(*) from {lots}").fetchone()[0]
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
    """Fill every *_usd + the fx columns on all three publish tables.

    Driven off the metric tuples in ``tables`` rather than a hardcoded pair, so
    a newly parsed amount is converted the moment it is stored. Converting a
    subset would leave the rest answerable in the notice's own currency only,
    which for a cross-country product is the same as not storing them.

    Each amount is converted with **its own** currency column: a notice can
    quote a Danish framework ceiling beside a Swedish award, and one shared
    currency would mislabel one of them.
    """
    targets = (
        (
            f"{DLT_DATASET_NAME}.{tables.NOTICES_TABLE}",
            tables.TED_NOTICE_VALUE_METRICS,
        ),
        (
            f"{DLT_DATASET_NAME}.{tables.NOTICE_LOTS_TABLE}",
            tables.TED_LOT_VALUE_METRICS,
        ),
        (
            f"{DLT_DATASET_NAME}.{tables.NOTICE_WINNERS_TABLE}",
            tables.TED_WINNER_VALUE_METRICS,
        ),
    )

    pair_query = "\nunion\n".join(
        f"""
        select distinct {metric}_currency as currency,
               cast(publication_date as varchar) as rate_date
        from {qualified}
        where {metric}_currency <> '' and publication_date is not null
        """
        for qualified, metrics in targets
        for metric, _ in metrics
    )
    pairs = duckdb_connection.execute(pair_query).fetchall()
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

    counts = {"rate_pairs": len(pairs), "rates_found": len(fx_rows)}
    for qualified, metrics in targets:
        for metric, _ in metrics:
            duckdb_connection.execute(
                f"""
                update {qualified} as records
                set {metric}_amount_usd = cast(
                        records.{metric}_amount_original * fx.fx_rate as decimal(38, 2)
                    ),
                    fx_rate_to_usd = fx.fx_rate,
                    fx_rate_date = fx.fx_rate_date,
                    fx_source = fx.fx_source
                from _ted_fx as fx
                where fx.currency = records.{metric}_currency
                  and fx.rate_date = records.publication_date
                """
            )
            # Per metric, so a figure that silently stops converting shows up in
            # the asset's metadata rather than only in whatever the view reads.
            table_name = qualified.split(".")[-1]
            counts[f"{table_name}.{metric}_converted"] = int(
                duckdb_connection.execute(
                    f"select count(*) from {qualified} "
                    f"where {metric}_amount_usd is not null"
                ).fetchone()[0]
            )
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
        tables=(
            tables.TED_NOTICES_TABLE,
            tables.TED_NOTICE_LOTS_TABLE,
            tables.TED_NOTICE_WINNERS_TABLE,
        ),
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
                tables.NOTICE_LOTS_TABLE,
                tables.TED_NOTICE_LOTS_TABLE,
                tables.TED_NOTICE_LOTS_COLUMNS,
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
