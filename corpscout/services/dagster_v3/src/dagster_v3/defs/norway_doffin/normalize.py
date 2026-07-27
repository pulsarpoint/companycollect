"""Turning a Doffin search hit plus its notice XML into candidate rows.

Kept free of I/O so the projection can be tested against real notices without a
network or a warehouse. The asset supplies the two halves; this decides what a
row says.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from dagster_v3.defs.norway_doffin import tables
from dagster_v3.defs.norway_doffin.parser import (
    Money,
    NoticeAmounts,
    iter_notice_winner_rows,
    value_for_winner,
)

# cbc:RegulatoryDomain, which Doffin publishes rather than leaving to inference.
# Anything else -- including 'other', meaning nationally regulated -- is 'no'.
_EU_PROCUREMENT_DIRECTIVES = frozenset(
    {
        "32014L0024",  # public sector
        "32014L0025",  # utilities
        "32014L0023",  # concessions
        "32009L0081",  # defence
    }
)


def directive_governed(regulatory_domain: str) -> str:
    """'yes' | 'no' | '' -- and the empty case is meaningful.

    Norway is in the EEA and follows the EU procurement directives, so unlike
    Brazil this is a real question. Doffin answers it outright, so an empty
    string here means the notice did not state it, not that we did not look.
    """
    if not regulatory_domain:
        return ""
    return "yes" if regulatory_domain in _EU_PROCUREMENT_DIRECTIVES else "no"


def _date(value: Any) -> str | None:
    """Doffin mixes a bare date with a full timestamp across its date fields."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return None


def _amount(money: Money) -> tuple[str | None, str]:
    return (money.amount or None), money.currency


def build_candidate_rows(
    *,
    hit: dict,
    amounts: NoticeAmounts,
    source_run_id: str,
    partition_key: str,
    retrieved_at: datetime,
    resolved_at: datetime,
) -> list[tuple]:
    """One row per (notice, lot, winner), in CANDIDATE_COLUMNS order."""
    doffin_id = str(hit.get("id") or "")
    buyers = hit.get("buyer") or []
    buyer = buyers[0] if buyers else {}

    estimated = hit.get("estimatedValue") or {}
    estimated_amount = estimated.get("amount")
    estimated_money = Money(
        amount="" if estimated_amount is None else f"{estimated_amount:.2f}",
        currency=str(estimated.get("currencyCode") or "").upper(),
    )

    notice_value_amount, notice_value_currency = _amount(amounts.notice_value)
    estimated_value_amount, estimated_value_currency = _amount(estimated_money)

    rows: list[tuple] = []
    for row in iter_notice_winner_rows(hit):
        value_amount, value_currency = _amount(
            value_for_winner(amounts, row.winner_org_number_raw)
        )
        rows.append(
            (
                tables.COUNTRY_CODE,
                tables.SOURCE_SLUG,
                source_run_id,
                # Unique per (notice, lot, winner). The ordinal is what makes it
                # unique when one company wins the same lot twice, which happens.
                f"{doffin_id}:{row.lot_id}:{row.winner_ordinal}",
                tables.NOTICE_URL_TEMPLATE.format(doffin_id=doffin_id),
                doffin_id,
                amounts.contract_folder_id,
                str(hit.get("type") or ""),
                str(hit.get("status") or ""),
                _date(hit.get("issueDate")),
                _date(hit.get("publicationDate")),
                _date(hit.get("deadline")),
                str(buyer.get("name") or ""),
                str(buyer.get("organizationId") or ""),
                str(hit.get("heading") or ""),
                str(hit.get("description") or ""),
                [str(code) for code in (hit.get("cpvCodes") or [])],
                [str(location) for location in (hit.get("locationId") or [])],
                row.lot_id,
                row.lot_heading,
                row.winner_ordinal,
                row.winner_name,
                row.winner_org_number_raw,
                row.winner_org_number,
                # Doffin publishes no winner country. Norwegian is inferred from
                # the org number normalising, and left blank otherwise rather
                # than guessed -- a Swedish number does not say 'SE' anywhere.
                "NO" if row.winner_org_number else "",
                value_amount,
                None,  # value_amount_usd, filled by the separate FX step
                value_currency,
                notice_value_amount,
                None,  # notice_value_amount_usd
                notice_value_currency,
                estimated_value_amount,
                None,  # estimated_value_amount_usd
                estimated_value_currency,
                None,  # fx_rate_to_usd
                None,  # fx_rate_date
                "",  # fx_source
                directive_governed(amounts.regulatory_domain),
                int(hit.get("receivedTenders") or 0),
                # Whether a winner was named at all, which is not the same as
                # whether we could match them to a company.
                "winner_selected" if row.winner_org_number_raw else "no_winner_named",
                partition_key,
                retrieved_at,
                resolved_at,
            )
        )
    return rows


CANDIDATE_DDL = """
    country_code varchar,
    source_slug varchar,
    source_run_id varchar,
    source_record_id varchar,
    source_url varchar,
    doffin_id varchar,
    contract_folder_id varchar,
    notice_type varchar,
    notice_status varchar,
    issue_date date,
    publication_date date,
    deadline_date date,
    buyer_name varchar,
    buyer_org_number varchar,
    notice_title varchar,
    notice_description varchar,
    cpv_codes varchar[],
    location_ids varchar[],
    lot_id varchar,
    lot_heading varchar,
    winner_ordinal integer,
    winner_name varchar,
    winner_org_number_raw varchar,
    winner_org_number varchar,
    winner_country varchar,
    value_amount_original decimal(38, 2),
    value_amount_usd decimal(38, 2),
    value_currency varchar,
    notice_value_amount_original decimal(38, 2),
    notice_value_amount_usd decimal(38, 2),
    notice_value_currency varchar,
    estimated_value_amount_original decimal(38, 2),
    estimated_value_amount_usd decimal(38, 2),
    estimated_value_currency varchar,
    fx_rate_to_usd decimal(38, 12),
    fx_rate_date date,
    fx_source varchar,
    directive_governed varchar,
    received_tenders integer,
    award_result varchar,
    partition_key varchar,
    source_retrieved_at timestamp,
    resolved_at timestamp
"""


def write_candidates(connection: Any, rows: list[tuple]) -> int:
    """Replace the candidates table with this partition's rows.

    Replaces rather than appends, matching every other source in this repo: the
    DuckDB file is scratch feeding the very next export, not an accumulating
    store, and the export replaces one partition in ClickHouse.
    """
    connection.execute(f"create schema if not exists {tables.DUCKDB_SCHEMA}")
    qualified = f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
    connection.execute(f"create or replace table {qualified} ({CANDIDATE_DDL})")
    if rows:
        placeholders = ", ".join("?" * len(tables.CANDIDATE_COLUMNS))
        connection.executemany(
            f"insert into {qualified} values ({placeholders})", rows
        )
    return int(
        connection.execute(f"select count(*) from {qualified}").fetchone()[0]
    )
