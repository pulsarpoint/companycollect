"""B3's listed-issuer register.

The Brazilian answer to a question the EU chain cannot reach: which company
does a ticker belong to. B3 publishes CNPJ, the trading-code root and the CVM
code in one record, so the link is register-verified rather than name-matched.

The endpoint is a public JSON API with its parameters base64-encoded into the
path -- B3's own site calls it that way. No key, no auth.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from dlt.sources.helpers import requests

B3_LISTED_COMPANIES_URL = (
    "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/"
    "CompanyCall/GetInitialCompanies"
)

# B3 rejects a page size much above this and pages are cheap.
PAGE_SIZE = 100

# What B3 reports for an issuer that has never listed a share -- a debenture
# issuer, typically. 1,976 of 2,512 real-CNPJ records carry it.
NEVER_LISTED = "31/12/9999"


@dataclass(frozen=True)
class B3Listing:
    cvm_code: str
    cnpj: str
    cnpj_basico: str
    ticker_root: str
    company_name: str
    trading_name: str
    market: str
    segment: str
    listing_date: date | None
    status: str


def _text(value: Any) -> str:
    return str(value or "").strip()


def _digits(value: Any) -> str:
    return "".join(c for c in str(value or "") if c.isdigit())


def _listing_date(value: Any) -> date | None:
    """B3's dd/mm/yyyy, or None for the never-listed sentinel."""
    text = _text(value)
    if not text or text == NEVER_LISTED:
        return None
    try:
        return datetime.strptime(text, "%d/%m/%Y").date()
    except ValueError:
        return None


def parse_b3_listing(entry: dict) -> B3Listing | None:
    """One issuer, or None when the record identifies nothing.

    A record with neither a CNPJ nor a trading code cannot be joined to a
    company or to a price series, so it is dropped rather than stored as a row
    that can only ever be a dead end.
    """
    cnpj = _digits(entry.get("cnpj"))
    # B3 reports '0' for the ETFs and BDRs it lists, which are not Brazilian
    # companies and have no CNPJ to resolve.
    if len(cnpj) != 14:
        cnpj = ""
    ticker_root = _text(entry.get("issuingCompany")).upper()
    if not cnpj and not ticker_root:
        return None

    return B3Listing(
        cvm_code=_text(entry.get("codeCVM")),
        cnpj=cnpj,
        cnpj_basico=cnpj[:8],
        ticker_root=ticker_root,
        company_name=_text(entry.get("companyName")),
        trading_name=_text(entry.get("tradingName")),
        market=_text(entry.get("market")),
        segment=_text(entry.get("segmentEng")) or _text(entry.get("segment")),
        listing_date=_listing_date(entry.get("dateListing")),
        status=_text(entry.get("status")),
    )


def _page_url(page: int, page_size: int) -> str:
    params = json.dumps(
        {"language": "pt-br", "pageNumber": page, "pageSize": page_size},
        separators=(",", ":"),
    )
    return f"{B3_LISTED_COMPANIES_URL}/{base64.b64encode(params.encode()).decode()}"


def fetch_b3_listings(*, page_size: int = PAGE_SIZE, timeout: int = 60) -> Iterator[dict]:
    """Every page of the register, following B3's own totalPages."""
    page = 1
    while True:
        response = requests.get(
            _page_url(page, page_size),
            headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        yield from payload.get("results") or []
        total_pages = int((payload.get("page") or {}).get("totalPages") or 0)
        if page >= total_pages:
            return
        page += 1


def build_b3_listing_rows(
    entries: list[dict],
    *,
    source_run_id: str,
    retrieved_at: datetime | None = None,
) -> list[tuple]:
    """Rows for `br_b3_listings`, in the migration's column order."""
    stamped = retrieved_at or datetime.now(UTC).replace(tzinfo=None)
    rows: list[tuple] = []
    seen: set[tuple[str, str, str]] = set()
    for entry in entries:
        listing = parse_b3_listing(entry)
        if listing is None:
            continue
        # B3 pages can repeat an issuer across requests; the table is a register,
        # not a log.
        key = (listing.cnpj_basico, listing.ticker_root, listing.cvm_code)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            (
                listing.cvm_code,
                listing.cnpj,
                listing.cnpj_basico,
                listing.ticker_root,
                listing.company_name,
                listing.trading_name,
                listing.market,
                listing.segment,
                listing.listing_date,
                listing.status,
                B3_LISTED_COMPANIES_URL,
                source_run_id,
                stamped,
            )
        )
    return rows


B3_COMPANY_DETAIL_URL = (
    "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/"
    "CompanyCall/GetDetail"
)


@dataclass(frozen=True)
class B3Instrument:
    cvm_code: str
    cnpj: str
    cnpj_basico: str
    ticker: str
    isin: str
    ticker_root: str


def _detail_payload(body: Any) -> dict:
    """B3 returns the detail as a JSON STRING inside a JSON response.

    Decoded twice rather than once, and tolerant of the endpoint being fixed:
    a plain object is accepted as-is, and a single-element list is unwrapped.
    """
    if isinstance(body, str):
        body = json.loads(body)
    if isinstance(body, list):
        body = body[0] if body else {}
    return body if isinstance(body, dict) else {}


def parse_b3_instruments(body: Any) -> tuple[B3Instrument, ...]:
    """Every (ticker, ISIN) pair one company lists.

    `otherCodes` is the authoritative mapping. The company's own `code` is
    folded in too: B3 reports it separately and it is occasionally absent from
    otherCodes, which would silently lose a company's main line.
    """
    detail = _detail_payload(body)
    cnpj = _digits(detail.get("cnpj"))
    if len(cnpj) != 14:
        cnpj = ""
    root = _text(detail.get("issuingCompany")).upper()
    cvm_code = _text(detail.get("codeCVM"))

    pairs: dict[str, str] = {}
    for entry in detail.get("otherCodes") or []:
        if not isinstance(entry, dict):
            continue
        ticker = _text(entry.get("code")).upper()
        if ticker:
            pairs.setdefault(ticker, _text(entry.get("isin")).upper())
    main = _text(detail.get("code")).upper()
    if main:
        pairs.setdefault(main, "")

    return tuple(
        B3Instrument(
            cvm_code=cvm_code,
            cnpj=cnpj,
            cnpj_basico=cnpj[:8],
            ticker=ticker,
            isin=isin,
            ticker_root=root,
        )
        for ticker, isin in sorted(pairs.items())
    )


def fetch_b3_company_detail(code_cvm: str, *, timeout: int = 45) -> Any:
    """One company's detail, including its trading codes and their ISINs."""
    params = json.dumps(
        {"codeCVM": str(code_cvm), "language": "pt-br"}, separators=(",", ":")
    )
    url = f"{B3_COMPANY_DETAIL_URL}/{base64.b64encode(params.encode()).decode()}"
    response = requests.get(
        url,
        headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def build_b3_instrument_rows(
    instruments: list[B3Instrument],
    *,
    source_run_id: str,
    retrieved_at: datetime | None = None,
) -> list[tuple]:
    """Rows for `br_b3_instruments`, in the migration's column order."""
    stamped = retrieved_at or datetime.now(UTC).replace(tzinfo=None)
    seen: set[tuple[str, str]] = set()
    rows: list[tuple] = []
    for item in instruments:
        key = (item.cnpj_basico, item.ticker)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            (
                item.cvm_code,
                item.cnpj,
                item.cnpj_basico,
                item.ticker,
                item.isin,
                item.ticker_root,
                B3_COMPANY_DETAIL_URL,
                source_run_id,
                stamped,
            )
        )
    return rows
