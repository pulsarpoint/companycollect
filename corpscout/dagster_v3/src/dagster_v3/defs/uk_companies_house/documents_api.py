"""On-demand 'latest accounts per company' via the Companies House API.

Unlike the bulk Accounts Data Product (one day's filings per archive), this fetches
the *latest* accounts iXBRL for a specific company on demand:
  Filing History API  ->  newest accounts filing's document_metadata
  Document API        ->  the iXBRL content
  xbrl_common.parser  ->  metrics
Rate-limited (600 req / 5 min ~= 2 calls/company), so it targets a provided list of
company numbers — not all 5.7M (that's the archive-accumulation path).
"""

import logging
import os
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.uk_companies_house import financials
from dagster_v3.defs.xbrl_common.parser import parse_ixbrl

LOGGER = logging.getLogger(__name__)

API_BASE = "https://api.company-information.service.gov.uk"
API_KEY_ENV = "COMPANY_HOUSE"
# iXBRL content types, in preference order. Many filings are PDF-only (no XBRL) —
# those carry no machine-readable figures and are skipped.
IXBRL_CONTENT_TYPES = ("application/xhtml+xml", "application/xml")


class CompaniesHouseClient:
    """Thin Companies House REST client (HTTP Basic: api_key as username)."""

    def __init__(self, api_key: str, *, session: Any = None, timeout_seconds: int = 60):
        self._api_key = api_key
        self._session = session or dlt_requests.Session()
        self._timeout = timeout_seconds

    @classmethod
    def from_env(cls, *, session: Any = None) -> "CompaniesHouseClient":
        key = os.environ.get(API_KEY_ENV)
        if not key:
            raise RuntimeError(f"{API_KEY_ENV} environment variable is not set")
        return cls(key, session=session)

    def _get_json(self, url: str) -> Any:
        resp = self._session.get(
            url, auth=(self._api_key, ""), timeout=self._timeout
        )
        resp.raise_for_status()
        return resp.json()

    def _get_content(self, url: str, *, accept: str) -> bytes:
        resp = self._session.get(
            url,
            auth=(self._api_key, ""),
            timeout=self._timeout,
            headers={"Accept": accept},
        )
        resp.raise_for_status()
        return resp.content

    def latest_accounts_ixbrl(self, company_number: str) -> bytes | None:
        """Return the iXBRL bytes of the company's most recent accounts filing.

        Walks accounts filings newest-first; for each it reads the document
        metadata and only fetches when an iXBRL content type is actually offered
        (PDF-only filings carry no machine-readable figures and are skipped).
        """
        cn = company_number.strip()
        history = self._get_json(
            f"{API_BASE}/company/{cn}/filing-history?category=accounts&items_per_page=100"
        )
        items = history.get("items") or []
        items.sort(key=lambda it: it.get("date", ""), reverse=True)
        for item in items:
            meta_url = (item.get("links") or {}).get("document_metadata")
            if not meta_url:
                continue
            try:
                meta = self._get_json(meta_url)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("CH document metadata failed for %s: %s", cn, exc)
                continue
            resources = meta.get("resources") or {}
            content_type = next(
                (ct for ct in IXBRL_CONTENT_TYPES if ct in resources), None
            )
            if content_type is None:
                continue  # PDF-only -> no XBRL to parse
            document_url = (meta.get("links") or {}).get("document") or f"{meta_url}/content"
            try:
                return self._get_content(document_url, accept=content_type)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("CH document fetch failed for %s: %s", cn, exc)
                continue
        return None


def build_financials_for_company_numbers(
    *,
    database_path: str | Path,
    company_numbers: Iterable[str],
    source_run_id: str,
    client: CompaniesHouseClient,
    request_delay_seconds: float = 0.5,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Fetch + parse the latest accounts for each company → native-GBP metrics table."""
    numbers = [str(n).strip() for n in company_numbers if str(n).strip()]
    rows: list[tuple[Any, ...]] = []
    fetched = 0
    missing = 0
    for index, cn in enumerate(numbers):
        if index and request_delay_seconds:
            time.sleep(request_delay_seconds)
        try:
            content = client.latest_accounts_ixbrl(cn)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("CH filing-history failed for %s: %s", cn, exc)
            missing += 1
            continue
        if not content:
            missing += 1
            continue
        extracted = financials._extract_metrics(content)
        if extracted is None:
            missing += 1
            continue
        company_number, period_end, metrics = extracted
        rows.append(financials.metrics_row(company_number, period_end, metrics))
        fetched += 1

    counts = financials.write_metrics_table(
        database_path=database_path,
        rows=rows,
        source_run_id=source_run_id,
        source_slug="uk_companies_house_accounts_api",
        allow_empty=True,
    )
    counts.update({"requested": len(numbers), "fetched": fetched, "missing": missing})
    if log is not None:
        log(
            "Fetched UK latest accounts via API: requested=%s fetched=%s missing=%s",
            len(numbers),
            fetched,
            missing,
        )
    return counts
