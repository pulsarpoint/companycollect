from __future__ import annotations

import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dagster as dg
import requests
from pydantic import PrivateAttr

from dagster_v3.defs.estonia_ar import resources as ar_resources
from dagster_v3.defs.estonia_ar import tables

LOGGER = logging.getLogger(__name__)

EE_FINANCIAL_INDEX_URL = (
    "https://avaandmed.ariregister.rik.ee/en/downloading-open-data"
)
DEFAULT_TIMEOUT_SECONDS = ar_resources.DEFAULT_TIMEOUT_SECONDS
DEFAULT_USER_AGENT = ar_resources.DEFAULT_USER_AGENT

_FINANCIAL_FILE_PATTERNS: dict[str, str] = {
    tables.REPORT_GENERAL_RAW_TABLE: r"1\.aruannete_yldandmed_kuni_\d+(?:_\d+)?\.zip",
    **{
        tables.key_indicators_raw_table(year): (
            rf"4\.{year}_aruannete_elemendid_kuni_\d+(?:_\d+)?\.zip"
        )
        for year in tables.EE_FINANCIAL_YEARS
    },
}


class EstoniaFinancialResource(dg.ConfigurableResource):
    """HTTP boundary for Estonia annual-report financial bulk files."""

    index_url: str = EE_FINANCIAL_INDEX_URL
    user_agent: str = DEFAULT_USER_AGENT
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS

    _session: Any | None = PrivateAttr(default=None)

    def __init__(self, session: Any | None = None, **data: Any) -> None:
        super().__init__(**data)
        self._session = session

    def session(self) -> Any:
        if self._session is None:
            session = requests.Session()
            session.headers["User-Agent"] = self.user_agent
            self._session = session
        return self._session

    def resolve_financial_url(
        self,
        raw_table: str,
        *,
        log: Callable[..., object] | None = None,
    ) -> str:
        return resolve_financial_url(
            raw_table,
            session=self.session(),
            index_url=self.index_url,
            timeout_seconds=self.timeout_seconds,
            log=log,
        )

    def download_financial_zip(
        self,
        *,
        download_url: str,
        dest: Path,
        log: Callable[..., None] | None = None,
    ) -> None:
        ar_resources._download_to_path(
            url=download_url,
            dest=dest,
            timeout_seconds=self.timeout_seconds,
            user_agent=self.user_agent,
            session=self.session(),
            log=log,
        )


def resolve_financial_url(
    raw_table: str,
    *,
    session: Any | None = None,
    index_url: str = EE_FINANCIAL_INDEX_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    log: Callable[..., object] | None = None,
) -> str:
    """Resolve the current download URL for a financial raw table from the index.

    The financial snapshot filenames rotate with a cumulative datestamp and
    optional Drupal suffix. Fall back to pinned constants if the index is not
    reachable or does not contain the expected file.
    """
    pinned = tables.EE_FINANCIAL_RAW_SOURCES[raw_table]
    warn = log or LOGGER.warning
    pattern = _FINANCIAL_FILE_PATTERNS.get(raw_table)
    if pattern is None:
        return pinned
    try:
        http = session or requests.Session()
        response = http.get(index_url, timeout=timeout_seconds)
        response.raise_for_status()
        html = getattr(response, "text", None) or response.content.decode(
            "utf-8", "replace"
        )
    except Exception as exc:  # noqa: BLE001 - stale index must not block a load
        warn("Estonia AR index fetch failed (%s); using pinned URL for %s", exc, raw_table)
        return pinned
    match = re.search(pattern, html)
    if match is None:
        warn("Estonia AR index missing %s; using pinned URL", raw_table)
        return pinned
    return f"{tables.EE_FINANCIAL_BASE_URL}/{match.group(0)}"
