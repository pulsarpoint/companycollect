from __future__ import annotations

from typing import Any, Protocol
from urllib.parse import urlencode

import dagster as dg
from dlt.sources.helpers.requests import Client as DltRequestsClient
from pydantic import PrivateAttr


class HttpSession(Protocol):
    headers: dict[str, str]

    def get(self, url: str, params: dict[str, Any] | None = None, timeout: int = 120) -> Any:
        ...


class XbrlApiResource(dg.ConfigurableResource):
    base_url: str = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
    user_agent: str = "corpscout-dagster-v3-dev/0.1"
    timeout_seconds: int = 120
    max_retries: int = 5
    retry_initial_delay_seconds: float = 10.0
    retry_max_delay_seconds: float = 120.0

    _session: HttpSession | None = PrivateAttr(default=None)

    def __init__(self, session: HttpSession | None = None, **data: Any) -> None:
        super().__init__(**data)
        self._session = session

    def session(self) -> HttpSession:
        if self._session is None:
            self._session = DltRequestsClient(
                request_timeout=self.timeout_seconds,
                request_max_attempts=self.max_retries,
                request_backoff_factor=self.retry_initial_delay_seconds,
                request_max_retry_delay=self.retry_max_delay_seconds,
                respect_retry_after_header=True,
                session_attrs={"headers": {"User-Agent": self.user_agent}},
            )
        return self._session

    def statement_xml_url(self, business_id: str, financial_date: str) -> str:
        query = urlencode({"businessId": business_id, "financialDate": financial_date})
        return f"{self.base_url}/financial?{query}"

    def download_statement_xml(self, business_id: str, financial_date: str) -> tuple[bytes, str]:
        response = self.session().get(
            f"{self.base_url}/financial",
            params={"businessId": business_id, "financialDate": financial_date},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.content, self.statement_xml_url(business_id, financial_date)
