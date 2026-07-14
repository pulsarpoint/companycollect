from typing import Any, Protocol

from dagster_v3.defs.slovakia_financials import tables

# A browser-like UA is mandatory — the RÚZ F5 WAF rejects default/empty UAs.
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
DEFAULT_TIMEOUT_SECONDS = 60


class HttpSession(Protocol):
    def get(self, url: str, *, params: dict[str, Any], timeout: int, headers: dict[str, str]) -> Any:
        ...


class RuzClient:
    """Thin client over the RÚZ open REST API (cruz-public/api)."""

    def __init__(
        self,
        *,
        session: HttpSession | None = None,
        base_url: str = tables.RUZ_BASE_URL,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        if session is None:
            from dlt.sources.helpers import requests as dlt_requests

            session = dlt_requests.Session()
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        response = self._session.get(
            f"{self._base_url}/{path}",
            params=params,
            timeout=self._timeout,
            headers={"User-Agent": BROWSER_USER_AGENT, "Accept": "application/json"},
        )
        response.raise_for_status()
        return response.json()

    def statement_ids(
        self, *, changed_since: str, after_id: int, max_records: int
    ) -> tuple[list[int], bool]:
        """Page financial-statement ids changed since a date, after a cursor id.

        Returns (ids, has_more). Ids are ascending and strictly greater than
        `after_id`, which makes this a resumable forward sweep.
        """
        payload = self._get(
            "uctovne-zavierky",
            {
                "zmenene-od": changed_since,
                "pokracovat-za-id": after_id,
                "max-zaznamov": max_records,
            },
        )
        ids = [int(i) for i in (payload.get("id") or [])]
        return ids, bool(payload.get("existujeDalsieId"))

    def statement(self, statement_id: int) -> dict[str, Any]:
        return self._get("uctovna-zavierka", {"id": statement_id})

    def entity(self, entity_id: int) -> dict[str, Any]:
        return self._get("uctovna-jednotka", {"id": entity_id})

    def report(self, report_id: int) -> dict[str, Any]:
        return self._get("uctovny-vykaz", {"id": report_id})

    def template(self, template_id: int) -> dict[str, Any]:
        return self._get("sablona", {"id": template_id})
