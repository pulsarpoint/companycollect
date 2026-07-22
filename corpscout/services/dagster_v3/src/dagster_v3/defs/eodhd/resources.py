import threading
from typing import Any

import dagster as dg
import requests
from pydantic import PrivateAttr
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

EODHD_API_TOKEN_ENV = "EODHD_API_TOKEN"
EODHD_API_BASE_URL = "https://eodhd.com/api"
DEFAULT_EODHD_TIMEOUT_SECONDS = 120
DEFAULT_EODHD_USER_AGENT = "corpscout-dagster-v3-eodhd/0.1"


class EodhdResource(dg.ConfigurableResource):
    """Authenticated EODHD HTTP boundary used by reference and price assets."""

    api_token: str = dg.EnvVar(EODHD_API_TOKEN_ENV)
    base_url: str = EODHD_API_BASE_URL
    timeout_seconds: int = DEFAULT_EODHD_TIMEOUT_SECONDS
    user_agent: str = DEFAULT_EODHD_USER_AGENT

    _session_override: Any = PrivateAttr(default=None)
    _thread_local: threading.local = PrivateAttr(default_factory=threading.local)

    def __init__(self, session: Any = None, **data: Any) -> None:
        super().__init__(**data)
        self._session_override = session

    def exchanges(self) -> list[dict[str, Any]]:
        return self._get_json_list("exchanges-list/", params={})

    def symbols(
        self,
        exchange_code: str,
        *,
        delisted: bool,
    ) -> list[dict[str, Any]]:
        return self._get_json_list(
            f"exchange-symbol-list/{exchange_code}",
            params={"delisted": 1 if delisted else 0},
        )

    def prices(
        self,
        symbol_key: str,
        *,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        return self._get_json_list(
            f"eod/{symbol_key}",
            params={
                "from": start_date,
                "to": end_date,
                "period": "d",
                "order": "a",
            },
        )

    def _get_json_list(
        self,
        path: str,
        *,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        request_params = {
            **params,
            "api_token": _resolve_env_value(self.api_token),
            "fmt": "json",
        }
        response = self._session().get(
            f"{self.base_url.rstrip('/')}/{path.lstrip('/')}",
            params=request_params,
            headers={"User-Agent": self.user_agent, "Accept": "application/json"},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError(
                f"EODHD endpoint {path} returned {type(payload).__name__}, expected a list"
            )
        if any(not isinstance(row, dict) for row in payload):
            raise ValueError(f"EODHD endpoint {path} returned a non-object row")
        return payload

    def _session(self) -> Any:
        if self._session_override is not None:
            return self._session_override
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            retries = Retry(
                total=4,
                backoff_factor=2.0,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset({"GET"}),
                respect_retry_after_header=True,
            )
            session.mount("https://", HTTPAdapter(max_retries=retries))
            self._thread_local.session = session
        return session


def _resolve_env_value(value: Any) -> Any:
    get_value = getattr(value, "get_value", None)
    if callable(get_value):
        return get_value()
    return value
