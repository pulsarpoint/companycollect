from dataclasses import dataclass
from time import monotonic
from typing import Any
from uuid import uuid4

import dagster as dg
import requests
from dlt.sources.helpers import requests as dlt_requests
from pydantic import Field, PrivateAttr

TOKEN_URL = "https://portal.api.bolagsverket.se/oauth2/token"
API_BASE_URL = "https://gw.api.bolagsverket.se/vardefulla-datamangder/v1"
TOKEN_SCOPE = "vardefulla-datamangder:read vardefulla-datamangder:ping"


@dataclass(frozen=True)
class ApiResponse:
    content: bytes
    request_id: str
    status_code: int


class BolagsverketVdmApiError(RuntimeError):
    """Safe API-boundary error that never includes credentials or response bodies."""


class BolagsverketVdmResource(dg.ConfigurableResource):
    """Small authenticated client for targeted Valuable Datasets refreshes."""

    client_id: str = dg.EnvVar("BOLAGSVERKET_VDM_CLIENT_ID")
    client_secret: str = dg.EnvVar("BOLAGSVERKET_VDM_CLIENT_SECRET")
    token_url: str = TOKEN_URL
    api_base_url: str = API_BASE_URL
    timeout_seconds: float = Field(default=30, gt=0, le=120)
    max_attempts: int = Field(default=4, ge=1, le=6)
    retry_backoff_seconds: float = Field(default=1, ge=0, le=30)
    max_retry_delay_seconds: float = Field(default=15, ge=0, le=60)

    _session: Any = PrivateAttr(default=None)
    _access_token: str | None = PrivateAttr(default=None)
    _access_token_expires_at: float = PrivateAttr(default=0)

    def __init__(self, session: Any = None, **data: Any) -> None:
        super().__init__(**data)
        self._session = session

    def session(self) -> Any:
        if self._session is None:
            self._session = dlt_requests.Client(
                request_timeout=self.timeout_seconds,
                request_max_attempts=self.max_attempts,
                request_backoff_factor=self.retry_backoff_seconds,
                request_max_retry_delay=self.max_retry_delay_seconds,
                respect_retry_after_header=True,
            ).session
        return self._session

    def fetch_organisationer(self, company_id: str) -> ApiResponse:
        return self._post_json("organisationer", company_id)

    def fetch_dokumentlista(self, company_id: str) -> ApiResponse:
        return self._post_json("dokumentlista", company_id)

    def _post_json(self, endpoint: str, company_id: str) -> ApiResponse:
        request_id = str(uuid4())
        response: Any = None
        try:
            response = self.session().post(
                f"{self.api_base_url}/{endpoint}",
                json={"identitetsbeteckning": company_id},
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self._token()}",
                    "X-Request-ID": request_id,
                },
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
        except BolagsverketVdmApiError:
            raise
        except requests.RequestException as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            raise BolagsverketVdmApiError(
                "Bolagsverket API request failed: "
                f"endpoint={endpoint} request_id={request_id} status={status_code}"
            ) from None
        except Exception:
            status_code = getattr(response, "status_code", None)
            raise BolagsverketVdmApiError(
                "Bolagsverket API request failed: "
                f"endpoint={endpoint} request_id={request_id} status={status_code}"
            ) from None
        return ApiResponse(
            content=bytes(response.content),
            request_id=request_id,
            status_code=int(response.status_code),
        )

    def _token(self) -> str:
        if (
            self._access_token is not None
            and monotonic() < self._access_token_expires_at - 60
        ):
            return self._access_token

        response: Any = None
        try:
            response = self.session().post(
                self.token_url,
                auth=(self.client_id, self.client_secret),
                data={"grant_type": "client_credentials", "scope": TOKEN_SCOPE},
                headers={"Accept": "application/json"},
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            access_token = payload.get("access_token")
            if not isinstance(access_token, str) or not access_token:
                raise ValueError("missing access token")
            expires_in = payload.get("expires_in", 3600)
            if isinstance(expires_in, bool) or not isinstance(expires_in, (int, float)):
                expires_in = 3600
        except Exception as exc:
            status_code = getattr(response, "status_code", None)
            if isinstance(exc, requests.RequestException):
                status_code = getattr(
                    getattr(exc, "response", None), "status_code", status_code
                )
            raise BolagsverketVdmApiError(
                f"Bolagsverket OAuth token request failed: status={status_code}"
            ) from None

        self._access_token = access_token
        self._access_token_expires_at = monotonic() + max(float(expires_in), 0)
        return access_token

    def teardown_after_execution(self, context: dg.InitResourceContext) -> None:
        if self._session is not None:
            self._session.close()
        self._session = None
        self._access_token = None
        self._access_token_expires_at = 0
