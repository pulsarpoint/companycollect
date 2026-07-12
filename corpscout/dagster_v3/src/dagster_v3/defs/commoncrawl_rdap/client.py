from collections.abc import Mapping
from ipaddress import ip_network
from typing import Any
from urllib.parse import unquote, urlsplit

import requests
import whoisit
from whoisit.errors import (
    ArgumentError,
    BootstrapError,
    ParseError,
    QueryError,
    RateLimitedError,
    RemoteServerError,
    ResourceAccessDeniedError,
    ResourceDoesNotExist,
    UnsupportedError,
)

from dagster_v3.defs.commoncrawl_rdap.rdap import RdapLookupResponse


class RdapClientError(Exception):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        retryable: bool,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.status_code = status_code


class RdapClient:
    def __init__(
        self,
        *,
        user_agent: str,
        session: requests.Session | None = None,
    ) -> None:
        if user_agent.strip() == "":
            raise ValueError("user_agent must not be empty")
        self._owns_session = session is None
        self._session = session if session is not None else requests.Session()
        self._session.headers["User-Agent"] = user_agent.strip()
        self._ready = False

    def lookup_ip(self, ip_address_or_network: str) -> RdapLookupResponse:
        return self._lookup(ip_address_or_network, rir=None)

    def lookup_up_url(self, up_url: str, *, rir: str) -> RdapLookupResponse:
        return self._lookup(ip_resource_from_up_url(up_url), rir=rir)

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def _lookup(
        self,
        ip_address_or_network: str,
        *,
        rir: str | None,
    ) -> RdapLookupResponse:
        self._ensure_bootstrapped()
        try:
            response = whoisit.ip(
                ip_address_or_network,
                rir=rir,
                include_raw=True,
                session=self._session,
            )
        except ResourceDoesNotExist as error:
            raise _client_error(error, code="not_found", retryable=False) from error
        except RateLimitedError as error:
            raise _client_error(error, code="rate_limited", retryable=True) from error
        except RemoteServerError as error:
            raise _client_error(error, code="remote_server", retryable=True) from error
        except ResourceAccessDeniedError as error:
            raise _client_error(error, code="access_denied", retryable=False) from error
        except UnsupportedError as error:
            raise RdapClientError(
                str(error),
                code="unsupported",
                retryable=False,
            ) from error
        except QueryError as error:
            retryable = _query_error_is_retryable(error)
            raise _client_error(
                error, code="query_error", retryable=retryable
            ) from error
        except (ArgumentError, ParseError) as error:
            raise RdapClientError(
                str(error),
                code="invalid_response",
                retryable=False,
            ) from error
        except requests.RequestException as error:
            raise RdapClientError(
                str(error),
                code="transport_error",
                retryable=True,
            ) from error
        return _lookup_response(response, requested_rir=rir)

    def _ensure_bootstrapped(self) -> None:
        if self._ready:
            return
        try:
            if not whoisit.is_bootstrapped():
                whoisit.bootstrap()
        except BootstrapError as error:
            raise RdapClientError(
                str(error),
                code="bootstrap_error",
                retryable=True,
            ) from error
        except requests.RequestException as error:
            raise RdapClientError(
                str(error),
                code="bootstrap_transport_error",
                retryable=True,
            ) from error
        self._ready = True


def ip_resource_from_up_url(up_url: str) -> str:
    parsed = urlsplit(up_url)
    if parsed.scheme not in {"http", "https"} or parsed.netloc == "":
        raise ValueError(f"Invalid RDAP up URL: {up_url!r}")
    segments = unquote(parsed.path).strip("/").split("/")
    try:
        ip_index = segments.index("ip")
    except ValueError as error:
        raise ValueError(f"RDAP up URL has no IP resource: {up_url!r}") from error
    resource = "/".join(segments[ip_index + 1 :])
    if resource == "":
        raise ValueError(f"RDAP up URL has no IP resource: {up_url!r}")
    try:
        return str(ip_network(resource, strict=False))
    except ValueError as error:
        raise ValueError(
            f"RDAP up URL contains an invalid IP resource: {up_url!r}"
        ) from error


def _lookup_response(
    response: Any,
    *,
    requested_rir: str | None,
) -> RdapLookupResponse:
    if not isinstance(response, Mapping):
        raise RdapClientError(
            "RDAP client returned a non-object response",
            code="invalid_response",
            retryable=False,
        )
    raw = response.get("raw")
    if not isinstance(raw, Mapping):
        raise RdapClientError(
            "RDAP client response did not include raw RDAP data",
            code="invalid_response",
            retryable=False,
        )
    response_rir = response.get("rir")
    rir = str(response_rir).strip() if response_rir is not None else ""
    if rir == "" and requested_rir is not None:
        rir = requested_rir.strip()
    if rir == "":
        raise RdapClientError(
            "RDAP client response did not identify the RIR",
            code="invalid_response",
            retryable=False,
        )
    return RdapLookupResponse(rir=rir.lower(), raw_response=dict(raw))


def _client_error(
    error: QueryError,
    *,
    code: str,
    retryable: bool,
) -> RdapClientError:
    status_code = error.status_code if error.status_code > 0 else None
    return RdapClientError(
        str(error),
        code=code,
        retryable=retryable,
        status_code=status_code,
    )


def _query_error_is_retryable(error: QueryError) -> bool:
    if error.status_code == 429 or error.status_code >= 500:
        return True
    return isinstance(error.__cause__, requests.RequestException)
