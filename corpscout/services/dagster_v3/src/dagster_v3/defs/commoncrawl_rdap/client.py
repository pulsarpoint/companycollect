from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from ipaddress import ip_network
from typing import Any
from urllib.parse import unquote, urljoin, urlsplit

import requests
import whoisit
from whoisit.bootstrap import BaseBootstrap
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
from whoisit.parser import parse as parse_rdap
from whoisit.query import Query
from whoisit.utils import http_request

from dagster_v3.defs.commoncrawl_rdap.rdap import RdapLookupResponse

# RDAP host -> whoisit's registry name ('ripe', 'arin', 'apnic', 'jpnic', ...), the same
# names whoisit reports as `rir` in a parsed response.
RIR_BY_HOST = {
    urlsplit(url).hostname: name
    for name, url in BaseBootstrap.RIR_RDAP_ENDPOINTS.items()
}
REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
MAX_REDIRECTS = 5


class RdapClientError(Exception):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        retryable: bool,
        status_code: int | None = None,
        retry_after: float | None = None,
        host: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.status_code = status_code
        # Seconds the server asked the client to wait (Retry-After), when it sent one.
        self.retry_after = retry_after
        # The host that answered (or failed), when the request was made by hand.
        self.host = host


class RdapRedirect(RdapClientError):
    """An RDAP server redirected to a host the caller asked to handle itself.

    Raised instead of following the redirect, so the target's body (e.g. RIPE's person
    objects) is never fetched; ``registry`` names the target registry.
    """

    def __init__(
        self, location: str, *, registry: str, status_code: int | None
    ) -> None:
        super().__init__(
            f"RDAP redirect to {location}",
            code="cross_registry_redirect",
            retryable=False,
            status_code=status_code,
        )
        self.location = location
        self.registry = registry


class RdapClient:
    def __init__(
        self,
        *,
        user_agent: str,
        session: requests.Session | None = None,
        reroute_hosts: Iterable[str] = (),
        only_hosts: Iterable[str] | None = None,
    ) -> None:
        if user_agent.strip() == "":
            raise ValueError("user_agent must not be empty")
        self._user_agent = user_agent.strip()
        self._owns_session = session is None
        self._session = session if session is not None else requests.Session()
        self._session.headers["User-Agent"] = user_agent.strip()
        self._ready = False
        # With hosts here, redirects are followed by hand (at most MAX_REDIRECTS hops) and
        # one to these hosts raises RdapRedirect instead; the target is never fetched.
        # Empty (the default) keeps whoisit's own request, which follows redirects. The
        # caller may change the set between requests.
        self.reroute_hosts: frozenset[str] = frozenset(
            host.lower() for host in reroute_hosts
        )
        # When set, only these hosts are fetched: the first URL or a redirect to any
        # other host raises RdapRedirect (registry '' for a host no registry owns). A
        # proxied client is restricted to its registry's hosts this way, so a redirect
        # to another registry never goes through that registry's proxy.
        self.only_hosts: frozenset[str] | None = (
            None if only_hosts is None else frozenset(h.lower() for h in only_hosts)
        )

    def clone(
        self, *, proxy: str | None = None, only_hosts: Iterable[str] | None = None
    ) -> "RdapClient":
        """A client with its own session and the same User-Agent and reroute hosts.

        ``proxy`` (an http:// or https:// URL, credentials allowed) sends every request of
        the new session through that proxy; environment proxy settings are then ignored
        (trust_env off), so the proxy is the only egress.
        """
        session = requests.Session()
        if proxy is not None:
            session.trust_env = False
            session.proxies = {"http": proxy, "https": proxy}
        client = RdapClient(
            user_agent=self._user_agent,
            session=session,
            reroute_hosts=self.reroute_hosts,
            only_hosts=only_hosts,
        )
        client._owns_session = True
        return client

    def lookup_ip(self, ip_address_or_network: str) -> RdapLookupResponse:
        return self._lookup(ip_address_or_network, rir=None)

    def lookup_up_url(self, up_url: str, *, rir: str) -> RdapLookupResponse:
        return self._lookup(ip_resource_from_up_url(up_url), rir=rir)

    def lookup_url(
        self, url: str, *, ip: str, rir: str | None = None
    ) -> RdapLookupResponse:
        """GET an RDAP ip URL as it is (e.g. a redirect Location another client refused).

        Redirects are followed by hand with this client's host rules; ``rir`` names the
        registry when the answer carries no self link.
        """
        return self._lookup(ip, rir=rir, url=url)

    def registry_for(self, ip_address_or_network: str) -> str:
        """The registry whoisit would ask for this address, or '' when it cannot tell.

        Resolved from the IANA bootstrap data already loaded for lookups (no HTTP), so
        the RIPE REST path and the per-registry budget are chosen before a request is sent.
        An address without an exact bootstrap match is '' (whoisit would pick a random
        default registry). Raises RdapClientError when the bootstrap cannot be loaded.
        """
        self._ensure_bootstrapped()
        try:
            _, url, exact_match = whoisit.build_query(
                query_type="ip", query_value=ip_address_or_network
            )
        except QueryError, BootstrapError, ArgumentError, UnsupportedError:
            return ""
        if not exact_match:
            return ""
        return RIR_BY_HOST.get(url_host(url), "")

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def _lookup(
        self,
        ip_address_or_network: str,
        *,
        rir: str | None,
        url: str | None = None,
    ) -> RdapLookupResponse:
        self._ensure_bootstrapped()
        try:
            if url is not None or self.reroute_hosts or self.only_hosts is not None:
                # Redirects by hand, so one to a rerouted host is never followed.
                if url is None:
                    _, url, _ = whoisit.build_query(
                        query_type="ip", query_value=ip_address_or_network, rir=rir
                    )
                raw = self._get(url)
                if not isinstance(raw, Mapping):
                    raise ParseError("RDAP answer is not a JSON object")
                response = parse_rdap(
                    whoisit._bootstrap,
                    "ip",
                    ip_address_or_network,
                    raw,
                    include_raw=True,
                )
            else:
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

    def _get(self, url: str) -> Any:
        """GET an RDAP URL following redirects by hand; whoisit's status mapping applies.

        A rerouted host is refused before any fetch, whether it is the first URL (e.g.
        whoisit's bootstrap sent the query there) or a redirect Location.
        """
        if self._refused(url_host(url)):
            raise RdapRedirect(
                url, registry=RIR_BY_HOST.get(url_host(url), ""), status_code=None
            )
        for _ in range(MAX_REDIRECTS + 1):
            try:
                response = http_request(self._session, url, allow_redirects=False)
            except QueryError as error:
                error.host = url_host(url)
                raise
            location = response.headers.get("Location")
            if response.status_code in REDIRECT_STATUSES and location:
                response.close()
                target = urljoin(url, location)
                host = url_host(target)
                if self._refused(host):
                    raise RdapRedirect(
                        target,
                        registry=RIR_BY_HOST.get(host, ""),
                        status_code=response.status_code,
                    )
                url = target
                continue
            try:
                return Query(self._session, "GET", url)._process_response(response)
            except QueryError as error:
                error.retry_after = retry_after_seconds(
                    response.headers.get("Retry-After")
                )
                error.host = url_host(url)
                raise
        raise QueryError(f"More than {MAX_REDIRECTS} RDAP redirects from {url}")

    def _refused(self, host: str) -> bool:
        return host in self.reroute_hosts or (
            self.only_hosts is not None and host not in self.only_hosts
        )

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


def retry_after_seconds(
    value: str | None, *, now: datetime | None = None
) -> float | None:
    """Seconds of a Retry-After header (delta-seconds or an HTTP date); None when absent or unreadable."""
    if value is None or not value.strip():
        return None
    text = value.strip()
    if text.isdigit():
        return float(text)
    try:
        when = parsedate_to_datetime(text)
    except TypeError, ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - (now or datetime.now(UTC))).total_seconds())


def url_host(url: str) -> str:
    """The lower-cased host of a URL, without port or credentials ('' when absent)."""
    return (urlsplit(url).hostname or "").lower()


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
        retry_after=getattr(error, "retry_after", None),
        host=getattr(error, "host", None),
    )


def _query_error_is_retryable(error: QueryError) -> bool:
    if error.status_code == 429 or error.status_code >= 500:
        return True
    return isinstance(error.__cause__, requests.RequestException)
