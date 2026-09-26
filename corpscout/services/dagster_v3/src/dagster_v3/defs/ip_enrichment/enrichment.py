"""Shared IP lookups: GeoIP per address, RDAP resolved one page at a time.

A page's ClickHouse work is a fixed number of round trips whatever its size: one
negative-cache read, one trie lookup, one read of the network rows the page needs
(never raw_response) and one insert of the page's lookup markers. Registry requests
happen only for misses; each miss also costs the registry-class context query
(commoncrawl_rdap/registry.py), so registry-level and unallocated answers are stored
for their address only. Freshness is judged against the frozen execution start, so a
resume gives the same answers.

RIPE addresses are resolved through the RIPE Database REST search and APNIC addresses
through APNIC's port-43 whois with -r, both without personal data (commoncrawl_rdap/
ripe_rest.py and apnic_whois.py; a placeholder or an NIR's own object falls back to RDAP);
every other registry through RDAP. RDAP redirects are followed by hand: one to RIPE's or
APNIC's RDAP server (RIPE-managed space inside an ARIN /8, for example) is not followed
but sends the miss to the REST or whois client instead, so the redirected body with its
person objects is never fetched.

Counters: requests and person entities (person and role vCards) are keyed by the registry
that answered
(RdapLookupResponse.rir); RDAP fallbacks of the no-personal paths are counted in
rdap_fallbacks_by_registry and their person entities under "<rir>:fallback" keys, so a
plain "ripe" or "apnic" key in person_entities_by_registry means personal data leaked.

Lanes: a page's misses are fetched in parallel, one lane per registry and one worker per
endpoint of it (direct, plus one per HTTP(S) proxy of a registry in use_proxies). The
FETCH phase (workers) does HTTP only: requests, reroutes, fallbacks and parents, into a
plain MissOutcome. The COMMIT phase (the calling thread, page order) classifies, persists,
remembers and writes markers: the ClickHouse client is never used by a worker. Every
endpoint is paced on its own (registry_request_delays, else request_delay_seconds) and
reroutes/fallbacks to RIPE or APNIC use their direct endpoint, whatever lane made them.

An optional rolling 24-hour request budget per endpoint (registry_daily_budgets) sends a
miss to another endpoint or defers it instead of exceeding it. A rate-limited or blocked
endpoint pauses for its Retry-After, else for rate_limit_pause_seconds doubling per
consecutive limit up to rate_limit_retry_seconds (reset by a success); the rate-limited
address is deferred (no result, no marker), and a registry's misses are deferred only when
all its endpoints are paused or at their budget. The loop waits for the window only when
nothing else remains. A failed IANA bootstrap defers every miss the same way (key
"bootstrap", back-off 1 to 15 minutes) instead of storing an error per address.

Addresses that carry an IPv4 address (6to4 2002::/16 and IPv4-mapped ::ffff:0:0/96,
detected with ipaddress) are resolved through that IPv4 when it is global: the page row is
rewritten to the IPv4 before the ClickHouse round trips, so the IPv4's marker, network,
class row and segments are written as for any IPv4 and every cache is shared with the
plain IPv4 and with other addresses embedding it. The IPv6 row gets the IPv4's registry
fields (an IPv4 rdap_matched_cidr on an IPv6 row is the evidence; the IPv4 itself is
recomputed from ip), and no marker or segment of its own. Its ip_scope, and so its GeoIP
lookup, follows the embedded IPv4. Teredo 2001::/32 hides the client address: it stays
not_global without any request and is only counted.
"""

import json
import re
import threading
from collections import OrderedDict, deque
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from ipaddress import IPv4Address, IPv6Address, ip_address, ip_network
from time import monotonic, sleep
from urllib.parse import urlsplit
from uuid import UUID

import dagster as dg
import maxminddb
import requests
from netaddr import iprange_to_cidrs
from pydantic import Field, field_validator

from dagster_v3.defs.commoncrawl_geoip.maxmind import (
    build_geoip_enrichment,
    classify_ip_scope,
    lookup_maxmind_record,
)
from dagster_v3.defs.commoncrawl_rdap.apnic_whois import ApnicWhoisClient, is_nir_object
from dagster_v3.defs.commoncrawl_rdap.assets import (
    RDAP_LOOKUP_INSERT_SQL,
    RDAP_NETWORK_INSERT_SQL,
    RDAP_SEGMENT_INSERT_SQL,
)
from dagster_v3.defs.commoncrawl_rdap.client import (
    RIR_BY_HOST,
    RdapClient,
    RdapClientError,
    RdapRedirect,
)
from dagster_v3.defs.commoncrawl_rdap.registry import (
    REGISTRY_CLASS_INSERT_SQL,
    RegistryClassification,
    classify_registration,
)
from dagster_v3.defs.commoncrawl_rdap.rdap import (
    NormalizedRdapNetwork,
    RdapLookupResponse,
    RdapNetwork,
    is_registry_catch_all,
    normalize_rdap_network,
)
from dagster_v3.defs.commoncrawl_rdap.ripe_rest import RipeRestClient

NEGATIVE_STATUSES = {"not_found", "unsupported", "terminal_error"}
# Every rdap_networks column except raw_response; the cache never loads the JSON.
NETWORK_COLUMNS = (
    "network_key",
    "rir",
    "handle",
    "ip_version",
    "start_address",
    "end_address",
    "name",
    "registration_type",
    "country_code",
    "status",
    "registrant_handles",
    "registrant_names",
    "parent_network_key",
    "parent_handle",
    "self_url",
    "up_url",
    "registration_date",
    "last_changed_at",
    "response_sha256",
    "fetched_at",
)
WRITE_SETTINGS = {"async_insert": 1, "wait_for_async_insert": 1}
BUDGET_WINDOW_SECONDS = 86_400
# Codes that pause the endpoint that answered them (403 from RIPE REST is retryable);
# the address is deferred, never stored as an error.
PAUSE_CODES = frozenset({"rate_limited", "access_denied"})
# LACNIC answers 403 when a source address exceeds its rate limit ("Rate Limit is maxed
# at 10 queries per 1 minutes"): read as a rate limit, not a terminal access denial.
RATE_LIMIT_403_REGISTRIES = frozenset({"lacnic"})
# A Retry-After is honoured between 1 s and one day.
MAX_RETRY_AFTER_SECONDS = BUDGET_WINDOW_SECONDS
# A failed IANA bootstrap pauses every miss of the run (deferral key "bootstrap") instead
# of writing a retryable error per address; the pause doubles from 1 to at most 15 minutes.
BOOTSTRAP = "bootstrap"
BOOTSTRAP_CODES = frozenset({"bootstrap_error", "bootstrap_transport_error"})
MIN_BOOTSTRAP_PAUSE_SECONDS = 60
MAX_BOOTSTRAP_PAUSE_SECONDS = 900
# Registries whose RDAP requests may go through HTTP(S) proxies (terms-of-use check
# 2026-09-26). RIPE's AUP forbids pooling source addresses and APNIC's whois is port 43;
# LACNIC limits per source IP and sells more throughput as an API key, not more addresses.
PROXY_ALLOWED_REGISTRIES = frozenset({"arin", "afrinic"})
# Proxy URLs are secrets: JSON {"arin": ["http://user:pass@host:port", ...], ...}.
RDAP_PROXIES_ENV = "RDAP_PROXIES"
# Cross-registry redirects re-sent through another endpoint, per request chain.
MAX_REROUTES = 3
# Safe defaults, merged under any explicit map: LACNIC allows 10 queries per minute per
# address (403 beyond), AFRINIC 5,000 per address and day (24 h block, then permanent).
DEFAULT_REQUEST_DELAYS = {"lacnic": 6.5}
MIN_LACNIC_DELAY_SECONDS = 6.0
DEFAULT_DAILY_BUDGETS = {"afrinic": 4500}
MAX_AFRINIC_DAILY_BUDGET = 5000
# APNIC's NIRs with their own RDAP servers: APNIC's (and RIPE's fallback) redirects to
# them are followed on the same endpoint.
NIR_REGISTRIES = frozenset({"jpnic", "idnic", "krnic", "twnic"})
# On a proxy endpoint these mean the proxy (or its route) is down, not the registry.
PROXY_DOWN_CODES = frozenset({"transport_error", "timeout"})
PROXY_DOWN_STATUSES = frozenset({407, 502, 503, 504})
# A registry whose every endpoint has paused this many times in a row, and for which the
# run already waited GIVE_UP_AFTER_EMPTY_WAITS times after a pass that processed nothing,
# has its deferred misses written as retryable_error in the next pass, so the run can
# finish; never in a pass where anything else still progresses.
MAX_CONSECUTIVE_PAUSES = 6
GIVE_UP_AFTER_EMPTY_WAITS = 1


def _registry_name(field: str, registry: str) -> str:
    known = sorted(set(RIR_BY_HOST.values()))
    name = registry.strip().lower()
    if name not in known:
        raise ValueError(f"{field}: unknown registry {registry!r}; use one of {known}")
    return name


class IpEnrichmentResultsConfig(dg.Config):
    task_id: str = Field(description="IP enrichment draft (task) UUID.")
    execution_id: str | None = Field(
        default=None,
        description="Saved execution to resume. Omit to start, or to resume the task's saved execution.",
    )
    batch_size: int = Field(default=250, ge=1, le=10_000)
    max_requests: int | None = Field(
        default=250,
        ge=1,
        description="Registry HTTP request budget for this run, including parents. Null processes the whole task.",
    )
    request_delay_seconds: float = Field(
        default=1.0,
        ge=0,
        le=60,
        description="Minimum seconds between two requests of one endpoint (direct or one "
        "proxy) of a registry without an entry in registry_request_delays.",
    )
    registry_request_delays: dict[str, float] = Field(
        default_factory=lambda: dict(DEFAULT_REQUEST_DELAYS),
        description="Seconds between two requests of one endpoint, per registry (whoisit's "
        "names), merged over the default {lacnic: 6.5}; LACNIC may not go below 6 s "
        "(10 queries per minute per address).",
    )
    registry_daily_budgets: dict[str, int] = Field(
        default_factory=lambda: dict(DEFAULT_DAILY_BUDGETS),
        description="Optional rolling 24-hour request budget per endpoint (source address) "
        "of a registry, keyed by whoisit's registry names (ripe, arin, apnic, lacnic, "
        "afrinic, jpnic, ...), merged over the default {afrinic: 4500}; AFRINIC must stay "
        "between 1 and 5,000 (its per-address daily limit). A miss of an endpoint at its "
        "budget goes to another endpoint or is deferred, never failed; the run waits when "
        "nothing else remains.",
    )
    rate_limit_pause_seconds: int = Field(
        default=300,
        ge=1,
        description="First pause of an endpoint that is rate limited or blocked without a "
        "Retry-After; it doubles per consecutive limit up to rate_limit_retry_seconds.",
    )
    use_proxies: list[str] = Field(
        default_factory=list,
        description="Registries whose RDAP requests also go through the HTTP(S) proxies of "
        f"the {RDAP_PROXIES_ENV} environment variable (allowed: "
        f"{', '.join(sorted(PROXY_ALLOWED_REGISTRIES))}). Direct stays one endpoint.",
    )
    ripe_rest: bool = Field(
        default=True,
        description="Resolve RIPE addresses through the RIPE Database REST search with "
        "no-referenced (no person or role objects) instead of RDAP.",
    )
    apnic_whois: bool = Field(
        default=True,
        description="Resolve APNIC addresses through APNIC's port-43 whois with -r (contact "
        "handles only) instead of RDAP; an NIR's own allocation object falls back to RDAP.",
    )
    parent_depth: int = Field(default=1, ge=0, le=5)
    rdap_cache_days: int = Field(default=30, ge=1)
    force_rdap: bool = False
    rate_limit_retry_seconds: int = Field(default=3600, ge=1)
    transient_retry_seconds: int = Field(default=900, ge=1)

    @field_validator("task_id", "execution_id")
    @classmethod
    def uuid_identity(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

    @field_validator("registry_daily_budgets")
    @classmethod
    def positive_budgets(cls, value: dict[str, int]) -> dict[str, int]:
        budgets = dict(DEFAULT_DAILY_BUDGETS)
        for registry, budget in value.items():
            name = _registry_name("registry_daily_budgets", registry)
            if budget < 1:
                raise ValueError("registry_daily_budgets needs budgets >= 1")
            budgets[name] = budget
        if not 1 <= budgets["afrinic"] <= MAX_AFRINIC_DAILY_BUDGET:
            raise ValueError(
                f"registry_daily_budgets.afrinic must be 1..{MAX_AFRINIC_DAILY_BUDGET} "
                "(AFRINIC blocks an address above 5,000 queries a day)"
            )
        return budgets

    @field_validator("registry_request_delays")
    @classmethod
    def bounded_delays(cls, value: dict[str, float]) -> dict[str, float]:
        delays = dict(DEFAULT_REQUEST_DELAYS)
        for registry, delay in value.items():
            name = _registry_name("registry_request_delays", registry)
            if not 0 <= delay <= 60:
                raise ValueError("registry_request_delays needs 0 <= seconds <= 60")
            delays[name] = float(delay)
        if delays["lacnic"] < MIN_LACNIC_DELAY_SECONDS:
            raise ValueError(
                f"registry_request_delays.lacnic must be >= {MIN_LACNIC_DELAY_SECONDS} s "
                "(LACNIC allows 10 queries per minute per address)"
            )
        return delays

    @field_validator("use_proxies")
    @classmethod
    def allowed_proxies(cls, value: list[str]) -> list[str]:
        names: list[str] = []
        for registry in value:
            name = registry.strip().lower()
            if name not in PROXY_ALLOWED_REGISTRIES:
                raise ValueError(
                    f"use_proxies: {registry!r} may not use proxies; allowed: "
                    f"{sorted(PROXY_ALLOWED_REGISTRIES)} (RIPE and APNIC always go direct)"
                )
            if name not in names:
                names.append(name)
        return names


def rdap_proxies(use_proxies: list[str], raw: str | None) -> dict[str, tuple[str, ...]]:
    """Proxy URLs per registry of ``use_proxies``, read from the RDAP_PROXIES JSON.

    Errors never quote a URL (they may carry credentials): they name the registry and the
    index. Entries of registries not in ``use_proxies`` are ignored.
    """
    if not use_proxies:
        return {}
    if raw is None or not raw.strip():
        raise ValueError(
            f"use_proxies names {use_proxies} but {RDAP_PROXIES_ENV} is not set"
        )
    try:
        document = json.loads(raw)
    except ValueError:
        raise ValueError(f"{RDAP_PROXIES_ENV} is not valid JSON") from None
    if not isinstance(document, dict):
        raise ValueError(f"{RDAP_PROXIES_ENV} must be a JSON object of URL lists")
    entries = {str(key).strip().lower(): value for key, value in document.items()}
    proxies: dict[str, tuple[str, ...]] = {}
    for registry in use_proxies:
        urls = entries.get(registry)
        if not isinstance(urls, list) or not urls:
            raise ValueError(
                f"use_proxies names {registry!r} but {RDAP_PROXIES_ENV} has no proxy "
                "list for it"
            )
        checked = []
        for index, url in enumerate(urls):
            where = f"{RDAP_PROXIES_ENV}[{registry!r}][{index}]"
            if not isinstance(url, str):
                raise ValueError(f"{where} must be a string")
            try:
                parts = urlsplit(url.strip())
                parts.port  # noqa: B018 - raises ValueError for a bad port
            except ValueError:
                raise ValueError(f"{where} is not a valid URL") from None
            if parts.scheme not in {"http", "https"} or not parts.hostname:
                raise ValueError(
                    f"{where} must be an http:// or https:// proxy URL with a host"
                )
            checked.append(url.strip())
        if len(set(checked)) != len(checked):
            raise ValueError(f"{RDAP_PROXIES_ENV}[{registry!r}] repeats a proxy")
        proxies[registry] = tuple(checked)
    return proxies


def embedded_ipv4(address) -> tuple[str, IPv4Address] | None:
    """('6to4' | 'ipv4_mapped', IPv4) for an address that carries an IPv4 address, else None."""
    if not isinstance(address, IPv6Address):
        return None
    if address.sixtofour is not None:
        return "6to4", address.sixtofour
    if address.ipv4_mapped is not None:
        return "ipv4_mapped", address.ipv4_mapped
    return None


def ip_scope_of(address) -> str:
    """The address's scope; for 6to4 and IPv4-mapped addresses the embedded IPv4's.

    Python's ipaddress calls all of 2002::/16 non-global (IANA: "N/A"), whatever IPv4 it
    carries; the embedded IPv4 decides instead, so 2002:0808:0808::1 is global.
    """
    embedded = embedded_ipv4(address)
    return classify_ip_scope(embedded[1] if embedded else address)


def geoip_result(ip, city_reader, asn_reader, *, checked_at, retry_seconds):
    """City and ASN fields for ``ip``.

    6to4 and IPv4-mapped addresses are looked up as they are: MaxMind's databases alias
    both ranges to their IPv4 tree (verified with the vendored test databases), so the
    record is the embedded IPv4's and the matched network lies inside 2002::/16 or
    ::ffff:0:0/96.
    """
    address = ip_address(ip)
    scope = ip_scope_of(address)
    city_meta, asn_meta = city_reader.metadata(), asn_reader.metadata()
    errors = {}
    lookups = {}
    for component, reader in (("city", city_reader), ("asn", asn_reader)):
        lookups[component] = None
        if scope == "global":
            try:
                lookups[component] = lookup_maxmind_record(reader, address)
            except ValueError, maxminddb.InvalidDatabaseError:
                errors[component] = "maxmind_lookup_error"
    result = asdict(
        build_geoip_enrichment(
            bucket=0,
            address=address,
            city_lookup=lookups["city"],
            asn_lookup=lookups["asn"],
            city_build_epoch=datetime.fromtimestamp(city_meta.build_epoch, UTC),
            asn_build_epoch=datetime.fromtimestamp(asn_meta.build_epoch, UTC),
            enriched_at=checked_at,
            ip_scope=scope,
        )
    )
    for key in ("ip", "ip_version", "bucket", "enriched_at"):
        del result[key]
    for component in ("city", "asn"):
        result[f"{component}_checked_at"] = checked_at
        result[f"{component}_error_code"] = errors.get(component)
        result[f"{component}_retry_after"] = None
        if component in errors:
            result[f"{component}_lookup_status"] = "retryable_error"
            result[f"{component}_retry_after"] = checked_at + timedelta(
                seconds=retry_seconds
            )
    return result


def rdap_result(
    *, status, checked_at, error_code=None, retry_after=None, network=None, cidr=None
):
    result = dict.fromkeys(
        (
            "rdap_network_key",
            "rdap_matched_cidr",
            "rdap_rir",
            "rdap_handle",
            "rdap_start_address",
            "rdap_end_address",
            "rdap_name",
            "rdap_registration_type",
            "rdap_country_code",
            "rdap_parent_network_key",
            "rdap_parent_handle",
            "rdap_self_url",
            "rdap_registration_date",
            "rdap_last_changed_at",
        )
    )
    result.update(
        rdap_lookup_status=status,
        rdap_checked_at=checked_at,
        rdap_error_code=error_code,
        rdap_retry_after=retry_after,
        rdap_statuses=[],
        rdap_registrant_handles=[],
        rdap_registrant_names=[],
    )
    if network is not None:
        for field in (
            "network_key",
            "rir",
            "handle",
            "start_address",
            "end_address",
            "name",
            "registration_type",
            "country_code",
            "parent_network_key",
            "parent_handle",
            "self_url",
            "registration_date",
            "last_changed_at",
        ):
            result[f"rdap_{field}"] = getattr(network, field)
        result.update(
            rdap_matched_cidr=cidr,
            rdap_statuses=list(network.status),
            rdap_registrant_handles=list(network.registrant_handles),
            rdap_registrant_names=list(network.registrant_names),
        )
    return result


def matching_cidr(normalized: NormalizedRdapNetwork, address) -> str | None:
    return next(
        (s.cidr for s in normalized.segments if address in ip_network(s.cidr)), None
    )


def cidr_containing(start: str, end: str, address) -> str | None:
    """The exact CIDR fragment of an inclusive range that holds ``address`` (None outside it)."""
    return next(
        (
            str(cidr)
            for cidr in iprange_to_cidrs(start, end)
            if cidr.version == address.version
            and cidr.first <= int(address) <= cidr.last
        ),
        None,
    )


def cached_network_row(row) -> RdapNetwork:
    """A network as read from the cache; raw_response is never loaded."""
    values = dict(zip(NETWORK_COLUMNS, row, strict=True))
    for key in ("status", "registrant_handles", "registrant_names"):
        values[key] = tuple(values[key])
    return RdapNetwork(raw_response="", **values)


# vCard kinds of personal data sets: 'individual' is a person object, 'group' a role
# object (RIPE's AUP counts both against its daily limit).
PERSONAL_VCARD_KINDS = frozenset({"individual", "group"})


def person_entities(raw: Mapping) -> int:
    """Entities whose vCard is of kind 'individual' or 'group', nested included.

    RIPE counts person and role objects against its daily limit; RDAP answers of the
    other registries carry them too. Both kinds land in the same counter key, so a role-only
    answer is as visible as one with persons. An upper bound (RIPE marks maintainers
    'individual' as well). The REST and whois shapes never carry any, so their count is 0.
    """
    count = 0
    pending = list(raw.get("entities") or [])
    while pending:
        entity = pending.pop()
        if not isinstance(entity, Mapping):
            continue
        vcard = entity.get("vcardArray")
        if isinstance(vcard, list) and len(vcard) == 2 and isinstance(vcard[1], list):
            if any(
                isinstance(item, list)
                and len(item) == 4
                and item[0] == "kind"
                and item[3] in PERSONAL_VCARD_KINDS
                for item in vcard[1]
            ):
                count += 1
        pending.extend(entity.get("entities") or [])
    return count


def _module_sleep() -> Callable[[float], None]:
    """The module's `sleep` (patchable), which RdapEnricher's parameter of that name shadows."""
    return sleep


def _clickhouse_time(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")


@dataclass(eq=False)
class Endpoint:
    """One source address of requests to a registry: direct, or one HTTP(S) proxy.

    ``lock`` is held for pacing plus the request, so the endpoint's client (and its
    session) is used by one thread at a time; ``paused_until``, ``limits`` and the budget
    window live under the enricher's state lock; ``last_send`` only under ``lock``.
    """

    name: str  # "arin:direct", "arin:proxy-1": never the proxy URL
    registry: str
    rdap: RdapClient
    delay: float
    last_send: float  # creation time, so the first request is paced too
    proxy: str | None = field(default=None, repr=False)  # a secret
    lock: threading.Lock = field(default_factory=threading.Lock)
    paused_until: float = float("-inf")
    egress: str = ""  # budget window key: "<registry>:direct" or "<registry>:egress-N"
    limits: int = 0  # consecutive pauses, reset by a success
    last_code: str = ""  # code of the latest pause


@dataclass(frozen=True)
class Miss:
    ip: str
    address: IPv4Address | IPv6Address
    registry: str


@dataclass
class MissOutcome:
    """What the FETCH phase learned about one miss; only the COMMIT phase stores anything.

    Exactly one of: ``direct`` (a validated registration, with ``cidr`` and the fetched
    ``parents``), ``error``, ``reused`` (the network key another miss of this page
    fetched, with ``cidr``) or ``deferred`` (``registry`` could not be asked).
    """

    ip: str
    registry: str = ""
    direct: NormalizedRdapNetwork | None = None
    cidr: str | None = None
    parents: list[NormalizedRdapNetwork] = field(default_factory=list)
    error: RdapClientError | ValueError | None = None
    checked_at: datetime | None = None
    reused: str | None = None
    deferred: bool = False


@dataclass
class _Page:
    """Lane state of one page, shared by its workers under the enricher's state lock."""

    queues: dict[str, deque[Miss]]
    outcomes: dict[str, MissOutcome] = field(default_factory=dict)
    fetched: list[NormalizedRdapNetwork] = field(default_factory=list)
    stop: threading.Event = field(default_factory=threading.Event)


class _Requeue(Exception):
    """The lane's own endpoint is paused or at its budget: another endpoint may take the miss."""


class _Blocked(Exception):
    """The endpoint is paused or at its daily budget; nothing was sent."""


class _RequestLimit(Exception):
    """The run's max_requests is reached; nothing was sent."""


class _EndpointDown(Exception):
    """A proxy endpoint failed to carry the request (transport, timeout, 407/502/503/504);
    it is paused and the miss goes to another endpoint or the next pass."""


def _pauses(error: RdapClientError) -> bool:
    return error.retryable and error.code in PAUSE_CODES


def _proxy_down(error: RdapClientError) -> bool:
    """A failure of the proxy or its route: transport, timeout, 407/502/503/504."""
    if error.code in PROXY_DOWN_CODES or error.status_code in PROXY_DOWN_STATUSES:
        return True
    cause = error.__cause__
    while cause is not None:
        if isinstance(cause, requests.RequestException):
            return True
        cause = cause.__cause__
    return False


def _scrub(error: RdapClientError, endpoint: "Endpoint") -> None:
    """Remove the proxy URL, host and credentials from a proxy endpoint's error."""
    secret = endpoint.proxy or ""
    parts = urlsplit(secret)
    message = str(error)
    texts = {
        secret,
        parts.netloc,
        parts.hostname or "",
        parts.username or "",
        parts.password or "",
    }
    for text in sorted((t for t in texts if t), key=len, reverse=True):
        message = re.sub(
            re.escape(text), f"<{endpoint.name}>", message, flags=re.IGNORECASE
        )
    error.args = (message,)
    # The chained library errors repeat the proxy details: drop them.
    error.__cause__ = None
    error.__context__ = None
    error.__suppress_context__ = True


class RdapEnricher:
    """Resolve registry coverage for a page of addresses with a bounded number of round trips.

    Misses are fetched in parallel lanes, one per registry, with one worker per endpoint
    of the registry (direct, plus one per configured proxy); the ClickHouse client is used
    only on the calling thread, which stores the outcomes in page order (COMMIT) once every
    lane has drained its queue or deferred it. ``concurrent=False`` fetches the misses one
    at a time in page order on the calling thread (the sequential reference).
    """

    def __init__(
        self,
        client,
        rdap: RdapClient,
        ripe: RipeRestClient,
        apnic: ApnicWhoisClient,
        config: IpEnrichmentResultsConfig,
        log,
        *,
        started_at: datetime,
        cache_cutoff: datetime,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], None] | None = None,
        proxies: Mapping[str, Sequence[str]] | None = None,
        concurrent: bool = True,
    ):
        self.client = client
        self.rdap = rdap  # registry routing (registry_for) on the calling thread only
        self.ripe = ripe  # used only under the ripe:direct endpoint lock
        self.apnic = apnic  # used only under the apnic:direct endpoint lock
        self.config = config
        self.log = log
        self.started_at = started_at
        self.cache_cutoff = cache_cutoff
        # Resolved at construction so tests can patch the module names.
        self._clock = clock or monotonic
        self._sleep = sleep or _module_sleep()
        self._concurrent = concurrent
        proxies = proxies or {}
        missing = [name for name in config.use_proxies if not proxies.get(name)]
        if missing:
            raise ValueError(f"use_proxies names {missing} without proxies")
        self._proxies = {name: tuple(proxies[name]) for name in config.use_proxies}
        # Guards every counter, budget window, pause and page field below; never held
        # while waiting for an endpoint lock (an endpoint lock may be held when taking it).
        self._lock = threading.RLock()
        self._notes: list[tuple[str, str, tuple]] = []
        self.requests = 0
        self.cache_hits = 0
        self.networks_written = 0
        self.parent_failures = 0
        self.registry_level_responses = 0
        self.budget_reached = False
        # Keyed by the answering registry (RdapLookupResponse.rir); a failed request by
        # the registry it was sent to.
        self.requests_by_registry: dict[str, int] = {}
        # Requests per endpoint ("arin:direct", "arin:proxy-1"; never a URL).
        self.requests_by_endpoint: dict[str, int] = {}
        # Person entities per answering registry; RDAP fallbacks of the no-personal
        # paths count under "<rir>:fallback", so a plain ripe/apnic key is a leak.
        self.person_entities_by_registry: dict[str, int] = {}
        # RDAP requests made because a REST/whois answer was a catch-all or an NIR's own
        # object, keyed by the registry whose no-personal path fell back.
        self.rdap_fallbacks_by_registry: dict[str, int] = {}
        # Misses re-sent to another registry's direct endpoint after a redirect that was
        # not followed: to RIPE/APNIC (REST/whois), or out of a proxy's registry.
        self.reroutes_by_registry: dict[str, int] = {}
        # Rate-limit / block pauses started, per registry and per endpoint.
        self.pauses_by_registry: dict[str, int] = {}
        self.pauses_by_endpoint: dict[str, int] = {}
        self.deferrals_by_registry: dict[str, int] = {}
        self.deferred: dict[str, int] = {}  # since the last reset_pass()
        # Addresses answered through their embedded IPv4, by form ('6to4',
        # 'ipv4_mapped'), and Teredo addresses left without a request.
        self.embedded_ipv4_lookups: dict[str, int] = {}
        self.teredo_special = 0
        # Registries resolved without personal data: a redirect into their space goes to
        # REST/whois (each endpoint refuses other registries' hosts, see _endpoints_of).
        self._private = frozenset(
            name
            for name, enabled in (
                ("ripe", config.ripe_rest),
                ("apnic", config.apnic_whois),
            )
            if enabled
        )
        # Endpoints per registry, direct first; created on first use.
        self._endpoints: dict[str, list[Endpoint]] = {}
        # The run-wide bootstrap pause (monotonic time) and its back-off.
        self._paused_until: dict[str, float] = {}
        self._bootstrap_failures = 0
        # Reusable networks fetched over HTTP in this run, checked before any request.
        # Read and written on the calling thread only (COMMIT runs after the lanes).
        self.recent: OrderedDict[str, NormalizedRdapNetwork] = OrderedDict()
        # Fresh network rows read from ClickHouse, keyed by network_key (calling thread).
        self.cached: OrderedDict[str, RdapNetwork] = OrderedDict()
        # Monotonic send times per budgeted egress (Endpoint.egress: the direct address, or
        # one proxy hostname shared by every proxy URL on it), oldest first.
        self._sent: dict[str, deque[float]] = {}
        # Consecutive waits after a pass that processed nothing, per deferred registry
        # (reset by the registry's next success); see GIVE_UP_AFTER_EMPTY_WAITS.
        self._empty_waits: dict[str, int] = {}
        # Never evict keys a page just loaded: a page needs at most batch_size keys.
        self._cached_cap = max(4096, 2 * config.batch_size)

    def close(self) -> None:
        for endpoints in self._endpoints.values():
            for endpoint in endpoints:
                endpoint.rdap.close()

    # --- page resolution -------------------------------------------------------

    def resolve_page(self, rows: list[dict]) -> dict[str, dict]:
        """Registry fields per address of the page.

        An address is absent when it was deferred (its registry at its daily budget or
        paused, rate limited, or the bootstrap paused) or when the run's max_requests
        budget ran out (``budget_reached`` is then True). A 6to4 or IPv4-mapped address
        whose IPv4 is global is resolved as that IPv4 and answered with its fields.
        """
        addresses: dict[str, IPv4Address | IPv6Address] = {}
        buckets: dict[str, int] = {}
        embedded: dict[str, tuple[str, str]] = {}  # page ip -> (form, IPv4 looked up)
        for row in rows:
            address = ip_address(row["ip"])
            if isinstance(address, IPv6Address) and address.teredo is not None:
                self.teredo_special += 1  # not_global below: the client is hidden
            found = embedded_ipv4(address)
            if found is not None and classify_ip_scope(found[1]) == "global":
                embedded[row["ip"]] = (found[0], str(found[1]))
                continue
            addresses[row["ip"]] = address
            buckets[row["ip"]] = row["bucket"]
        if embedded:
            # The IPv4's own bucket, as every other writer computes it: in ClickHouse.
            missing = sorted(
                {ipv4 for _, ipv4 in embedded.values() if ipv4 not in buckets}
            )
            if missing:
                for ipv4, bucket in self.client.execute(
                    "SELECT ip, toUInt16(cityHash64(ip) %% 256) FROM "
                    "(SELECT arrayJoin(CAST(%(ips)s, 'Array(String)')) AS ip)",
                    {"ips": missing},
                ):
                    addresses[ipv4] = ip_address(ipv4)
                    buckets[ipv4] = bucket
        results = self._resolve(addresses, buckets)
        page = {row["ip"] for row in rows}
        answered = {ip: result for ip, result in results.items() if ip in page}
        for ip, (form, ipv4) in embedded.items():
            if ipv4 in results:
                answered[ip] = dict(results[ipv4])
                self._count(self.embedded_ipv4_lookups, form)
        return answered

    def _resolve(self, addresses: dict, buckets: dict[str, int]) -> dict[str, dict]:
        results: dict[str, dict] = {}
        markers: list[tuple] = []
        pending: list[str] = []
        for ip, address in addresses.items():
            if classify_ip_scope(address) != "global":
                results[ip] = rdap_result(
                    status="not_global", checked_at=datetime.now(UTC)
                )
                markers.append(self._marker(ip, address, buckets[ip], results[ip]))
            else:
                pending.append(ip)
        if pending and not self.config.force_rdap:
            hits: dict[str, str] = {}
            for (
                ip,
                status,
                network_key,
                code,
                retry_after,
                checked_at,
                fresh,
                backoff,
            ) in self._markers_of(pending, addresses, buckets):
                if status == "retryable_error" and backoff:
                    results[ip] = rdap_result(
                        status=status,
                        checked_at=checked_at,
                        error_code=code,
                        retry_after=retry_after,
                    )
                elif status in NEGATIVE_STATUSES and fresh:
                    results[ip] = rdap_result(
                        status="terminal_error" if status == "unsupported" else status,
                        checked_at=checked_at,
                        error_code=code,
                        retry_after=retry_after,
                    )
                elif status == "found" and network_key and fresh:
                    hits[ip] = (
                        network_key  # a per-address answer, e.g. a registry-level block
                    )
                else:
                    continue
                if ip in results:
                    self.cache_hits += 1
            pending = [ip for ip in pending if ip not in results and ip not in hits]
            for ip, network_key in self._trie(pending, addresses):
                if network_key:
                    hits[ip] = network_key
            pending = [ip for ip in pending if ip not in hits]
            for network_key in hits.values():  # keep this page's keys newest
                if network_key in self.cached:
                    self.cached.move_to_end(network_key)
            self._load_networks(
                {key for key in hits.values() if key not in self.cached}
            )
            for ip, network_key in hits.items():
                network = self.cached.get(network_key)
                cidr = (
                    cidr_containing(
                        network.start_address, network.end_address, addresses[ip]
                    )
                    if network is not None
                    else None
                )
                if cidr is None:  # stale, gone or not containing the address: ask again
                    pending.append(ip)
                    continue
                results[ip] = rdap_result(
                    status="found",
                    checked_at=network.fetched_at,
                    network=network,
                    cidr=cidr,
                )
                self.cache_hits += 1
        # Misses: in-run cache, then registry routing (local bootstrap data), both here.
        order: list[str] = []
        misses: list[Miss] = []
        outcomes: dict[str, MissOutcome] = {}
        for ip in pending:
            recent = self._recent_match(addresses[ip])
            if recent is not None:  # fetched earlier in this run: no HTTP, no marker
                normalized, cidr = recent
                self.cache_hits += 1
                results[ip] = rdap_result(
                    status="found",
                    checked_at=normalized.network.fetched_at,
                    network=normalized.network,
                    cidr=cidr,
                )
                continue
            order.append(ip)
            routed = self._route(ip)
            if isinstance(routed, MissOutcome):
                outcomes[ip] = routed
            elif routed is not None:
                misses.append(Miss(ip, addresses[ip], routed))
        # FETCH: HTTP only, in lanes; then COMMIT on this thread, in page order.
        try:
            outcomes.update(self._fetch_misses(misses))
        finally:
            self._flush_notes()
        self._commit(order, outcomes, addresses, buckets, results, markers)
        if markers:
            self.client.execute(
                RDAP_LOOKUP_INSERT_SQL, markers, settings=WRITE_SETTINGS
            )
        return results

    def _markers_of(self, ips, addresses, buckets):
        return self.client.execute(
            """SELECT ip, lookup_status, ifNull(network_key, ''), error_code, retry_after, queried_at,
                queried_at >= toDateTime64(%(cutoff)s, 6, 'UTC') AS fresh,
                ifNull(retry_after > toDateTime64(%(started)s, 6, 'UTC'), 0) AS backoff
            FROM corpscout.rdap_ip_lookup_results_current
            WHERE (bucket, ip_version, ip) IN %(keys)s""",
            {
                "keys": tuple((buckets[ip], addresses[ip].version, ip) for ip in ips),
                "cutoff": _clickhouse_time(self.cache_cutoff),
                "started": _clickhouse_time(self.started_at),
            },
        )

    def _trie(self, ips, addresses):
        # The CAST types an empty side (a page of one IP version), which ClickHouse
        # otherwise rejects as Array(Nothing).
        if not ips:
            return []
        return self.client.execute(
            """SELECT ip, dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv4(ip)), '')
            FROM (SELECT arrayJoin(CAST(%(v4)s, 'Array(String)')) AS ip)
            UNION ALL
            SELECT ip, dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv6(ip)), '')
            FROM (SELECT arrayJoin(CAST(%(v6)s, 'Array(String)')) AS ip)""",
            {
                "v4": [ip for ip in ips if addresses[ip].version == 4],
                "v6": [ip for ip in ips if addresses[ip].version == 6],
            },
        )

    def _load_networks(self, keys: set[str]) -> None:
        if not keys:
            return
        rows = self.client.execute(
            f"""SELECT {", ".join(NETWORK_COLUMNS)} FROM corpscout.rdap_networks_current
            WHERE network_key IN %(keys)s AND fetched_at >= toDateTime64(%(cutoff)s, 6, 'UTC')""",
            {"keys": tuple(keys), "cutoff": _clickhouse_time(self.cache_cutoff)},
        )
        for row in rows:
            network = cached_network_row(row)
            self.cached[network.network_key] = network
            self.cached.move_to_end(network.network_key)
        while len(self.cached) > self._cached_cap:
            self.cached.popitem(last=False)

    # --- endpoints, pacing, pauses and budgets -----------------------------------

    def _endpoints_of(self, registry: str) -> list[Endpoint]:
        """The registry's endpoints, direct first, then one per proxy (created once)."""
        with self._lock:
            endpoints = self._endpoints.get(registry)
            if endpoints is None:
                delay = self.config.registry_request_delays.get(
                    registry, self.config.request_delay_seconds
                )
                # Every endpoint only fetches its own registry's hosts (APNIC's and RIPE's
                # also their NIRs'): a redirect elsewhere is re-sent through the target's
                # direct endpoint, with that registry's pace, budget and pauses. A direct
                # endpoint may still fetch hosts no registry owns; a proxy may not.
                own = {
                    host
                    for host, name in RIR_BY_HOST.items()
                    if name == registry
                    or (registry in {"apnic", "ripe"} and name in NIR_REGISTRIES)
                }
                direct = self.rdap.clone()
                direct.reroute_hosts = frozenset(RIR_BY_HOST) - own
                now = self._clock()
                endpoints = [
                    Endpoint(
                        f"{registry}:direct",
                        registry,
                        direct,
                        delay,
                        now,
                        egress=f"{registry}:direct",
                    )
                ]
                # Proxy URLs on one hostname share one egress (and one budget window);
                # the egress key numbers hostnames, it never names them.
                egress: dict[str, str] = {}
                for number, url in enumerate(self._proxies.get(registry, ()), 1):
                    host = (urlsplit(url).hostname or "").lower()
                    egress.setdefault(host, f"{registry}:egress-{len(egress) + 1}")
                    endpoints.append(
                        Endpoint(
                            f"{registry}:proxy-{number}",
                            registry,
                            self.rdap.clone(proxy=url, only_hosts=own),
                            delay,
                            now,
                            egress=egress[host],
                            proxy=url,
                        )
                    )
                self._endpoints[registry] = endpoints
            return endpoints

    def _direct(self, registry: str) -> Endpoint:
        return self._endpoints_of(registry)[0]

    def seed_registry_usage(self, rows: list[tuple[str, float]]) -> None:
        """Requests any run made in the last day (registry, seconds ago), oldest first.

        The rows name the registry, not the source address, so every egress of a
        budgeted registry is charged with all of them (never under-counted per address).
        """
        now = self._clock()
        for registry, seconds_ago in rows:
            if (
                registry in self.config.registry_daily_budgets
                and seconds_ago < BUDGET_WINDOW_SECONDS
            ):
                for egress in {e.egress for e in self._endpoints_of(registry)}:
                    self._sent.setdefault(egress, deque()).append(now - seconds_ago)

    def _trim(self, times: deque[float]) -> deque[float]:
        horizon = self._clock() - BUDGET_WINDOW_SECONDS
        while times and times[0] <= horizon:
            times.popleft()
        return times

    def _window(self, endpoint: Endpoint) -> deque[float]:
        """The rolling send times of the endpoint's egress (shared by proxies on one host)."""
        return self._trim(self._sent.setdefault(endpoint.egress, deque()))

    def _over_budget(self, endpoint: Endpoint) -> bool:
        budget = self.config.registry_daily_budgets.get(endpoint.registry)
        return budget is not None and len(self._window(endpoint)) >= budget

    def _charge(self, endpoint: Endpoint, stamp: float, sign: int) -> None:
        """Count one request (sign +1), or take back one that was never sent (-1)."""
        self.requests += sign
        self._count(self.requests_by_endpoint, endpoint.name, sign)
        if not self.requests_by_endpoint[endpoint.name]:
            del self.requests_by_endpoint[endpoint.name]
        if endpoint.registry in self.config.registry_daily_budgets:
            times = self._window(endpoint)
            if sign > 0:
                times.append(stamp)
            elif stamp in times:
                times.remove(stamp)

    def _endpoint_blocked(self, endpoint: Endpoint) -> bool:
        """Paused after a rate limit, or at its daily budget: it sends nothing."""
        with self._lock:
            return endpoint.paused_until > self._clock() or self._over_budget(endpoint)

    def _pause_endpoint(
        self, endpoint: Endpoint, error: RdapClientError, answered: str
    ) -> None:
        """Pause the endpoint for max(Retry-After, back-off).

        The back-off doubles from rate_limit_pause_seconds per consecutive pause up to
        rate_limit_retry_seconds; an access denial waits at least rate_limit_retry_seconds.
        ``answered`` (the registry whose server answered) keys pauses_by_registry.
        """
        with self._lock:
            endpoint.limits += 1
            endpoint.last_code = error.code
            seconds = float(
                min(
                    self.config.rate_limit_retry_seconds,
                    self.config.rate_limit_pause_seconds
                    * 2 ** min(endpoint.limits - 1, 32),
                )
            )
            if error.retry_after is not None:
                seconds = max(seconds, float(error.retry_after))
            if error.code == "access_denied":
                seconds = max(seconds, float(self.config.rate_limit_retry_seconds))
            seconds = min(max(seconds, 1.0), MAX_RETRY_AFTER_SECONDS)
            endpoint.paused_until = max(endpoint.paused_until, self._clock() + seconds)
            self._count(self.pauses_by_registry, answered)
            self._count(self.pauses_by_endpoint, endpoint.name)
        self._note(
            "warning",
            "Registry endpoint %s is rate limiting, blocking or unreachable (%s); "
            "paused for %.0f s",
            endpoint.name,
            error.code,
            seconds,
        )

    def _all_paused_out(self, registry: str) -> bool:
        """Every endpoint of the registry has paused MAX_CONSECUTIVE_PAUSES times in a row."""
        with self._lock:
            endpoints = self._endpoints.get(registry) or []
            return bool(endpoints) and all(
                e.limits >= MAX_CONSECUTIVE_PAUSES for e in endpoints
            )

    def _gave_up(self, registry: str) -> str | None:
        """The latest pause code when the registry is given up, else None: every endpoint
        paused out, and the run already waited for it after a pass that processed nothing
        (GIVE_UP_AFTER_EMPTY_WAITS times), so a pass that still progresses never gives up."""
        with self._lock:
            if self._empty_waits.get(
                registry, 0
            ) < GIVE_UP_AFTER_EMPTY_WAITS or not self._all_paused_out(registry):
                return None
            endpoints = self._endpoints[registry]
            return max(endpoints, key=lambda e: e.paused_until).last_code

    def _bootstrap_paused(self) -> bool:
        return self._paused_until.get(BOOTSTRAP, float("-inf")) > self._clock()

    def _bootstrap_failed(self, error: RdapClientError) -> None:
        """Pause every miss until the bootstrap is retried: 60 s, doubling to 15 min."""
        self._bootstrap_failures += 1
        seconds = min(
            MAX_BOOTSTRAP_PAUSE_SECONDS,
            MIN_BOOTSTRAP_PAUSE_SECONDS * 2 ** (self._bootstrap_failures - 1),
        )
        self._paused_until[BOOTSTRAP] = self._clock() + seconds
        self._count(self.pauses_by_registry, BOOTSTRAP)
        self.log.warning(
            "RDAP bootstrap failed (%s: %s); misses paused for %.0f s",
            error.code,
            error,
            seconds,
        )

    def _defer(self, registry: str) -> None:
        with self._lock:
            self.deferred[registry] = self.deferred.get(registry, 0) + 1
            self.deferrals_by_registry[registry] = (
                self.deferrals_by_registry.get(registry, 0) + 1
            )

    def reset_pass(self) -> None:
        self.deferred = {}

    def _endpoint_wait(self, endpoint: Endpoint, deferred: int, now: float) -> float:
        wait = max(0.0, endpoint.paused_until - now)
        budget = self.config.registry_daily_budgets.get(endpoint.registry)
        if budget is not None:
            times = self._window(endpoint)
            if len(times) >= budget:
                slots = max(1, min(deferred, budget // 24))
                wait = max(
                    wait,
                    times[len(times) - budget + slots - 1]
                    + BUDGET_WINDOW_SECONDS
                    - now,
                )
        return wait

    def seconds_until_budget_frees(self) -> float:
        """Seconds until a deferred registry may be asked again (0 when none is deferred).

        A registry waits for its first endpoint to free: the end of that endpoint's pause,
        or for an endpoint at its budget an hour's share of the budget (at least one
        request), so the pass that follows is worth its ClickHouse queries.
        """
        with self._lock:
            now = self._clock()
            waits = []
            for registry, deferred in self.deferred.items():
                if registry == BOOTSTRAP:
                    waits.append(max(0.0, self._paused_until.get(BOOTSTRAP, now) - now))
                    continue
                waits.append(
                    min(
                        (
                            self._endpoint_wait(endpoint, deferred, now)
                            for endpoint in self._endpoints_of(registry)
                        ),
                        default=0.0,
                    )
                )
            return max(0.0, min(waits)) if waits else 0.0

    def wait_for_registry_budget(self) -> float:
        """Sleep, in slices of at most a minute, until a deferred registry frees slots.

        The loop calls this only after a pass that processed nothing: each deferred
        registry whose endpoints are all paused out counts one empty-pass wait, which
        arms its give-up for the next pass (_gave_up).
        """
        with self._lock:
            for registry in self.deferred:
                if registry != BOOTSTRAP and self._all_paused_out(registry):
                    self._empty_waits[registry] = self._empty_waits.get(registry, 0) + 1
        total = self.seconds_until_budget_frees()
        waited = 0.0
        while waited < total:
            step = min(60.0, total - waited)
            self._sleep(step)
            waited += step
            if int(waited) % 600 == 0:
                self.log.info("Registry budget wait: %.0f of %.0f s", waited, total)
        self.reset_pass()
        return waited

    # --- misses: routing and FETCH (HTTP only, any thread) -------------------------

    def _budget_exhausted(self) -> bool:
        return (
            self.config.max_requests is not None
            and self.requests >= self.config.max_requests
        )

    def _source(self, registry: str) -> str:
        """'ripe_rest' or 'apnic_whois' for the no-personal paths, else 'rdap'."""
        if registry == "ripe" and "ripe" in self._private:
            return "ripe_rest"
        if registry == "apnic" and "apnic" in self._private:
            return "apnic_whois"
        return "rdap"

    def _count(self, counter: dict[str, int], key: str, amount: int = 1) -> None:
        with self._lock:
            counter[key] = counter.get(key, 0) + amount

    def _note(self, level: str, message: str, *args) -> None:
        """A log line from any thread, emitted on the calling thread by _flush_notes."""
        with self._lock:
            self._notes.append((level, message, args))

    def _flush_notes(self) -> None:
        with self._lock:
            notes, self._notes = self._notes, []
        for level, message, args in notes:
            getattr(self.log, level)(message, *args)

    def _route(self, ip: str) -> str | MissOutcome | None:
        """The miss's registry (bootstrap data, no HTTP); an outcome; None when deferred."""
        if self._bootstrap_paused():
            self._defer(BOOTSTRAP)
            return None
        try:
            registry = self.rdap.registry_for(ip)
        except RdapClientError as error:
            if error.code not in BOOTSTRAP_CODES:
                return MissOutcome(ip, error=error, checked_at=datetime.now(UTC))
            # No registry can be chosen without the bootstrap: a run-wide pause, never
            # an error per address.
            self._bootstrap_failed(error)
            self._defer(BOOTSTRAP)
            return None
        self._bootstrap_failures = 0
        if registry == "":
            # No exact bootstrap match (global IPv6 outside the bootstrap's prefixes;
            # 6to4 and IPv4-mapped addresses never get here, they are resolved as their
            # IPv4): whoisit would send the query to a random registry (possibly RIPE's
            # or APNIC's RDAP). Nothing is requested; a terminal marker is cached for
            # rdap_cache_days like other terminal errors, so retry drafts do not re-queue
            # it into a request every time.
            return MissOutcome(
                ip,
                error=RdapClientError(
                    f"No registry is known for {ip}",
                    code="no_registry",
                    retryable=False,
                ),
                checked_at=datetime.now(UTC),
            )
        return registry

    def _fetch_misses(self, misses: list[Miss]) -> dict[str, MissOutcome]:
        """Fetch every miss: one lane per registry, one worker per endpoint of it.

        A miss still queued when its lane stops is deferred (every endpoint of the
        registry is paused or at its budget), unless max_requests stopped the lanes.
        """
        if not misses:
            return {}
        queues: dict[str, deque[Miss]] = {}
        for miss in misses:
            queues.setdefault(miss.registry, deque()).append(miss)
        page = _Page(queues=queues)
        if self._concurrent:
            workers = [
                (registry, endpoint)
                for registry in queues
                for endpoint in self._endpoints_of(registry)
            ]
            pool = ThreadPoolExecutor(
                max_workers=len(workers), thread_name_prefix="rdap-lane"
            )
            try:
                futures = [
                    pool.submit(self._lane, page, registry, endpoint)
                    for registry, endpoint in workers
                ]
                done, _ = wait(futures, return_when=FIRST_EXCEPTION)
                for future in done:
                    future.result()  # the first failure, as soon as it happens
            except BaseException:
                page.stop.set()  # the other workers stop at their next miss
                raise
            finally:
                pool.shutdown(wait=True)
        else:
            self._sequential(page, misses)
        with self._lock:
            outcomes = dict(page.outcomes)
            leftovers = [miss for queue in queues.values() for miss in queue]
        if not self.budget_reached:
            for miss in leftovers:
                outcomes[miss.ip] = MissOutcome(
                    miss.ip, registry=miss.registry, deferred=True
                )
        return outcomes

    def _lane(self, page: _Page, registry: str, endpoint: Endpoint) -> None:
        """One worker: take the registry's misses until the queue is empty, the endpoint
        is paused or at its budget, or max_requests is reached."""
        queue = page.queues[registry]
        while not page.stop.is_set():
            with self._lock:
                if self.budget_reached or not queue or self._endpoint_blocked(endpoint):
                    return
                miss = queue.popleft()
            try:
                outcome = self._fetch(page, miss, endpoint)
            except _Requeue:
                with self._lock:
                    queue.appendleft(miss)  # another endpoint may take it
                continue
            except _RequestLimit:
                with self._lock:
                    queue.appendleft(miss)
                    self.budget_reached = True
                return
            with self._lock:
                page.outcomes[miss.ip] = outcome

    def _sequential(self, page: _Page, misses: list[Miss]) -> None:
        """The same FETCH, one miss at a time in page order, on the calling thread."""
        for miss in misses:
            queue = page.queues[miss.registry]
            while not self.budget_reached:
                endpoint = next(
                    (
                        e
                        for e in self._endpoints_of(miss.registry)
                        if not self._endpoint_blocked(e)
                    ),
                    None,
                )
                if endpoint is None:
                    break  # stays queued: deferred
                try:
                    page.outcomes[miss.ip] = self._fetch(page, miss, endpoint)
                except _Requeue:
                    continue
                except _RequestLimit:
                    self.budget_reached = True
                    break
                queue.remove(miss)
                break

    def _page_match(self, page: _Page, address) -> tuple[str, str] | None:
        """(network_key, cidr) of the most specific network fetched in this page for ``address``."""
        with self._lock:
            found = _best_match(page.fetched, address)
        if found is None:
            return None
        normalized, cidr = found
        return normalized.network.network_key, cidr

    def _send(
        self,
        endpoint: Endpoint,
        target: str,
        *,
        source: str = "rdap",
        rir: str | None = None,
        url: str | None = None,
        fallback: bool = False,
        gate: bool = False,
    ) -> RdapLookupResponse:
        """One paced request through an endpoint: 'rdap', 'ripe_rest' or 'apnic_whois'.

        ``gate`` charges max_requests (raises _RequestLimit when reached); a paused
        endpoint or one at its budget raises _Blocked. Nothing is sent then. The
        endpoint's lock is held for pacing and the request. A redirect to another
        registry's host raises RdapRedirect (a refusal before any fetch is not charged).
        A pause code pauses the endpoint; a dead proxy pauses it and raises
        _EndpointDown; a success resets its back-off. A proxy endpoint's errors are
        scrubbed of the proxy URL. ``fallback`` marks the person entities of the answer.
        """
        registry = endpoint.registry
        if source != "rdap" and endpoint is not self._direct(registry):
            raise RuntimeError(f"{source} requests go direct, not via {endpoint.name}")
        with endpoint.lock:
            with self._lock:
                if gate and self._budget_exhausted():
                    raise _RequestLimit
                if self._endpoint_blocked(endpoint):
                    raise _Blocked
                stamp = self._clock()
                self._charge(endpoint, stamp, +1)
            wait_seconds = endpoint.last_send + endpoint.delay - self._clock()
            if endpoint.delay > 0 and wait_seconds > 0:
                self._sleep(wait_seconds)
            previous, endpoint.last_send = endpoint.last_send, self._clock()
            try:
                if rir is not None:
                    response = endpoint.rdap.lookup_up_url(target, rir=rir)
                elif url is not None:
                    response = endpoint.rdap.lookup_url(url, ip=target, rir=registry)
                elif source == "ripe_rest":
                    response = self.ripe.lookup_ip(target)
                elif source == "apnic_whois":
                    response = self.apnic.lookup_ip(target)
                else:
                    response = endpoint.rdap.lookup_ip(target)
            except RdapRedirect as redirect:
                if redirect.status_code is None:  # refused before anything was sent
                    with self._lock:
                        self._charge(endpoint, stamp, -1)
                    endpoint.last_send = previous
                else:
                    self._count(self.requests_by_registry, registry)
                raise
            except RdapClientError as error:
                answered = RIR_BY_HOST.get(error.host or "", registry)
                if (
                    error.code == "access_denied"
                    and error.status_code == 403
                    and answered in RATE_LIMIT_403_REGISTRIES
                ):
                    error.retryable = True
                self._count(self.requests_by_registry, answered)
                if endpoint.proxy is not None:
                    down = _proxy_down(error)
                    _scrub(error, endpoint)
                    if down:
                        self._pause_endpoint(endpoint, error, answered)
                        raise _EndpointDown from None
                if _pauses(error):
                    self._pause_endpoint(endpoint, error, answered)
                raise
        with self._lock:
            endpoint.limits = 0
            self._empty_waits.pop(registry, None)
        answered = response.rir or registry
        self._count(self.requests_by_registry, answered)
        persons = person_entities(response.raw_response)
        if persons:
            key = f"{answered}:fallback" if fallback else answered
            self._count(self.person_entities_by_registry, key, persons)
        return response

    def _chain(
        self,
        page: _Page,
        miss: Miss,
        first: Endpoint,
        registry: str,
        *,
        fallback: bool,
    ) -> tuple[RdapLookupResponse, str, str] | MissOutcome:
        """Send one request, re-sending refused cross-registry redirects through the target
        registry's direct endpoint (its pace, budget and pauses); (response, registry that
        answered, source), or a deferred or reused outcome.

        Only the miss's own first request (not a fallback) is charged to max_requests and
        raises _Requeue when the lane's endpoint cannot carry it. RIPE- and APNIC-managed
        space found by a redirect goes to REST/whois, except inside a fallback, which asks
        RDAP (the redirect Location) all the way.
        """
        ip, address = miss.ip, miss.address
        current, url = first, None
        source = "rdap" if fallback else self._source(registry)
        for hop in range(MAX_REROUTES + 1):
            own = hop == 0 and not fallback
            try:
                response = self._send(
                    current, ip, source=source, url=url, fallback=fallback, gate=own
                )
                return response, registry, source
            except _Blocked, _EndpointDown:
                if own:
                    raise _Requeue from None
                return MissOutcome(ip, registry=registry, deferred=True)
            except RdapRedirect as redirect:
                registry = redirect.registry or registry
                self._count(self.reroutes_by_registry, registry)
                if not fallback:
                    matched = self._page_match(page, address)
                    if matched is not None:
                        return MissOutcome(
                            ip, registry=registry, reused=matched[0], cidr=matched[1]
                        )
                current = self._direct(registry)
                source = "rdap" if fallback else self._source(registry)
                url = redirect.location if source == "rdap" else None
            except RdapClientError as error:
                if not _pauses(error):
                    raise
                if own:
                    raise _Requeue from None
                return MissOutcome(ip, registry=registry, deferred=True)
        raise RdapClientError(
            f"More than {MAX_REROUTES} cross-registry redirects",
            code="query_error",
            retryable=False,
        )

    def _fetch(self, page: _Page, miss: Miss, endpoint: Endpoint) -> MissOutcome:
        """FETCH one miss through ``endpoint``: HTTP only, never ClickHouse.

        Raises _Requeue when the endpoint itself cannot be used (paused, at its budget,
        rate limited by this request, or a dead proxy) and _RequestLimit at max_requests.
        """
        ip, address, registry = miss.ip, miss.address, miss.registry
        matched = self._page_match(page, address)
        if matched is not None:  # another miss of this page fetched it: no HTTP
            return MissOutcome(
                ip, registry=registry, reused=matched[0], cidr=matched[1]
            )
        try:
            answer = self._chain(page, miss, endpoint, registry, fallback=False)
            if isinstance(answer, MissOutcome):
                return answer
            response, registry, source = answer
            direct = normalize_rdap_network(
                response, fetched_at=datetime.now(UTC), segment_role="lookup_result"
            )
            if source != "rdap" and (
                is_registry_catch_all(direct) or is_nir_object(response.raw_response)
            ):
                # RIPE's root object or APNIC's placeholder for unallocated / non-authoritative
                # space, or an NIR's own allocation object: RDAP, routed by the IANA bootstrap
                # (which redirects to the NIR server), knows the holder. It is paced, budgeted
                # and paused like any request, through the direct endpoints it reaches.
                self._note(
                    "info",
                    "%s answer for %s is %s; asking RDAP",
                    source,
                    ip,
                    "an NIR object (nir_fallback)"
                    if is_nir_object(response.raw_response)
                    else "a catch-all",
                )
                self._count(self.rdap_fallbacks_by_registry, registry)
                answer = self._chain(
                    page, miss, self._direct(registry), registry, fallback=True
                )
                if isinstance(answer, MissOutcome):
                    return answer
                response = answer[0]
                direct = normalize_rdap_network(
                    response, fetched_at=datetime.now(UTC), segment_role="lookup_result"
                )
            cidr = matching_cidr(direct, address)
            if is_registry_catch_all(direct):
                raise RdapClientError(
                    "Universal registry coverage",
                    code="registry_catch_all",
                    retryable=False,
                )
            if cidr is None:
                raise RdapClientError(
                    "Response range does not contain requested IP",
                    code="range_mismatch",
                    retryable=False,
                )
        except (RdapClientError, ValueError) as error:
            return MissOutcome(
                ip, registry=registry, error=error, checked_at=datetime.now(UTC)
            )
        with self._lock:
            page.fetched.append(direct)  # later misses of any lane reuse it
        outcome = MissOutcome(ip, registry=registry, direct=direct, cidr=cidr)
        self._fetch_parents(outcome, endpoint)
        return outcome

    def _fetch_parents(self, outcome: MissOutcome, endpoint: Endpoint) -> None:
        """Optional parents (up links) up to parent_depth; failures are counted, never fatal."""
        current = outcome.direct
        visited = {current.network.network_key}
        for _ in range(self.config.parent_depth):
            rir = current.network.rir
            # Parents are optional: never from a registry resolved without personal data
            # (its RDAP answers carry person objects).
            if current.network.up_url is None or rir in self._private:
                break
            # Through the lane's endpoint for its own registry, else the parent registry's
            # direct endpoint: a proxy never carries another registry's request.
            via = endpoint if rir == endpoint.registry else self._direct(rir)
            try:
                parent = normalize_rdap_network(
                    self._send(via, current.network.up_url, rir=rir, gate=True),
                    fetched_at=datetime.now(UTC),
                    segment_role="parent",
                )
            except _RequestLimit, _Blocked:
                break
            except (_EndpointDown, RdapClientError, ValueError) as error:
                with self._lock:
                    self.parent_failures += 1
                self._note(
                    "warning",
                    "Optional parent lookup failed for %s: %s",
                    outcome.ip,
                    type(error).__name__,
                )
                break
            if parent.network.network_key in visited:
                break
            visited.add(parent.network.network_key)
            outcome.parents.append(parent)
            current = parent

    # --- COMMIT (calling thread, page order) -------------------------------------

    def _commit(self, order, outcomes, addresses, buckets, results, markers) -> None:
        """Store the page's outcomes in page order: coverage, class and segments before the
        marker that refers to them. A miss that reused a network fetched in this page is
        answered once that network is known to be reusable, else deferred to the next pass
        (where it is asked itself)."""
        reusable: dict[str, NormalizedRdapNetwork] = {}
        reused: list[MissOutcome] = []
        gave_up: dict[str, int] = {}
        for ip in order:
            outcome = outcomes.get(ip)
            if outcome is None:  # deferred while routing, or max_requests reached
                continue
            code = self._gave_up(outcome.registry) if outcome.deferred else None
            if code:
                # Every endpoint of the registry paused out and the run already waited
                # for it after an empty pass: stop deferring, so the run can finish; a
                # retry draft asks again.
                gave_up[outcome.registry] = gave_up.get(outcome.registry, 0) + 1
                outcome.error = RdapClientError(
                    f"{outcome.registry} keeps refusing requests",
                    code=code,
                    retryable=True,
                )
                outcome.checked_at = datetime.now(UTC)
                outcome.deferred = False
            if outcome.deferred:
                self._defer(outcome.registry)
            elif outcome.reused is not None:
                reused.append(outcome)
            elif outcome.error is not None:
                results[ip] = self._error_result(outcome)
                markers.append(
                    self._marker(ip, addresses[ip], buckets[ip], results[ip])
                )
            else:
                results[ip] = self._commit_found(outcome, reusable)
                markers.append(
                    self._marker(ip, addresses[ip], buckets[ip], results[ip])
                )
        for registry, count in gave_up.items():
            self.log.warning(
                "Registry %s paused %s+ times in a row on every endpoint, also after a "
                "wait; %s addresses stored as retryable_error instead of deferred",
                registry,
                MAX_CONSECUTIVE_PAUSES,
                count,
            )
        for outcome in reused:
            source = reusable.get(outcome.reused)
            if source is None:  # a registry-level registration answers only its own IP
                self._defer(outcome.registry)
                continue
            self.cache_hits += 1
            results[outcome.ip] = rdap_result(
                status="found",
                checked_at=source.network.fetched_at,
                network=source.network,
                cidr=outcome.cidr,
            )

    def _error_result(self, outcome: MissOutcome) -> dict:
        error, checked_at = outcome.error, outcome.checked_at
        if isinstance(error, RdapClientError):
            code = error.code
            status = "retryable_error" if error.retryable else "terminal_error"
            if code == "not_found":
                status = "not_found"
        else:
            code, status = "invalid_response", "terminal_error"
        retry_after = None
        if status == "retryable_error":
            seconds = (
                self.config.rate_limit_retry_seconds
                if code == "rate_limited"
                else self.config.transient_retry_seconds
            )
            retry_after = checked_at + timedelta(seconds=seconds)
        return rdap_result(
            status=status,
            checked_at=checked_at,
            error_code=code,
            retry_after=retry_after,
        )

    def _commit_found(self, outcome: MissOutcome, reusable: dict) -> dict:
        direct = outcome.direct
        # Coverage and its class are durable before any exact-IP outcome refers to them.
        # A registry-level or unallocated registration is stored for this address only:
        # the trie excludes it by class (migration 000451) and the in-run cache never holds it.
        classification = classify_registration(self.client, direct.network)
        self._persist(direct, classification)
        if classification.reusable:
            self._remember(direct)
            reusable[direct.network.network_key] = direct
        else:
            self.registry_level_responses += 1
            self.log.info(
                "Registration %s is %s; it answers only %s",
                direct.network.network_key,
                classification.registry_class,
                outcome.ip,
            )
        for parent in outcome.parents:
            self._persist(parent)
        return rdap_result(
            status="found",
            checked_at=direct.network.fetched_at,
            network=direct.network,
            cidr=outcome.cidr,
        )

    def _persist(
        self,
        normalized: NormalizedRdapNetwork,
        classification: RegistryClassification | None = None,
    ) -> None:
        self.client.execute(
            RDAP_NETWORK_INSERT_SQL,
            [normalized.network.clickhouse_values()],
            settings=WRITE_SETTINGS,
        )
        if classification is not None and classification.registry_class != "unknown":
            # The class row lands before the segments: the trie source (migration 000451)
            # never sees a segment whose class it does not know.
            self.client.execute(
                REGISTRY_CLASS_INSERT_SQL,
                [
                    classification.clickhouse_values(
                        normalized.network.network_key, normalized.network.fetched_at
                    )
                ],
                settings=WRITE_SETTINGS,
            )
        self.client.execute(
            RDAP_SEGMENT_INSERT_SQL,
            [segment.clickhouse_values() for segment in normalized.segments],
            settings=WRITE_SETTINGS,
        )
        self.networks_written += 1

    def _remember(self, normalized: NormalizedRdapNetwork) -> None:
        self.recent[normalized.network.network_key] = normalized
        self.recent.move_to_end(normalized.network.network_key)
        if len(self.recent) > 1024:
            self.recent.popitem(last=False)

    def _recent_match(self, address):
        return _best_match(self.recent.values(), address)

    def _marker(self, ip, address, bucket, result) -> tuple:
        return (
            bucket,
            ip,
            address.version,
            result["rdap_lookup_status"],
            result["rdap_network_key"],
            result["rdap_error_code"],
            result["rdap_retry_after"],
            result["rdap_checked_at"],
        )


def _best_match(networks, address):
    """(normalized, cidr) of the most specific (then newest) network holding ``address``."""
    matches = []
    for normalized in networks:
        cidr = matching_cidr(normalized, address)
        if cidr is not None:
            matches.append(
                (
                    ip_network(cidr).prefixlen,
                    normalized.network.fetched_at,
                    normalized.network.network_key,
                    normalized,
                    cidr,
                )
            )
    if not matches:
        return None
    _, _, _, normalized, cidr = max(matches, key=lambda match: match[:3])
    return normalized, cidr
