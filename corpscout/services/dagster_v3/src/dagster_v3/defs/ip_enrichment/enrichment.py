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

Counters: requests and person entities are keyed by the registry that answered
(RdapLookupResponse.rir); RDAP fallbacks of the no-personal paths are counted in
rdap_fallbacks_by_registry and their person entities under "<rir>:fallback" keys, so a
plain "ripe" or "apnic" key in person_entities_by_registry means personal data leaked.

An optional rolling 24-hour request budget per registry defers misses instead of
exceeding it, and a rate-limited or blocked registry is paused for
max(retry delay, 15 min) with its misses deferred the same way; the loop waits for the
window only when nothing else remains. A failed IANA bootstrap defers every miss the same
way (key "bootstrap", back-off 1 to 15 minutes) instead of storing an error per address.
"""

from collections import OrderedDict, deque
from collections.abc import Callable, Mapping
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from ipaddress import ip_address, ip_network
from time import monotonic, sleep
from uuid import UUID

import dagster as dg
import maxminddb
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
# A rate-limited or blocked registry is left alone at least this long.
MIN_PAUSE_SECONDS = 900
# Codes that pause the registry that answered them (403 from RIPE REST is retryable).
PAUSE_CODES = frozenset({"rate_limited", "access_denied"})
# A failed IANA bootstrap pauses every miss of the run (deferral key "bootstrap") instead
# of writing a retryable error per address; the pause doubles from 1 to at most 15 minutes.
BOOTSTRAP = "bootstrap"
BOOTSTRAP_CODES = frozenset({"bootstrap_error", "bootstrap_transport_error"})
MIN_BOOTSTRAP_PAUSE_SECONDS = 60
MAX_BOOTSTRAP_PAUSE_SECONDS = 900


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
    request_delay_seconds: float = Field(default=1.0, ge=0, le=60)
    registry_daily_budgets: dict[str, int] = Field(
        default_factory=dict,
        description="Optional rolling 24-hour request budget per registry, keyed by whoisit's "
        "registry names (ripe, arin, apnic, lacnic, afrinic, jpnic, ...). A miss of a registry "
        "at its budget is deferred, never failed; the run waits when nothing else remains.",
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
        known = sorted(set(RIR_BY_HOST.values()))
        budgets = {}
        for registry, budget in value.items():
            name = registry.strip().lower()
            if name not in known:
                raise ValueError(
                    f"registry_daily_budgets: unknown registry {registry!r}; use one of {known}"
                )
            if budget < 1:
                raise ValueError("registry_daily_budgets needs budgets >= 1")
            budgets[name] = budget
        return budgets


def geoip_result(ip, city_reader, asn_reader, *, checked_at, retry_seconds):
    address = ip_address(ip)
    city_meta, asn_meta = city_reader.metadata(), asn_reader.metadata()
    errors = {}
    lookups = {}
    for component, reader in (("city", city_reader), ("asn", asn_reader)):
        lookups[component] = None
        if classify_ip_scope(address) == "global":
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


def person_entities(raw: Mapping) -> int:
    """Entities whose vCard is of kind 'individual', nested included.

    RIPE counts person objects against its daily limit; RDAP answers of the other
    registries carry them too. An upper bound (RIPE marks maintainers 'individual' as well).
    The REST and whois shapes never carry any, so their count is 0.
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
                and item[3] == "individual"
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


class RdapEnricher:
    """Resolve registry coverage for a page of addresses with a bounded number of round trips."""

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
    ):
        self.client = client
        self.rdap = rdap
        self.ripe = ripe
        self.apnic = apnic
        self.config = config
        self.log = log
        self.started_at = started_at
        self.cache_cutoff = cache_cutoff
        # Resolved at construction so tests can patch the module names.
        self._clock = clock or monotonic
        self._sleep = sleep or _module_sleep()
        self.requests = 0
        self.cache_hits = 0
        self.networks_written = 0
        self.parent_failures = 0
        self.registry_level_responses = 0
        self.budget_reached = False
        # Keyed by the answering registry (RdapLookupResponse.rir); a failed request by
        # the registry it was sent to.
        self.requests_by_registry: dict[str, int] = {}
        # Person entities per answering registry; RDAP fallbacks of the no-personal
        # paths count under "<rir>:fallback", so a plain ripe/apnic key is a leak.
        self.person_entities_by_registry: dict[str, int] = {}
        # RDAP requests made because a REST/whois answer was a catch-all or an NIR's own
        # object, keyed by the registry whose no-personal path fell back.
        self.rdap_fallbacks_by_registry: dict[str, int] = {}
        # Misses an RDAP server redirected to RIPE/APNIC, re-sent to REST/whois.
        self.reroutes_by_registry: dict[str, int] = {}
        # Rate-limit / block pauses started per registry.
        self.pauses_by_registry: dict[str, int] = {}
        self.deferrals_by_registry: dict[str, int] = {}
        self.deferred: dict[str, int] = {}  # since the last reset_pass()
        # Registries resolved without personal data, and the RDAP hosts whose redirects
        # are re-sent to those paths instead of being followed.
        self._private = frozenset(
            name
            for name, enabled in (
                ("ripe", config.ripe_rest),
                ("apnic", config.apnic_whois),
            )
            if enabled
        )
        self._reroute_hosts = frozenset(
            host for host, name in RIR_BY_HOST.items() if name in self._private
        )
        # Monotonic time until which a rate-limited or blocked registry is not asked.
        self._paused_until: dict[str, float] = {}
        self._bootstrap_failures = 0
        # Reusable networks fetched over HTTP in this run, checked before any request.
        self.recent: OrderedDict[str, NormalizedRdapNetwork] = OrderedDict()
        # Fresh network rows read from ClickHouse, keyed by network_key.
        self.cached: OrderedDict[str, RdapNetwork] = OrderedDict()
        # Monotonic send times per budgeted registry inside the rolling window, oldest first.
        self._sent: dict[str, deque[float]] = {}
        # Never evict keys a page just loaded: a page needs at most batch_size keys.
        self._cached_cap = max(4096, 2 * config.batch_size)

    # --- page resolution -------------------------------------------------------

    def resolve_page(self, rows: list[dict]) -> dict[str, dict]:
        """Registry fields per address of the page.

        An address is absent when it was deferred (its registry at its daily budget or
        paused, or the bootstrap paused) or when the run's max_requests budget ran out
        (``budget_reached`` is then True).
        """
        addresses = {row["ip"]: ip_address(row["ip"]) for row in rows}
        buckets = {row["ip"]: row["bucket"] for row in rows}
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
            if self._budget_exhausted():
                self.budget_reached = True
                break
            result = self._request_ip(ip, addresses[ip], buckets[ip], markers)
            if result is not None:
                results[ip] = result
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

    # --- registry budgets -------------------------------------------------------

    def seed_registry_usage(self, rows: list[tuple[str, float]]) -> None:
        """Requests any run made in the last day (registry, seconds ago), oldest first."""
        now = self._clock()
        for registry, seconds_ago in rows:
            if (
                registry in self.config.registry_daily_budgets
                and seconds_ago < BUDGET_WINDOW_SECONDS
            ):
                self._sent.setdefault(registry, deque()).append(now - seconds_ago)

    def _window(self, registry: str) -> deque[float]:
        times = self._sent.setdefault(registry, deque())
        horizon = self._clock() - BUDGET_WINDOW_SECONDS
        while times and times[0] <= horizon:
            times.popleft()
        return times

    def _over_budget(self, registry: str) -> bool:
        budget = self.config.registry_daily_budgets.get(registry)
        return budget is not None and len(self._window(registry)) >= budget

    def _paused(self, registry: str) -> bool:
        return self._paused_until.get(registry, float("-inf")) > self._clock()

    def _blocked(self, registry: str) -> bool:
        """At its daily budget or paused after a rate limit: its misses are deferred."""
        return self._over_budget(registry) or self._paused(registry)

    def _pause(self, registry: str, seconds: float) -> None:
        until = self._clock() + max(seconds, MIN_PAUSE_SECONDS)
        if until > self._paused_until.get(registry, float("-inf")):
            self._paused_until[registry] = until
        self.pauses_by_registry[registry] = self.pauses_by_registry.get(registry, 0) + 1
        self.log.warning(
            "Registry %r is rate limiting or blocking; paused for %.0f s",
            registry,
            max(seconds, MIN_PAUSE_SECONDS),
        )

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
        self.deferred[registry] = self.deferred.get(registry, 0) + 1
        self.deferrals_by_registry[registry] = (
            self.deferrals_by_registry.get(registry, 0) + 1
        )

    def reset_pass(self) -> None:
        self.deferred = {}

    def seconds_until_budget_frees(self) -> float:
        """Seconds until a deferred registry may be asked again (0 when none is deferred).

        A paused registry waits for the end of its pause; a registry at its budget waits
        for an hour's share of the budget (at least one request), so the pass that
        follows is worth its ClickHouse queries.
        """
        now = self._clock()
        waits = []
        for registry, deferred in self.deferred.items():
            wait = max(0.0, self._paused_until.get(registry, now) - now)
            budget = self.config.registry_daily_budgets.get(registry)
            if budget is not None:
                times = self._window(registry)
                if len(times) >= budget:
                    slots = max(1, min(deferred, budget // 24))
                    wait = max(
                        wait,
                        times[len(times) - budget + slots - 1]
                        + BUDGET_WINDOW_SECONDS
                        - now,
                    )
            waits.append(wait)
        return max(0.0, min(waits)) if waits else 0.0

    def wait_for_registry_budget(self) -> float:
        """Sleep, in slices of at most a minute, until a deferred registry frees slots."""
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

    # --- misses ---------------------------------------------------------------

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
        counter[key] = counter.get(key, 0) + amount

    def _request(
        self,
        target: str,
        *,
        registry: str,
        rir: str | None = None,
        source: str = "rdap",
        fallback: bool = False,
    ) -> RdapLookupResponse:
        """One paced request through a source: 'rdap', 'ripe_rest' or 'apnic_whois'.

        The budget window is charged to ``registry`` (where the request is sent); the
        counters to the registry that answered. A fallback request follows every RDAP
        redirect (it is the deliberate RDAP answer); any other RDAP request raises
        RdapRedirect for a redirect to RIPE's or APNIC's RDAP server.
        """
        if self.requests and self.config.request_delay_seconds:
            self._sleep(self.config.request_delay_seconds)
        self.requests += 1
        if registry in self.config.registry_daily_budgets:
            self._sent.setdefault(registry, deque()).append(self._clock())
        if fallback:
            self._count(self.rdap_fallbacks_by_registry, registry)
        self.rdap.reroute_hosts = frozenset() if fallback else self._reroute_hosts
        try:
            if rir is not None:
                response = self.rdap.lookup_up_url(target, rir=rir)
            elif source == "ripe_rest":
                response = self.ripe.lookup_ip(target)
            elif source == "apnic_whois":
                response = self.apnic.lookup_ip(target)
            else:
                response = self.rdap.lookup_ip(target)
        except RdapClientError:
            self._count(self.requests_by_registry, registry)
            raise
        answered = response.rir or registry
        self._count(self.requests_by_registry, answered)
        persons = person_entities(response.raw_response)
        if persons:
            key = f"{answered}:fallback" if fallback else answered
            self._count(self.person_entities_by_registry, key, persons)
        return response

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
        matches = []
        for normalized in self.recent.values():
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

    def _request_ip(self, ip, address, bucket, markers: list[tuple]) -> dict | None:
        registry = ""
        if self._paused(BOOTSTRAP):
            self._defer(BOOTSTRAP)
            return None
        try:
            try:
                registry = self.rdap.registry_for(ip)
            except RdapClientError as error:
                if error.code not in BOOTSTRAP_CODES:
                    raise
                # No registry can be chosen without the bootstrap: a run-wide pause,
                # never an error per address.
                self._bootstrap_failed(error)
                self._defer(BOOTSTRAP)
                return None
            self._bootstrap_failures = 0
            if registry == "":
                # No exact bootstrap match: whoisit would send the query to a random
                # registry (possibly RIPE's or APNIC's RDAP). Nothing is requested; the
                # miss is retried after transient_retry_seconds.
                raise RdapClientError(
                    f"No registry is known for {ip}",
                    code="no_registry",
                    retryable=True,
                )
            if self._blocked(registry):
                self._defer(registry)
                return None
            source = self._source(registry)
            try:
                response = self._request(ip, registry=registry, source=source)
            except RdapRedirect as redirect:
                # RIPE- or APNIC-managed space inside another registry's block: the
                # redirect is not followed; the no-personal path answers instead.
                self._count(self.reroutes_by_registry, redirect.registry)
                registry = redirect.registry
                if self._blocked(registry):
                    self._defer(registry)
                    return None
                source = self._source(registry)
                response = self._request(ip, registry=registry, source=source)
            checked_at = datetime.now(UTC)
            direct = normalize_rdap_network(
                response, fetched_at=checked_at, segment_role="lookup_result"
            )
            if source != "rdap" and (
                is_registry_catch_all(direct) or is_nir_object(response.raw_response)
            ):
                # RIPE's root object or APNIC's placeholder for unallocated / non-authoritative
                # space, or an NIR's own allocation object: RDAP, routed by the IANA bootstrap
                # (which redirects to the NIR server), knows the holder. One request past the
                # budget at most.
                self.log.info(
                    "%s answer for %s is %s; asking RDAP",
                    source,
                    ip,
                    "an NIR object (nir_fallback)"
                    if is_nir_object(response.raw_response)
                    else "a catch-all",
                )
                response = self._request(
                    ip, registry=registry, source="rdap", fallback=True
                )
                checked_at = datetime.now(UTC)
                direct = normalize_rdap_network(
                    response, fetched_at=checked_at, segment_role="lookup_result"
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
            checked_at = datetime.now(UTC)
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
                if code in PAUSE_CODES:
                    self._pause(registry, seconds)
            result = rdap_result(
                status=status,
                checked_at=checked_at,
                error_code=code,
                retry_after=retry_after,
            )
            markers.append(self._marker(ip, address, bucket, result))
            return result
        # Coverage and its class are durable before any exact-IP outcome refers to them.
        # A registry-level or unallocated registration is stored for this address only:
        # the trie excludes it by class (migration 000451) and the in-run cache never holds it.
        classification = classify_registration(self.client, direct.network)
        self._persist(direct, classification)
        if classification.reusable:
            self._remember(direct)
        else:
            self.registry_level_responses += 1
            self.log.info(
                "Registration %s is %s; it answers only %s",
                direct.network.network_key,
                classification.registry_class,
                ip,
            )
        current = direct
        visited = {direct.network.network_key}
        for _ in range(self.config.parent_depth):
            if (
                current.network.up_url is None
                or self._budget_exhausted()
                # Parents are optional: never from a registry resolved without personal
                # data (its RDAP answers carry person objects), nor a blocked one.
                or current.network.rir in self._private
                or self._blocked(current.network.rir)
            ):
                break
            try:
                parent = normalize_rdap_network(
                    self._request(
                        current.network.up_url,
                        registry=current.network.rir,
                        rir=current.network.rir,
                    ),
                    fetched_at=datetime.now(UTC),
                    segment_role="parent",
                )
                if parent.network.network_key in visited:
                    break
                visited.add(parent.network.network_key)
                self._persist(parent)
                current = parent
            except (RdapClientError, ValueError) as error:
                self.parent_failures += 1
                if (
                    isinstance(error, RdapClientError)
                    and error.retryable
                    and error.code in PAUSE_CODES
                ):
                    self._pause(
                        current.network.rir,
                        self.config.rate_limit_retry_seconds
                        if error.code == "rate_limited"
                        else self.config.transient_retry_seconds,
                    )
                self.log.warning(
                    "Optional parent lookup failed for %s: %s", ip, type(error).__name__
                )
                break
        result = rdap_result(
            status="found",
            checked_at=direct.network.fetched_at,
            network=direct.network,
            cidr=cidr,
        )
        markers.append(self._marker(ip, address, bucket, result))
        return result
