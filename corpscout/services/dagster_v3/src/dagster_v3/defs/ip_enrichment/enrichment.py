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
every other registry through RDAP. An optional
rolling 24-hour request budget per registry defers misses instead of exceeding it; the
loop waits for the window only when nothing else remains.
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
from dagster_v3.defs.commoncrawl_rdap.client import RdapClient, RdapClientError
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


class RequestBudgetReached(Exception):
    """Kept for results.py until Task 5 replaces it with RdapEnricher.budget_reached."""


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
        budgets = {}
        for registry, budget in value.items():
            if not registry.strip() or budget < 1:
                raise ValueError(
                    "registry_daily_budgets needs registry names and budgets >= 1"
                )
            budgets[registry.strip().lower()] = budget
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
        self.requests_by_registry: dict[str, int] = {}
        self.person_entities_by_registry: dict[str, int] = {}
        self.deferrals_by_registry: dict[str, int] = {}
        self.deferred: dict[str, int] = {}  # since the last reset_pass()
        # Reusable networks fetched over HTTP in this run, checked before any request.
        self.recent: OrderedDict[str, NormalizedRdapNetwork] = OrderedDict()
        # Fresh network rows read from ClickHouse, keyed by network_key.
        self.cached: OrderedDict[str, RdapNetwork] = OrderedDict()
        # Monotonic send times per registry inside the rolling window, oldest first.
        self._sent: dict[str, deque[float]] = {}

    # --- page resolution -------------------------------------------------------

    def resolve_page(self, rows: list[dict]) -> dict[str, dict]:
        """Registry fields per address of the page.

        An address is absent when its registry's daily budget deferred it or when the
        run's max_requests budget ran out (``budget_reached`` is then True).
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
        while len(self.cached) > 4096:
            self.cached.popitem(last=False)

    # --- registry budgets -------------------------------------------------------

    def seed_registry_usage(self, rows: list[tuple[str, float]]) -> None:
        """Requests any run made in the last day (registry, seconds ago), oldest first."""
        now = self._clock()
        for registry, seconds_ago in rows:
            if seconds_ago < BUDGET_WINDOW_SECONDS:
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

    def _defer(self, registry: str) -> None:
        self.deferred[registry] = self.deferred.get(registry, 0) + 1
        self.deferrals_by_registry[registry] = (
            self.deferrals_by_registry.get(registry, 0) + 1
        )

    def reset_pass(self) -> None:
        self.deferred = {}

    def seconds_until_budget_frees(self) -> float:
        """Seconds until a deferred registry may be asked again (0 when none is deferred).

        Waits for an hour's share of the registry's budget (at least one request), so
        the pass that follows is worth its ClickHouse queries.
        """
        waits = []
        for registry, deferred in self.deferred.items():
            budget = self.config.registry_daily_budgets.get(registry)
            times = self._window(registry)
            if budget is None or len(times) < budget:
                return 0.0
            slots = max(1, min(deferred, budget // 24))
            waits.append(
                times[len(times) - budget + slots - 1]
                + BUDGET_WINDOW_SECONDS
                - self._clock()
            )
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

    def _request(
        self,
        target: str,
        *,
        registry: str,
        rir: str | None = None,
        source: str = "rdap",
    ) -> RdapLookupResponse:
        """One paced request through the registry's source: 'rdap', 'ripe_rest' or 'apnic_whois'."""
        if self.requests and self.config.request_delay_seconds:
            self._sleep(self.config.request_delay_seconds)
        self.requests += 1
        self._sent.setdefault(registry, deque()).append(self._clock())
        self.requests_by_registry[registry] = (
            self.requests_by_registry.get(registry, 0) + 1
        )
        if rir is not None:
            response = self.rdap.lookup_up_url(target, rir=rir)
        elif source == "ripe_rest":
            response = self.ripe.lookup_ip(target)
        elif source == "apnic_whois":
            response = self.apnic.lookup_ip(target)
        else:
            response = self.rdap.lookup_ip(target)
        persons = person_entities(response.raw_response)
        if persons:
            self.person_entities_by_registry[registry] = (
                self.person_entities_by_registry.get(registry, 0) + persons
            )
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
        registry = self.rdap.registry_for(ip)
        if self._over_budget(registry):
            self._defer(registry)
            return None
        source = "rdap"
        if registry == "ripe" and self.config.ripe_rest:
            source = "ripe_rest"
        elif registry == "apnic" and self.config.apnic_whois:
            source = "apnic_whois"
        try:
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
                response = self._request(ip, registry=registry, source="rdap")
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
                or self._over_budget(current.network.rir)  # parents are optional
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
