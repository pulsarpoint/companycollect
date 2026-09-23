"""Shared IP lookups using the existing MaxMind mappings and RDAP network cache."""

import json
import time
from collections import OrderedDict
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from ipaddress import ip_address, ip_network
from uuid import UUID

import dagster as dg
import maxminddb
from pydantic import Field, field_validator

from dagster_v3.defs.commoncrawl_geoip.maxmind import (
    build_geoip_enrichment,
    classify_ip_scope,
    lookup_maxmind_record,
)
from dagster_v3.defs.commoncrawl_rdap.assets import (
    RDAP_LOOKUP_INSERT_SQL,
    RDAP_NETWORK_INSERT_SQL,
    RDAP_SEGMENT_INSERT_SQL,
)
from dagster_v3.defs.commoncrawl_rdap.client import RdapClient, RdapClientError
from dagster_v3.defs.commoncrawl_rdap.rdap import (
    NormalizedRdapNetwork,
    RdapLookupResponse,
    is_registry_catch_all,
    normalize_rdap_network,
)


class IpEnrichmentResultsConfig(dg.Config):
    task_id: str = Field(description="UUID produced by ip_enrichment_input.")
    execution_id: str | None = Field(
        default=None,
        description="Original results run UUID to resume without repeating saved outcomes.",
    )
    batch_size: int = Field(default=250, ge=1, le=10_000)
    max_requests: int = Field(
        default=250,
        ge=1,
        description="RDAP HTTP request budget per run, including parents.",
    )
    request_delay_seconds: float = Field(default=1.0, ge=0, le=60)
    parent_depth: int = Field(default=1, ge=0, le=5)
    rdap_cache_days: int = Field(default=30, ge=1)
    force_rdap: bool = False
    rate_limit_retry_seconds: int = Field(default=3600, ge=1)
    transient_retry_seconds: int = Field(default=900, ge=1)

    @field_validator("task_id", "execution_id")
    @classmethod
    def uuid_identity(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None


class RequestBudgetReached(Exception):
    """Leave this input unfinished so the same execution can resume it later."""


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


class RdapEnricher:
    """Bound HTTP work and reuse the most-specific known fresh network coverage."""

    def __init__(
        self, client, rdap: RdapClient, config: IpEnrichmentResultsConfig, log
    ):
        self.client = client
        self.rdap = rdap
        self.config = config
        self.log = log
        self.requests = 0
        self.cache_hits = 0
        self.networks_written = 0
        self.parent_failures = 0
        self.recent: OrderedDict[str, NormalizedRdapNetwork] = OrderedDict()

    def _request(self, address_or_url, *, rir=None):
        if self.requests >= self.config.max_requests:
            raise RequestBudgetReached
        if self.requests and self.config.request_delay_seconds:
            time.sleep(self.config.request_delay_seconds)
        self.requests += 1
        if rir is not None:
            return self.rdap.lookup_up_url(address_or_url, rir=rir)
        return self.rdap.lookup_ip(address_or_url)

    def _persist(self, normalized):
        self.client.execute(
            RDAP_NETWORK_INSERT_SQL,
            [normalized.network.clickhouse_values()],
            settings={"async_insert": 0},
        )
        self.client.execute(
            RDAP_SEGMENT_INSERT_SQL,
            [segment.clickhouse_values() for segment in normalized.segments],
            settings={"async_insert": 0},
        )
        self.networks_written += 1

    def _remember(self, normalized):
        self.recent[normalized.network.network_key] = normalized
        self.recent.move_to_end(normalized.network.network_key)
        if len(self.recent) > 1024:
            self.recent.popitem(last=False)

    def _cached_network(self, ip, address, now):
        candidates = list(self.recent.values())
        if not self.config.force_rdap:
            conversion = "toIPv4" if address.version == 4 else "toIPv6"
            rows = self.client.execute(
                f"""SELECT rir, raw_response, fetched_at
                FROM corpscout.rdap_networks_current
                WHERE network_key = dictGetOrDefault('corpscout.rdap_network_trie',
                    'network_key', tuple({conversion}(%(ip)s)), '')
                  AND fetched_at >= %(cutoff)s""",
                {"ip": ip, "cutoff": now - timedelta(days=self.config.rdap_cache_days)},
            )
            for rir, raw, fetched_at in rows:
                candidates.append(
                    normalize_rdap_network(
                        RdapLookupResponse(rir=rir, raw_response=json.loads(raw)),
                        fetched_at=fetched_at,
                        segment_role="lookup_result",
                    )
                )
        matches = []
        for normalized in candidates:
            cidr = matching_cidr(normalized, address)
            if cidr is not None and not is_registry_catch_all(normalized):
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
        self._remember(normalized)
        self.cache_hits += 1
        return rdap_result(
            status="found",
            checked_at=normalized.network.fetched_at,
            network=normalized.network,
            cidr=cidr,
        )

    def _cached_negative(self, ip, address, bucket, now):
        if self.config.force_rdap:
            return None
        rows = self.client.execute(
            """SELECT lookup_status, error_code, retry_after, queried_at
            FROM corpscout.rdap_ip_lookup_results_current
            WHERE bucket=%(bucket)s AND ip_version=%(version)s AND ip=%(ip)s""",
            {"bucket": bucket, "version": address.version, "ip": ip},
        )
        if not rows:
            return None
        status, code, retry_after, checked_at = rows[0]
        if status == "retryable_error":
            if retry_after is None or retry_after <= now:
                return None
        elif status in {"not_found", "unsupported", "terminal_error"}:
            if checked_at < now - timedelta(days=self.config.rdap_cache_days):
                return None
        else:
            return None
        self.cache_hits += 1
        return rdap_result(
            status="terminal_error" if status == "unsupported" else status,
            checked_at=checked_at,
            error_code=code,
            retry_after=retry_after,
        )

    def _save_lookup(self, ip, address, bucket, result):
        self.client.execute(
            RDAP_LOOKUP_INSERT_SQL,
            [
                (
                    bucket,
                    ip,
                    address.version,
                    result["rdap_lookup_status"],
                    result["rdap_network_key"],
                    result["rdap_error_code"],
                    result["rdap_retry_after"],
                    result["rdap_checked_at"],
                )
            ],
            settings={"async_insert": 0},
        )
        return result

    def lookup(self, ip, bucket):
        address = ip_address(ip)
        now = datetime.now(UTC)
        if classify_ip_scope(address) != "global":
            return self._save_lookup(
                ip, address, bucket, rdap_result(status="not_global", checked_at=now)
            )
        cached = self._cached_negative(ip, address, bucket, now)
        if cached is None:
            cached = self._cached_network(ip, address, now)
        if cached is not None:
            return cached
        try:
            response = self._request(ip)
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
            return self._save_lookup(
                ip,
                address,
                bucket,
                rdap_result(
                    status=status,
                    checked_at=checked_at,
                    error_code=code,
                    retry_after=retry_after,
                ),
            )
        # Coverage is durable before any exact-IP outcome refers to it.
        self._persist(direct)
        self._remember(direct)
        current = direct
        visited = {direct.network.network_key}
        for _ in range(self.config.parent_depth):
            if (
                current.network.up_url is None
                or self.requests >= self.config.max_requests
            ):
                break
            try:
                parent_response = self._request(
                    current.network.up_url, rir=current.network.rir
                )
                parent = normalize_rdap_network(
                    parent_response, fetched_at=datetime.now(UTC), segment_role="parent"
                )
                if parent.network.network_key in visited:
                    break
                visited.add(parent.network.network_key)
                self._persist(parent)
                current = parent
            except (RdapClientError, ValueError) as error:
                self.parent_failures += 1
                self.log.warning(
                    "Optional RDAP parent lookup failed for %s: %s",
                    ip,
                    type(error).__name__,
                )
                break
        return self._save_lookup(
            ip,
            address,
            bucket,
            rdap_result(
                status="found",
                checked_at=direct.network.fetched_at,
                network=direct.network,
                cidr=cidr,
            ),
        )
