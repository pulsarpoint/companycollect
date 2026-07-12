import hashlib
import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from ipaddress import ip_address
from typing import Any

from netaddr import iprange_to_cidrs


VALID_SEGMENT_ROLES = frozenset({"lookup_result", "parent"})


@dataclass(frozen=True)
class RdapLookupResponse:
    rir: str
    raw_response: Mapping[str, Any]


@dataclass(frozen=True)
class RdapNetwork:
    network_key: str
    rir: str
    handle: str
    ip_version: int
    start_address: str
    end_address: str
    name: str | None
    registration_type: str | None
    country_code: str | None
    status: tuple[str, ...]
    registrant_handles: tuple[str, ...]
    registrant_names: tuple[str, ...]
    parent_network_key: str | None
    parent_handle: str | None
    self_url: str | None
    up_url: str | None
    registration_date: datetime | None
    last_changed_at: datetime | None
    response_sha256: str
    raw_response: str
    fetched_at: datetime

    def clickhouse_values(self) -> tuple[Any, ...]:
        return (
            self.network_key,
            self.rir,
            self.handle,
            self.ip_version,
            self.start_address,
            self.end_address,
            self.name,
            self.registration_type,
            self.country_code,
            list(self.status),
            list(self.registrant_handles),
            list(self.registrant_names),
            self.parent_network_key,
            self.parent_handle,
            self.self_url,
            self.up_url,
            self.registration_date,
            self.last_changed_at,
            self.response_sha256,
            self.raw_response,
            self.fetched_at,
        )


@dataclass(frozen=True)
class RdapNetworkSegment:
    network_key: str
    cidr: str
    ip_version: int
    prefix_length: int
    segment_role: str
    response_sha256: str
    derived_at: datetime

    def clickhouse_values(self) -> tuple[Any, ...]:
        return (
            self.network_key,
            self.cidr,
            self.ip_version,
            self.prefix_length,
            self.segment_role,
            self.response_sha256,
            self.derived_at,
        )


@dataclass(frozen=True)
class NormalizedRdapNetwork:
    network: RdapNetwork
    segments: tuple[RdapNetworkSegment, ...]


@dataclass(frozen=True)
class RdapIpLookupResult:
    bucket: int
    ip: str
    ip_version: int
    lookup_status: str
    network_key: str | None
    error_code: str | None
    retry_after: datetime | None
    queried_at: datetime

    def clickhouse_values(self) -> tuple[Any, ...]:
        return (
            self.bucket,
            self.ip,
            self.ip_version,
            self.lookup_status,
            self.network_key,
            self.error_code,
            self.retry_after,
            self.queried_at,
        )


def normalize_rdap_network(
    response: RdapLookupResponse,
    *,
    fetched_at: datetime,
    segment_role: str,
) -> NormalizedRdapNetwork:
    if segment_role not in VALID_SEGMENT_ROLES:
        raise ValueError(
            f"segment_role must be one of {sorted(VALID_SEGMENT_ROLES)}, "
            f"got {segment_role!r}"
        )
    if fetched_at.tzinfo is None:
        raise ValueError("fetched_at must be timezone-aware")

    raw = response.raw_response
    if _optional_string(raw.get("objectClassName")) != "ip network":
        raise ValueError("RDAP response must describe an ip network")

    rir = _required_string(response.rir, "rir").lower()
    handle = _required_string(raw.get("handle"), "handle").upper()
    start = ip_address(_required_string(raw.get("startAddress"), "startAddress"))
    end = ip_address(_required_string(raw.get("endAddress"), "endAddress"))
    if start.version != end.version:
        raise ValueError("RDAP startAddress and endAddress use different IP versions")
    if int(start) > int(end):
        raise ValueError("RDAP startAddress must not be greater than endAddress")
    _validate_declared_ip_version(raw.get("ipVersion"), start.version)

    network_key = f"{rir}:{handle}"
    parent_handle = _optional_string(raw.get("parentHandle"))
    if parent_handle is not None:
        parent_handle = parent_handle.upper()
    registrant_handles, registrant_names = _registrants(raw.get("entities"))
    canonical_raw = json.dumps(
        raw,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    response_sha256 = hashlib.sha256(canonical_raw.encode("utf-8")).hexdigest()

    network = RdapNetwork(
        network_key=network_key,
        rir=rir,
        handle=handle,
        ip_version=start.version,
        start_address=str(start),
        end_address=str(end),
        name=_optional_string(raw.get("name")),
        registration_type=_optional_string(raw.get("type")),
        country_code=_uppercase_optional_string(raw.get("country")),
        status=_string_tuple(raw.get("status")),
        registrant_handles=registrant_handles,
        registrant_names=registrant_names,
        parent_network_key=(
            f"{rir}:{parent_handle}" if parent_handle is not None else None
        ),
        parent_handle=parent_handle,
        self_url=_link(raw.get("links"), "self"),
        up_url=_link(raw.get("links"), "up"),
        registration_date=_event_date(raw.get("events"), "registration"),
        last_changed_at=_event_date(raw.get("events"), "last changed"),
        response_sha256=response_sha256,
        raw_response=canonical_raw,
        fetched_at=fetched_at.astimezone(UTC),
    )
    cidrs = tuple(iprange_to_cidrs(str(start), str(end)))
    for cidr in cidrs:
        if cidr.version != start.version:
            raise ValueError("Derived CIDR uses a different IP version than its range")
        if cidr.first < int(start) or cidr.last > int(end):
            raise ValueError("Derived CIDR extends outside its RDAP address range")

    segments = tuple(
        RdapNetworkSegment(
            network_key=network_key,
            cidr=str(cidr),
            ip_version=start.version,
            prefix_length=int(cidr.prefixlen),
            segment_role=segment_role,
            response_sha256=response_sha256,
            derived_at=fetched_at.astimezone(UTC),
        )
        for cidr in cidrs
    )
    return NormalizedRdapNetwork(network=network, segments=segments)


def _validate_declared_ip_version(value: Any, actual_version: int) -> None:
    declared = _optional_string(value)
    if declared is None:
        return
    expected = f"v{actual_version}"
    if declared.lower() != expected:
        raise ValueError(
            f"RDAP ipVersion {declared!r} does not match address version {expected!r}"
        )


def _registrants(value: Any) -> tuple[tuple[str, ...], tuple[str, ...]]:
    handles: list[str] = []
    names: list[str] = []
    for entity in _iter_entities(value):
        roles = {role.lower() for role in _string_tuple(entity.get("roles"))}
        if "registrant" not in roles:
            continue
        handle = _uppercase_optional_string(entity.get("handle"))
        name = _vcard_name(entity.get("vcardArray"))
        if handle is not None and handle not in handles:
            handles.append(handle)
        if name is not None and name not in names:
            names.append(name)
    return tuple(handles), tuple(names)


def _iter_entities(value: Any) -> Iterator[Mapping[str, Any]]:
    if not isinstance(value, list):
        return
    for entity in value:
        if not isinstance(entity, Mapping):
            continue
        yield entity
        yield from _iter_entities(entity.get("entities"))


def _vcard_name(value: Any) -> str | None:
    if not isinstance(value, list) or len(value) != 2 or value[0] != "vcard":
        return None
    properties = value[1]
    if not isinstance(properties, list):
        return None

    full_name: str | None = None
    organization: str | None = None
    for prop in properties:
        if not isinstance(prop, list) or len(prop) != 4:
            continue
        field_name = _optional_string(prop[0])
        text = _vcard_text(prop[3])
        if field_name == "fn" and text is not None:
            full_name = text
        if field_name == "org" and text is not None:
            organization = text
    return organization or full_name


def _vcard_text(value: Any) -> str | None:
    if isinstance(value, list):
        parts = tuple(
            text for item in value if (text := _optional_string(item)) is not None
        )
        return " ".join(parts) if parts else None
    return _optional_string(value)


def _link(value: Any, relation: str) -> str | None:
    if not isinstance(value, list):
        return None
    for link in value:
        if not isinstance(link, Mapping):
            continue
        rel = _optional_string(link.get("rel"))
        if rel is not None and rel.lower() == relation:
            return _optional_string(link.get("href"))
    return None


def _event_date(value: Any, action: str) -> datetime | None:
    if not isinstance(value, list):
        return None
    dates: list[datetime] = []
    for event in value:
        if not isinstance(event, Mapping):
            continue
        event_action = _optional_string(event.get("eventAction"))
        event_date = _optional_string(event.get("eventDate"))
        if (
            event_action is not None
            and event_action.lower() == action
            and event_date is not None
        ):
            dates.append(_parse_datetime(event_date))
    if not dates:
        return None
    if action == "registration":
        return min(dates)
    return max(dates)


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(text for item in value if (text := _optional_string(item)) is not None)


def _uppercase_optional_string(value: Any) -> str | None:
    text = _optional_string(value)
    return text.upper() if text is not None else None


def _required_string(value: Any, field_name: str) -> str:
    text = _optional_string(value)
    if text is None:
        raise ValueError(f"RDAP response is missing required {field_name}")
    return text


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text != "" else None
