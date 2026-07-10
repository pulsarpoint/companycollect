from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import cache
from ipaddress import (
    IPv4Address,
    IPv4Network,
    IPv6Address,
    IPv6Network,
    ip_network,
)
from typing import Any


type IPAddress = IPv4Address | IPv6Address
type IPNetwork = IPv4Network | IPv6Network
type MaxMindRecord = Mapping[str, Any]


@dataclass(frozen=True)
class MaxMindLookup:
    record: MaxMindRecord | None
    prefix_length: int


@dataclass(frozen=True)
class GeoIPEnrichment:
    bucket: int
    ip: str
    ip_version: int
    ip_scope: str
    city_lookup_status: str
    asn_lookup_status: str
    continent_code: str | None
    continent_name: str | None
    country_iso_code: str | None
    country_name: str | None
    country_geoname_id: int | None
    registered_country_iso_code: str | None
    registered_country_name: str | None
    subdivision_iso_codes: tuple[str, ...]
    subdivision_names: tuple[str, ...]
    city_geoname_id: int | None
    city_name: str | None
    latitude: float | None
    longitude: float | None
    accuracy_radius_km: int | None
    timezone: str | None
    asn: int | None
    asn_organization: str | None
    city_network: str | None
    asn_network: str | None
    city_db_build_epoch: datetime
    asn_db_build_epoch: datetime
    enriched_at: datetime

    def clickhouse_values(self) -> tuple[Any, ...]:
        return (
            self.bucket,
            self.ip,
            self.ip_version,
            self.ip_scope,
            self.city_lookup_status,
            self.asn_lookup_status,
            self.continent_code,
            self.continent_name,
            self.country_iso_code,
            self.country_name,
            self.country_geoname_id,
            self.registered_country_iso_code,
            self.registered_country_name,
            list(self.subdivision_iso_codes),
            list(self.subdivision_names),
            self.city_geoname_id,
            self.city_name,
            self.latitude,
            self.longitude,
            self.accuracy_radius_km,
            self.timezone,
            self.asn,
            self.asn_organization,
            self.city_network,
            self.asn_network,
            self.city_db_build_epoch,
            self.asn_db_build_epoch,
            self.enriched_at,
        )


def classify_ip_scope(address: IPAddress) -> str:
    if address.is_loopback:
        return "loopback"
    if address.is_link_local:
        return "link_local"
    if address.is_multicast:
        return "multicast"
    if address.is_unspecified:
        return "unspecified"
    if _is_in_networks(address, _cgnat_networks()):
        return "cgnat"
    if _is_in_networks(address, _documentation_networks()):
        return "documentation"
    if address.is_private:
        return "private"
    if address.is_reserved:
        return "reserved"
    if address.is_global:
        return "global"
    return "non_global"


def build_geoip_enrichment(
    *,
    bucket: int,
    address: IPAddress,
    city_lookup: MaxMindLookup | None,
    asn_lookup: MaxMindLookup | None,
    city_build_epoch: datetime,
    asn_build_epoch: datetime,
    enriched_at: datetime,
) -> GeoIPEnrichment:
    ip_scope = classify_ip_scope(address)
    city_record = city_lookup.record if city_lookup is not None else None
    asn_record = asn_lookup.record if asn_lookup is not None else None
    continent = _section(city_record, "continent")
    country = _section(city_record, "country")
    registered_country = _section(city_record, "registered_country")
    city = _section(city_record, "city")
    location = _section(city_record, "location")
    subdivisions = _sections(city_record, "subdivisions")

    return GeoIPEnrichment(
        bucket=bucket,
        ip=str(address),
        ip_version=address.version,
        ip_scope=ip_scope,
        city_lookup_status=_lookup_status(ip_scope, city_lookup),
        asn_lookup_status=_lookup_status(ip_scope, asn_lookup),
        continent_code=_optional_string(continent.get("code")),
        continent_name=_english_name(continent),
        country_iso_code=_optional_string(country.get("iso_code")),
        country_name=_english_name(country),
        country_geoname_id=_optional_int(country.get("geoname_id")),
        registered_country_iso_code=_optional_string(
            registered_country.get("iso_code")
        ),
        registered_country_name=_english_name(registered_country),
        subdivision_iso_codes=tuple(
            _optional_string(subdivision.get("iso_code")) or ""
            for subdivision in subdivisions
        ),
        subdivision_names=tuple(
            _english_name(subdivision) or "" for subdivision in subdivisions
        ),
        city_geoname_id=_optional_int(city.get("geoname_id")),
        city_name=_english_name(city),
        latitude=_optional_float(location.get("latitude")),
        longitude=_optional_float(location.get("longitude")),
        accuracy_radius_km=_optional_int(location.get("accuracy_radius")),
        timezone=_optional_string(location.get("time_zone")),
        asn=_optional_int(
            asn_record.get("autonomous_system_number")
            if asn_record is not None
            else None
        ),
        asn_organization=_optional_string(
            asn_record.get("autonomous_system_organization")
            if asn_record is not None
            else None
        ),
        city_network=_matched_network(address, city_lookup),
        asn_network=_matched_network(address, asn_lookup),
        city_db_build_epoch=city_build_epoch,
        asn_db_build_epoch=asn_build_epoch,
        enriched_at=enriched_at,
    )


def lookup_maxmind_record(reader: Any, address: IPAddress) -> MaxMindLookup:
    record, prefix_length = reader.get_with_prefix_len(address)
    return MaxMindLookup(record=record, prefix_length=int(prefix_length))


def _lookup_status(ip_scope: str, lookup: MaxMindLookup | None) -> str:
    if ip_scope != "global":
        return "not_global"
    if lookup is None or lookup.record is None:
        return "not_found"
    return "found"


def _matched_network(address: IPAddress, lookup: MaxMindLookup | None) -> str | None:
    if lookup is None or lookup.record is None:
        return None
    prefix_length = lookup.prefix_length
    if address.version == 4 and prefix_length > address.max_prefixlen:
        prefix_length -= 96
    return str(ip_network(f"{address}/{prefix_length}", strict=False))


def _section(record: MaxMindRecord | None, key: str) -> MaxMindRecord:
    if record is None:
        return {}
    value = record.get(key)
    if isinstance(value, Mapping):
        return value
    return {}


def _sections(record: MaxMindRecord | None, key: str) -> tuple[MaxMindRecord, ...]:
    if record is None:
        return ()
    value = record.get(key)
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _english_name(section: MaxMindRecord) -> str | None:
    names = section.get("names")
    if not isinstance(names, Mapping):
        return None
    return _optional_string(names.get("en"))


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text != "" else None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _is_in_networks(address: IPAddress, networks: tuple[IPNetwork, ...]) -> bool:
    return any(
        address.version == network.version and address in network
        for network in networks
    )


@cache
def _cgnat_networks() -> tuple[IPNetwork, ...]:
    return (ip_network("100.64.0.0/10"),)


@cache
def _documentation_networks() -> tuple[IPNetwork, ...]:
    return (
        ip_network("192.0.2.0/24"),
        ip_network("198.51.100.0/24"),
        ip_network("203.0.113.0/24"),
        ip_network("2001:db8::/32"),
    )
