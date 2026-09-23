"""Shared MaxMind helpers used by the generic IP enrichment worker."""

from datetime import UTC, datetime
from ipaddress import ip_address
from pathlib import Path

from dagster_v3.defs.commoncrawl_geoip.maxmind import (
    MaxMindLookup,
    build_geoip_enrichment,
    classify_ip_scope,
)
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource


def test_ip_scope_classification_keeps_non_global_reasons() -> None:
    assert classify_ip_scope(ip_address("8.8.8.8")) == "global"
    assert classify_ip_scope(ip_address("10.0.0.1")) == "private"
    assert classify_ip_scope(ip_address("100.64.0.1")) == "cgnat"
    assert classify_ip_scope(ip_address("127.0.0.1")) == "loopback"
    assert classify_ip_scope(ip_address("169.254.1.1")) == "link_local"
    assert classify_ip_scope(ip_address("192.0.2.1")) == "documentation"
    assert classify_ip_scope(ip_address("224.0.0.1")) == "multicast"


def test_maxmind_records_map_to_clickhouse_enrichment_columns() -> None:
    city_build_epoch = datetime(2026, 7, 10, tzinfo=UTC)
    asn_build_epoch = datetime(2026, 7, 9, tzinfo=UTC)
    enriched_at = datetime(2026, 7, 10, 12, 30, tzinfo=UTC)
    enrichment = build_geoip_enrichment(
        bucket=7,
        address=ip_address("8.8.8.8"),
        city_lookup=MaxMindLookup(
            record={
                "continent": {"code": "NA", "names": {"en": "North America"}},
                "country": {
                    "geoname_id": 6252001,
                    "iso_code": "US",
                    "names": {"en": "United States"},
                },
                "registered_country": {
                    "iso_code": "US",
                    "names": {"en": "United States"},
                },
                "subdivisions": [
                    {
                        "iso_code": "CA",
                        "names": {"en": "California"},
                    }
                ],
                "city": {
                    "geoname_id": 5375480,
                    "names": {"en": "Mountain View"},
                },
                "location": {
                    "latitude": 37.4056,
                    "longitude": -122.0775,
                    "accuracy_radius": 1000,
                    "time_zone": "America/Los_Angeles",
                },
            },
            prefix_length=24,
        ),
        asn_lookup=MaxMindLookup(
            record={
                "autonomous_system_number": 15169,
                "autonomous_system_organization": "Google LLC",
            },
            prefix_length=24,
        ),
        city_build_epoch=city_build_epoch,
        asn_build_epoch=asn_build_epoch,
        enriched_at=enriched_at,
    )

    assert enrichment.bucket == 7
    assert enrichment.ip == "8.8.8.8"
    assert enrichment.ip_version == 4
    assert enrichment.ip_scope == "global"
    assert enrichment.city_lookup_status == "found"
    assert enrichment.asn_lookup_status == "found"
    assert enrichment.continent_code == "NA"
    assert enrichment.country_iso_code == "US"
    assert enrichment.country_geoname_id == 6252001
    assert enrichment.subdivision_iso_codes == ("CA",)
    assert enrichment.subdivision_names == ("California",)
    assert enrichment.city_geoname_id == 5375480
    assert enrichment.city_name == "Mountain View"
    assert enrichment.latitude == 37.4056
    assert enrichment.longitude == -122.0775
    assert enrichment.accuracy_radius_km == 1000
    assert enrichment.timezone == "America/Los_Angeles"
    assert enrichment.asn == 15169
    assert enrichment.asn_organization == "Google LLC"
    assert enrichment.city_network == "8.8.8.0/24"
    assert enrichment.asn_network == "8.8.8.0/24"
    assert enrichment.city_db_build_epoch == city_build_epoch
    assert enrichment.asn_db_build_epoch == asn_build_epoch
    assert enrichment.enriched_at == enriched_at


def test_non_global_ip_does_not_claim_maxmind_matches() -> None:
    build_epoch = datetime(2026, 7, 10, tzinfo=UTC)
    enrichment = build_geoip_enrichment(
        bucket=3,
        address=ip_address("10.0.0.1"),
        city_lookup=None,
        asn_lookup=None,
        city_build_epoch=build_epoch,
        asn_build_epoch=build_epoch,
        enriched_at=build_epoch,
    )

    assert enrichment.ip_scope == "private"
    assert enrichment.city_lookup_status == "not_global"
    assert enrichment.asn_lookup_status == "not_global"
    assert enrichment.latitude is None
    assert enrichment.longitude is None
    assert enrichment.asn is None


def test_maxmind_resource_supports_databases_directly_in_directory(
    tmp_path: Path,
) -> None:
    city = tmp_path / "GeoLite2-City.mmdb"
    asn = tmp_path / "GeoLite2-ASN.mmdb"
    city.touch()
    asn.touch()

    resource = MaxMindDatabaseResource(database_directory=str(tmp_path))

    assert resource.database_paths() == (city, asn)
