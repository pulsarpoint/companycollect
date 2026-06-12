from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.partitions import registration_month_partitions


def test_object_keys_are_deterministic_and_readable():
    assert spec.document_object_key("0176460-0", "2024-09-30") == "companies/0176460-0/2024-09-30.xml"
    assert spec.window_listing_object_key("2025-01-01") == "windows/2025-01-01/listing.json"


def test_partitions_are_monthly_registration_windows():
    assert registration_month_partitions.start.strftime("%Y-%m-%d") == "2025-01-01"
