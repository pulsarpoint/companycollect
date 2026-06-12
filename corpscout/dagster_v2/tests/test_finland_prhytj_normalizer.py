import uuid
from datetime import datetime, timezone

from dagster_corpscout.sources.finland.prh_ytj.normalizer import (
    ImportRun,
    normalize_record,
    source_item_hash,
)
from dagster_corpscout.sources.finland.prh_ytj.parser import ParsedRecord


def make_run() -> ImportRun:
    return ImportRun(
        run_id="20260611T100000Z-abc12345",
        source_export_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
        ingested_at=datetime(2026, 6, 11, 10, 0, tzinfo=timezone.utc),
    )


def test_normalize_record_emits_lineage_status_identifier_and_name():
    parsed = ParsedRecord(
        line_number=7,
        payload_hash="payload-hash",
        payload={
            "businessId": {
                "type": "businessId",
                "value": "1234567-8",
                "registrationDate": "2020-01-01",
            },
            "names": [
                {
                    "name": "Example Oy",
                    "type": "1",
                    "version": 1,
                    "registrationDate": "2020-01-01",
                }
            ],
            "tradeRegisterStatus": "1",
            "status": "2",
            "registrationDate": "2020-01-01",
            "lastModified": "2026-06-01T00:00:00Z",
        },
    )

    rows = normalize_record(make_run(), parsed)

    assert rows["fi_prhytj_identifiers"][0]["business_id"] == "1234567-8"
    assert rows["fi_prhytj_identifiers"][0]["identifier_scope"] == "business_id"
    assert rows["fi_prhytj_identifiers"][0]["source_slug"] == "prhytj"
    assert rows["fi_prhytj_statuses"][0]["source_line_number"] == 7
    assert rows["fi_prhytj_statuses"][0]["source_payload_hash"] == "payload-hash"
    assert rows["fi_prhytj_statuses"][0]["lifecycle_status"] == "active"
    assert rows["fi_prhytj_statuses"][0]["is_active"] is True
    assert rows["fi_prhytj_names"][0]["is_primary"] is True


def test_normalize_record_marks_ceased_when_end_date_or_trade_register_status_3():
    parsed = ParsedRecord(
        line_number=1,
        payload_hash="payload-hash",
        payload={
            "businessId": {"value": "1234567-8"},
            "tradeRegisterStatus": "3",
            "endDate": "2025-12-31",
        },
    )

    rows = normalize_record(make_run(), parsed)

    assert rows["fi_prhytj_statuses"][0]["lifecycle_status"] == "ceased"
    assert rows["fi_prhytj_statuses"][0]["is_active"] is False


def test_normalize_record_normalizes_website_host_and_path():
    parsed = ParsedRecord(
        line_number=1,
        payload_hash="payload-hash",
        payload={
            "businessId": {"value": "1234567-8"},
            "website": {"url": "example.fi/path", "registrationDate": "2020-01-01"},
        },
    )

    rows = normalize_record(make_run(), parsed)
    website = rows["fi_prhytj_websites"][0]

    assert website["normalized_url"] == "https://example.fi/path"
    assert website["host"] == "example.fi"
    assert website["path"] == "/path"
    assert website["is_current"] is True


def test_normalize_record_links_child_descriptions_to_parent_hashes():
    parsed = ParsedRecord(
        line_number=1,
        payload_hash="payload-hash",
        payload={
            "businessId": {"value": "1234567-8"},
            "mainBusinessLine": {
                "type": "62010",
                "typeCodeSet": "TOL2008",
                "registrationDate": "2020-01-01",
                "descriptions": [{"languageCode": "en", "description": "Programming"}],
            },
        },
    )

    rows = normalize_record(make_run(), parsed)
    business_line = rows["fi_prhytj_business_lines"][0]
    description = rows["fi_prhytj_business_line_descriptions"][0]

    assert business_line["source_item_hash"] == source_item_hash(
        "20260611T100000Z-abc12345",
        "1234567-8",
        "mainBusinessLine",
        "62010",
        "TOL2008",
        "2020-01-01",
    )
    assert description["business_line_item_hash"] == business_line["source_item_hash"]
