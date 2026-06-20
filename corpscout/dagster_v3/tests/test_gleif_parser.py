import json
import zipfile
from io import BytesIO

from dagster_v3.defs.gleif import parser


def _zip_json(member_name: str, payload: dict) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, json.dumps(payload))
    return buffer.getvalue()


def test_parse_lei_records_zip_normalizes_entity_rows() -> None:
    payload = {
        "records": [
            {
                "LEI": "HWUPKR0MPOU8FGXBT394",
                "Entity": {
                    "LegalName": {"$": "Apple Inc.", "@lang": "en"},
                    "OtherEntityNames": {
                        "OtherEntityName": [
                            {
                                "$": "Apple Computer",
                                "@lang": "en",
                                "@type": "PREVIOUS_LEGAL_NAME",
                            }
                        ]
                    },
                    "EntityStatus": "ACTIVE",
                    "LegalJurisdiction": "US-CA",
                    "EntityCategory": "GENERAL",
                    "LegalForm": {"EntityLegalFormCode": "XTIQ"},
                    "LegalAddress": {
                        "@lang": "en",
                        "FirstAddressLine": "One Apple Park Way",
                        "City": "Cupertino",
                        "Region": "US-CA",
                        "Country": "US",
                        "PostalCode": "95014",
                    },
                    "HeadquartersAddress": {
                        "@lang": "en",
                        "FirstAddressLine": "One Apple Park Way",
                        "City": "Cupertino",
                        "Region": "US-CA",
                        "Country": "US",
                        "PostalCode": "95014",
                    },
                },
                "Registration": {
                    "RegistrationStatus": "ISSUED",
                    "InitialRegistrationDate": "2012-06-06T15:53:00Z",
                    "LastUpdateDate": "2026-06-20T08:00:00Z",
                    "ManagingLOU": "EVK05KS7XY1DEII3R011",
                },
                "BIC": ["APLEUS66XXX"],
            }
        ]
    }

    rows = parser.parse_lei_records_zip(
        _zip_json("lei2.json", payload),
        source_run_id="run-1",
        retrieved_at="2026-06-20T17:00:00+00:00",
        resolved_at="2026-06-20T17:01:00+00:00",
        golden_copy_publish_date="2026-06-20T16:00:00+00:00",
    )

    assert rows.lei_records[0]["lei"] == "HWUPKR0MPOU8FGXBT394"
    assert rows.lei_records[0]["legal_name"] == "Apple Inc."
    assert rows.lei_records[0]["primary_country_iso2"] == "US"
    assert rows.lei_names[0]["name"] == "Apple Computer"
    assert rows.lei_addresses[0]["address_role"] == "legal"
    assert rows.lei_addresses[1]["address_role"] == "headquarters"
    assert rows.lei_identifiers[0]["identifier_type"] == "BIC"
    assert rows.lei_identifiers[0]["identifier_value"] == "APLEUS66XXX"


def test_parse_relationship_records_zip_normalizes_relationship_rows() -> None:
    payload = {
        "records": [
            {
                "RelationshipRecord": {
                    "Relationship": {
                        "StartNode": {"NodeID": "CHILDLEI12345678901", "NodeIDType": "LEI"},
                        "EndNode": {"NodeID": "PARENTLEI1234567890", "NodeIDType": "LEI"},
                        "RelationshipType": "IS_DIRECTLY_CONSOLIDATED_BY",
                        "RelationshipStatus": "ACTIVE",
                        "RelationshipPeriods": {
                            "RelationshipPeriod": [
                                {
                                    "StartDate": "2025-01-01T00:00:00Z",
                                    "EndDate": "2025-12-31T00:00:00Z",
                                    "PeriodType": "ACCOUNTING_PERIOD",
                                }
                            ]
                        },
                    },
                    "Registration": {"RegistrationStatus": "PUBLISHED"},
                }
            }
        ]
    }

    rows = parser.parse_relationships_zip(
        _zip_json("rr.json", payload),
        source_run_id="run-1",
        retrieved_at="2026-06-20T17:00:00+00:00",
        resolved_at="2026-06-20T17:01:00+00:00",
    )

    assert rows.relationships[0]["relationship_type"] == "IS_DIRECTLY_CONSOLIDATED_BY"
    assert rows.relationships[0]["relationship_record_id"].startswith(
        "CHILDLEI12345678901:"
    )
    assert rows.relationship_periods[0]["period_type"] == "ACCOUNTING_PERIOD"


def test_parse_reporting_exceptions_zip_normalizes_exception_rows() -> None:
    payload = {
        "records": [
            {
                "Exception": {
                    "LEI": "CHILDLEI12345678901",
                    "ExceptionCategory": "NO_KNOWN_PERSON",
                    "ExceptionReason": "NO_LEI",
                    "ParentRelationshipType": "IS_DIRECTLY_CONSOLIDATED_BY",
                },
                "Registration": {"RegistrationStatus": "PUBLISHED"},
            }
        ]
    }

    rows = parser.parse_reporting_exceptions_zip(
        _zip_json("repex.json", payload),
        source_run_id="run-1",
        retrieved_at="2026-06-20T17:00:00+00:00",
        resolved_at="2026-06-20T17:01:00+00:00",
    )

    assert rows.reporting_exceptions[0]["lei"] == "CHILDLEI12345678901"
    assert rows.reporting_exceptions[0]["exception_category"] == "NO_KNOWN_PERSON"
