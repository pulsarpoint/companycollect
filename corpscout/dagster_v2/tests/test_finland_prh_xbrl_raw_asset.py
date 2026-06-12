import dagster as dg
import responses
from moto import mock_aws

from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets.external import source_system
from dagster_corpscout.sources.finland.prh_xbrl.assets.raw import raw_xml_documents


@mock_aws
def test_raw_xml_documents_downloads_window_and_writes_listing():
    rustfs = RustFSResource(endpoint_url="", access_key="test", secret_key="test")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{spec.BASE_URL}/all_financial_statements",
            json={
                "totalResults": 1,
                "financials": [
                    {
                        "businessId": "0176460-0",
                        "financialDate": "2024-09-30",
                        "registrationDate": "2025-01-23",
                    }
                ],
            },
        )
        rsps.add(responses.GET, f"{spec.BASE_URL}/financial", body=b"<xbrl />")

        result = dg.materialize(
            [source_system, raw_xml_documents],
            selection=[raw_xml_documents],
            partition_key="2025-01-01",
            resources={"rustfs": rustfs},
        )

        assert "registeredDateStart=2025-01-01" in rsps.calls[0].request.url
        assert "registeredDateEnd=2025-01-31" in rsps.calls[0].request.url

    assert result.success
    assert rustfs.get_bytes(spec.BUCKET, "companies/0176460-0/2024-09-30.xml") == b"<xbrl />"
    listing = rustfs.get_json(spec.BUCKET, spec.window_listing_object_key("2025-01-01"))
    assert listing["registered_date_start"] == "2025-01-01"
    assert listing["registered_date_end"] == "2025-01-31"
    [entry] = listing["documents"]
    assert entry["business_id"] == "0176460-0"
    assert entry["object_key"] == "companies/0176460-0/2024-09-30.xml"
    assert entry["registration_date"] == "2025-01-23"
