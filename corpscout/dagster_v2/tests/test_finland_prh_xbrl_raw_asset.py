from types import SimpleNamespace

import dagster as dg
import responses
from moto import mock_aws

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets.external import source_system
from dagster_corpscout.sources.finland.prh_xbrl.assets.raw import raw_xml_documents
from dagster_corpscout.sources.finland.prh_xbrl.eligibility import COMPANY_CACHE_ASSET_KEY

# Stand-in for the prh_ytj serving asset so the dep edge resolves without
# importing the whole prh_ytj asset chain into this test.
company_cache_stub = dg.AssetSpec(key=dg.AssetKey(COMPANY_CACHE_ASSET_KEY))


class _EligibilityClient:
    def __init__(self):
        self.rows = []

    def query(self, _sql):
        return SimpleNamespace(result_rows=list(self.rows))


_eligibility_client = _EligibilityClient()


class FakeClickHouseResource(ClickHouseResource):
    def client(self):
        return _eligibility_client


def _materialize(rustfs, raise_on_error=True):
    return dg.materialize(
        [source_system, company_cache_stub, raw_xml_documents],
        selection=[raw_xml_documents],
        partition_key="2025-01-01",
        resources={
            "rustfs": rustfs,
            "clickhouse": FakeClickHouseResource(host="test", password="test"),
        },
        raise_on_error=raise_on_error,
    )


def _discovery_response(statements):
    return {
        "totalResults": len(statements),
        "financials": [
            {
                "businessId": business_id,
                "financialDate": financial_date,
                "registrationDate": "2025-01-23",
            }
            for business_id, financial_date in statements
        ],
    }


@mock_aws
def test_only_eligible_companies_are_downloaded_and_skips_are_recorded():
    _eligibility_client.rows = [("0176460-0",)]
    rustfs = RustFSResource(endpoint_url="", access_key="test", secret_key="test")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{spec.BASE_URL}/all_financial_statements",
            json=_discovery_response(
                [("0176460-0", "2024-09-30"), ("9999999-9", "2024-12-31")]
            ),
        )
        rsps.add(responses.GET, f"{spec.BASE_URL}/financial", body=b"<xbrl />")

        result = _materialize(rustfs)

        financial_calls = [c for c in rsps.calls if "/financial?" in c.request.url]
        assert len(financial_calls) == 1
        assert "businessId=0176460-0" in financial_calls[0].request.url

    assert result.success
    assert rustfs.get_bytes(spec.BUCKET, "companies/0176460-0/2024-09-30.xml") == b"<xbrl />"
    listing = rustfs.get_json(spec.BUCKET, spec.window_listing_object_key("2025-01-01"))
    [document] = listing["documents"]
    assert document["business_id"] == "0176460-0"
    [skipped] = listing["skipped"]
    assert skipped["business_id"] == "9999999-9"
    assert skipped["financial_date"] == "2024-12-31"
    assert skipped["reason"] == "not_eligible"


@mock_aws
def test_existing_objects_are_reused_without_downloading():
    _eligibility_client.rows = [("0176460-0",)]
    rustfs = RustFSResource(endpoint_url="", access_key="test", secret_key="test")
    rustfs.ensure_bucket(spec.BUCKET)
    rustfs.put_bytes(spec.BUCKET, "companies/0176460-0/2024-09-30.xml", b"<existing />")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{spec.BASE_URL}/all_financial_statements",
            json=_discovery_response([("0176460-0", "2024-09-30")]),
        )

        result = _materialize(rustfs)

    assert result.success
    assert rustfs.get_bytes(spec.BUCKET, "companies/0176460-0/2024-09-30.xml") == b"<existing />"
    listing = rustfs.get_json(spec.BUCKET, spec.window_listing_object_key("2025-01-01"))
    [document] = listing["documents"]
    assert document["object_key"] == "companies/0176460-0/2024-09-30.xml"
    assert document["source_url"].endswith(
        "/financial?businessId=0176460-0&financialDate=2024-09-30"
    )
    assert listing["skipped"] == []


@mock_aws
def test_empty_eligibility_set_fails_the_run_instead_of_skipping_everything():
    import pytest

    _eligibility_client.rows = []
    rustfs = RustFSResource(endpoint_url="", access_key="test", secret_key="test")

    from dagster_corpscout.sources.finland.prh_xbrl.assets.raw import RawPullConfig

    with pytest.raises(dg.Failure, match="eligibility query returned no companies"):
        raw_xml_documents(
            context=dg.build_asset_context(partition_key="2025-01-01"),
            config=RawPullConfig(),
            rustfs=rustfs,
            clickhouse=FakeClickHouseResource(host="test", password="test"),
        )
