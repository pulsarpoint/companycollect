from moto import mock_aws

from dagster_corpscout.resources.rustfs import RustFSResource


@mock_aws
def test_ensure_bucket_is_idempotent_and_list_get_roundtrip():
    resource = RustFSResource(endpoint_url="", access_key="test", secret_key="test")

    resource.ensure_bucket("source-test")
    resource.ensure_bucket("source-test")
    resource.put_bytes("source-test", "companies/0176460-0/2024-09-30.xml", b"<xbrl />")

    assert resource.list_keys("source-test", "companies/") == [
        "companies/0176460-0/2024-09-30.xml"
    ]
    assert resource.get_bytes("source-test", "companies/0176460-0/2024-09-30.xml") == b"<xbrl />"
