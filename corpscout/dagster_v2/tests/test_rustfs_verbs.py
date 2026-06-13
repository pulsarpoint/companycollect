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


@mock_aws
def test_list_keys_paginates_beyond_one_page():
    resource = RustFSResource(endpoint_url="", access_key="test", secret_key="test")
    resource.ensure_bucket("source-test")
    for index in range(1001):
        resource.put_bytes("source-test", f"companies/{index:04d}.xml", b"<xbrl />")

    keys = resource.list_keys("source-test", "companies/")

    assert len(keys) == 1001


@mock_aws
def test_object_exists_distinguishes_present_and_absent_keys():
    resource = RustFSResource(endpoint_url="", access_key="test", secret_key="test")
    resource.ensure_bucket("source-test")
    resource.put_bytes("source-test", "companies/0176460-0/2024-09-30.xml", b"<xbrl />")

    assert resource.object_exists("source-test", "companies/0176460-0/2024-09-30.xml") is True
    assert resource.object_exists("source-test", "companies/9999999-9/2024-09-30.xml") is False
