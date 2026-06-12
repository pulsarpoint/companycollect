import hashlib
import json

import boto3
from moto import mock_aws

from dagster_corpscout.resources.rustfs import RustFSResource


def make_resource() -> RustFSResource:
    return RustFSResource(endpoint_url="", access_key="test", secret_key="test")


@mock_aws
def test_upload_stream_and_stats():
    resource = make_resource()
    resource.client().create_bucket(Bucket="bkt")

    stats = resource.upload_stream("bkt", "runs/x/source.ndjson", iter([b"line1\n", b"line2\n"]))

    body = boto3.client("s3").get_object(Bucket="bkt", Key="runs/x/source.ndjson")["Body"].read()
    assert body == b"line1\nline2\n"
    assert stats.bytes_read == 12
    assert stats.sha256_hex == hashlib.sha256(b"line1\nline2\n").hexdigest()


@mock_aws
def test_put_bytes_and_put_json():
    resource = make_resource()
    resource.client().create_bucket(Bucket="bkt")

    sha = resource.put_bytes("bkt", "codelists/REK.en.tsv", b"K\tV\n")
    assert sha == hashlib.sha256(b"K\tV\n").hexdigest()

    resource.put_json("bkt", "runs/x/manifest.json", {"run_id": "x"})
    body = boto3.client("s3").get_object(Bucket="bkt", Key="runs/x/manifest.json")["Body"].read()
    assert json.loads(body) == {"run_id": "x"}


@mock_aws
def test_get_json_reads_manifest():
    resource = make_resource()
    resource.client().create_bucket(Bucket="source-finland-prhytj")
    resource.client().put_object(
        Bucket="source-finland-prhytj",
        Key="runs/20260611T100000Z-abc12345/manifest.json",
        Body=b'{"run_id":"20260611T100000Z-abc12345","artifacts":[]}',
    )

    manifest = resource.get_json(
        "source-finland-prhytj",
        "runs/20260611T100000Z-abc12345/manifest.json",
    )

    assert manifest["run_id"] == "20260611T100000Z-abc12345"


@mock_aws
def test_latest_manifest_uses_timestamp_sorted_completed_manifest():
    resource = make_resource()
    resource.client().create_bucket(Bucket="source-finland-prhytj")
    resource.client().put_object(
        Bucket="source-finland-prhytj",
        Key="runs/20260611T100000Z-aaaaaaaa/manifest.json",
        Body=b'{"run_id":"old","artifacts":[]}',
    )
    resource.client().put_object(
        Bucket="source-finland-prhytj",
        Key="runs/20260611T110000Z-bbbbbbbb/manifest.json",
        Body=b'{"run_id":"new","artifacts":[]}',
    )

    manifest = resource.latest_manifest("source-finland-prhytj")

    assert manifest["run_id"] == "new"


@mock_aws
def test_open_object_streams_body():
    resource = make_resource()
    resource.client().create_bucket(Bucket="source-finland-prhytj")
    resource.client().put_object(
        Bucket="source-finland-prhytj",
        Key="runs/x/source.ndjson",
        Body=b'{"businessId":{"value":"1234567-8"}}\n',
    )

    with resource.open_object("source-finland-prhytj", "runs/x/source.ndjson") as stream:
        body = stream.read()

    assert body == b'{"businessId":{"value":"1234567-8"}}\n'
