import json

import boto3
import responses
from dagster import materialize
from moto import mock_aws

from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland_prhytj import spec
from dagster_corpscout.sources.finland_prhytj.assets import raw_snapshot

BASE = spec.BASE_URL


def register_api_stubs():
    responses.get(
        f"{BASE}?page=1",
        json={"totalResults": 2, "companies": [{"businessId": "111"}, {"businessId": "222"}]},
    )
    for code, lang in spec.CODE_LISTS:
        responses.get(
            f"https://avoindata.prh.fi/opendata-ytj-api/v3/description?code={code}&lang={lang}",
            body=f"1\t{code} entry\n".encode("utf-8"),
        )


@mock_aws
@responses.activate
def test_raw_snapshot_writes_artifacts_and_manifest():
    register_api_stubs()
    rustfs = RustFSResource(endpoint_url="", access_key="t", secret_key="t")
    rustfs.client().create_bucket(Bucket=spec.BUCKET)

    result = materialize([raw_snapshot], resources={"rustfs": rustfs})
    assert result.success

    s3 = boto3.client("s3")
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=spec.BUCKET)["Contents"]]
    run_id = sorted(keys)[0].split("/")[1]
    assert f"runs/{run_id}/source.ndjson" in keys
    assert f"runs/{run_id}/manifest.json" in keys
    for code, lang in spec.CODE_LISTS:
        assert f"runs/{run_id}/codelists/{code}.{lang}.tsv" in keys

    snapshot = s3.get_object(Bucket=spec.BUCKET, Key=f"runs/{run_id}/source.ndjson")[
        "Body"
    ].read()
    assert snapshot == b'{"businessId":"111"}\n{"businessId":"222"}\n'

    manifest = json.loads(
        s3.get_object(Bucket=spec.BUCKET, Key=f"runs/{run_id}/manifest.json")["Body"].read()
    )
    assert manifest["run_id"] == run_id
    assert manifest["source"] == spec.SOURCE_NAME
    assert len(manifest["artifacts"]) == 8
    snap_artifact = manifest["artifacts"][0]
    assert snap_artifact["key"] == "source"
    assert snap_artifact["records_written"] == 2
    assert snap_artifact["content_length_bytes"] == len(snapshot)
