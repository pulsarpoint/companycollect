"""Upload immutable, complete research JSON; verify bytes before issuing a receipt."""

import argparse
import hashlib
import json
import os
import re
from pathlib import Path

import boto3
import botocore.config
import botocore.exceptions
from dotenv import dotenv_values

from company_research.storage import utc_now, write_json


def s3_client(environment: dict[str, str]):
    return boto3.client(
        "s3",
        endpoint_url=environment["CORPSCOUT_S3_ENDPOINT"],
        aws_access_key_id=environment["CORPSCOUT_S3_ACCESS_KEY"],
        aws_secret_access_key=environment["CORPSCOUT_S3_SECRET_KEY"],
        region_name=environment.get("CORPSCOUT_S3_REGION", "us-east-1"),
        config=botocore.config.Config(
            s3={"addressing_style": "path"},
            connect_timeout=10,
            read_timeout=30,
            retries={"mode": "standard", "total_max_attempts": 3},
        ),
    )


def upload_result(path: Path, client, bucket: str) -> dict:
    payload = path.read_bytes()
    document = json.loads(payload)
    if document.get("schema_version") != "company-research-result/1.0":
        raise ValueError("Unsupported complete research result schema")
    run_id, revision_id = document["run_id"], document["revision_id"]
    if any(
        not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", value)
        for value in (run_id, revision_id)
    ):
        raise ValueError("Unsafe run or revision ID")
    digest = hashlib.sha256(payload).hexdigest()
    key = f"company-research/{run_id}/{revision_id}/result.json"
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=payload,
            ContentType="application/json",
            Metadata={"sha256": digest},
            IfNoneMatch="*",
        )
        disposition = "created"
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
            raise
        disposition = "already_present"
    response = client.get_object(Bucket=bucket, Key=key)
    with response["Body"] as body:
        stored = body.read()
    if hashlib.sha256(stored).hexdigest() != digest:
        raise ValueError(
            "Immutable result collision or read-back checksum mismatch; use a new revision"
        )
    receipt = {
        "status": "verified",
        "disposition": disposition,
        "bucket": bucket,
        "key": key,
        "uri": f"s3://{bucket}/{key}",
        "sha256": digest,
        "bytes": len(payload),
        "verified_at": utc_now(),
    }
    write_json(path.with_name("upload-receipt.json"), receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", type=Path)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()
    environment = {
        key: value
        for key, value in dotenv_values(args.env_file).items()
        if value is not None
    } | dict(os.environ)
    print(
        json.dumps(
            upload_result(args.result, s3_client(environment), args.bucket), indent=2
        )
    )


if __name__ == "__main__":
    main()
