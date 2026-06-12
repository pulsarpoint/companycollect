"""S3-compatible object storage resource for RustFS."""

import hashlib
import json
from collections.abc import Iterator

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
from dagster import ConfigurableResource

from dagster_corpscout.lib.streaming import IterableReader, StreamStats, observe_chunks

_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=1,
    multipart_chunksize=64 * 1024 * 1024,
)

# Required for RustFS/MinIO. Mirrors UsePathStyle in the Go scheduler S3 client.
_S3_CONFIG = Config(s3={"addressing_style": "path"})


class RustFSResource(ConfigurableResource):
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"

    def client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url or None,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            config=_S3_CONFIG,
        )

    def upload_stream(self, bucket: str, key: str, chunks: Iterator[bytes]) -> StreamStats:
        stats = StreamStats()
        reader = IterableReader(observe_chunks(chunks, stats))
        self.client().upload_fileobj(reader, bucket, key, Config=_TRANSFER_CONFIG)
        return stats

    def put_bytes(self, bucket: str, key: str, body: bytes) -> str:
        self.client().put_object(Bucket=bucket, Key=key, Body=body)
        return hashlib.sha256(body).hexdigest()

    def put_json(self, bucket: str, key: str, payload: dict) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.client().put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )

    def get_json(self, bucket: str, key: str) -> dict:
        response = self.client().get_object(Bucket=bucket, Key=key)
        with response["Body"] as body:
            return json.loads(body.read().decode("utf-8"))

    def open_object(self, bucket: str, key: str):
        return self.client().get_object(Bucket=bucket, Key=key)["Body"]

    def latest_manifest(self, bucket: str, prefix: str = "runs/") -> dict:
        paginator = self.client().get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                if key.endswith("/manifest.json"):
                    keys.append(key)
        if not keys:
            raise FileNotFoundError(f"no manifests found in s3://{bucket}/{prefix}")
        return self.get_json(bucket, sorted(keys)[-1])

    def ensure_bucket(self, bucket: str) -> None:
        try:
            kwargs: dict = {"Bucket": bucket}
            if self.region != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {"LocationConstraint": self.region}
            self.client().create_bucket(**kwargs)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise

    def list_keys(self, bucket: str, prefix: str) -> list[str]:
        paginator = self.client().get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(item["Key"] for item in page.get("Contents", []))
        return keys

    def get_bytes(self, bucket: str, key: str) -> bytes:
        response = self.client().get_object(Bucket=bucket, Key=key)
        with response["Body"] as body:
            return body.read()
