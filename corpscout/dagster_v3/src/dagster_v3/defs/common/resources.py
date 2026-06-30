from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

import boto3
import dagster as dg
from botocore.config import Config
from pydantic import PrivateAttr


class S3Client(Protocol):
    def create_bucket(self, Bucket: str) -> Any:
        ...

    def head_object(self, Bucket: str, Key: str) -> Any:
        ...

    def put_object(self, Bucket: str, Key: str, Body: Any) -> Any:
        ...

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> Any:
        ...

    def get_object(self, Bucket: str, Key: str) -> Mapping[str, Any]:
        ...

    def download_file(self, Bucket: str, Key: str, Filename: str) -> Any:
        ...

    def get_paginator(self, operation_name: str) -> Any:
        ...

    def delete_objects(self, Bucket: str, Delete: Mapping[str, Any]) -> Any:
        ...


class ObjectStoreResource(dg.ConfigurableResource):
    bucket: str = "source-finland-prhytj"
    endpoint_url: str = dg.EnvVar("CORPSCOUT_S3_ENDPOINT")
    access_key: str = dg.EnvVar("CORPSCOUT_S3_ACCESS_KEY")
    secret_key: str = dg.EnvVar("CORPSCOUT_S3_SECRET_KEY")
    region_name: str = "us-east-1"

    _s3_client: S3Client | None = PrivateAttr(default=None)

    def __init__(self, s3_client: S3Client | None = None, **data: Any) -> None:
        super().__init__(**data)
        self._s3_client = s3_client

    def client(self) -> S3Client:
        if self._s3_client is None:
            self._s3_client = boto3.client(
                "s3",
                endpoint_url=_resolve_env_value(self.endpoint_url),
                aws_access_key_id=_resolve_env_value(self.access_key),
                aws_secret_access_key=_resolve_env_value(self.secret_key),
                region_name=self.region_name,
                config=Config(s3={"addressing_style": "path"}),
            )
        return self._s3_client

    def ensure_bucket(self, bucket: str | None = None) -> None:
        target_bucket = bucket or self.bucket
        try:
            self.client().create_bucket(Bucket=target_bucket)
        except Exception as exc:
            if _error_code(exc) not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                raise

    def exists(self, key: str, bucket: str | None = None) -> bool:
        target_bucket = bucket or self.bucket
        try:
            self.client().head_object(Bucket=target_bucket, Key=key)
            return True
        except Exception as exc:
            if _error_code(exc) in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        target_bucket = bucket or self.bucket
        self.client().put_object(Bucket=target_bucket, Key=key, Body=body)

    def write_json(self, key: str, body: str, bucket: str | None = None) -> None:
        target_bucket = bucket or self.bucket
        self.client().put_object(Bucket=target_bucket, Key=key, Body=body)

    def upload_file(self, key: str, source_path: str | Path, bucket: str | None = None) -> None:
        target_bucket = bucket or self.bucket
        self.client().upload_file(str(source_path), target_bucket, key)

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        target_bucket = bucket or self.bucket
        return self.client().get_object(Bucket=target_bucket, Key=key)["Body"].read()

    def download_file(self, key: str, target_path: str | Path, bucket: str | None = None) -> None:
        target_bucket = bucket or self.bucket
        self.client().download_file(target_bucket, key, str(target_path))

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        target_bucket = bucket or self.bucket
        paginator = self.client().get_paginator("list_objects_v2")
        return [
            item["Key"]
            for page in paginator.paginate(Bucket=target_bucket, Prefix=prefix)
            for item in page.get("Contents", [])
        ]

    def delete_keys(self, keys: list[str] | tuple[str, ...], bucket: str | None = None) -> int:
        if not keys:
            return 0

        target_bucket = bucket or self.bucket
        deleted_count = 0
        for offset in range(0, len(keys), 1000):
            key_batch = keys[offset : offset + 1000]
            self.client().delete_objects(
                Bucket=target_bucket,
                Delete={"Objects": [{"Key": key} for key in key_batch]},
            )
            deleted_count += len(key_batch)
        return deleted_count


def _error_code(exc: Exception) -> str:
    response = getattr(exc, "response", {})
    error = response.get("Error", {}) if isinstance(response, dict) else {}
    return str(error.get("Code", ""))


def _resolve_env_value(value: Any) -> Any:
    get_value = getattr(value, "get_value", None)
    if callable(get_value):
        return get_value()
    return value
