from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from time import monotonic
from typing import Any, Protocol

import boto3
import dagster as dg
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from pydantic import Field, PrivateAttr


_LOGGER = logging.getLogger(__name__)


class S3Client(Protocol):
    def create_bucket(self, Bucket: str) -> Any: ...

    def head_object(self, Bucket: str, Key: str) -> Any: ...

    def put_object(self, Bucket: str, Key: str, Body: Any) -> Any: ...

    def upload_file(
        self,
        Filename: str,
        Bucket: str,
        Key: str,
        Config: TransferConfig | None = None,
    ) -> Any: ...

    def get_object(self, Bucket: str, Key: str) -> Mapping[str, Any]: ...

    def download_file(self, Bucket: str, Key: str, Filename: str) -> Any: ...

    def get_paginator(self, operation_name: str) -> Any: ...

    def put_bucket_lifecycle_configuration(
        self, Bucket: str, LifecycleConfiguration: Mapping[str, Any]
    ) -> Any: ...

    def delete_objects(self, Bucket: str, Delete: Mapping[str, Any]) -> Any: ...


class ObjectStoreListingLimitError(RuntimeError):
    def __init__(
        self,
        *,
        reason: str,
        bucket: str,
        prefix: str,
        page_count: int,
        key_count: int,
        elapsed_seconds: float,
        limit: int | float,
    ) -> None:
        limit_text = f"{limit:.3f}" if isinstance(limit, float) else str(limit)
        super().__init__(
            f"Object-store listing {reason}: bucket={bucket} prefix={prefix!r} "
            f"pages={page_count} keys={key_count} "
            f"elapsed_seconds={elapsed_seconds:.3f} limit={limit_text}"
        )
        self.reason = reason
        self.bucket = bucket
        self.prefix = prefix
        self.page_count = page_count
        self.key_count = key_count
        self.elapsed_seconds = elapsed_seconds
        self.limit = limit


class ObjectStoreResource(dg.ConfigurableResource):
    bucket: str = "source-finland-prhytj"
    endpoint_url: str = dg.EnvVar("CORPSCOUT_S3_ENDPOINT")
    access_key: str = dg.EnvVar("CORPSCOUT_S3_ACCESS_KEY")
    secret_key: str = dg.EnvVar("CORPSCOUT_S3_SECRET_KEY")
    region_name: str = "us-east-1"
    multipart_max_concurrency: int = Field(default=2, ge=1)
    s3_connect_timeout_seconds: float = Field(default=5.0, gt=0)
    s3_read_timeout_seconds: float = Field(default=30.0, gt=0)
    s3_max_attempts: int = Field(default=2, ge=1)
    list_max_keys: int = Field(default=50_000, ge=1)
    list_max_pages: int = Field(default=50, ge=1)
    list_max_elapsed_seconds: float = Field(default=60.0, gt=0)
    list_warn_seconds: float = Field(default=2.0, ge=0)

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
                config=Config(
                    connect_timeout=self.s3_connect_timeout_seconds,
                    read_timeout=self.s3_read_timeout_seconds,
                    retries={
                        "mode": "standard",
                        "total_max_attempts": self.s3_max_attempts,
                    },
                    s3={"addressing_style": "path"},
                ),
            )
        return self._s3_client

    def ensure_bucket(self, bucket: str | None = None) -> None:
        target_bucket = bucket or self.bucket
        try:
            self.client().create_bucket(Bucket=target_bucket)
        except Exception as exc:
            if _error_code(exc) not in {
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            }:
                raise

    def apply_lifecycle_rules(
        self,
        rules: Sequence[Mapping[str, Any]],
        bucket: str | None = None,
    ) -> None:
        """Replace the bucket's lifecycle configuration. Idempotent.

        Retention policy belongs in version control: applied by hand it would
        live nowhere, not survive a bucket being recreated, and appear in no
        review.
        """
        target_bucket = bucket or self.bucket
        self.client().put_bucket_lifecycle_configuration(
            Bucket=target_bucket,
            LifecycleConfiguration={"Rules": list(rules)},
        )

    def exists(self, key: str, bucket: str | None = None) -> bool:
        target_bucket = bucket or self.bucket
        try:
            self.client().head_object(Bucket=target_bucket, Key=key)
            return True
        except Exception as exc:
            if _error_code(exc) in {"404", "NoSuchBucket", "NoSuchKey", "NotFound"}:
                return False
            raise

    def object_size(self, key: str, bucket: str | None = None) -> int:
        target_bucket = bucket or self.bucket
        response = self.client().head_object(Bucket=target_bucket, Key=key)
        size_bytes = response.get("ContentLength")
        if (
            not isinstance(size_bytes, int)
            or isinstance(size_bytes, bool)
            or size_bytes < 0
        ):
            raise ValueError(
                "Object-store HEAD response has invalid content length: "
                f"bucket={target_bucket} key={key} content_length={size_bytes!r}"
            )
        return size_bytes

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        target_bucket = bucket or self.bucket
        self.client().put_object(Bucket=target_bucket, Key=key, Body=body)

    def write_json(self, key: str, body: str, bucket: str | None = None) -> None:
        target_bucket = bucket or self.bucket
        self.client().put_object(Bucket=target_bucket, Key=key, Body=body)

    def upload_file(
        self,
        key: str,
        source_path: str | Path,
        bucket: str | None = None,
        *,
        transfer_config: TransferConfig | None = None,
    ) -> None:
        target_bucket = bucket or self.bucket
        if transfer_config is None:
            transfer_config = TransferConfig(
                max_concurrency=self.multipart_max_concurrency
            )
        self.client().upload_file(
            str(source_path),
            target_bucket,
            key,
            Config=transfer_config,
        )

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        target_bucket = bucket or self.bucket
        return self.client().get_object(Bucket=target_bucket, Key=key)["Body"].read()

    def download_file(
        self, key: str, target_path: str | Path, bucket: str | None = None
    ) -> None:
        target_bucket = bucket or self.bucket
        self.client().download_file(target_bucket, key, str(target_path))

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        if prefix.strip() == "":
            raise ValueError("Object-store listing prefix must not be blank")

        target_bucket = bucket or self.bucket
        paginator = self.client().get_paginator("list_objects_v2")
        started_at = monotonic()
        keys: list[str] = []
        page_count = 0
        observed_key_count = 0
        limit_exceeded = False
        try:
            for page in paginator.paginate(Bucket=target_bucket, Prefix=prefix):
                page_count += 1
                page_keys = [item["Key"] for item in page.get("Contents", [])]
                observed_key_count = len(keys) + len(page_keys)
                elapsed_seconds = monotonic() - started_at

                if observed_key_count > self.list_max_keys:
                    limit_exceeded = True
                    raise ObjectStoreListingLimitError(
                        reason="key limit exceeded",
                        bucket=target_bucket,
                        prefix=prefix,
                        page_count=page_count,
                        key_count=observed_key_count,
                        elapsed_seconds=elapsed_seconds,
                        limit=self.list_max_keys,
                    )

                keys.extend(page_keys)
                if page_count >= self.list_max_pages and bool(
                    page.get("IsTruncated", False)
                ):
                    limit_exceeded = True
                    raise ObjectStoreListingLimitError(
                        reason="page limit reached",
                        bucket=target_bucket,
                        prefix=prefix,
                        page_count=page_count,
                        key_count=observed_key_count,
                        elapsed_seconds=elapsed_seconds,
                        limit=self.list_max_pages,
                    )

                if elapsed_seconds > self.list_max_elapsed_seconds:
                    limit_exceeded = True
                    raise ObjectStoreListingLimitError(
                        reason="elapsed-time limit exceeded",
                        bucket=target_bucket,
                        prefix=prefix,
                        page_count=page_count,
                        key_count=observed_key_count,
                        elapsed_seconds=elapsed_seconds,
                        limit=self.list_max_elapsed_seconds,
                    )
            return keys
        finally:
            elapsed_seconds = monotonic() - started_at
            if limit_exceeded or elapsed_seconds >= self.list_warn_seconds:
                message = (
                    "Object-store listing guardrail triggered"
                    if limit_exceeded
                    else "Slow object-store listing"
                )
                _LOGGER.warning(
                    "%s bucket=%s prefix=%r pages=%s keys=%s "
                    "elapsed_seconds=%.3f limit_exceeded=%s",
                    message,
                    target_bucket,
                    prefix,
                    page_count,
                    observed_key_count,
                    elapsed_seconds,
                    limit_exceeded,
                    extra={
                        "object_store_bucket": target_bucket,
                        "object_store_prefix": prefix,
                        "object_store_page_count": page_count,
                        "object_store_key_count": observed_key_count,
                        "object_store_elapsed_seconds": elapsed_seconds,
                        "object_store_limit_exceeded": limit_exceeded,
                    },
                )

    def delete_keys(
        self, keys: list[str] | tuple[str, ...], bucket: str | None = None
    ) -> int:
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
