import hashlib
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from pydantic import BaseModel


@dataclass(frozen=True, slots=True)
class S3Location:
    """Validated bucket and optional object prefix."""

    bucket: str
    key: str

    @property
    def uri(self) -> str:
        suffix = f"/{self.key}" if self.key else ""
        return f"s3://{self.bucket}{suffix}"


@dataclass(frozen=True, slots=True)
class StoredObject:
    """Content identity returned after one successful object write."""

    location: S3Location
    sha256: str
    size_bytes: int


class RustfsStore:
    """Small synchronous RustFS client used at explicit thread boundaries."""

    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region_name: str,
        base_location: S3Location,
        client: Any | None = None,
    ) -> None:
        self.base_location = base_location
        self._client = client or boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name,
            config=Config(s3={"addressing_style": "path"}),
        )

    def ensure_bucket(self) -> None:
        try:
            self._client.create_bucket(Bucket=self.base_location.bucket)
        except Exception as error:
            if _error_code(error) not in {
                "BucketAlreadyExists",
                "BucketAlreadyOwnedByYou",
            }:
                raise

    def read_bytes(self, location: S3Location) -> bytes:
        self._validate_location(location)
        response = self._client.get_object(Bucket=location.bucket, Key=location.key)
        return response["Body"].read()

    def write_json(
        self,
        location: S3Location,
        document: BaseModel | dict[str, object],
    ) -> StoredObject:
        self._validate_location(location)
        value = (
            document.model_dump(mode="json")
            if isinstance(document, BaseModel)
            else document
        )
        body = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        self._client.put_object(
            Bucket=location.bucket,
            Key=location.key,
            Body=body,
            ContentType="application/json",
        )
        return StoredObject(
            location=location,
            sha256=hashlib.sha256(body).hexdigest(),
            size_bytes=len(body),
        )

    def exists(self, location: S3Location) -> bool:
        self._validate_location(location)
        try:
            self._client.head_object(Bucket=location.bucket, Key=location.key)
            return True
        except Exception as error:
            if _error_code(error) in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def list_keys(self, prefix: S3Location) -> tuple[str, ...]:
        self._validate_location(prefix)
        paginator = self._client.get_paginator("list_objects_v2")
        return tuple(
            item["Key"]
            for page in paginator.paginate(
                Bucket=prefix.bucket,
                Prefix=prefix.key,
            )
            for item in page.get("Contents", [])
        )

    def child(self, *parts: str) -> S3Location:
        key = "/".join(
            part.strip("/")
            for part in (self.base_location.key, *parts)
            if part.strip("/")
        )
        return S3Location(bucket=self.base_location.bucket, key=key)

    def parse_allowed_uri(self, uri: str) -> S3Location:
        location = parse_s3_uri(uri)
        self._validate_location(location)
        return location

    def _validate_location(self, location: S3Location) -> None:
        if location.bucket != self.base_location.bucket:
            raise ValueError("S3 object must use the configured Webtech bucket")
        base_prefix = self.base_location.key.rstrip("/")
        if base_prefix and not (
            location.key == base_prefix
            or location.key.startswith(f"{base_prefix}/")
        ):
            raise ValueError("S3 object is outside WEBTECH_S3_PATH")


def parse_s3_uri(value: str) -> S3Location:
    """Parse an S3 URI without accepting ambiguous path segments."""
    parsed = urlsplit(value.strip())
    if parsed.scheme != "s3" or parsed.netloc == "":
        raise ValueError("S3 location must use s3://bucket/prefix")
    if parsed.query or parsed.fragment:
        raise ValueError("S3 location must not contain a query or fragment")
    key = parsed.path.strip("/")
    if any(part in {"", ".", ".."} for part in key.split("/")):
        raise ValueError("S3 location contains an invalid path segment")
    return S3Location(bucket=parsed.netloc, key=key)


def _error_code(error: Exception) -> str:
    response = getattr(error, "response", {})
    detail = response.get("Error", {}) if isinstance(response, dict) else {}
    return str(detail.get("Code", ""))
