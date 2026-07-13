"""Publish one immutable DuckDB catalog to RustFS."""

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

import boto3
from botocore.client import BaseClient
from botocore.config import Config


@dataclass(frozen=True, slots=True)
class CatalogDestination:
    bucket: str
    prefix: str
    endpoint: str
    region: str
    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class PublishedObject:
    key: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class CatalogPublication:
    bucket: str
    ready_key: str
    catalog: PublishedObject


def destination_from_environment(
    environment: Mapping[str, str],
) -> CatalogDestination:
    base = environment.get("COMMONCRAWL_CATALOG_S3_BASE", "").strip()
    if not base:
        raise ValueError("COMMONCRAWL_CATALOG_S3_BASE is required")

    parsed = urlsplit(base)
    if parsed.scheme != "s3":
        raise ValueError("COMMONCRAWL_CATALOG_S3_BASE must use s3://")
    if not parsed.netloc:
        raise ValueError("COMMONCRAWL_CATALOG_S3_BASE must contain a bucket")
    if parsed.query or parsed.fragment:
        raise ValueError(
            "COMMONCRAWL_CATALOG_S3_BASE must not contain a query or fragment"
        )
    prefix = parsed.path.strip("/")
    if prefix and any(part in {"", ".", ".."} for part in prefix.split("/")):
        raise ValueError("COMMONCRAWL_CATALOG_S3_BASE contains an invalid prefix")

    required_names = (
        "CORPSCOUT_S3_ENDPOINT",
        "CORPSCOUT_S3_ACCESS_KEY",
        "CORPSCOUT_S3_SECRET_KEY",
    )
    missing = [
        name for name in required_names if not environment.get(name, "").strip()
    ]
    if missing:
        raise ValueError(
            "RustFS catalog publication requires " + ", ".join(missing)
        )
    endpoint = environment["CORPSCOUT_S3_ENDPOINT"].strip()
    parsed_endpoint = urlsplit(endpoint)
    if (
        parsed_endpoint.scheme not in {"http", "https"}
        or not parsed_endpoint.netloc
        or parsed_endpoint.query
        or parsed_endpoint.fragment
    ):
        raise ValueError("CORPSCOUT_S3_ENDPOINT must be an HTTP(S) endpoint URL")
    region = environment.get("CORPSCOUT_S3_REGION", "us-east-1").strip()
    if not region:
        raise ValueError("CORPSCOUT_S3_REGION must not be blank")

    return CatalogDestination(
        bucket=parsed.netloc,
        prefix=prefix,
        endpoint=endpoint,
        region=region,
        access_key=environment["CORPSCOUT_S3_ACCESS_KEY"],
        secret_key=environment["CORPSCOUT_S3_SECRET_KEY"],
    )


def _published_object(path: Path, key: str) -> PublishedObject:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"catalog publication input is not a regular file: {path}")
    size_bytes = path.stat().st_size
    if size_bytes <= 0:
        raise ValueError(f"catalog publication input is empty: {path}")
    with path.open("rb") as source:
        sha256 = hashlib.file_digest(source, "sha256").hexdigest()
    return PublishedObject(key=key, size_bytes=size_bytes, sha256=sha256)


def _verify_object(
    client: BaseClient,
    bucket: str,
    published: PublishedObject,
) -> None:
    head = client.head_object(Bucket=bucket, Key=published.key)
    metadata = head.get("Metadata", {})
    if (
        int(head.get("ContentLength", -1)) != published.size_bytes
        or metadata.get("sha256") != published.sha256
    ):
        raise RuntimeError(f"HEAD verification failed for s3://{bucket}/{published.key}")


def publish_catalog(
    destination: CatalogDestination,
    *,
    crawl: str,
    selection: str,
    catalog_path: Path,
) -> CatalogPublication:
    """Replace one remote catalog, committing readiness only after verified uploads."""
    if not crawl or "/" in crawl or crawl in {".", ".."}:
        raise ValueError("crawl must be one non-empty object-key component")
    if not selection or "/" in selection or selection in {".", ".."}:
        raise ValueError("selection must be one non-empty object-key component")

    catalog_prefix = "/".join(
        part for part in (destination.prefix, crawl, selection) if part
    )
    catalog = _published_object(catalog_path, f"{catalog_prefix}/catalog.duckdb")
    ready_key = f"{catalog_prefix}/ready.json"
    client = boto3.client(
        "s3",
        endpoint_url=destination.endpoint,
        region_name=destination.region,
        aws_access_key_id=destination.access_key,
        aws_secret_access_key=destination.secret_key,
        config=Config(s3={"addressing_style": "path"}),
    )

    client.delete_object(Bucket=destination.bucket, Key=ready_key)
    client.upload_file(
        str(catalog_path),
        destination.bucket,
        catalog.key,
        ExtraArgs={
            "ContentType": "application/octet-stream",
            "Metadata": {"sha256": catalog.sha256},
        },
    )
    _verify_object(client, destination.bucket, catalog)

    ready = {
        "schema_version": 1,
        "crawl_id": crawl,
        "selection": selection,
        "catalog": {
            "key": catalog.key,
            "size_bytes": catalog.size_bytes,
            "sha256": catalog.sha256,
        },
    }
    client.put_object(
        Bucket=destination.bucket,
        Key=ready_key,
        Body=json.dumps(ready, sort_keys=True, separators=(",", ":")).encode(),
        ContentType="application/json",
    )
    return CatalogPublication(
        bucket=destination.bucket,
        ready_key=ready_key,
        catalog=catalog,
    )
