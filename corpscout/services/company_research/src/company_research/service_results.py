"""Immutable S3 result objects for the crawl completion outbox."""

import gzip
import hashlib
import json
import shutil
import tarfile
from pathlib import Path
from tempfile import TemporaryFile
from typing import BinaryIO
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseModel, ConfigDict, Field, field_validator

from company_research.service import CrawlJob, CrawlRequest


class ResultDeliveryError(Exception):
    """A saved result must remain retryable until its destination is available."""


class S3Settings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    bucket: str = Field(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    prefix: str = Field(default="company-crawls", pattern=r"^[A-Za-z0-9_/-]*$")
    endpoint_url: str | None = None
    region: str = "us-east-1"

    @field_validator("prefix")
    @classmethod
    def normalize_prefix(cls, value: str) -> str:
        return value.strip("/")

    @field_validator("endpoint_url")
    @classmethod
    def validate_endpoint(cls, value: str | None) -> str | None:
        if value is not None:
            parsed = urlsplit(value)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
            ):
                raise ValueError(
                    "S3 endpoint must be an HTTP(S) URL without credentials"
                )
        return value


class S3Results:
    def __init__(self, settings: S3Settings, environment: dict[str, str]):
        self.settings = settings
        self.client = boto3.Session(
            aws_access_key_id=environment.get("AWS_ACCESS_KEY_ID") or None,
            aws_secret_access_key=environment.get("AWS_SECRET_ACCESS_KEY") or None,
            aws_session_token=environment.get("AWS_SESSION_TOKEN") or None,
            region_name=settings.region,
        ).client(
            "s3",
            endpoint_url=settings.endpoint_url,
            config=Config(
                connect_timeout=10,
                read_timeout=60,
                retries={"mode": "standard", "total_max_attempts": 3},
                s3={"addressing_style": "path"},
            ),
        )

    def put(
        self,
        body: BinaryIO,
        *,
        key: str,
        request_sha256: str,
        content_type: str,
        content_encoding: str | None = None,
    ) -> dict:
        body.seek(0)
        hasher = hashlib.sha256()
        for chunk in iter(lambda: body.read(1024 * 1024), b""):
            hasher.update(chunk)
        digest = hasher.hexdigest()
        size = body.seek(0, 2)
        body.seek(0)
        metadata = {"sha256": digest, "request-sha256": request_sha256}
        params = {
            "Bucket": self.settings.bucket,
            "Key": key,
            "Body": body,
            "ContentLength": size,
            "ContentType": content_type,
            "Metadata": metadata,
            "IfNoneMatch": "*",
        }
        if content_encoding is not None:
            params["ContentEncoding"] = content_encoding
        try:
            try:
                receipt = self.client.put_object(**params)
            except ClientError as error:
                if error.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
                    raise
                # A lost response may leave an already stored object. Never overwrite it.
                receipt = self.client.head_object(Bucket=self.settings.bucket, Key=key)
                if (
                    receipt.get("Metadata") != metadata
                    or receipt["ContentLength"] != size
                    or receipt["ContentType"] != content_type
                    or receipt.get("ContentEncoding") != content_encoding
                ):
                    raise ResultDeliveryError(
                        "S3 key already contains a different result"
                    ) from None
        except (BotoCoreError, ClientError) as error:
            raise ResultDeliveryError(
                f"S3 delivery failed ({type(error).__name__})"
            ) from error
        descriptor = {
            "bucket": self.settings.bucket,
            "key": key,
            "sha256": digest,
            "bytes": size,
            "content_type": content_type,
        }
        if content_encoding is not None:
            descriptor["content_encoding"] = content_encoding
        if receipt.get("VersionId") is not None:
            descriptor["version_id"] = receipt["VersionId"]
        return descriptor

    def prepare_event(self, root: Path, job: CrawlJob) -> dict:
        """Upload deterministic objects; the caller persists the event before publishing."""
        assert job.result_file is not None
        result_file = root / job.result_file
        result = json.loads(result_file.read_text(encoding="utf-8"))
        request = CrawlRequest.model_validate_json(
            (root / "jobs" / job.request_id / "request.json").read_text(
                encoding="utf-8"
            )
        )
        request_hash = hashlib.sha256(
            json.dumps(
                request.model_dump(), sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest()
        prefix = "/".join(
            part
            for part in (
                self.settings.prefix,
                job.request_id,
                "attempts",
                f"{job.attempt:04}",
            )
            if part
        )
        with TemporaryFile() as body:
            with gzip.GzipFile(
                fileobj=body, mode="wb", filename="", mtime=0
            ) as compressed:
                with result_file.open("rb") as source:
                    shutil.copyfileobj(source, compressed)
            stored_result = self.put(
                body,
                key=f"{prefix}/result.json.gz",
                request_sha256=request_hash,
                content_type="application/json",
                content_encoding="gzip",
            )
        artifacts = None
        if request.save_artifacts or job.state in {"failed", "cancelled"}:
            files = sorted(
                path
                for path in result_file.parent.rglob("*")
                if path.is_file() and path != result_file and not path.is_symlink()
            )
            if files:
                with TemporaryFile() as body:
                    with gzip.GzipFile(
                        fileobj=body, mode="wb", filename="", mtime=0
                    ) as compressed:
                        with tarfile.open(fileobj=compressed, mode="w|") as archive:
                            for path in files:
                                info = tarfile.TarInfo(
                                    str(path.relative_to(result_file.parent))
                                )
                                info.size = path.stat().st_size
                                info.mode = 0o600
                                with path.open("rb") as source:
                                    archive.addfile(info, source)
                    artifacts = self.put(
                        body,
                        key=f"{prefix}/artifacts.tar.gz",
                        request_sha256=request_hash,
                        content_type="application/gzip",
                    )
        event_id = hashlib.sha256(
            f"{self.settings.bucket}:{prefix}:{request_hash}".encode()
        ).hexdigest()
        return {
            "schema_version": "company-crawl-event/1.0",
            "event_id": f"crawl-{event_id}",
            "request_id": job.request_id,
            "attempt": job.attempt,
            "retry_of": job.retry_of,
            "retry_of_attempt": job.retry_of_attempt,
            "state": job.state,
            "crawl_status": job.crawl_status,
            "finished_at": job.finished_at,
            "page_count": len(result.get("documents", [])),
            "error": job.error,
            "result": stored_result,
            "artifacts": artifacts,
        }
