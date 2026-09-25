"""Bounded, verified gzip downloads and immutable object-store cache identities."""

import gzip
import hashlib
import logging
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from botocore.exceptions import ClientError
from dlt.sources.helpers import requests
from requests.exceptions import RequestException

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.commoncrawl_domain_graph.source import validate_graph_release

RANK_HEADER = [
    "harmonicc_pos",
    "harmonicc_val",
    "pr_pos",
    "pr_val",
    "host_rev",
    "n_hosts",
]
SCHEMAS = {
    "ranks": "domain-ranks-tsv-v1",
    "nodes": "domain-vertices-tsv-v1",
    "edges": "domain-edges-tsv-v1",
}


@dataclass(frozen=True)
class ArtifactSource:
    graph_release: str
    artifact_kind: str
    source_url: str
    source_etag: str
    source_bytes: int
    expected_rows: int

    def __post_init__(self) -> None:
        validate_graph_release(self.graph_release)
        if self.artifact_kind not in ("nodes", "edges", "ranks"):
            raise ValueError("Unknown graph artifact kind")
        if (
            not self.source_url
            or not self.source_etag
            or type(self.source_bytes) is not int
            or type(self.expected_rows) is not int
            or self.source_bytes <= 0
            or self.expected_rows <= 0
        ):
            raise ValueError(
                "A graph artifact needs URL, ETag, byte size and positive expected rows"
            )


@dataclass(frozen=True)
class CachedArtifact:
    source: ArtifactSource
    bucket: str
    key: str
    sha256: str
    schema_version: str


def artifact_key(source: ArtifactSource) -> str:
    identity = hashlib.sha256(
        f"{source.source_url}\n{source.source_etag}\n{source.source_bytes}".encode()
    ).hexdigest()
    return f"domain/{source.graph_release}/{identity}/{source.artifact_kind}.txt.gz"


def verify_gzip(path: Path, kind: str) -> str:
    schema = SCHEMAS[kind]
    with gzip.open(path, "rb") as stream:
        if kind == "ranks":
            header = stream.readline(4096).decode("utf-8").strip().split("\t")
            columns = [column.removeprefix("#") for column in header]
            if columns == RANK_HEADER[:-1]:
                schema = "domain-ranks-without-host-count-tsv-v1"
            elif columns != RANK_HEADER:
                raise ValueError("Unsupported domain rank header")
        while stream.read(1024 * 1024):
            pass  # Read through every member's trailer so truncated/corrupt gzip fails.
    return schema


def cache_artifact(
    source: ArtifactSource, objects: ObjectStoreResource, log: logging.Logger
) -> CachedArtifact:
    key = artifact_key(source)
    client = objects.client()
    try:
        head = client.head_object(Bucket=objects.bucket, Key=key)
    except ClientError as error:
        if error.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
            raise
    else:
        metadata = head.get("Metadata", {})
        if (
            head["ContentLength"] != source.source_bytes
            or not re.fullmatch(r"[a-f0-9]{64}", metadata.get("sha256", ""))
            or metadata.get("schema")
            not in (
                {SCHEMAS["ranks"], "domain-ranks-without-host-count-tsv-v1"}
                if source.artifact_kind == "ranks"
                else {SCHEMAS[source.artifact_kind]}
            )
        ):
            raise ValueError(
                "Existing graph cache object failed size/schema/checksum metadata validation"
            )
        log.info("Reusing cached %s for %s", source.artifact_kind, source.graph_release)
        return CachedArtifact(
            source, objects.bucket, key, metadata["sha256"], metadata["schema"]
        )

    with TemporaryDirectory(prefix="commoncrawl-graph-") as directory:
        path = Path(directory) / "source.txt.gz"
        if shutil.disk_usage(directory).free < source.source_bytes * 1.05:
            raise ValueError(
                "Insufficient temporary disk space for this compressed graph file; configure TMPDIR on the data volume"
            )
        for attempt in range(3):
            checksum = hashlib.sha256()
            size = 0
            last_log = time.monotonic()
            try:
                with (
                    requests.Session() as session,
                    session.get(
                        source.source_url,
                        headers={
                            "If-Match": source.source_etag,
                            "Accept-Encoding": "identity",
                        },
                        timeout=(30, 120),
                        stream=True,
                        allow_redirects=False,
                    ) as response,
                    path.open("wb") as output,
                ):
                    response.raise_for_status()
                    if (
                        response.status_code != 200
                        or response.headers.get("ETag") != source.source_etag
                    ):
                        raise ValueError("Graph source changed after discovery")
                    if (
                        int(response.headers.get("Content-Length", "0"))
                        != source.source_bytes
                    ):
                        raise ValueError("Graph source length changed after discovery")
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        size += len(chunk)
                        if size > source.source_bytes:
                            raise ValueError("Graph stream exceeds its expected length")
                        checksum.update(chunk)
                        output.write(chunk)
                        if time.monotonic() - last_log >= 30:
                            log.info(
                                "%s: downloaded %s / %s compressed bytes",
                                source.artifact_kind,
                                size,
                                source.source_bytes,
                            )
                            last_log = time.monotonic()
                if size != source.source_bytes:
                    raise EOFError("Incomplete graph download")
                log.info("Verifying gzip integrity for %s", source.artifact_kind)
                schema = verify_gzip(path, source.artifact_kind)
                break
            except RequestException, EOFError, gzip.BadGzipFile:
                if attempt == 2:
                    raise
                log.warning(
                    "Retrying interrupted %s download, attempt %s",
                    source.artifact_kind,
                    attempt + 2,
                )
                time.sleep(2**attempt)
        # Multipart upload becomes visible only on completion. The object is already validated.
        client.upload_file(
            str(path),
            objects.bucket,
            key,
            ExtraArgs={
                "Metadata": {"sha256": checksum.hexdigest(), "schema": schema},
                "ContentType": "application/gzip",
            },
        )
    return CachedArtifact(source, objects.bucket, key, checksum.hexdigest(), schema)
