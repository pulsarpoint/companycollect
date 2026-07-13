import json
from pathlib import Path

import pytest

from warc_index_builder import publication


CRAWL = "CC-MAIN-2026-25"
SELECTION = "pages25"


class FakeS3Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.objects: dict[tuple[str, str], tuple[bytes, dict[str, str]]] = {}
        self.corrupt_head_key: str | None = None

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        self.calls.append(("delete", Key))
        self.objects.pop((Bucket, Key), None)

    def upload_file(
        self,
        filename: str,
        bucket: str,
        key: str,
        *,
        ExtraArgs: dict[str, object],
    ) -> None:
        self.calls.append(("upload", key))
        metadata = ExtraArgs["Metadata"]
        assert isinstance(metadata, dict)
        self.objects[(bucket, key)] = (
            Path(filename).read_bytes(),
            {str(name): str(value) for name, value in metadata.items()},
        )

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        self.calls.append(("head", Key))
        content, metadata = self.objects[(Bucket, Key)]
        if Key == self.corrupt_head_key:
            metadata = {"sha256": "wrong"}
        return {"ContentLength": len(content), "Metadata": metadata}

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
    ) -> None:
        assert ContentType == "application/json"
        self.calls.append(("put", Key))
        self.objects[(Bucket, Key)] = (Body, {})


def _environment() -> dict[str, str]:
    return {
        "COMMONCRAWL_CATALOG_S3_BASE": "s3://crawls/catalogs",
        "CORPSCOUT_S3_ENDPOINT": "http://rustfs:9000",
        "CORPSCOUT_S3_ACCESS_KEY": "access",
        "CORPSCOUT_S3_SECRET_KEY": "secret",
    }


def test_destination_from_environment_uses_path_style_defaults() -> None:
    destination = publication.destination_from_environment(_environment())

    assert destination.bucket == "crawls"
    assert destination.prefix == "catalogs"
    assert destination.endpoint == "http://rustfs:9000"
    assert destination.region == "us-east-1"


@pytest.mark.parametrize(
    "environment,error",
    [
        ({}, "COMMONCRAWL_CATALOG_S3_BASE is required"),
        (
            {"COMMONCRAWL_CATALOG_S3_BASE": "https://crawls/catalogs"},
            "must use s3://",
        ),
        (
            {"COMMONCRAWL_CATALOG_S3_BASE": "s3:///catalogs"},
            "bucket",
        ),
        (
            {"COMMONCRAWL_CATALOG_S3_BASE": "s3://crawls/a//b"},
            "prefix",
        ),
        (
            {"COMMONCRAWL_CATALOG_S3_BASE": "s3://crawls/catalogs"},
            "CORPSCOUT_S3_ENDPOINT",
        ),
        (
            {
                **_environment(),
                "CORPSCOUT_S3_ENDPOINT": "rustfs:9000",
            },
            "HTTP\\(S\\) endpoint",
        ),
    ],
)
def test_destination_rejects_invalid_or_incomplete_configuration(
    environment: dict[str, str],
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        publication.destination_from_environment(environment)


def test_publish_removes_ready_verifies_catalog_and_commits_ready_last(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog_path = tmp_path / "catalog.duckdb"
    catalog_path.write_bytes(b"catalog-data")
    client = FakeS3Client()
    captured: dict[str, object] = {}

    def client_factory(service: str, **options: object) -> FakeS3Client:
        captured["service"] = service
        captured.update(options)
        return client

    monkeypatch.setattr(publication.boto3, "client", client_factory)
    destination = publication.destination_from_environment(_environment())

    result = publication.publish_catalog(
        destination,
        crawl=CRAWL,
        selection=SELECTION,
        catalog_path=catalog_path,
    )

    prefix = f"catalogs/{CRAWL}/{SELECTION}"
    catalog_key = f"{prefix}/catalog.duckdb"
    ready_key = f"{prefix}/ready.json"
    assert client.calls == [
        ("delete", ready_key),
        ("upload", catalog_key),
        ("head", catalog_key),
        ("put", ready_key),
    ]
    ready = json.loads(client.objects[("crawls", ready_key)][0])
    assert ready == {
        "schema_version": 1,
        "crawl_id": CRAWL,
        "selection": SELECTION,
        "catalog": {
            "key": catalog_key,
            "size_bytes": 12,
            "sha256": result.catalog.sha256,
        },
    }
    assert client.objects[("crawls", catalog_key)][1] == {
        "sha256": result.catalog.sha256
    }
    assert captured["service"] == "s3"
    assert captured["endpoint_url"] == "http://rustfs:9000"
    assert captured["region_name"] == "us-east-1"
    assert captured["config"].s3 == {"addressing_style": "path"}


def test_failed_head_verification_leaves_catalog_not_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog_path = tmp_path / "catalog.duckdb"
    catalog_path.write_bytes(b"catalog")
    client = FakeS3Client()
    ready_key = f"catalogs/{CRAWL}/{SELECTION}/ready.json"
    client.objects[("crawls", ready_key)] = (b"old", {})
    client.corrupt_head_key = f"catalogs/{CRAWL}/{SELECTION}/catalog.duckdb"
    monkeypatch.setattr(publication.boto3, "client", lambda *_args, **_kwargs: client)
    destination = publication.destination_from_environment(_environment())

    with pytest.raises(RuntimeError, match="HEAD verification failed"):
        publication.publish_catalog(
            destination,
            crawl=CRAWL,
            selection=SELECTION,
            catalog_path=catalog_path,
        )

    assert ("crawls", ready_key) not in client.objects
    assert not any(call == ("put", ready_key) for call in client.calls)
