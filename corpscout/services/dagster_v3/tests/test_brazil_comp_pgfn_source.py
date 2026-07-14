import json
from hashlib import sha256
from pathlib import Path
from typing import Any

from dagster_v3.defs.brazil_companies.pgfn.source import (
    BRAZIL_PGFN_RAW_BUCKET,
    BrazilPgfnResource,
    pgfn_archive_object_key,
    pgfn_metadata_object_key,
    pgfn_partition_keys,
    pgfn_snapshot_source_files,
)


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.created_buckets: list[str] = []
        self.uploaded_files: list[tuple[str, str]] = []
        self.written_json: list[tuple[str, str]] = []

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def upload_file(
        self,
        key: str,
        source_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        self.uploaded_files.append((bucket, key))
        self.objects[(bucket, key)] = Path(source_path).read_bytes()

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        return self.objects[(bucket, key)]

    def write_json(self, key: str, body: str, bucket: str | None = None) -> None:
        assert bucket is not None
        self.written_json.append((bucket, key))
        self.objects[(bucket, key)] = body.encode("utf-8")


class FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body
        self.headers = {
            "Content-Length": str(len(body)),
            "Content-Type": "application/zip",
            "Last-Modified": "Fri, 17 Apr 2026 19:20:58 GMT",
        }

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 0) -> list[bytes]:
        return [self._body[:3], self._body[3:]]


class FakeSession:
    def __init__(self, responses_by_url: dict[str, bytes]) -> None:
        self.responses_by_url = responses_by_url
        self.requested_urls: list[str] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.requested_urls.append(url)
        assert kwargs["stream"] is True
        assert isinstance(kwargs["timeout"], int)
        return FakeResponse(self.responses_by_url[url])


class NoDownloadSession:
    def __init__(self) -> None:
        self.requested_urls: list[str] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.requested_urls.append(url)
        raise AssertionError(f"unexpected HTTP request: {url}")


def test_pgfn_partition_keys_cover_official_quarterly_inventory() -> None:
    keys = pgfn_partition_keys()

    assert keys[0] == "2020-Q1"
    assert keys[-1] == "2026-Q1"
    assert "2022-Q4" in keys
    assert len(keys) == 25


def test_pgfn_snapshot_source_files_are_deterministic_with_official_filename_typo() -> (
    None
):
    files = pgfn_snapshot_source_files("2026-Q1")

    assert [file.source_system for file in files] == [
        "nao_previdenciario",
        "fgts",
        "previdenciario",
    ]
    assert files[0].url == (
        "https://dadosabertos.pgfn.gov.br/2026_trimestre_01/"
        "Dados_abertos_Nao_Previdenciario.zip"
    )
    assert (
        pgfn_snapshot_source_files("2022-Q4")[0].url
        == "https://dadosabertos.pgfn.gov.br/2022_trimestre_04/"
        "Dados_abertos_Nao_Previdenciariozip.zip"
    )


def test_pgfn_object_keys_are_stable_per_snapshot_and_source_system() -> None:
    assert pgfn_archive_object_key("2026-Q1", "fgts") == (
        "brazil_pgfn/raw_archives/snapshot=2026-Q1/source_system=fgts/archive.zip"
    )
    assert pgfn_metadata_object_key("2026-Q1", "fgts") == (
        "brazil_pgfn/raw_archives/snapshot=2026-Q1/source_system=fgts/metadata.json"
    )


def test_pgfn_resource_downloads_missing_snapshot_archives() -> None:
    resource = BrazilPgfnResource()
    object_store = FakeObjectStore()
    files = pgfn_snapshot_source_files("2026-Q1")
    responses = {
        source_file.url: f"{source_file.source_system}-zip".encode()
        for source_file in files
    }
    session = FakeSession(responses)

    result = resource.sync_snapshot_archives(
        snapshot_quarter="2026-Q1",
        object_store=object_store,
        session=session,
    )

    assert object_store.created_buckets == [BRAZIL_PGFN_RAW_BUCKET]
    assert session.requested_urls == [file.url for file in files]
    assert len(result.archives) == 3
    for archive in result.archives:
        archive_key = pgfn_archive_object_key("2026-Q1", archive.source_system)
        metadata_key = pgfn_metadata_object_key("2026-Q1", archive.source_system)
        body = f"{archive.source_system}-zip".encode()
        assert object_store.objects[(BRAZIL_PGFN_RAW_BUCKET, archive_key)] == body
        assert archive.downloaded is True
        assert archive.reused_existing_archive is False
        assert archive.size_bytes == len(body)
        assert archive.sha256 == sha256(body).hexdigest()
        metadata = json.loads(
            object_store.objects[(BRAZIL_PGFN_RAW_BUCKET, metadata_key)]
        )
        assert metadata["archive_key"] == archive_key


def test_pgfn_resource_skips_existing_snapshot_archives() -> None:
    resource = BrazilPgfnResource()
    object_store = FakeObjectStore()
    for source_file in pgfn_snapshot_source_files("2026-Q1"):
        archive_key = pgfn_archive_object_key("2026-Q1", source_file.source_system)
        metadata_key = pgfn_metadata_object_key("2026-Q1", source_file.source_system)
        body = f"existing-{source_file.source_system}".encode()
        object_store.objects[(BRAZIL_PGFN_RAW_BUCKET, archive_key)] = body
        object_store.objects[(BRAZIL_PGFN_RAW_BUCKET, metadata_key)] = json.dumps(
            {
                "size_bytes": len(body),
                "sha256": sha256(body).hexdigest(),
                "content_type": "application/zip",
                "source_last_modified": "Fri, 17 Apr 2026 19:20:58 GMT",
            }
        ).encode()

    session = NoDownloadSession()
    result = resource.sync_snapshot_archives(
        snapshot_quarter="2026-Q1",
        object_store=object_store,
        session=session,
    )

    assert session.requested_urls == []
    assert all(archive.downloaded is False for archive in result.archives)
    assert all(archive.reused_existing_archive is True for archive in result.archives)
