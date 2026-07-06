import json
from hashlib import sha256
from pathlib import Path
from typing import Any

from dagster_v3.defs.brazil_companies.cgu.source import (
    BRAZIL_CGU_RAW_BUCKET,
    BrazilCguResource,
    cgu_archive_object_key,
    cgu_metadata_object_key,
    cgu_source_files_from_pages,
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
        self.text = body.decode("utf-8")
        self.headers = {
            "Content-Length": str(len(body)),
            "Content-Type": "application/x-zip-compressed",
            "Last-Modified": "Mon, 06 Jul 2026 18:00:04 GMT",
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
        assert isinstance(kwargs["timeout"], int)
        return FakeResponse(self.responses_by_url[url])


class NoDownloadSession:
    def __init__(self, page_responses_by_url: dict[str, bytes]) -> None:
        self.page_responses_by_url = page_responses_by_url
        self.requested_urls: list[str] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.requested_urls.append(url)
        if url not in self.page_responses_by_url:
            raise AssertionError(f"unexpected archive download: {url}")
        return FakeResponse(self.page_responses_by_url[url])


def test_cgu_source_files_are_resolved_from_portal_pages() -> None:
    files = cgu_source_files_from_pages(
        {
            "ceis": _portal_page("CEIS", "2026", "07", "06"),
            "cnep": _portal_page("CNEP", "2026", "07", "06"),
            "cepim": _portal_page("CEPIM", "2026", "07", "03"),
            "leniency_agreements": _portal_page("AcordosLeniencia", "2026", "07", "04"),
        }
    )

    assert [(file.dataset, file.snapshot_date) for file in files] == [
        ("ceis", "2026-07-06"),
        ("cnep", "2026-07-06"),
        ("cepim", "2026-07-03"),
        ("leniency_agreements", "2026-07-04"),
    ]
    assert files[0].url == (
        "https://portaldatransparencia.gov.br/download-de-dados/ceis/20260706"
    )
    assert files[-1].url == (
        "https://portaldatransparencia.gov.br/download-de-dados/"
        "acordos-leniencia/20260704"
    )


def test_cgu_object_keys_are_stable_per_dataset_snapshot() -> None:
    assert cgu_archive_object_key("ceis", "2026-07-06") == (
        "brazil_cgu/sanctions/raw_archives/dataset=ceis/"
        "snapshot_date=2026-07-06/archive.zip"
    )
    assert cgu_metadata_object_key("ceis", "2026-07-06") == (
        "brazil_cgu/sanctions/raw_archives/dataset=ceis/"
        "snapshot_date=2026-07-06/metadata.json"
    )


def test_cgu_resource_downloads_missing_latest_archives() -> None:
    resource = BrazilCguResource()
    object_store = FakeObjectStore()
    page_responses = _page_responses()
    source_files = cgu_source_files_from_pages(
        {dataset: page.decode("utf-8") for dataset, page in page_responses.items()}
    )
    archive_responses = {
        source_file.url: f"{source_file.dataset}-zip".encode()
        for source_file in source_files
    }
    session = FakeSession({**resource_page_urls(page_responses), **archive_responses})

    result = resource.sync_latest_archives(
        object_store=object_store,
        session=session,
    )

    assert object_store.created_buckets == [BRAZIL_CGU_RAW_BUCKET]
    assert [archive.dataset for archive in result.archives] == [
        "ceis",
        "cnep",
        "cepim",
        "leniency_agreements",
    ]
    for archive in result.archives:
        body = f"{archive.dataset}-zip".encode()
        archive_key = cgu_archive_object_key(archive.dataset, archive.snapshot_date)
        metadata_key = cgu_metadata_object_key(archive.dataset, archive.snapshot_date)
        assert object_store.objects[(BRAZIL_CGU_RAW_BUCKET, archive_key)] == body
        assert archive.downloaded is True
        assert archive.reused_existing_archive is False
        assert archive.size_bytes == len(body)
        assert archive.sha256 == sha256(body).hexdigest()
        metadata = json.loads(
            object_store.objects[(BRAZIL_CGU_RAW_BUCKET, metadata_key)]
        )
        assert metadata["archive_key"] == archive_key


def test_cgu_resource_skips_existing_latest_archives() -> None:
    resource = BrazilCguResource()
    object_store = FakeObjectStore()
    page_responses = _page_responses()
    source_files = cgu_source_files_from_pages(
        {dataset: page.decode("utf-8") for dataset, page in page_responses.items()}
    )
    for source_file in source_files:
        archive_key = cgu_archive_object_key(
            source_file.dataset, source_file.snapshot_date
        )
        metadata_key = cgu_metadata_object_key(
            source_file.dataset, source_file.snapshot_date
        )
        body = f"existing-{source_file.dataset}".encode()
        object_store.objects[(BRAZIL_CGU_RAW_BUCKET, archive_key)] = body
        object_store.objects[(BRAZIL_CGU_RAW_BUCKET, metadata_key)] = json.dumps(
            {
                "size_bytes": len(body),
                "sha256": sha256(body).hexdigest(),
                "content_type": "application/x-zip-compressed",
                "source_last_modified": "Mon, 06 Jul 2026 18:00:04 GMT",
            }
        ).encode()

    session = NoDownloadSession(resource_page_urls(page_responses))
    result = resource.sync_latest_archives(
        object_store=object_store,
        session=session,
    )

    assert session.requested_urls == [
        resource.source_page_url(dataset) for dataset in page_responses
    ]
    assert all(archive.downloaded is False for archive in result.archives)
    assert all(archive.reused_existing_archive is True for archive in result.archives)


def _portal_page(origin: str, year: str, month: str, day: str) -> str:
    return (
        '<script>arquivos.push({"ano" : "'
        + year
        + '", "mes" : "'
        + month
        + '", "dia" : "'
        + day
        + '", "origem" :  "'
        + origin
        + '"});</script>'
    )


def _page_responses() -> dict[str, bytes]:
    return {
        "ceis": _portal_page("CEIS", "2026", "07", "06").encode(),
        "cnep": _portal_page("CNEP", "2026", "07", "06").encode(),
        "cepim": _portal_page("CEPIM", "2026", "07", "03").encode(),
        "leniency_agreements": _portal_page(
            "AcordosLeniencia", "2026", "07", "04"
        ).encode(),
    }


def resource_page_urls(page_responses: dict[str, bytes]) -> dict[str, bytes]:
    resource = BrazilCguResource()
    return {
        resource.source_page_url(dataset): page
        for dataset, page in page_responses.items()
    }
