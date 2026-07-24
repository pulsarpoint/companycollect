import json
from datetime import date
from hashlib import md5
from pathlib import Path

import pytest

from dagster_v3.defs.esma_firds.source import (
    FirdsSourceFile,
    FirdsResource,
    archive_object_key,
    build_download_plan,
    complete_file_sets,
    metadata_object_key,
    parse_solr_response,
)
from dagster_v3.defs.esma_firds.tables import EXPECTED_FULL_CFI_CATEGORIES


def _source_file(name: str, file_type: str, publication_date: str) -> FirdsSourceFile:
    return FirdsSourceFile.from_solr_doc(
        {
            "id": name,
            "published_instrument_file_id": name,
            "file_name": name,
            "file_type": file_type,
            "publication_date": f"{publication_date}T00:00:00Z",
            "download_link": f"https://firds.esma.europa.eu/firds/{name}",
            "checksum": "0" * 32,
        }
    )


def test_parse_solr_response_ignores_last_modification_marker() -> None:
    payload = {
        "response": {
            "numFound": 2,
            "docs": [
                {
                    "id": "lastModification",
                    "file_name": "",
                    "file_type": "",
                    "publication_date": "2026-07-23T07:03:54Z",
                },
                {
                    "id": "113941",
                    "published_instrument_file_id": "113941",
                    "file_name": "DLTINS_20260723_01of01.zip",
                    "file_type": "DLTINS",
                    "publication_date": "2026-07-23T00:00:00Z",
                    "download_link": (
                        "https://firds.esma.europa.eu/firds/"
                        "DLTINS_20260723_01of01.zip"
                    ),
                    "checksum": "73b6b0615f0c7914088371e2ed84e35f",
                },
            ],
        }
    }

    files = parse_solr_response(payload)

    assert len(files) == 1
    assert files[0].file_type == "DLTINS"
    assert files[0].publication_date == date(2026, 7, 23)
    assert files[0].part_number == 1
    assert files[0].part_count == 1


def test_complete_file_sets_require_every_full_cfi_category_and_part() -> None:
    full_files = [
        _source_file(
            f"FULINS_{category}_20260718_01of01.zip",
            "FULINS",
            "2026-07-18",
        )
        for category in sorted(EXPECTED_FULL_CFI_CATEGORIES)
    ]

    sets = complete_file_sets(full_files, file_type="FULINS")

    assert len(sets) == 1
    assert sets[0].publication_date == date(2026, 7, 18)
    assert sets[0].is_complete is True

    incomplete = complete_file_sets(full_files[:-1], file_type="FULINS")
    assert incomplete[0].is_complete is False
    assert incomplete[0].missing_categories


def test_complete_delta_set_detects_missing_part() -> None:
    files = [
        _source_file("DLTINS_20260723_01of02.zip", "DLTINS", "2026-07-23")
    ]

    sets = complete_file_sets(files, file_type="DLTINS")

    assert sets[0].is_complete is False
    assert sets[0].missing_parts == (2,)


def test_complete_delta_set_rejects_duplicate_part() -> None:
    source_file = _source_file(
        "DLTINS_20260723_01of01.zip",
        "DLTINS",
        "2026-07-23",
    )

    sets = complete_file_sets(
        [source_file, source_file],
        file_type="DLTINS",
    )

    assert sets[0].is_complete is False
    assert sets[0].duplicate_parts == (1,)


def test_download_plan_uses_latest_complete_baselines_and_post_baseline_deltas() -> None:
    files = [
        *[
            _source_file(
                f"FULINS_{category}_20260718_01of01.zip",
                "FULINS",
                "2026-07-18",
            )
            for category in sorted(EXPECTED_FULL_CFI_CATEGORIES)
        ],
        _source_file("DLTINS_20260718_01of01.zip", "DLTINS", "2026-07-18"),
        _source_file("DLTINS_20260719_01of01.zip", "DLTINS", "2026-07-19"),
        _source_file("DLTINS_20260720_01of01.zip", "DLTINS", "2026-07-20"),
        _source_file("FULCAN_20260718_01of01.zip", "FULCAN", "2026-07-18"),
    ]

    plan = build_download_plan(files)

    assert plan.full.publication_date == date(2026, 7, 18)
    assert [item.publication_date for item in plan.deltas] == [
        date(2026, 7, 18),
        date(2026, 7, 19),
        date(2026, 7, 20),
    ]
    assert plan.cancellations.publication_date == date(2026, 7, 18)
    assert len(plan.files) == len(EXPECTED_FULL_CFI_CATEGORIES) + 4


def test_download_plan_rejects_missing_complete_full_snapshot() -> None:
    with pytest.raises(ValueError, match="complete FULINS"):
        build_download_plan(
            [_source_file("DLTINS_20260720_01of01.zip", "DLTINS", "2026-07-20")]
        )


def test_object_keys_are_immutable_per_source_checksum() -> None:
    file = _source_file("DLTINS_20260720_01of01.zip", "DLTINS", "2026-07-20")

    assert archive_object_key(file) == (
        "esma_firds/raw/file_type=DLTINS/publication_date=2026-07-20/"
        f"checksum={'0' * 32}/DLTINS_20260720_01of01.zip"
    )
    assert metadata_object_key(file).endswith("/metadata.json")


class _DownloadResponse:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.headers = {"Content-Length": str(len(body))}

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, *, chunk_size: int):
        for offset in range(0, len(self.body), chunk_size):
            yield self.body[offset : offset + chunk_size]


class _DownloadSession:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.calls = 0

    def get(self, *_args, **_kwargs) -> _DownloadResponse:
        self.calls += 1
        return _DownloadResponse(self.body)


class _MemoryObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def ensure_bucket(self, _bucket: str) -> None:
        return None

    def exists(self, key: str, *, bucket: str) -> bool:
        assert bucket == "source-esma-firds"
        return key in self.objects

    def read_bytes(self, key: str, *, bucket: str) -> bytes:
        assert bucket == "source-esma-firds"
        return self.objects[key]

    def upload_file(self, key: str, path: Path, *, bucket: str) -> None:
        assert bucket == "source-esma-firds"
        self.objects[key] = path.read_bytes()

    def write_json(self, key: str, body: str, *, bucket: str) -> None:
        assert bucket == "source-esma-firds"
        self.objects[key] = body.encode()


def test_sync_files_verifies_checksum_and_reuses_immutable_archive() -> None:
    body = b"synthetic FIRDS archive bytes"
    checksum = md5(body, usedforsecurity=False).hexdigest()
    source_file = FirdsSourceFile.from_solr_doc(
        {
            "id": "source-1",
            "file_name": "DLTINS_20260723_01of01.zip",
            "file_type": "DLTINS",
            "publication_date": "2026-07-23T00:00:00Z",
            "download_link": "https://example.test/DLTINS.zip",
            "checksum": checksum,
        }
    )
    object_store = _MemoryObjectStore()
    session = _DownloadSession(body)
    resource = FirdsResource(download_max_attempts=1)

    first = resource.sync_files(
        files=(source_file,),
        object_store=object_store,
        session=session,
    )
    second = resource.sync_files(
        files=(source_file,),
        object_store=object_store,
        session=session,
    )

    assert first[0].downloaded is True
    assert second[0].downloaded is False
    assert session.calls == 1
    metadata = json.loads(object_store.objects[first[0].metadata_key])
    assert metadata["source_checksum_md5"] == checksum
    assert metadata["archive_size_bytes"] == len(body)
