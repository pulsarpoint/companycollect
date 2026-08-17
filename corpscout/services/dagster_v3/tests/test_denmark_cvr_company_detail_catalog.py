import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq

from dagster_v3.defs.denmark_cvr.company_detail_catalog import (
    DENMARK_CVR_COMPANY_DETAIL_CATALOG_PILOT_PARTITION,
    load_company_detail_catalog,
)
from dagster_v3.defs.denmark_cvr.company_detail_compaction import (
    compact_company_detail_partition,
    denmark_cvr_company_details_compacted_s3,
    load_company_detail_compacted_catalog,
)
from dagster_v3.defs.denmark_cvr.company_details import (
    DenmarkCvrCompanyDetailDownload,
    company_detail_api_url,
    company_detail_object_key,
    denmark_cvr_company_details_s3,
    write_company_detail_catalog_partition,
)


DENMARK_CVR_BUCKET = "source-denmark-cvr"
PILOT_PARTITION = "bucket_000"
COMPLETE_CVR = "10000286"
ORIGINAL_ONLY_CVR = "10000312"
DOWNLOAD_CVR = "10000322"


class NoListingObjectStore:
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or {})
        self.read_keys: list[str] = []
        self.write_keys: list[str] = []
        self.exists_keys: list[str] = []

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket == DENMARK_CVR_BUCKET

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        raise AssertionError(f"prefix listing is forbidden: {bucket}/{prefix}")

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket == DENMARK_CVR_BUCKET
        self.exists_keys.append(key)
        return key in self.objects

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket == DENMARK_CVR_BUCKET
        self.read_keys.append(key)
        return self.objects[key]

    def write_bytes(
        self,
        key: str,
        body: bytes,
        bucket: str | None = None,
    ) -> None:
        assert bucket == DENMARK_CVR_BUCKET
        self.objects[key] = body
        self.write_keys.append(key)


class FakeDetailResource:
    def __init__(self, downloads: dict[str, DenmarkCvrCompanyDetailDownload]) -> None:
        self.downloads = downloads
        self.requested_cvrs: list[str] = []

    def iter_company_details(self, cvrs: tuple[str, ...], **_: Any):
        for cvr in cvrs:
            self.requested_cvrs.append(cvr)
            yield self.downloads[cvr]


def _json_body(cvr: str, name: str) -> bytes:
    return json.dumps(
        {"stamdata": {"cvrnummer": cvr, "navn": name}},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


def _download(cvr: str, name: str) -> DenmarkCvrCompanyDetailDownload:
    body = _json_body(cvr, name).decode()
    return DenmarkCvrCompanyDetailDownload(
        cvr=cvr,
        source_url=company_detail_api_url("https://datacvr.virk.dk", cvr),
        raw_body=body,
        payload=json.loads(body),
        status=200,
        response_headers={"content-type": "application/json"},
    )


def _seeded_store() -> NoListingObjectStore:
    return NoListingObjectStore(
        {
            company_detail_object_key(
                PILOT_PARTITION,
                COMPLETE_CVR,
                english_keys=False,
            ): _json_body(COMPLETE_CVR, "Komplet"),
            company_detail_object_key(
                PILOT_PARTITION,
                COMPLETE_CVR,
                english_keys=True,
            ): b'{"masterData":{"companyRegistrationNumber":"10000286"}}',
            company_detail_object_key(
                PILOT_PARTITION,
                ORIGINAL_ONLY_CVR,
                english_keys=False,
            ): _json_body(ORIGINAL_ONLY_CVR, "Kun original"),
        }
    )


def test_catalog_partition_bootstraps_from_exact_keys_without_listing(
    tmp_path: Path,
) -> None:
    store = _seeded_store()
    details = FakeDetailResource({DOWNLOAD_CVR: _download(DOWNLOAD_CVR, "Ny")})

    result = write_company_detail_catalog_partition(
        object_store=store,
        details=details,
        partition_key=PILOT_PARTITION,
        cvrs=(COMPLETE_CVR, ORIGINAL_ONLY_CVR, DOWNLOAD_CVR),
        failure_database_path=tmp_path / "company-detail-failures.sqlite3",
        observed_at=datetime(2026, 8, 17, 8, tzinfo=UTC),
        source_run_id="bootstrap-run",
        record_failure=lambda _: None,
    )

    assert details.requested_cvrs == [DOWNLOAD_CVR]
    assert result.catalog_bootstrapped is True
    assert result.catalog_reused is False
    assert result.bootstrap_object_read_count == 3
    assert result.detail_summary.complete_company_count == 3
    assert result.catalog_reference.partition_key == PILOT_PARTITION

    catalog = load_company_detail_catalog(
        object_store=store,
        reference=result.catalog_reference,
    )
    assert len(catalog.entries) == 6
    assert {entry.cvr for entry in catalog.entries} == {
        COMPLETE_CVR,
        ORIGINAL_ONLY_CVR,
        DOWNLOAD_CVR,
    }
    assert {entry.object_kind for entry in catalog.entries} == {
        "original",
        "english",
    }


def test_catalog_partition_reuses_commit_without_reading_detail_objects(
    tmp_path: Path,
) -> None:
    store = _seeded_store()
    database_path = tmp_path / "company-detail-failures.sqlite3"
    first = write_company_detail_catalog_partition(
        object_store=store,
        details=FakeDetailResource({DOWNLOAD_CVR: _download(DOWNLOAD_CVR, "Ny")}),
        partition_key=PILOT_PARTITION,
        cvrs=(COMPLETE_CVR, ORIGINAL_ONLY_CVR, DOWNLOAD_CVR),
        failure_database_path=database_path,
        observed_at=datetime(2026, 8, 17, 8, tzinfo=UTC),
        source_run_id="bootstrap-run",
        record_failure=lambda _: None,
    )
    raw_object_keys = {
        entry.object_key
        for entry in load_company_detail_catalog(
            object_store=store,
            reference=first.catalog_reference,
        ).entries
    }
    store.read_keys.clear()
    store.write_keys.clear()

    second = write_company_detail_catalog_partition(
        object_store=store,
        details=FakeDetailResource({}),
        partition_key=PILOT_PARTITION,
        cvrs=(COMPLETE_CVR, ORIGINAL_ONLY_CVR, DOWNLOAD_CVR),
        failure_database_path=database_path,
        observed_at=datetime(2026, 8, 17, 9, tzinfo=UTC),
        source_run_id="steady-state-run",
        record_failure=lambda _: None,
    )

    assert second.catalog_bootstrapped is False
    assert second.catalog_reused is True
    assert second.bootstrap_object_read_count == 0
    assert second.detail_summary.already_complete_company_count == 3
    assert second.catalog_reference == first.catalog_reference
    assert raw_object_keys.isdisjoint(store.read_keys)
    assert store.write_keys == []


def test_compaction_writes_parquet_and_reuses_unchanged_source_catalog(
    tmp_path: Path,
) -> None:
    store = _seeded_store()
    raw = write_company_detail_catalog_partition(
        object_store=store,
        details=FakeDetailResource({DOWNLOAD_CVR: _download(DOWNLOAD_CVR, "Ny")}),
        partition_key=PILOT_PARTITION,
        cvrs=(COMPLETE_CVR, ORIGINAL_ONLY_CVR, DOWNLOAD_CVR),
        failure_database_path=tmp_path / "company-detail-failures.sqlite3",
        observed_at=datetime(2026, 8, 17, 8, tzinfo=UTC),
        source_run_id="bootstrap-run",
        record_failure=lambda _: None,
    )
    store.read_keys.clear()
    store.write_keys.clear()

    first = compact_company_detail_partition(
        object_store=store,
        source_catalog_reference=raw.catalog_reference,
        source_run_id="compaction-run",
        created_at=datetime(2026, 8, 17, 8, 30, tzinfo=UTC),
    )
    compacted = load_company_detail_compacted_catalog(
        object_store=store,
        reference=first.catalog_reference,
    )

    assert first.reused is False
    assert first.source_object_count == 6
    assert first.compacted_object_count == 1
    assert len(compacted.entries) == 1
    parquet_body = store.objects[compacted.entries[0].object_key]
    table = pq.read_table(pa.BufferReader(parquet_body))
    assert table.num_rows == 6
    assert set(table.column("cvr").to_pylist()) == {
        COMPLETE_CVR,
        ORIGINAL_ONLY_CVR,
        DOWNLOAD_CVR,
    }
    assert set(table.column("object_kind").to_pylist()) == {
        "original",
        "english",
    }

    raw_object_keys = {
        entry.object_key
        for entry in load_company_detail_catalog(
            object_store=store,
            reference=raw.catalog_reference,
        ).entries
    }
    store.read_keys.clear()
    store.write_keys.clear()
    second = compact_company_detail_partition(
        object_store=store,
        source_catalog_reference=raw.catalog_reference,
        source_run_id="second-compaction-run",
        created_at=datetime(2026, 8, 17, 9, 30, tzinfo=UTC),
    )

    assert second.reused is True
    assert second.catalog_reference == first.catalog_reference
    assert raw_object_keys.isdisjoint(store.read_keys)
    assert store.write_keys == []


def test_compacted_asset_is_a_typed_pilot_gated_downstream_asset() -> None:
    spec = denmark_cvr_company_details_compacted_s3.get_asset_spec()

    assert DENMARK_CVR_COMPANY_DETAIL_CATALOG_PILOT_PARTITION == PILOT_PARTITION
    assert (
        denmark_cvr_company_details_compacted_s3.partitions_def
        == denmark_cvr_company_details_s3.partitions_def
    )
    assert spec.group_name == "denmark_cvr_company_details"
    assert {dependency.asset_key for dependency in spec.deps} == {
        dg.AssetKey("denmark_cvr_company_details_s3")
    }

    from dagster_v3.definitions import defs as load_defs

    node = (
        load_defs()
        .get_repository_def()
        .asset_graph.get(dg.AssetKey("denmark_cvr_company_details_compacted_s3"))
    )
    assert node.parent_keys == {dg.AssetKey("denmark_cvr_company_details_s3")}
