from __future__ import annotations

import gzip
import json
from io import BytesIO
from typing import Any

import dagster as dg
import polars as pl

from dagster_v3.defs.norway_brreg.assets.entity_snapshot import (
    NORWAY_BRREG_ENTITY_BUCKET,
    entries_snapshot_raw_object_key,
    entity_snapshot_object_key,
    norway_brreg_entries_snapshot_raw_s3,
    norway_brreg_entities_snapshot_s3,
)
from dagster_v3.defs.norway_brreg.assets.entity_updates import (
    entity_update_window,
    norway_brreg_entity_updates_s3,
)


class FakeNorwayBrregApi:
    def __init__(self) -> None:
        self.entries_snapshot_kwargs = None

    def entries_snapshot(self, **kwargs):
        self.entries_snapshot_kwargs = kwargs
        return {
            "s3_bucket": kwargs["bucket"],
            "s3_key": kwargs["key"],
            "downloaded": True,
            "bytes_downloaded": 123,
        }


class FakeDagsterS3Resource:
    def __init__(self, client: "FakeS3Client") -> None:
        self._client = client

    def get_client(self) -> "FakeS3Client":
        return self._client


class FakeS3Client:
    def __init__(self, objects: dict[tuple[str, str], bytes] | None = None) -> None:
        self.objects = objects or {}
        self.get_calls: list[tuple[str, str]] = []

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.get_calls.append((Bucket, Key))
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}


class FakeObjectStore:
    def __init__(self) -> None:
        self.created_buckets: list[str] = []
        self.objects: dict[tuple[str, str], bytes] = {}

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects


def test_entries_snapshot_raw_asset_delegates_to_api_resource() -> None:
    api = FakeNorwayBrregApi()
    s3 = FakeDagsterS3Resource(FakeS3Client())

    result = norway_brreg_entries_snapshot_raw_s3(
        context=dg.build_asset_context(),
        norway_brreg_api=api,
        s3=s3,
    )

    assert api.entries_snapshot_kwargs is not None
    assert api.entries_snapshot_kwargs["s3"] is s3
    assert api.entries_snapshot_kwargs["bucket"] == NORWAY_BRREG_ENTITY_BUCKET
    assert api.entries_snapshot_kwargs["key"] == entries_snapshot_raw_object_key()
    assert callable(api.entries_snapshot_kwargs["log"])
    assert result.metadata == {
        "s3_bucket": NORWAY_BRREG_ENTITY_BUCKET,
        "s3_key": entries_snapshot_raw_object_key(),
        "downloaded": True,
        "bytes_downloaded": 123,
    }


def test_entities_snapshot_asset_writes_uniform_records_as_parquet_to_s3() -> None:
    s3_client = FakeS3Client(
        {
            (
                NORWAY_BRREG_ENTITY_BUCKET,
                entries_snapshot_raw_object_key(),
            ): gzip.compress(
                json.dumps(
                    [
                        {
                            "organisasjonsnummer": "923609016",
                            "navn": "EQUINOR ASA",
                            "sisteInnsendteAarsregnskap": "2024",
                            "_links": {
                                "self": {
                                    "href": "https://data.brreg.no/enhetsregisteret/api/enheter/923609016"
                                }
                            },
                        }
                    ]
                ).encode("utf-8")
            )
        }
    )
    object_store = FakeObjectStore()
    context = dg.build_asset_context()

    result = norway_brreg_entities_snapshot_s3(
        context=context,
        s3=FakeDagsterS3Resource(s3_client),
        object_store=object_store,
    )

    assert s3_client.get_calls == [
        (NORWAY_BRREG_ENTITY_BUCKET, entries_snapshot_raw_object_key())
    ]
    assert object_store.created_buckets == [NORWAY_BRREG_ENTITY_BUCKET]
    assert result.metadata["row_count"] == 1
    assert result.metadata["s3_bucket"] == NORWAY_BRREG_ENTITY_BUCKET
    assert result.metadata["s3_key"] == entity_snapshot_object_key()
    assert result.metadata["downloaded"] is True
    assert result.metadata["reused_existing_snapshot"] is False
    assert len(result.metadata["parquet_sha256"]) == 64

    uploaded = object_store.objects[
        (NORWAY_BRREG_ENTITY_BUCKET, result.metadata["s3_key"])
    ]
    frame = pl.read_parquet(BytesIO(uploaded))

    assert frame.select(
        [
            "org_number",
            "change_type",
            "source_change_type",
            "entity_url",
            "entity_json",
            "raw_update_json",
        ]
    ).to_dicts() == [
        {
            "org_number": "923609016",
            "change_type": "snapshot",
            "source_change_type": "snapshot",
            "entity_url": "https://data.brreg.no/enhetsregisteret/api/enheter/923609016",
            "entity_json": json.dumps(
                {
                    "organisasjonsnummer": "923609016",
                    "navn": "EQUINOR ASA",
                    "sisteInnsendteAarsregnskap": "2024",
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
            "raw_update_json": "",
        }
    ]


def test_entities_snapshot_asset_reuses_existing_stable_snapshot_without_api_download() -> None:
    object_store = FakeObjectStore()
    object_store.objects[
        (NORWAY_BRREG_ENTITY_BUCKET, entity_snapshot_object_key())
    ] = b"existing parquet"
    s3_client = FakeS3Client()

    result = norway_brreg_entities_snapshot_s3(
        context=dg.build_asset_context(),
        s3=FakeDagsterS3Resource(s3_client),
        object_store=object_store,
    )

    assert s3_client.get_calls == []
    assert object_store.created_buckets == []
    assert result.metadata["s3_bucket"] == NORWAY_BRREG_ENTITY_BUCKET
    assert result.metadata["s3_key"] == entity_snapshot_object_key()
    assert result.metadata["downloaded"] is False
    assert result.metadata["reused_existing_snapshot"] is True


def test_entities_snapshot_asset_refuses_empty_snapshot() -> None:
    s3_client = FakeS3Client(
        {
            (
                NORWAY_BRREG_ENTITY_BUCKET,
                entries_snapshot_raw_object_key(),
            ): gzip.compress(b"[]")
        }
    )
    object_store = FakeObjectStore()

    try:
        norway_brreg_entities_snapshot_s3(
            context=dg.build_asset_context(),
            s3=FakeDagsterS3Resource(s3_client),
            object_store=object_store,
        )
    except ValueError as exc:
        assert str(exc) == "Norway Brreg entity snapshot produced no rows"
    else:
        raise AssertionError("Expected empty snapshot to fail")

    assert object_store.objects == {}


def test_entity_update_window_covers_one_utc_day() -> None:
    assert entity_update_window("2026-06-28") == (
        "2026-06-28T00:00:00.000Z",
        "2026-06-28T23:59:59.999Z",
    )


def test_entity_updates_asset_writes_changed_records_as_daily_parquet_to_s3() -> None:
    class UpdateApi:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []
            self.kwargs = None

        def iter_updated_entities(self, *, start: str, end: str, **kwargs):
            self.calls.append((start, end))
            self.kwargs = kwargs
            yield {
                "org_number": "923609016",
                "change_type": "changed",
                "source_change_type": "Endring",
                "updated_at": "2026-06-28T12:34:56.000Z",
                "update_id": 123,
                "entity_url": "https://data.brreg.no/enhetsregisteret/api/enheter/923609016",
                "entity": {
                    "organisasjonsnummer": "923609016",
                    "navn": "EQUINOR ASA",
                },
                "raw_update": {
                    "organisasjonsnummer": "923609016",
                    "endringstype": "Endring",
                    "oppdateringsid": 123,
                },
            }

    api = UpdateApi()
    object_store = FakeObjectStore()
    context = dg.build_asset_context(partition_key="2026-06-28")

    result = norway_brreg_entity_updates_s3(
        context=context,
        norway_brreg_api=api,
        object_store=object_store,
    )

    assert api.calls == [
        ("2026-06-28T00:00:00.000Z", "2026-06-28T23:59:59.999Z")
    ]
    assert api.kwargs is not None
    assert callable(api.kwargs["log"])
    assert object_store.created_buckets == [NORWAY_BRREG_ENTITY_BUCKET]
    assert result.metadata["partition_date"] == "2026-06-28"
    assert result.metadata["row_count"] == 1
    assert result.metadata["s3_key"] == (
        "norway_brreg/entities/updates/date=2026-06-28/entities.parquet"
    )

    uploaded = object_store.objects[
        (NORWAY_BRREG_ENTITY_BUCKET, result.metadata["s3_key"])
    ]
    frame = pl.read_parquet(BytesIO(uploaded))

    assert frame.select(
        [
            "org_number",
            "change_type",
            "source_change_type",
            "updated_at",
            "update_id",
            "entity_json",
            "raw_update_json",
        ]
    ).to_dicts() == [
        {
            "org_number": "923609016",
            "change_type": "changed",
            "source_change_type": "Endring",
            "updated_at": "2026-06-28T12:34:56.000Z",
            "update_id": 123,
            "entity_json": json.dumps(
                {
                    "organisasjonsnummer": "923609016",
                    "navn": "EQUINOR ASA",
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
            "raw_update_json": json.dumps(
                {
                    "organisasjonsnummer": "923609016",
                    "endringstype": "Endring",
                    "oppdateringsid": 123,
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
        }
    ]


def test_entity_updates_asset_writes_empty_daily_parquet_when_no_updates() -> None:
    class EmptyUpdateApi:
        def iter_updated_entities(self, *, start: str, end: str, **_kwargs):
            return iter(())

    object_store = FakeObjectStore()

    result = norway_brreg_entity_updates_s3(
        context=dg.build_asset_context(partition_key="2026-06-29"),
        norway_brreg_api=EmptyUpdateApi(),
        object_store=object_store,
    )

    assert result.metadata["row_count"] == 0
    assert result.metadata["s3_key"] == (
        "norway_brreg/entities/updates/date=2026-06-29/entities.parquet"
    )
    uploaded = object_store.objects[
        (NORWAY_BRREG_ENTITY_BUCKET, result.metadata["s3_key"])
    ]
    frame = pl.read_parquet(BytesIO(uploaded))

    assert frame.height == 0
    assert frame.columns == [
        "org_number",
        "change_type",
        "source_change_type",
        "updated_at",
        "update_id",
        "entity_url",
        "entity_json",
        "raw_update_json",
    ]
