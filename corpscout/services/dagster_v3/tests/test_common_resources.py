import importlib
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb
import pytest
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.definitions import defs as load_project_defs


class ListingPaginator:
    def __init__(self, pages: list[dict[str, object]]) -> None:
        self.pages = pages
        self.yielded_page_count = 0

    def paginate(self, *, Bucket: str, Prefix: str) -> Iterator[dict[str, object]]:
        assert Bucket == "source-test"
        assert Prefix == "raw/partition=2026/"
        for page in self.pages:
            self.yielded_page_count += 1
            yield page


class ListingClient:
    def __init__(self, pages: list[dict[str, object]]) -> None:
        self.paginator = ListingPaginator(pages)

    def get_paginator(self, operation_name: str) -> ListingPaginator:
        assert operation_name == "list_objects_v2"
        return self.paginator


def test_shared_resources_live_in_common() -> None:
    module = importlib.import_module("dagster_v3.defs.common.resources")
    assert hasattr(module, "ObjectStoreResource")


def test_object_store_resource_resolves_env_vars_before_creating_boto_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resources = importlib.import_module("dagster_v3.defs.common.resources")
    calls: dict[str, object] = {}
    fake_client = object()

    def fake_boto_client(service_name: str, **kwargs: object) -> object:
        calls["service_name"] = service_name
        calls.update(kwargs)
        return fake_client

    monkeypatch.setenv("CORPSCOUT_S3_ENDPOINT", "http://s3.test:9000")
    monkeypatch.setenv("CORPSCOUT_S3_ACCESS_KEY", "test-access-key")
    monkeypatch.setenv("CORPSCOUT_S3_SECRET_KEY", "test-secret-key")
    monkeypatch.setattr(resources.boto3, "client", fake_boto_client)

    resource = resources.ObjectStoreResource()

    assert resource.client() is fake_client
    assert calls["service_name"] == "s3"
    assert calls["endpoint_url"] == "http://s3.test:9000"
    assert calls["aws_access_key_id"] == "test-access-key"
    assert calls["aws_secret_access_key"] == "test-secret-key"
    client_config = calls["config"]
    assert client_config.connect_timeout == 5
    assert client_config.read_timeout == 30
    assert client_config.retries == {
        "mode": "standard",
        "total_max_attempts": 2,
    }


def test_object_store_exists_returns_false_when_bucket_is_missing() -> None:
    resources = importlib.import_module("dagster_v3.defs.common.resources")

    class FakeClient:
        def head_object(self, Bucket: str, Key: str) -> None:
            assert Bucket == "missing-bucket"
            assert Key == "snapshot.parquet"
            raise ClientError(
                {"Error": {"Code": "NoSuchBucket"}},
                operation_name="HeadObject",
            )

    resource = resources.ObjectStoreResource(s3_client=FakeClient())

    assert resource.exists("snapshot.parquet", bucket="missing-bucket") is False


def test_object_store_reads_exact_object_size_without_listing() -> None:
    resources = importlib.import_module("dagster_v3.defs.common.resources")

    class FakeClient:
        def head_object(self, Bucket: str, Key: str) -> dict[str, int]:
            assert Bucket == "source-sweden-company"
            assert Key == "raw/source.zip"
            return {"ContentLength": 12_345}

    resource = resources.ObjectStoreResource(s3_client=FakeClient())

    assert (
        resource.object_size("raw/source.zip", bucket="source-sweden-company")
        == 12_345
    )


def test_object_store_limits_default_multipart_upload_concurrency(
    tmp_path: Path,
) -> None:
    resources = importlib.import_module("dagster_v3.defs.common.resources")

    class FakeClient:
        transfer_config: TransferConfig | None = None

        def upload_file(
            self,
            Filename: str,
            Bucket: str,
            Key: str,
            Config: TransferConfig | None = None,
        ) -> None:
            assert Filename == str(tmp_path / "source.zip")
            assert Bucket == "source-sweden-company"
            assert Key == "raw/source.zip"
            self.transfer_config = Config

    client = FakeClient()
    resource = resources.ObjectStoreResource(s3_client=client)

    resource.upload_file(
        "raw/source.zip",
        tmp_path / "source.zip",
        bucket="source-sweden-company",
    )

    assert client.transfer_config is not None
    assert client.transfer_config.max_concurrency == 2


def test_object_store_accepts_explicit_transfer_config(tmp_path: Path) -> None:
    resources = importlib.import_module("dagster_v3.defs.common.resources")

    class FakeClient:
        transfer_config: TransferConfig | None = None

        def upload_file(
            self,
            Filename: str,
            Bucket: str,
            Key: str,
            Config: TransferConfig | None = None,
        ) -> None:
            self.transfer_config = Config

    client = FakeClient()
    resource = resources.ObjectStoreResource(s3_client=client)
    transfer_config = TransferConfig(max_concurrency=1)

    resource.upload_file(
        "raw/source.zip",
        tmp_path / "source.zip",
        transfer_config=transfer_config,
    )

    assert client.transfer_config is transfer_config


def test_object_store_lists_bounded_prefix() -> None:
    resources = importlib.import_module("dagster_v3.defs.common.resources")
    client = ListingClient(
        [
            {
                "Contents": [{"Key": "raw/partition=2026/part-1.json"}],
                "IsTruncated": False,
            }
        ]
    )
    resource = resources.ObjectStoreResource(
        bucket="source-test",
        s3_client=client,
    )

    assert resource.list_keys("raw/partition=2026/") == [
        "raw/partition=2026/part-1.json"
    ]


def test_object_store_rejects_root_listing_before_calling_s3() -> None:
    resources = importlib.import_module("dagster_v3.defs.common.resources")

    class UnexpectedClient:
        def get_paginator(self, operation_name: str) -> object:
            raise AssertionError(f"Unexpected paginator request: {operation_name}")

    resource = resources.ObjectStoreResource(s3_client=UnexpectedClient())

    with pytest.raises(ValueError, match="prefix must not be blank"):
        resource.list_keys("")


def test_object_store_stops_when_key_limit_is_exceeded() -> None:
    resources = importlib.import_module("dagster_v3.defs.common.resources")
    client = ListingClient(
        [
            {
                "Contents": [
                    {"Key": "raw/partition=2026/part-1.json"},
                    {"Key": "raw/partition=2026/part-2.json"},
                    {"Key": "raw/partition=2026/part-3.json"},
                ],
                "IsTruncated": True,
            },
            {"Contents": [], "IsTruncated": False},
        ]
    )
    resource = resources.ObjectStoreResource(
        bucket="source-test",
        s3_client=client,
        list_max_keys=2,
    )

    with pytest.raises(
        resources.ObjectStoreListingLimitError,
        match="key limit exceeded.*keys=3.*limit=2",
    ):
        resource.list_keys("raw/partition=2026/")

    assert client.paginator.yielded_page_count == 1


def test_object_store_stops_before_requesting_page_beyond_limit() -> None:
    resources = importlib.import_module("dagster_v3.defs.common.resources")
    client = ListingClient(
        [
            {
                "Contents": [{"Key": "raw/partition=2026/part-1.json"}],
                "IsTruncated": True,
            },
            {
                "Contents": [{"Key": "raw/partition=2026/part-2.json"}],
                "IsTruncated": False,
            },
        ]
    )
    resource = resources.ObjectStoreResource(
        bucket="source-test",
        s3_client=client,
        list_max_pages=1,
    )

    with pytest.raises(
        resources.ObjectStoreListingLimitError,
        match="page limit reached.*pages=1.*limit=1",
    ):
        resource.list_keys("raw/partition=2026/")

    assert client.paginator.yielded_page_count == 1


def test_object_store_stops_when_elapsed_limit_is_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resources = importlib.import_module("dagster_v3.defs.common.resources")
    clock = iter([0.0, 61.0, 61.0])
    monkeypatch.setattr(resources, "monotonic", lambda: next(clock))
    resource = resources.ObjectStoreResource(
        bucket="source-test",
        s3_client=ListingClient(
            [
                {
                    "Contents": [{"Key": "raw/partition=2026/part-1.json"}],
                    "IsTruncated": False,
                }
            ]
        ),
    )

    with pytest.raises(
        resources.ObjectStoreListingLimitError,
        match="elapsed-time limit exceeded.*elapsed_seconds=61.000.*limit=60.000",
    ):
        resource.list_keys("raw/partition=2026/")


def test_object_store_logs_slow_listing_telemetry(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    resources = importlib.import_module("dagster_v3.defs.common.resources")
    clock = iter([0.0, 3.0, 3.0])
    monkeypatch.setattr(resources, "monotonic", lambda: next(clock))
    resource = resources.ObjectStoreResource(
        bucket="source-test",
        s3_client=ListingClient(
            [
                {
                    "Contents": [{"Key": "raw/partition=2026/part-1.json"}],
                    "IsTruncated": False,
                }
            ]
        ),
    )

    with caplog.at_level(logging.WARNING, logger=resources.__name__):
        resource.list_keys("raw/partition=2026/")

    record = caplog.records[-1]
    assert record.getMessage().startswith("Slow object-store listing")
    assert record.object_store_bucket == "source-test"
    assert record.object_store_prefix == "raw/partition=2026/"
    assert record.object_store_page_count == 1
    assert record.object_store_key_count == 1
    assert record.object_store_elapsed_seconds == 3.0
    assert record.object_store_limit_exceeded is False


def test_project_clickhouse_resource_uses_official_dagster_resource(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLICKHOUSE_NATIVE_PORT", "9443")
    monkeypatch.setenv("CLICKHOUSE_SECURE", "on")

    repository = load_project_defs().get_repository_def()
    resource_def = repository.get_top_level_resources()["clickhouse"]
    resource = resource_def.resource_fn.__self__

    assert resource_def.configurable_resource_cls is ClickhouseResource
    assert isinstance(resource, ClickhouseResource)
    assert resource.port == 9443
    assert resource.secure is True


def test_duckdb_resource_factory_uses_generic_runtime_env(
    monkeypatch,
    tmp_path: Path,
) -> None:
    helpers = importlib.import_module("dagster_v3.defs.common.duckdb_resources")
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "8GiB")
    monkeypatch.setenv("DUCKDB_THREADS", "2")
    monkeypatch.setenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", "150GiB")
    monkeypatch.setenv("DUCKDB_TEMP_DIRECTORY", str(tmp_path / "duckdb-temp"))

    resource = helpers.duckdb_resource(tmp_path / "source.duckdb")

    assert isinstance(resource, DuckDBResource)
    assert helpers.duckdb_database_path(resource) == tmp_path / "source.duckdb"
    assert resource.connection_config["memory_limit"] == "8GiB"
    assert resource.connection_config["threads"] == 2
    assert resource.connection_config["max_temp_directory_size"] == "150GiB"
    assert resource.connection_config["temp_directory"] == str(tmp_path / "duckdb-temp")
    assert resource.connection_config["preserve_insertion_order"] is False


def test_duckdb_resource_reuses_same_object_for_same_normalized_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    helpers = importlib.import_module("dagster_v3.defs.common.duckdb_resources")
    monkeypatch.chdir(tmp_path)

    relative = helpers.duckdb_resource("data/source.duckdb")
    absolute = helpers.duckdb_resource(tmp_path / "data" / "source.duckdb")

    assert relative is absolute
    assert helpers.duckdb_database_path(relative) == tmp_path / "data" / "source.duckdb"


def test_duckdb_resource_rejects_invalid_threads_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    helpers = importlib.import_module("dagster_v3.defs.common.duckdb_resources")
    monkeypatch.setenv("DUCKDB_THREADS", "4y")

    with pytest.raises(ValueError, match="DUCKDB_THREADS must be a positive integer"):
        helpers.duckdb_resource(tmp_path / "source.duckdb")


def test_read_only_duckdb_connection_rejects_writes(tmp_path: Path) -> None:
    helpers = importlib.import_module("dagster_v3.defs.common.duckdb_resources")
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create table companies (business_id text)")
        connection.execute("insert into companies values ('a')")

    resource = helpers.duckdb_resource(database_path)

    with helpers.read_only_duckdb_connection(resource) as connection:
        assert (
            connection.execute("select business_id from companies").fetchone()[0] == "a"
        )
        with pytest.raises(Exception):
            connection.execute("insert into companies values ('b')")


def test_safe_duckdb_connection_closes_underlying_connection_on_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    helpers = importlib.import_module("dagster_v3.defs.common.duckdb_resources")

    class RecordingConnection:
        closed = False

        def close(self) -> None:
            self.closed = True

    fake_connection = RecordingConnection()

    @contextmanager
    def fake_get_connection(_self: DuckDBResource) -> Iterator[Any]:
        yield fake_connection
        fake_connection.close()

    monkeypatch.setattr(
        DuckDBResource,
        "get_connection",
        fake_get_connection,
    )
    resource = helpers.duckdb_resource(tmp_path / "source.duckdb")

    with pytest.raises(RuntimeError, match="write failed"):
        with helpers.safe_duckdb_connection(resource):
            raise RuntimeError("write failed")

    assert fake_connection.closed is True


def test_finland_ytj_resources_module_exposes_api_resource() -> None:
    resources = importlib.import_module("dagster_v3.defs.finland_ytj.resources")

    assert resources.YtjApiResource.__name__ == "YtjApiResource"
