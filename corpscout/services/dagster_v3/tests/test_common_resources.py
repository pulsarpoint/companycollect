import importlib
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb
import pytest
from botocore.exceptions import ClientError
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.definitions import defs as load_project_defs


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
