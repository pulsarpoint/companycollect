import importlib
from pathlib import Path

import duckdb
import pytest
from dagster_duckdb import DuckDBResource


def test_shared_resources_live_in_common() -> None:
    module = importlib.import_module("dagster_v3.defs.common.resources")
    assert hasattr(module, "ObjectStoreResource")


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
    assert resource.connection_config["threads"] == "2"
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


def test_read_only_duckdb_connection_rejects_writes(tmp_path: Path) -> None:
    helpers = importlib.import_module("dagster_v3.defs.common.duckdb_resources")
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create table companies (business_id text)")
        connection.execute("insert into companies values ('a')")

    resource = helpers.duckdb_resource(database_path)

    with helpers.read_only_duckdb_connection(resource) as connection:
        assert connection.execute("select business_id from companies").fetchone()[0] == "a"
        with pytest.raises(Exception):
            connection.execute("insert into companies values ('b')")


def test_old_finland_ytj_resources_module_is_gone() -> None:
    try:
        importlib.import_module("dagster_v3.defs.finland_ytj.resources")
    except ModuleNotFoundError:
        return
    raise AssertionError("finland_ytj.resources should no longer exist")
