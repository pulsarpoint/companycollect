import importlib
from pathlib import Path

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
    assert (tmp_path / "duckdb-temp").is_dir()


def test_old_finland_ytj_resources_module_is_gone() -> None:
    try:
        importlib.import_module("dagster_v3.defs.finland_ytj.resources")
    except ModuleNotFoundError:
        return
    raise AssertionError("finland_ytj.resources should no longer exist")
