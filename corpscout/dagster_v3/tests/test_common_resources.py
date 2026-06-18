import importlib


def test_shared_resources_live_in_common() -> None:
    module = importlib.import_module("dagster_v3.defs.common.resources")
    assert hasattr(module, "LocalDuckDBResource")
    assert hasattr(module, "ObjectStoreResource")


def test_old_finland_ytj_resources_module_is_gone() -> None:
    try:
        importlib.import_module("dagster_v3.defs.finland_ytj.resources")
    except ModuleNotFoundError:
        return
    raise AssertionError("finland_ytj.resources should no longer exist")
