import importlib
from pathlib import Path

import dagster as dg

from dagster_corpscout.registry import source_bundles, source_modules
from dagster_corpscout.source_bundle import SourceBundle

LAYER_VOCABULARY = {"external", "raw", "parsed", "reference", "normalized", "mapping", "serving"}


def test_registered_source_packages_follow_layout_convention():
    seen_source_names = set()
    seen_asset_prefixes = set()

    for module_name in source_modules:
        module = importlib.import_module(module_name)
        source_bundle = module.source_bundle

        assert isinstance(source_bundle, SourceBundle)
        assert source_bundle in source_bundles
        assert source_bundle.source_name not in seen_source_names
        assert source_bundle.asset_key_prefix not in seen_asset_prefixes
        assert len(source_bundle.asset_key_prefix) == 3
        assert source_bundle.asset_key_prefix[0] == "sources"

        seen_source_names.add(source_bundle.source_name)
        seen_asset_prefixes.add(source_bundle.asset_key_prefix)

        country, source = source_bundle.asset_key_prefix[1:]
        assert module_name.endswith(f".{country}.{source}")
        assert module.spec.GROUP_NAME == f"source_{country}_{source}"
        assert module.spec.TAGS == {
            "country": country,
            "source": source,
            "source_name": source_bundle.source_name,
        }

        package_dir = Path(module.__file__).parent
        for relative_path in [
            "spec.py",
            "jobs.py",
            "schedules.py",
            "assets/__init__.py",
            "assets/external.py",
            "assets/raw.py",
        ]:
            assert (package_dir / relative_path).is_file(), f"{module_name} missing {relative_path}"


def test_all_assets_declare_a_layer_from_the_vocabulary():
    from dagster_corpscout.definitions import defs

    graph = defs.resolve_asset_graph()
    for node in graph.asset_nodes:
        assert node.tags.get("layer") in LAYER_VOCABULARY, node.key


def test_all_schedules_default_to_stopped():
    from dagster_corpscout.definitions import defs

    for schedule in defs.schedules:
        assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED, schedule.name
