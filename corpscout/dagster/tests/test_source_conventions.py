import importlib
from pathlib import Path

from dagster_corpscout.registry import source_bundles, source_modules
from dagster_corpscout.source_bundle import SourceBundle


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


def test_source_scaffold_creates_expected_package_layout(tmp_path):
    from dagster_corpscout.source_scaffold import scaffold_source

    sources_root = tmp_path / "sources"
    package_dir = scaffold_source(sources_root, country="serbia", source="apr")

    assert package_dir == sources_root / "serbia" / "apr"
    for relative_path in [
        "__init__.py",
        "spec.py",
        "jobs.py",
        "schedules.py",
        "assets/__init__.py",
        "assets/external.py",
        "assets/raw.py",
    ]:
        assert (package_dir / relative_path).is_file()

    spec_text = (package_dir / "spec.py").read_text()
    assert 'SOURCE_NAME = "serbia_apr"' in spec_text
    assert 'ASSET_KEY_PREFIX = ["sources", COUNTRY, SOURCE_SLUG]' in spec_text
    assert 'GROUP_NAME = f"source_{COUNTRY}_{SOURCE_SLUG}"' in spec_text
    assert '"source_name": SOURCE_NAME' in spec_text

    init_text = (package_dir / "__init__.py").read_text()
    assert "SourceBundle(" in init_text
    assert "source_name=spec.SOURCE_NAME" in init_text
    assert "asset_key_prefix=tuple(spec.ASSET_KEY_PREFIX)" in init_text
