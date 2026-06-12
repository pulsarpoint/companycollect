import pytest

from dagster_corpscout.source_scaffold import scaffold_source


def test_snapshot_archetype_creates_v1_compatible_layout(tmp_path):
    package_dir = scaffold_source(tmp_path / "sources", country="serbia", source="apr")

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
    assert not (package_dir / "partitions.py").exists()
    assert 'GROUP_NAME = f"source_{COUNTRY}_{SOURCE_SLUG}"' in (package_dir / "spec.py").read_text()


def test_window_archetype_adds_partitions_and_parsed_asset(tmp_path):
    package_dir = scaffold_source(
        tmp_path / "sources", country="france", source="inpi", archetype="window"
    )

    assert (package_dir / "partitions.py").is_file()
    assert (package_dir / "assets/parsed.py").is_file()
    partitions_text = (package_dir / "partitions.py").read_text()
    assert "MonthlyPartitionsDefinition" in partitions_text
    raw_text = (package_dir / "assets/raw.py").read_text()
    assert "partitions_def" in raw_text
    parsed_text = (package_dir / "assets/parsed.py").read_text()
    assert "AutomationCondition.eager()" in parsed_text


def test_unknown_archetype_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        scaffold_source(tmp_path / "sources", country="x", source="y", archetype="entity")
