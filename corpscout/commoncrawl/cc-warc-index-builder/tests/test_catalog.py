from pathlib import Path

import pytest

from warc_index_builder.catalog import prepare_build_directory, require_path_within


def test_require_path_within_rejects_outside_target(tmp_path: Path) -> None:
    base = tmp_path / "base"
    base.mkdir()

    with pytest.raises(ValueError, match="escapes base"):
        require_path_within(base, tmp_path / "outside")


def test_prepare_build_directory_rejects_file(tmp_path: Path) -> None:
    catalog_directory = tmp_path / "CC-MAIN-2026-25/catalog/pages25"
    catalog_directory.mkdir(parents=True)
    build_path = catalog_directory / ".build"
    build_path.write_text("not a directory")

    with pytest.raises(ValueError, match="not a directory"):
        prepare_build_directory(tmp_path, catalog_directory, rebuild=True)

    assert build_path.read_text() == "not a directory"
