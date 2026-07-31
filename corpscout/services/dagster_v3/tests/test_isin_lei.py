"""GLEIF's ISIN-to-LEI mapping: choosing the right file, and reading it."""

import io
import zipfile
from pathlib import Path

import pytest

from dagster_v3.defs.isin_lei.source import choose_latest_file, extract_isin_lei_csv

LISTING = {
    "data": [
        {"attributes": {"downloadLink": "https://gleif/one"}},
        {"attributes": {"downloadLink": "https://gleif/two"}},
        {"attributes": {"downloadLink": "https://gleif/three"}},
    ]
}


def test_picks_the_newest_published_file():
    """By the timestamp GLEIF puts in the filename — the listing order is not
    documented as sorted, and a stale mapping would look perfectly fine."""
    names = {
        "https://gleif/one": "isin-lei-20260729T071511.zip",
        "https://gleif/two": "isin-lei-20260731T071511.zip",
        "https://gleif/three": "isin-lei-20260730T071511.zip",
    }
    assert choose_latest_file(LISTING, file_names=names).stamp == "20260731T071511"


def test_ignores_a_file_whose_name_carries_no_date():
    names = {
        "https://gleif/one": "isin-lei-20260729T071511.zip",
        "https://gleif/two": "readme.txt",
        "https://gleif/three": "",
    }
    assert choose_latest_file(LISTING, file_names=names).stamp == "20260729T071511"


def test_raises_when_nothing_is_datable():
    """Rather than guessing. A silently stale mapping is worse than a failure."""
    with pytest.raises(ValueError, match="no datable"):
        choose_latest_file(LISTING, file_names={})


def test_extracts_the_single_csv(tmp_path: Path):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("lei-isin-20260731T071511.csv", b"LEI,ISIN\nABC,US1234567890\n")
    out = extract_isin_lei_csv(buffer.getvalue(), tmp_path / "out.csv")
    assert out.read_text().splitlines()[0] == "LEI,ISIN"


def test_refuses_an_archive_that_is_not_one_csv(tmp_path: Path):
    """Two CSVs means the layout changed, and picking one would be a guess."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("a.csv", b"LEI,ISIN\n")
        archive.writestr("b.csv", b"LEI,ISIN\n")
    with pytest.raises(ValueError, match="exactly one CSV"):
        extract_isin_lei_csv(buffer.getvalue(), tmp_path / "out.csv")
