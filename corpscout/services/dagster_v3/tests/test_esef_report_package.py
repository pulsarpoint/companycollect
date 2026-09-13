"""Opening an archived ESEF report package without arelle: member safety
limits and report-member selection, moved out of segment_parser."""

import zipfile
from pathlib import Path

import pytest

from dagster_v3.defs.esef_filings import segment_parser
from dagster_v3.defs.esef_filings.report_package import (
    INLINE_XBRL_NAMESPACE_MARKERS,
    contains_inline_xbrl_namespace,
    extract_report_package,
    report_paths_for,
)

_IX_REPORT = (
    '<html xmlns="http://www.w3.org/1999/xhtml" '
    'xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"><body><p>x</p></body></html>'
)
_PLAIN_HTML = '<html xmlns="http://www.w3.org/1999/xhtml"><body><p>x</p></body></html>'


def _package(path: Path, members: dict[str, str]) -> Path:
    with zipfile.ZipFile(path, "w") as package:
        for name, text in members.items():
            package.writestr(name, text)
    return path


def test_selects_report_members_under_a_reports_folder(tmp_path: Path) -> None:
    package = _package(
        tmp_path / "p.zip",
        {
            "pkg/META-INF/taxonomyPackage.xml": "<x/>",
            "pkg/reports/b.xhtml": _IX_REPORT,
            "pkg/reports/a.html": _IX_REPORT,
            "pkg/reports/styles.css": "p{}",
        },
    )
    temp_dir = tmp_path / "out"
    temp_dir.mkdir()

    members, repaired = extract_report_package(package, temp_dir)

    assert members == ["pkg/reports/a.html", "pkg/reports/b.xhtml"]
    assert repaired == 0
    paths = report_paths_for(temp_dir, members)
    assert paths["pkg/reports/b.xhtml"] == temp_dir / "pkg" / "reports" / "b.xhtml"
    assert paths["pkg/reports/b.xhtml"].read_text() == _IX_REPORT


def test_falls_back_to_nonstandard_members_with_the_inline_xbrl_namespace(
    tmp_path: Path,
) -> None:
    package = _package(
        tmp_path / "p.zip",
        {"report.xhtml": _IX_REPORT, "readme.html": _PLAIN_HTML},
    )
    temp_dir = tmp_path / "out"
    temp_dir.mkdir()

    members, _ = extract_report_package(package, temp_dir)

    assert members == ["report.xhtml"]


def test_repairs_backslash_member_names(tmp_path: Path) -> None:
    package = _package(tmp_path / "p.zip", {"pkg\\reports\\a.xhtml": _IX_REPORT})
    temp_dir = tmp_path / "out"
    temp_dir.mkdir()

    members, repaired = extract_report_package(package, temp_dir)

    assert members == ["pkg/reports/a.xhtml"]
    assert repaired == 1


def test_rejects_unsafe_paths_and_empty_packages(tmp_path: Path) -> None:
    unsafe = _package(tmp_path / "unsafe.zip", {"../evil.xhtml": _IX_REPORT})
    empty = _package(tmp_path / "empty.zip", {"pkg/META-INF/x.xml": "<x/>"})
    for package in (unsafe, empty):
        temp_dir = tmp_path / f"out-{package.stem}"
        temp_dir.mkdir()
        with pytest.raises(ValueError):
            extract_report_package(package, temp_dir)


def test_namespace_marker_scan(tmp_path: Path) -> None:
    with_marker = tmp_path / "a.xhtml"
    with_marker.write_text(_IX_REPORT)
    without = tmp_path / "b.xhtml"
    without.write_text(_PLAIN_HTML)
    assert contains_inline_xbrl_namespace(with_marker)
    assert not contains_inline_xbrl_namespace(without)
    assert b"http://www.xbrl.org/2013/inlineXBRL" in INLINE_XBRL_NAMESPACE_MARKERS


def test_segment_parser_keeps_its_names() -> None:
    assert segment_parser._extract_report_package is extract_report_package
    assert segment_parser._contains_inline_xbrl_namespace is contains_inline_xbrl_namespace
    assert segment_parser.MAX_PACKAGE_FILES == 10_000
