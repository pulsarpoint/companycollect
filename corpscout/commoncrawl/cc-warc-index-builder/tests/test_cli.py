from pathlib import Path

import pytest

from warc_index_builder.__main__ import main, parse_options


def test_defaults_resolve_from_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("OUT_BASE_DIR", raising=False)

    options = parse_options(["--crawl", "CC-MAIN-2026-25"])

    assert options.base == tmp_path / "data"
    assert options.pages_per_domain == 25
    assert options.selection_name == "pages25"
    assert options.catalog_directory == tmp_path / "data/CC-MAIN-2026-25/catalog/pages25"
    assert options.catalog_path == options.catalog_directory / "catalog.duckdb"
    assert options.threads is None
    assert options.memory_limit is None
    assert options.temp_dir is None
    assert options.warc_size_concurrency == 64
    assert options.http_attempts == 5
    assert options.rebuild is False
    assert options.check is False


def test_out_base_dir_supplies_default_base(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    environment_base = tmp_path / "environment-base"
    monkeypatch.setenv("OUT_BASE_DIR", str(environment_base))

    options = parse_options(["--crawl", "CC-MAIN-2016-22"])

    assert options.base == environment_base


def test_explicit_values_override_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OUT_BASE_DIR", str(tmp_path / "ignored"))
    explicit_base = tmp_path / "explicit"
    temp_dir = tmp_path / "fast-temp"

    options = parse_options(
        [
            "--base",
            str(explicit_base),
            "--crawl",
            "CC-MAIN-2013-20",
            "--pages-per-domain",
            "1",
            "--threads",
            "8",
            "--memory-limit",
            "48GB",
            "--temp-dir",
            str(temp_dir),
            "--warc-size-concurrency",
            "128",
            "--http-attempts",
            "7",
            "--check",
        ]
    )

    assert options.base == explicit_base
    assert options.catalog_directory == explicit_base / "CC-MAIN-2013-20/catalog/pages1"
    assert options.threads == 8
    assert options.memory_limit == "48GB"
    assert options.temp_dir == temp_dir
    assert options.warc_size_concurrency == 128
    assert options.http_attempts == 7
    assert options.check is True


@pytest.mark.parametrize(
    "crawl",
    ["", "CC-MAIN-2026-5", "CC-MAIN-26-25", "cc-main-2026-25", "../CC-MAIN-2026-25"],
)
def test_invalid_crawl_is_rejected(crawl: str) -> None:
    with pytest.raises(SystemExit) as exit_info:
        parse_options(["--crawl", crawl])

    assert exit_info.value.code == 2


@pytest.mark.parametrize("pages", ["0", "65536", "not-a-number"])
def test_invalid_pages_per_domain_is_rejected(pages: str) -> None:
    with pytest.raises(SystemExit) as exit_info:
        parse_options(["--crawl", "CC-MAIN-2026-25", "--pages-per-domain", pages])

    assert exit_info.value.code == 2


@pytest.mark.parametrize("flag", ["--threads", "--warc-size-concurrency", "--http-attempts"])
@pytest.mark.parametrize("value", ["0", "-1", "invalid"])
def test_positive_integer_flags_are_validated(flag: str, value: str) -> None:
    with pytest.raises(SystemExit) as exit_info:
        parse_options(["--crawl", "CC-MAIN-2026-25", flag, value])

    assert exit_info.value.code == 2


@pytest.mark.parametrize("flag", ["--base", "--memory-limit", "--temp-dir"])
def test_text_flags_reject_empty_values(flag: str) -> None:
    with pytest.raises(SystemExit) as exit_info:
        parse_options(["--crawl", "CC-MAIN-2026-25", flag, ""])

    assert exit_info.value.code == 2


def test_check_and_rebuild_are_mutually_exclusive() -> None:
    with pytest.raises(SystemExit) as exit_info:
        parse_options(["--crawl", "CC-MAIN-2026-25", "--check", "--rebuild"])

    assert exit_info.value.code == 2


def test_existing_symlink_cannot_escape_base(tmp_path: Path) -> None:
    base = tmp_path / "base"
    outside = tmp_path / "outside"
    base.mkdir()
    outside.mkdir()
    (base / "CC-MAIN-2026-25").symlink_to(outside, target_is_directory=True)

    with pytest.raises(SystemExit) as exit_info:
        parse_options(["--base", str(base), "--crawl", "CC-MAIN-2026-25"])

    assert exit_info.value.code == 2


def test_parsing_does_not_create_catalog_directories(tmp_path: Path) -> None:
    base = tmp_path / "not-created"

    options = parse_options(["--base", str(base), "--crawl", "CC-MAIN-2026-25"])

    assert options.catalog_directory == base / "CC-MAIN-2026-25/catalog/pages25"
    assert base.exists() is False


def test_main_accepts_valid_options(tmp_path: Path) -> None:
    assert main(["--base", str(tmp_path), "--crawl", "CC-MAIN-2026-25"]) == 0
