import json
import subprocess
import sys
from pathlib import Path

import pytest

import warc_index_builder.__main__ as command
from warc_index_builder.__main__ import main, parse_options
from warc_index_builder.events import binary_size, emit_event


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


def test_build_creates_only_local_lifecycle_paths(tmp_path: Path) -> None:
    assert main(["--base", str(tmp_path), "--crawl", "CC-MAIN-2026-25"]) == 0

    catalog_directory = tmp_path / "CC-MAIN-2026-25/catalog/pages25"
    assert (catalog_directory / "build.lock").is_file()
    assert (catalog_directory / ".build").is_dir()
    assert (catalog_directory / "catalog.duckdb").exists() is False


def test_check_does_not_create_catalog_paths(tmp_path: Path) -> None:
    base = tmp_path / "unused"

    assert main(["--base", str(base), "--crawl", "CC-MAIN-2026-25", "--check"]) == 0

    assert base.exists() is False


def test_rebuild_removes_only_staging_and_preserves_final_catalog(tmp_path: Path) -> None:
    catalog_directory = tmp_path / "CC-MAIN-2026-25/catalog/pages25"
    build_directory = catalog_directory / ".build"
    build_directory.mkdir(parents=True)
    (build_directory / "stale.partial").write_text("stale")
    final_catalog = catalog_directory / "catalog.duckdb"
    final_catalog.write_bytes(b"existing catalog")
    sibling = catalog_directory / "keep.me"
    sibling.write_text("keep")

    assert main(["--base", str(tmp_path), "--crawl", "CC-MAIN-2026-25", "--rebuild"]) == 0

    assert build_directory.is_dir()
    assert list(build_directory.iterdir()) == []
    assert final_catalog.read_bytes() == b"existing catalog"
    assert sibling.read_text() == "keep"


def test_rebuild_rejects_symlinked_build_directory(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    catalog_directory = tmp_path / "CC-MAIN-2026-25/catalog/pages25"
    catalog_directory.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel"
    sentinel.write_text("keep")
    (catalog_directory / ".build").symlink_to(outside, target_is_directory=True)

    exit_code = main(["--base", str(tmp_path), "--crawl", "CC-MAIN-2026-25", "--rebuild"])

    assert exit_code == 1
    assert sentinel.read_text() == "keep"
    event = json.loads(capsys.readouterr().err)
    assert event["error_type"] == "ValueError"
    assert "must not be a symlink" in event["error"]


def test_another_process_holding_lock_blocks_builder(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    catalog_directory = tmp_path / "CC-MAIN-2026-25/catalog/pages25"
    lock_holder = """
import sys
from pathlib import Path
from warc_index_builder.catalog import catalog_build_lock

with catalog_build_lock(Path(sys.argv[1])):
    print("locked", flush=True)
    sys.stdin.readline()
"""
    process = subprocess.Popen(
        [sys.executable, "-c", lock_holder, str(catalog_directory)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == "locked"

        exit_code = main(["--base", str(tmp_path), "--crawl", "CC-MAIN-2026-25"])

        assert exit_code == 1
        event = json.loads(capsys.readouterr().err)
        assert event["error_type"] == "CatalogBuildLocked"
        assert "another builder holds" in event["error"]
    finally:
        if process.stdin is not None:
            process.stdin.close()
        process.wait(timeout=5)

    assert main(["--base", str(tmp_path), "--crawl", "CC-MAIN-2026-25"]) == 0


def test_event_is_structured_json(capsys: pytest.CaptureFixture[str]) -> None:
    emit_event("candidate shard ready", shard=3, rows=120, source_size=binary_size(271_088_921))

    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["time"].endswith("Z")
    assert event["level"] == "INFO"
    assert event["msg"] == "candidate shard ready"
    assert event["shard"] == 3
    assert event["rows"] == 120
    assert event["source_size"] == "258.5 MiB"


def test_binary_size_rejects_negative_bytes() -> None:
    with pytest.raises(ValueError, match="must not be negative"):
        binary_size(-1)


def test_reserved_event_fields_cannot_be_overwritten() -> None:
    with pytest.raises(ValueError, match="reserved"):
        emit_event("invalid", msg="replacement")


def test_main_logs_runtime_failure_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def fail(_options: command.CommandOptions) -> int:
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(command, "run", fail)

    exit_code = main(["--base", str(tmp_path), "--crawl", "CC-MAIN-2026-25"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    lines = captured.err.splitlines()
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["level"] == "ERROR"
    assert event["msg"] == "catalog build failed"
    assert event["crawl"] == "CC-MAIN-2026-25"
    assert event["selection"] == "pages25"
    assert event["error_type"] == "RuntimeError"
    assert event["error"] == "simulated failure"
