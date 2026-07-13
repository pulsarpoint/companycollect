from pathlib import Path

import pytest

import warc_index_builder.__main__ as command


def test_concurrent_build_for_same_crawl_and_selection_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = command.parse_options(
        ["--crawl", "CC-MAIN-2026-25", "--base", str(tmp_path)]
    )

    def already_locked(_file: object, _operation: int) -> None:
        raise BlockingIOError

    monkeypatch.setattr(command.fcntl, "flock", already_locked)
    monkeypatch.setattr(
        command,
        "_run_locked",
        lambda _options: pytest.fail("locked build must not start"),
    )

    with pytest.raises(RuntimeError, match="another index build is active"):
        command.run(options)
