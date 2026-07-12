import pytest

from warc_index_builder.__main__ import main


def test_help_exits_successfully(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(["--help"])

    assert exit_info.value.code == 0
    assert "WARC-oriented catalog" in capsys.readouterr().out
