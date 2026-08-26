import asyncio
from ipaddress import ip_address
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from ratsit_server import main as server


class FakeBrowser:
    def __init__(self, *, disconnect_on_subscribe: bool = False) -> None:
        self.disconnect_on_subscribe = disconnect_on_subscribe

    def on(self, event: str, callback: Any) -> None:
        assert event == "disconnected"
        if self.disconnect_on_subscribe:
            callback(self)


class FakeContext:
    def __init__(self, *, disconnect_on_subscribe: bool = False) -> None:
        self.browser = FakeBrowser(
            disconnect_on_subscribe=disconnect_on_subscribe,
        )
        self.pages: list[object] = []
        self.new_page_called = False
        self.closed = False

    async def new_page(self) -> None:
        self.new_page_called = True
        self.pages.append(object())

    async def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize(
    ("headless", "license_key"),
    [(False, None), (True, "test-license")],
)
def test_server_passes_cdp_configuration_to_cloakbrowser(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    headless: bool,
    license_key: str | None,
) -> None:
    context = FakeContext()
    launch_arguments: dict[str, Any] = {}

    if license_key is None:
        monkeypatch.delenv("CLOAKBROWSER_LICENSE_KEY", raising=False)
    else:
        monkeypatch.setenv("CLOAKBROWSER_LICENSE_KEY", license_key)

    async def fake_launch(
        user_data_dir: Path,
        *,
        license_key: str | None,
        headless: bool,
        geoip: bool,
        args: list[str],
    ) -> FakeContext:
        launch_arguments.update(
            {
                "user_data_dir": user_data_dir,
                "license_key": license_key,
                "headless": headless,
                "geoip": geoip,
                "args": args,
            }
        )
        return context

    monkeypatch.setattr(server, "launch_persistent_context_async", fake_launch)

    async def run_test() -> None:
        stop_event = asyncio.Event()
        stop_event.set()
        await server.serve_browser(
            ip_address("127.0.0.1"),
            9222,
            tmp_path / "profile",
            headless,
            stop_event=stop_event,
        )

    asyncio.run(run_test())

    assert launch_arguments == {
        "user_data_dir": tmp_path / "profile",
        "license_key": license_key,
        "headless": headless,
        "geoip": True,
        "args": [
            "--remote-debugging-address=127.0.0.1",
            "--remote-debugging-port=9222",
        ],
    }
    assert context.new_page_called
    assert context.closed


def test_server_fails_when_browser_disconnects(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    context = FakeContext(disconnect_on_subscribe=True)

    async def fake_launch(*_args: Any, **_kwargs: Any) -> FakeContext:
        return context

    monkeypatch.setattr(server, "launch_persistent_context_async", fake_launch)

    with pytest.raises(RuntimeError, match="disconnected unexpectedly"):
        asyncio.run(
            server.serve_browser(
                ip_address("127.0.0.1"),
                9222,
                tmp_path / "profile",
                True,
            )
        )

    assert context.closed


def test_cli_rejects_invalid_bind_address() -> None:
    result = CliRunner().invoke(
        server.main,
        ["--profile-dir", "profile", "--address", "not-an-ip"],
    )

    assert result.exit_code == 2
    assert "Invalid value for '--address'" in result.output
