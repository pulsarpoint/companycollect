import asyncio
from pathlib import Path
from typing import Any

import pytest

import crawler_ratsit.browser as browser_module
from crawler_ratsit.browser import launch_browser_context
from crawler_ratsit.config import BrowserSettings


class FakeContext:
    def __init__(self) -> None:
        self.browser = object()
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def test_launch_browser_context_uses_configured_proxy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    context = FakeContext()
    launch_arguments: dict[str, Any] = {}

    async def fake_launch(
        profile_directory: Path,
        *,
        license_key: str | None,
        headless: bool,
        proxy: str | None,
        geoip: bool,
    ) -> FakeContext:
        launch_arguments.update(
            {
                "profile_directory": profile_directory,
                "license_key": license_key,
                "headless": headless,
                "proxy": proxy,
                "geoip": geoip,
            }
        )
        return context

    monkeypatch.setattr(
        browser_module,
        "launch_persistent_context_async",
        fake_launch,
    )
    profile_directory = tmp_path / "profile"
    result = asyncio.run(
        launch_browser_context(
            BrowserSettings(
                browser_id="proxy1",
                proxy_url="http://user:password@proxy1:8080",
            ),
            profile_directory=profile_directory,
            license_key="license",
            headless=True,
        )
    )

    assert result is context
    assert profile_directory.is_dir()
    assert launch_arguments == {
        "profile_directory": profile_directory,
        "license_key": "license",
        "headless": True,
        "proxy": "http://user:password@proxy1:8080",
        "geoip": True,
    }
