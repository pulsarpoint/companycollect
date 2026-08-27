import asyncio
from pathlib import Path
from typing import Any

import pytest

from crawler_ratsit import process
from crawler_ratsit.config import BrowserSettings, ProcessSettings, WorkerSettings
from crawler_ratsit.constants import HTTP_TASK_QUEUE
from crawler_ratsit.object_store import RatsitObjectStore


class FakeBrowser:
    def __init__(self) -> None:
        self.subscriptions: dict[str, Any] = {}

    def on(self, event: str, callback: Any) -> None:
        self.subscriptions[event] = callback


class FakeContext:
    def __init__(self) -> None:
        self.browser = FakeBrowser()
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def test_browser_launch_uses_its_profile_and_optional_proxy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    context = FakeContext()
    launch_arguments: dict[str, Any] = {}

    async def fake_launch(
        selected_browser_settings: BrowserSettings,
        *,
        profile_directory: Path,
        license_key: str | None,
        headless: bool,
    ) -> FakeContext:
        launch_arguments.update(
            {
                "browser_settings": selected_browser_settings,
                "profile_directory": profile_directory,
                "license_key": license_key,
                "headless": headless,
            }
        )
        return context

    monkeypatch.setattr(process, "launch_browser_context", fake_launch)
    browser_settings = BrowserSettings(
        browser_id="proxy1",
        proxy_url="http://user:password@proxy1:8080",
    )
    process_settings = ProcessSettings(
        state_directory=tmp_path,
        headless=False,
        per_browser_activities_per_second=0.2,
        task_queue_activities_per_second=0.4,
        browsers=(browser_settings,),
    )

    runtime = asyncio.run(
        process._launch_browser(
            browser_settings,
            process_settings=process_settings,
            license_key="license",
            disconnected_callback=lambda _browser_id: None,
        )
    )

    assert runtime.context is context
    assert launch_arguments == {
        "browser_settings": browser_settings,
        "profile_directory": tmp_path / "proxy1",
        "license_key": "license",
        "headless": False,
    }
    assert "disconnected" in context.browser.subscriptions


def test_http_worker_uses_one_temporal_worker_for_the_browser_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeWorker:
        def __init__(self, client: object, **kwargs: Any) -> None:
            captured["client"] = client
            captured.update(kwargs)

    monkeypatch.setattr(process, "Worker", FakeWorker)
    worker_settings = WorkerSettings.from_environment(
        {
            "CORPSCOUT_S3_ENDPOINT": "http://rustfs:9000",
            "CORPSCOUT_S3_ACCESS_KEY": "access",
            "CORPSCOUT_S3_SECRET_KEY": "secret",
        }
    )
    direct_settings = BrowserSettings(browser_id="direct", proxy_url=None)
    proxy_settings = BrowserSettings(
        browser_id="proxy1",
        proxy_url="http://user:password@proxy1:8080",
    )
    process_settings = ProcessSettings(
        state_directory=Path("/var/lib/ratsit-process"),
        headless=True,
        per_browser_activities_per_second=0.2,
        task_queue_activities_per_second=0.4,
        browsers=(direct_settings, proxy_settings),
    )
    direct_context = FakeContext()
    proxy_context = FakeContext()
    temporal_client = object()

    process._http_worker(
        temporal_client,
        browsers=[
            process.BrowserRuntime(
                settings=direct_settings,
                context=direct_context,
            ),
            process.BrowserRuntime(
                settings=proxy_settings,
                context=proxy_context,
            ),
        ],
        worker_settings=worker_settings,
        process_settings=process_settings,
        object_store=RatsitObjectStore(
            object(),
            bucket="source-sweden-ratsit",
            prefix="raw",
        ),
        identity_prefix="ratsit-process/test/1",
    )

    assert captured["client"] is temporal_client
    assert captured["task_queue"] == HTTP_TASK_QUEUE
    assert captured["max_concurrent_activities"] == 2
    assert "max_activities_per_second" not in captured
    assert captured["max_task_queue_activities_per_second"] == 0.4
    assert captured["identity"] == "ratsit-process/test/1/http-pool"
    crawl_activity = captured["activities"][0]
    activities = crawl_activity.__self__
    direct_browser = activities._available_browsers.get_nowait()
    proxy_browser = activities._available_browsers.get_nowait()
    assert direct_browser.browser_id == "direct"
    assert direct_browser.crawl_page.keywords["context"] is direct_context
    assert proxy_browser.browser_id == "proxy1"
    assert proxy_browser.crawl_page.keywords["context"] is proxy_context
