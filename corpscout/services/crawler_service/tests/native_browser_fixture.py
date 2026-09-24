"""Native-driver fixture for legacy rendering tests; production uses HTTP only."""

from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from browser_service.browser import BrowserSession
from browser_service.runtime import BrowserRuntimeSettings, BrowserService

from crawler_service.browser import PageCapture, interpret_capture


class CapturingTab(BrowserSession):
    async def capture(self):
        raw = await super().capture()
        return interpret_capture(
            url=raw.url,
            html=raw.html,
            status_code=raw.status_code,
            headers=raw.headers,
            error=raw.error,
        )

    async def navigate(self, url, **kwargs):
        raw = await super().navigate(url, **kwargs)
        if isinstance(raw, PageCapture):
            return raw
        return interpret_capture(
            url=raw.url,
            html=raw.html,
            status_code=raw.status_code,
            headers=raw.headers,
            error=raw.error,
        )


@asynccontextmanager
async def open_test_browser(human=None):
    with TemporaryDirectory() as directory:
        service = BrowserService(
            Path(directory),
            settings=BrowserRuntimeSettings(
                max_browsers=1, idle_timeout_seconds=120, session_retention_days=7
            ),
        )
        await service.start()
        try:
            session = await service.claim(
                identifier=uuid4().hex,
                request_id="fixture",
                domain="fixture",
                headless=False,
            )
            profile = session.profile
            if human is not None:
                human.browser_available = True
            yield CapturingTab(profile.context, await profile.context.new_page())
        finally:
            await service.close()
