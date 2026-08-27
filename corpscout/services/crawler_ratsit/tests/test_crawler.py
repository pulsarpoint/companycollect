import asyncio
from typing import Any

import pytest
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from crawler_ratsit.crawler import (
    RatsitRateLimitedError,
    RatsitTransientError,
    crawl_ratsit_page,
    fetch_ratsit_page,
)


class FakeResponse:
    def __init__(self, status: int) -> None:
        self.status = status

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 400


class FakeLocator:
    def __init__(self, *, content: str, timeout: bool) -> None:
        self.first = self
        self._content = content
        self._timeout = timeout

    async def wait_for(self, **_kwargs: Any) -> None:
        if self._timeout:
            raise PlaywrightTimeoutError("selector missing")

    async def inner_html(self) -> str:
        return self._content


class FakePage:
    def __init__(
        self,
        *,
        status: int,
        content: str = "",
        selector_timeout: bool = False,
        final_url: str = "https://www.ratsit.se/5562434182",
        document_content: str = "<html><body>Document</body></html>",
    ) -> None:
        self.url = final_url
        self.requested_url: str | None = None
        self._status = status
        self._locator = FakeLocator(content=content, timeout=selector_timeout)
        self._document_content = document_content
        self.closed = False

    async def goto(self, url: str, **_kwargs: Any) -> FakeResponse:
        self.requested_url = url
        return FakeResponse(self._status)

    def locator(self, selector: str) -> FakeLocator:
        assert selector == "main .main-inner"
        return self._locator

    async def content(self) -> str:
        return self._document_content

    async def close(self) -> None:
        self.closed = True


class FakeContext:
    def __init__(self, page: FakePage) -> None:
        self.page = page
        self.new_page_calls = 0

    async def new_page(self) -> FakePage:
        self.new_page_calls += 1
        return self.page


def test_crawl_uses_bound_browser_context_and_closes_page() -> None:
    page = FakePage(status=200, content="<h1>Company</h1>")
    context = FakeContext(page)

    result = asyncio.run(
        crawl_ratsit_page(
            "5562434182",
            context=context,
            content_selector="main .main-inner",
            timeout_ms=1000,
        )
    )

    assert result.outcome == "success"
    assert context.new_page_calls == 1
    assert page.closed


def test_fetch_page_returns_selected_html() -> None:
    result = asyncio.run(
        fetch_ratsit_page(
            FakePage(status=200, content="<h1>Company</h1>"),
            "5562434182",
            content_selector="main .main-inner",
            timeout_ms=1000,
        )
    )

    assert result.outcome == "success"
    assert result.content == "<h1>Company</h1>"
    assert result.http_status == 200


def test_fetch_page_normalizes_twelve_digit_company_id_for_ratsit_url() -> None:
    page = FakePage(status=200, content="<h1>Sole proprietor</h1>")

    result = asyncio.run(
        fetch_ratsit_page(
            page,
            "195562434182",
            content_selector="main .main-inner",
            timeout_ms=1000,
        )
    )

    assert page.requested_url == "https://www.ratsit.se/5562434182"
    assert result.requested_url == "https://www.ratsit.se/5562434182"


@pytest.mark.parametrize(
    ("status", "expected_outcome"),
    [(404, "not_found"), (403, "blocked")],
)
def test_fetch_page_returns_terminal_http_outcome(
    status: int,
    expected_outcome: str,
) -> None:
    result = asyncio.run(
        fetch_ratsit_page(
            FakePage(status=status),
            "5562434182",
            content_selector="main .main-inner",
            timeout_ms=1000,
        )
    )

    assert result.outcome == expected_outcome
    assert result.content == ""


def test_fetch_page_marks_missing_selector() -> None:
    result = asyncio.run(
        fetch_ratsit_page(
            FakePage(status=200, selector_timeout=True),
            "5562434182",
            content_selector="main .main-inner",
            timeout_ms=1000,
        )
    )

    assert result.outcome == "selector_changed"
    assert result.content == ""
    assert result.diagnostic_content == "<html><body>Document</body></html>"


def test_fetch_page_treats_ratsit_missing_redirect_as_not_found() -> None:
    result = asyncio.run(
        fetch_ratsit_page(
            FakePage(
                status=200,
                selector_timeout=True,
                final_url="https://www.ratsit.se/foretag?saknas",
                document_content="<html><body>Företaget saknas</body></html>",
            ),
            "195562434182",
            content_selector="main .main-inner",
            timeout_ms=1000,
        )
    )

    assert result.outcome == "not_found"
    assert result.http_status == 200
    assert result.content == ""
    assert result.error_type == "ratsit_missing"
    assert result.error_message == "Ratsit redirected to /foretag?saknas"
    assert result.diagnostic_content == (
        "<html><body>Företaget saknas</body></html>"
    )


def test_fetch_page_retries_rate_limit() -> None:
    with pytest.raises(
        RatsitRateLimitedError,
        match="retryable HTTP status 429",
    ):
        asyncio.run(
            fetch_ratsit_page(
                FakePage(status=429),
                "5562434182",
                content_selector="main .main-inner",
                timeout_ms=1000,
            )
        )


def test_fetch_page_retries_server_error() -> None:
    with pytest.raises(RatsitTransientError, match="retryable HTTP status 503"):
        asyncio.run(
            fetch_ratsit_page(
                FakePage(status=503),
                "5562434182",
                content_selector="main .main-inner",
                timeout_ms=1000,
            )
        )
