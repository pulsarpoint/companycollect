"""The browser/rendering setup used by the successful cleaned-HTML benchmarks."""

import asyncio
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from cloakbrowser import launch_async
from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig

from company_research.models import Page, ResearchConfig
from company_research.storage import content_hash, utc_now, write_json


@asynccontextmanager
async def open_browser() -> AsyncIterator[AsyncWebCrawler]:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    browser = await launch_async(
        headless=True,
        args=[
            f"--remote-debugging-port={port}",
            "--remote-debugging-address=127.0.0.1",
        ],
    )
    try:
        browser_config = BrowserConfig(
            browser_mode="cdp",
            cdp_url=f"http://127.0.0.1:{port}",
            headers={"Accept-Language": "en-US,en;q=0.9"},
            verbose=False,
        )
        async with AsyncWebCrawler(config=browser_config) as crawler:
            yield crawler
    finally:
        await browser.close()


async def fetch_page(
    crawler: AsyncWebCrawler, page: Page, config: ResearchConfig, output_dir: Path
) -> tuple[str, list[dict]]:
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=int(config.page_timeout_seconds * 1000),
        delay_before_return_html=2.0,
        check_robots_txt=config.check_robots_txt,
        verbose=False,
    )
    for attempt in range(1, config.page_attempts + 1):
        page.attempts = attempt
        page.fetched_at = utc_now()
        try:
            async with asyncio.timeout(config.page_timeout_seconds + 30):
                results = await crawler.arun(url=page.requested_url, config=run_config)  # ty: ignore[missing-argument] -- Crawl4AI decorator typing.
            result = next(iter(results))
            page.source_url = result.redirected_url or result.url
            page.status_code = result.status_code
            write_json(
                output_dir / "fetches" / f"{page.page_id}-{attempt}.json",
                {
                    "url": page.requested_url,
                    "final_url": page.source_url,
                    "success": result.success,
                    "status_code": result.status_code,
                    "error": (result.error_message or "")[:1000],
                    "metadata": result.metadata,
                },
            )
            if (
                result.success
                and result.status_code is not None
                and 200 <= result.status_code < 400
                and result.cleaned_html
            ):
                html = result.cleaned_html
                filename = f"html/{page.page_id}.html"
                target = output_dir / filename
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(html, encoding="utf-8")
                page.html_file, page.html_sha256, page.fetch_status = (
                    filename,
                    content_hash(html),
                    "fetched",
                )
                links = [
                    *result.links.get("internal", []),
                    *result.links.get("external", []),
                ]
                return html, links
            page.errors.append(
                f"Fetch returned HTTP {result.status_code}; {result.error_message or 'no usable HTML'}"[
                    :1000
                ]
            )
            if (
                result.status_code is not None
                and 400 <= result.status_code < 500
                and result.status_code != 429
            ):
                break
        except Exception as error:  # Keep a failed page as a visible outcome and continue other candidates.
            page.errors.append(f"{type(error).__name__}: {str(error)[:500]}")
        if attempt < config.page_attempts:
            await asyncio.sleep(1)
    page.fetch_status = "failed"
    return "", []
