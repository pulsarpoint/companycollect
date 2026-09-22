"""The browser/rendering setup used by the successful cleaned-HTML benchmarks."""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit

from company_research.browser import BrowserUnavailable
from company_research.browser_client import BrowserLeaseClient, BrowserPageClient
from company_research.captures import page_inventory
from company_research.external_links import collect_external_links, context_payload
from company_research.human_control import HumanAssistanceExpired, HumanSession
from company_research.models import Page, ResearchConfig
from company_research.page_observations import public_response_headers
from company_research.storage import content_hash, utc_now, write_json


def browser_closed(message: str) -> bool:
    return any(
        marker in message.casefold()
        for marker in (
            "target page, context or browser has been closed",
            "browser has been closed",
            "browser disconnected",
            "connection closed while reading from the driver",
        )
    )


@asynccontextmanager
async def open_browser(
    human: HumanSession | None = None,
    *,
    browser_client: BrowserLeaseClient | None = None,
) -> AsyncIterator[BrowserPageClient]:
    if browser_client is None:
        raise ValueError("An external browser-service session is required")
    tab = await browser_client.open_tab("site")
    try:
        await tab.focus()
        yield tab
    finally:
        await tab.close()


async def fetch_page(
    crawler: BrowserPageClient,
    page: Page,
    config: ResearchConfig,
    output_dir: Path,
    human: HumanSession | None = None,
) -> tuple[str, list[dict]]:
    for attempt in range(1, config.page_attempts + 1):
        page.attempts += 1
        page.fetched_at = utc_now()
        if human is not None:
            human.notify(
                "running",
                {"current_url": page.requested_url, "reason": "Fetching page"},
            )
        try:
            async with asyncio.timeout(config.page_timeout_seconds + 30):
                result = await crawler.navigate(
                    page.requested_url,
                    timeout_seconds=config.page_timeout_seconds,
                    check_robots_txt=config.check_robots_txt,
                )
            page.source_url = result.url
            page.status_code = result.status_code
            if human is not None:
                result = await human.check_result(
                    crawler, result, page.requested_url, output_dir, page.page_id
                )
                page.source_url = result.url
                page.status_code = result.status_code
            page.redirects = result.redirects
            page.navigation_attempts = result.navigation_attempts
            write_json(
                output_dir / "fetches" / f"{page.page_id}-{page.attempts}.json",
                {
                    "url": page.requested_url,
                    "final_url": page.source_url,
                    "redirects": page.redirects,
                    "navigation_attempts": page.navigation_attempts,
                    "success": result.successful,
                    "status_code": result.status_code,
                    "error": (result.error or "")[:1000],
                    "metadata": result.metadata,
                    "html_representation": "rendered_html"
                    if result.html
                    else "cleaned_html",
                    "response_headers": public_response_headers(result.headers),
                },
            )
            if (
                result.successful
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
                links = list(result.links)
                write_json(output_dir / "fetches" / f"{page.page_id}-links.json", links)
                link_html = result.html or html
                link_html_file = f"link-html/{page.page_id}.html"
                link_source = output_dir / link_html_file
                link_source.parent.mkdir(parents=True, exist_ok=True)
                link_source.write_text(link_html, encoding="utf-8")
                observations = collect_external_links(
                    link_html,
                    page,
                    html_file=link_html_file,
                    html_kind="rendered_html" if result.html else "cleaned_html",
                    supplemental_links=links,
                )
                write_json(
                    output_dir / "external-links" / f"{page.page_id}.json",
                    [observation.model_dump() for observation in observations],
                )
                page.external_links_file = f"external-links/{page.page_id}.json"
                page.external_link_count = len(observations)
                # The queue may exclude these destinations. Their complete inventory
                # remains in the observation artifact, independently of queue limits.
                links.extend(
                    {
                        "href": observation.url,
                        "text": observation.anchor_text,
                        "title": observation.title,
                        "context": context_payload(observation),
                    }
                    for observation in observations
                )
                inventory, _ = page_inventory(
                    link_html,
                    page.source_url,
                    html_kind="rendered_html" if result.html else "cleaned_html",
                )
                links.extend(
                    {
                        "href": link["url"],
                        "text": link["anchor_text"],
                        "context": link["context"]
                        | {
                            "source_url": page.source_url,
                            "anchor_text": link["anchor_text"],
                        },
                    }
                    for link in inventory
                    if link["url"] is not None
                    and re.search(
                        r"\.(?:pdf|xlsx?|docx?)$",
                        urlsplit(link["url"]).path.rstrip("/"),
                        re.I,
                    )
                )
                return html, links
            page.errors.append(
                f"Fetch returned HTTP {result.status_code}; {result.error or 'no usable HTML'}"[
                    :1000
                ]
            )
            if browser_closed(result.error or ""):
                page.fetch_status = "failed"
                raise BrowserUnavailable("Browser closed during page fetch")
            if (
                result.status_code is not None
                and 400 <= result.status_code < 500
                and result.status_code != 429
            ):
                break
        except HumanAssistanceExpired as error:
            page.fetch_status = "failed"
            page.errors.append(str(error))
            raise
        except BrowserUnavailable:
            raise
        except Exception as error:  # Keep a failed page as a visible outcome and continue other candidates.
            page.errors.append(f"{type(error).__name__}: {str(error)[:500]}")
            write_json(
                output_dir / "fetches" / f"{page.page_id}-{page.attempts}.json",
                {"url": page.requested_url, "success": False, "error": page.errors[-1]},
            )
            if browser_closed(str(error)):
                page.fetch_status = "failed"
                raise BrowserUnavailable("Browser closed during page fetch") from error
        if attempt < config.page_attempts:
            await asyncio.sleep(1)
    page.fetch_status = "failed"
    return "", []
