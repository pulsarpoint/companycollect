from playwright.async_api import BrowserContext, Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from crawler_ratsit.models import FetchedPage, ratsit_url, validate_company_id


class RatsitTransientError(RuntimeError):
    pass


class RatsitRateLimitedError(RatsitTransientError):
    pass


async def crawl_ratsit_page(
    company_id: str,
    *,
    context: BrowserContext,
    content_selector: str,
    timeout_ms: int,
) -> FetchedPage:
    validate_company_id(company_id)
    page = await context.new_page()
    try:
        return await fetch_ratsit_page(
            page,
            company_id,
            content_selector=content_selector,
            timeout_ms=timeout_ms,
        )
    finally:
        await page.close()


async def fetch_ratsit_page(
    page: Page,
    company_id: str,
    *,
    content_selector: str,
    timeout_ms: int,
) -> FetchedPage:
    requested_url = ratsit_url(company_id)
    response = await page.goto(
        requested_url,
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    if response is None:
        raise RatsitTransientError("Ratsit navigation returned no HTTP response")

    if response.status == 404:
        return FetchedPage(
            outcome="not_found",
            requested_url=requested_url,
            final_url=page.url,
            http_status=response.status,
            content="",
            error_type="http_not_found",
            error_message="Ratsit returned HTTP status 404",
        )
    if response.status in {401, 403}:
        return FetchedPage(
            outcome="blocked",
            requested_url=requested_url,
            final_url=page.url,
            http_status=response.status,
            content="",
            error_type="http_blocked",
            error_message=f"Ratsit returned HTTP status {response.status}",
        )
    if response.status == 429:
        raise RatsitRateLimitedError("Ratsit returned retryable HTTP status 429")
    if response.status >= 500:
        raise RatsitTransientError(
            f"Ratsit returned retryable HTTP status {response.status}"
        )
    if not response.ok:
        raise RatsitTransientError(f"Ratsit returned HTTP status {response.status}")

    content = page.locator(content_selector).first
    try:
        await content.wait_for(state="visible", timeout=timeout_ms)
    except PlaywrightTimeoutError:
        return FetchedPage(
            outcome="selector_changed",
            requested_url=requested_url,
            final_url=page.url,
            http_status=response.status,
            content="",
            error_type="content_selector_missing",
            error_message=f"Ratsit content selector was not visible: {content_selector}",
        )

    return FetchedPage(
        outcome="success",
        requested_url=requested_url,
        final_url=page.url,
        http_status=response.status,
        content=await content.inner_html(),
        error_type="",
        error_message="",
    )
