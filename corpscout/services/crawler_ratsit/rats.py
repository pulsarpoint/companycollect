import asyncio

from markdownify import markdownify
from playwright.async_api import Page, async_playwright


CDP_URL = "http://127.0.0.1:9222"
CONTENT_SELECTOR = "main .main-inner"


async def fetch_ratsit_markdown(page: Page, company_id: str) -> str:
    RATSIT_URL = f"https://www.ratsit.se/{company_id}"
    response = await page.goto(
        RATSIT_URL,
        wait_until="domcontentloaded",
        timeout=60_000,
    )
    if response is None or not response.ok:
        status = response.status if response is not None else "unknown"
        raise RuntimeError(f"Ratsit returned HTTP status {status}")

    content = page.locator(CONTENT_SELECTOR).first
    await content.wait_for(state="visible", timeout=60_000)

    return markdownify(
        await content.inner_html(),
        heading_style="ATX",
        bullets="-",
    ).strip()


async def crawl_ratsit_markdown(company_id: str) -> str:
    """Crawl the fixed Ratsit company page through the local Chromium instance."""
    async with async_playwright() as playwright:
        browser = await playwright.chromium.connect_over_cdp(
            CDP_URL,
            is_local=True,
        )
        if not browser.contexts:
            raise RuntimeError("CloakBrowser has no browser context")

        page = await browser.contexts[0].new_page()
        try:
            return await fetch_ratsit_markdown(page, company_id)
        finally:
            await page.close()


async def main() -> None:
    print(await crawl_ratsit_markdown("5562434182"))


if __name__ == "__main__":
    asyncio.run(main())
