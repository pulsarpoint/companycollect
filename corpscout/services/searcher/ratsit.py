import asyncio

from markdownify import markdownify
from playwright.async_api import Page, async_playwright


CDP_URL = "http://127.0.0.1:9222"
RATSIT_URL = "https://www.ratsit.se/5562434182-T.I.R._Byggnads_Aktiebolaget_Rajaharju#antal-anstallda"
CONTENT_SELECTOR = "main .main-inner"


async def fetch_ratsit_markdown(page: Page) -> str:
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


async def main() -> None:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.connect_over_cdp(
            CDP_URL,
            is_local=True,
        )
        if not browser.contexts:
            raise RuntimeError("CloakBrowser has no browser context")

        page = await browser.contexts[0].new_page()
        try:
            print(await fetch_ratsit_markdown(page))
        finally:
            await page.close()


if __name__ == "__main__":
    asyncio.run(main())
