import asyncio

from playwright.async_api import Page, async_playwright


BRAVE_ORIGIN = "https://search.brave.com"
QUERY = "Can you give me more information about Sweden company +1 Kommunikationsbyrå AB"


async def copy_brave_answer(page: Page, query: str) -> str:
    await page.goto(BRAVE_ORIGIN)

    searchbox = page.get_by_test_id("searchbox")
    await searchbox.fill(query)
    await searchbox.press("Enter")

    more_button = page.get_by_role("button", name="More")
    await more_button.wait_for(state="visible", timeout=60_000)
    await more_button.click()

    await page.get_by_role("button", name="Copy").click()

    return await page.evaluate(
        "() => navigator.clipboard.readText()"
    )


async def main() -> None:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.connect_over_cdp(
            "http://127.0.0.1:9222",
            is_local=True,
        )

        if not browser.contexts:
            raise RuntimeError("CloakBrowser has no browser context")

        context = browser.contexts[0]
        await context.grant_permissions(
            ["clipboard-read", "clipboard-write"],
            origin=BRAVE_ORIGIN,
        )

        page = context.pages[0] if context.pages else await context.new_page()

        copied_text = await copy_brave_answer(page, QUERY)
        print(copied_text)


if __name__ == "__main__":
    asyncio.run(main())