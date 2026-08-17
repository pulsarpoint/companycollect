from cloakbrowser import launch_async
import asyncio


async def my_launch_async() :
    browser = await launch_async(headless=False)
    try:
        page = await browser.new_page()
        await page.goto("https://example.com")
        return await page.title()
    finally:
        await browser.close()

async def main():
    result = await my_launch_async()
    print(f"Hello from searcher! Title: {result}")


if __name__ == "__main__":
    asyncio.run(main())

