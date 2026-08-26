import asyncio
import os

import pytest

from crawler_ratsit.crawler import crawl_ratsit_markdown

pytestmark = pytest.mark.integration


def test_remote_cdp_returns_ratsit_company_page_as_markdown() -> None:
    if os.getenv("RATSIT_RUN_INTEGRATION_TESTS") != "1":
        pytest.skip("set RATSIT_RUN_INTEGRATION_TESTS=1 to use the live CDP server")

    company_id = os.getenv("RATSIT_INTEGRATION_COMPANY_ID", "5562434182")
    markdown = asyncio.run(
        crawl_ratsit_markdown(
            company_id,
            cdp_url=os.getenv("RATSIT_CDP_URL", "http://127.0.0.1:9222"),
            content_selector=os.getenv(
                "RATSIT_CONTENT_SELECTOR",
                "main .main-inner",
            ),
            timeout_ms=int(os.getenv("RATSIT_PAGE_TIMEOUT_MS", "60000")),
        )
    )

    formatted_company_id = f"{company_id[:6]}-{company_id[6:]}"
    assert markdown
    assert company_id in markdown or formatted_company_id in markdown
    assert "<main" not in markdown.lower()
