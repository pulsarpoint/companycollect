import asyncio
import os
from pathlib import Path

import pytest
from cloakbrowser import launch_persistent_context_async

from crawler_ratsit.crawler import crawl_ratsit_page
from crawler_ratsit.models import ratsit_path_id

pytestmark = pytest.mark.integration


def test_direct_cloakbrowser_returns_ratsit_company_page(
    tmp_path: Path,
) -> None:
    if os.getenv("RATSIT_RUN_INTEGRATION_TESTS") != "1":
        pytest.skip("set RATSIT_RUN_INTEGRATION_TESTS=1 to use CloakBrowser")

    async def run_test() -> str:
        context = await launch_persistent_context_async(
            tmp_path / "profile",
            license_key=os.getenv("CLOAKBROWSER_LICENSE_KEY") or None,
            headless=True,
            proxy=os.getenv("RATSIT_INTEGRATION_PROXY_URL") or None,
            geoip=True,
        )
        try:
            result = await crawl_ratsit_page(
                os.getenv("RATSIT_INTEGRATION_COMPANY_ID", "5562434182"),
                context=context,
                content_selector=os.getenv(
                    "RATSIT_CONTENT_SELECTOR",
                    "main .main-inner",
                ),
                timeout_ms=int(os.getenv("RATSIT_PAGE_TIMEOUT_MS", "60000")),
            )
            assert result.outcome == "success"
            return result.content
        finally:
            await context.close()

    content = asyncio.run(run_test())
    company_id = os.getenv("RATSIT_INTEGRATION_COMPANY_ID", "5562434182")
    path_id = ratsit_path_id(company_id)
    formatted_company_id = f"{path_id[:6]}-{path_id[6:]}"
    assert content
    assert path_id in content or formatted_company_id in content
