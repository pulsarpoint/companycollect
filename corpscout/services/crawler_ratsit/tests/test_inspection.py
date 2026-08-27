import asyncio
from pathlib import Path
from stat import S_IMODE
from typing import Any

import pytest

import crawler_ratsit.inspection as inspection_module
from crawler_ratsit.config import BrowserSettings, ProcessSettings
from crawler_ratsit.inspection import inspect_company
from crawler_ratsit.models import FetchedPage


class FakeContext:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def test_inspect_company_uses_selected_proxy_and_writes_markdown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    context = FakeContext()
    launch_arguments: dict[str, Any] = {}
    crawl_arguments: dict[str, Any] = {}
    process_settings = ProcessSettings(
        state_directory=Path("/unused/production/state"),
        headless=False,
        per_browser_activities_per_second=0.2,
        task_queue_activities_per_second=0.2,
        browsers=(
            BrowserSettings(browser_id="direct", proxy_url=None),
            BrowserSettings(
                browser_id="proxy1",
                proxy_url="http://user:password@proxy1:8080",
            ),
        ),
    )

    async def fake_launch(
        browser_settings: BrowserSettings,
        *,
        profile_directory: Path,
        license_key: str | None,
        headless: bool,
    ) -> FakeContext:
        launch_arguments.update(
            {
                "browser_settings": browser_settings,
                "profile_directory": profile_directory,
                "license_key": license_key,
                "headless": headless,
            }
        )
        return context

    async def fake_crawl(
        company_id: str,
        *,
        context: FakeContext,
        content_selector: str,
        timeout_ms: int,
    ) -> FetchedPage:
        crawl_arguments.update(
            {
                "company_id": company_id,
                "context": context,
                "content_selector": content_selector,
                "timeout_ms": timeout_ms,
            }
        )
        return FetchedPage(
            outcome="success",
            requested_url="https://www.ratsit.se/redacted",
            final_url="https://www.ratsit.se/redacted-name",
            http_status=200,
            content="<h1>Example profile</h1><p>Captured details</p>",
            error_type="",
            error_message="",
        )

    monkeypatch.setattr(inspection_module, "launch_browser_context", fake_launch)
    monkeypatch.setattr(inspection_module, "crawl_ratsit_page", fake_crawl)
    output_directory = tmp_path / "inspections"
    output_path = asyncio.run(
        inspect_company(
            "195562434182",
            process_settings=process_settings,
            browser_id="proxy1",
            output_directory=output_directory,
            environment={
                "CLOAKBROWSER_LICENSE_KEY": "license",
                "RATSIT_CONTENT_SELECTOR": "main .main-inner",
                "RATSIT_PAGE_TIMEOUT_MS": "45000",
            },
            headless=True,
        )
    )

    assert output_path.parent == output_directory
    assert "195562434182" not in output_path.name
    assert output_path.suffix == ".md"
    assert S_IMODE(output_path.stat().st_mode) == 0o600
    assert context.closed
    assert launch_arguments == {
        "browser_settings": process_settings.browsers[1],
        "profile_directory": output_directory / ".profiles" / "proxy1",
        "license_key": "license",
        "headless": True,
    }
    assert crawl_arguments == {
        "company_id": "195562434182",
        "context": context,
        "content_selector": "main .main-inner",
        "timeout_ms": 45000,
    }
    markdown = output_path.read_text(encoding="utf-8")
    assert "# Example profile" in markdown
    assert "Captured details" in markdown
    assert "Outcome: `success`" in markdown
    assert "Identity kind: `individual_owner`" in markdown
    assert "user:password" not in markdown


def test_inspection_uses_diagnostic_document_for_not_found() -> None:
    markdown = inspection_module._inspection_markdown(
        "195562434182",
        browser_id="direct",
        fetched_page=FetchedPage(
            outcome="not_found",
            requested_url="https://www.ratsit.se/redacted",
            final_url="https://www.ratsit.se/foretag?saknas",
            http_status=200,
            content="",
            error_type="ratsit_missing",
            error_message="Ratsit redirected to /foretag?saknas",
            diagnostic_content="<h1>Missing</h1><p>No matching page</p>",
        ),
    )

    assert "Outcome: `not_found`" in markdown
    assert "HTML source: `diagnostic_document`" in markdown
    assert "# Missing" in markdown
    assert "No matching page" in markdown
