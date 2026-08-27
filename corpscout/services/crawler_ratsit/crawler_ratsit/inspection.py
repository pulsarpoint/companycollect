import asyncio
import logging
import os
from collections.abc import Mapping
from pathlib import Path

import click
from markdownify import markdownify

from crawler_ratsit.browser import launch_browser_context
from crawler_ratsit.config import BrowserSettings, ProcessSettings
from crawler_ratsit.crawler import crawl_ratsit_page
from crawler_ratsit.models import (
    FetchedPage,
    identity_kind,
    identity_sha256,
    validate_company_id,
)


async def inspect_company(
    company_id: str,
    *,
    process_settings: ProcessSettings,
    browser_id: str,
    output_directory: Path,
    environment: Mapping[str, str],
    headless: bool | None,
) -> Path:
    """Fetch one Ratsit page locally and write a Markdown inspection artifact."""
    validate_company_id(company_id)
    browser_settings = _browser_settings(process_settings, browser_id)
    output_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    output_path = output_directory / _artifact_name(company_id, browser_id)
    if output_path.exists():
        raise FileExistsError(
            f"inspection artifact already exists: {output_path}; remove it to rerun"
        )
    profile_directory = output_directory / ".profiles" / browser_id
    context = await launch_browser_context(
        browser_settings,
        profile_directory=profile_directory,
        license_key=environment.get("CLOAKBROWSER_LICENSE_KEY", "").strip() or None,
        headless=process_settings.headless if headless is None else headless,
    )
    try:
        fetched_page = await crawl_ratsit_page(
            company_id,
            context=context,
            content_selector=_content_selector(environment),
            timeout_ms=_page_timeout_ms(environment),
        )
    finally:
        await context.close()

    output_path.touch(mode=0o600, exist_ok=False)
    output_path.write_text(
        _inspection_markdown(
            company_id,
            browser_id=browser_id,
            fetched_page=fetched_page,
        ),
        encoding="utf-8",
    )
    return output_path


def _browser_settings(
    process_settings: ProcessSettings,
    browser_id: str,
) -> BrowserSettings:
    browser_settings = next(
        (
            browser
            for browser in process_settings.browsers
            if browser.browser_id == browser_id
        ),
        None,
    )
    if browser_settings is None:
        available = ", ".join(
            browser.browser_id for browser in process_settings.browsers
        )
        raise ValueError(
            f"browser {browser_id!r} is not enabled; available browsers: {available}"
        )
    return browser_settings


def _page_timeout_ms(environment: Mapping[str, str]) -> int:
    raw_timeout = environment.get("RATSIT_PAGE_TIMEOUT_MS", "60000").strip()
    try:
        timeout_ms = int(raw_timeout)
    except ValueError as error:
        raise ValueError("RATSIT_PAGE_TIMEOUT_MS must be an integer") from error
    if timeout_ms < 1:
        raise ValueError("RATSIT_PAGE_TIMEOUT_MS must be positive")
    return timeout_ms


def _content_selector(environment: Mapping[str, str]) -> str:
    content_selector = environment.get(
        "RATSIT_CONTENT_SELECTOR",
        "main .main-inner",
    ).strip()
    if not content_selector:
        raise ValueError("RATSIT_CONTENT_SELECTOR must not be blank")
    return content_selector


def _artifact_name(company_id: str, browser_id: str) -> str:
    return f"{browser_id}-{identity_sha256(company_id)[:16]}.md"


def _inspection_markdown(
    company_id: str,
    *,
    browser_id: str,
    fetched_page: FetchedPage,
) -> str:
    html = fetched_page.content or fetched_page.diagnostic_content
    rendered_page = markdownify(html, heading_style="ATX").strip() if html else ""
    if fetched_page.content:
        html_source = "selected_content"
    elif fetched_page.diagnostic_content:
        html_source = "diagnostic_document"
    else:
        html_source = "none"
    metadata = [
        "# Ratsit local inspection",
        "",
        f"- Browser: `{browser_id}`",
        f"- Identity kind: `{identity_kind(company_id)}`",
        f"- Identity SHA-256: `{identity_sha256(company_id)}`",
        f"- Outcome: `{fetched_page.outcome}`",
        f"- HTTP status: `{fetched_page.http_status}`",
        f"- HTML source: `{html_source}`",
        f"- HTML bytes: `{len(html.encode('utf-8'))}`",
        f"- Error type: `{fetched_page.error_type or 'none'}`",
        f"- Error message: {fetched_page.error_message or 'none'}",
        "",
        "## Captured page",
        "",
        rendered_page or "_No HTML body was captured._",
        "",
    ]
    return "\n".join(metadata)


@click.command()
@click.argument("company_id")
@click.option(
    "--config",
    "config_path",
    type=click.Path(
        path_type=Path,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    envvar="RATSIT_PROCESS_CONFIG",
    required=True,
    help="Protected TOML file containing direct and proxy browser entries.",
)
@click.option(
    "--browser",
    "browser_id",
    required=True,
    help="Enabled browser ID from the process TOML, such as direct or proxy1.",
)
@click.option(
    "--output-directory",
    type=click.Path(path_type=Path, file_okay=False, dir_okay=True),
    default=Path("ratsit-inspections"),
    show_default=True,
)
@click.option(
    "--headless/--headed",
    default=None,
    help="Override process.headless for this local inspection.",
)
def main(
    company_id: str,
    config_path: Path,
    browser_id: str,
    output_directory: Path,
    headless: bool | None,
) -> None:
    """Open one Ratsit page with CloakBrowser and save local Markdown."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        process_settings = ProcessSettings.from_file(config_path)
        output_path = asyncio.run(
            inspect_company(
                company_id,
                process_settings=process_settings,
                browser_id=browser_id,
                output_directory=output_directory,
                environment=os.environ,
                headless=headless,
            )
        )
    except (FileExistsError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Wrote Ratsit inspection to {output_path}")


if __name__ == "__main__":
    main()
