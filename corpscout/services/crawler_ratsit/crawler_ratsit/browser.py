import logging
from pathlib import Path

from cloakbrowser import launch_persistent_context_async
from playwright.async_api import BrowserContext

from crawler_ratsit.config import BrowserSettings

LOGGER = logging.getLogger(__name__)


async def launch_browser_context(
    browser_settings: BrowserSettings,
    *,
    profile_directory: Path,
    license_key: str | None,
    headless: bool,
) -> BrowserContext:
    """Launch one configured CloakBrowser context without exposing proxy secrets."""
    profile_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    LOGGER.info(
        "launching CloakBrowser browser_id=%s proxy=%s profile=%s mode=%s",
        browser_settings.browser_id,
        "configured" if browser_settings.proxy_url is not None else "direct",
        profile_directory,
        "headless" if headless else "headed",
    )
    context = await launch_persistent_context_async(
        profile_directory,
        license_key=license_key,
        headless=headless,
        proxy=browser_settings.proxy_url,
        geoip=True,
    )
    if context.browser is None:
        await context.close()
        raise RuntimeError(
            f"persistent browser context {browser_settings.browser_id} has no browser"
        )
    return context
