"""Test sites: the list, verification of their sitemaps, selection by domain."""

import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from ex1.models import StrictModel
from ex3.crawler import save_model
from ex3.seeding import fetch_head_metadata, seed_sitemap_urls

LOGGER = logging.getLogger(__name__)


class Site(StrictModel):
    domain: str
    start_url: str
    country: str
    size: Literal["small", "mid", "large"]
    note: str = ""
    sitemap_found: bool | None = None
    inventory_urls: int = 0
    base_language: str | None = None
    verified_at: str | None = None
    error: str | None = None


class SiteList(StrictModel):
    sites: list[Site]


def load_sites(path: Path) -> SiteList:
    return SiteList.model_validate_json(path.read_text(encoding="utf-8"))


def save_sites(site_list: SiteList, path: Path) -> None:
    save_model(site_list, path)


def select_sites(site_list: SiteList, domains: Sequence[str] | None) -> list[Site]:
    """Return the requested sites, or every site with a verified sitemap."""
    by_domain = {site.domain: site for site in site_list.sites}
    if domains is None:
        return [site for site in site_list.sites if site.sitemap_found]
    missing = [domain for domain in domains if domain not in by_domain]
    if missing:
        raise ValueError(f"Unknown site domain(s): {', '.join(missing)}")
    return [by_domain[domain] for domain in domains]


async def verify_site(site: Site, *, seed_max_urls: int, accept_language: str) -> Site:
    """Seed the sitemap and read the base page language; never raises."""
    outcome = await seed_sitemap_urls(
        site.start_url,
        source="sitemap",
        max_urls=seed_max_urls,
        accept_language=accept_language,
        proxy=None,
    )
    heads = await fetch_head_metadata(
        [site.start_url], accept_language=accept_language, proxy=None, concurrency=1
    )
    head = heads.get(site.start_url)
    found = len(outcome.urls) > 0
    error = (
        outcome.error if outcome.error else (None if found else "no sitemap URLs found")
    )
    LOGGER.info(
        "%s: sitemap_found=%s inventory=%d language=%s",
        site.domain,
        found,
        len(outcome.urls),
        head.language if head else None,
    )
    return site.model_copy(
        update={
            "sitemap_found": found,
            "inventory_urls": len(outcome.urls),
            "base_language": head.language if head is not None else None,
            "verified_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "error": error,
        }
    )
