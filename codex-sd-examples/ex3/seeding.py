"""Pre-crawl URL inventory (sitemap, Common Crawl) and deterministic page picks.

Everything here runs before the browser crawl and without LLM tokens. The
inventory comes from Crawl4AI's ``AsyncUrlSeeder``; ranking is done by
:mod:`ex3.selection`; a bounded ``<head>`` fetch for the shortlist supplies the
declared document language and title before any page is rendered.
"""

import logging
from collections.abc import Awaitable, Callable, Collection, Sequence
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import httpx
from crawl4ai import AsyncUrlSeeder, SeedingConfig

from ex3.models import ScoredUrl, SeedingSource
from ex3.selection import apply_head_metadata, rank_urls
from ex3.urls import normalize_start_url, url_key

LOGGER = logging.getLogger(__name__)
SHORTLIST_FACTOR = 3
SHORTLIST_CAP = 150
HEAD_FETCH_TIMEOUT_SECONDS = 8


@dataclass(frozen=True, slots=True)
class HeadMetadata:
    language: str | None
    title: str | None
    description: str | None


@dataclass(frozen=True, slots=True)
class SeedingOutcome:
    urls: list[str]
    error: str | None = None


@dataclass(frozen=True, slots=True)
class SelectionResult:
    selected: list[ScoredUrl]
    eligible: list[ScoredUrl] = field(default_factory=list)
    excluded: list[ScoredUrl] = field(default_factory=list)
    inventory_urls: int = 0
    head_checked_urls: int = 0
    base_page_links: int = 0


type HeadFetcher = Callable[[list[str]], Awaitable[dict[str, HeadMetadata]]]


async def seed_sitemap_urls(
    base_url: str,
    *,
    source: SeedingSource,
    max_urls: int,
    accept_language: str,
    proxy: str | None,
) -> SeedingOutcome:
    """Collect the URL inventory for the base URL's host without raising."""
    hostname = urlsplit(normalize_start_url(base_url)).hostname or ""
    config = SeedingConfig(
        source=source,
        extract_head=False,
        live_check=False,
        max_urls=max_urls,
        verbose=False,
        filter_nonsense_urls=True,
    )
    try:
        async with AsyncUrlSeeder(
            client=_http_client(accept_language, proxy)
        ) as seeder:
            entries = await seeder.urls(hostname, config)
    except Exception as error:
        LOGGER.exception("URL seeding for %s failed", hostname)
        return SeedingOutcome(urls=[], error=f"{type(error).__name__}: {error}")

    urls: list[str] = []
    seen: set[str] = set()
    for entry in entries:
        url = entry.get("url") if isinstance(entry, dict) else None
        if not isinstance(url, str) or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    LOGGER.info("URL seeding for %s returned %d unique URL(s)", hostname, len(urls))
    return SeedingOutcome(urls=urls)


async def fetch_head_metadata(
    urls: Sequence[str],
    *,
    accept_language: str,
    proxy: str | None,
    concurrency: int,
) -> dict[str, HeadMetadata]:
    """Fetch declared language, title and description for a URL shortlist."""
    if not urls:
        return {}
    config = SeedingConfig(
        source="sitemap",
        extract_head=True,
        concurrency=concurrency,
        hits_per_sec=max(concurrency, 1),
        verbose=False,
    )
    metadata: dict[str, HeadMetadata] = {}
    try:
        async with AsyncUrlSeeder(
            client=_http_client(accept_language, proxy)
        ) as seeder:
            entries = await seeder.extract_head_for_urls(
                list(urls),
                config,
                concurrency=concurrency,
                timeout=HEAD_FETCH_TIMEOUT_SECONDS,
            )
    except Exception:
        LOGGER.exception("Head metadata fetch failed")
        return metadata

    for entry in entries:
        if not isinstance(entry, dict) or entry.get("status") != "valid":
            continue
        url = entry.get("url")
        head = entry.get("head_data")
        if not isinstance(url, str) or not isinstance(head, dict):
            continue
        meta = head.get("meta")
        description = meta.get("description") if isinstance(meta, dict) else None
        metadata[url] = HeadMetadata(
            language=_clean(head.get("lang")),
            title=_clean(head.get("title")),
            description=_clean(description),
        )
    return metadata


async def select_pages(
    inventory: Sequence[str],
    *,
    base_url: str,
    limit: int,
    fetch_heads: HeadFetcher,
    base_page_links: Sequence[str] = (),
    exclude_urls: Collection[str] = (),
) -> SelectionResult:
    """Rank the inventory, verify a shortlist's heads, and pick the top pages.

    The base URL and the links found on the base page always join the
    inventory: sitemaps frequently omit the homepage and section landing pages.
    ``exclude_urls`` (already crawled pages) never take part in the ranking.
    """
    unique_links = list(dict.fromkeys(base_page_links))
    excluded_keys = {url_key(url) for url in exclude_urls}
    candidates = [
        url
        for url in (base_url, *inventory, *unique_links)
        if url_key(url) not in excluded_keys
    ]
    eligible, excluded = rank_urls(
        candidates,
        base_url=base_url,
        linked_from_base=unique_links,
    )
    inventory_urls = len(eligible) + len(excluded)
    if not eligible:
        return SelectionResult(
            selected=[],
            eligible=[],
            excluded=excluded,
            inventory_urls=inventory_urls,
            base_page_links=len(unique_links),
        )

    shortlist_size = min(max(limit * SHORTLIST_FACTOR, limit), SHORTLIST_CAP)
    shortlist = eligible[:shortlist_size]
    remainder = eligible[shortlist_size:]
    heads = await fetch_heads([scored.url for scored in shortlist])

    refined: list[ScoredUrl] = []
    for scored in shortlist:
        head = heads.get(scored.url)
        if head is None:
            refined.append(scored)
            continue
        refined.append(
            apply_head_metadata(
                scored,
                language=head.language,
                title=head.title,
                description=head.description,
            )
        )

    still_eligible = sorted(
        (scored for scored in refined if scored.exclusion is None),
        key=lambda scored: (-scored.score, scored.url),
    )
    newly_excluded = [scored for scored in refined if scored.exclusion is not None]
    return SelectionResult(
        selected=still_eligible[:limit],
        eligible=[*still_eligible, *remainder],
        excluded=sorted([*excluded, *newly_excluded], key=lambda scored: scored.url),
        inventory_urls=inventory_urls,
        head_checked_urls=len(shortlist),
        base_page_links=len(unique_links),
    )


def _http_client(accept_language: str, proxy: str | None) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        http2=True,
        timeout=20,
        proxy=proxy,
        follow_redirects=True,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept-Language": accept_language,
        },
    )


def _clean(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None
