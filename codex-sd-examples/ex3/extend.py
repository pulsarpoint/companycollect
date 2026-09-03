"""Crawl LLM-suggested pages into an existing crawl manifest."""

import logging
from contextlib import aclosing
from dataclasses import dataclass
from pathlib import Path
from types import AsyncGeneratorType
from urllib.parse import urlsplit

from crawl4ai import AsyncWebCrawler
from crawl4ai.models import CrawlResult

from ex3.crawler import (
    _open_crawler,
    _run_config,
    load_manifest,
    persist_markdown_pages,
    save_model,
)
from ex3.models import CrawlManifest, PassSuggestions, SuggestedPage
from ex3.urls import canonical_domain, same_domain_tree, url_key

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ExtendSettings:
    manifest_path: Path
    suggestions_path: Path
    max_markdown_chars: int = 80_000
    crawl_concurrency: int = 5
    cdp_port: int = 9_245
    headless: bool = True
    check_robots_txt: bool = True
    proxy: str | None = None
    accept_language: str = "en-US,en;q=0.9"


async def run_extend(settings: ExtendSettings) -> CrawlManifest:
    """Crawl the suggested pages and append them to the existing manifest."""
    manifest = load_manifest(settings.manifest_path)
    suggestions = PassSuggestions.model_validate_json(
        settings.suggestions_path.read_text(encoding="utf-8")
    )
    manifest_path = settings.manifest_path.resolve()
    if not _pages_to_crawl(manifest, suggestions):
        LOGGER.info("No new suggested pages to crawl")
        return manifest

    async with _open_crawler(
        headless=settings.headless,
        proxy=settings.proxy,
        cdp_port=settings.cdp_port,
        accept_language=settings.accept_language,
    ) as crawler:
        return await _extend_manifest(
            crawler,
            manifest=manifest,
            suggestions=suggestions,
            settings=settings,
            manifest_path=manifest_path,
        )


def _pages_to_crawl(
    manifest: CrawlManifest, suggestions: PassSuggestions
) -> list[SuggestedPage]:
    """Return the suggestions that are new and stay inside the crawled site."""
    known = {url_key(page.source_url) for page in manifest.markdown_pages}
    searched = canonical_domain(urlsplit(manifest.selected_base_url).hostname or "")
    todo: list[SuggestedPage] = []
    for item in suggestions.suggestions:
        if url_key(item.url) in known:
            continue
        domain = canonical_domain(urlsplit(item.url).hostname or "")
        if not domain or not same_domain_tree(domain, searched):
            LOGGER.warning(
                "Skipping suggested page outside %s: %s",
                searched,
                item.url,
            )
            continue
        todo.append(item)
    return todo


async def _extend_manifest(
    crawler: AsyncWebCrawler,
    *,
    manifest: CrawlManifest,
    suggestions: PassSuggestions,
    settings: ExtendSettings,
    manifest_path: Path,
) -> CrawlManifest:
    todo = _pages_to_crawl(manifest, suggestions)
    if not todo:
        LOGGER.info("No new suggested pages to crawl")
        return manifest

    run_config = _run_config(
        stream=True,
        accept_language=settings.accept_language,
        check_robots_txt=settings.check_robots_txt,
        crawl_concurrency=settings.crawl_concurrency,
    )
    reasons = {item.url: item.reason for item in todo}
    LOGGER.info(
        "Crawling %d suggested page(s) for pass %d", len(todo), suggestions.pass_number
    )
    stream = await crawler.arun_many(
        urls=[item.url for item in todo], config=run_config
    )
    if not isinstance(stream, AsyncGeneratorType):
        raise TypeError("Crawl4AI did not return an async generator for arun_many")
    results: list[CrawlResult] = []
    async with aclosing(stream):
        async for result in stream:
            metadata = dict(result.metadata or {})
            metadata.update(
                {
                    "depth": 0,
                    "selection": "suggested",
                    "pass_number": suggestions.pass_number,
                    "selection_reason": reasons.get(result.url)
                    or next(
                        (
                            reason
                            for url, reason in reasons.items()
                            if url_key(url) == url_key(result.url)
                        ),
                        None,
                    ),
                }
            )
            result.metadata = metadata
            results.append(result)

    new_pages, failures = persist_markdown_pages(
        results,
        markdown_dir=manifest_path.parent,
        max_markdown_chars=settings.max_markdown_chars,
        start_index=len(manifest.markdown_pages) + 1,
    )
    stats = manifest.crawl_stats
    successful = sum(result.success for result in results)
    updated = manifest.model_copy(
        update={
            "markdown_pages": [*manifest.markdown_pages, *new_pages],
            "failed_pages": [*manifest.failed_pages, *failures],
            "crawl_stats": stats.model_copy(
                update={
                    "pages_returned": stats.pages_returned + len(results),
                    "successful_pages": stats.successful_pages + successful,
                    "failed_pages": stats.failed_pages + len(results) - successful,
                    "stored_markdown_pages": stats.stored_markdown_pages
                    + len(new_pages),
                    "stored_markdown_chars": stats.stored_markdown_chars
                    + sum(page.markdown_chars for page in new_pages),
                    "suggested_pages": stats.suggested_pages + len(new_pages),
                }
            ),
        }
    )
    save_model(updated, manifest_path)
    return updated
