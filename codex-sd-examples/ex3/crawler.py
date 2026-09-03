import hashlib
import json
import logging
import math
import re
from collections.abc import AsyncGenerator, Callable, Collection
from contextlib import aclosing
from dataclasses import dataclass
from pathlib import Path
from types import AsyncGeneratorType
from typing import Literal
from urllib.parse import urljoin, urlsplit, urlunsplit

from cloakbrowser import launch_async
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.deep_crawling import (
    BestFirstCrawlingStrategy,
    BFSDeepCrawlStrategy,
    FilterChain,
)
from crawl4ai.models import CrawlResult
from pydantic import BaseModel, ValidationError

from ex1.models import AnalysisTokenUsage, FailedPage
from ex3.language import (
    detect_document_language,
    inspect_language_page,
    is_english_language,
)
from ex3.llm import run_structured_turn
from ex3.models import (
    BatchAnalysis,
    BatchExtraction,
    BatchStats,
    CrawlManifest,
    CrawlStats,
    DiscoveredDomainCandidate,
    DiscoveredUrl,
    DiscoveryStrategy,
    LanguageDiscovery,
    MarkdownPage,
    PageExtraction,
    PageExtractionMetadata,
    RelatedDomain,
    RelatedDomainAnalysis,
    RelatedDomainSelection,
    ResearchReport,
    ScoredUrl,
    SeedingSource,
    UrlSeeding,
    build_analysis_stats,
    consolidate_extractions,
    set_source_url,
)
from ex3.prompty import create_prompt, create_related_domains_prompt
from ex3.seeding import (
    HeadMetadata,
    SelectionResult,
    fetch_head_metadata,
    seed_sitemap_urls,
    select_pages,
)
from ex3.selection import (
    DEFAULT_PREFERRED_LANGUAGES,
    SelectionFilter,
    SelectionScorer,
)
from ex3.urls import canonical_domain, normalize_start_url, same_domain_tree, url_key

LOGGER = logging.getLogger(__name__)
LANGUAGE_CANDIDATE_LIMIT = 5
DOMAIN_URL_SAMPLE_LIMIT = 3
DOMAIN_URL_SAMPLE_CHAR_LIMIT = 500
DOMAIN_LABEL_SAMPLE_LIMIT = 10
MARKDOWN_LINK_PATTERN = re.compile(r"\]\(([^\s<>\)]+)")
HEAD_FETCH_CONCURRENCY = 8
INVENTORY_FILENAME = "url-inventory.json"


@dataclass(frozen=True, slots=True)
class CrawlSettings:
    start_url: str
    markdown_dir: Path
    max_pages: int
    max_depth: int
    max_markdown_chars: int
    crawl_concurrency: int
    cdp_port: int
    headless: bool
    include_external: bool
    check_robots_txt: bool
    proxy: str | None
    seed: bool = True
    seed_source: SeedingSource = "sitemap"
    seed_max_urls: int = 5_000
    seed_share: float = 0.6
    discovery_strategy: DiscoveryStrategy = "best_first"
    accept_language: str = "en-US,en;q=0.9"


@dataclass(frozen=True, slots=True)
class AnalysisSettings:
    manifest_path: Path
    max_page_chars: int
    max_batch_pages: int
    max_batch_chars: int
    analysis_timeout_seconds: int
    skip_non_english: bool = False


@dataclass(frozen=True, slots=True)
class MarkdownBatch:
    number: int
    pages: tuple[MarkdownPage, ...]
    markdown_chars: int


@dataclass(frozen=True, slots=True)
class BatchOutcome:
    extraction: BatchExtraction
    token_usage: AnalysisTokenUsage | None
    error: str | None


async def run_crawl(settings: CrawlSettings) -> CrawlManifest:
    """Discover an English base URL, pick pages, and persist a bounded crawl."""
    requested_start_url = normalize_start_url(settings.start_url)
    language_discovery, url_seeding, crawl_results = await _discover_and_crawl(
        settings,
        requested_start_url=requested_start_url,
    )
    successful_crawls = sum(result.success for result in crawl_results)
    LOGGER.info(
        "Crawl finished and browser closed: %d successful page(s), %d failed result(s)",
        successful_crawls,
        len(crawl_results) - successful_crawls,
    )
    markdown_pages, crawl_failures = persist_markdown_pages(
        crawl_results,
        markdown_dir=settings.markdown_dir,
        max_markdown_chars=settings.max_markdown_chars,
    )
    crawl_stats = _crawl_stats(settings, crawl_results, markdown_pages)
    stopped_reason = (
        "max_pages_reached"
        if len(markdown_pages) >= settings.max_pages
        else "crawl_completed"
    )
    manifest_path = settings.markdown_dir.resolve() / "crawl-manifest.json"
    manifest = CrawlManifest(
        requested_start_url=requested_start_url,
        selected_base_url=language_discovery.selected_base_url,
        stopped_reason=stopped_reason,
        discovery_strategy=settings.discovery_strategy,
        url_seeding=url_seeding,
        language_discovery=language_discovery,
        crawl_stats=crawl_stats,
        failed_pages=crawl_failures,
        markdown_pages=markdown_pages,
    )
    save_model(manifest, manifest_path)

    LOGGER.info(
        "Crawl complete: persisted %d/%d Markdown page(s) and manifest %s",
        len(markdown_pages),
        settings.max_pages,
        manifest_path,
    )
    return manifest


async def run_analysis(settings: AnalysisSettings) -> ResearchReport:
    """Analyze Markdown referenced by an existing crawl manifest."""
    if settings.max_page_chars > settings.max_batch_chars:
        raise ValueError("max_page_chars must not exceed max_batch_chars")

    manifest = load_manifest(settings.manifest_path)
    markdown_pages, skipped_pages = _split_pages_by_language(
        manifest.markdown_pages,
        skip_non_english=settings.skip_non_english,
    )
    if skipped_pages:
        LOGGER.info(
            "Skipping %d stored page(s) declared in a non-English language",
            len(skipped_pages),
        )
    discovered_urls = discover_urls(
        manifest.markdown_pages,
        searched_url=manifest.selected_base_url,
    )
    LOGGER.info(
        "Discovered %d unique HTTP(S) URL(s) in stored Markdown",
        len(discovered_urls),
    )
    related_domains, related_domain_analysis = await _analyze_related_domains(
        discovered_urls,
        searched_url=manifest.selected_base_url,
        timeout_seconds=settings.analysis_timeout_seconds,
    )
    LOGGER.info(
        "LLM selected %d related domain(s)",
        len(related_domains),
    )
    batches = create_batches(
        markdown_pages,
        max_page_chars=settings.max_page_chars,
        max_batch_pages=settings.max_batch_pages,
        max_batch_chars=settings.max_batch_chars,
    )
    LOGGER.info(
        "Loaded %d stored Markdown page(s); starting %d LLM batch(es)",
        len(markdown_pages),
        len(batches),
    )
    pages, batch_analyses, analysis_failures = await _analyze_batches(
        batches,
        max_page_chars=settings.max_page_chars,
        timeout_seconds=settings.analysis_timeout_seconds,
    )
    failed_pages = [*manifest.failed_pages, *analysis_failures]
    successful_extractions = sum(
        page.extraction_metadata is not None and page.extraction_metadata.succeeded
        for page in pages
    )

    return ResearchReport(
        requested_start_url=manifest.requested_start_url,
        selected_base_url=manifest.selected_base_url,
        crawl_strategy=_crawl_strategy_label(manifest),
        stopped_reason=manifest.stopped_reason,
        manifest_path=str(settings.manifest_path.resolve()),
        language_discovery=manifest.language_discovery,
        url_seeding=manifest.url_seeding,
        crawl_stats=manifest.crawl_stats,
        batch_stats=BatchStats(
            configured_max_page_chars=settings.max_page_chars,
            configured_max_batch_pages=settings.max_batch_pages,
            configured_max_batch_chars=settings.max_batch_chars,
            total_batches=len(batch_analyses),
            successful_batches=sum(batch.succeeded for batch in batch_analyses),
            failed_batches=sum(not batch.succeeded for batch in batch_analyses),
            submitted_pages=len(markdown_pages),
            skipped_non_english_pages=len(skipped_pages),
            successfully_extracted_pages=successful_extractions,
            failed_extraction_pages=len(pages) - successful_extractions,
        ),
        failed_pages=failed_pages,
        markdown_pages=manifest.markdown_pages,
        skipped_pages=skipped_pages,
        analysis_stats=build_analysis_stats(
            batch_analyses,
            related_domain_analysis,
        ),
        batches=batch_analyses,
        discovered_urls=discovered_urls,
        related_domain_analysis=related_domain_analysis,
        related_domains=related_domains,
        useful_information=consolidate_extractions(pages),
        pages=pages,
    )


async def _discover_and_crawl(
    settings: CrawlSettings,
    *,
    requested_start_url: str,
) -> tuple[LanguageDiscovery, UrlSeeding, list[CrawlResult]]:
    cloak_browser = await launch_async(
        headless=settings.headless,
        proxy=settings.proxy,
        args=[
            f"--remote-debugging-port={settings.cdp_port}",
            "--remote-debugging-address=127.0.0.1",
        ],
    )

    try:
        browser_config = BrowserConfig(
            browser_mode="cdp",
            cdp_url=f"http://127.0.0.1:{settings.cdp_port}",
            headers={"Accept-Language": settings.accept_language},
        )
        async with AsyncWebCrawler(config=browser_config) as crawler:
            language_discovery, base_result = await _discover_english_base(
                crawler,
                settings=settings,
                requested_start_url=requested_start_url,
            )
            base_url = language_discovery.selected_base_url
            preferred_languages = _preferred_languages(
                language_discovery.selected_language
            )
            url_seeding, crawl_results = await _select_and_crawl(
                crawler,
                settings=settings,
                base_url=base_url,
                base_result=base_result,
                markdown_dir=settings.markdown_dir,
                preferred_languages=preferred_languages,
            )
            crawled_urls = {result.url for result in crawl_results if result.success}
            remaining = settings.max_pages - len(crawled_urls)
            if remaining > 0:
                crawl_results.extend(
                    await _stream_discovery(
                        crawler,
                        settings=settings,
                        base_url=base_url,
                        max_new_pages=remaining,
                        already_crawled=crawled_urls,
                        preferred_languages=preferred_languages,
                    )
                )
            return language_discovery, url_seeding, crawl_results
    finally:
        await cloak_browser.close()
        LOGGER.info("CloakBrowser and Playwright have been closed")


async def _discover_english_base(
    crawler: AsyncWebCrawler,
    *,
    settings: CrawlSettings,
    requested_start_url: str,
) -> tuple[LanguageDiscovery, CrawlResult | None]:
    """Select the English base URL and return the rendered base page, if any."""
    LOGGER.info("Looking for an English website version from %s", requested_start_url)
    probe = await _crawl_single_page(
        crawler,
        url=requested_start_url,
        settings=settings,
    )
    if probe is None or not probe.success:
        error_message = (
            "Crawl4AI returned no language probe result"
            if probe is None
            else probe.error_message or "Language probe crawl failed"
        )
        return (
            LanguageDiscovery(
                requested_url=requested_start_url,
                probe_url=requested_start_url,
                probe_succeeded=False,
                selected_base_url=requested_start_url,
                selection_method="fallback_requested_url",
                error=error_message,
            ),
            None,
        )

    probe_url = normalize_start_url(probe.url or requested_start_url)
    probe_language, candidates = inspect_language_page(probe_url, probe.html or "")
    if is_english_language(probe_language):
        return (
            LanguageDiscovery(
                requested_url=requested_start_url,
                probe_url=probe_url,
                probe_succeeded=True,
                probe_language=probe_language,
                selected_base_url=probe_url,
                selected_language=probe_language,
                selection_method="already_english",
                candidates=candidates,
            ),
            probe,
        )

    for candidate in candidates[:LANGUAGE_CANDIDATE_LIMIT]:
        verification = await _crawl_single_page(
            crawler,
            url=candidate.url,
            settings=settings,
        )
        if verification is None or not verification.success:
            candidate.verification_status = "failed"
            candidate.verification_error = (
                "Crawl4AI returned no verification result"
                if verification is None
                else verification.error_message or "Candidate crawl failed"
            )
            continue

        selected_url = normalize_start_url(verification.url or candidate.url)
        candidate_language, _ = inspect_language_page(
            selected_url,
            verification.html or "",
        )
        candidate.detected_language = candidate_language
        if is_english_language(candidate_language):
            candidate.verification_status = "confirmed"
        elif candidate_language is None:
            candidate.verification_status = "reachable"
        else:
            candidate.verification_status = "rejected"
            candidate.verification_error = (
                f"Candidate declared non-English language {candidate_language!r}"
            )
            continue

        LOGGER.info(
            "Selected English base URL via %s: %s",
            candidate.detection_method,
            selected_url,
        )
        return (
            LanguageDiscovery(
                requested_url=requested_start_url,
                probe_url=probe_url,
                probe_succeeded=True,
                probe_language=probe_language,
                selected_base_url=selected_url,
                selected_language=candidate_language or "en",
                selection_method=candidate.detection_method,
                candidates=candidates,
            ),
            verification,
        )

    return (
        LanguageDiscovery(
            requested_url=requested_start_url,
            probe_url=probe_url,
            probe_succeeded=True,
            probe_language=probe_language,
            selected_base_url=probe_url,
            selected_language=probe_language,
            selection_method="fallback_requested_url",
            candidates=candidates,
            error="No verified English version was found; using the probed URL.",
        ),
        probe,
    )


def _internal_links(result: CrawlResult | None, *, base_url: str) -> list[str]:
    """Return normalized same-site links from a rendered page."""
    if result is None or not isinstance(result.links, dict):
        return []
    links: list[str] = []
    seen: set[str] = set()
    for link in result.links.get("internal", []):
        href = link.get("href") if isinstance(link, dict) else None
        if not isinstance(href, str) or not href.strip():
            continue
        absolute = urljoin(result.url or base_url, href.strip())
        if urlsplit(absolute).scheme.casefold() not in {"http", "https"}:
            continue
        try:
            normalized = normalize_start_url(absolute)
        except ValueError:
            continue
        if url_key(normalized) in seen:
            continue
        seen.add(url_key(normalized))
        links.append(normalized)
    return links


async def _crawl_single_page(
    crawler: AsyncWebCrawler,
    *,
    url: str,
    settings: CrawlSettings,
) -> CrawlResult | None:
    results = await crawler.arun(  # ty: ignore[missing-argument]
        url=url,
        config=_base_run_config(settings, stream=False),
    )
    result_list = list(results)
    return result_list[0] if result_list else None


async def _select_and_crawl(
    crawler: AsyncWebCrawler,
    *,
    settings: CrawlSettings,
    base_url: str,
    base_result: CrawlResult | None,
    markdown_dir: Path,
    preferred_languages: Collection[str] = DEFAULT_PREFERRED_LANGUAGES,
) -> tuple[UrlSeeding, list[CrawlResult]]:
    """Pick pages from the URL inventory and render them in up to two waves.

    Wave one takes ``seed_share`` of the page budget from the sitemap inventory
    plus the base page's links. Its rendered pages contribute their own links,
    the union is ranked again, and wave two fills the remaining budget. Link
    discovery afterwards only covers what both waves left open.
    """
    if not settings.seed:
        return (
            UrlSeeding(enabled=False, source=settings.seed_source, succeeded=True),
            [],
        )

    outcome = await seed_sitemap_urls(
        base_url,
        source=settings.seed_source,
        max_urls=settings.seed_max_urls,
        accept_language=settings.accept_language,
        proxy=settings.proxy,
    )
    base_links = _internal_links(base_result, base_url=base_url)

    async def fetch_heads(urls: list[str]) -> dict[str, HeadMetadata]:
        return await fetch_head_metadata(
            urls,
            accept_language=settings.accept_language,
            proxy=settings.proxy,
            concurrency=HEAD_FETCH_CONCURRENCY,
        )

    first_limit = min(
        settings.max_pages,
        max(1, math.ceil(settings.max_pages * settings.seed_share)),
    )
    first_wave = await select_pages(
        outcome.urls,
        base_url=base_url,
        limit=first_limit,
        fetch_heads=fetch_heads,
        base_page_links=base_links,
        preferred_languages=preferred_languages,
    )
    _log_selection("wave 1", first_wave)
    results = await _crawl_seeded_pages(
        crawler,
        scored_urls={scored.url: scored.score for scored in first_wave.selected},
        settings=settings,
    )

    selected = list(first_wave.selected)
    final_wave = first_wave
    waves = 1 if first_wave.selected else 0
    head_checked = first_wave.head_checked_urls
    harvested: list[str] = []
    attempted = {scored.url for scored in first_wave.selected}
    attempted.update(result.url for result in results)
    remaining = settings.max_pages - sum(result.success for result in results)
    if remaining > 0:
        attempted_keys = {url_key(url) for url in attempted}
        for result in results:
            if not result.success:
                continue
            for link in _internal_links(result, base_url=base_url):
                if url_key(link) not in attempted_keys:
                    attempted_keys.add(url_key(link))
                    harvested.append(link)
        second_wave = await select_pages(
            [*outcome.urls, *harvested],
            base_url=base_url,
            limit=remaining,
            fetch_heads=fetch_heads,
            base_page_links=base_links,
            exclude_urls=attempted,
            preferred_languages=preferred_languages,
        )
        head_checked += second_wave.head_checked_urls
        final_wave = second_wave
        if second_wave.selected:
            _log_selection("wave 2", second_wave)
            waves += 1
            results.extend(
                await _crawl_seeded_pages(
                    crawler,
                    scored_urls={
                        scored.url: scored.score for scored in second_wave.selected
                    },
                    settings=settings,
                )
            )
            selected.extend(second_wave.selected)

    inventory_urls = len(
        {url_key(url) for url in (base_url, *outcome.urls, *base_links, *harvested)}
    )
    inventory_path = markdown_dir.resolve() / INVENTORY_FILENAME
    _write_inventory(
        selected=selected,
        eligible=final_wave.eligible,
        excluded=final_wave.excluded,
        inventory_urls=inventory_urls,
        path=inventory_path,
    )
    LOGGER.info(
        "Page selection: %d inventory URL(s) incl. %d base-page link(s) and "
        "%d harvested link(s); %d selected over %d wave(s)",
        inventory_urls,
        len(base_links),
        len(harvested),
        len(selected),
        waves,
    )
    return (
        UrlSeeding(
            enabled=True,
            source=settings.seed_source,
            succeeded=outcome.error is None,
            error=outcome.error,
            inventory_urls=inventory_urls,
            eligible_urls=len(final_wave.eligible),
            excluded_urls=len(final_wave.excluded),
            head_checked_urls=head_checked,
            base_page_links=len(base_links),
            harvested_links=len(harvested),
            selection_waves=waves,
            selected=selected,
            inventory_path=str(inventory_path),
        ),
        results,
    )


def _preferred_languages(selected_language: str | None) -> frozenset[str]:
    """English plus the selected site's own language, as primary subtags."""
    preferred = {"en"}
    if selected_language:
        primary = (
            selected_language.strip().replace("_", "-").casefold().split("-", 1)[0]
        )
        if primary:
            preferred.add(primary)
    return frozenset(preferred)


def _log_selection(label: str, selection: SelectionResult) -> None:
    LOGGER.info(
        "Selection %s: %d eligible, %d excluded, %d head-checked, %d selected",
        label,
        len(selection.eligible),
        len(selection.excluded),
        selection.head_checked_urls,
        len(selection.selected),
    )
    for scored in selection.selected:
        LOGGER.debug(
            "Selected %.1f %s (%s)",
            scored.score,
            scored.url,
            ", ".join(scored.reasons),
        )


async def _crawl_seeded_pages(
    crawler: AsyncWebCrawler,
    *,
    scored_urls: dict[str, float],
    settings: CrawlSettings,
) -> list[CrawlResult]:
    """Render the selected pages directly, tagging each result with its score."""
    urls = list(scored_urls)
    if not urls:
        return []

    LOGGER.info("Crawling %d selected page(s) from the URL inventory", len(urls))
    stream = await crawler.arun_many(
        urls=urls,
        config=_base_run_config(settings, stream=True),
    )
    if not isinstance(stream, AsyncGeneratorType):
        raise TypeError("Crawl4AI did not return an async generator for arun_many")

    results: list[CrawlResult] = []
    async with aclosing(stream):
        async for result in stream:
            score = scored_urls.get(result.url)
            if score is None:
                score = next(
                    (
                        value
                        for url, value in scored_urls.items()
                        if url_key(url) == url_key(result.url)
                    ),
                    None,
                )
            metadata = dict(result.metadata or {})
            metadata.update({"depth": 0, "selection": "selected", "score": score})
            result.metadata = metadata
            results.append(result)
    return results


async def _stream_discovery(
    crawler: AsyncWebCrawler,
    *,
    settings: CrawlSettings,
    base_url: str,
    max_new_pages: int,
    already_crawled: set[str],
    preferred_languages: Collection[str] = DEFAULT_PREFERRED_LANGUAGES,
) -> list[CrawlResult]:
    """Fill the remaining page budget by following links from the base URL."""
    strategy = _discovery_strategy(
        settings,
        base_url=base_url,
        max_new_pages=max_new_pages,
        already_crawled=already_crawled,
        preferred_languages=preferred_languages,
    )
    run_config = _base_run_config(settings, stream=True)
    run_config.deep_crawl_strategy = strategy

    LOGGER.info(
        "Starting Crawl4AI %s discovery from %s: up to %d new page(s), max_depth=%d",
        settings.discovery_strategy,
        base_url,
        max_new_pages,
        settings.max_depth,
    )
    result_stream = await crawler.arun(  # ty: ignore[missing-argument]
        url=base_url,
        config=run_config,
    )
    if not isinstance(result_stream, AsyncGeneratorType):
        raise TypeError("Crawl4AI did not return an async generator for streaming")
    return await _collect_bfs_results(
        result_stream,
        max_successful_pages=max_new_pages,
        cancel=strategy.cancel,
        already_crawled=already_crawled,
    )


def _discovery_strategy(
    settings: CrawlSettings,
    *,
    base_url: str,
    max_new_pages: int,
    already_crawled: set[str],
    preferred_languages: Collection[str] = DEFAULT_PREFERRED_LANGUAGES,
) -> BestFirstCrawlingStrategy | BFSDeepCrawlStrategy:
    # Crawl4AI counts the start page and checks its limit before yielding the
    # page that reaches it; the result counter in _collect_bfs_results enforces
    # the exact number of new pages, so give the strategy one page of headroom
    # plus room for re-yielding an already crawled start page.
    strategy_budget = max_new_pages + 2
    if settings.discovery_strategy == "breadth_first":
        return BFSDeepCrawlStrategy(
            max_depth=settings.max_depth,
            include_external=settings.include_external,
            max_pages=strategy_budget,
        )
    return BestFirstCrawlingStrategy(
        max_depth=settings.max_depth,
        filter_chain=FilterChain(
            [
                SelectionFilter(
                    base_url=base_url,
                    exclude_urls=already_crawled,
                    preferred_languages=preferred_languages,
                )
            ]
        ),
        url_scorer=SelectionScorer(
            base_url=base_url,
            preferred_languages=preferred_languages,
        ),
        include_external=settings.include_external,
        max_pages=strategy_budget,
    )


def _write_inventory(
    *,
    selected: list[ScoredUrl],
    eligible: list[ScoredUrl],
    excluded: list[ScoredUrl],
    inventory_urls: int,
    path: Path,
) -> None:
    payload = {
        "inventory_urls": inventory_urls,
        "selected": [scored.model_dump(mode="json") for scored in selected],
        "eligible": [scored.model_dump(mode="json") for scored in eligible],
        "excluded": [scored.model_dump(mode="json") for scored in excluded],
    }
    _write_text_atomic(path, json.dumps(payload, indent=2, ensure_ascii=False))


async def _collect_bfs_results(
    result_stream: AsyncGenerator[CrawlResult, None],
    *,
    max_successful_pages: int,
    cancel: Callable[[], None],
    already_crawled: set[str] | frozenset[str] = frozenset(),
) -> list[CrawlResult]:
    """Collect up to the requested number of new successful Markdown candidates."""
    results: list[CrawlResult] = []
    successful_pages = 0
    known_keys = {url_key(url) for url in already_crawled}
    async with aclosing(result_stream):
        async for result in result_stream:
            results.append(result)
            if not result.success or url_key(result.url) in known_keys:
                continue
            known_keys.add(url_key(result.url))
            successful_pages += 1
            if successful_pages >= max_successful_pages:
                cancel()
                break
    return results


def persist_markdown_pages(
    crawl_results: list[CrawlResult],
    *,
    markdown_dir: Path,
    max_markdown_chars: int,
) -> tuple[list[MarkdownPage], list[FailedPage]]:
    """Persist successful crawl results before any LLM analysis starts."""
    resolved_directory = markdown_dir.resolve()
    resolved_directory.mkdir(parents=True, exist_ok=True)
    markdown_pages: list[MarkdownPage] = []
    failed_pages: list[FailedPage] = []
    seen_urls: set[str] = set()

    for crawl_result in crawl_results:
        url = crawl_result.url
        depth = _crawl_depth(crawl_result)
        if not crawl_result.success:
            failed_pages.append(
                FailedPage(
                    url=url,
                    depth=depth,
                    error=(
                        "crawl: "
                        + (
                            crawl_result.error_message
                            or "Crawl4AI returned an unsuccessful result"
                        )
                    ),
                )
            )
            continue
        if url_key(url) in seen_urls:
            continue
        seen_urls.add(url_key(url))

        markdown = truncate_markdown(
            _markdown_text(crawl_result.markdown),
            max_chars=max_markdown_chars,
        )
        markdown_path = resolved_directory / _markdown_filename(
            len(markdown_pages) + 1,
            url,
        )
        _write_text_atomic(markdown_path, markdown)
        markdown_pages.append(
            MarkdownPage(
                source_url=url,
                depth=depth,
                markdown_path=str(markdown_path),
                markdown_chars=len(markdown),
                language=detect_document_language(crawl_result.html or ""),
                selection=_crawl_selection(crawl_result),
                score=_crawl_score(crawl_result),
            )
        )

    return markdown_pages, failed_pages


def _split_pages_by_language(
    pages: list[MarkdownPage],
    *,
    skip_non_english: bool,
) -> tuple[list[MarkdownPage], list[MarkdownPage]]:
    """Separate pages to extract from pages declared in another language."""
    if not skip_non_english:
        return list(pages), []
    kept: list[MarkdownPage] = []
    skipped: list[MarkdownPage] = []
    for page in pages:
        if page.language is not None and not is_english_language(page.language):
            skipped.append(page)
        else:
            kept.append(page)
    return kept, skipped


def _crawl_strategy_label(manifest: CrawlManifest) -> str:
    if manifest.url_seeding is not None and manifest.url_seeding.enabled:
        return f"{manifest.url_seeding.source}+{manifest.discovery_strategy}"
    return manifest.discovery_strategy


def create_batches(
    pages: list[MarkdownPage],
    *,
    max_page_chars: int,
    max_batch_pages: int,
    max_batch_chars: int,
) -> list[MarkdownBatch]:
    """Group stored pages by both page count and Markdown character budget."""
    batches: list[MarkdownBatch] = []
    current_pages: list[MarkdownPage] = []
    current_chars = 0

    for page in pages:
        page_chars = min(page.markdown_chars, max_page_chars)
        page_would_overflow = (
            current_pages and current_chars + page_chars > max_batch_chars
        )
        page_limit_reached = len(current_pages) >= max_batch_pages
        if page_would_overflow or page_limit_reached:
            batches.append(
                MarkdownBatch(
                    number=len(batches) + 1,
                    pages=tuple(current_pages),
                    markdown_chars=current_chars,
                )
            )
            current_pages = []
            current_chars = 0

        current_pages.append(page)
        current_chars += page_chars

    if current_pages:
        batches.append(
            MarkdownBatch(
                number=len(batches) + 1,
                pages=tuple(current_pages),
                markdown_chars=current_chars,
            )
        )
    return batches


def discover_urls(
    pages: list[MarkdownPage],
    *,
    searched_url: str,
) -> list[DiscoveredUrl]:
    """Collect every unique HTTP(S) link from persisted Markdown."""
    searched_hostname = urlsplit(normalize_start_url(searched_url)).hostname
    if searched_hostname is None:
        return []
    searched_domain = canonical_domain(searched_hostname)

    discovered_by_url: dict[str, DiscoveredUrl] = {}
    for page in pages:
        markdown = Path(page.markdown_path).read_text(encoding="utf-8")
        for match in MARKDOWN_LINK_PATTERN.finditer(markdown):
            raw_url = match.group(1)
            absolute_url = urljoin(page.source_url, raw_url)
            if urlsplit(absolute_url).scheme.casefold() not in {"http", "https"}:
                continue
            try:
                normalized_url = normalize_start_url(absolute_url)
            except ValueError:
                LOGGER.debug(
                    "Ignoring invalid Markdown link on %s: %s",
                    page.source_url,
                    raw_url,
                )
                continue

            parsed_url = urlsplit(normalized_url)
            if parsed_url.hostname is None:
                continue
            domain = canonical_domain(parsed_url.hostname)
            label = _markdown_link_label(markdown, match.start())
            discovered_url = discovered_by_url.get(normalized_url)
            if discovered_url is None:
                discovered_by_url[normalized_url] = DiscoveredUrl(
                    url=normalized_url,
                    domain=domain,
                    link_type=(
                        "internal"
                        if same_domain_tree(domain, searched_domain)
                        else "external"
                    ),
                    labels=[label] if label else [],
                    source_urls=[page.source_url],
                    occurrences=1,
                )
                continue

            discovered_url.occurrences += 1
            if label and label.casefold() not in {
                existing.casefold() for existing in discovered_url.labels
            }:
                discovered_url.labels.append(label)
            if page.source_url not in discovered_url.source_urls:
                discovered_url.source_urls.append(page.source_url)

    return list(discovered_by_url.values())


def _markdown_link_label(markdown: str, destination_start: int) -> str:
    line_start = markdown.rfind("\n", 0, destination_start) + 1
    label_start = markdown.rfind("[", line_start, destination_start)
    if label_start < 0:
        return ""
    label = markdown[label_start + 1 : destination_start]
    if "](" in label:
        return ""
    return re.sub(r"\s+", " ", label).strip()[:200]


def _external_domain_candidates(
    discovered_urls: list[DiscoveredUrl],
) -> list[DiscoveredDomainCandidate]:
    urls_by_domain: dict[str, list[DiscoveredUrl]] = {}
    for discovered_url in discovered_urls:
        if discovered_url.link_type == "external":
            urls_by_domain.setdefault(discovered_url.domain, []).append(discovered_url)

    candidates: list[DiscoveredDomainCandidate] = []
    for domain, domain_urls in urls_by_domain.items():
        first_url = urlsplit(domain_urls[0].url)
        candidates.append(
            DiscoveredDomainCandidate(
                domain=domain,
                homepage_url=urlunsplit(
                    (first_url.scheme, first_url.netloc, "/", "", "")
                ),
                discovered_urls=[
                    discovered_url.url[:DOMAIN_URL_SAMPLE_CHAR_LIMIT]
                    for discovered_url in domain_urls
                ][:DOMAIN_URL_SAMPLE_LIMIT],
                labels=_unique_labels(domain_urls)[:DOMAIN_LABEL_SAMPLE_LIMIT],
                occurrences=sum(
                    discovered_url.occurrences for discovered_url in domain_urls
                ),
                source_page_count=len(
                    {
                        source_url
                        for discovered_url in domain_urls
                        for source_url in discovered_url.source_urls
                    }
                ),
            )
        )
    return candidates


def _unique_labels(discovered_urls: list[DiscoveredUrl]) -> list[str]:
    labels: list[str] = []
    seen_labels: set[str] = set()
    for discovered_url in discovered_urls:
        for label in discovered_url.labels:
            normalized_label = label.casefold()
            if normalized_label in seen_labels:
                continue
            seen_labels.add(normalized_label)
            labels.append(label)
    return labels


async def _analyze_related_domains(
    discovered_urls: list[DiscoveredUrl],
    *,
    searched_url: str,
    timeout_seconds: int,
) -> tuple[list[RelatedDomain], RelatedDomainAnalysis]:
    candidates = _external_domain_candidates(discovered_urls)
    if not candidates:
        return [], RelatedDomainAnalysis(
            attempted=False,
            candidate_domains=0,
            succeeded=True,
        )

    outcome = await run_structured_turn(
        prompt=create_related_domains_prompt(searched_url, candidates=candidates),
        base_instructions=(
            "Classify only supplied candidate domains. Do not navigate or "
            "use tools. Return only data matching the schema."
        ),
        output_model=RelatedDomainSelection,
        timeout_seconds=timeout_seconds,
        operation_name="related-domain classification",
    )
    if outcome.value is None:
        return [], RelatedDomainAnalysis(
            attempted=True,
            candidate_domains=len(candidates),
            succeeded=False,
            error=outcome.error,
            token_usage=outcome.token_usage,
        )
    selection = outcome.value
    token_usage = outcome.token_usage

    related_domains, warnings = _validate_related_domain_selection(
        selection,
        candidates=candidates,
        discovered_urls=discovered_urls,
    )
    return related_domains, RelatedDomainAnalysis(
        attempted=True,
        candidate_domains=len(candidates),
        succeeded=True,
        warnings=warnings,
        token_usage=token_usage,
    )


def _validate_related_domain_selection(
    selection: RelatedDomainSelection,
    *,
    candidates: list[DiscoveredDomainCandidate],
    discovered_urls: list[DiscoveredUrl],
) -> tuple[list[RelatedDomain], list[str]]:
    candidates_by_domain = {candidate.domain: candidate for candidate in candidates}
    related_domains: list[RelatedDomain] = []
    warnings: list[str] = []
    selected_domains: set[str] = set()

    for decision in selection.related_domains:
        candidate = candidates_by_domain.get(decision.domain)
        if candidate is None:
            warnings.append(f"Ignored unknown related domain: {decision.domain}")
            continue
        if decision.domain in selected_domains:
            warnings.append(f"Ignored duplicate related domain: {decision.domain}")
            continue
        selected_domains.add(decision.domain)
        related_domains.append(
            RelatedDomain(
                domain=candidate.domain,
                url=candidate.homepage_url,
                relationship=decision.relationship,
                reason=decision.reason,
                labels=candidate.labels,
                source_urls=_source_urls_for_domain(
                    candidate.domain,
                    discovered_urls=discovered_urls,
                ),
                occurrences=candidate.occurrences,
            )
        )

    return related_domains, warnings


def _source_urls_for_domain(
    domain: str,
    *,
    discovered_urls: list[DiscoveredUrl],
) -> list[str]:
    source_urls: list[str] = []
    for discovered_url in discovered_urls:
        if discovered_url.domain != domain:
            continue
        for source_url in discovered_url.source_urls:
            if source_url not in source_urls:
                source_urls.append(source_url)
    return source_urls


async def _analyze_batches(
    batches: list[MarkdownBatch],
    *,
    max_page_chars: int,
    timeout_seconds: int,
) -> tuple[list[PageExtraction], list[BatchAnalysis], list[FailedPage]]:
    pages: list[PageExtraction] = []
    analyses: list[BatchAnalysis] = []
    failed_pages: list[FailedPage] = []
    if not batches:
        return pages, analyses, failed_pages

    for batch in batches:
        LOGGER.info(
            "Analyzing Markdown batch %d/%d (%d pages, %d chars)",
            batch.number,
            len(batches),
            len(batch.pages),
            batch.markdown_chars,
        )
        try:
            outcome = await _analyze_batch(
                batch=batch,
                max_page_chars=max_page_chars,
                timeout_seconds=timeout_seconds,
            )
        except Exception as error:
            LOGGER.exception("LLM batch %d failed", batch.number)
            outcome = BatchOutcome(
                extraction=BatchExtraction(),
                token_usage=None,
                error=str(error),
            )

        validated, warnings, page_failures = _validate_batch_output(
            batch,
            outcome=outcome,
        )
        pages.extend(validated)
        failed_pages.extend(page_failures)
        analyses.append(
            BatchAnalysis(
                batch_number=batch.number,
                page_urls=[page.source_url for page in batch.pages],
                markdown_chars=batch.markdown_chars,
                succeeded=outcome.error is None,
                error=outcome.error,
                warnings=warnings,
                token_usage=outcome.token_usage,
            )
        )

    return pages, analyses, failed_pages


async def _analyze_batch(
    *,
    batch: MarkdownBatch,
    max_page_chars: int,
    timeout_seconds: int,
) -> BatchOutcome:
    prompt_pages = [
        (
            page,
            truncate_markdown(
                Path(page.markdown_path).read_text(encoding="utf-8"),
                max_chars=max_page_chars,
            ),
        )
        for page in batch.pages
    ]
    outcome = await run_structured_turn(
        prompt=create_prompt(batch.number, pages=prompt_pages),
        base_instructions=(
            "Extract page-separated structured facts only from supplied Markdown. "
            "Do not navigate or use tools. Return only data matching the schema."
        ),
        output_model=BatchExtraction,
        timeout_seconds=timeout_seconds,
        operation_name=f"analysis batch {batch.number} ({len(batch.pages)} pages)",
    )
    return BatchOutcome(
        extraction=outcome.value or BatchExtraction(),
        token_usage=outcome.token_usage,
        error=outcome.error,
    )


def _validate_batch_output(
    batch: MarkdownBatch,
    *,
    outcome: BatchOutcome,
) -> tuple[list[PageExtraction], list[str], list[FailedPage]]:
    extracted_by_url: dict[str, PageExtraction] = {}
    warnings: list[str] = []
    expected_urls = {page.source_url for page in batch.pages}

    for extraction in outcome.extraction.pages:
        if extraction.source_url not in expected_urls:
            warnings.append(f"Ignored unknown source URL: {extraction.source_url}")
            continue
        if extraction.source_url in extracted_by_url:
            warnings.append(f"Ignored duplicate source URL: {extraction.source_url}")
            continue
        extracted_by_url[extraction.source_url] = extraction

    validated: list[PageExtraction] = []
    failed_pages: list[FailedPage] = []
    for page in batch.pages:
        extraction = extracted_by_url.get(page.source_url)
        error = outcome.error
        if extraction is None:
            extraction = PageExtraction(source_url=page.source_url)
            if error is None:
                error = "LLM omitted this page from the batch response"
                warnings.append(f"Missing page response: {page.source_url}")
        else:
            set_source_url(extraction, page.source_url)

        extraction.extraction_metadata = PageExtractionMetadata(
            depth=page.depth,
            markdown_path=page.markdown_path,
            batch_number=batch.number,
            succeeded=error is None,
            error=error,
        )
        validated.append(extraction)
        if error is not None:
            failed_pages.append(
                FailedPage(
                    url=page.source_url,
                    depth=page.depth,
                    error=f"analysis batch {batch.number}: {error}",
                )
            )

    return validated, warnings, failed_pages


def save_report(report: ResearchReport, path: Path) -> None:
    save_model(report, path)


def load_manifest(path: Path) -> CrawlManifest:
    """Load a crawl manifest and resolve every referenced Markdown file."""
    try:
        manifest = CrawlManifest.model_validate_json(path.read_text(encoding="utf-8"))
    except ValidationError as error:
        raise ValueError(f"Invalid crawl manifest {path}: {error}") from error

    resolved_pages: list[MarkdownPage] = []
    missing_paths: list[Path] = []
    for page in manifest.markdown_pages:
        markdown_path = Path(page.markdown_path)
        if not markdown_path.is_absolute():
            markdown_path = path.parent / markdown_path
        markdown_path = markdown_path.resolve()
        if not markdown_path.is_file():
            missing_paths.append(markdown_path)
            continue
        resolved_pages.append(
            page.model_copy(
                update={
                    "markdown_path": str(markdown_path),
                    "markdown_chars": len(markdown_path.read_text(encoding="utf-8")),
                }
            )
        )

    if missing_paths:
        formatted = ", ".join(str(missing_path) for missing_path in missing_paths)
        raise FileNotFoundError(f"Manifest Markdown file(s) not found: {formatted}")

    return manifest.model_copy(update={"markdown_pages": resolved_pages})


def save_model(model: BaseModel, path: Path) -> None:
    """Atomically persist a Pydantic model as formatted JSON."""
    _write_text_atomic(path, model.model_dump_json(indent=2))


def truncate_markdown(markdown: str, *, max_chars: int) -> str:
    """Keep the beginning and footer of oversized pages."""
    if len(markdown) <= max_chars:
        return markdown
    marker = "\n\n[... page Markdown truncated ...]\n\n"
    if max_chars <= len(marker):
        return markdown[:max_chars]
    content_chars = max_chars - len(marker)
    head_size = content_chars * 3 // 4
    tail_size = content_chars - head_size
    return markdown[:head_size] + marker + markdown[-tail_size:]


def _base_run_config(
    settings: CrawlSettings,
    *,
    stream: bool,
) -> CrawlerRunConfig:
    return CrawlerRunConfig(
        stream=stream,
        locale=_primary_locale(settings.accept_language),
        check_robots_txt=settings.check_robots_txt,
        page_timeout=60_000,
        semaphore_count=settings.crawl_concurrency,
        remove_overlay_elements=True,
        remove_consent_popups=True,
        scan_full_page=True,
        max_scroll_steps=10,
        preserve_https_for_internal_links=True,
        verbose=False,
    )


def _primary_locale(accept_language: str) -> str | None:
    first = accept_language.split(",", 1)[0].split(";", 1)[0].strip()
    return first or None


def _crawl_stats(
    settings: CrawlSettings,
    crawl_results: list[CrawlResult],
    markdown_pages: list[MarkdownPage],
) -> CrawlStats:
    successful_pages = sum(result.success for result in crawl_results)
    return CrawlStats(
        configured_max_pages=settings.max_pages,
        configured_max_depth=settings.max_depth,
        include_external=settings.include_external,
        pages_returned=len(crawl_results),
        successful_pages=successful_pages,
        failed_pages=len(crawl_results) - successful_pages,
        stored_markdown_pages=len(markdown_pages),
        stored_markdown_chars=sum(page.markdown_chars for page in markdown_pages),
        max_depth_reached=max(
            (_crawl_depth(result) for result in crawl_results),
            default=0,
        ),
        selected_pages=sum(page.selection == "selected" for page in markdown_pages),
        discovered_pages=sum(page.selection == "discovery" for page in markdown_pages),
    )


def _crawl_depth(result: CrawlResult) -> int:
    metadata = result.metadata
    if not isinstance(metadata, dict):
        return 0
    depth = metadata.get("depth")
    return depth if isinstance(depth, int) and depth >= 0 else 0


def _crawl_selection(result: CrawlResult) -> Literal["selected", "discovery"]:
    metadata = result.metadata
    if isinstance(metadata, dict) and metadata.get("selection") == "selected":
        return "selected"
    return "discovery"


def _crawl_score(result: CrawlResult) -> float | None:
    metadata = result.metadata
    if not isinstance(metadata, dict):
        return None
    score = metadata.get("score")
    return float(score) if isinstance(score, (int, float)) else None


def _markdown_text(value: object) -> str:
    if isinstance(value, str):
        return value
    raw_markdown = getattr(value, "raw_markdown", None)
    return raw_markdown if isinstance(raw_markdown, str) else ""


def _markdown_filename(index: int, url: str) -> str:
    parsed = urlsplit(url)
    source_name = f"{parsed.hostname or 'page'}-{parsed.path.strip('/') or 'home'}"
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", source_name).strip("-").casefold()
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:10]
    return f"{index:04d}-{slug[:70]}-{digest}.md"


def _write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.tmp")
    temporary_path.write_text(content, encoding="utf-8")
    temporary_path.replace(path)
