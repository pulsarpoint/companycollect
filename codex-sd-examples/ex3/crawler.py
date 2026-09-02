import asyncio
import hashlib
import logging
import re
from collections.abc import AsyncGenerator, Callable
from contextlib import aclosing
from dataclasses import dataclass
from pathlib import Path
from types import AsyncGeneratorType
from urllib.parse import urlsplit, urlunsplit

from cloakbrowser import launch_async
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.models import CrawlResult
from openai_codex import AsyncCodex, AsyncTurnHandle, Sandbox, TurnResult
from pydantic import BaseModel, ValidationError

from ex1.aux import print_usage
from ex1.models import AnalysisTokenUsage, FailedPage
from ex3.language import inspect_language_page, is_english_language
from ex3.models import (
    BatchAnalysis,
    BatchExtraction,
    BatchStats,
    CrawlManifest,
    CrawlStats,
    LanguageDiscovery,
    MarkdownPage,
    PageExtraction,
    PageExtractionMetadata,
    ResearchReport,
    batch_extraction_output_schema,
    build_analysis_stats,
    consolidate_extractions,
    set_source_url,
)
from ex3.prompty import create_prompt

LOGGER = logging.getLogger(__name__)
LANGUAGE_CANDIDATE_LIMIT = 5
TURN_INTERRUPT_TIMEOUT_SECONDS = 5
TURN_COMPLETION_TIMEOUT_SECONDS = 10


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


@dataclass(frozen=True, slots=True)
class AnalysisSettings:
    manifest_path: Path
    max_page_chars: int
    max_batch_pages: int
    max_batch_chars: int
    analysis_timeout_seconds: int


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
    """Discover an English base URL and persist a complete bounded BFS crawl."""
    requested_start_url = normalize_start_url(settings.start_url)
    language_discovery, crawl_results = await _discover_and_crawl(
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
    markdown_pages = manifest.markdown_pages
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
        stopped_reason=manifest.stopped_reason,
        manifest_path=str(settings.manifest_path.resolve()),
        language_discovery=manifest.language_discovery,
        crawl_stats=manifest.crawl_stats,
        batch_stats=BatchStats(
            configured_max_page_chars=settings.max_page_chars,
            configured_max_batch_pages=settings.max_batch_pages,
            configured_max_batch_chars=settings.max_batch_chars,
            total_batches=len(batch_analyses),
            successful_batches=sum(batch.succeeded for batch in batch_analyses),
            failed_batches=sum(not batch.succeeded for batch in batch_analyses),
            submitted_pages=len(markdown_pages),
            successfully_extracted_pages=successful_extractions,
            failed_extraction_pages=len(pages) - successful_extractions,
        ),
        failed_pages=failed_pages,
        markdown_pages=markdown_pages,
        analysis_stats=build_analysis_stats(batch_analyses),
        batches=batch_analyses,
        useful_information=consolidate_extractions(pages),
        pages=pages,
    )


async def _discover_and_crawl(
    settings: CrawlSettings,
    *,
    requested_start_url: str,
) -> tuple[LanguageDiscovery, list[CrawlResult]]:
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
        )
        async with AsyncWebCrawler(config=browser_config) as crawler:
            language_discovery = await _discover_english_base(
                crawler,
                settings=settings,
                requested_start_url=requested_start_url,
            )
            crawl_results = await _stream_bfs(
                crawler,
                settings=settings,
                base_url=language_discovery.selected_base_url,
            )
            return language_discovery, crawl_results
    finally:
        await cloak_browser.close()
        LOGGER.info("CloakBrowser and Playwright have been closed")


async def _discover_english_base(
    crawler: AsyncWebCrawler,
    *,
    settings: CrawlSettings,
    requested_start_url: str,
) -> LanguageDiscovery:
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
        return LanguageDiscovery(
            requested_url=requested_start_url,
            probe_url=requested_start_url,
            probe_succeeded=False,
            selected_base_url=requested_start_url,
            selection_method="fallback_requested_url",
            error=error_message,
        )

    probe_url = normalize_start_url(probe.url or requested_start_url)
    probe_language, candidates = inspect_language_page(probe_url, probe.html or "")
    if is_english_language(probe_language):
        return LanguageDiscovery(
            requested_url=requested_start_url,
            probe_url=probe_url,
            probe_succeeded=True,
            probe_language=probe_language,
            selected_base_url=probe_url,
            selected_language=probe_language,
            selection_method="already_english",
            candidates=candidates,
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
        return LanguageDiscovery(
            requested_url=requested_start_url,
            probe_url=probe_url,
            probe_succeeded=True,
            probe_language=probe_language,
            selected_base_url=selected_url,
            selected_language=candidate_language or "en",
            selection_method=candidate.detection_method,
            candidates=candidates,
        )

    return LanguageDiscovery(
        requested_url=requested_start_url,
        probe_url=probe_url,
        probe_succeeded=True,
        probe_language=probe_language,
        selected_base_url=probe_url,
        selected_language=probe_language,
        selection_method="fallback_requested_url",
        candidates=candidates,
        error="No verified English version was found; using the probed URL.",
    )


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


async def _stream_bfs(
    crawler: AsyncWebCrawler,
    *,
    settings: CrawlSettings,
    base_url: str,
) -> list[CrawlResult]:
    strategy = BFSDeepCrawlStrategy(
        max_depth=settings.max_depth,
        include_external=settings.include_external,
        # Crawl4AI 0.9.3 checks its streaming limit before yielding the page
        # that reaches it. Our result counter below enforces the exact hard cap.
        max_pages=settings.max_pages + 1,
    )
    run_config = _base_run_config(settings, stream=True)
    run_config.deep_crawl_strategy = strategy

    LOGGER.info(
        "Starting Crawl4AI BFS from %s: max_pages=%d, max_depth=%d",
        base_url,
        settings.max_pages,
        settings.max_depth,
    )
    result_stream = await crawler.arun(  # ty: ignore[missing-argument]
        url=base_url,
        config=run_config,
    )
    if not isinstance(result_stream, AsyncGeneratorType):
        raise TypeError("Crawl4AI did not return an async generator for streaming BFS")
    return await _collect_bfs_results(
        result_stream,
        max_successful_pages=settings.max_pages,
        cancel=strategy.cancel,
    )


async def _collect_bfs_results(
    result_stream: AsyncGenerator[CrawlResult, None],
    *,
    max_successful_pages: int,
    cancel: Callable[[], None],
) -> list[CrawlResult]:
    """Collect up to the requested number of successful Markdown candidates."""
    results: list[CrawlResult] = []
    successful_pages = 0
    async with aclosing(result_stream):
        async for result in result_stream:
            results.append(result)
            if not result.success:
                continue
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
        if url in seen_urls:
            continue
        seen_urls.add(url)

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
            )
        )

    return markdown_pages, failed_pages


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
        except TimeoutError:
            outcome = BatchOutcome(
                extraction=BatchExtraction(),
                token_usage=None,
                error=f"analysis timed out after {timeout_seconds} seconds",
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
    # A separate client per batch keeps a failed or force-closed Codex process from
    # affecting later batches. It also gives each batch a firm lifecycle boundary.
    async with AsyncCodex() as codex:
        thread = await codex.thread_start(
            base_instructions=(
                "Extract page-separated structured facts only from supplied Markdown. "
                "Do not navigate or use tools. Return only data matching the schema."
            ),
            ephemeral=True,
            sandbox=Sandbox.read_only,
        )
        turn = await thread.turn(
            create_prompt(batch.number, pages=prompt_pages),
            output_schema=batch_extraction_output_schema(),
            sandbox=Sandbox.read_only,
        )
        result, timed_out = await _run_turn_with_timeout(
            codex,
            turn,
            timeout_seconds=timeout_seconds,
            batch_number=batch.number,
        )

    if result is None:
        return BatchOutcome(
            extraction=BatchExtraction(),
            token_usage=None,
            error=f"analysis timed out after {timeout_seconds} seconds",
        )

    token_usage = print_usage(
        result,
        page_url=f"batch {batch.number} ({len(batch.pages)} pages)",
    )
    if timed_out:
        return BatchOutcome(
            extraction=BatchExtraction(),
            token_usage=token_usage,
            error=f"analysis timed out after {timeout_seconds} seconds",
        )
    if result.final_response is None:
        error_message = (
            result.error.message if result.error is not None else result.status
        )
        return BatchOutcome(
            extraction=BatchExtraction(),
            token_usage=token_usage,
            error=f"Codex returned no final response: {error_message}",
        )

    try:
        extraction = BatchExtraction.model_validate_json(result.final_response)
    except ValidationError as error:
        return BatchOutcome(
            extraction=BatchExtraction(),
            token_usage=token_usage,
            error=f"Codex returned invalid structured data: {error}",
        )
    return BatchOutcome(
        extraction=extraction,
        token_usage=token_usage,
        error=None,
    )


async def _run_turn_with_timeout(
    codex: AsyncCodex,
    turn: AsyncTurnHandle,
    *,
    timeout_seconds: int,
    batch_number: int,
) -> tuple[TurnResult | None, bool]:
    """Run a turn without cancelling its blocking notification worker."""
    turn_task = asyncio.create_task(turn.run())
    completed, _ = await asyncio.wait({turn_task}, timeout=timeout_seconds)
    if turn_task in completed:
        return turn_task.result(), False

    LOGGER.warning(
        "Analysis batch %d timed out after %d seconds; interrupting it",
        batch_number,
        timeout_seconds,
    )
    interrupt_task = asyncio.create_task(turn.interrupt())
    interrupted, _ = await asyncio.wait(
        {interrupt_task},
        timeout=TURN_INTERRUPT_TIMEOUT_SECONDS,
    )
    interrupt_succeeded = False
    if interrupt_task in interrupted:
        try:
            interrupt_task.result()
            interrupt_succeeded = True
        except Exception:
            LOGGER.exception(
                "Could not interrupt timed-out batch %d",
                batch_number,
            )

    turn_completed: set[asyncio.Task[TurnResult]] = set()
    if interrupt_succeeded:
        turn_completed, _ = await asyncio.wait(
            {turn_task},
            timeout=TURN_COMPLETION_TIMEOUT_SECONDS,
        )

    if turn_task not in turn_completed or interrupt_task not in interrupted:
        LOGGER.warning(
            "Closing the Codex client for timed-out batch %d to release its waiters",
            batch_number,
        )
        await codex.close()

    # Closing the client sends a transport error to every still-registered waiter.
    # Drain both tasks before returning so asyncio has no executor work left at exit.
    turn_outcome, _ = await asyncio.gather(
        turn_task,
        interrupt_task,
        return_exceptions=True,
    )
    result = turn_outcome if isinstance(turn_outcome, TurnResult) else None
    return result, True


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


def normalize_start_url(url: str) -> str:
    """Normalize and validate an absolute HTTP(S) URL."""
    try:
        parsed = urlsplit(url.strip())
        port = parsed.port
    except ValueError as error:
        raise ValueError(f"Start URL is not valid: {url}") from error

    scheme = parsed.scheme.casefold()
    hostname = parsed.hostname
    if scheme not in {"http", "https"} or hostname is None:
        raise ValueError(f"Start URL must be an absolute HTTP(S) URL: {url}")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Start URL must not contain credentials")

    normalized_hostname = hostname.casefold()
    if ":" in normalized_hostname:
        normalized_hostname = f"[{normalized_hostname}]"
    default_port = (scheme == "http" and port == 80) or (
        scheme == "https" and port == 443
    )
    netloc = normalized_hostname
    if port is not None and not default_port:
        netloc = f"{normalized_hostname}:{port}"
    return urlunsplit((scheme, netloc, parsed.path or "/", parsed.query, ""))


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
    )


def _crawl_depth(result: CrawlResult) -> int:
    metadata = result.metadata
    if not isinstance(metadata, dict):
        return 0
    depth = metadata.get("depth")
    return depth if isinstance(depth, int) and depth >= 0 else 0


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
