import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from cloakbrowser import launch_async
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.models import CrawlResult
from openai_codex import AsyncCodex, Sandbox
from pydantic import ValidationError

from ex1.aux import print_usage
from ex1.models import AnalysisTokenUsage, FailedPage, PageAnalysisMetadata
from ex2.models import (
    PageExtraction,
    ResearchReport,
    build_report,
    page_extraction_output_schema,
    set_source_url,
)
from ex2.prompty import create_prompt

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CrawlSettings:
    start_url: str
    output_path: Path
    max_pages: int
    max_depth: int
    max_markdown_chars: int
    analysis_timeout_seconds: int
    crawl_concurrency: int
    cdp_port: int
    headless: bool
    include_external: bool
    check_robots_txt: bool
    proxy: str | None


@dataclass(frozen=True, slots=True)
class AnalysisOutcome:
    extraction: PageExtraction
    token_usage: AnalysisTokenUsage | None
    error: str | None


async def run_crawl(settings: CrawlSettings) -> ResearchReport:
    """Let Crawl4AI perform BFS, then use Codex only for page extraction."""
    start_url = normalize_start_url(settings.start_url)
    crawl_results = await _crawl_with_bfs(settings, start_url=start_url)

    pages: list[PageExtraction] = []
    failed_pages: list[FailedPage] = []
    successful_crawls = 0

    async with AsyncCodex() as codex:
        for index, crawl_result in enumerate(crawl_results, start=1):
            url = crawl_result.url or start_url
            depth = _crawl_depth(crawl_result)
            LOGGER.info(
                "Extracting page %d/%d at depth %d: %s",
                index,
                len(crawl_results),
                depth,
                url,
            )

            if not crawl_result.success:
                error_message = (
                    crawl_result.error_message
                    or "Crawl4AI returned an unsuccessful result"
                )
                failed_pages.append(
                    FailedPage(url=url, depth=depth, error=f"crawl: {error_message}")
                )
                continue

            successful_crawls += 1
            markdown = truncate_markdown(
                _markdown_text(crawl_result.markdown),
                max_chars=settings.max_markdown_chars,
            )
            try:
                outcome = await _analyze_page(
                    codex,
                    url=url,
                    markdown=markdown,
                    timeout_seconds=settings.analysis_timeout_seconds,
                )
            except TimeoutError:
                LOGGER.error(
                    "LLM extraction timed out after %d seconds for %s",
                    settings.analysis_timeout_seconds,
                    url,
                )
                outcome = AnalysisOutcome(
                    extraction=PageExtraction(source_url=url),
                    token_usage=None,
                    error=(
                        "analysis timed out after "
                        f"{settings.analysis_timeout_seconds} seconds"
                    ),
                )
            except Exception as error:
                LOGGER.exception("LLM extraction failed for %s", url)
                outcome = AnalysisOutcome(
                    extraction=PageExtraction(source_url=url),
                    token_usage=None,
                    error=str(error),
                )

            if outcome.error is not None:
                failed_pages.append(
                    FailedPage(
                        url=url,
                        depth=depth,
                        error=f"analysis: {outcome.error}",
                    )
                )

            outcome.extraction.analysis_metadata = PageAnalysisMetadata(
                depth=depth,
                succeeded=outcome.error is None,
                error=outcome.error,
                token_usage=outcome.token_usage,
            )
            pages.append(outcome.extraction)

    crawled_urls = [result.url or start_url for result in crawl_results]
    return build_report(
        start_url=start_url,
        max_pages=settings.max_pages,
        max_depth=settings.max_depth,
        include_external=settings.include_external,
        crawled_urls=crawled_urls,
        crawl_depths=[_crawl_depth(result) for result in crawl_results],
        successful_crawls=successful_crawls,
        failed_pages=failed_pages,
        pages=pages,
    )


async def _crawl_with_bfs(
    settings: CrawlSettings,
    *,
    start_url: str,
) -> list[CrawlResult]:
    strategy = BFSDeepCrawlStrategy(
        max_depth=settings.max_depth,
        include_external=settings.include_external,
        # Crawl4AI 0.9.3 checks its streaming limit before yielding the page
        # that reaches it. Our result counter below enforces the exact hard cap.
        max_pages=settings.max_pages + 1,
    )
    run_config = CrawlerRunConfig(
        deep_crawl_strategy=strategy,
        stream=True,
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
            LOGGER.info(
                "Starting Crawl4AI BFS: max_pages=%d, max_depth=%d",
                settings.max_pages,
                settings.max_depth,
            )
            result_stream = await crawler.arun(
                url=start_url,
                config=run_config,
            )
            results: list[CrawlResult] = []
            async for result in result_stream:
                results.append(result)
                if len(results) >= settings.max_pages:
                    strategy.cancel()
                    break
            return results
    finally:
        await cloak_browser.close()


async def _analyze_page(
    codex: AsyncCodex,
    *,
    url: str,
    markdown: str,
    timeout_seconds: int,
) -> AnalysisOutcome:
    thread = await codex.thread_start(
        base_instructions=(
            "Extract structured facts only from the supplied website Markdown. "
            "Do not navigate, use tools, or select links. Return only data matching "
            "the requested output schema."
        ),
        ephemeral=True,
        sandbox=Sandbox.read_only,
    )
    turn = await thread.turn(
        create_prompt(url, markdown=markdown),
        output_schema=page_extraction_output_schema(),
        sandbox=Sandbox.read_only,
    )
    try:
        async with asyncio.timeout(timeout_seconds):
            result = await turn.run()
    except TimeoutError:
        try:
            async with asyncio.timeout(5):
                await turn.interrupt()
        except Exception:
            LOGGER.exception("Could not interrupt timed-out Codex turn for %s", url)
        raise

    token_usage = print_usage(result, page_url=url)
    if result.final_response is None:
        error_message = (
            result.error.message if result.error is not None else result.status
        )
        return AnalysisOutcome(
            extraction=PageExtraction(source_url=url),
            token_usage=token_usage,
            error=f"Codex returned no final response: {error_message}",
        )

    try:
        extraction = PageExtraction.model_validate_json(result.final_response)
    except ValidationError as error:
        return AnalysisOutcome(
            extraction=PageExtraction(source_url=url),
            token_usage=token_usage,
            error=f"Codex returned invalid structured data: {error}",
        )

    set_source_url(extraction, url)
    return AnalysisOutcome(
        extraction=extraction,
        token_usage=token_usage,
        error=None,
    )


def normalize_start_url(url: str) -> str:
    """Normalize and validate a crawl's HTTP(S) start URL."""
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

    head_size = max_chars * 3 // 4
    tail_size = max_chars - head_size
    return (
        markdown[:head_size]
        + "\n\n[... page markdown truncated by crawler ...]\n\n"
        + markdown[-tail_size:]
    )


def save_report(report: ResearchReport, path: Path) -> None:
    """Atomically write the consolidated report."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.tmp")
    temporary_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    temporary_path.replace(path)


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
