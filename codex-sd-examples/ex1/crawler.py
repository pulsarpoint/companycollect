import heapq
import logging
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from cloakbrowser import launch_async
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from openai_codex import AsyncCodex, Sandbox
from pydantic import ValidationError

from ex1.aux import print_usage
from ex1.models import (
    AnalysisTokenUsage,
    CandidateLink,
    CrawlState,
    FailedPage,
    LinkRecommendation,
    PageAnalysis,
    PageAnalysisMetadata,
    PendingUrl,
    ResearchReport,
    aggregate_analysis_stats,
    consolidate_information,
    page_analysis_output_schema,
    set_source_url,
)
from ex1.prompty import create_prompt

LOGGER = logging.getLogger(__name__)

TRACKING_QUERY_PARAMETERS = frozenset(
    {
        "fbclid",
        "gclid",
        "mc_cid",
        "mc_eid",
        "ref",
        "referrer",
    }
)
BLOCKED_PATH_SEGMENTS = frozenset(
    {
        "cart",
        "checkout",
        "login",
        "logout",
        "register",
        "signin",
        "signout",
        "signup",
    }
)
BLOCKED_FILE_SUFFIXES = (
    ".7z",
    ".avi",
    ".bmp",
    ".css",
    ".doc",
    ".docx",
    ".eot",
    ".gif",
    ".gz",
    ".ico",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".mov",
    ".mp3",
    ".mp4",
    ".pdf",
    ".png",
    ".rar",
    ".rss",
    ".svg",
    ".tar",
    ".tif",
    ".tiff",
    ".ttf",
    ".wav",
    ".webm",
    ".webp",
    ".woff",
    ".woff2",
    ".xls",
    ".xlsx",
    ".xml",
    ".zip",
)
LINK_KEYWORDS = (
    (0, ("contact", "location", "office", "support")),
    (10, ("about", "company", "corporate", "imprint", "legal", "who-we-are")),
    (20, ("product", "service", "solution", "pricing", "platform")),
    (30, ("career", "job", "join-us", "vacancy", "work-with-us")),
)

type PendingHeap = list[tuple[int, int, int, str]]
type RawLinks = dict[str, list[dict[str, object]]]


@dataclass(frozen=True, slots=True)
class CrawlSettings:
    start_url: str
    output_path: Path
    state_path: Path
    max_pages: int
    max_depth: int
    max_prompt_links: int
    max_markdown_chars: int
    cdp_port: int
    headless: bool
    allow_external: bool
    check_robots_txt: bool
    proxy: str | None


@dataclass(frozen=True, slots=True)
class AnalysisOutcome:
    analysis: PageAnalysis
    token_usage: AnalysisTokenUsage | None
    error: str | None


def normalize_url(url: str, *, base_url: str) -> str | None:
    """Return a stable HTTP(S) URL or None when the value is not crawlable."""
    try:
        parsed = urlsplit(urljoin(base_url, url.strip()))
        port = parsed.port
    except ValueError:
        return None

    scheme = parsed.scheme.casefold()
    hostname = parsed.hostname
    if scheme not in {"http", "https"} or hostname is None:
        return None
    if parsed.username is not None or parsed.password is not None:
        return None

    normalized_hostname = hostname.casefold()
    if ":" in normalized_hostname:
        normalized_hostname = f"[{normalized_hostname}]"

    default_port = (scheme == "http" and port == 80) or (
        scheme == "https" and port == 443
    )
    netloc = normalized_hostname
    if port is not None and not default_port:
        netloc = f"{normalized_hostname}:{port}"

    path = parsed.path or "/"
    if _is_blocked_path(path):
        return None

    query_items = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.casefold().startswith("utm_")
        and key.casefold() not in TRACKING_QUERY_PARAMETERS
    ]
    return urlunsplit((scheme, netloc, path, urlencode(query_items, doseq=True), ""))


def collect_candidate_links(
    raw_links: RawLinks,
    *,
    current_url: str,
    start_url: str,
    allow_external: bool,
) -> list[CandidateLink]:
    """Normalize, classify, score, and deduplicate Crawl4AI link results."""
    candidates_by_url: dict[str, CandidateLink] = {}

    for raw_link in [*raw_links.get("internal", []), *raw_links.get("external", [])]:
        raw_href = raw_link.get("href")
        if not isinstance(raw_href, str):
            continue

        normalized = normalize_url(raw_href, base_url=current_url)
        if normalized is None or normalized == current_url:
            continue

        link_type = "internal" if _same_site(normalized, start_url) else "external"
        if link_type == "external" and not allow_external:
            continue

        text = _string_value(raw_link.get("text"))
        title = _string_value(raw_link.get("title"))
        candidate = CandidateLink(
            url=normalized,
            text=text,
            title=title,
            link_type=link_type,
            score=_link_score(normalized, text, title),
        )
        existing = candidates_by_url.get(normalized)
        if existing is None or candidate.score < existing.score:
            candidates_by_url[normalized] = candidate

    return sorted(
        candidates_by_url.values(),
        key=lambda candidate: (candidate.score, candidate.url),
    )


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


def create_initial_state(start_url: str) -> CrawlState:
    normalized = normalize_url(start_url, base_url=start_url)
    if normalized is None:
        raise ValueError(f"Start URL is not a valid HTTP(S) URL: {start_url}")

    return CrawlState(
        start_url=normalized,
        known_urls=[normalized],
        pending_urls=[PendingUrl(url=normalized, depth=0, priority=0, sequence=0)],
        next_sequence=1,
    )


def load_state(path: Path, *, expected_start_url: str) -> CrawlState:
    """Load a prior crawl and verify that it belongs to this start URL."""
    state = CrawlState.model_validate_json(path.read_text(encoding="utf-8"))
    normalized_start_url = normalize_url(
        expected_start_url, base_url=expected_start_url
    )
    if normalized_start_url is None:
        raise ValueError(f"Start URL is not a valid HTTP(S) URL: {expected_start_url}")
    if state.start_url != normalized_start_url:
        raise ValueError(
            f"State file is for {state.start_url}, not {normalized_start_url}"
        )
    return state


def save_state(state: CrawlState, path: Path, *, pending_heap: PendingHeap) -> None:
    """Atomically persist the crawl frontier and extracted page results."""
    state.pending_urls = [
        PendingUrl(url=url, depth=depth, priority=priority, sequence=sequence)
        for priority, sequence, depth, url in sorted(pending_heap)
    ]
    _write_json(path, state.model_dump_json(indent=2))


def save_report(report: ResearchReport, path: Path) -> None:
    """Atomically write the consolidated report."""
    _write_json(path, report.model_dump_json(indent=2))


def build_report(
    state: CrawlState,
    *,
    pending_heap: PendingHeap,
    max_pages: int,
) -> ResearchReport:
    """Create the final consolidated report from persisted crawl state."""
    remaining_urls = [url for _, _, _, url in sorted(pending_heap)]
    if remaining_urls and len(state.visited_urls) >= max_pages:
        stopped_reason = "max_pages_reached"
    elif remaining_urls:
        stopped_reason = "stopped_with_pending_urls"
    else:
        stopped_reason = "frontier_exhausted"

    return ResearchReport(
        start_url=state.start_url,
        crawl_complete=len(remaining_urls) == 0,
        stopped_reason=stopped_reason,
        visited_urls=state.visited_urls,
        remaining_urls=remaining_urls,
        failed_pages=state.failed_pages,
        analysis_stats=aggregate_analysis_stats(state.pages),
        useful_information=consolidate_information(state.pages),
        pages=state.pages,
    )


async def run_crawl(settings: CrawlSettings, state: CrawlState) -> ResearchReport:
    """Run the bounded crawl, saving state after every attempted page."""
    pending_heap = _pending_heap(state)
    if not pending_heap or len(state.visited_urls) >= settings.max_pages:
        return build_report(
            state,
            pending_heap=pending_heap,
            max_pages=settings.max_pages,
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
        async with (
            AsyncCodex() as codex,
            AsyncWebCrawler(config=browser_config) as crawler,
        ):
            await _process_frontier(
                state,
                pending_heap=pending_heap,
                settings=settings,
                crawler=crawler,
                codex=codex,
            )
    finally:
        try:
            await cloak_browser.close()
        finally:
            save_state(state, settings.state_path, pending_heap=pending_heap)

    return build_report(
        state,
        pending_heap=pending_heap,
        max_pages=settings.max_pages,
    )


async def _process_frontier(
    state: CrawlState,
    *,
    pending_heap: PendingHeap,
    settings: CrawlSettings,
    crawler: AsyncWebCrawler,
    codex: AsyncCodex,
) -> None:
    visited = set(state.visited_urls)
    known = set(state.known_urls)

    while pending_heap and len(state.visited_urls) < settings.max_pages:
        _, _, depth, url = heapq.heappop(pending_heap)
        if url in visited:
            continue

        LOGGER.info(
            "Crawling page %d/%d at depth %d: %s",
            len(state.visited_urls) + 1,
            settings.max_pages,
            depth,
            url,
        )
        state.visited_urls.append(url)
        visited.add(url)

        try:
            # Crawl4AI's decorated arun currently exposes both bound and unbound
            # variants to type checkers even though this is a bound call.
            result = await crawler.arun(  # ty: ignore[missing-argument]
                url=url,
                config=_crawler_run_config(settings),
            )
        except Exception as error:
            LOGGER.exception("Crawl request failed for %s", url)
            state.failed_pages.append(
                FailedPage(url=url, depth=depth, error=f"crawl: {error}")
            )
            save_state(state, settings.state_path, pending_heap=pending_heap)
            continue

        if not result.success:
            error_message = (
                result.error_message or "Crawl4AI returned an unsuccessful result"
            )
            LOGGER.warning("Crawl failed for %s: %s", url, error_message)
            state.failed_pages.append(
                FailedPage(url=url, depth=depth, error=f"crawl: {error_message}")
            )
            save_state(state, settings.state_path, pending_heap=pending_heap)
            continue

        final_url = normalize_url(result.url, base_url=url) or url
        if final_url not in known:
            state.known_urls.append(final_url)
            known.add(final_url)

        candidates = collect_candidate_links(
            _raw_links(result.links),
            current_url=final_url,
            start_url=state.start_url,
            allow_external=settings.allow_external,
        )
        prompt_candidates = _prompt_candidates(
            candidates,
            known_urls=known,
            limit=settings.max_prompt_links,
            include_links=depth < settings.max_depth,
        )
        markdown = truncate_markdown(
            _markdown_text(result.markdown),
            max_chars=settings.max_markdown_chars,
        )

        outcome = AnalysisOutcome(
            analysis=PageAnalysis(source_url=final_url),
            token_usage=None,
            error=None,
        )
        try:
            outcome = await _analyze_page(
                codex,
                url=final_url,
                markdown=markdown,
                candidate_links=prompt_candidates,
                visited_urls=state.visited_urls,
            )
        except Exception as error:
            LOGGER.exception("LLM analysis failed for %s", final_url)
            outcome = AnalysisOutcome(
                analysis=PageAnalysis(source_url=final_url),
                token_usage=None,
                error=str(error),
            )

        if outcome.error is not None:
            LOGGER.warning("Analysis failed for %s: %s", final_url, outcome.error)
            state.failed_pages.append(
                FailedPage(
                    url=final_url,
                    depth=depth,
                    error=f"analysis: {outcome.error}",
                )
            )

        analysis = outcome.analysis
        analysis.analysis_metadata = PageAnalysisMetadata(
            depth=depth,
            succeeded=outcome.error is None,
            error=outcome.error,
            token_usage=outcome.token_usage,
        )
        state.pages.append(analysis)
        if depth < settings.max_depth:
            _schedule_candidates(
                state,
                pending_heap=pending_heap,
                candidates=candidates,
                recommendations=analysis.next_links,
                current_depth=depth,
                known_urls=known,
            )

        save_state(state, settings.state_path, pending_heap=pending_heap)


async def _analyze_page(
    codex: AsyncCodex,
    *,
    url: str,
    markdown: str,
    candidate_links: list[CandidateLink],
    visited_urls: list[str],
) -> AnalysisOutcome:
    thread = await codex.thread_start(
        base_instructions=(
            "Analyze the supplied website data without using tools or external knowledge. "
            "Return only data matching the requested output schema."
        ),
        ephemeral=True,
        sandbox=Sandbox.read_only,
    )
    result = await thread.run(
        create_prompt(
            url,
            markdown=markdown,
            candidate_links=candidate_links,
            visited_urls=visited_urls,
        ),
        output_schema=page_analysis_output_schema(),
        sandbox=Sandbox.read_only,
    )
    token_usage = print_usage(result, page_url=url)
    if result.final_response is None:
        error_message = (
            result.error.message if result.error is not None else result.status
        )
        return AnalysisOutcome(
            analysis=PageAnalysis(source_url=url),
            token_usage=token_usage,
            error=f"Codex returned no final response: {error_message}",
        )

    try:
        analysis = PageAnalysis.model_validate_json(result.final_response)
    except ValidationError as error:
        return AnalysisOutcome(
            analysis=PageAnalysis(source_url=url),
            token_usage=token_usage,
            error=f"Codex returned invalid structured data: {error}",
        )

    set_source_url(analysis, url)
    _validate_recommendations(analysis, candidate_links)
    return AnalysisOutcome(
        analysis=analysis,
        token_usage=token_usage,
        error=None,
    )


def _validate_recommendations(
    analysis: PageAnalysis,
    candidates: list[CandidateLink],
) -> None:
    candidate_by_url = {candidate.url: candidate for candidate in candidates}
    validated: list[LinkRecommendation] = []
    seen_urls: set[str] = set()
    seen_priorities: set[int] = set()

    for recommendation in sorted(
        analysis.next_links,
        key=lambda item: item.priority,
    ):
        candidate = candidate_by_url.get(recommendation.url)
        if candidate is None:
            continue
        if (
            recommendation.url in seen_urls
            or recommendation.priority in seen_priorities
        ):
            continue

        recommendation.link_type = candidate.link_type
        validated.append(recommendation)
        seen_urls.add(recommendation.url)
        seen_priorities.add(recommendation.priority)

    analysis.next_links = validated


def _schedule_candidates(
    state: CrawlState,
    *,
    pending_heap: PendingHeap,
    candidates: list[CandidateLink],
    recommendations: list[LinkRecommendation],
    current_depth: int,
    known_urls: set[str],
) -> None:
    recommendation_priority = {
        recommendation.url: recommendation.priority
        for recommendation in recommendations
    }
    next_depth = current_depth + 1

    for candidate in candidates:
        if candidate.url in known_urls:
            continue
        if (
            candidate.link_type == "external"
            and candidate.url not in recommendation_priority
        ):
            continue

        llm_priority = recommendation_priority.get(candidate.url)
        link_priority = candidate.score if llm_priority is None else llm_priority - 1
        priority = next_depth * 25 + link_priority
        heapq.heappush(
            pending_heap,
            (priority, state.next_sequence, next_depth, candidate.url),
        )
        state.next_sequence += 1
        state.known_urls.append(candidate.url)
        known_urls.add(candidate.url)


def _prompt_candidates(
    candidates: list[CandidateLink],
    *,
    known_urls: set[str],
    limit: int,
    include_links: bool,
) -> list[CandidateLink]:
    if not include_links:
        return []
    return [candidate for candidate in candidates if candidate.url not in known_urls][
        :limit
    ]


def _pending_heap(state: CrawlState) -> PendingHeap:
    pending_heap = [
        (item.priority, item.sequence, item.depth, item.url)
        for item in state.pending_urls
    ]
    heapq.heapify(pending_heap)
    return pending_heap


def _crawler_run_config(settings: CrawlSettings) -> CrawlerRunConfig:
    return CrawlerRunConfig(
        check_robots_txt=settings.check_robots_txt,
        page_timeout=60_000,
        remove_overlay_elements=True,
        remove_consent_popups=True,
        scan_full_page=True,
        max_scroll_steps=10,
        verbose=False,
    )


def _raw_links(value: object) -> RawLinks:
    if not isinstance(value, dict):
        return {}

    result: RawLinks = {}
    for link_type in ("internal", "external"):
        raw_items = value.get(link_type)
        if not isinstance(raw_items, list):
            continue
        result[link_type] = [item for item in raw_items if isinstance(item, dict)]
    return result


def _markdown_text(value: object) -> str:
    if isinstance(value, str):
        return value
    raw_markdown = getattr(value, "raw_markdown", None)
    return raw_markdown if isinstance(raw_markdown, str) else ""


def _same_site(first_url: str, second_url: str) -> bool:
    first_hostname = urlsplit(first_url).hostname
    second_hostname = urlsplit(second_url).hostname
    if first_hostname is None or second_hostname is None:
        return False
    return _without_www(first_hostname) == _without_www(second_hostname)


def _without_www(hostname: str) -> str:
    return hostname.casefold().removeprefix("www.")


def _is_blocked_path(path: str) -> bool:
    lowered = path.casefold()
    if lowered.endswith(BLOCKED_FILE_SUFFIXES):
        return True
    segments = {segment for segment in lowered.split("/") if segment}
    return bool(segments & BLOCKED_PATH_SEGMENTS)


def _link_score(url: str, text: str, title: str) -> int:
    searchable = f"{url} {text} {title}".casefold()
    for score, keywords in LINK_KEYWORDS:
        if any(keyword in searchable for keyword in keywords):
            return score
    return 100


def _string_value(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _write_json(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.tmp")
    temporary_path.write_text(content, encoding="utf-8")
    temporary_path.replace(path)
