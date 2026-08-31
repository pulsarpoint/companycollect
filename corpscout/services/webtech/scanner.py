import asyncio
import inspect
import json
import logging
import shutil
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from cloakbrowser import launch_persistent_context_async
from playwright.async_api import (
    BrowserContext,
    Page,
)
from playwright.async_api import (
    Error as PlaywrightError,
)
from playwright.async_api import (
    TimeoutError as PlaywrightTimeoutError,
)
from pydantic import ValidationError

from models import (
    ExtensionReport,
    WebtechCandidate,
    WebtechDomainResult,
    WebtechTimeoutStage,
)

PAGE_TOKEN_ATTRIBUTE = "data-webtech-page-token"
MAX_CALLBACK_BODY_BYTES = 1_000_000
DEFAULT_DOMAIN_TIMEOUT_SECONDS = 20.0
PAGE_CLOSE_TIMEOUT_SECONDS = 2.0
CONTEXT_CLOSE_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class WebtechScannerSettings:
    """Runtime settings for independent browsers and their page workers."""

    headless: bool
    browser_count: int
    pages_per_browser: int
    domain_timeout_seconds: float
    domains_per_context: int | None
    context_launch_interval_seconds: float


@dataclass(slots=True)
class _ScanProgress:
    """Identify the operation occupying a domain's shared hard deadline."""

    requested_url: str
    timeout_stage: WebtechTimeoutStage = "page_creation"
    http_fallback_used: bool = False


class ExtensionReportRouter:
    """Route callback reports to pages by UUID, independent of redirect URL."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._waiters: dict[UUID, asyncio.Future[ExtensionReport]] = {}
        self._early_reports: dict[UUID, ExtensionReport] = {}
        self._registered_tokens: set[UUID] = set()
        self._lock = Lock()

    def register(self, page_token: UUID) -> asyncio.Future[ExtensionReport]:
        future = self._loop.create_future()
        with self._lock:
            if page_token in self._registered_tokens:
                raise ValueError(f"Duplicate page token: {page_token}")
            self._registered_tokens.add(page_token)
            early_report = self._early_reports.pop(page_token, None)
            if early_report is None:
                self._waiters[page_token] = future
        if early_report is not None:
            _resolve_waiter(future, early_report)
        return future

    def accept(self, report: ExtensionReport) -> bool:
        with self._lock:
            future = self._waiters.pop(report.page_token, None)
            if future is None:
                if (
                    report.page_token in self._registered_tokens
                    or report.page_token in self._early_reports
                ):
                    return False
                self._early_reports[report.page_token] = report
                return True
        if future.done():
            return False
        self._loop.call_soon_threadsafe(_resolve_waiter, future, report)
        return True

    def discard(
        self,
        page_token: UUID,
        future: asyncio.Future[ExtensionReport],
    ) -> None:
        with self._lock:
            if self._waiters.get(page_token) is future:
                del self._waiters[page_token]
        if not future.done():
            future.cancel()


type WebtechProgressCallback = Callable[
    [WebtechDomainResult], Awaitable[None] | None
]


async def scan_webtech_candidates(
    candidates: Sequence[WebtechCandidate],
    *,
    settings: WebtechScannerSettings,
    progress_callback: WebtechProgressCallback,
) -> tuple[WebtechDomainResult, ...]:
    """Keep each browser's page workers busy until every candidate completes."""
    selected = tuple(candidates)
    if not selected:
        return ()
    _validate_settings(settings)

    loop = asyncio.get_running_loop()
    router = ExtensionReportRouter(loop)
    route_token = uuid4().hex
    with (
        _callback_server(router, route_token=route_token) as callback_url,
        _runtime_extension_copy(callback_url=callback_url) as extension_dir,
    ):
        if settings.domains_per_context is not None:
            return await _scan_in_context_batches(
                selected,
                router=router,
                extension_dir=extension_dir,
                settings=settings,
                progress_callback=progress_callback,
            )

        contexts: list[BrowserContext] = []
        profile_directories: list[tempfile.TemporaryDirectory[str]] = []
        try:
            for browser_number in range(min(settings.browser_count, len(selected))):
                profile_directory = tempfile.TemporaryDirectory(
                    prefix=f"webtech-profile-{browser_number + 1}-"
                )
                profile_directories.append(profile_directory)
                context = await _launch_browser_context(
                    profile_directory.name,
                    extension_dir=extension_dir,
                    headless=settings.headless,
                )
                contexts.append(context)

            return await _scan_pages(
                tuple(contexts),
                selected,
                router=router,
                settings=settings,
                progress_callback=progress_callback,
            )
        finally:
            await asyncio.gather(*(_close_context(context) for context in contexts))
            for profile_directory in reversed(profile_directories):
                profile_directory.cleanup()


async def _scan_in_context_batches(
    candidates: tuple[WebtechCandidate, ...],
    *,
    router: ExtensionReportRouter,
    extension_dir: Path,
    settings: WebtechScannerSettings,
    progress_callback: WebtechProgressCallback,
) -> tuple[WebtechDomainResult, ...]:
    """Scan batches across fresh contexts, with bounded context concurrency."""
    if settings.domains_per_context is None:
        raise ValueError("domains_per_context is required for context batching")

    batches = tuple(
        candidates[start : start + settings.domains_per_context]
        for start in range(0, len(candidates), settings.domains_per_context)
    )
    batch_iterator = iter(batches)
    results: dict[WebtechCandidate, WebtechDomainResult] = {}
    context_launch_lock = asyncio.Lock()
    next_context_launch_at = time.monotonic()

    async def wait_for_context_launch_slot() -> None:
        nonlocal next_context_launch_at
        if settings.context_launch_interval_seconds == 0:
            return
        async with context_launch_lock:
            wait_seconds = next_context_launch_at - time.monotonic()
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            next_context_launch_at = (
                time.monotonic() + settings.context_launch_interval_seconds
            )

    async def context_worker(worker_number: int) -> None:
        for batch in batch_iterator:
            await wait_for_context_launch_slot()
            with tempfile.TemporaryDirectory(
                prefix=f"webtech-profile-{worker_number}-"
            ) as profile_directory:
                context = await _launch_browser_context(
                    profile_directory,
                    extension_dir=extension_dir,
                    headless=settings.headless,
                )
                try:
                    batch_results = await _scan_pages(
                        (context,),
                        batch,
                        router=router,
                        settings=settings,
                        progress_callback=progress_callback,
                    )
                finally:
                    await _close_context(context)
            results.update(zip(batch, batch_results, strict=True))

    await asyncio.gather(
        *(
            context_worker(worker_number)
            for worker_number in range(1, min(settings.browser_count, len(batches)) + 1)
        )
    )
    return tuple(results[candidate] for candidate in candidates)


async def _launch_browser_context(
    profile_directory: str,
    *,
    extension_dir: Path,
    headless: bool,
) -> BrowserContext:
    return await launch_persistent_context_async(
        profile_directory,
        headless=headless,
        args=(
            ["--ozone-platform=x11"]
            if not headless and sys.platform == "linux"
            else None
        ),
        extension_paths=[str(extension_dir)],
    )


async def _scan_pages(
    contexts: tuple[BrowserContext, ...],
    candidates: tuple[WebtechCandidate, ...],
    *,
    router: ExtensionReportRouter,
    settings: WebtechScannerSettings,
    progress_callback: WebtechProgressCallback,
) -> tuple[WebtechDomainResult, ...]:
    candidate_iterator = iter(candidates)
    results: dict[WebtechCandidate, WebtechDomainResult] = {}

    async def page_worker(context: BrowserContext) -> None:
        for candidate in candidate_iterator:
            result = await _scan_candidate_with_hard_timeout(
                context,
                candidate,
                router=router,
                timeout_seconds=settings.domain_timeout_seconds,
            )
            progress = progress_callback(result)
            if inspect.isawaitable(progress):
                await progress
            results[candidate] = result

    await asyncio.gather(
        *(
            page_worker(context)
            for context in contexts
            for _ in range(settings.pages_per_browser)
        )
    )
    return tuple(results[candidate] for candidate in candidates)


async def _scan_candidate_with_hard_timeout(
    context: BrowserContext,
    candidate: WebtechCandidate,
    *,
    router: ExtensionReportRouter,
    timeout_seconds: float,
) -> WebtechDomainResult:
    started_at = datetime.now(UTC)
    started_monotonic = time.monotonic()
    page: Page | None = None
    progress = _ScanProgress(requested_url=f"https://{candidate.root_domain}")

    try:
        async with asyncio.timeout(timeout_seconds):
            page = await context.new_page()
            return await _scan_candidate(
                page,
                candidate,
                router=router,
                started_at=started_at,
                started_monotonic=started_monotonic,
                progress=progress,
            )
    except TimeoutError:
        return WebtechDomainResult.failure(
            candidate=candidate,
            outcome="hard_timeout",
            requested_url=progress.requested_url,
            final_url=(page.url if page is not None and _is_web_url(page.url) else ""),
            scanned_at=started_at,
            duration_ms=_elapsed_ms(started_monotonic),
            http_fallback_used=progress.http_fallback_used,
            error_message=(
                f"Domain exceeded the hard {timeout_seconds:g}-second timeout"
            ),
            timeout_stage=progress.timeout_stage,
        )
    finally:
        if page is not None and not page.is_closed():
            await _close_page(page, candidate.root_domain)


async def _close_page(page: Page, root_domain: str) -> None:
    """Prevent a stalled Chromium renderer from consuming a page worker forever."""
    try:
        async with asyncio.timeout(PAGE_CLOSE_TIMEOUT_SECONDS):
            await page.close()
    except (TimeoutError, PlaywrightError) as error:
        logging.getLogger(__name__).warning(
            "Page cleanup failed domain=%s error_type=%s error=%r",
            root_domain,
            type(error).__name__,
            error,
        )


async def _close_context(context: BrowserContext) -> None:
    """Bound final browser cleanup so a completed benchmark can always exit."""
    try:
        async with asyncio.timeout(CONTEXT_CLOSE_TIMEOUT_SECONDS):
            await context.close()
    except (TimeoutError, PlaywrightError) as error:
        logging.getLogger(__name__).warning(
            "CloakBrowser context cleanup failed error_type=%s error=%r",
            type(error).__name__,
            error,
        )


async def _scan_candidate(
    page: Page,
    candidate: WebtechCandidate,
    *,
    router: ExtensionReportRouter,
    started_at: datetime,
    started_monotonic: float,
    progress: _ScanProgress,
) -> WebtechDomainResult:
    https_url = f"https://{candidate.root_domain}"
    requested_url = https_url
    http_fallback_used = False

    try:
        try:
            progress.timeout_stage = "https_navigation"
            await _navigate_page(
                page,
                https_url,
            )
        except (PlaywrightError, PlaywrightTimeoutError) as https_error:
            requested_url = f"http://{candidate.root_domain}"
            http_fallback_used = True
            progress.requested_url = requested_url
            progress.http_fallback_used = True
            progress.timeout_stage = "http_navigation"
            try:
                await _navigate_page(
                    page,
                    requested_url,
                )
            except (PlaywrightError, PlaywrightTimeoutError) as http_error:
                return WebtechDomainResult.failure(
                    candidate=candidate,
                    outcome="navigation_error",
                    requested_url=requested_url,
                    final_url=page.url if _is_web_url(page.url) else "",
                    scanned_at=started_at,
                    duration_ms=_elapsed_ms(started_monotonic),
                    http_fallback_used=True,
                    error_message=(
                        f"HTTPS navigation failed: {https_error}; "
                        f"HTTP fallback failed: {http_error}"
                    ),
                )

        progress.timeout_stage = "extension_token"
        page_token = await _read_page_token(page)
        future = router.register(page_token)
        progress.timeout_stage = "wappalyzer_report"
        try:
            report = await future
        except asyncio.CancelledError:
            router.discard(page_token, future)
            raise

        if not report.analysis_complete:
            return WebtechDomainResult.extension_error(
                candidate=candidate,
                requested_url=requested_url,
                final_url=report.url,
                report=report,
                scanned_at=started_at,
                duration_ms=_elapsed_ms(started_monotonic),
                http_fallback_used=http_fallback_used,
            )

        return WebtechDomainResult.success(
            candidate=candidate,
            requested_url=requested_url,
            final_url=report.url,
            report=report,
            scanned_at=started_at,
            duration_ms=_elapsed_ms(started_monotonic),
            http_fallback_used=http_fallback_used,
        )
    except (PlaywrightError, PlaywrightTimeoutError, TypeError, ValueError) as error:
        return WebtechDomainResult.failure(
            candidate=candidate,
            outcome="browser_error",
            requested_url=requested_url,
            final_url=page.url if _is_web_url(page.url) else "",
            scanned_at=started_at,
            duration_ms=_elapsed_ms(started_monotonic),
            http_fallback_used=http_fallback_used,
            error_message=str(error),
        )


async def _navigate_page(
    page: Page,
    url: str,
) -> None:
    """Navigate, accepting a server redirect that settles after Playwright interrupts."""
    try:
        await page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=0,
        )
    except (PlaywrightError, PlaywrightTimeoutError) as navigation_error:
        if "is interrupted by another navigation to" not in str(navigation_error):
            raise
        try:
            await page.wait_for_load_state(
                state="domcontentloaded",
                timeout=5_000,
            )
        except (PlaywrightError, PlaywrightTimeoutError) as settle_error:
            raise navigation_error from settle_error
        if not _is_web_url(page.url):
            raise


async def _read_page_token(page: Page) -> UUID:
    expression = (
        "() => Boolean(document.documentElement?.getAttribute("
        f"'{PAGE_TOKEN_ATTRIBUTE}'))"
    )
    await page.wait_for_function(expression, timeout=0)
    value = await page.evaluate(
        f"document.documentElement.getAttribute('{PAGE_TOKEN_ATTRIBUTE}')"
    )
    await page.evaluate(
        f"document.documentElement.removeAttribute('{PAGE_TOKEN_ATTRIBUTE}')"
    )
    if not isinstance(value, str):
        raise TypeError("Extension page token was not a string")
    return UUID(value)


@contextmanager
def _callback_server(
    router: ExtensionReportRouter,
    *,
    route_token: str,
):
    callback_path = f"/technologies/{route_token}"
    handler = _callback_handler(router, callback_path=callback_path)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = Thread(
        target=server.serve_forever,
        name="webtech-extension-callback",
        daemon=True,
    )
    thread.start()
    port = int(server.server_address[1])
    try:
        yield f"http://127.0.0.1:{port}{callback_path}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _callback_handler(
    router: ExtensionReportRouter,
    *,
    callback_path: str,
) -> type[BaseHTTPRequestHandler]:
    class TechnologyReportHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            if self.path != callback_path:
                self._respond(HTTPStatus.NOT_FOUND)
                return

            content_length = self.headers.get("Content-Length", "")
            if not content_length.isdigit():
                self._respond(HTTPStatus.LENGTH_REQUIRED)
                return
            body_size = int(content_length)
            if body_size <= 0 or body_size > MAX_CALLBACK_BODY_BYTES:
                self._respond(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
                return

            try:
                report = ExtensionReport.model_validate_json(self.rfile.read(body_size))
            except ValidationError:
                self._respond(HTTPStatus.BAD_REQUEST)
                return

            status = HTTPStatus.ACCEPTED if router.accept(report) else HTTPStatus.GONE
            self._respond(status)

        def _respond(self, status: HTTPStatus) -> None:
            body = json.dumps({"status": status.phrase}).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return TechnologyReportHandler


@contextmanager
def _runtime_extension_copy(*, callback_url: str):
    source_dir = Path(__file__).resolve().with_name("extension")
    if not (source_dir / "manifest.json").is_file():
        raise FileNotFoundError(f"Webtech extension is incomplete: {source_dir}")

    with tempfile.TemporaryDirectory(prefix="webtech-extension-") as temp_dir:
        extension_dir = Path(temp_dir) / "extension"
        shutil.copytree(source_dir, extension_dir)
        (extension_dir / "runtime-config.json").write_text(
            json.dumps({"callback_url": callback_url}),
            encoding="utf-8",
        )
        yield extension_dir


def _resolve_waiter(
    future: asyncio.Future[ExtensionReport],
    report: ExtensionReport,
) -> None:
    if not future.done():
        future.set_result(report)


def _validate_settings(settings: WebtechScannerSettings) -> None:
    if not 1 <= settings.browser_count <= 20:
        raise ValueError("browser_count must be between 1 and 20")
    if not 1 <= settings.pages_per_browser <= 30:
        raise ValueError("pages_per_browser must be between 1 and 30")
    if settings.browser_count * settings.pages_per_browser > 30:
        raise ValueError("browser_count * pages_per_browser must not exceed 30")
    if settings.domain_timeout_seconds <= 0:
        raise ValueError("domain_timeout_seconds must be greater than zero")
    if settings.context_launch_interval_seconds < 0:
        raise ValueError("context_launch_interval_seconds must not be negative")
    if settings.domains_per_context is not None:
        if settings.domains_per_context <= 0:
            raise ValueError("domains_per_context must be greater than zero")
        if settings.pages_per_browser > settings.domains_per_context:
            raise ValueError(
                "pages_per_browser must not exceed domains_per_context when batching"
            )
    elif settings.context_launch_interval_seconds > 0:
        raise ValueError("context_launch_interval_seconds requires domains_per_context")


def _elapsed_ms(started_monotonic: float) -> int:
    return max(0, round((time.monotonic() - started_monotonic) * 1_000))


def _is_web_url(url: str) -> bool:
    parsed = urlsplit(url)
    return parsed.scheme in {"http", "https"} and parsed.hostname is not None
