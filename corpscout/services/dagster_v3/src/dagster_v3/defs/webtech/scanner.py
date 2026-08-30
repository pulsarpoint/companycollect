import asyncio
import json
import shutil
import tempfile
import time
from collections.abc import Callable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import batched
from pathlib import Path
from threading import Lock, Thread
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from cloakbrowser import launch_persistent_context_async
from playwright.async_api import (
    BrowserContext,
    Error as PlaywrightError,
    Page,
    TimeoutError as PlaywrightTimeoutError,
)
from pydantic import ValidationError

from dagster_v3.defs.webtech.models import (
    ExtensionReport,
    WebtechCandidate,
    WebtechDomainResult,
)

PAGE_TOKEN_ATTRIBUTE = "data-webtech-page-token"
MAX_CALLBACK_BODY_BYTES = 1_000_000


@dataclass(frozen=True, slots=True)
class WebtechScannerSettings:
    """Explicit runtime limits for isolated browser-context batches."""

    headless: bool
    page_worker_count: int
    navigation_timeout_seconds: float
    report_timeout_seconds: float


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


type WebtechProgressCallback = Callable[[WebtechDomainResult], None]


async def scan_webtech_candidates(
    candidates: Sequence[WebtechCandidate],
    *,
    settings: WebtechScannerSettings,
    progress_callback: WebtechProgressCallback,
) -> tuple[WebtechDomainResult, ...]:
    """Scan each worker-sized batch in a fresh browser context and profile."""
    selected = tuple(candidates)
    if not selected:
        return ()
    _validate_settings(settings)

    loop = asyncio.get_running_loop()
    router = ExtensionReportRouter(loop)
    route_token = uuid4().hex
    with _callback_server(router, route_token=route_token) as callback_url:
        with _runtime_extension_copy(callback_url=callback_url) as extension_dir:
            results: list[WebtechDomainResult] = []
            for batch in batched(selected, settings.page_worker_count):
                results.extend(
                    await _scan_context_batch(
                        batch,
                        extension_dir=extension_dir,
                        router=router,
                        settings=settings,
                        progress_callback=progress_callback,
                    )
                )
            return tuple(results)


async def _scan_context_batch(
    candidates: tuple[WebtechCandidate, ...],
    *,
    extension_dir: Path,
    router: ExtensionReportRouter,
    settings: WebtechScannerSettings,
    progress_callback: WebtechProgressCallback,
) -> tuple[WebtechDomainResult, ...]:
    with tempfile.TemporaryDirectory(prefix="webtech-profile-") as profile_dir:
        context = await launch_persistent_context_async(
            profile_dir,
            headless=settings.headless,
            extension_paths=[str(extension_dir)],
        )
        try:
            for page in tuple(context.pages):
                await page.close()
            return await _scan_batch_pages(
                context,
                candidates,
                router=router,
                settings=settings,
                progress_callback=progress_callback,
            )
        finally:
            await context.close()


async def _scan_batch_pages(
    context: BrowserContext,
    candidates: tuple[WebtechCandidate, ...],
    *,
    router: ExtensionReportRouter,
    settings: WebtechScannerSettings,
    progress_callback: WebtechProgressCallback,
) -> tuple[WebtechDomainResult, ...]:
    async def scan_one(candidate: WebtechCandidate) -> WebtechDomainResult:
        page = await context.new_page()
        try:
            result, page = await _scan_candidate(
                context,
                page,
                candidate,
                router=router,
                settings=settings,
            )
            progress_callback(result)
            return result
        finally:
            if not page.is_closed():
                await page.close()

    return tuple(await asyncio.gather(*(scan_one(candidate) for candidate in candidates)))


async def _scan_candidate(
    context: BrowserContext,
    page: Page,
    candidate: WebtechCandidate,
    *,
    router: ExtensionReportRouter,
    settings: WebtechScannerSettings,
) -> tuple[WebtechDomainResult, Page]:
    started_at = datetime.now(UTC)
    started_monotonic = time.monotonic()
    https_url = f"https://{candidate.root_domain}"
    requested_url = https_url
    http_fallback_used = False

    try:
        try:
            await _navigate_page(
                page,
                https_url,
                timeout_seconds=settings.navigation_timeout_seconds,
            )
        except (PlaywrightError, PlaywrightTimeoutError) as https_error:
            requested_url = f"http://{candidate.root_domain}"
            http_fallback_used = True
            page = await _replace_page(context, page)
            try:
                await _navigate_page(
                    page,
                    requested_url,
                    timeout_seconds=settings.navigation_timeout_seconds,
                )
            except (PlaywrightError, PlaywrightTimeoutError) as http_error:
                return (
                    WebtechDomainResult.failure(
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
                    ),
                    page,
                )

        page_token = await _read_page_token(
            page,
            timeout_seconds=settings.report_timeout_seconds,
        )
        future = router.register(page_token)
        try:
            report = await asyncio.wait_for(
                future,
                timeout=settings.report_timeout_seconds,
            )
        except TimeoutError:
            router.discard(page_token, future)
            return (
                WebtechDomainResult.failure(
                    candidate=candidate,
                    outcome="report_timeout",
                    requested_url=requested_url,
                    final_url=page.url if _is_web_url(page.url) else "",
                    scanned_at=started_at,
                    duration_ms=_elapsed_ms(started_monotonic),
                    http_fallback_used=http_fallback_used,
                    error_message=(
                        "The extension did not emit a final schema-v2 report in time"
                    ),
                ),
                page,
            )

        return (
            WebtechDomainResult.success(
                candidate=candidate,
                requested_url=requested_url,
                final_url=report.url,
                report=report,
                scanned_at=started_at,
                duration_ms=_elapsed_ms(started_monotonic),
                http_fallback_used=http_fallback_used,
            ),
            page,
        )
    except (PlaywrightError, PlaywrightTimeoutError, ValueError) as error:
        return (
            WebtechDomainResult.failure(
                candidate=candidate,
                outcome="browser_error",
                requested_url=requested_url,
                final_url=page.url if _is_web_url(page.url) else "",
                scanned_at=started_at,
                duration_ms=_elapsed_ms(started_monotonic),
                http_fallback_used=http_fallback_used,
                error_message=str(error),
            ),
            page,
        )


async def _navigate_page(
    page: Page,
    url: str,
    *,
    timeout_seconds: float,
) -> None:
    """Navigate, accepting a server redirect that settles after Playwright interrupts."""
    try:
        await page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=timeout_seconds * 1_000,
        )
    except (PlaywrightError, PlaywrightTimeoutError) as navigation_error:
        if "is interrupted by another navigation to" not in str(navigation_error):
            raise
        try:
            await page.wait_for_load_state(
                state="domcontentloaded",
                timeout=min(5_000, timeout_seconds * 1_000),
            )
        except (PlaywrightError, PlaywrightTimeoutError) as settle_error:
            raise navigation_error from settle_error
        if not _is_web_url(page.url):
            raise navigation_error


async def _replace_page(context: BrowserContext, page: Page) -> Page:
    if not page.is_closed():
        await page.close()
    return await context.new_page()


async def _read_page_token(page: Page, *, timeout_seconds: float) -> UUID:
    expression = (
        "() => Boolean(document.documentElement?.getAttribute("
        f"'{PAGE_TOKEN_ATTRIBUTE}'))"
    )
    await page.wait_for_function(expression, timeout=timeout_seconds * 1_000)
    value = await page.evaluate(
        "document.documentElement.getAttribute("
        f"'{PAGE_TOKEN_ATTRIBUTE}')"
    )
    await page.evaluate(
        "document.documentElement.removeAttribute("
        f"'{PAGE_TOKEN_ATTRIBUTE}')"
    )
    if not isinstance(value, str):
        raise ValueError("Extension page token was not a string")
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
                report = ExtensionReport.model_validate_json(
                    self.rfile.read(body_size)
                )
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
    if not 1 <= settings.page_worker_count <= 20:
        raise ValueError("page_worker_count must be between 1 and 20")
    if settings.navigation_timeout_seconds <= 0:
        raise ValueError("navigation_timeout_seconds must be positive")
    if settings.report_timeout_seconds <= 0:
        raise ValueError("report_timeout_seconds must be positive")


def _elapsed_ms(started_monotonic: float) -> int:
    return max(0, round((time.monotonic() - started_monotonic) * 1_000))


def _is_web_url(url: str) -> bool:
    parsed = urlsplit(url)
    return parsed.scheme in {"http", "https"} and parsed.hostname is not None
