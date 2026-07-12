"""Determine exact Common Crawl WARC object sizes without downloading objects."""

import random
import re
import time
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from queue import Queue
from threading import TIMEOUT_MAX, Condition
from typing import TypeAlias

import httpx

from .manifests import COMMON_CRAWL_DATA_URL, WarcObject


_TRANSIENT_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
_THROTTLE_STATUS_CODES = frozenset({429, 503})
_CONTENT_RANGE = re.compile(r"bytes 0-0/([0-9]+)")
_DECIMAL_INTEGER = re.compile(r"[0-9]+")
_MAX_OBJECT_BYTES = 2**64 - 1
_MAX_BACKOFF_SECONDS = 30.0


@dataclass(frozen=True, slots=True)
class ProbeMetrics:
    head_requests: int = 0
    range_requests: int = 0
    http_429: int = 0
    http_503: int = 0

    @property
    def http_requests(self) -> int:
        return self.head_requests + self.range_requests


@dataclass(frozen=True, slots=True)
class WarcSizeResult:
    warc_index: int
    warc_filename: str
    object_bytes: int
    used_range_fallback: bool
    metrics: ProbeMetrics


class WarcSizeProbeError(RuntimeError):
    """Base error carrying the exact context and metrics for one probe attempt."""

    def __init__(
        self,
        message: str,
        *,
        warc: WarcObject,
        method: str,
        status_code: int | None,
        metrics: ProbeMetrics,
    ) -> None:
        super().__init__(message)
        self.warc_index = warc.warc_index
        self.warc_filename = warc.warc_filename
        self.method = method
        self.status_code = status_code
        self.metrics = metrics


class PermanentWarcSizeError(WarcSizeProbeError):
    """A probe failure that another attempt cannot repair."""


class TransientWarcSizeError(WarcSizeProbeError):
    """A probe failure that the concurrent coordinator may retry."""

    def __init__(
        self,
        message: str,
        *,
        warc: WarcObject,
        method: str,
        status_code: int | None,
        metrics: ProbeMetrics,
        retry_after_seconds: float | None,
    ) -> None:
        super().__init__(
            message,
            warc=warc,
            method=method,
            status_code=status_code,
            metrics=metrics,
        )
        self.retry_after_seconds = retry_after_seconds
        self.throttled = status_code in _THROTTLE_STATUS_CODES


class WarcSizePoolError(RuntimeError):
    """An unexpected worker failure outside the classified probe contract."""


class _CooldownCancelled(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class WarcSizeSuccess:
    warc: WarcObject
    object_bytes: int
    attempts: int
    retries: int
    metrics: ProbeMetrics


@dataclass(frozen=True, slots=True)
class WarcSizeFailure:
    warc: WarcObject
    attempts: int
    retries: int
    metrics: ProbeMetrics
    error: WarcSizeProbeError
    permanent: bool


WarcSizeOutcome: TypeAlias = WarcSizeSuccess | WarcSizeFailure


@dataclass(slots=True)
class _ProbeCounters:
    head_requests: int = 0
    range_requests: int = 0
    http_429: int = 0
    http_503: int = 0

    def record_status(self, status_code: int) -> None:
        if status_code == 429:
            self.http_429 += 1
        elif status_code == 503:
            self.http_503 += 1

    def snapshot(self) -> ProbeMetrics:
        return ProbeMetrics(
            head_requests=self.head_requests,
            range_requests=self.range_requests,
            http_429=self.http_429,
            http_503=self.http_503,
        )


class SharedCooldown:
    """One monotonic throttle deadline shared by every size-probe worker."""

    def __init__(self, *, monotonic: Callable[[], float] = time.monotonic) -> None:
        self._monotonic = monotonic
        self._condition = Condition()
        self._deadline = 0.0
        self._cancelled = False

    def wait(self) -> None:
        with self._condition:
            while True:
                if self._cancelled:
                    raise _CooldownCancelled
                remaining = self._deadline - self._monotonic()
                if remaining <= 0:
                    return
                self._condition.wait(timeout=min(remaining, TIMEOUT_MAX))

    def wait_local(self, delay_seconds: float) -> None:
        if delay_seconds < 0:
            raise ValueError("local retry delay must not be negative")
        deadline = self._monotonic() + delay_seconds
        with self._condition:
            while True:
                if self._cancelled:
                    raise _CooldownCancelled
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    return
                self._condition.wait(timeout=min(remaining, TIMEOUT_MAX))

    def extend(self, delay_seconds: float) -> None:
        if delay_seconds < 0:
            raise ValueError("cooldown delay must not be negative")
        with self._condition:
            if self._cancelled:
                raise _CooldownCancelled
            self._deadline = max(
                self._deadline,
                self._monotonic() + delay_seconds,
            )
            self._condition.notify_all()

    def cancel(self) -> None:
        with self._condition:
            self._cancelled = True
            self._condition.notify_all()


def warc_object_url(warc_filename: str) -> str:
    if not warc_filename or warc_filename.startswith("/"):
        raise ValueError("WARC filename must be a nonempty relative object path")
    return f"{COMMON_CRAWL_DATA_URL}/{warc_filename}"


def parse_retry_after(value: str | None, *, now: datetime) -> float | None:
    """Parse either legal Retry-After representation into nonnegative seconds."""
    if value is None:
        return None
    if _DECIMAL_INTEGER.fullmatch(value) is not None:
        try:
            return float(int(value))
        except (ValueError, OverflowError):
            return None
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return max(0.0, (retry_at - now).total_seconds())


def retry_delay_seconds(
    failed_attempt: int,
    *,
    retry_after_seconds: float | None,
    random_fraction: Callable[[], float],
) -> float:
    """Calculate equal-jitter exponential backoff for a one-based failed attempt."""
    if failed_attempt <= 0:
        raise ValueError("failed_attempt must be positive")
    if retry_after_seconds is not None and retry_after_seconds < 0:
        raise ValueError("retry_after_seconds must not be negative")
    fraction = random_fraction()
    if not 0.0 <= fraction <= 1.0:
        raise ValueError("random_fraction must return a value between 0 and 1")
    exponent = min(failed_attempt - 1, 5)
    base = min(_MAX_BACKOFF_SECONDS, 2.0**exponent)
    jittered = base / 2.0 + fraction * base / 2.0
    return max(jittered, retry_after_seconds or 0.0)


def _strict_positive_size(value: str | None) -> int | None:
    if value is None or _DECIMAL_INTEGER.fullmatch(value) is None:
        return None
    try:
        size = int(value)
    except ValueError:
        return None
    if not 1 <= size <= _MAX_OBJECT_BYTES:
        return None
    return size


def _transport_error_is_transient(error: httpx.HTTPError) -> bool:
    return isinstance(
        error,
        (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError),
    )


def _http_error(
    error: httpx.HTTPError,
    *,
    warc: WarcObject,
    method: str,
    counters: _ProbeCounters,
) -> WarcSizeProbeError:
    error_type = (
        TransientWarcSizeError
        if _transport_error_is_transient(error)
        else PermanentWarcSizeError
    )
    arguments = {
        "warc": warc,
        "method": method,
        "status_code": None,
        "metrics": counters.snapshot(),
    }
    message = f"probe WARC size {warc.warc_filename}: {method} failed: {error}"
    if error_type is TransientWarcSizeError:
        return TransientWarcSizeError(
            message,
            **arguments,
            retry_after_seconds=None,
        )
    return PermanentWarcSizeError(message, **arguments)


def _status_error(
    response: httpx.Response,
    *,
    warc: WarcObject,
    method: str,
    counters: _ProbeCounters,
    now: Callable[[], datetime],
) -> WarcSizeProbeError:
    status_code = response.status_code
    message = f"probe WARC size {warc.warc_filename}: {method} returned HTTP {status_code}"
    arguments = {
        "warc": warc,
        "method": method,
        "status_code": status_code,
        "metrics": counters.snapshot(),
    }
    if status_code in _TRANSIENT_STATUS_CODES:
        return TransientWarcSizeError(
            message,
            **arguments,
            retry_after_seconds=parse_retry_after(
                response.headers.get("Retry-After"),
                now=now(),
            ),
        )
    return PermanentWarcSizeError(message, **arguments)


def _metadata_error(
    message: str,
    *,
    warc: WarcObject,
    counters: _ProbeCounters,
) -> PermanentWarcSizeError:
    return PermanentWarcSizeError(
        f"probe WARC size {warc.warc_filename}: {message}",
        warc=warc,
        method="GET",
        status_code=206,
        metrics=counters.snapshot(),
    )


def _range_size(
    response: httpx.Response,
    *,
    warc: WarcObject,
    counters: _ProbeCounters,
) -> int:
    content_range = response.headers.get("Content-Range")
    match = _CONTENT_RANGE.fullmatch(content_range or "")
    object_bytes = _strict_positive_size(match.group(1)) if match is not None else None
    if object_bytes is None:
        raise _metadata_error(
            f"invalid Content-Range {content_range!r}",
            warc=warc,
            counters=counters,
        )

    if response.headers.get("Content-Length") != "1":
        raise _metadata_error(
            f"one-byte range response has invalid Content-Length {response.headers.get('Content-Length')!r}",
            warc=warc,
            counters=counters,
        )
    content_encoding = response.headers.get("Content-Encoding")
    if content_encoding is not None and content_encoding.lower() != "identity":
        raise _metadata_error(
            f"one-byte range response has invalid Content-Encoding {content_encoding!r}",
            warc=warc,
            counters=counters,
        )

    body = bytearray()
    for chunk in response.iter_raw(chunk_size=2):
        body.extend(chunk)
        if len(body) > 1:
            break
    if len(body) != 1:
        raise _metadata_error(
            f"one-byte range response returned {len(body)} body bytes",
            warc=warc,
            counters=counters,
        )
    return object_bytes


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def probe_warc_size_once(
    client: httpx.Client,
    warc: WarcObject,
    *,
    timeout: httpx.Timeout | float = 30.0,
    now: Callable[[], datetime] = _utc_now,
) -> WarcSizeResult:
    """Perform one logical size probe, issuing HEAD and at most one Range GET."""
    url = warc_object_url(warc.warc_filename)
    counters = _ProbeCounters(head_requests=1)
    try:
        with client.stream(
            "HEAD",
            url,
            headers={"Accept-Encoding": "identity"},
            timeout=timeout,
            follow_redirects=True,
        ) as response:
            counters.record_status(response.status_code)
            if 200 <= response.status_code < 300:
                content_encoding = response.headers.get("Content-Encoding")
                identity_encoded = (
                    content_encoding is None or content_encoding.lower() == "identity"
                )
                object_bytes = _strict_positive_size(
                    response.headers.get("Content-Length")
                )
                if identity_encoded and object_bytes is not None:
                    return WarcSizeResult(
                        warc_index=warc.warc_index,
                        warc_filename=warc.warc_filename,
                        object_bytes=object_bytes,
                        used_range_fallback=False,
                        metrics=counters.snapshot(),
                    )
            elif response.status_code not in {405, 501}:
                raise _status_error(
                    response,
                    warc=warc,
                    method="HEAD",
                    counters=counters,
                    now=now,
                )
    except WarcSizeProbeError:
        raise
    except httpx.HTTPError as error:
        raise _http_error(
            error,
            warc=warc,
            method="HEAD",
            counters=counters,
        ) from error

    counters.range_requests += 1
    try:
        with client.stream(
            "GET",
            url,
            headers={"Range": "bytes=0-0", "Accept-Encoding": "identity"},
            timeout=timeout,
            follow_redirects=True,
        ) as response:
            counters.record_status(response.status_code)
            if response.status_code != 206:
                raise _status_error(
                    response,
                    warc=warc,
                    method="GET",
                    counters=counters,
                    now=now,
                )
            object_bytes = _range_size(response, warc=warc, counters=counters)
    except WarcSizeProbeError:
        raise
    except httpx.HTTPError as error:
        raise _http_error(
            error,
            warc=warc,
            method="GET",
            counters=counters,
        ) from error

    return WarcSizeResult(
        warc_index=warc.warc_index,
        warc_filename=warc.warc_filename,
        object_bytes=object_bytes,
        used_range_fallback=True,
        metrics=counters.snapshot(),
    )


def _add_probe_metrics(first: ProbeMetrics, second: ProbeMetrics) -> ProbeMetrics:
    return ProbeMetrics(
        head_requests=first.head_requests + second.head_requests,
        range_requests=first.range_requests + second.range_requests,
        http_429=first.http_429 + second.http_429,
        http_503=first.http_503 + second.http_503,
    )


def probe_warc_size_with_retries(
    client: httpx.Client,
    warc: WarcObject,
    *,
    max_attempts: int,
    cooldown: SharedCooldown,
    timeout: httpx.Timeout | float = 30.0,
    random_fraction: Callable[[], float] | None = None,
) -> WarcSizeOutcome:
    """Probe one WARC to a terminal outcome while honoring shared throttling."""
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")
    choose_fraction = random.random if random_fraction is None else random_fraction
    metrics = ProbeMetrics()

    for attempt in range(1, max_attempts + 1):
        cooldown.wait()
        try:
            result = probe_warc_size_once(
                client,
                warc,
                timeout=timeout,
            )
        except PermanentWarcSizeError as error:
            metrics = _add_probe_metrics(metrics, error.metrics)
            return WarcSizeFailure(
                warc=warc,
                attempts=attempt,
                retries=attempt - 1,
                metrics=metrics,
                error=error,
                permanent=True,
            )
        except TransientWarcSizeError as error:
            metrics = _add_probe_metrics(metrics, error.metrics)
            delay: float | None = None
            if error.throttled or attempt < max_attempts:
                delay = retry_delay_seconds(
                    attempt,
                    retry_after_seconds=error.retry_after_seconds,
                    random_fraction=choose_fraction,
                )
            if error.throttled:
                assert delay is not None
                cooldown.extend(delay)
            if attempt == max_attempts:
                return WarcSizeFailure(
                    warc=warc,
                    attempts=attempt,
                    retries=attempt - 1,
                    metrics=metrics,
                    error=error,
                    permanent=False,
                )
            if not error.throttled:
                assert delay is not None
                cooldown.wait_local(delay)
            continue

        if (
            result.warc_index != warc.warc_index
            or result.warc_filename != warc.warc_filename
        ):
            raise WarcSizePoolError(
                f"size probe returned a different WARC for index {warc.warc_index}"
            )
        metrics = _add_probe_metrics(metrics, result.metrics)
        return WarcSizeSuccess(
            warc=warc,
            object_bytes=result.object_bytes,
            attempts=attempt,
            retries=attempt - 1,
            metrics=metrics,
        )

    raise AssertionError("WARC size attempt loop ended without a terminal outcome")


def iter_warc_size_outcomes(
    client: httpx.Client,
    warcs: Sequence[WarcObject],
    *,
    concurrency: int,
    max_attempts: int,
    timeout: httpx.Timeout | float = 30.0,
    random_fraction: Callable[[], float] | None = None,
) -> Iterator[WarcSizeOutcome]:
    """Yield completion-ordered outcomes with at most ``concurrency`` active jobs."""
    if concurrency <= 0:
        raise ValueError("concurrency must be positive")
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")
    if not warcs:
        return

    cooldown = SharedCooldown()
    completed: Queue[Future[WarcSizeOutcome]] = Queue()
    remaining_warcs = iter(warcs)
    active = 0
    stop_submitting = False
    executor = ThreadPoolExecutor(
        max_workers=min(concurrency, len(warcs)),
        thread_name_prefix="warc-size",
    )

    def submit(warc: WarcObject) -> None:
        nonlocal active
        future = executor.submit(
            probe_warc_size_with_retries,
            client,
            warc,
            max_attempts=max_attempts,
            cooldown=cooldown,
            timeout=timeout,
            random_fraction=random_fraction,
        )
        future.add_done_callback(completed.put)
        active += 1

    try:
        for _ in range(min(concurrency, len(warcs))):
            submit(next(remaining_warcs))

        while active:
            future = completed.get()
            active -= 1
            try:
                outcome = future.result()
            except BaseException as error:
                stop_submitting = True
                raise WarcSizePoolError("unexpected WARC size worker failure") from error

            if isinstance(outcome, WarcSizeFailure):
                stop_submitting = True
            if not stop_submitting:
                try:
                    submit(next(remaining_warcs))
                except StopIteration:
                    stop_submitting = True
            yield outcome
    finally:
        cooldown.cancel()
        executor.shutdown(wait=True, cancel_futures=True)
