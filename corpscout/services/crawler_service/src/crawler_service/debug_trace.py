"""Opt-in, per-attempt traces. Payloads stay on disk; live streaming reads only the index."""

import json
import logging
import re
import threading
import time
import traceback
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path

import httpx


CURRENT_TRACE: ContextVar["CrawlTrace | None"] = ContextVar("crawl_trace", default=None)
SECRET_FIELD = re.compile(r"authorization|cookie|password|secret|(?:^|[_-])(?:api[_-]?key|key|token)(?:$|[_-])|access[_-]?token|refresh[_-]?token|credential|encrypted", re.I)


class CrawlTrace(logging.Handler):
    def __init__(self, directory: Path, secrets: list[str]):
        super().__init__(logging.DEBUG)
        self.directory = directory / "debug"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.secrets = {secret for secret in secrets if secret}
        self.started = time.monotonic()
        self.sequence = 0
        self.write_lock = threading.Lock()

    def redact(self, value: object) -> object:
        if isinstance(value, dict):
            return {str(self.redact(str(key))): "[REDACTED]" if SECRET_FIELD.search(str(key))
                    else "[image omitted]" if str(key).lower() == "screenshot"
                    else self.redact(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self.redact(item) for item in value]
        if isinstance(value, str):
            for secret in sorted(self.secrets, key=len, reverse=True):
                value = value.replace(secret, "[REDACTED]").replace(json.dumps(secret)[1:-1], "[REDACTED]")
            value = re.sub(r"(?i)(bearer\s+)[^\s\"'<>]+", r"\1[REDACTED]", value)
            value = re.sub(r"(?i)([?&](?:api[_-]?key|token|access_token|signature|x-amz-signature|x-amz-credential)=)[^&#\s\"']+", r"\1[REDACTED]", value)
            return re.sub(r"(https?://)[^/\s:@]+:[^/\s@]+@", r"\1[REDACTED]@", value)
        return value

    def event(self, stage: str, message: str, *, level: str = "info", details: object = None,
              duration_ms: float | None = None, operation: str | None = None) -> int:
        # Synchronous appends keep order across asyncio tasks; the lock also covers
        # to_thread callbacks. No provider data reaches disk before redaction.
        with self.write_lock:
            self.sequence += 1
            if operation is None and stage.endswith("_http") and message.endswith(" · sent"):
                operation = f"http-{self.sequence}"
            event = self.redact({
                "id": self.sequence, "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
                "elapsed_ms": round((time.monotonic() - self.started) * 1000, 1),
                "stage": stage, "level": level, "message": message,
                "duration_ms": duration_ms, "operation": operation, "has_details": True,
            })
            (self.directory / f"{self.sequence:08}.json").write_text(
                json.dumps(self.redact(details) if details is not None else event, ensure_ascii=False, default=str), encoding="utf-8")
            with (self.directory / "events.jsonl").open("ab") as output:
                output.write((json.dumps(event, ensure_ascii=False) + "\n").encode())
            return self.sequence

    def emit(self, record: logging.LogRecord) -> None:
        if CURRENT_TRACE.get() is self and record.name.startswith("crawler_service"):
            self.event("log", record.getMessage(), level=record.levelname.lower(), details={
                "logger": record.name,
                "traceback": "".join(traceback.format_exception(*record.exc_info)) if record.exc_info else None,
            })


def trace_event(stage: str, message: str, **values) -> None:
    trace = CURRENT_TRACE.get()
    if trace is not None:
        trace.event(stage, message, **values)


def trace_artifact(path: Path, value: object) -> None:
    trace = CURRENT_TRACE.get()
    if trace is None or path.name in {"job.json", "request.json"}:
        return
    name = "/".join(path.parts[-2:])
    if path.parent.name == "calls" and isinstance(value, dict):
        outcome = "failed" if value.get("error") else "received" if value.get("response") is not None else "updated"
        trace.event("model", f"{value.get('task', 'Model call')} · call {value.get('call_id')} · {outcome}",
                    level="error" if value.get("error") else "info", details=value,
                    operation=f"model-{value.get('call_id')}",
                    duration_ms=round(value["elapsed_seconds"] * 1000, 1) if isinstance(value.get("elapsed_seconds"), (int, float)) else None)
    else:
        trace.event("artifact", f"Saved {name}", details=value)


def trace_http_hooks(stage: str) -> dict:
    trace = CURRENT_TRACE.get()
    if trace is None:
        return {}

    async def sent(request: httpx.Request) -> None:
        content = await request.aread()
        try:
            body = json.loads(content) if content else None
        except ValueError:
            body = content.decode("utf-8", errors="replace")
        started = time.monotonic()
        identifier = trace.event(stage, f"{request.method} {request.url.path} · sent", details={
            "method": request.method, "url": str(request.url), "body": body,
        })
        request.extensions["crawl_debug"] = (started, f"http-{identifier}")

    async def received(response: httpx.Response) -> None:
        started, operation = response.request.extensions["crawl_debug"]
        # Include body download time, not just time to response headers.
        await response.aread()
        try:
            body = response.json()
        except ValueError:
            body = response.text
        trace.event(stage, f"{response.request.method} {response.request.url.path} · HTTP {response.status_code}",
                    level="error" if response.is_error else "info", operation=operation,
                    duration_ms=round((time.monotonic() - started) * 1000, 1), details={
                        "method": response.request.method, "url": str(response.request.url),
                        "status": response.status_code, "content_type": response.headers.get("content-type"), "body": body,
                    })
    return {"request": [sent], "response": [received]}


def read_trace(directory: Path, after: int, limit: int) -> dict:
    path = directory / "debug" / "events.jsonl"
    if not path.exists():
        return {"events": [], "cursor": 0, "has_more": False}
    events = []
    with path.open("rb") as source:
        size = path.stat().st_size
        if after > size:
            raise ValueError("Trace cursor is beyond the saved trace")
        if after:
            source.seek(after - 1)
            if source.read(1) != b"\n":
                raise ValueError("Invalid trace cursor")
        cursor = after
        incomplete = False
        for _ in range(limit):
            line = source.readline()
            if not line.endswith(b"\n"):
                incomplete = True
                break  # A partial last write will be returned by the next poll.
            events.append(json.loads(line))
            cursor = source.tell()
        return {"events": events, "cursor": cursor, "has_more": cursor < size and not incomplete}
