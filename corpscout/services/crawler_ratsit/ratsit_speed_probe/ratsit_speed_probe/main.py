import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from urllib.request import urlopen

MAX_COMPANIES = 5_000
CONTENT_SELECTOR = ".main-inner"


class ProbeResponse(Protocol):
    status: int
    url: str
    headers: Mapping[str, str]
    body: bytes

    def css(self, selector: str) -> Sequence[object]: ...


FetchPage = Callable[[str], Awaitable[ProbeResponse]]
ReportResult = Callable[[dict[str, object]], None]


def validate_company_id(company_id: str) -> None:
    if (
        len(company_id) not in (10, 12)
        or not company_id.isascii()
        or not company_id.isdigit()
    ):
        raise ValueError(
            f"invalid company ID {company_id!r}: expected 10 or 12 ASCII digits"
        )


def ratsit_url(company_id: str) -> str:
    validate_company_id(company_id)
    return f"https://www.ratsit.se/{company_id[-10:]}"


def load_company_ids(path: Path, *, limit: int) -> list[str]:
    if not 1 <= limit <= MAX_COMPANIES:
        raise ValueError(f"limit must be between 1 and {MAX_COMPANIES}")

    if path == Path("-"):
        text = sys.stdin.read()
    else:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as error:
            raise ValueError(f"cannot read company ID file {path}: {error}") from error

    if path != Path("-") and path.suffix.lower() == ".json":
        try:
            value = json.loads(text)
        except json.JSONDecodeError as error:
            raise ValueError(f"invalid JSON in {path}: {error}") from error
        if not isinstance(value, list) or not all(
            isinstance(item, str) for item in value
        ):
            raise ValueError("JSON input must be an array of company ID strings")
        company_ids = value[:limit]
    else:
        company_ids = [line.strip() for line in text.splitlines() if line.strip()][
            :limit
        ]

    if not company_ids:
        raise ValueError("company ID file is empty")

    seen_company_ids: set[str] = set()
    seen_path_ids: set[str] = set()
    for company_id in company_ids:
        validate_company_id(company_id)
        if company_id in seen_company_ids:
            raise ValueError(f"duplicate company ID: {company_id}")
        if company_id[-10:] in seen_path_ids:
            raise ValueError(
                f"multiple company IDs resolve to Ratsit path {company_id[-10:]}"
            )
        seen_company_ids.add(company_id)
        seen_path_ids.add(company_id[-10:])

    return company_ids


def stop_reason_for_status(status: int) -> str | None:
    if status == 429:
        return "rate_limited"
    if status in (401, 403):
        return "blocked"
    if status >= 500:
        return "server_error"
    if status >= 400 and status != 404:
        return "unexpected_http_status"
    return None


async def probe_company_ids(
    company_ids: Sequence[str],
    *,
    fetch: FetchPage,
    output_path: Path,
    delay_ms: int = 0,
    report: ReportResult | None = None,
) -> dict[str, object]:
    if delay_ms < 0:
        raise ValueError("delay_ms must not be negative")

    summary_path = output_path.with_suffix(".summary.json")
    if output_path.exists() or summary_path.exists():
        raise FileExistsError("output or summary file already exists")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(UTC)
    started = time.perf_counter()
    statuses: Counter[int] = Counter()
    stop_reason = "completed"
    attempted = 0

    with output_path.open("x", encoding="utf-8") as output:
        for sequence, company_id in enumerate(company_ids, start=1):
            url = ratsit_url(company_id)
            request_started = time.perf_counter()
            result: dict[str, object] = {
                "sequence": sequence,
                "company_id": company_id,
                "url": url,
                "requested_at": datetime.now(UTC).isoformat(),
            }

            try:
                response = await fetch(url)
                elapsed_ms = round((time.perf_counter() - request_started) * 1_000)
                status = response.status
                statuses[status] += 1
                result.update(
                    {
                        "status": status,
                        "final_url": response.url,
                        "elapsed_ms": elapsed_ms,
                        "content_size_bytes": len(response.body),
                        "content_selector_found": bool(response.css(CONTENT_SELECTOR)),
                        "retry_after": _header(response.headers, "retry-after"),
                    }
                )
                current_stop_reason = stop_reason_for_status(status)
            except Exception as error:  # noqa: BLE001 - persist browser failures
                result.update(
                    {
                        "status": None,
                        "elapsed_ms": round(
                            (time.perf_counter() - request_started) * 1_000
                        ),
                        "error_type": type(error).__name__,
                        "error_message": str(error),
                    }
                )
                current_stop_reason = "fetch_error"

            attempted = sequence
            output.write(json.dumps(result, ensure_ascii=False) + "\n")
            output.flush()
            if report is not None:
                report(result)

            if current_stop_reason is not None:
                stop_reason = current_stop_reason
                break
            if sequence < len(company_ids) and delay_ms:
                await asyncio.sleep(delay_ms / 1_000)

    elapsed_seconds = time.perf_counter() - started
    summary: dict[str, object] = {
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "input_count": len(company_ids),
        "attempted": attempted,
        "stop_reason": stop_reason,
        "first_429_sequence": (attempted if stop_reason == "rate_limited" else None),
        "status_counts": {str(key): value for key, value in sorted(statuses.items())},
        "elapsed_seconds": round(elapsed_seconds, 3),
        "requests_per_second": round(attempted / elapsed_seconds, 3)
        if elapsed_seconds
        else None,
        "results_file": str(output_path),
    }
    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return summary


def _header(headers: Mapping[str, str], name: str) -> str | None:
    return next(
        (value for key, value in headers.items() if key.lower() == name.lower()),
        None,
    )


def read_cdp_websocket_url(port: int) -> str:
    endpoint = f"http://127.0.0.1:{port}/json/version"
    with urlopen(endpoint, timeout=5) as response:
        value = json.loads(response.read())
    websocket_url = value.get("webSocketDebuggerUrl")
    if not isinstance(websocket_url, str) or not websocket_url.startswith("ws"):
        raise RuntimeError("CloakBrowser did not expose a CDP WebSocket URL")
    return websocket_url


async def wait_for_cdp_websocket_url(port: int) -> str:
    error: Exception | None = None
    for _ in range(50):
        try:
            return await asyncio.to_thread(read_cdp_websocket_url, port)
        except (OSError, json.JSONDecodeError, RuntimeError) as current_error:
            error = current_error
            await asyncio.sleep(0.1)
    raise RuntimeError(f"CloakBrowser CDP endpoint did not start: {error}") from error


async def run_live_probe(
    company_ids: Sequence[str],
    *,
    output_path: Path,
    cdp_port: int,
    headless: bool,
    timeout_ms: int,
    delay_ms: int,
    disable_resources: bool,
    proxy: str | None,
) -> dict[str, object]:
    from cloakbrowser import launch_async
    from scrapling.fetchers import AsyncStealthySession

    browser = await launch_async(
        headless=headless,
        proxy=proxy,
        args=[
            f"--remote-debugging-port={cdp_port}",
            "--remote-debugging-address=127.0.0.1",
        ],
    )
    try:
        websocket_url = await wait_for_cdp_websocket_url(cdp_port)
        async with AsyncStealthySession(
            cdp_url=websocket_url,
            max_pages=1,
            retries=1,
            timeout=timeout_ms,
            disable_resources=disable_resources,
            google_search=False,
        ) as session:
            return await probe_company_ids(
                company_ids,
                fetch=session.fetch,
                output_path=output_path,
                delay_ms=delay_ms,
                report=_print_result,
            )
    finally:
        if browser.is_connected():
            await browser.close()


def _print_result(result: dict[str, object]) -> None:
    print(
        f"{result['sequence']}: company={result['company_id']} "
        f"status={result.get('status')} elapsed_ms={result['elapsed_ms']}",
        flush=True,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch Ratsit company pages sequentially and stop at the first 429."
    )
    parser.add_argument(
        "company_ids",
        type=Path,
        help="newline or JSON ID list; use - for newline-delimited stdin",
    )
    parser.add_argument("--output", type=Path, required=True, help="new JSONL file")
    parser.add_argument("--limit", type=int, default=MAX_COMPANIES)
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--timeout-ms", type=int, default=30_000)
    parser.add_argument("--cdp-port", type=int, default=9_245)
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--disable-resources", action="store_true")
    parser.add_argument(
        "--proxy",
        default=os.getenv("RATSIT_SPEED_PROBE_PROXY"),
        help="optional single CloakBrowser proxy; no rotation",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        company_ids = load_company_ids(args.company_ids, limit=args.limit)
        summary = asyncio.run(
            run_live_probe(
                company_ids,
                output_path=args.output,
                cdp_port=args.cdp_port,
                headless=not args.headed,
                timeout_ms=args.timeout_ms,
                delay_ms=args.delay_ms,
                disable_resources=args.disable_resources,
                proxy=args.proxy,
            )
        )
    except (FileExistsError, ValueError) as error:
        raise SystemExit(str(error)) from error
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
