"""Collect real job-listing pages and persist each success before continuing."""

import asyncio
import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import click
from crawl4ai import CacheMode, CrawlerRunConfig
from crawl4ai.models import CrawlResult

from ex3.crawler import _open_crawler
from ex3.language import detect_document_language
from jobs_extraction_lab.models import Page, Source


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def content_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def load_pages(data_dir: Path) -> list[Page]:
    path = data_dir / "manifest.json"
    if not path.is_file():
        return []
    return [
        Page.model_validate(item)
        for item in json.loads(path.read_text(encoding="utf-8"))["pages"]
    ]


def is_job_link(url: str) -> bool:
    parsed = urlsplit(url)
    return (
        bool(re.search(r"/jobs/\d+", parsed.path))
        or bool(re.search(r"/(?:[a-f0-9]{8}-[a-f0-9-]{27,})(?:/|$)", parsed.path))
        or "gh_jid=" in parsed.query
    ) and not parsed.path.endswith("/apply")


async def collect_pages(
    sources_path: Path,
    data_dir: Path,
    *,
    target: int,
    max_jobs: int,
    concurrency: int,
    cdp_port: int,
    retry_rejected: bool,
) -> list[Page]:
    sources = [
        Source.model_validate(item)
        for item in json.loads(sources_path.read_text(encoding="utf-8"))
    ]
    pages = load_pages(data_dir)
    attempts_path = data_dir / "collection-attempts.json"
    attempts = (
        json.loads(attempts_path.read_text(encoding="utf-8"))
        if attempts_path.is_file()
        else []
    )
    accepted_ids = {page.id for page in pages}
    attempted_ids = {item["id"] for item in attempts}
    pending = [
        source
        for source in sources
        if source.id not in accepted_ids
        and (retry_rejected or source.id not in attempted_ids)
    ]
    for page in pages:
        markdown = (data_dir / page.markdown_file).read_text(encoding="utf-8")
        if content_hash(markdown) != page.markdown_sha256:
            raise ValueError(
                f"Stored Markdown changed: {page.id}; use a new data directory"
            )
    if len(pages) >= target:
        return pages

    async with _open_crawler(
        headless=True, proxy=None, cdp_port=cdp_port, accept_language="en-US,en;q=0.9"
    ) as crawler:

        async def fetch(source: Source) -> tuple[Source, CrawlResult | Exception]:
            try:
                result = await crawler.arun(  # ty: ignore[missing-argument] -- Crawl4AI decorator typing.
                    url=source.url,
                    config=CrawlerRunConfig(
                        cache_mode=CacheMode.BYPASS,
                        check_robots_txt=True,
                        page_timeout=45_000,
                        delay_before_return_html=2.0,
                        remove_overlay_elements=True,
                        remove_consent_popups=True,
                        verbose=False,
                    ),
                )
                return source, next(iter(result))
            except Exception as error:  # noqa: BLE001 -- Persist browser failures per page.
                return source, error

        offset = 0
        while offset < len(pending):
            group = pending[offset : offset + min(concurrency, target - len(pages))]
            if not group:
                break
            offset += len(group)
            for completed in asyncio.as_completed([fetch(source) for source in group]):
                source, result = await completed
                attempt = {"id": source.id, "url": source.url, "fetched_at": utc_now()}
                rejection = None
                if isinstance(result, Exception):
                    rejection = f"{type(result).__name__}: {result}"
                elif not result.success or (
                    result.status_code is not None and result.status_code >= 400
                ):
                    rejection = result.error_message or f"HTTP {result.status_code}"
                else:
                    markdown = (
                        result.markdown.raw_markdown
                        if result.markdown is not None
                        else ""
                    )
                    job_links = sorted(
                        {
                            link["href"]
                            for links in result.links.values()
                            if isinstance(links, list)
                            for link in links
                            if isinstance(link, dict)
                            and isinstance(link.get("href"), str)
                            and is_job_link(link["href"])
                        }
                    )
                    attempt.update(
                        markdown_chars=len(markdown), job_links=len(job_links)
                    )
                    if len(markdown.strip()) < 300:
                        rejection = "Empty or insufficient rendered content"
                    elif not 2 <= len(job_links) <= max_jobs:
                        rejection = f"Need 2–{max_jobs} visible job links; found {len(job_links)}"
                    elif len(markdown) > 60_000:
                        rejection = "Page exceeds this experiment's 60,000-character input budget"
                    elif content_hash(markdown) in {
                        page.markdown_sha256 for page in pages
                    }:
                        rejection = "Duplicate page content"
                    else:
                        markdown_file = f"markdown/{source.id}.md"
                        html_file = f"html/{source.id}.html"
                        for filename, body in (
                            (markdown_file, markdown),
                            (html_file, result.html or ""),
                        ):
                            path = data_dir / filename
                            path.parent.mkdir(parents=True, exist_ok=True)
                            path.write_text(body, encoding="utf-8")
                        pages.append(
                            Page(
                                id=source.id,
                                company=source.company,
                                source_url=source.url,
                                final_url=result.redirected_url or result.url,
                                platform=source.platform,
                                fetched_at=attempt["fetched_at"],
                                markdown_file=markdown_file,
                                html_file=html_file,
                                markdown_sha256=content_hash(markdown),
                                markdown_chars=len(markdown),
                                job_links=job_links,
                                title=(result.metadata or {}).get("title"),
                                language=detect_document_language(result.html or ""),
                            )
                        )
                        write_json(
                            data_dir / "manifest.json",
                            {
                                "created_for": "Jobs listing extraction: Codex versus Liquid on OpenRouter",
                                "target_pages": target,
                                "selection": f"Real rendered boards, 2–{max_jobs} visible job links, at most 60,000 Markdown characters; no truncation",
                                "pages": [page.model_dump() for page in pages],
                            },
                        )
                attempt.update(accepted=rejection is None, reason=rejection)
                attempts.append(attempt)
                write_json(attempts_path, attempts)
                click.echo(
                    f"[{len(pages)}/{target}] {source.id}: {rejection or 'saved'}"
                )
            if len(pages) >= target:
                break
    return pages
