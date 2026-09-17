"""Collect useful company pages as local HTML, independently of fact extraction."""

import argparse
import asyncio
import json
import logging
import os
import sys
from collections.abc import Sequence
from contextlib import AsyncExitStack, redirect_stdout
from pathlib import Path
from typing import Literal
from uuid import uuid4

import click
import httpx
from dotenv import dotenv_values

from company_research.captures import capture_metadata, save_capture, save_crawl_result
from company_research.content import HtmlWindow
from company_research.discovery import (
    Candidate,
    CrawlQueue,
    crawlable_url,
    normalize_url,
    sitemap_urls,
)
from company_research.fetch import BrowserUnavailable, fetch_page, open_browser
from company_research.link_selection import assess_links
from company_research.llm import ModelBudgetExceeded, ModelClient, ModelUnavailable
from company_research.models import (
    OBJECTIVES,
    Page,
    RequestedContentAssessment,
    ResearchConfig,
)
from company_research.profiles import (
    classify_site,
    profile_objectives,
    site_information,
)
from company_research.storage import utc_now, write_json

LOGGER = logging.getLogger(__name__)


async def select_next_page(
    queue: CrawlQueue, llm: ModelClient, root: Path, manifest: dict
) -> tuple[Candidate, str] | None:
    """Rank available candidates against the same caller request on every pass."""
    if queue.available() and llm.unavailable:
        raise ModelUnavailable("Navigation model unavailable")
    if queue.available() and llm.remaining <= 0:
        raise ModelBudgetExceeded("Navigation model budget exhausted")
    manifest["errors"].extend(await assess_links(queue, llm, root, reserved_calls=0))
    selected = queue.pick(dict.fromkeys(OBJECTIVES, 0))
    while selected is None and queue.assessment_batch() and llm.remaining > 0:
        manifest["errors"].extend(
            await assess_links(queue, llm, root, reserved_calls=0)
        )
        selected = queue.pick(dict.fromkeys(OBJECTIVES, 0))
    if selected is None:
        manifest["stop_reason"] = (
            "model_call_budget"
            if queue.assessment_batch() and llm.remaining <= 0
            else "external_page_budget"
            if queue.external_pages >= queue.config.max_external_pages
            and any(
                candidate.external
                and candidate.url not in queue.visited
                and (
                    not isinstance(candidate.assessment, RequestedContentAssessment)
                    or candidate.assessment.requested_content.potential != "low"
                )
                for candidate in queue.candidates.values()
            )
            else "no_matching_candidates"
            if queue.instructions is not None
            else "no_promising_candidates"
        )
    return selected


async def collect_pages(
    root: Path, manifest: dict, queue: CrawlQueue, llm: ModelClient | None
) -> None:
    """Own browser lifetime and bounded traversal; publish each capture before ranking."""
    settings = queue.config
    site_url = queue.site_url
    requested = manifest["requested_pages"]
    classify_first_page = not requested or manifest["site_info_requested"]
    if requested and queue.instructions is not None and not classify_first_page:
        assert llm is not None
        selected = await select_next_page(queue, llm, root, manifest)
        if selected is None:
            return
        candidate, focus = selected
        next_url = candidate.url
    else:
        next_url, focus = (
            (site_url, "input_url")
            if classify_first_page
            else (requested[0], "supplied_page")
        )
    async with AsyncExitStack() as browser_stack:
        crawler = await browser_stack.enter_async_context(open_browser())
        while len(manifest["pages"]) < settings.max_pages:
            page = Page(
                page_id=f"p{len(manifest['pages']) + 1:04}",
                requested_url=next_url,
                source_url=next_url,
                selected_for=focus,
                fetched_at=utc_now(),
                status_code=None,
                fetch_status="pending",
                extraction_status="not_assessed",
                objectives_examined=[],
                attempts=0,
                chunks_planned=0,
                chunks_completed=0,
                html_sha256=None,
                html_file=None,
                errors=[],
            )
            queue.visited.add(next_url)
            LOGGER.info("Fetching %s (%s)", next_url, focus)
            # Persist the attempted page even if the browser cannot recover.
            manifest["pages"].append(capture_metadata(page))
            write_json(root / "crawl-manifest.json", manifest)
            while True:
                try:
                    html, links = await fetch_page(crawler, page, settings, root)
                    break
                except BrowserUnavailable:
                    manifest["pages"][-1] = capture_metadata(page)
                    if (
                        len(manifest["browser_recoveries"])
                        >= settings.max_browser_restarts
                    ):
                        raise
                    recovery = {
                        "page_id": page.page_id,
                        "failed_attempt": page.attempts,
                        "started_at": utc_now(),
                    }
                    manifest["browser_recoveries"].append(recovery)
                    try:
                        await browser_stack.aclose()
                    except Exception as error:
                        recovery["cleanup_error"] = type(error).__name__
                    crawler = await browser_stack.enter_async_context(open_browser())
            if page.fetch_status == "fetched":
                final_url = normalize_url(page.source_url)
                duplicate = any(
                    saved["fetch_status"] == "fetched"
                    and normalize_url(saved["source_url"]) == final_url
                    for saved in manifest["pages"][:-1]
                )
                queue.visited.add(final_url)
                if classify_first_page and len(manifest["pages"]) == 1:
                    site_url = final_url
                    manifest["site_url"] = final_url
                    queue.site_url, queue.site_domain = (
                        final_url,
                        queue.domain(final_url),
                    )
                    for candidate in queue.candidates.values():
                        candidate.external = (
                            queue.domain(candidate.url) != queue.site_domain
                        )
                if duplicate:
                    page.fetch_status = "duplicate"
                else:
                    manifest["pages"][-1] = save_capture(root, page, html, site_url)
            if page.fetch_status != "fetched":
                manifest["pages"][-1] = capture_metadata(page)
            candidate = queue.candidates.get(page.requested_url)
            if candidate is not None and isinstance(
                candidate.assessment, RequestedContentAssessment
            ):
                manifest["pages"][-1]["selection"] = {
                    "requested_content": candidate.assessment.requested_content.model_dump(),
                    "reason": candidate.assessment.reason,
                    "basis": "link_metadata_hypothesis",
                }
            write_json(root / "crawl-manifest.json", manifest)
            if classify_first_page and len(manifest["pages"]) == 1:
                assert llm is not None
                if page.fetch_status != "fetched":
                    manifest["status"] = "needs_review"
                    manifest["site_gate"] = {
                        "decision": "needs_review",
                        "reason": "initial_page_unavailable",
                    }
                    manifest["stop_reason"] = "initial_page_unavailable"
                    break
                profile = await classify_site(
                    HtmlWindow(0, len(html), html), page, llm, root
                )
                decision = (
                    profile.data["crawl_decision"]
                    if profile.evidence_status == "source_matched"
                    else "needs_review"
                )
                manifest["site_gate"] = {
                    "decision": decision,
                    "profile": profile.model_dump(),
                    "scope": "first_page_only",
                }
                if manifest["site_info_requested"] or decision != "continue_crawling":
                    manifest["site_info"] = site_information(profile, page.source_url)
                if decision != "continue_crawling":
                    manifest["status"] = decision
                    manifest["stop_reason"] = (
                        "not_company_website"
                        if decision == "skip_crawling"
                        else "site_eligibility_uncertain"
                    )
                    break
                if not manifest["crawl_requested"]:
                    manifest["stop_reason"] = "site_info_complete"
                    break
                queue.site_profile = profile.data
                queue.objective_order = profile_objectives(profile)
                if not requested:
                    async with httpx.AsyncClient() as web_http:
                        inventory = await sitemap_urls(web_http, site_url, settings)
                    write_json(root / "sitemaps.json", inventory)
                    manifest["sitemap"] = {
                        name: value
                        for name, value in inventory.items()
                        if name not in {"urls", "url_sources"}
                    }
                    manifest["sitemap"]["url_count"] = len(inventory["urls"])
                    for value in inventory["urls"]:
                        if queue.domain(value) == queue.site_domain:
                            queue.add(value, source=inventory["url_sources"][value])
            if requested and queue.instructions is None:
                remaining = [value for value in requested if value not in queue.visited]
                if not remaining:
                    manifest["stop_reason"] = "supplied_pages_exhausted"
                    break
                next_url, focus = remaining[0], "supplied_page"
                continue
            assert llm is not None
            for link in links:
                if isinstance(link.get("href"), str):
                    queue.add(
                        link["href"],
                        source=page.source_url,
                        title=link.get("title"),
                        label=link.get("text"),
                        context=link.get("context"),
                    )
            write_json(root / "queue.json", queue.snapshot())
            if len(manifest["pages"]) >= settings.max_pages:
                manifest["stop_reason"] = "page_budget"
                break
            selected = await select_next_page(queue, llm, root, manifest)
            if selected is None:
                break
            candidate, focus = selected
            next_url = candidate.url


async def crawl_company(
    url: str,
    *,
    output_dir: Path,
    pages: Sequence[str] | None = None,
    instructions: str | None = None,
    site_info: bool = False,
    crawl: bool | None = None,
    config: ResearchConfig | None = None,
    api: Literal["deepseek", "openrouter"] = "deepseek",
    api_key: str | None = None,
) -> dict:
    """Save cleaned HTML and return its manifest; never extract company facts.

    Supplied pages restrict the allowed URLs. Without instructions all are fetched
    without LLM calls; with instructions the selector chooses useful pages from
    that list. Without a list, discover pages from the target site. Instructions
    guide every selection pass. Fact extraction runs separately on saved HTML.

    site_info alone describes the input page and stops. Combine it with pages,
    instructions, or crawl=True to also crawl. With a page list it explicitly
    requests the input page before the list; that page counts toward max_pages.
    """
    site_url = normalize_url(url)
    if instructions is not None:
        instructions = instructions.strip()
        if not instructions:
            raise ValueError("Selection instructions must not be empty")
    if isinstance(pages, str):
        raise ValueError("pages must be a list of URLs, not a single string")
    crawl_requested = (
        not site_info or pages is not None or instructions is not None
        if crawl is None
        else crawl
    )
    if not crawl_requested and (
        not site_info or pages is not None or instructions is not None
    ):
        raise ValueError(
            "crawl=False requires site_info=True without pages or instructions"
        )
    settings = ResearchConfig.model_validate(
        (
            {"model": "deepseek-flash", "provider": None, "reasoning_effort": "high"}
            if api == "deepseek"
            else {}
        )
        | (config.model_dump(exclude_unset=True) if config is not None else {})
    )
    requested = (
        list(dict.fromkeys(normalize_url(value, site_url) for value in pages))
        if pages is not None
        else []
    )
    if pages is not None and not requested:
        raise ValueError("Supply at least one page, or omit pages for discovery")
    if any(not crawlable_url(value) for value in requested):
        raise ValueError(
            "Supplied pages must be public HTML page URLs, not documents or account pages"
        )
    required_pages = set(requested)
    if site_info:
        required_pages.add(site_url)
    if instructions is None and len(required_pages) > settings.max_pages:
        raise ValueError(
            "max_pages is smaller than the supplied pages plus the requested site-info page"
        )
    if instructions is not None and len(requested) > settings.max_candidates:
        raise ValueError("max_candidates is smaller than the supplied page list")
    credential = "DEEPSEEK" if api == "deepseek" else "OPENROUTER_API_KEY"
    needs_model = pages is None or instructions is not None or site_info
    key = (api_key or os.environ.get(credential)) if needs_model else None
    if needs_model and not key:
        raise ValueError(f"Set {credential} or pass api_key for automatic discovery")
    root = output_dir.resolve()
    if root.exists() and (not root.is_dir() or any(root.iterdir())):
        raise ValueError(f"Output directory is not empty: {root}")
    root.mkdir(parents=True, exist_ok=True)
    queue = CrawlQueue(
        site_url,
        settings,
        instructions=instructions,
        allowed_urls=set(requested) if pages is not None else None,
    )
    for candidate_url in requested or [site_url]:
        queue.add(
            candidate_url, source=site_url, label=None if requested else "Input website"
        )
    manifest: dict = {
        "schema_version": "company-crawl/1.0",
        "run_id": uuid4().hex,
        "input_url": url,
        "site_url": site_url,
        "mode": "site_info"
        if not crawl_requested
        else "supplied_pages"
        if pages is not None
        else "discovery",
        "requested_pages": requested,
        "selection_instructions": instructions,
        "site_info_requested": site_info,
        "crawl_requested": crawl_requested,
        "site_info": None,
        "started_at": utc_now(),
        "finished_at": None,
        "status": "running",
        "stop_reason": None,
        "config": settings.model_dump(),
        "pages": [],
        "site_gate": {
            "decision": "not_requested",
            "reason": "supplied_pages"
            if pages is not None and not site_info
            else "pending",
        },
        "sitemap": {"status": "not_requested"},
        "selection_feedback": "navigation_only; extraction coverage and record yields are unavailable",
        "site_coverage": "not_established",
        "errors": [],
        "browser_recoveries": [],
        "usage": {"calls": 0},
    }
    write_json(root / "crawl-manifest.json", manifest)
    llm = None
    try:
        async with AsyncExitStack() as stack:
            if needs_model:
                model_http = await stack.enter_async_context(
                    httpx.AsyncClient(
                        base_url="https://api.deepseek.com/"
                        if api == "deepseek"
                        else "https://openrouter.ai/api/v1/"
                    )
                )
                assert key is not None
                llm = ModelClient(model_http, key, settings, root, api=api)
            await collect_pages(root, manifest, queue, llm)
    except (BrowserUnavailable, ModelBudgetExceeded, ModelUnavailable) as error:
        manifest["stop_reason"] = (
            "browser_unavailable"
            if isinstance(error, BrowserUnavailable)
            else "model_call_budget"
            if isinstance(error, ModelBudgetExceeded)
            else "model_unavailable"
        )
        manifest["errors"].append({"stage": "crawl", "error": str(error)})
    except Exception as error:
        manifest["stop_reason"] = "run_error"
        message = str(error).replace(key, "[REDACTED]") if key else str(error)
        manifest["errors"].append(
            {"stage": "crawl", "error": f"{type(error).__name__}: {message[:1000]}"}
        )
        LOGGER.error("Crawl stopped: %s", manifest["errors"][-1]["error"])
    finally:
        manifest["finished_at"] = utc_now()
        if llm is not None:
            manifest["usage"] = llm.usage()
        if (not requested or site_info) and manifest["site_gate"]["decision"] not in {
            "continue_crawling",
            "skip_crawling",
        }:
            manifest["status"] = "needs_review"
            manifest["site_gate"]["decision"] = "needs_review"
            manifest["stop_reason"] = (
                manifest["stop_reason"] or "site_eligibility_uncertain"
            )
        if manifest["status"] == "running":
            manifest["stop_reason"] = manifest["stop_reason"] or "interrupted"
            success = any(
                page["fetch_status"] == "fetched" for page in manifest["pages"]
            )
            incomplete = (
                manifest["stop_reason"]
                not in {
                    "supplied_pages_exhausted",
                    "no_promising_candidates",
                    "no_matching_candidates",
                    "site_info_complete",
                }
                or bool(manifest["errors"])
                or any(
                    page["fetch_status"] in {"pending", "failed"}
                    for page in manifest["pages"]
                )
            )
            manifest["status"] = (
                "finished"
                if not incomplete
                and manifest["stop_reason"] == "no_matching_candidates"
                else "failed"
                if not success
                else "partial"
                if incomplete
                else "finished"
            )
        if not requested or instructions is not None:
            write_json(root / "queue.json", queue.snapshot())
        if manifest["site_info"] is None and (
            site_info or manifest["status"] in {"skip_crawling", "needs_review"}
        ):
            manifest["site_info"] = site_information(None, manifest["site_url"])
        if manifest["site_info"] is not None:
            write_json(root / "site-info.json", manifest["site_info"])
        write_json(root / "crawl-manifest.json", manifest)
        save_crawl_result(root, manifest)
    return manifest


def main() -> None:
    """Save local HTML from a page list or instruction-guided discovery."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="Target company website")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--site-info",
        action="store_true",
        help="Describe the input page in a few sentences. Alone, stop there; with pages/instructions/--crawl, continue crawling.",
    )
    parser.add_argument(
        "--crawl",
        action="store_const",
        const=True,
        default=None,
        help="Continue normal discovery with --site-info, even without pages or instructions.",
    )
    parser.add_argument(
        "--pages",
        "--page",
        nargs="+",
        action="extend",
        help="Allowed page URL list. Without instructions, fetch all; otherwise select within this list.",
    )
    instruction_source = parser.add_mutually_exclusive_group()
    instruction_source.add_argument(
        "--instructions",
        help="Custom instructions applied at every page-selection step",
    )
    instruction_source.add_argument(
        "--instructions-file",
        type=Path,
        help="UTF-8 file containing custom selection instructions",
    )
    parser.add_argument("--max-pages", type=int)
    parser.add_argument("--max-model-calls", type=int)
    parser.add_argument("--max-external-pages", type=int)
    parser.add_argument("--api", choices=["deepseek", "openrouter"], default="deepseek")
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
    environment = dict(os.environ)
    if args.env_file is not None:
        environment.update(
            {
                key: value
                for key, value in dotenv_values(args.env_file).items()
                if value is not None
            }
        )
    values = {
        key: value
        for key, value in {
            "max_pages": args.max_pages,
            "max_model_calls": args.max_model_calls,
            "max_external_pages": args.max_external_pages,
        }.items()
        if value is not None
    }
    if args.api == "deepseek":
        values.update(model="deepseek-flash", provider=None, reasoning_effort="high")
    try:
        instructions = (
            args.instructions_file.read_text(encoding="utf-8")
            if args.instructions_file is not None
            else args.instructions
        )
        config = ResearchConfig.model_validate(values)
        with redirect_stdout(sys.stderr):
            manifest = asyncio.run(
                crawl_company(
                    args.url,
                    output_dir=args.output_dir,
                    pages=args.pages,
                    instructions=instructions,
                    site_info=args.site_info,
                    crawl=args.crawl,
                    config=config,
                    api=args.api,
                    api_key=environment.get(
                        "DEEPSEEK" if args.api == "deepseek" else "OPENROUTER_API_KEY"
                    ),
                )
            )
    except (ValueError, OSError) as error:
        parser.error(str(error))
    click.echo(json.dumps(manifest, ensure_ascii=False))
    if manifest["status"] in {"failed", "needs_review"}:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
