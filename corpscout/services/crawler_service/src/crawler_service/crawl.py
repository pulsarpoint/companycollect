"""Collect useful company pages as local HTML, independently of fact extraction."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from collections.abc import Sequence
from contextlib import AsyncExitStack, nullcontext, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal
from urllib.parse import urlsplit
from uuid import uuid4

import click
import httpx
from dotenv import dotenv_values

from crawler_service.brave_browser import BraveSearch, BraveSearchBlocked
from crawler_service.browser_client import BrowserLeaseClient
from crawler_service.captures import capture_metadata, save_capture, save_crawl_result
from crawler_service.content import HtmlWindow
from crawler_service.discovery import (
    Candidate,
    CrawlQueue,
    crawlable_url,
    normalize_url,
    sitemap_urls,
)
from crawler_service.fetch import BrowserUnavailable, fetch_page, open_browser
from crawler_service.human_control import HumanAssistanceExpired, HumanSession
from crawler_service.link_selection import assess_links
from crawler_service.llm import ModelBudgetExceeded, ModelClient, ModelUnavailable
from crawler_service.models import (
    Page,
    RequestedContentAssessment,
    ResearchConfig,
)
from crawler_service.profiles import (
    classify_site,
    site_information,
)
from crawler_service.storage import utc_now, write_json
from crawler_service.web_search import discover_search_sources

LOGGER = logging.getLogger(__name__)

DEFAULT_SELECTION_INSTRUCTIONS = """Collect all useful source pages for these four areas:
1. Contact information: contact pages, office addresses, email, phone and profile links.
2. Jobs: careers listings and full job descriptions, including the target employer's
   external job board and its individual ads. Collect all useful job pages within budget.
3. About the company: identity, activities, products and services that describe what
   the company does. Include useful service/product detail pages as well as overviews.
4. Financial information: investor relations, financial statements, annual reports
   and filings. Find the pages linking to these documents; document links are saved
   without downloading the documents.
Choose pages for their source content only. Do not analyze jobs, infer skills or
technology usage, or seek technology-stack evidence. Skip engineering blogs,
tutorials, employee stories and news unless they directly provide the requested
company or financial information. Preserve job descriptions even when they mention
technologies; interpretation belongs to later offline processing.
"""


async def select_next_page(
    queue: CrawlQueue, llm: ModelClient, root: Path, manifest: dict
) -> tuple[Candidate, str] | None:
    """Rank available candidates against the same caller request on every pass."""
    if queue.available() and llm.unavailable:
        raise ModelUnavailable("Navigation model unavailable")
    if queue.available() and llm.remaining <= 0:
        raise ModelBudgetExceeded("Navigation model budget exhausted")
    manifest["errors"].extend(await assess_links(queue, llm, root, reserved_calls=0))
    selected = queue.pick_for_instructions()
    while selected is None and queue.assessment_batch() and llm.remaining > 0:
        manifest["errors"].extend(
            await assess_links(queue, llm, root, reserved_calls=0)
        )
        selected = queue.pick_for_instructions()
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
            else queue.source_budget_reason()
            or (
                "no_matching_candidates"
                if queue.instructions is not None
                else "no_promising_candidates"
            )
        )
    return selected


async def collect_pages(
    root: Path,
    manifest: dict,
    queue: CrawlQueue,
    llm: ModelClient | None,
    human: HumanSession | None = None,
    search: BraveSearch | None = None,
    browser_client: BrowserLeaseClient | None = None,
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
        crawler = await browser_stack.enter_async_context(
            open_browser(human, browser_client=browser_client)
            if browser_client is not None
            else open_browser(human)
            if human is not None
            else open_browser()
        )
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
                    html, links = await fetch_page(
                        crawler, page, settings, root, human=human
                    )
                    break
                except HumanAssistanceExpired:
                    manifest["pages"][-1] = capture_metadata(page)
                    raise
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
                    crawler = await browser_stack.enter_async_context(
                        open_browser(human, browser_client=browser_client)
                        if browser_client is not None
                        else open_browser(human)
                        if human is not None
                        else open_browser()
                    )
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
            if candidate is not None and page.source_url != page.requested_url:
                if queue.domain(page.source_url) == queue.domain(page.requested_url):
                    queue.redirects[page.source_url] = page.requested_url
            if candidate is not None and isinstance(
                candidate.assessment, RequestedContentAssessment
            ):
                manifest["pages"][-1]["selection"] = {
                    "requested_content": candidate.assessment.requested_content.model_dump(),
                    "reason": candidate.assessment.reason,
                    "basis": "link_metadata_hypothesis",
                    "target_relevance": candidate.assessment.target_relevance,
                    "follow_scope": candidate.assessment.follow_scope,
                    "priority": candidate.assessment.priority,
                    "navigation_root": candidate.navigation_root,
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
                # A one-page description is complete for any identified site type.
                # The classification decision only gates deeper collection.
                if (
                    not manifest["crawl_requested"]
                    and profile.evidence_status == "source_matched"
                ):
                    manifest["stop_reason"] = "site_info_complete"
                    break
                if decision != "continue_crawling":
                    manifest["status"] = decision
                    manifest["stop_reason"] = (
                        "not_company_website"
                        if decision == "skip_crawling"
                        else "site_eligibility_uncertain"
                    )
                    break
                queue.site_profile = profile.data
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
            if classify_first_page and len(manifest["pages"]) == 1:
                await discover_search_sources(
                    crawler,
                    queue,
                    llm,
                    root,
                    manifest,
                    phase="initial",
                    search=search,
                    human=human,
                )
            elif (
                len(manifest["pages"]) >= min(5, settings.max_pages // 2)
                and len(manifest["web_search"]["queries"])
                < settings.max_search_queries - 1
            ):
                await discover_search_sources(
                    crawler,
                    queue,
                    llm,
                    root,
                    manifest,
                    phase="followup",
                    search=search,
                    human=human,
                )
            selected = await select_next_page(queue, llm, root, manifest)
            if selected is None and manifest["stop_reason"] in {
                "no_matching_candidates",
                "source_page_budget",
                "source_domain_budget",
            }:
                await discover_search_sources(
                    crawler,
                    queue,
                    llm,
                    root,
                    manifest,
                    phase="exhausted",
                    search=search,
                    human=human,
                )
                if queue.assessment_batch():
                    manifest["stop_reason"] = None
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
    save_artifacts: bool = True,
    crawl: bool | Literal["full"] | None = None,
    config: ResearchConfig | None = None,
    api: Literal["deepseek", "openrouter"] = "deepseek",
    api_key: str | None = None,
    human: HumanSession | None = None,
    search: BraveSearch | None = None,
    browser_client: BrowserLeaseClient | None = None,
) -> dict:
    """Save cleaned HTML and return its manifest; never extract company facts.

    Supplied pages restrict the allowed URLs. Without instructions all are fetched
    without LLM calls; with instructions the selector chooses useful pages from
    that list. Without a list, discover contacts, jobs, company information and
    financial-information pages. Instructions guide every selection pass. Job and
    technology interpretation are deferred to offline processing of stored data.

    crawl="full" discovers all four areas with default limits of 100 pages and 30
    external pages. Explicit config limits override these defaults. Use pages or
    instructions separately for restricted/targeted collection.

    site_info alone describes the input page and stops. Combine it with pages,
    instructions, or crawl=True to also crawl. With a page list it explicitly
    requests the input page before the list; that page counts toward max_pages.

    save_artifacts=False retains only result.json; temporary working captures are
    removed when the run exits. The bundled JSON remains sufficient for analysis.
    """
    site_url = normalize_url(url)
    if crawl is not None and not isinstance(crawl, bool) and crawl != "full":
        raise ValueError('crawl must be true, false, or "full"')
    if crawl == "full" and (pages is not None or instructions is not None):
        raise ValueError(
            'crawl="full" cannot be combined with pages or instructions; omit crawl for targeted collection'
        )
    if instructions is not None:
        instructions = instructions.strip()
        if not instructions:
            raise ValueError("Selection instructions must not be empty")
    if isinstance(pages, str):
        raise ValueError("pages must be a list of URLs, not a single string")
    crawl_requested = (
        not site_info or pages is not None or instructions is not None
        if crawl is None
        else crawl is not False
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
        | (
            {"max_pages": 100, "max_external_pages": 30, "web_search": True}
            if crawl == "full"
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
    destination = output_dir.resolve()
    if destination.exists() and (
        not destination.is_dir() or any(destination.iterdir())
    ):
        raise ValueError(f"Output directory is not empty: {destination}")
    destination.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with (
        nullcontext(destination)
        if save_artifacts
        else TemporaryDirectory(prefix="company-crawl-work-")
    ) as workspace:
        root = Path(workspace)
        queue = CrawlQueue(
            site_url,
            settings,
            instructions=instructions
            if instructions is not None or pages is not None
            else DEFAULT_SELECTION_INSTRUCTIONS,
            allowed_urls=set(requested) if pages is not None else None,
        )
        # Nominate source pages without the legacy engineering/technology priorities.
        queue.objective_order = [
            "company_contacts",
            "jobs",
            "company_profile",
            "document_links",
            "locations",
            "products_services",
        ]
        for candidate_url in requested or [site_url]:
            queue.add(
                candidate_url,
                source=site_url,
                label=None if requested else "Input website",
            )
        manifest: dict = {
            "schema_version": "company-crawl/1.0",
            "artifacts_saved": save_artifacts,
            "run_id": uuid4().hex,
            "input_url": url,
            "site_url": site_url,
            "mode": "site_info"
            if not crawl_requested
            else "full"
            if crawl == "full"
            else "supplied_pages"
            if pages is not None
            else "discovery",
            "requested_pages": requested,
            "selection_instructions": instructions,
            "effective_selection_instructions": queue.instructions,
            "processing": {
                "stage": "collection",
                "job_analysis": "deferred",
                "technology_analysis": "deferred",
            },
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
            "web_search": {
                "provider": "brave_browser",
                "status": "pending"
                if settings.web_search
                and settings.max_search_queries > 0
                and pages is None
                and crawl_requested
                else "not_requested",
                "phases": [],
                "queries": [],
                "errors": [],
            },
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
                await collect_pages(
                    root,
                    manifest,
                    queue,
                    llm,
                    human=human,
                    search=search,
                    browser_client=browser_client,
                )
        except BraveSearchBlocked as error:
            manifest["status"] = "failed"
            manifest["stop_reason"] = "brave_search_blocked"
            manifest["errors"].append({"stage": "search", "error": str(error)})
        except HumanAssistanceExpired as error:
            manifest["status"] = (
                "partial"
                if any(page["fetch_status"] == "fetched" for page in manifest["pages"])
                else "failed"
            )
            manifest["stop_reason"] = "human_assistance_timeout"
            manifest["human_assistance"] = human.failure if human is not None else None
            manifest["errors"].append({"stage": "crawl", "error": str(error)})
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
            manifest["elapsed_seconds"] = round(time.monotonic() - started, 3)
            if manifest["web_search"]["status"] == "pending":
                manifest["web_search"].update(
                    status="not_run",
                    reason=manifest["stop_reason"] or "crawl_interrupted",
                )
            manifest["discovery"] = {
                "navigation_sources": queue.navigation_sources,
                "source_pages": dict(queue.source_pages),
                "document_links": list(queue.document_candidates.values()),
                "excluded": dict(queue.excluded),
            }
            if llm is not None:
                manifest["usage"] = llm.usage()
            if (
                manifest["stop_reason"] not in {
                    "human_assistance_timeout",
                    "site_info_complete",
                }
                and (not requested or site_info)
                and manifest["site_gate"]["decision"]
                not in {
                    "continue_crawling",
                    "skip_crawling",
                }
            ):
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
                    or bool(manifest["web_search"]["errors"])
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
            if human is not None:
                manifest["challenge_agent_max_runs"] = human.challenge_agent_max_runs
                manifest["challenge_agent_model"] = human.challenge_agent_model
                manifest["challenge_agent_budget_exhausted"] = (
                    human.challenge_agent_budget_exhausted
                )
                manifest["challenge_agent_results"] = human.challenge_agent_results
                if human.challenge_agent_result is not None:
                    manifest["challenge_agent"] = human.challenge_agent_result
            write_json(root / "crawl-manifest.json", manifest)
            save_crawl_result(root, manifest, destination=destination)
        return manifest


def main() -> None:
    """Save local HTML from a page list or instruction-guided discovery."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="Target company website")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--save-artifacts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Retain separate HTML and diagnostic files (development default). Use --no-save-artifacts for result.json only.",
    )
    parser.add_argument(
        "--site-info",
        action="store_true",
        help="Describe the input page in a few sentences. Alone, stop there; with pages/instructions/--crawl, continue crawling.",
    )
    parser.add_argument(
        "--crawl",
        nargs="?",
        choices=["full"],
        const=True,
        default=None,
        help="Use --crawl full for all four collection areas (100 pages/30 external by default). Bare --crawl continues discovery with --site-info.",
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
    parser.add_argument(
        "--challenge-agent-max-runs",
        type=int,
        help="Enable automatic CAPTCHA assistance with this per-crawl budget (3–1000).",
    )
    parser.add_argument(
        "--challenge-agent-model",
        choices=["deepseek-flash", "z-ai/glm-5.3-flash"],
        default="deepseek-flash",
    )
    parser.add_argument("--max-model-calls", type=int)
    parser.add_argument("--max-external-pages", type=int)
    parser.add_argument(
        "--web-search",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Discover additional sources through Brave search (enabled for --crawl full).",
    )
    parser.add_argument("--max-search-queries", type=int)
    parser.add_argument("--max-source-domains", type=int)
    parser.add_argument("--max-source-pages-per-domain", type=int)
    parser.add_argument("--max-source-depth", type=int)
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
            "web_search": args.web_search,
            "max_search_queries": args.max_search_queries,
            "max_source_domains": args.max_source_domains,
            "max_source_pages_per_domain": args.max_source_pages_per_domain,
            "max_source_depth": args.max_source_depth,
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
        agent_budget = args.challenge_agent_max_runs
        if (
            agent_budget is None
            and environment.get("CRAWL_CHALLENGE_AGENT_ENABLED", "false").lower()
            == "true"
        ):
            agent_budget = int(environment.get("CRAWL_CHALLENGE_AGENT_MAX_RUNS", "3"))
        if agent_budget is not None and not 3 <= agent_budget <= 1000:
            raise ValueError("--challenge-agent-max-runs must be between 3 and 1000")

        async def run() -> dict:
            browser_url = environment.get("BROWSER_API_URL")
            if not browser_url:
                raise ValueError(
                    "Configure BROWSER_API_URL for the external browser service"
                )
            token = environment.get("BROWSER_API_TOKEN")
            async with httpx.AsyncClient(
                base_url=browser_url,
                timeout=330,
                headers={"Authorization": f"Bearer {token}"} if token else {},
            ) as http:
                identifier = uuid4().hex
                async with BrowserLeaseClient(http).lease(
                    identifier=identifier,
                    request_id=identifier,
                    domain=urlsplit(normalize_url(args.url)).hostname or args.url,
                ) as browser_client:
                    search = BraveSearch(
                        args.output_dir / ".brave-search", browser_client=browser_client
                    )
                    try:
                        human = (
                            HumanSession(
                                headed=True,
                                interactive=False,
                                timeout=10,
                                challenge_agent_max_runs=agent_budget,
                                challenge_agent_model=args.challenge_agent_model,
                                notify=lambda state, values: LOGGER.info(
                                    "%s: %s", state, values.get("reason", "")
                                ),
                            )
                            if agent_budget is not None
                            else None
                        )
                        return await crawl_company(
                            args.url,
                            browser_client=browser_client,
                            human=human,
                            search=search,
                            output_dir=args.output_dir,
                            pages=args.pages,
                            instructions=instructions,
                            site_info=args.site_info,
                            save_artifacts=args.save_artifacts,
                            crawl=args.crawl,
                            config=config,
                            api=args.api,
                            api_key=environment.get(
                                "DEEPSEEK"
                                if args.api == "deepseek"
                                else "OPENROUTER_API_KEY"
                            ),
                        )
                    finally:
                        await search.close()

        with redirect_stdout(sys.stderr):
            manifest = asyncio.run(run())
    except (ValueError, OSError) as error:
        parser.error(str(error))
    click.echo(json.dumps(manifest, ensure_ascii=False))
    if manifest["status"] in {"failed", "needs_review"}:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
