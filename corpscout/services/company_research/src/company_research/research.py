"""One URL in, attributed company findings and explicit coverage statuses out."""

import json
import logging
import os
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Literal
from uuid import uuid4

import httpx
from bs4 import BeautifulSoup
from pydantic import ValidationError

from company_research.analytics import (
    accepted_finding,
    source_supported_finding,
    summarize_entities,
    summarize_technologies,
)
from company_research.content import (
    HtmlWindow,
    merge_finding,
    source_finding,
    split_html,
)
from company_research.discovery import CrawlQueue, normalize_url, sitemap_urls
from company_research.external_links import assess_external_links
from company_research.fetch import BrowserUnavailable, fetch_page, open_browser
from company_research.link_selection import assess_links
from company_research.llm import ModelBudgetExceeded, ModelClient, ModelUnavailable
from company_research.models import (
    OBJECTIVES,
    RECORD_TYPES,
    ChunkExtractionAttempt,
    ExternalLink,
    Extraction,
    Finding,
    Findings,
    JobDetailContext,
    ObjectiveStatus,
    Page,
    ResearchConfig,
    ResearchResult,
)
from company_research.profiles import (
    classify_site,
    profile_objectives,
    summarize_company,
)
from company_research.prompts import extraction_prompt
from company_research.resolution import resolve_technologies
from company_research.review import (
    REVIEW_OBJECTIVES,
    correct_reviewed_claims,
    repair_evidence,
    review_claims,
    review_proposals,
)
from company_research.storage import content_hash, utc_now, write_json
from company_research.technology_catalog import (
    TechnologyCatalog,
)

LOGGER = logging.getLogger(__name__)


async def extract_window(
    window: HtmlWindow,
    page: Page,
    llm: ModelClient,
    root: Path,
    index: int,
    catalog: TechnologyCatalog | None = None,
    site_profile: dict | None = None,
    attempt: int = 1,
) -> tuple[list[tuple[str, Finding]], bool, list[str], set[str]]:
    original = extraction_prompt(
        page.source_url,
        window.content,
        use_catalog=False,
        site_profile=site_profile,
        known_job_detail=page.job_detail.model_dump() if page.job_detail else None,
    )
    extraction_schema = Extraction
    prompt = original
    findings, attempts, issues = [], [], []
    assessed = set()
    for correction in range(llm.config.max_corrections + 1):
        try:
            reply = await llm.ask(
                prompt,
                extraction_schema.model_json_schema(),
                task=f"extract:{page.page_id}:{index}:a{attempt}",
            )
        except (ModelBudgetExceeded, ModelUnavailable) as error:
            issues.append(str(error))
            break
        issues = []
        document = reply.document
        structurally_valid = isinstance(document, dict) and set(document) == set(
            RECORD_TYPES
        )
        if not isinstance(document, dict) or set(document) != set(RECORD_TYPES):
            issues.append(reply.error or "Missing or extra objective arrays")
        if isinstance(document, dict):
            for objective, schema in RECORD_TYPES.items():
                values = document.get(objective)
                if not isinstance(values, list):
                    structurally_valid = False
                    issues.append(f"{objective}: expected array")
                    continue
                objective_valid = True
                for value in values:
                    try:
                        parsed = schema.model_validate(value).model_dump()
                    except ValidationError as error:
                        structurally_valid = False
                        objective_valid = False
                        issues.append(
                            f"{objective}: {error.errors(include_input=False, include_url=False)}"
                        )
                    else:
                        finding = source_finding(
                            objective, parsed, page=page.model_dump(), window=window
                        )
                        if finding.evidence_status == "needs_review":
                            issues.append(
                                f"{objective}: {parsed.get('technology', parsed.get('name', parsed.get('title', 'record')))}: evidence issues {finding.sources[0].issues}"
                            )
                        findings.append((objective, finding))
                if objective_valid and objective in OBJECTIVES:
                    assessed.add(objective)
            if all(
                document.get(objective) == [] for objective in RECORD_TYPES
            ) and BeautifulSoup(window.content, "html.parser").select(
                'a[href^="mailto:"]'
            ):
                issues.append(
                    "Empty extraction despite a published email link. Recheck this source and extract its supported contacts and other facts; leave uncertain attribution null."
                )
                assessed.clear()
        attempts.append(
            {
                "correction": correction,
                "issues": issues,
                "model_error": reply.error,
                "findings_so_far": len(findings),
            }
        )
        write_json(
            root / "extractions" / f"{page.page_id}-{index:03}-a{attempt}.json",
            {
                "page_id": page.page_id,
                "source_start": window.start,
                "source_end": window.end,
                "attempts": attempts,
            },
        )
        if (
            structurally_valid
            and issues
            and correction < llm.config.max_corrections
            and any(
                finding.evidence_status == "needs_review" for _, finding in findings
            )
        ):
            issues = await repair_evidence(
                findings, window, page, llm, root, f"{page.page_id}-{index}-a{attempt}"
            )
            attempts.append(
                {
                    "correction": "evidence_only",
                    "issues": issues,
                    "findings_so_far": len(findings),
                }
            )
            write_json(
                root / "extractions" / f"{page.page_id}-{index:03}-a{attempt}.json",
                {
                    "page_id": page.page_id,
                    "source_start": window.start,
                    "source_end": window.end,
                    "attempts": attempts,
                },
            )
            break
        if not issues or reply.error is not None and reply.raw is None:
            break
        prompt = (
            original
            + "\nCORRECTION: Return the complete JSON again. Fix these schema/JSON, catalog or evidence issues. Preserve correct data; copy separate exact supporting fragments including attribution. Never fabricate missing evidence:\n"
            + json.dumps(issues[:30])
        )
    issues.extend(
        await review_claims(
            [
                finding
                for objective, finding in findings
                if objective in REVIEW_OBJECTIVES
            ],
            llm,
            root,
            f"{page.page_id}-{index}-a{attempt}-source",
        )
    )
    for objective in ("company_relationships", "technology_signals"):
        corrections = await correct_reviewed_claims(
            [finding for kind, finding in findings if kind == objective],
            llm,
            root,
            f"{page.page_id}-{index}-a{attempt}-{objective}",
        )
        findings.extend((objective, finding) for finding in corrections)
    source_complete = not issues
    if catalog is not None:
        metadata_errors = await process_technology_metadata(
            [
                finding
                for objective, finding in findings
                if objective == "technology_signals"
            ],
            catalog,
            llm,
            root,
            f"{page.page_id}-{index}-a{attempt}",
        )
        issues.extend(f"technology_metadata: {error}" for error in metadata_errors)
    return findings, source_complete, issues, assessed


async def process_technology_metadata(
    records: list[Finding],
    catalog: TechnologyCatalog,
    llm: ModelClient,
    root: Path,
    task: str,
) -> list[str]:
    """Resume catalog stages using existing claims; never re-extract source HTML."""
    supported = [record for record in records if source_supported_finding(record)]
    issues = await resolve_technologies(
        [
            record
            for record in supported
            if record.data.get("catalog_match") is None
            or record.data.get("catalog_error")
        ],
        catalog,
        llm,
        root,
        task,
    )
    issues.extend(
        await review_proposals(supported, llm, root, task, catalog.category_options())
    )
    return issues


def set_job_detail_context(page: Page, queue: CrawlQueue, html: str) -> None:
    candidate = queue.candidates.get(page.requested_url)
    if (
        candidate is None
        or not candidate.job_record_ids
        or normalize_url(page.requested_url) != normalize_url(page.source_url)
        or page.fetch_status != "fetched"
    ):
        page.job_detail = None
        return
    headings = {
        h.get_text(" ", strip=True)
        for h in BeautifulSoup(html, "html.parser").find_all("h1")
    }
    headings.discard("")
    page.job_detail = (
        JobDetailContext(url=page.source_url, title=next(iter(headings)))
        if len(headings) == 1
        else None
    )


async def extract_saved_page(
    result: ResearchResult,
    page: Page,
    llm: ModelClient,
    root: Path,
    catalog: TechnologyCatalog,
) -> None:
    """Process saved native HTML, retrying only unfinished processing within run limits.

    Completed chunks and semantic rejections are not repeated. Every attempt retains
    its own artifacts; no browser or page-fetch operation is performed here.
    """
    if page.fetch_status != "fetched" or page.html_file is None:
        raise ValueError("Extraction requires a fetched page with saved HTML")
    html = (root / page.html_file).read_text(encoding="utf-8")
    if content_hash(html) != page.html_sha256:
        raise ValueError(f"Saved HTML hash mismatch: {page.page_id}")
    windows = split_html(
        html,
        max_chars=result.config.chunk_chars,
        overlap_chars=result.config.overlap_chars,
    )
    page.chunks_planned = len(windows)
    retries = result.discovery.setdefault("saved_extraction_retries", [])
    for index, window in enumerate(windows):
        history = [a for a in page.extraction_attempts if a.chunk_index == index]
        while len(history) < result.config.max_extraction_attempts:
            if history and history[-1].status != "retry_pending":
                break
            if llm.unavailable or llm.remaining <= 3:
                break
            if history:
                if len(retries) >= result.config.max_saved_extraction_retries:
                    break
                retries.append(
                    {
                        "page_id": page.page_id,
                        "chunk_index": index,
                        "attempt": len(history) + 1,
                        "html_sha256": page.html_sha256,
                        "started_at": utc_now(),
                    }
                )
                LOGGER.info(
                    "Retrying saved extraction %s chunk %s", page.page_id, index
                )
            attempt = ChunkExtractionAttempt(
                chunk_index=index,
                attempt=len(history) + 1,
                started_at=utc_now(),
                finished_at=None,
                status="running",
                call_ids=[],
                errors=[],
            )
            page.extraction_attempts.append(attempt)
            history.append(attempt)
            first_call = len(llm.calls)
            result.usage = llm.usage()
            write_json(
                root
                / "extraction-attempts"
                / f"{page.page_id}-{index:03}-a{attempt.attempt}.json",
                {"html_sha256": page.html_sha256, **attempt.model_dump()},
            )
            write_json(root / "result.json", result.model_dump())
            try:
                findings, complete, errors, assessed = await extract_window(
                    window,
                    page,
                    llm,
                    root,
                    index,
                    catalog,
                    result.site_profile.data if result.site_profile else None,
                    attempt=attempt.attempt,
                )
                for objective, finding in findings:
                    merge_finding(getattr(result.records, objective), finding)
                page.objectives_examined = [
                    o for o in OBJECTIVES if o in {*page.objectives_examined, *assessed}
                ]
                retryable = any(
                    not error.startswith("technology_metadata:")
                    and ("OpenRouter" in error or "ModelUnavailable" in error)
                    for error in errors
                ) or any(
                    finding.data.get(key, {}).get("status") == "processing_failed"
                    for _, finding in findings
                    for key in ("interpretation_review",)
                )
                attempt.status = (
                    "complete"
                    if complete
                    else "retry_pending"
                    if retryable
                    else "partial"
                )
                attempt.errors = errors
            except (ModelBudgetExceeded, ModelUnavailable) as error:
                attempt.status = "retry_pending"
                attempt.errors = [str(error)]
            except Exception as error:
                attempt.status = "failed"
                attempt.errors = [f"{type(error).__name__}: {error}"]
            attempt.finished_at = utc_now()
            attempt.call_ids = [c["call_id"] for c in llm.calls[first_call:]]
            page.errors.extend(
                error for error in attempt.errors if error not in page.errors
            )
            write_json(
                root
                / "extraction-attempts"
                / f"{page.page_id}-{index:03}-a{attempt.attempt}.json",
                {"html_sha256": page.html_sha256, **attempt.model_dump()},
            )
            write_json(root / "result.json", result.model_dump())
    latest = {attempt.chunk_index: attempt for attempt in page.extraction_attempts}
    page.chunks_completed = sum(
        attempt.status == "complete" for attempt in latest.values()
    )
    page.extraction_status = (
        "complete"
        if windows and page.chunks_completed == len(windows)
        else "partial"
        if page.objectives_examined
        else "failed"
    )


def update_statuses(
    result: ResearchResult, queue: CrawlQueue, llm: ModelClient
) -> None:
    result.discovery["external_links"] = {
        "pages_with_inventory": sum(
            page.external_link_count is not None for page in result.pages
        ),
        "fetched_pages_without_inventory": [
            page.page_id
            for page in result.pages
            if page.fetch_status in {"fetched", "duplicate"}
            and page.external_link_count is None
        ],
        "observations": len(result.external_links),
        "unique_urls": len({link.url for link in result.external_links}),
        "destination_domains": len(
            {link.destination_domain for link in result.external_links}
        ),
        "assessment_counts": {
            status: sum(
                link.assessment_status == status for link in result.external_links
            )
            for status in ("not_assessed", "assessed", "needs_review", "failed")
        },
        "note": "Observed external hyperlinks, including uncrawled destinations. Context assessments are source claims or hints, not verified corporate relationships.",
    }
    result.discovery["pending_technology_metadata"] = [
        {
            "record_id": record.record_id,
            "technology": record.data["technology"],
            "catalog_error": record.data.get("catalog_error"),
            "proposal_review": record.data.get("proposal_review"),
        }
        for record in result.records.technology_signals
        if source_supported_finding(record)
        and (record.data.get("catalog_match") is None or not accepted_finding(record))
    ]
    result.entities = summarize_entities(result.records, result.pages)
    result.technology_summary = summarize_technologies(
        result.records.technology_signals
    )
    for objective in OBJECTIVES:
        complete = sum(
            p.extraction_status == "complete" and objective in p.objectives_examined
            for p in result.pages
        )
        partial = sum(
            p.extraction_status != "complete" and objective in p.objectives_examined
            for p in result.pages
        )
        records = getattr(result.records, objective)
        supported = sum(accepted_finding(r) for r in records)
        negatives = sum(
            r.data["objective"] == objective and r.evidence_status == "source_matched"
            for r in result.records.explicit_negatives
        )
        if supported:
            status, note = (
                "found",
                "Source-matched records found; the collection may still be incomplete.",
            )
        elif records:
            status, note = (
                "needs_review",
                "Candidate records were extracted but need evidence or URL review.",
            )
        elif negatives:
            status, note = (
                "explicit_negative_found",
                "An explicit negative statement was found; consult its source and stated scope.",
            )
        elif complete or partial:
            status, note = (
                "not_found",
                "Not found in the successfully examined content. This is not proof of absence from the company or website.",
            )
        else:
            status, note = (
                "not_assessed",
                "No page produced an assessable extraction; empty results do not imply absence.",
            )
        result.objectives[objective] = ObjectiveStatus(
            status=status,
            record_count=len(records),
            source_matched_count=supported,
            needs_review_count=len(records) - supported,
            explicit_negative_count=negatives,
            pages_examined=complete + partial,
            partial_pages=partial,
            promising_urls_remaining=queue.promising_remaining(objective),
            note=note,
        )
    result.usage = llm.usage()
    pending_extractions = []
    for page in result.pages:
        latest = {a.chunk_index: a for a in page.extraction_attempts}
        for index in range(page.chunks_planned):
            attempt = latest.get(index)
            if attempt is not None and attempt.status not in {
                "retry_pending",
                "running",
            }:
                continue
            pending_extractions.append(
                {
                    "page_id": page.page_id,
                    "chunk_index": index,
                    "attempts": attempt.attempt if attempt is not None else 0,
                    "status": attempt.status
                    if attempt is not None
                    else "not_attempted",
                }
            )
    result.discovery["pending_extractions"] = pending_extractions
    queue.coverage = {
        objective: status.model_dump()
        for objective, status in result.objectives.items()
    }
    snapshot = queue.snapshot()
    result.discovery.update({k: v for k, v in snapshot.items() if k != "candidates"})


async def research_company(
    url: str,
    *,
    api_key: str | None = None,
    api: Literal["openrouter", "deepseek"] = "openrouter",
    output_dir: Path | None = None,
    config: ResearchConfig | None = None,
    technology_catalog: TechnologyCatalog,
) -> ResearchResult:
    """Research a company using built-in objectives. API key defaults to the environment.

    The output directory must be empty/new. Partial results and every attempted
    model request are saved, including when a budget or external failure stops work.
    """
    settings = (
        config
        if config is not None
        else (
            ResearchConfig(model="deepseek-flash", provider=None)
            if api == "deepseek"
            else ResearchConfig()
        )
    )
    site_url = normalize_url(url)
    key_name = "DEEPSEEK" if api == "deepseek" else "OPENROUTER_API_KEY"
    key = api_key or os.environ.get(key_name)
    if not key:
        raise ValueError(f"Set {key_name} or pass api_key")
    root = (
        output_dir
        if output_dir is not None
        else Path("company-research-output") / uuid4().hex
    ).resolve()
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"Output directory is not empty: {root}")
    root.mkdir(parents=True, exist_ok=True)
    result = ResearchResult(
        schema_version="1.11",
        run_id=uuid4().hex,
        technology_catalog={
            "version": technology_catalog.snapshot.version,
            "synced_at": technology_catalog.snapshot.synced_at,
            "entries": len(technology_catalog.entries),
        },
        input_url=url,
        site_url=site_url,
        started_at=utc_now(),
        finished_at=None,
        status="running",
        stop_reason=None,
        config=settings,
        objectives={},
        records=Findings(**{objective: [] for objective in RECORD_TYPES}),
        technology_summary=[],
        site_profile=None,
        company_overview=None,
        pages=[],
        discovery={"model_api": api},
        usage={},
        errors=[],
        output_directory=str(root),
    )
    write_json(
        root / "technology-catalog.json", technology_catalog.snapshot.model_dump()
    )
    queue = CrawlQueue(site_url, settings)
    queue.add(site_url, source=site_url, label="Input website")
    write_json(
        root / "settings.json",
        {
            "url": site_url,
            "model_api": api,
            "config": settings.model_dump(),
            "objectives": OBJECTIVES,
            "extraction_schema": Extraction.model_json_schema(),
        },
    )
    async with (
        httpx.AsyncClient(
            base_url="https://api.deepseek.com/"
            if api == "deepseek"
            else "https://openrouter.ai/api/v1/"
        ) as model_http,
        httpx.AsyncClient() as web_http,
    ):
        llm = ModelClient(model_http, key, settings, root, api=api)
        try:
            async with AsyncExitStack() as browser_stack:
                crawler = await browser_stack.enter_async_context(open_browser())
                recoveries = result.discovery.setdefault("browser_recoveries", [])
                next_url, focus = site_url, "input_url"
                while len(result.pages) < settings.max_pages:
                    if llm.remaining <= (0 if not result.pages else 3):
                        result.stop_reason = "model_call_budget"
                        break
                    queue.visited.add(next_url)
                    page = Page(
                        page_id=f"p{len(result.pages) + 1:04}",
                        requested_url=next_url,
                        source_url=next_url,
                        selected_for=focus,
                        fetched_at=utc_now(),
                        status_code=None,
                        fetch_status="pending",
                        extraction_status="not_assessed",
                        attempts=0,
                        chunks_planned=0,
                        chunks_completed=0,
                        objectives_examined=[],
                        html_sha256=None,
                        html_file=None,
                        errors=[],
                    )
                    result.pages.append(page)
                    LOGGER.info("Fetching %s (%s)", next_url, focus)
                    while True:
                        try:
                            html, links = await fetch_page(
                                crawler, page, settings, root
                            )
                            break
                        except BrowserUnavailable:
                            if len(recoveries) >= settings.max_browser_restarts:
                                raise
                            recovery = {
                                "page_id": page.page_id,
                                "failed_attempt": page.attempts,
                                "started_at": utc_now(),
                                "status": "restarting",
                            }
                            recoveries.append(recovery)
                            LOGGER.warning(
                                "Restarting browser for %s", page.requested_url
                            )
                            try:
                                await browser_stack.aclose()
                            except Exception as error:
                                recovery["cleanup_error"] = str(error)[:500]
                            try:
                                crawler = await browser_stack.enter_async_context(
                                    open_browser()
                                )
                            except Exception as error:
                                recovery["status"] = "failed"
                                raise BrowserUnavailable(
                                    "Browser restart failed"
                                ) from error
                            recovery["status"] = "restarted"
                            write_json(root / "browser-recoveries.json", recoveries)
                    if page.fetch_status == "fetched":
                        inventory = root / "external-links" / f"{page.page_id}.json"
                        if inventory.is_file():
                            result.external_links.extend(
                                ExternalLink.model_validate(value)
                                for value in json.loads(
                                    inventory.read_text(encoding="utf-8")
                                )
                            )
                        # Persist before extraction/ranking can fail or exhaust budget.
                        write_json(
                            root / "external-links.json",
                            [link.model_dump() for link in result.external_links],
                        )
                        write_json(root / "result.json", result.model_dump())
                        final_url = normalize_url(page.source_url)
                        duplicate = any(
                            p.page_id != page.page_id
                            and p.fetch_status == "fetched"
                            and normalize_url(p.source_url) == final_url
                            for p in result.pages
                        )
                        queue.visited.add(final_url)
                        if len(result.pages) == 1:
                            queue.site_domain = queue.domain(final_url)
                            queue.site_url = final_url
                            result.site_url = final_url
                            for candidate in queue.candidates.values():
                                candidate.external = (
                                    queue.domain(candidate.url) != queue.site_domain
                                )
                            LOGGER.info(
                                "Checking company-site eligibility from %s",
                                page.source_url,
                            )
                            result.site_profile = await classify_site(
                                HtmlWindow(0, len(html), html), page, llm, root
                            )
                            result.site_description = result.site_profile.data[
                                "site_description"
                            ]
                            decision = (
                                result.site_profile.data["crawl_decision"]
                                if result.site_profile.evidence_status
                                == "source_matched"
                                else "needs_review"
                            )
                            result.discovery["site_gate"] = {
                                "decision": decision,
                                "page_id": page.page_id,
                                "source_url": page.source_url,
                                "scope": "first_page_only",
                                "evidence_status": result.site_profile.evidence_status,
                            }
                            write_json(
                                root / "research-profile.json",
                                {
                                    "site_profile": result.site_profile.model_dump(),
                                    "objective_order": profile_objectives(
                                        result.site_profile
                                    )
                                    if decision == "continue_crawling"
                                    else [],
                                },
                            )
                            if decision != "continue_crawling":
                                result.status = decision
                                result.stop_reason = (
                                    "not_company_website"
                                    if decision == "skip_crawling"
                                    else "site_eligibility_uncertain"
                                )
                                break
                            queue.objective_order = profile_objectives(
                                result.site_profile
                            )
                            queue.site_profile = result.site_profile.data
                            inventory = await sitemap_urls(
                                web_http, result.site_url, settings
                            )
                            write_json(root / "sitemaps.json", inventory)
                            result.discovery["sitemap"] = {
                                k: v
                                for k, v in inventory.items()
                                if k not in {"urls", "url_sources"}
                            }
                            result.discovery["sitemap"]["url_count"] = len(
                                inventory["urls"]
                            )
                            for candidate_url in inventory["urls"]:
                                if queue.domain(candidate_url) == queue.site_domain:
                                    queue.add(
                                        candidate_url,
                                        source=inventory["url_sources"][candidate_url],
                                    )
                        for link in links:
                            if isinstance(link.get("href"), str):
                                queue.add(
                                    link["href"],
                                    source=page.source_url,
                                    title=link.get("title"),
                                    label=link.get("text"),
                                    context=link.get("context"),
                                )
                        if duplicate:
                            page.fetch_status = "duplicate"
                        else:
                            windows = split_html(
                                html,
                                max_chars=settings.chunk_chars,
                                overlap_chars=settings.overlap_chars,
                            )
                            page.chunks_planned = len(windows)
                            set_job_detail_context(page, queue, html)
                            await extract_saved_page(
                                result, page, llm, root, technology_catalog
                            )
                    if len(result.pages) == 1 and page.fetch_status != "fetched":
                        result.status = "needs_review"
                        result.stop_reason = "initial_page_unavailable"
                        result.site_description = "The first page could not be retrieved, so the site's purpose and company eligibility could not be determined."
                        break
                    queue.observe_findings(
                        page, result.records.jobs, result.records.company_profile
                    )
                    queue.observe_page_yield(page, result.records)
                    if queue.permits_external_navigation(page.requested_url):
                        for link in links:
                            if isinstance(link.get("href"), str):
                                queue.add(
                                    link["href"],
                                    source=page.requested_url,
                                    title=link.get("title"),
                                    label=link.get("text"),
                                    context=link.get("context"),
                                )
                    update_statuses(result, queue, llm)
                    write_json(root / "result.json", result.model_dump())
                    write_json(root / "queue.json", queue.snapshot())
                    if llm.remaining <= 0 and page.extraction_status != "complete":
                        raise ModelBudgetExceeded(
                            "Model request budget reached during extraction"
                        )
                    if len(result.pages) >= settings.max_pages:
                        result.stop_reason = "page_budget"
                        break
                    if llm.unavailable:
                        raise ModelUnavailable(
                            f"{llm.api_name} unavailable after repeated or permanent errors"
                        )
                    result.errors.extend(await assess_links(queue, llm, root))
                    counts = {
                        o: sum(accepted_finding(r) for r in getattr(result.records, o))
                        for o in OBJECTIVES
                    }
                    selected = queue.pick(counts)
                    while (
                        selected is None
                        and queue.assessment_batch()
                        and llm.remaining > 3
                    ):
                        result.errors.extend(await assess_links(queue, llm, root))
                        selected = queue.pick(counts)
                    if selected is None:
                        if queue.assessment_batch() and llm.remaining <= 1:
                            raise ModelBudgetExceeded(
                                "Insufficient model budget to assess links and extract another page"
                            )
                        result.stop_reason = "no_promising_candidates"
                        break
                    candidate, focus = selected
                    next_url = candidate.url
        except BrowserUnavailable as error:
            result.stop_reason = "browser_unavailable"
            result.errors.append({"stage": "browser", "error": str(error)})
        except ModelBudgetExceeded as error:
            result.stop_reason = "model_call_budget"
            result.errors.append({"stage": "model", "error": str(error)})
        except ModelUnavailable as error:
            result.stop_reason = "model_unavailable"
            result.errors.append({"stage": "model", "error": str(error)})
        except (
            Exception
        ) as error:  # Preserve a usable JSON result at the public run boundary.
            result.stop_reason = "run_error"
            result.errors.append(
                {
                    "stage": "run",
                    "error": f"{type(error).__name__}: {str(error).replace(key, '[REDACTED]')[:1000]}",
                }
            )
            LOGGER.error("Research stopped: %s", result.errors[-1]["error"])
        if result.discovery.get("site_gate", {}).get("decision") != "continue_crawling":
            if result.status != "skip_crawling":
                result.status = "needs_review"
                result.stop_reason = result.stop_reason or "site_eligibility_uncertain"
                result.site_description = (
                    result.site_description
                    or "The site's company eligibility could not be determined from the first page. Further crawling was not started."
                )
            result.discovery.setdefault(
                "site_gate", {"decision": "needs_review", "scope": "first_page_only"}
            )
            result.discovery["sitemap"] = {
                "status": "not_requested",
                "url_count": 0,
                "reason": "site_not_admitted",
            }
            result.finished_at = utc_now()
            update_statuses(result, queue, llm)
            write_json(root / "result.json", result.model_dump())
            write_json(root / "queue.json", queue.snapshot())
            return result
        update_statuses(result, queue, llm)
        LOGGER.info("Consolidating site and company overview")
        try:
            result.company_overview = await summarize_company(result, llm, root)
        except (ValueError, ModelBudgetExceeded, ModelUnavailable) as error:
            result.errors.append(
                {"stage": "company_summary", "error": str(error)[:1000]}
            )
        await assess_external_links(result.external_links, llm, root)
        result.finished_at = utc_now()
        update_statuses(result, queue, llm)
        any_examined = any(
            p.extraction_status in {"complete", "partial"} for p in result.pages
        )
        incomplete = (
            result.stop_reason in {"page_budget", "model_call_budget"}
            or bool(result.errors)
            or any(
                p.fetch_status == "failed"
                or p.extraction_status in {"partial", "failed"}
                for p in result.pages
            )
        )
        result.status = (
            "failed" if not any_examined else "partial" if incomplete else "finished"
        )
        write_json(root / "result.json", result.model_dump())
        write_json(root / "queue.json", queue.snapshot())
    return result
