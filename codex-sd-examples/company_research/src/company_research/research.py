"""One URL in, attributed company findings and explicit coverage statuses out."""

import asyncio
import json
import logging
import os
from pathlib import Path
from uuid import uuid4

import httpx
from bs4 import BeautifulSoup
from pydantic import ValidationError

from company_research.analytics import summarize_technologies
from company_research.content import (
    HtmlWindow,
    merge_finding,
    source_finding,
    split_html,
)
from company_research.discovery import CrawlQueue, normalize_url, sitemap_urls
from company_research.fetch import fetch_page, open_browser
from company_research.llm import ModelBudgetExceeded, ModelUnavailable, OpenRouter
from company_research.models import (
    OBJECTIVES,
    RECORD_TYPES,
    CandidateAssessment,
    Extraction,
    Finding,
    Findings,
    ObjectiveStatus,
    Page,
    ResearchConfig,
    ResearchResult,
    Selection,
)
from company_research.profiles import (
    classify_site,
    profile_objectives,
    summarize_company,
)
from company_research.prompts import extraction_prompt, selection_prompt
from company_research.resolution import resolve_technologies
from company_research.review import (
    correct_reviewed_claims,
    repair_evidence,
    review_claims,
)
from company_research.storage import utc_now, write_json
from company_research.technology_catalog import (
    TechnologyCatalog,
)

LOGGER = logging.getLogger(__name__)


async def assess_links(queue: CrawlQueue, llm: OpenRouter, root: Path) -> list[dict]:
    errors = []
    for _ in range(queue.config.selection_batches_per_page):
        # Preserve capacity to extract the next page instead of consuming it all on ranking.
        if llm.remaining <= 3:
            return errors
        batch = queue.assessment_batch()
        if not batch:
            return errors
        by_id = {candidate.candidate_id: candidate for candidate in batch}
        original = selection_prompt(
            queue.site_url,
            [c.prompt_data() for c in batch],
            site_profile=queue.site_profile,
            coverage=queue.coverage,
        )
        prompt = original
        for correction in range(queue.config.max_corrections + 1):
            if llm.remaining <= 3:
                break
            reply = await llm.ask(
                prompt, Selection.model_json_schema(), task="link_assessment"
            )
            issues = []
            values = (
                reply.document.get("assessments")
                if isinstance(reply.document, dict)
                else None
            )
            if not isinstance(values, list):
                issues.append(reply.error or "Missing assessments array")
            else:
                seen = set()
                for value in values:
                    try:
                        assessment = CandidateAssessment.model_validate(value)
                    except ValidationError:
                        issues.append("Invalid candidate assessment schema")
                        continue
                    if assessment.candidate_id not in by_id:
                        issues.append("Unknown candidate ID")
                        continue
                    if assessment.candidate_id in seen:
                        issues.append("Duplicate candidate ID")
                        continue
                    seen.add(assessment.candidate_id)
                    if any(
                        p.potential in {"high", "medium"} and p.role == "none"
                        for p in (getattr(assessment.objectives, o) for o in OBJECTIVES)
                    ):
                        issues.append(
                            "Useful potential needs a direct or navigation role"
                        )
                        continue
                    by_id[assessment.candidate_id].assessment = assessment
            missing = [
                cid for cid, candidate in by_id.items() if candidate.assessment is None
            ]
            if missing:
                issues.append("Missing valid assessments: " + ", ".join(missing))
            write_json(
                root
                / "assessments"
                / f"{batch[0].candidate_id}-{batch[0].assessment_attempts}-{correction}.json",
                {
                    "candidate_ids": list(by_id),
                    "issues": issues,
                    "model_error": reply.error,
                },
            )
            if not issues or reply.error is not None and reply.raw is None:
                break
            prompt = (
                original
                + "\nReturn the complete JSON again. Correct these issues:\n"
                + json.dumps(issues)
            )
        # Unassessed candidates retain uncertainty and remain eligible for bounded exploration.
        for candidate in batch:
            candidate.assessment_attempts += 1
            candidate.assessed = (
                candidate.assessment is not None
                or candidate.assessment_attempts >= queue.config.max_assessment_attempts
            )
        if issues:
            errors.append(
                {
                    "stage": "link_assessment",
                    "candidate_ids": list(by_id),
                    "issues": issues,
                }
            )
    return errors


async def extract_window(
    window: HtmlWindow,
    page: Page,
    llm: OpenRouter,
    root: Path,
    index: int,
    catalog: TechnologyCatalog | None = None,
    site_profile: dict | None = None,
) -> tuple[list[tuple[str, Finding]], bool, list[str], set[str]]:
    original = extraction_prompt(
        page.source_url,
        window.content,
        use_catalog=False,
        site_profile=site_profile,
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
                task=f"extract:{page.page_id}:{index}",
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
            root / "extractions" / f"{page.page_id}-{index:03}.json",
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
                findings, window, page, llm, root, f"{page.page_id}-{index}"
            )
            attempts.append(
                {
                    "correction": "evidence_only",
                    "issues": issues,
                    "findings_so_far": len(findings),
                }
            )
            write_json(
                root / "extractions" / f"{page.page_id}-{index:03}.json",
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
    if catalog is not None:
        resolution_errors = await resolve_technologies(
            [
                finding
                for objective, finding in findings
                if objective == "technology_signals"
            ],
            catalog,
            llm,
            root,
            f"{page.page_id}-{index}",
        )
        issues.extend(resolution_errors)
        if resolution_errors:
            assessed.discard("technology_signals")
    return findings, not issues, issues, assessed


def update_statuses(result: ResearchResult, queue: CrawlQueue, llm: OpenRouter) -> None:
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
        supported = sum(r.evidence_status == "source_matched" for r in records)
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
    output_dir: Path | None = None,
    config: ResearchConfig | None = None,
    technology_catalog: TechnologyCatalog,
) -> ResearchResult:
    """Research a company using built-in objectives. API key defaults to the environment.

    The output directory must be empty/new. Partial results and every attempted
    model request are saved, including when a budget or external failure stops work.
    """
    settings = config if config is not None else ResearchConfig()
    site_url = normalize_url(url)
    key = api_key or os.environ.get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("Set OPENROUTER_API_KEY or pass api_key")
    root = (
        output_dir
        if output_dir is not None
        else Path("company-research-output") / uuid4().hex
    ).resolve()
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"Output directory is not empty: {root}")
    root.mkdir(parents=True, exist_ok=True)
    result = ResearchResult(
        schema_version="1.4",
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
        discovery={},
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
            "config": settings.model_dump(),
            "objectives": OBJECTIVES,
            "extraction_schema": Extraction.model_json_schema(),
        },
    )
    async with (
        httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as model_http,
        httpx.AsyncClient() as web_http,
    ):
        llm = OpenRouter(model_http, key, settings, root)
        try:
            inventory = await sitemap_urls(web_http, site_url, settings)
            write_json(root / "sitemaps.json", inventory)
            result.discovery["sitemap"] = {
                k: v for k, v in inventory.items() if k not in {"urls", "url_sources"}
            }
            result.discovery["sitemap"]["url_count"] = len(inventory["urls"])
            for candidate_url in inventory["urls"]:
                if queue.domain(candidate_url) == queue.site_domain:
                    queue.add(
                        candidate_url, source=inventory["url_sources"][candidate_url]
                    )
            async with open_browser() as crawler:
                next_url, focus = site_url, "input_url"
                classification_attempts = 0
                while len(result.pages) < settings.max_pages:
                    if llm.remaining <= 3:
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
                    html, links = await fetch_page(crawler, page, settings, root)
                    if page.fetch_status == "fetched":
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
                        for link in links:
                            if isinstance(link.get("href"), str):
                                queue.add(
                                    link["href"],
                                    source=page.source_url,
                                    title=link.get("title"),
                                    label=link.get("text"),
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
                            if (
                                windows
                                and classification_attempts < 2
                                and (
                                    result.site_profile is None
                                    or result.site_profile.evidence_status
                                    != "source_matched"
                                )
                            ):
                                classification_attempts += 1
                                LOGGER.info("Classifying site from %s", page.source_url)
                                try:
                                    result.site_profile = await classify_site(
                                        windows[0],
                                        page,
                                        list(queue.candidates),
                                        llm,
                                        root,
                                    )
                                except ValueError as error:
                                    result.errors.append(
                                        {
                                            "stage": "site_classification",
                                            "page_id": page.page_id,
                                            "error": str(error)[:1000],
                                        }
                                    )
                                else:
                                    queue.objective_order = profile_objectives(
                                        result.site_profile
                                    )
                                    if (
                                        result.site_profile.evidence_status
                                        == "source_matched"
                                    ):
                                        queue.site_profile = result.site_profile.data
                                    write_json(
                                        root / "research-profile.json",
                                        {
                                            "site_profile": result.site_profile.model_dump(),
                                            "objective_order": queue.objective_order,
                                        },
                                    )
                            outcomes = await asyncio.gather(
                                *(
                                    extract_window(
                                        window,
                                        page,
                                        llm,
                                        root,
                                        i,
                                        technology_catalog,
                                        queue.site_profile,
                                    )
                                    for i, window in enumerate(windows)
                                ),
                                return_exceptions=True,
                            )
                            for outcome in outcomes:
                                if isinstance(outcome, BaseException):
                                    page.errors.append(
                                        f"{type(outcome).__name__}: {outcome}"
                                    )
                                else:
                                    findings, complete, errors, assessed = outcome
                                    page.chunks_completed += complete
                                    page.errors.extend(errors)
                                    page.objectives_examined = [
                                        o
                                        for o in OBJECTIVES
                                        if o in {*page.objectives_examined, *assessed}
                                    ]
                                    for objective, finding in findings:
                                        merge_finding(
                                            getattr(result.records, objective), finding
                                        )
                            page.extraction_status = (
                                "complete"
                                if page.chunks_completed == page.chunks_planned
                                else "partial"
                                if page.objectives_examined
                                or any(
                                    isinstance(o, tuple) and bool(o[0])
                                    for o in outcomes
                                )
                                else "failed"
                            )
                            claim_issues = await review_claims(
                                [
                                    finding
                                    for objective in (
                                        "company_relationships",
                                        "technology_signals",
                                    )
                                    for finding in getattr(result.records, objective)
                                    if any(
                                        source.page_id == page.page_id
                                        for source in finding.sources
                                    )
                                    and "interpretation_review" not in finding.data
                                ],
                                llm,
                                root,
                                page.page_id,
                            )
                            for objective in (
                                "company_relationships",
                                "technology_signals",
                            ):
                                corrections = await correct_reviewed_claims(
                                    getattr(result.records, objective),
                                    llm,
                                    root,
                                    f"{page.page_id}-{objective}",
                                )
                                for correction in corrections:
                                    merge_finding(
                                        getattr(result.records, objective), correction
                                    )
                            if claim_issues:
                                page.errors.extend(claim_issues)
                                page.extraction_status = "partial"
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
                            "OpenRouter unavailable after repeated or permanent errors"
                        )
                    result.errors.extend(await assess_links(queue, llm, root))
                    counts = {
                        o: sum(
                            r.evidence_status == "source_matched"
                            for r in getattr(result.records, o)
                        )
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
        update_statuses(result, queue, llm)
        LOGGER.info("Consolidating site and company overview")
        try:
            result.company_overview = await summarize_company(result, llm, root)
        except (ValueError, ModelBudgetExceeded, ModelUnavailable) as error:
            result.errors.append(
                {"stage": "company_summary", "error": str(error)[:1000]}
            )
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
