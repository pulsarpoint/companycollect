"""Finish a saved benchmark queue in a new directory, preserving all original artifacts."""

import argparse
import asyncio
import json
import re
import shutil
from collections import Counter
from dataclasses import fields
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from dotenv import dotenv_values

from company_research.discovery import Candidate, CrawlQueue, normalize_url
from company_research.fetch import fetch_page, open_browser
from company_research.llm import ModelClient
from company_research.models import (
    OBJECTIVES,
    CandidateAssessment,
    ChunkExtractionAttempt,
    Page,
    ResearchResult,
)
from company_research.profiles import profile_objectives, summarize_company
from company_research.research import (
    assess_links,
    extract_saved_page,
    set_job_detail_context,
    update_statuses,
)
from company_research.storage import utc_now, write_json
from company_research.technology_catalog import TechnologyCatalog


def restore_queue(result: ResearchResult, snapshot: dict) -> CrawlQueue:
    queue = CrawlQueue(result.site_url, result.config)
    for value in snapshot["candidates"]:
        data = {
            field.name: value[field.name]
            for field in fields(Candidate)
            if field.name in value
        }
        if data.get("assessment") is not None:
            data["assessment"] = CandidateAssessment.model_validate(data["assessment"])
        queue.candidates[value["url"]] = Candidate(**data)
    queue.visited = {
        normalize_url(url)
        for page in result.pages
        for url in (page.requested_url, page.source_url)
    }
    queue.site_profile = (
        result.site_profile.data
        if result.site_profile
        and result.site_profile.evidence_status == "source_matched"
        else None
    )
    queue.objective_order = profile_objectives(result.site_profile)
    queue.target_names = set(snapshot.get("target_names", []))
    queue.coverage = {
        objective: status.model_dump()
        for objective, status in result.objectives.items()
    }
    queue.excluded = Counter(snapshot["excluded"])
    queue.document_candidates = {
        value["url"]: value for value in snapshot["document_candidates"]
    }
    queue.job_detail_attempts = set(snapshot["job_coverage"]["detail_attempts"])
    queue.job_details_fetched = set(
        snapshot["job_coverage"]["details_fetched_with_target_jobs"]
    )
    queue.engineering_attempts = set(
        snapshot.get("engineering_coverage", {}).get("detail_attempts", [])
    )
    queue.engineering_attempts.update(
        p.requested_url
        for p in result.pages
        if p.selected_for == "engineering_followup"
    )
    queue.job_detail_attempts.update(
        p.requested_url for p in result.pages if p.selected_for == "job_detail_followup"
    )
    queue.page_yields = snapshot.get("page_yields", {})
    queue.observed_record_ids = {
        finding.record_id
        for _, records in result.records
        for finding in records
        if finding.evidence_status == "source_matched"
    }
    for page in result.pages:
        focus = page.selected_for
        queue.external_pages += queue.domain(page.requested_url) != queue.site_domain
        if focus.startswith("exploration"):
            queue.explored += 1
            focus = focus.partition(":")[2]
        if focus == "job_detail_followup":
            focus = "jobs"
        if focus == "engineering_followup":
            focus = "technology_signals"
        if focus in OBJECTIVES:
            queue.focus_visits[focus] += 1
            candidate = queue.candidates.get(page.requested_url)
            if (
                candidate
                and candidate.assessment
                and getattr(candidate.assessment.objectives, focus).role == "navigation"
            ):
                queue.navigation_visits[focus] += 1
    if any(candidate.job_record_ids for candidate in queue.candidates.values()):
        queue.navigation_visits["jobs"] = max(1, queue.navigation_visits["jobs"])
    return queue


async def resume(args):
    if args.output.exists():
        raise ValueError("Recovery output must be a new directory")
    result = ResearchResult.model_validate_json(
        (args.snapshot / "result.json").read_bytes()
    )
    if result.stop_reason != "model_unavailable" and not (
        args.recover_saved and (args.snapshot / "interruption.json").exists()
    ):
        raise ValueError("This harness only resumes a provider-interrupted benchmark")
    key = dotenv_values(args.env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    shutil.copytree(args.snapshot, args.output)
    root = args.output.resolve()
    # Use the package's regular HTTP retry policy during recovery; model and provider stay fixed.
    result.config.max_http_attempts = 2
    if args.provider is not None:
        result.config.provider = args.provider
    if args.model_timeout is not None:
        result.config.model_timeout_seconds = args.model_timeout
    if args.recover_saved:
        result.config.max_extraction_attempts = args.max_extraction_attempts or 3
    if args.saved_retries is not None:
        result.config.max_saved_extraction_retries = args.saved_retries
    result.output_directory = str(root)
    result.status, result.stop_reason, result.finished_at = "running", None, None
    recovery = {
        "resumed_from": str(args.snapshot.resolve()),
        "started_at": utc_now(),
        "initial_page_count": len(result.pages),
        "initial_model_calls": result.usage["calls"],
        "provider": result.config.provider,
        "max_http_attempts": result.config.max_http_attempts,
        "model_timeout_seconds": result.config.model_timeout_seconds,
        "recover_saved_before_fetching": args.recover_saved,
        "max_extraction_attempts": result.config.max_extraction_attempts,
        "max_saved_extraction_retries": result.config.max_saved_extraction_retries,
        "new_page_seeds": [],
        "queue_restoration": "saved assessments, sources, observed job links and visit counters",
        "status": "running",
    }
    write_json(root / "recovery.json", recovery)
    queue = restore_queue(result, json.loads((root / "queue.json").read_text()))
    catalog = TechnologyCatalog.read(root / "technology-catalog.json")
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = ModelClient(client, key, result.config, root)
        for path in sorted((root / "calls").glob("*.json")):
            call = json.loads(path.read_text(encoding="utf-8"))
            if not call.get("usage") and not call.get("error"):
                call["error"] = "Interrupted before response; cost unknown"
                write_json(path, call)
                match = re.match(r"extract:(p\d+):(\d+):a(\d+)$", call["task"])
                if match:
                    page_id, index, number = match.groups()
                    source_page = next(p for p in result.pages if p.page_id == page_id)
                    if not any(
                        a.chunk_index == int(index) and a.attempt == int(number)
                        for a in source_page.extraction_attempts
                    ):
                        source_page.extraction_attempts.append(
                            ChunkExtractionAttempt(
                                chunk_index=int(index),
                                attempt=int(number),
                                started_at=call["started_at"],
                                finished_at=None,
                                status="retry_pending",
                                call_ids=[call["call_id"]],
                                errors=[call["error"]],
                            )
                        )
            llm.calls.append(
                {k: v for k, v in call.items() if k not in {"request", "response"}}
            )
        recovery["initial_model_calls"] = len(llm.calls)
        for page in result.pages:
            for attempt in page.extraction_attempts:
                if attempt.status == "running":
                    attempt.status = "retry_pending"
                    attempt.errors.append("Interrupted during processing")
        try:
            if args.recover_saved:
                for source_page in result.pages:
                    if (
                        source_page.fetch_status != "fetched"
                        or source_page.html_file is None
                    ):
                        continue
                    html = (root / source_page.html_file).read_text(encoding="utf-8")
                    for anchor in BeautifulSoup(html, "html.parser").find_all(
                        "a", href=True
                    ):
                        queue.add(
                            str(anchor["href"]),
                            source=source_page.source_url,
                            label=anchor.get_text(" ", strip=True),
                        )
                    set_job_detail_context(source_page, queue, html)
                    print(
                        f"Recovering saved page {source_page.page_id}: {source_page.source_url}",
                        flush=True,
                    )
                    await extract_saved_page(result, source_page, llm, root, catalog)
                    queue.observe_findings(
                        source_page, result.records.jobs, result.records.company_profile
                    )
                    queue.observe_page_yield(source_page, result.records)
                    update_statuses(result, queue, llm)
                    write_json(root / "result.json", result.model_dump())
                    write_json(root / "queue.json", queue.snapshot())
            async with open_browser() as crawler:
                while len(result.pages) < result.config.max_pages and llm.remaining > 3:
                    result.errors.extend(await assess_links(queue, llm, root))
                    selected = queue.pick(
                        {
                            objective: result.objectives[objective].source_matched_count
                            for objective in OBJECTIVES
                        }
                    )
                    if selected is None:
                        result.stop_reason = "no_promising_candidates"
                        break
                    candidate, focus = selected
                    page = Page(
                        page_id=f"p{len(result.pages) + 1:04}",
                        requested_url=candidate.url,
                        source_url=candidate.url,
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
                    queue.visited.add(candidate.url)
                    print(
                        f"Recovery fetching {page.page_id}: {page.source_url} ({focus})",
                        flush=True,
                    )
                    html, links = await fetch_page(crawler, page, result.config, root)
                    if page.fetch_status == "fetched":
                        queue.visited.add(normalize_url(page.source_url))
                        for link in links:
                            if isinstance(link.get("href"), str):
                                queue.add(
                                    link["href"],
                                    source=page.source_url,
                                    title=link.get("title"),
                                    label=link.get("text"),
                                )
                        if any(
                            previous.source_url == page.source_url
                            and previous.fetch_status == "fetched"
                            for previous in result.pages[:-1]
                        ):
                            page.fetch_status = "duplicate"
                        else:
                            set_job_detail_context(page, queue, html)
                            await extract_saved_page(result, page, llm, root, catalog)
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
                                )
                    update_statuses(result, queue, llm)
                    write_json(root / "result.json", result.model_dump())
                    write_json(root / "queue.json", queue.snapshot())
                if result.stop_reason is None:
                    result.stop_reason = (
                        "page_budget"
                        if len(result.pages) >= result.config.max_pages
                        else "model_call_budget"
                    )
            result.company_overview = await summarize_company(result, llm, root)
        except Exception as error:
            result.stop_reason = "recovery_error"
            result.errors.append(
                {
                    "stage": "checkpoint_recovery",
                    "error": f"{type(error).__name__}: {str(error).replace(key, '[REDACTED]')}",
                }
            )
        result.status, result.finished_at = "partial", utc_now()
        update_statuses(result, queue, llm)
        recovery.update(
            status=result.stop_reason,
            finished_at=result.finished_at,
            final_page_count=len(result.pages),
            final_model_calls=result.usage["calls"],
        )
        write_json(root / "recovery.json", recovery)
        write_json(root / "result.json", result.model_dump())
        write_json(root / "queue.json", queue.snapshot())
        print(json.dumps(recovery), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--model-timeout", type=float)
    parser.add_argument("--recover-saved", action="store_true")
    parser.add_argument("--provider")
    parser.add_argument("--max-extraction-attempts", type=int, choices=range(1, 6))
    parser.add_argument("--saved-retries", type=int)
    asyncio.run(resume(parser.parse_args()))
