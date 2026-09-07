"""Revalidate and repair a saved crawl without refetching or changing source HTML."""

import argparse
import asyncio
import json
import os
import shutil
from pathlib import Path

import httpx
from dotenv import dotenv_values
from pydantic import ValidationError

from company_research.analytics import summarize_technologies
from company_research.content import HtmlWindow, source_finding
from company_research.llm import ModelBudgetExceeded, ModelUnavailable, OpenRouter
from company_research.models import RECORD_TYPES, ResearchConfig, ResearchResult
from company_research.profiles import summarize_company
from company_research.review import repair_evidence, review_claims
from company_research.storage import content_hash, utc_now, write_json


async def run(args):
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    snapshot = args.snapshot.resolve()
    original_path = snapshot / "result.json"
    original = json.loads(original_path.read_text())
    result = ResearchResult.model_validate(original)
    key = os.environ.get("OPENROUTER_API_KEY") or dotenv_values(args.env_file).get(
        "OPENROUTER_API_KEY"
    )
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    config = ResearchConfig(
        provider=args.provider,
        reasoning_effort="none",
        max_model_calls=35,
        max_http_attempts=1,
        model_timeout_seconds=90,
    )
    pages = {p.page_id: p for p in result.pages}
    html_by_page = {}
    for page in result.pages:
        if page.html_file is None:
            continue
        html = (snapshot / page.html_file).read_text()
        if content_hash(html) != page.html_sha256:
            raise ValueError(f"HTML hash mismatch: {page.page_id}")
        html_by_page[page.page_id] = html
        target = root / page.html_file
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(snapshot / page.html_file, target)
    audit = {
        "stage": "saved_source_revalidation_and_evidence_repair",
        "started_at": utc_now(),
        "original_run": str(snapshot),
        "original_result_sha256": content_hash(original_path.read_text()),
        "config": config.model_dump(),
        "html_modified": False,
        "refetched": False,
        "historical_page_errors_retained": True,
        "before": {
            o: sum(r.evidence_status == "source_matched" for r in rows)
            for o, rows in result.records
        },
        "schema_rejections": [],
        "repair_outcomes": [],
    }
    invalid_ids = set()
    for objective, rows in result.records:
        schema = RECORD_TYPES[objective]
        for record in rows:
            record.data.pop("interpretation_review", None)
            sources = []
            for source in record.sources:
                page = pages[source.page_id]
                html = html_by_page[source.page_id]
                try:
                    parsed = schema.model_validate(
                        {
                            k: record.data.get(k)
                            for k in schema.model_fields
                            if k != "evidence"
                        }
                        | {"evidence": [f.text for f in source.evidence]}
                    )
                except ValidationError as error:
                    invalid_ids.add(record.record_id)
                    record.evidence_status = "needs_review"
                    audit["schema_rejections"].append(
                        {
                            "record_id": record.record_id,
                            "objective": objective,
                            "error": str(error),
                        }
                    )
                    break
                checked = source_finding(
                    objective,
                    parsed.model_dump(),
                    page=page.model_dump(),
                    window=HtmlWindow(
                        source.chunk_start,
                        source.chunk_end,
                        html[source.chunk_start : source.chunk_end],
                    ),
                )
                sources.extend(checked.sources)
            if record.record_id not in invalid_ids:
                record.sources = sources
                record.evidence_status = (
                    "source_matched"
                    if any(s.evidence_status == "source_matched" for s in sources)
                    else "needs_review"
                )
    audit["after_local_revalidation"] = {
        o: sum(r.evidence_status == "source_matched" for r in rows)
        for o, rows in result.records
    }
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = OpenRouter(client, key, config, root)
        for page_id, html in html_by_page.items():
            spans = sorted(
                {
                    (s.chunk_start, s.chunk_end)
                    for _, rows in result.records
                    for r in rows
                    if r.evidence_status == "needs_review"
                    and r.record_id not in invalid_ids
                    for s in r.sources
                    if s.page_id == page_id
                }
            )
            for start, end in spans:
                pending = [
                    (o, r)
                    for o, rows in result.records
                    for r in rows
                    if r.evidence_status == "needs_review"
                    and r.record_id not in invalid_ids
                    and any(
                        s.page_id == page_id
                        and s.chunk_start == start
                        and s.chunk_end == end
                        for s in r.sources
                    )
                ]
                for offset in range(0, len(pending), 20):
                    batch = pending[offset : offset + 20]
                    print(
                        f"Repair {page_id} {start}:{end}: {len(batch)} records",
                        flush=True,
                    )
                    issues = await repair_evidence(
                        batch,
                        HtmlWindow(start, end, html[start:end]),
                        pages[page_id],
                        llm,
                        root,
                        f"{page_id}-{start}-{offset}",
                    )
                    audit["repair_outcomes"].append(
                        {
                            "page_id": page_id,
                            "record_ids": [r.record_id for _, r in batch],
                            "issues": issues,
                        }
                    )
                    write_json(
                        root / "records-checkpoint.json", result.records.model_dump()
                    )
        claims = [
            r
            for o in ("company_relationships", "technology_signals")
            for r in getattr(result.records, o)
            if r.record_id not in invalid_ids
        ]
        audit["claim_review_issues"] = await review_claims(
            claims, llm, root, "saved-crawl"
        )
        result.company_overview = None
        try:
            result.company_overview = await summarize_company(result, llm, root)
        except (ValueError, ModelUnavailable, ModelBudgetExceeded) as error:
            audit["summary_error"] = str(error)
        result.technology_summary = summarize_technologies(
            result.records.technology_signals
        )
        for objective, status in result.objectives.items():
            rows = getattr(result.records, objective)
            matched = sum(r.evidence_status == "source_matched" for r in rows)
            status.source_matched_count = matched
            status.needs_review_count = len(rows) - matched
            if rows:
                status.status = "found" if matched else "needs_review"
                status.note = "Saved-source audit; original crawl coverage and partial page outcomes remain applicable. Source matching and model review are not independent verification."
        result.run_id += "-audited"
        result.finished_at = utc_now()
        result.output_directory = str(root)
        result.usage = {
            "original_crawl": original["usage"],
            "saved_source_audit": llm.usage(),
            "known_cost_usd": original["usage"]["known_cost_usd"]
            + llm.usage()["known_cost_usd"],
            "unknown_cost_calls": original["usage"]["unknown_cost_calls"]
            + llm.usage()["unknown_cost_calls"],
        }
        audit["finished_at"] = utc_now()
        audit["after"] = {
            o: sum(r.evidence_status == "source_matched" for r in rows)
            for o, rows in result.records
        }
        write_json(root / "result.json", result.model_dump())
        write_json(root / "audit.json", audit)
        print(
            json.dumps(
                {
                    "counts": audit["after"],
                    "usage": {k: v for k, v in llm.usage().items() if k != "by_call"},
                }
            ),
            flush=True,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--provider", default="together")
    asyncio.run(run(parser.parse_args()))
