"""Check controller provenance and recovery invariants without model calls."""

import argparse
from pathlib import Path

from crawler_service.analytics import accepted_finding
from crawler_service.content import normalize_evidence
from crawler_service.models import ResearchResult
from crawler_service.storage import content_hash, write_json


def audit(root: Path) -> dict:
    result = ResearchResult.model_validate_json((root / "result.json").read_bytes())
    pages = {page.page_id: page for page in result.pages}
    issues = []
    bindings = []
    for objective in ("jobs", "technology_signals"):
        for record in getattr(result.records, objective):
            binding = record.data.get("job_url_binding")
            if binding is None:
                continue
            title = record.data.get("title" if objective == "jobs" else "job_title")
            contexts = [pages[source.page_id].job_detail for source in record.sources]
            if not any(
                context is not None
                and context.url == record.data.get("job_url") == binding["bound_url"]
                and normalize_evidence(context.title) == normalize_evidence(title or "")
                for context in contexts
            ):
                issues.append(f"Invalid posting binding: {record.record_id}")
            bindings.append(
                {
                    "record_id": record.record_id,
                    "objective": objective,
                    "job_url": record.data.get("job_url"),
                    "source_matched": record.evidence_status == "source_matched",
                    "accepted": accepted_finding(record),
                }
            )
    attempts = []
    for page in result.pages:
        if (
            page.html_file
            and content_hash((root / page.html_file).read_text(encoding="utf-8"))
            != page.html_sha256
        ):
            issues.append(f"HTML changed: {page.page_id}")
        previous = {}
        for attempt in page.extraction_attempts:
            earlier = previous.get(attempt.chunk_index)
            if earlier and earlier.status not in {"retry_pending", "running"}:
                issues.append(
                    f"Repeated completed/semantic chunk: {page.page_id}/{attempt.chunk_index}"
                )
            if earlier and attempt.attempt != earlier.attempt + 1:
                issues.append(
                    f"Nonsequential attempt: {page.page_id}/{attempt.chunk_index}"
                )
            previous[attempt.chunk_index] = attempt
        attempts.append(
            {
                "page_id": page.page_id,
                "url": page.source_url,
                "fetch_attempts": page.attempts,
                "chunks_planned": page.chunks_planned,
                "chunk_attempts": [a.model_dump() for a in page.extraction_attempts],
            }
        )
    if len(result.pages) > result.config.max_pages:
        issues.append("Page budget exceeded")
    if result.usage["calls"] > result.config.max_model_calls:
        issues.append("Model-call budget exceeded")
    report = {
        "result_sha256": content_hash(
            (root / "result.json").read_text(encoding="utf-8")
        ),
        "scope": "Controller invariants and source hashes, not independent semantic verification.",
        "issues": issues,
        "bindings": bindings,
        "attempts": attempts,
        "engineering_coverage": result.discovery.get("engineering_coverage"),
        "pending_extractions": result.discovery.get("pending_extractions"),
        "submitted": False,
    }
    write_json(root / "controller-audit.json", report)
    print(
        {
            "issues": issues,
            "bound_jobs": sum(b["objective"] == "jobs" for b in bindings),
            "bound_technology_observations": sum(
                b["objective"] == "technology_signals" for b in bindings
            ),
        },
        flush=True,
    )
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    audit(parser.parse_args().root)
