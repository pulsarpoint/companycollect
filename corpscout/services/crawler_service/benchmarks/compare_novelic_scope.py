"""Compare fixed-budget NOVELIC runs using selected regression checks, without model calls."""

import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit

from crawler_service.analytics import accepted_finding
from crawler_service.models import ResearchResult
from crawler_service.storage import write_json


def metrics(root: Path) -> dict:
    result = ResearchResult.model_validate_json((root / "result.json").read_bytes())
    records = {
        objective: [finding for finding in values if accepted_finding(finding)]
        for objective, values in result.records
    }
    jobs = records["jobs"]
    technologies = records["technology_signals"]
    calls = [
        json.loads(path.read_text()) for path in sorted((root / "calls").glob("*.json"))
    ]
    return {
        "run_id": result.run_id,
        "status": result.status,
        "stop_reason": result.stop_reason,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "pages_fetched": sum(page.fetch_status == "fetched" for page in result.pages),
        "fetch_failures": [
            page.source_url for page in result.pages if page.fetch_status == "failed"
        ],
        "extraction_failures": [
            page.source_url
            for page in result.pages
            if page.extraction_status == "failed"
        ],
        "partner_general_pages": [
            page.source_url
            for page in result.pages
            if "analog.com" in (urlsplit(page.source_url).hostname or "")
            and "novelic" not in urlsplit(page.source_url).path
        ],
        "ats_pages": [
            {
                "url": page.source_url,
                "fetch_status": page.fetch_status,
                "extraction_status": page.extraction_status,
            }
            for page in result.pages
            if (urlsplit(page.source_url).hostname or "").endswith("oneassessment.com")
        ],
        "careers_job_urls": sorted(
            {
                record.data["job_url"]
                for record in jobs
                if record.data.get("job_url")
                and any(
                    source.url == "https://www.novelic.com/careers/"
                    for source in record.sources
                )
            }
        ),
        "management_names": sorted(
            {
                record.data["name"]
                for record in records["people"]
                if any(
                    source.url == "https://www.novelic.com/management/"
                    for source in record.sources
                )
            }
        ),
        "job_technology_signals": [
            record.model_dump() for record in technologies if record.data.get("job_url")
        ],
        "antenna_tools": sorted(
            {
                record.data["technology"]
                for record in technologies
                if any("antenna-design" in source.url for source in record.sources)
            }
        ),
        "credentials": [
            record.model_dump() for record in records["certifications_compliance"]
        ],
        "legal_names": [
            record.model_dump()
            for record in records["company_profile"]
            if record.data.get("field") == "legal_name"
        ],
        "relationships": [
            record.model_dump() for record in records["company_relationships"]
        ],
        "documents": [record.model_dump() for record in records["document_links"]],
        "target_names": result.discovery.get("target_names"),
        "job_coverage": result.discovery.get("job_coverage"),
        "entity_counts": {
            objective: len(values) for objective, values in result.entities
        },
        "reported_cost_usd": sum(
            (call.get("usage") or {}).get("cost") or 0 for call in calls
        ),
        "unknown_cost_calls": sum(
            (call.get("usage") or {}).get("cost") is None for call in calls
        ),
        "calls": len(calls),
        "request_errors": [
            {key: call.get(key) for key in ("call_id", "task", "error", "http_status")}
            for call in calls
            if call.get("error")
        ],
        "overview_validation": (result.company_overview or {}).get("validation"),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("current", type=Path)
    args = parser.parse_args()
    report = {
        "scope": "Selected source checks, not exhaustive accuracy. Inspect source HTML before accepting semantic claims.",
        "baseline": metrics(args.baseline),
        "current": metrics(args.current),
    }
    write_json(args.current / "scope-comparison.json", report)
    print(
        json.dumps(
            {
                "baseline": {
                    key: report["baseline"][key]
                    for key in ("pages_fetched", "calls", "reported_cost_usd")
                },
                "current": {
                    key: report["current"][key]
                    for key in ("pages_fetched", "calls", "reported_cost_usd")
                },
            },
            indent=2,
        )
    )
