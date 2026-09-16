"""Audit a homepage-only run without fetching pages, calling models, or submitting data."""

import argparse
import json
from pathlib import Path

from company_research.analytics import accepted_finding, technology_submission_records
from company_research.content import normalize
from company_research.models import ResearchResult
from company_research.storage import content_hash, write_json


def audit(root: Path) -> dict:
    result = ResearchResult.model_validate_json((root / "result.json").read_bytes())
    pages = {p.page_id: p for p in result.pages}
    issues = []
    accepted_ids = set()
    for objective, records in result.records:
        for finding in records:
            if not accepted_finding(finding):
                continue
            accepted_ids.add(finding.record_id)
            for source in finding.sources:
                if source.evidence_status != "source_matched":
                    continue
                page = pages[source.page_id]
                if (
                    page.html_file is None
                    or content_hash((root / page.html_file).read_text())
                    != source.html_sha256
                ):
                    issues.append(
                        f"Source hash mismatch: {objective}/{finding.record_id}/{source.page_id}"
                    )
    for objective, entities in result.entities:
        for entity in entities:
            if not set(entity.record_ids) <= accepted_ids:
                issues.append(
                    f"Unaccepted record in entity: {objective}/{entity.entity_id}"
                )
    submission = {
        "schema_version": "1.0",
        "run_id": result.run_id,
        "site_url": result.site_url,
        "model": result.config.model,
        "records": technology_submission_records(result.records.technology_signals),
    }
    write_json(root / "technology-submission-preview.json", submission)
    proposals = {}
    for record in submission["records"]:
        match = record["data"]["catalog_match"]
        if match["status"] != "proposed":
            continue
        proposal = match["proposed_technology"]
        if not proposal["description"].strip() or not (
            proposal["category_ids"] or proposal["category_suggestion"]
        ):
            issues.append(f"Incomplete proposal metadata: {proposal['name']}")
        proposals[match["proposal_id"]] = proposal
    expected = json.loads(
        (
            Path(__file__).parents[1] / "tests/fixtures/novelic_acceptance.json"
        ).read_text()
    )
    selected_checks = []
    for expected_page in expected["sources"]:
        found_pages = [
            p
            for p in result.pages
            if p.source_url == expected_page["url"] and p.fetch_status == "fetched"
        ]
        technology_names = {
            normalize(finding.data["technology"])
            for finding in result.records.technology_signals
            if accepted_finding(finding)
            and any(
                source.url == expected_page["url"]
                and source.evidence_status == "source_matched"
                for source in finding.sources
            )
        }
        selected_checks.append(
            {
                "url": expected_page["url"],
                "fetched": bool(found_pages),
                "expected_names": expected_page["expected_technology_names"],
                "found_names": sorted(technology_names),
                "missing_names": [
                    n
                    for n in expected_page["expected_technology_names"]
                    if normalize(n) not in technology_names
                ],
                "forbidden_names_returned": [
                    n
                    for n in expected_page["forbidden_technology_names"]
                    if normalize(n) in technology_names
                ],
            }
        )
    report = {
        "run_id": result.run_id,
        "status": result.status,
        "stop_reason": result.stop_reason,
        "scope": "Selected previously source-checked facts; not exhaustive recall. Unvisited pages are discovery gaps, not extraction failures.",
        "acceptance_meaning": "Accepted means passed the current pipeline checks, not independently verified or administrator-approved.",
        "pages": [
            {
                "page_id": p.page_id,
                "url": p.source_url,
                "selected_for": p.selected_for,
                "fetch_status": p.fetch_status,
                "extraction_status": p.extraction_status,
                "attempts": p.attempts,
            }
            for p in result.pages
        ],
        "objective_counts": {
            objective: {
                "raw": len(records),
                "accepted": sum(accepted_finding(r) for r in records),
            }
            for objective, records in result.records
        },
        "entity_counts": {
            objective: len(entities) for objective, entities in result.entities
        },
        "entities_by_company": {
            objective: {
                company: sum(
                    (
                        e.identity.get("company")
                        or e.identity.get("employer")
                        or "unknown"
                    )
                    == company
                    for e in entities
                )
                for company in sorted(
                    {
                        e.identity.get("company")
                        or e.identity.get("employer")
                        or "unknown"
                        for e in entities
                    }
                )
            }
            for objective, entities in result.entities
        },
        "management_names": sorted(
            {
                r.data["name"]
                for r in result.records.people
                if accepted_finding(r)
                and any(
                    s.url == "https://www.novelic.com/management/"
                    and s.evidence_status == "source_matched"
                    for s in r.sources
                )
            }
        ),
        "careers_jobs": {
            r.data["job_url"]: r.data["title"]
            for r in result.records.jobs
            if accepted_finding(r)
            and r.data.get("job_url")
            and any(
                s.url == "https://www.novelic.com/careers/"
                and s.evidence_status == "source_matched"
                for s in r.sources
            )
        },
        "selected_source_checks": selected_checks,
        "accepted_technology_observations": len(submission["records"]),
        "complete_model_responses": sum(
            c.get("finish_reason") in {"stop", "tool_calls"} and not c.get("error")
            for c in result.usage.get("by_call", [])
        ),
        "distinct_proposals": list(proposals.values()),
        "integrity_issues": issues,
        "usage": result.usage,
        "submitted": False,
    }
    write_json(root / "audit.json", report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_directory", type=Path)
    print(json.dumps(audit(parser.parse_args().run_directory), indent=2))
