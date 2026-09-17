"""Audit saved benchmark provenance and stage losses without additional model calls."""

import json
from collections import Counter
from pathlib import Path

import click

from company_research.analytics import (
    accepted_finding,
    source_supported_finding,
    summarize_technologies,
)
from company_research.models import Finding
from company_research.statement_review import accepted_description
from company_research.storage import content_hash, write_json


def audit_site(root: Path) -> dict:
    calls = [
        json.loads(p.read_text(encoding="utf-8")) for p in root.glob("*/calls/*.json")
    ]
    audit = {
        "calls": len(calls),
        "reported_cost_usd": sum(
            (c.get("usage") or {}).get("cost") or 0 for c in calls
        ),
        "unknown_cost_calls": [
            {"task": c["task"], "error": c.get("error")}
            for c in calls
            if (c.get("usage") or {}).get("cost") is None
        ],
        "calls_by_stage": dict(Counter(c["task"].split(":")[0] for c in calls)),
        "integrity_errors": [],
        "notes": ["Automated provenance audit, not a factual accuracy score"],
    }
    result_path = root / "statements/statement-result.json"
    if not result_path.exists():
        audit["status"] = "no_statement_result"
        return audit
    result = json.loads(result_path.read_text(encoding="utf-8"))
    page_outputs = [
        json.loads(p.read_text(encoding="utf-8"))
        for p in (root / "statements/page-statements").glob("*.json")
    ]
    audit["statement_page_coverage"] = {
        "pages_with_work_attempted": len(page_outputs),
        "pages_with_complete_extraction": sum(
            p["status"] == "complete" for p in page_outputs
        ),
        "pages_with_any_statements": sum(bool(p["statements"]) for p in page_outputs),
        "pages_with_valid_extraction": sum(
            bool(p["statements"]) or p["status"] == "complete" for p in page_outputs
        ),
        "incomplete_page_ids": [
            p["page_id"] for p in page_outputs if p["status"] != "complete"
        ],
    }
    # The frozen harness counted attempted files as examined pages. Keep that raw
    # output, and provide a separate audited copy that exposes processing failures.
    company_path = root / "accepted-company.json"
    if company_path.exists():
        company = json.loads(company_path.read_text(encoding="utf-8"))
        holds_path = root / "manual-holds.json"
        holds = (
            json.loads(holds_path.read_text(encoding="utf-8"))
            if holds_path.exists()
            else []
        )
        held_ids = {h["record_id"] for h in holds}
        held_records = {}
        for objective, records in company["records"].items():
            held_records[objective] = [r for r in records if r["record_id"] in held_ids]
            company["records"][objective] = [
                r for r in records if r["record_id"] not in held_ids
            ]
        company["manual_holds"] = holds
        company["technology_summary"] = [
            r.model_dump()
            for r in summarize_technologies(
                [
                    Finding.model_validate(r)
                    for r in company["records"]["technology_signals"]
                ]
            )
        ]
        audit["manual_hold_count"] = sum(len(rows) for rows in held_records.values())
        write_json(root / "manual-held-records.json", held_records)
        for objective in ("technology_signals", "certifications_compliance"):
            status = company["objectives"][objective]
            status.update(audit["statement_page_coverage"])
            status["pages_examined"] = audit["statement_page_coverage"][
                "pages_with_valid_extraction"
            ]
            if (
                not company["records"][objective]
                and audit["statement_page_coverage"]["incomplete_page_ids"]
            ):
                status["status"] = "processing_incomplete"
            status["manual_held_count"] = len(held_records[objective])
            if held_records[objective] and not company["records"][objective]:
                status["status"] = "needs_review"
        company["audit_note"] = (
            "Derived offline view with corrected processing coverage and explicit manual holds. Original observations and frozen model outputs are unchanged. Remaining records are not exhaustively manually verified."
        )
        write_json(root / "audited-company.json", company)
    descriptions = {
        r["record_id"]: Finding.model_validate(r) for r in result["page_statements"]
    }
    stages = {}
    for key in ["technology_signals", "certifications_compliance"]:
        values = [Finding.model_validate(r) for r in result[key]]
        stages[key] = {
            "record_versions": len(values),
            "source_supported": sum(source_supported_finding(r) for r in values),
            "fully_accepted": sum(accepted_finding(r) for r in values),
            "rejection_reasons": [
                {
                    "id": r.record_id,
                    "name": r.data.get("technology") or r.data.get("standard_name"),
                    "evidence_status": r.evidence_status,
                    "source_review": r.data.get("interpretation_review"),
                    "catalog_error": r.data.get("catalog_error"),
                    "proposal_review": r.data.get("proposal_review"),
                }
                for r in values
                if not accepted_finding(r)
            ],
        }
        accepted_ids = set(result["accepted_record_ids"][key])
        if accepted_ids != {r.record_id for r in values if accepted_finding(r)}:
            audit["integrity_errors"].append(
                f"{key}: saved acceptance differs from current guards"
            )
        for record in values:
            if not accepted_finding(record):
                continue
            for source in record.sources:
                p = root / "statements/html" / f"{source.page_id}.html"
                if (
                    not p.exists()
                    or content_hash(p.read_text(encoding="utf-8")) != source.html_sha256
                ):
                    audit["integrity_errors"].append(
                        f"{record.record_id}: changed or missing HTML"
                    )
            for statement_id in record.data["statement_ids"]:
                if statement_id not in descriptions or not accepted_description(
                    descriptions[statement_id]
                ):
                    audit["integrity_errors"].append(
                        f"{record.record_id}: unreviewed description"
                    )
            if record.data.get("job_title") and record.data.get("scope") != "role":
                audit["integrity_errors"].append(
                    f"{record.record_id}: job technology outside role scope"
                )
    audit["stages"] = stages
    audit["pending_descriptions"] = [
        {
            "id": r.record_id,
            "name": r.data.get("source_name"),
            "context": r.data["context"],
            "review": r.data.get("description_review"),
            "source_issues": [s.issues for s in r.sources],
            "source_urls": [s.url for s in r.sources],
        }
        for r in descriptions.values()
        if not accepted_description(r)
    ]
    audit["status"] = "complete"
    return audit


@click.command()
@click.argument("root", type=click.Path(exists=True, path_type=Path))
def main(root: Path) -> None:
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    results = {site["id"]: audit_site(root / site["id"]) for site in manifest["sites"]}
    write_json(root / "automatic-audit.json", results)
    for name, result in results.items():
        click.echo(
            f"{name}: {result['calls']} calls; {len(result['integrity_errors'])} integrity errors"
        )


if __name__ == "__main__":
    main()
