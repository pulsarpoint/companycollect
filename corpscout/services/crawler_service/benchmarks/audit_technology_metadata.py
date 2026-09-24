"""Audit replay provenance and preserve manual quality holds without editing model output."""

import argparse
import json
from collections import Counter
from pathlib import Path

from crawler_service.analytics import accepted_finding, source_supported_finding
from crawler_service.content import proposal_metadata_hash
from crawler_service.models import Finding, TechnologySignal
from crawler_service.storage import content_hash, write_json


def audit(root: Path) -> dict:
    state = json.loads((root / "result.json").read_text(encoding="utf-8"))
    records = [Finding.model_validate(record) for record in state["records"]]
    baseline = {
        record["record_id"]: record
        for record in json.loads(
            (root / "baseline-records.json").read_text(encoding="utf-8")
        )
    }
    failures = []
    original_text = Path(state["baseline_result"]).read_text(encoding="utf-8")
    if content_hash(original_text) != state["baseline_sha256"]:
        failures.append("Baseline result changed")
    original = json.loads(original_text)
    if (
        content_hash(json.dumps(original["records"]["jobs"], sort_keys=True))
        != state["jobs_unchanged_sha256"]
    ):
        failures.append("Baseline job records changed")
    checked_sources = set()
    repairs = set()
    for record in records:
        for source in record.sources:
            key = (source.page_id, source.html_sha256)
            if key not in checked_sources:
                html = (root / "html" / f"{source.page_id}.html").read_text(
                    encoding="utf-8"
                )
                if content_hash(html) != source.html_sha256:
                    failures.append(f"Saved HTML changed: {source.page_id}")
                checked_sources.add(key)
        parent = baseline.get(record.record_id) or baseline.get(
            record.data.get("correction_of")
        )
        if parent is None:
            failures.append(f"Missing original: {record.record_id}")
            continue
        mutable = {"signal", "context"} if record.data.get("correction_of") else set()
        for key in TechnologySignal.model_fields.keys() - {"evidence"} - mutable:
            if record.data[key] != parent["data"][key]:
                failures.append(f"Claim field changed: {record.record_id}/{key}")
        for source in record.sources:
            if not any(
                source.evidence
                == Finding.model_validate(parent).sources[index].evidence
                for index in range(len(parent["sources"]))
            ):
                failures.append(f"Quotation changed: {record.record_id}")
        for repair in record.data.get("proposal_metadata_repairs", []):
            repairs.add(content_hash(json.dumps(repair, sort_keys=True)))
            if repair["after"] is None:
                continue
            for key in repair["before"].keys() - {
                "description",
                "category_ids",
                "category_suggestion",
            }:
                if repair["after"][key] != repair["before"][key]:
                    failures.append(
                        f"Identity changed in metadata repair: {record.record_id}/{key}"
                    )
        review = record.data.get("proposal_review", {})
        if accepted_finding(record) and review.get("metadata_sha256"):
            if (
                proposal_metadata_hash(
                    record.data["catalog_match"]["proposed_technology"]
                )
                != review["metadata_sha256"]
            ):
                failures.append(f"Accepted changed metadata: {record.record_id}")
    call_stages = Counter(
        call["task"].split(":")[0] for call in state["usage"]["by_call"]
    )
    if set(call_stages) - {
        "review",
        "proposal_review",
        "proposal_metadata_repair",
        "resolve",
    }:
        failures.append("Replay called a non-review/resolution model stage")
    accepted = [record for record in records if accepted_finding(record)]
    engineering_usage = [
        record.record_id
        for record in accepted
        if record.data["signal"] == "stated_use"
        and any(source.page_id in {"p0002", "p0003"} for source in record.sources)
    ]
    if engineering_usage:
        failures.append("Engineering skills still accepted as usage")
    manual_holds = [
        {
            "kind": "incorrect_vendor_in_proposal_description",
            "record_ids": [
                r.record_id for r in accepted if r.data["technology"] == "AutoPLANT"
            ],
            "source_observation_supported": True,
            "finding": "The company lists AutoPLANT in its mechanical engineering skills. Its draft incorrectly calls AutoPLANT an Autodesk product. It is a Bentley product; source evidence did not establish a vendor.",
            "verification_source": "https://bentleysystems.service-now.com/community?id=kb_article_view&sysparm_article=KB0101406",
            "action": "Administrator should correct the catalog draft before approval; preserve advertised expertise and original quotations.",
        },
        {
            "kind": "generic_vendor_tool_collection",
            "record_ids": [
                r.record_id
                for r in accepted
                if r.data["technology"] == "Cadence Design Flow Tools"
            ],
            "source_observation_supported": False,
            "finding": "This phrase names an unspecified collection of Cadence tools. The source does not identify one application or named product family. Automated acceptance is too broad for the agreed specificity policy.",
            "action": "Hold this proposed identity; preserve it as service/capability context unless a specific application is sourced.",
        },
    ]
    remaining = [
        {
            "record_id": record.record_id,
            "technology": record.data["technology"],
            "signal": record.data["signal"],
            "review": record.data["proposal_review"],
            "repair_attempts": len(record.data.get("proposal_metadata_repairs", [])),
        }
        for record in records
        if source_supported_finding(record)
        and record.data.get("proposal_review", {}).get("status") == "rejected"
    ]
    result = {
        "scope": "Integrity checks plus selected manual quality checks; not an exhaustive technology precision/recall evaluation. Manual holds do not modify model output.",
        "result_sha256": content_hash(
            (root / "result.json").read_text(encoding="utf-8")
        ),
        "integrity_failures": failures,
        "distinct_saved_sources_checked": len(checked_sources),
        "distinct_metadata_repair_attempts": len(repairs),
        "accepted_engineering_stated_use": engineering_usage,
        "manual_holds": manual_holds,
        "source_review_followups": [
            {
                "record_id": record.record_id,
                "technology": record.data["technology"],
                "review": record.data.get("interpretation_review"),
                "note": "The source names NOVELIC's branded perception software. The reviewer describes explicit use but marks the identity non-specific. Investigate this likely false negative and distinguish an offered product from an internal stack.",
            }
            for record in records
            if record.data["technology"] == "Norasoft™"
        ],
        "remaining_metadata_rejections": remaining,
        "method_limitations": [
            "One company, selected historical observations, changed prompts across three preserved replay phases.",
            "The final signal checks cover 19 known errors and 28 positive job controls; they do not measure unseen-site accuracy.",
            "AutoPLANT vendor verification was an external manual audit lookup after the saved-data experiment, never a new crawler fetch or model input.",
            "One HTTP 429 and one 120-second deadline attempt did not report usage/cost; the reported total is not a guaranteed final billed total.",
            "Draft descriptions still include unsourced vendor and product details. Administrator verification is required.",
            "Simulink has an accepted job observation and a rejected expertise observation with different drafts of the same proposed identity. Metadata decisions should eventually be shared by identical validated drafts.",
        ],
    }
    write_json(root / "manual-audit.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    result = audit(parser.parse_args().root.resolve())
    print(
        json.dumps(
            {
                key: value
                for key, value in result.items()
                if key
                not in {
                    "manual_holds",
                    "remaining_metadata_rejections",
                    "method_limitations",
                }
            },
            indent=2,
        )
    )
