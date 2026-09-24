"""Evaluate saved job outputs against source-read controls; no model calls."""

import argparse
import json
import shutil
from pathlib import Path

from compare_deepseek_direct import summarize_calls

from crawler_service.analytics import accepted_finding, source_supported_finding
from crawler_service.models import Finding
from crawler_service.storage import content_hash, utc_now, write_json


def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def audit(root: Path) -> dict:
    controls = read(root / "controls.json")
    page = read(root / "source.json")
    aliases = {
        "AWS": {"aws", "amazon web services"},
        "S3": {"s3", "amazon s3", "aws s3"},
        "IAM": {"iam", "aws iam", "amazon iam"},
        "Lambda": {"lambda", "aws lambda", "amazon lambda"},
        "Glue": {"glue", "aws glue", "amazon glue"},
        "Athena": {"athena", "amazon athena", "aws athena"},
        "Protobuf": {"protobuf", "protocol buffers"},
        "ROS2": {"ros2", "ros 2", "robot operating system 2"},
        "DVC": {"dvc", "data version control"},
    }
    arms, integrity = {}, []
    for api in ("deepseek", "openrouter"):
        arm = root / api
        result = read(arm / "result.json")
        records = [Finding.model_validate(r) for r in result["technology_signals"]]
        accepted = [r for r in records if accepted_finding(r)]
        supported = [r for r in records if source_supported_finding(r)]
        checks = []
        for control in controls["technology_controls"]:
            names = aliases.get(control["name"], {control["name"].casefold()})
            matching = [
                r
                for r in accepted
                if r.data["technology"].casefold() in names
                and r.data["signal"] == control["signal"]
                and r.data["scope"] == control["scope"]
                and r.data["company"] == controls["company"]
                and r.data["job_title"] == controls["job_title"]
            ]
            checks.append(
                control
                | {
                    "matched_record_ids": [r.record_id for r in matching],
                    "passed": bool(matching),
                }
            )
        calls = [read(p) for p in sorted((arm / "calls").glob("*.json"))]
        extracted = read(arm / "extracted-statements.json")
        arms[api] = summarize_calls(calls, api) | {
            "extracted_statements": len(extracted),
            "source_matched_extracted_statements": sum(
                r["evidence_status"] == "source_matched" for r in extracted
            ),
            "accepted_descriptions": result["page_description_validation"]["accepted"],
            "accepted_technology_observations": len(accepted),
            "source_supported_technology_observations": len(supported),
            "accepted_credentials": result["accepted_record_ids"][
                "certifications_compliance"
            ],
            "pending_extraction_page_ids": result["pending_extraction_page_ids"],
            "pending_description_ids": result["pending_description_ids"],
            "controls_passed": sum(c["passed"] for c in checks),
            "controls_total": len(checks),
            "controls": checks,
            "accepted_records": [r.model_dump() for r in accepted],
            "catalog_pending_records": [
                r.model_dump() for r in supported if not accepted_finding(r)
            ],
            "note": "Application acceptance includes reviewed technology proposals; no administrator approval or DB submission. These controls are a targeted source review, not exhaustive recall.",
        }
        html = (arm / page["html_file"]).read_text(encoding="utf-8")
        if content_hash(html) != page["html_sha256"]:
            integrity.append(f"Changed HTML for {api}")
    left = read(root / "deepseek/calls/00001.json")["request"]["messages"]
    right = read(root / "openrouter/calls/00001.json")["request"]["messages"]
    if left != right:
        integrity.append("Different initial extraction prompts")
    result = {"created_at": utc_now(), "integrity_errors": integrity, "arms": arms}
    write_json(root / "comparison.json", result)
    shutil.copy2(__file__, root / "audit-harness.py")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    result = audit(parser.parse_args().root)
    print(
        json.dumps(
            {
                api: {
                    k: v
                    for k, v in arm.items()
                    if k
                    not in {"controls", "accepted_records", "catalog_pending_records"}
                }
                for api, arm in result["arms"].items()
            },
            indent=2,
        )
    )
