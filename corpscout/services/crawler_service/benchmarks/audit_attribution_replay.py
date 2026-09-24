"""Compare the targeted replay to frozen failure controls without sending them to the model."""

import argparse
import json
from pathlib import Path

from crawler_service.analytics import accepted_finding, source_supported_finding
from crawler_service.models import Finding
from crawler_service.storage import content_hash, write_json


def audit(root: Path):
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    output: dict = {
        "cases": {},
        "integrity_errors": [],
        "independently_verified": False,
    }
    for name, case in manifest["cases"].items():
        for page in case["pages"]:
            html = (root / name / page["html_file"]).read_text(encoding="utf-8")
            if content_hash(html) != page["html_sha256"]:
                output["integrity_errors"].append(
                    f"{name}/{page['page_id']}: HTML hash mismatch"
                )
        result = json.loads((root / name / "result.json").read_text(encoding="utf-8"))
        if name == "plausible":
            output["cases"][name] = {
                "repaired": [
                    r["record"]["record_id"] for r in result["records"] if r["accepted"]
                ],
                "total": len(result["records"]),
            }
            continue
        records = [Finding.model_validate(r) for r in result["technology_signals"]]
        accepted = [r for r in records if accepted_finding(r)]
        source_supported = [r for r in records if source_supported_finding(r)]
        subjects = {
            "dmc": {"dmc", "dmc, inc.", "dmc inc."},
            "thoughtbot": {"thoughtbot"},
            "mcap": {"novelic"},
        }[name]
        wrong_actors = [
            r.record_id
            for r in accepted
            if (r.data["company"] or "").casefold() not in subjects
        ]
        unsupported_offers = [
            r.record_id
            for r in accepted
            if name in {"dmc", "thoughtbot"} and r.data["signal"] == "offers"
        ]
        unexpected_negatives = [
            r.record_id for r in accepted if r.data["signal"] == "explicitly_not_used"
        ]
        inputs = json.loads((root / name / "input.json").read_text(encoding="utf-8"))
        # This measures retention of previously manually retained thoughtbot relationships,
        # not recall against an exhaustive ground-truth dataset.
        retained, lost, retained_before_catalog = [], [], []
        if name == "thoughtbot":
            for original in inputs["original_accepted"]:
                data = original["data"]
                if data["signal"] == "offers":
                    continue
                matched = any(
                    r.data["signal"] == data["signal"]
                    and r.data["scope"] == data["scope"]
                    and set(r.data["statement_ids"]) & set(data["statement_ids"])
                    for r in accepted
                )
                (retained if matched else lost).append(original["record_id"])
                if any(
                    r.data["signal"] == data["signal"]
                    and r.data["scope"] == data["scope"]
                    and set(r.data["statement_ids"]) & set(data["statement_ids"])
                    for r in source_supported
                ):
                    retained_before_catalog.append(original["record_id"])
        output["cases"][name] = {
            "input_statements": len(inputs["records"]),
            "accepted_descriptions": result["page_description_validation"]["accepted"],
            "accepted_observations": len(accepted),
            "source_supported_observations": len(source_supported),
            "catalog_pending_ids": [
                r.record_id for r in source_supported if not accepted_finding(r)
            ],
            "wrong_actors": wrong_actors,
            "unsupported_offers": unsupported_offers,
            "unexpected_negatives": unexpected_negatives,
            "retained_controls": retained,
            "retained_controls_before_catalog": retained_before_catalog,
            "lost_controls": lost,
            "pending_descriptions": result["pending_description_ids"],
            "observations": [
                {
                    "record_id": r.record_id,
                    "technology": r.data["technology"],
                    "company": r.data["company"],
                    "signal": r.data["signal"],
                    "scope": r.data["scope"],
                    "statement_ids": r.data["statement_ids"],
                }
                for r in accepted
            ],
        }
        if name == "mcap":
            output["cases"][name]["both_relationships_preserved"] = {
                (r.data["signal"], r.data["scope"])
                for r in accepted
                if r.data["company"] == "NOVELIC"
                and r.data["job_title"] == "Senior Data Engineer/Data Architect"
                and r.data["technology"] == "MCAP"
            } >= {("stated_use", "role"), ("preferred_experience", "role")}
    output["usage"] = {
        key: sum(
            case.get("usage", {}).get(key, 0) for case in manifest["cases"].values()
        )
        for key in [
            "calls",
            "successful_responses",
            "known_cost_usd",
            "unknown_cost_calls",
        ]
    }
    write_json(root / "comparison.json", output)
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    audit(parser.parse_args().root)
