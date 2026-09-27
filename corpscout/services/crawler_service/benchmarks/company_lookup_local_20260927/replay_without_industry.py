"""Offline control: apply identity validation to the identical assessment without NACE.

The deployed identity model already receives no NACE information. Removing the
subsequent industry checks therefore isolates their effect without another model
request, different fetched pages, or different extraction/model sampling.
"""

import json
from pathlib import Path

from crawler_service.company_lookup import CompanyAssessment, accept_match
from crawler_service.storage import write_json

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "data/company-lookup-industry-v2-20260927"
OUTPUT = ROOT / "data/company-lookup-industry-control-20260927"


def main() -> None:
    summary = json.loads((SOURCE / "summary.json").read_text())
    results = []
    for case in summary["cases"]:
        domain = case["root_domain"]
        saved = json.loads((SOURCE / domain / "result.json").read_text())
        if not saved.get("assessment"):
            continue
        assessment = CompanyAssessment.model_validate(saved["assessment"])
        assessment.industry_checks = []
        matched, reason = accept_match(
            assessment, saved["candidates"], saved["identity"]
        )
        result = {
            "domain": domain,
            "source_request_id": case["request_id"],
            "same_verified_facts": True,
            "same_registry_candidates": True,
            "same_identity_model_response": True,
            "industry_company_id": saved["company_id"],
            "without_industry_company_id": matched["company_id"] if matched else None,
            "without_industry_status": "matched" if matched else "not_found",
            "validation": reason,
            "gate_only_control_company_id": matched["company_id"] if matched else None,
        }
        write_json(OUTPUT / domain / "result.json", result)
        results.append(result)
        print(json.dumps(result), flush=True)
    write_json(OUTPUT / "summary.json", {"method": __doc__, "cases": results})


if __name__ == "__main__":
    main()
