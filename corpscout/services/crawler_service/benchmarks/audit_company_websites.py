"""Source-controlled comparison of the new website experiment; makes no model calls."""

import argparse
import json
import shutil
from pathlib import Path

from bs4 import BeautifulSoup
from compare_deepseek_direct import summarize_calls

from crawler_service.analytics import accepted_finding, source_supported_finding
from crawler_service.content import normalize, normalize_evidence
from crawler_service.models import Finding
from crawler_service.storage import content_hash, utc_now, write_json


def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def technology_names(record: Finding) -> set[str]:
    match = record.data.get("catalog_match") or {}
    proposal = match.get("proposed_technology") or {}
    return {
        normalize(name)
        for name in [
            record.data.get("technology"),
            match.get("canonical_technology"),
            proposal.get("name"),
        ]
        if isinstance(name, str)
    }


def positive_match(record: Finding, control: dict) -> bool:
    return (
        bool(technology_names(record) & {normalize(name) for name in control["names"]})
        and normalize(record.data.get("company") or "")
        in {normalize(name) for name in control["companies"]}
        and record.data["signal"] in control["signals"]
        and record.data["scope"] in control["scopes"]
        and any(source.page_id == control["page"] for source in record.sources)
        and (
            "job_title" not in control
            or record.data.get("job_title") == control["job_title"]
        )
    )


def audit(root: Path) -> dict:
    controls = read(root / "controls.json")
    experiment = read(root / "experiment.json")
    result: dict = {
        "audited_at": utc_now(),
        "sites": {},
        "integrity_errors": [],
        "limit": "Frozen targeted controls plus automated checks. Manual review remains necessary; not exhaustive recall or an independent verification of company facts.",
    }
    if (
        content_hash((root / "controls.json").read_text(encoding="utf-8"))
        != experiment["controls_sha256"]
    ):
        result["integrity_errors"].append("Controls changed after experiment started")
    if (
        content_hash((root / "technology-catalog.json").read_text(encoding="utf-8"))
        != experiment["catalog_sha256"]
    ):
        result["integrity_errors"].append("Catalog changed after experiment started")
    all_calls = {"deepseek": [], "openrouter": []}
    for site, expectations in controls["sites"].items():
        result["sites"][site] = {}
        pages = read(root / "sources" / site / "pages.json")
        for control in expectations["positive"]:
            page = next(p for p in pages if p["page_id"] == control["page"])
            html = (root / "sources" / site / page["html_file"]).read_text(
                encoding="utf-8"
            )
            text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
            if normalize_evidence(control["evidence"]) not in normalize_evidence(text):
                result["integrity_errors"].append(
                    f"Control quotation missing: {site}/{control['id']}"
                )
        for api in ("deepseek", "openrouter"):
            arm = root / "runs" / site / api
            if not (arm / "manifest.json").is_file():
                result["sites"][site][api] = {"status": "not_started"}
                continue
            manifest = read(arm / "manifest.json")
            calls = [read(p) for p in sorted((arm / "calls").glob("*.json"))]
            all_calls[api].extend(calls)
            metrics = summarize_calls(calls, api)
            metrics["status"] = manifest["status"]
            for page in pages:
                if page["html_file"] is None:
                    continue
                text = (arm / page["html_file"]).read_text(encoding="utf-8")
                if content_hash(text) != page["html_sha256"]:
                    result["integrity_errors"].append(
                        f"Changed HTML: {site}/{api}/{page['page_id']}"
                    )
            if not (arm / "result.json").is_file():
                metrics["error"] = manifest.get("error", "Result not available")
                result["sites"][site][api] = metrics
                continue
            document = read(arm / "result.json")
            records = [
                Finding.model_validate(r) for r in document["technology_signals"]
            ]
            supported = [r for r in records if source_supported_finding(r)]
            accepted = [r for r in records if accepted_finding(r)]
            checks = []
            for control in expectations["positive"]:
                checked = control | {
                    "source_supported_ids": [
                        r.record_id for r in supported if positive_match(r, control)
                    ],
                    "accepted_ids": [
                        r.record_id for r in accepted if positive_match(r, control)
                    ],
                }
                checks.append(checked)
            violations = []
            for control in expectations["negative"]:
                forbidden = {normalize(name) for name in control["names"]}
                companies = {normalize(name) for name in control.get("companies", [])}
                for record in accepted:
                    if (
                        technology_names(record) & forbidden
                        and (
                            not companies
                            or normalize(record.data.get("company") or "") in companies
                        )
                        and (
                            control["forbidden_signals"] == "all"
                            or record.data["signal"] in control["forbidden_signals"]
                        )
                    ):
                        violations.append(
                            {
                                "control_id": control["id"],
                                "record_id": record.record_id,
                                "technology": record.data["technology"],
                                "company": record.data["company"],
                                "signal": record.data["signal"],
                                "reason": control["reason"],
                            }
                        )
            metrics.update(
                extracted_statements=len(read(arm / "extracted-statements.json")),
                accepted_descriptions=document["page_description_validation"][
                    "accepted"
                ],
                accepted_technology_observations=len(accepted),
                source_supported_technology_observations=len(supported),
                accepted_credentials=len(
                    document["accepted_record_ids"]["certifications_compliance"]
                ),
                pending_extraction_page_ids=document["pending_extraction_page_ids"],
                pending_description_ids=document["pending_description_ids"],
                extraction_coverage=document["extraction_coverage"],
                controls_passed=sum(bool(c["accepted_ids"]) for c in checks),
                controls_source_supported=sum(
                    bool(c["source_supported_ids"]) for c in checks
                ),
                controls_total=len(checks),
                controls=checks,
                violations=violations,
                accepted_records=[r.model_dump() for r in accepted],
                catalog_pending_records=[
                    r.model_dump() for r in supported if not accepted_finding(r)
                ],
                accepted_company_context=[
                    r
                    for r in document["page_statements"]
                    if r["data"]["kind"] == "company_context"
                    and (r["data"].get("description_review") or {}).get("status")
                    == "accepted"
                ],
            )
            result["sites"][site][api] = metrics
        first_messages = []
        for api in ("deepseek", "openrouter"):
            path = root / "runs" / site / api / "calls/00001.json"
            if path.exists():
                first_messages.append(read(path)["request"]["messages"])
        if len(first_messages) == 2 and first_messages[0] != first_messages[1]:
            result["integrity_errors"].append(f"Different initial prompts: {site}")
    result["totals"] = {
        api: summarize_calls(calls, api) for api, calls in all_calls.items()
    }
    manual_path = root / "manual-review.json"
    if manual_path.is_file():
        manual = read(manual_path)
        reviewed = {
            "generated_at": utc_now(),
            "method": manual["method"],
            "manual_review_sha256": content_hash(
                manual_path.read_text(encoding="utf-8")
            ),
            "limit": "Derived automatic outputs minus explicit manual holds. Retains neutral mentions and proposed catalog entries; not verified company usage or administrator approval.",
            "sites": {},
        }
        for site, arms in result["sites"].items():
            reviewed["sites"][site] = {}
            for api, metrics in arms.items():
                if "accepted_records" not in metrics:
                    continue
                decisions = manual["sites"][site].get(api, {})
                holds = set(decisions.get("specificity_hold_ids", []))
                accepted = metrics["accepted_records"]
                accepted_ids = {r["record_id"] for r in accepted}
                if not holds <= accepted_ids:
                    raise ValueError(f"Unknown manual technology hold: {site}/{api}")
                retained = [r for r in accepted if r["record_id"] not in holds]
                metrics["retained_after_manual_specificity_holds"] = len(retained)
                reviewed["sites"][site][api] = {
                    "technology_signals": retained,
                    "held_technology_ids": sorted(holds),
                    "held_credential_ids": decisions.get("hold_credential_ids", []),
                    "qualified_control_flags": decisions.get(
                        "qualified_control_flags", []
                    ),
                }
        write_json(root / "reviewed-records.json", reviewed)
    write_json(root / "comparison.json", result)
    shutil.copy2(__file__, root / "audit-harness.py")
    shutil.copy2(
        Path(__file__).with_name("compare_deepseek_direct.py"),
        root / "call-metrics-harness.py",
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    result = audit(parser.parse_args().root)
    print(
        json.dumps(
            {
                "integrity_errors": result["integrity_errors"],
                "sites": {
                    site: {
                        api: {
                            k: m.get(k)
                            for k in [
                                "status",
                                "extracted_statements",
                                "accepted_descriptions",
                                "accepted_technology_observations",
                                "controls_passed",
                                "controls_total",
                                "violations",
                                "pending_extraction_page_ids",
                            ]
                        }
                        for api, m in arms.items()
                    }
                    for site, arms in result["sites"].items()
                },
            },
            indent=2,
        )
    )
