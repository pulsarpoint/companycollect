"""Publish a reviewed comparison; retain automatic outputs and explicit manual holds."""

import argparse
import json
from pathlib import Path

from company_research.analytics import accepted_finding, source_supported_finding
from company_research.models import Finding
from company_research.storage import content_hash, utc_now, write_json


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def audit(data: Path):
    replay = data / "attribution-replay-v0141"
    recovery = data / "attribution-recovery-v0142"
    focused = data / "attribution-recovery-v0143"
    output = data / "attribution-audit-v0143"
    output.mkdir(exist_ok=True)
    dmc = read(recovery / "result.json")
    latest = read(focused / "result.json")
    replaced = {r["record_id"] for r in latest["page_statements"]}
    dmc["technology_signals"] = [
        r
        for r in dmc["technology_signals"]
        if not set(r["data"]["statement_ids"]) & replaced
    ] + latest["technology_signals"]
    manual_holds = {
        "AVEVA": "The source names a broad vendor/software label without identifying an individual application. Hold for a more specific source label; do not guess an AVEVA product.",
        "Beckhoff Motion Control": "The source names a vendor's motion-control service/category, without an identifiable named product family. Hold rather than treating the category as a specific technology.",
    }
    summary: dict = {
        "created_at": utc_now(),
        "package_version": "0.14.3",
        "page_fetches": 0,
        "independently_verified": False,
        "cases": {},
        "phases": {},
        "integrity_errors": [],
    }
    for name, result in [
        ("dmc", dmc),
        ("thoughtbot", read(replay / "thoughtbot/result.json")),
        ("mcap", read(replay / "mcap/result.json")),
    ]:
        records = [Finding.model_validate(r) for r in result["technology_signals"]]
        supported = [r for r in records if source_supported_finding(r)]
        automatic = [r for r in records if accepted_finding(r)]
        held = [
            r
            for r in automatic
            if name == "dmc" and r.data["technology"] in manual_holds
        ]
        accepted = [r for r in automatic if r not in held]
        pending_catalog = [r for r in supported if not accepted_finding(r)]
        document = {
            "accepted_records": [r.model_dump() for r in accepted],
            "catalog_pending_records": [r.model_dump() for r in pending_catalog],
            "manually_held_records": [
                {"record": r.model_dump(), "reason": manual_holds[r.data["technology"]]}
                for r in held
            ],
            "unaccepted_records": [
                r.model_dump() for r in records if not source_supported_finding(r)
            ],
            "pending_description_ids": result["pending_description_ids"],
            "source_files": [str(replay / name / "result.json")]
            if name != "dmc"
            else [str(recovery / "result.json"), str(focused / "result.json")],
            "note": "Derived manual audit, not a backend submission. Original automatic findings remain unchanged.",
        }
        write_json(output / f"{name}.json", document)
        summary["cases"][name] = {
            "source_reviewed": len(supported),
            "automatically_accepted": len(automatic),
            "manual_holds": len(held),
            "accepted_after_manual_audit": len(accepted),
            "catalog_pending": len(pending_catalog),
            "pending_descriptions": len(result["pending_description_ids"]),
        }
        if any(r.data["signal"] in {"offers", "explicitly_not_used"} for r in accepted):
            summary["integrity_errors"].append(
                f"Unexpected relationship in audited {name} output"
            )
    plausible = read(replay / "plausible/result.json")
    write_json(
        output / "plausible.json",
        plausible
        | {
            "manual_source_audit": "Saved homepage identifies Plausible Analytics, since 2018, a team of 10, and Uku/Marko as co-founders. Retain first names only; no external factual verification or inferred full identity."
        },
    )
    summary["cases"]["plausible"] = {
        "repaired_records": sum(r["accepted"] for r in plausible["records"]),
        "total": 4,
    }
    controls = read(replay / "comparison.json")["cases"]
    summary["controls"] = {
        "thoughtbot_prior_valid_relationships": 12,
        "thoughtbot_retained_before_catalog": len(
            controls["thoughtbot"]["retained_controls_before_catalog"]
        ),
        "thoughtbot_retained_after_catalog": len(
            controls["thoughtbot"]["retained_controls"]
        ),
        "mcap_both_relationships_preserved": controls["mcap"][
            "both_relationships_preserved"
        ],
        "notes": "One earlier Rails past-use observation was omitted; expertise remained. MCAP preferred experience was omitted while role use remained. Do not interpret precision improvements as complete recall.",
    }
    for phase in [data / "attribution-replay-v014", replay, recovery, focused]:
        paths = (
            list(phase.glob("*/calls/*.json"))
            if "replay" in phase.name
            else list(phase.glob("calls/*.json"))
        )
        calls = [read(path) for path in paths]
        summary["phases"][phase.name] = {
            "calls": len(calls),
            "known_cost_usd": sum(c.get("usage", {}).get("cost") or 0 for c in calls),
            "unknown_cost_calls": sum(
                c.get("usage", {}).get("cost") is None for c in calls
            ),
            "manifest_sha256": content_hash(
                (phase / "manifest.json").read_text(encoding="utf-8")
            ),
        }
        manifest = read(phase / "manifest.json")
        cases = manifest.get("cases") or {"": manifest}
        for name, case in cases.items():
            for page in case["pages"]:
                if (
                    content_hash(
                        (phase / name / page["html_file"]).read_text(encoding="utf-8")
                    )
                    != page["html_sha256"]
                ):
                    summary["integrity_errors"].append(
                        f"Changed HTML: {phase.name}/{name}/{page['page_id']}"
                    )
    source = Path(__file__).parents[1] / "src/company_research"
    for path in (focused / "implementation").glob("*.py"):
        if path.read_bytes() != (source / path.name).read_bytes():
            summary["integrity_errors"].append(
                f"Final implementation differs: {path.name}"
            )
    summary["total_usage"] = {
        key: sum(phase[key] for phase in summary["phases"].values())
        for key in ["calls", "known_cost_usd", "unknown_cost_calls"]
    }
    write_json(output / "summary.json", summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("data"))
    audit(parser.parse_args().data)
