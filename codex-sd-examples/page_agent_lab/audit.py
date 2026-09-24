"""Offline accounting and selected controls; semantic precision needs source review."""

import argparse
import json
import re
import statistics
import unicodedata
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from crawler_service.storage import content_hash, write_json

from page_agent_lab.fixtures import read


def norm(value: object) -> str:
    return re.sub(
        r"[^\w+#]", "", unicodedata.normalize("NFKC", str(value or "")).casefold()
    )


def matches(record: dict, rules: dict) -> bool:
    for field, expected in rules.items():
        if field in {"ownership_pair", "ownership_direction"}:
            target = norm(expected[0])
            subject, obj = norm(record.get("subject")), norm(record.get("object"))
            forward = (
                record.get("relationship") == "parent_of"
                and target in obj
                and "handelsbanken" in subject
            )
            reverse = (
                record.get("relationship") == "subsidiary_of"
                and target in subject
                and "handelsbanken" in obj
            )
            if not (forward or reverse):
                return False
        elif field.endswith("_contains"):
            if not any(
                norm(value) in norm(record.get(field.removesuffix("_contains")))
                for value in expected
            ):
                return False
        elif norm(record.get(field)) not in {norm(value) for value in expected}:
            return False
    return True


def usage(records: list[dict]) -> dict:
    metered = [c for c in records if isinstance(c.get("usage"), dict)]
    estimate = 0
    for call in metered:
        value = call["usage"]
        stamp = datetime.fromisoformat(call["started_at"]).astimezone(UTC)
        peak = stamp.weekday() < 5 and (1 <= stamp.hour < 4 or 6 <= stamp.hour < 10)
        hit = value.get("prompt_cache_hit_tokens", 0)
        miss = value.get(
            "prompt_cache_miss_tokens", value.get("prompt_tokens", 0) - hit
        )
        estimate += (
            (hit * 0.003 + miss * 0.15 + value.get("completion_tokens", 0) * 0.6)
            * (2 if peak else 1)
            / 1_000_000
        )
    return {
        "calls": len(records),
        "responses_with_usage": len(metered),
        "prompt_tokens": sum(c["usage"].get("prompt_tokens", 0) for c in metered),
        "cache_hit_tokens": sum(
            c["usage"].get("prompt_cache_hit_tokens", 0) for c in metered
        ),
        "completion_tokens": sum(
            c["usage"].get("completion_tokens", 0) for c in metered
        ),
        "reasoning_tokens": sum(
            c["usage"].get("completion_tokens_details", {}).get("reasoning_tokens", 0)
            for c in metered
        ),
        "estimated_usd": estimate,
        "unknown_cost_calls": len(records) - len(metered),
        "pricing_source": "https://api-docs.deepseek.com/quick_start/pricing/",
        "pricing_checked": "2026-09-16",
        "median_response_seconds": statistics.median(
            c["elapsed_seconds"] for c in metered
        )
        if metered
        else None,
        "response_models": sorted({str(c.get("response_model")) for c in metered}),
        "errors": [
            {"task": c["task"], "error": c["error"]} for c in records if c.get("error")
        ],
        "length_stops": sum(c.get("finish_reason") == "length" for c in records),
    }


def audit_arm(root: Path, mode: str, controls: dict) -> dict:
    manifest = read(root / mode / "manifest.json")
    results = {
        p.name: read(root / mode / f"{p.name}.json")
        for p in (root / "fixtures").iterdir()
    }
    checks = []
    for control in controls["positive"]:
        result = results[control["page"]]["data"]
        items = result["records"][control["objective"]]
        hits = [item for item in items if matches(item["data"], control["identity"])]
        rejected_hits = [
            item
            for item in result["rejections"]
            if item["objective"] == control["objective"]
            and isinstance(item["record"], dict)
            and matches(item["record"], control["identity"])
        ]
        checks.append(
            control
            | {
                "identity_found": bool(hits),
                "raw_identity_found": bool(hits or rejected_hits),
                "strict_match": any(
                    matches(item["data"], control["qualifiers"]) for item in hits
                ),
                "source_checked": any(
                    item["evidence_status"] == "source_matched" for item in hits
                ),
                "record_ids": [item["record_id"] for item in hits],
                "route_decision": result["processing"]["decisions"].get(
                    control["objective"]
                ),
            }
        )
    negative_checks = []
    for control in controls["negative"]:
        data = results[control["page"]]["data"]
        all_records = [r["data"] for r in data["records"][control["objective"]]] + [
            r["record"]
            for r in data["rejections"]
            if r["objective"] == control["objective"] and isinstance(r["record"], dict)
        ]
        negative_checks.append(
            control
            | {
                "evaluated": data["coverage"][control["objective"]]["status"]
                in {"processed", "partial"},
                "violations": [
                    r for r in all_records if matches(r, control["forbidden"])
                ],
            }
        )
    links = []
    for control in controls["link_controls"]:
        items = results[control["page"]]["links"]
        wanted = [
            item
            for item in items
            if ("kind" not in control or item["kind"] == control["kind"])
            and ("anchor" not in control or item["anchor_text"] == control["anchor"])
            and (
                "url_contains" not in control
                or control["url_contains"] in (item["url"] or "")
            )
        ]
        cookie = [
            item["assessment"]["priority"]
            for item in items
            if item["assessment"]
            and any(
                token in (item["url"] or "").lower()
                for token in ["cookie", "privacy", "personuppgifter"]
            )
        ]
        priorities = [
            item["assessment"]["priority"] for item in wanted if item["assessment"]
        ]
        links.append(
            control
            | {
                "observed": len(wanted),
                "assessed": len(priorities),
                "priorities": priorities,
                "all_above_routine_policy": bool(priorities)
                and bool(cookie)
                and min(priorities) > max(cookie),
            }
        )
    calls = [read(p) for p in sorted((root / mode / "calls").glob("*.json"))]
    all_findings = [
        r
        for result in results.values()
        for records in result["data"]["records"].values()
        for r in records
    ]
    routes = []
    for page_id, objective in sorted(
        {(c["page"], c["objective"]) for c in controls["positive"]}
    ):
        data = results[page_id]["data"]
        routes.append(
            {
                "page": page_id,
                "objective": objective,
                "dispatched": data["coverage"][objective]["status"] != "not_selected",
                "raw_decision": data["processing"]["decisions"].get(objective),
                "override": data["processing"]["routing_overrides"].get(objective),
            }
        )
    return {
        "status": manifest["status"],
        "pages": len(results),
        "wall_seconds": manifest["wall_seconds"],
        "records": len(all_findings),
        "source_matched_records": sum(
            r["evidence_status"] == "source_matched" for r in all_findings
        ),
        "schema_rejected_records": sum(
            len(r["data"]["rejections"]) for r in results.values()
        ),
        "identity_controls": sum(c["identity_found"] for c in checks),
        "strict_controls": sum(c["strict_match"] for c in checks),
        "source_checked_controls": sum(c["source_checked"] for c in checks),
        "control_count": len(checks),
        "checks": checks,
        "negative_checks": negative_checks,
        "negative_passed": sum(
            check["evaluated"] and not check["violations"] for check in negative_checks
        ),
        "negative_evaluated": sum(check["evaluated"] for check in negative_checks),
        "negative_count": len(negative_checks),
        "link_controls": links,
        "required_routes": routes,
        "assessed_links": sum(
            item["assessment"] is not None
            for result in results.values()
            for item in result["links"]
        ),
        "observed_links": sum(len(result["links"]) for result in results.values()),
        "full_schema_failures": sum(
            bool(response.get("schema_errors") or response.get("error"))
            for result in results.values()
            for response in result["data"]["processing"]["responses"].values()
        ),
        "processing_statuses": dict(
            Counter(
                status["status"]
                for result in results.values()
                for status in result["data"]["coverage"].values()
            )
        ),
        "usage": usage(calls),
    }


def audit(root: Path) -> dict:
    experiment = read(root / "experiment.json")
    if experiment["status"] != "finished":
        raise ValueError("The experiment has not finished")
    for name, expected in experiment["code_sha256"].items():
        if content_hash((root / name).read_text(encoding="utf-8")) != expected:
            raise ValueError("Frozen implementation changed")
    controls = read(root / "controls.json")
    if (
        content_hash((root / "controls.json").read_text(encoding="utf-8"))
        != experiment["controls_sha256"]
    ):
        raise ValueError("Controls changed after freezing")
    for fixture in experiment["pages"]:
        directory = root / "fixtures" / fixture["fixture_id"]
        for name, field in [
            ("page.html", "html_sha256"),
            ("link-page.html", "link_html_sha256"),
            ("input.json", "input_sha256"),
        ]:
            if (
                content_hash((directory / name).read_text(encoding="utf-8"))
                != fixture[field]
            ):
                raise ValueError("Frozen input changed")
    modes = experiment.get("modes", ["one_pass", "routed"])
    report = {
        "method": "Selected source-read controls. Schema/source-presence checks are not semantic correctness or exhaustive precision/recall. Single run per variant; no autonomous crawl.",
        "arms": {mode: audit_arm(root, mode, controls) for mode in modes},
        "routing_diagnostic": (
            read(root / "routing_diagnostic/manifest.json")
            if "routed" in modes
            else {"status": "not_applicable", "calls": 0}
        ),
    }
    write_json(root / "comparison.json", report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    print(json.dumps(audit(parser.parse_args().root), ensure_ascii=False, indent=2))
