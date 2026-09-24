"""Audit a saved NOVELIC page run against frozen source-derived controls."""

import argparse
import json
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import click

from crawler_service.storage import content_hash, write_json


def key(value: str | None) -> str:
    return re.sub(
        r"[^a-z0-9]",
        "",
        unicodedata.normalize("NFKD", value or "")
        .encode("ascii", "ignore")
        .decode()
        .casefold(),
    )


def usage(run: Path) -> dict:
    stages = defaultdict(Counter)
    missing = []
    calls = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((run / "calls").glob("*.json"))
    ]
    providers, models = Counter(), Counter()
    for call in calls:
        stage = (
            "classification"
            if call["task"].startswith("classify:")
            else "collection_retry"
            if call["task"].endswith("attempt2")
            else "collection_initial"
        )
        counts = stages[stage]
        counts["calls"] += 1
        counts["errors"] += bool(call.get("error"))
        providers[call.get("provider") or call.get("api", "unknown")] += 1
        models[call.get("response_model") or "not_returned"] += 1
        values = call.get("usage")
        if not values:
            missing.append({k: call.get(k) for k in ("call_id", "task", "error")})
            counts["unknown_cost_calls"] += 1
            continue
        counts["responses_with_usage"] += 1
        for field in ("prompt_tokens", "completion_tokens"):
            counts[field] += values.get(field, 0)
        cached = (
            values.get(
                "prompt_cache_hit_tokens",
                values.get("prompt_tokens_details", {}).get("cached_tokens", 0),
            )
            or 0
        )
        counts["cached_input_tokens"] += cached
        counts["reasoning_tokens"] += (
            values.get("completion_tokens_details", {}).get("reasoning_tokens", 0) or 0
        )
        if call["api"] == "deepseek":
            assert call["request"]["model"] == "deepseek-flash", (
                "Dated estimate applies only to the frozen DeepSeek Flash baseline"
            )
            started = datetime.fromisoformat(call["started_at"])
            assert started.date().isoformat() == "2026-09-17", (
                "Verify prices before applying this baseline estimate to another date"
            )
            peak = started.weekday() < 5 and (
                1 <= started.hour < 4 or 6 <= started.hour < 10
            )
            cost = (
                (
                    cached * 0.003
                    + (values["prompt_tokens"] - cached) * 0.15
                    + values["completion_tokens"] * 0.6
                )
                * (2 if peak else 1)
                / 1_000_000
            )
            counts["estimated_cost_usd"] += cost
            counts["accounted_cost_usd"] += cost
        elif values.get("cost") is not None:
            counts["reported_cost_usd"] += values["cost"]
            counts["accounted_cost_usd"] += values["cost"]
        else:
            counts["unknown_cost_calls"] += 1
    total = Counter()
    for counts in stages.values():
        total.update(counts)
    return {
        "totals": dict(total),
        "by_stage": dict(stages),
        "unknown_usage": missing,
        "providers": dict(providers),
        "response_models": dict(models),
        "first_call_at": calls[0]["started_at"] if calls else None,
        "last_call_started_at": calls[-1]["started_at"] if calls else None,
        "cost_basis": "OpenRouter: returned usage.cost. DeepSeek baseline: dated September 17 peak/off-peak estimate. Reasoning is included in completion tokens; unknown usage is excluded, not zero.",
    }


def audit(run: Path, controls_path: Path, mechanical_path: Path) -> dict:
    files = list(run.glob("company-*/page-*.json"))
    pages = {
        page["source"]["page_id"]: page
        for path in files
        if (page := json.loads(path.read_text(encoding="utf-8")))
    }
    result_files = list(run.glob("company-*/result.json"))
    result = (
        json.loads(result_files[0].read_text(encoding="utf-8"))
        if result_files
        else None
    )
    decisions = (
        {d["mention_id"]: d for d in result["technology_classification"]["decisions"]}
        if result
        else {}
    )
    controls = json.loads(controls_path.read_text(encoding="utf-8"))
    mechanical = json.loads(mechanical_path.read_text(encoding="utf-8"))
    checks = controls["technology_checks"] + [
        {
            "page": "mechanical",
            "name": c["name"],
            "aliases": c["aliases"],
            "relationship": c["signal"],
            "scope": "company",
            "actor": c["company"],
        }
        for c in mechanical["expected_technologies"]
    ]
    technology_checks = []
    for check in checks:
        aliases = {key(n) for n in [check["name"], *check.get("aliases", [])]}
        candidates = [
            m
            for m in pages[check["page"]]["technology_mentions"]["mentions"]
            if key(m.get("source_name")) in aliases
        ]
        outcomes = []
        for mention in candidates:
            decision = decisions.get(mention["mention_id"], {})
            classification = decision.get("classification") or {}
            relations = classification.get("relationships", [])
            expected_relation = any(
                r["relationship"] == check["relationship"]
                and r["scope"] == check["scope"]
                and key(r.get("actor")) == key(check["actor"])
                for r in relations
            )
            outcomes.append(
                {
                    "mention_id": mention["mention_id"],
                    "source_name": mention["source_name"],
                    "source_status": mention["status"],
                    "status": decision.get("status"),
                    "disposition": classification.get("disposition"),
                    "relationships": [
                        {
                            k: r[k]
                            for k in (
                                "relationship",
                                "actor",
                                "scope",
                                "subject",
                                "job_title",
                            )
                        }
                        for r in relations
                    ],
                    "expected_relation": expected_relation,
                    "passed": decision.get("status") == "classified"
                    and classification.get("disposition") == "specific_technology"
                    and expected_relation,
                }
            )
        technology_checks.append(
            check
            | {
                "raw_recovered": bool(candidates),
                "source_linked": any(
                    m["status"] == "source_linked" for m in candidates
                ),
                "passed": any(o["passed"] for o in outcomes),
                "outcomes": outcomes,
            }
        )
    scope_reviews = [
        {
            "page": "mechanical",
            "name": check["name"],
            "accepted": True,
            "reason": "Source explicitly describes NOVELIC's mechanical engineering team and its tools/skills. Team advertised_expertise is a valid narrower scope than the frozen company-scope expectation. Original control remains unchanged.",
        }
        for check in technology_checks
        if check["page"] == "mechanical"
        and check["name"] in {"Creo Parametric", "OpenFOAM", "MathCAD", "Windchill"}
        and any(
            outcome["status"] == "classified"
            and outcome["disposition"] == "specific_technology"
            and any(
                relation["relationship"] == "advertised_expertise"
                and relation["scope"] == "team"
                and key(relation["actor"]) == "novelic"
                for relation in outcome["relationships"]
            )
            for outcome in check["outcomes"]
        )
    ]
    jobs = []
    for i, expected in enumerate(controls["jobs"]):
        page = pages[f"job-{i:02}"]
        matches = [
            f
            for f in page["data"]["records"]["jobs"]
            if key(f["data"]["title"]) == key(expected["title"])
        ]
        jobs.append(
            expected
            | {
                "recovered": bool(matches),
                "source_matched": any(
                    f["evidence_status"] == "source_matched" for f in matches
                ),
                "returned_urls": [f["data"]["job_url"] for f in matches],
            }
        )
    people = []
    for name in controls["management"]:
        matches = [
            f
            for f in pages["management"]["data"]["records"]["people"]
            if key(f["data"]["name"]) == key(name)
        ]
        people.append(
            {
                "name": name,
                "recovered": bool(matches),
                "source_matched": any(
                    f["evidence_status"] == "source_matched" for f in matches
                ),
                "roles": [f["data"]["role"] for f in matches],
                "expected_source_passage": controls["management_role_passages"][name],
            }
        )
    objective_counts = {}
    issues = defaultdict(Counter)
    for objective in pages["home"]["data"]["records"]:
        records = [f for p in pages.values() for f in p["data"]["records"][objective]]
        objective_counts[objective] = {
            "raw_records": len(records),
            "source_matched": sum(
                f["evidence_status"] == "source_matched" for f in records
            ),
        }
        for record in records:
            issues[objective].update(
                set(issue for source in record["sources"] for issue in source["issues"])
            )
    integrity = []
    for page in pages.values():
        for representation, capture in page["captures"].items():
            if content_hash(capture["content"]) != capture["sha256"]:
                integrity.append([page["source"]["page_id"], representation])
        for section in page["source_sections"]:
            if content_hash(section["text"]) != section["text_sha256"]:
                integrity.append(section["section_id"])
    mentions = [
        m | {"page": p["source"]["page_id"]}
        for p in pages.values()
        for m in p["technology_mentions"]["mentions"]
    ]
    cookies, generic = [], []
    for mention in mentions:
        name = key(mention.get("source_name"))
        if name in {
            key(n)
            for n in [
                "Microsoft Clarity",
                "Google Analytics",
                "Google AdSense",
                "Facebook Pixel",
            ]
        } or name in {
            key(n)
            for n in [
                "XML",
                "JSON",
                "FPGA",
                "AI",
                "CAD",
                "CFD",
                "MCAP",
                "Protobuf",
                "FlatBuffers",
            ]
        }:
            decision = decisions.get(mention["mention_id"], {})
            item = {
                k: mention[k] for k in ("page", "source_name", "mention_id", "status")
            }
            item["decision"] = decision
            (
                cookies
                if name
                in {
                    key(n)
                    for n in [
                        "Microsoft Clarity",
                        "Google Analytics",
                        "Google AdSense",
                        "Facebook Pixel",
                    ]
                }
                else generic
            ).append(item)
    output = {
        "interpretation": "Selected controls and targeted failure checks only; these counts do not establish general precision, recall or site coverage. Inspect semantic failures and review-held evidence separately.",
        "scope": "Source-derived selected controls, not general precision/recall. Guided pages; no autonomous coverage claim.",
        "complete": result is not None,
        "result_file": str(result_files[0]) if result_files else None,
        "processing_status": result.get("processing_status") if result else None,
        "usage": usage(run),
        "pages": len(pages),
        "page_processing": [
            {
                "page": n,
                "status": p["processing"]["response"]["status"],
                "attempts": [
                    {"error": a.get("error"), "schema_errors": a.get("schema_errors")}
                    for a in p["processing"]["response"]["attempts"]
                ],
                "link_errors": p["processing"]["link_errors"],
            }
            for n, p in sorted(pages.items())
        ],
        "jobs": jobs,
        "people": people,
        "technology_checks": technology_checks,
        "manual_scope_reviews": scope_reviews,
        "control_totals": {
            "jobs_recovered": sum(c["recovered"] for c in jobs),
            "job_details_source_matched": sum(c["source_matched"] for c in jobs),
            "people_recovered": sum(1 for c in people if c["recovered"]),
            "management_people_source_matched": sum(
                1 for c in people if c["source_matched"]
            ),
            "technology_controls": len(technology_checks),
            "technology_raw_recovered": sum(
                c["raw_recovered"] for c in technology_checks
            ),
            "technology_source_linked": sum(
                c["source_linked"] for c in technology_checks
            ),
            "technology_classified_correct_relation": sum(
                c["passed"] for c in technology_checks
            ),
        },
        "objective_counts": objective_counts,
        "record_issues": dict(issues),
        "integrity_issues": integrity,
        "mention_statuses": dict(Counter(m["status"] for m in mentions)),
        "decision_statuses": dict(Counter(d["status"] for d in decisions.values())),
        "dispositions": dict(
            Counter(
                (d.get("classification") or {}).get("disposition", "missing")
                for d in decisions.values()
            )
        ),
        "catalog_statuses": dict(
            Counter(d["catalog_lookup"]["status"] for d in decisions.values())
        ),
        "links": {
            "occurrences": sum(len(p["links"]) for p in pages.values()),
            "distinct_urls": len(
                {link["url"] for p in pages.values() for link in p["links"]}
            ),
            "assessments": dict(
                Counter(
                    link["assessment_status"]
                    for p in pages.values()
                    for link in p["links"]
                )
            ),
        },
        "cookie_mentions": cookies,
        "cookie_attribution_failures": [
            {
                "page": item["page"],
                "source_name": item["source_name"],
                "mention_id": item["mention_id"],
                "reason": "OneAssessment cookie inventory does not establish NOVELIC company technology use.",
            }
            for item in cookies
            if any(
                key(relation.get("actor")) == "novelic"
                and relation["relationship"] == "stated_use"
                for relation in (item["decision"].get("classification") or {}).get(
                    "relationships", []
                )
            )
        ],
        "generic_mentions": generic,
        "manual_policy_failures": [
            {
                "page": item["page"],
                "source_name": item["source_name"],
                "mention_id": item["mention_id"],
                "reason": "Format, protocol or generic technical category classified as specific technology; source-level manual review required.",
            }
            for item in generic
            if (item["decision"].get("classification") or {}).get("disposition")
            == "specific_technology"
        ],
        "job_pages_with_company_certification_records": [
            name
            for name, page in pages.items()
            if name.startswith("job-")
            and page["data"]["records"]["certifications_compliance"]
        ],
        "classification_batches": [
            {
                "batch": b["batch"],
                "mentions": len(b["mention_ids"]),
                "status": b["response"]["status"],
                "error": b["response"].get("error"),
                "attempts": len(b["response"]["attempts"]),
                "rejections": len(b["rejections"]),
            }
            for b in result["technology_classification"]["batches"]
        ]
        if result
        else [],
    }
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--controls", type=Path, required=True)
    parser.add_argument("--mechanical-controls", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.run, args.controls, args.mechanical_controls)
    write_json(args.output, result)
    click.echo(
        json.dumps(
            {
                key: result[key]
                for key in [
                    "complete",
                    "processing_status",
                    "usage",
                    "control_totals",
                    "mention_statuses",
                    "decision_statuses",
                    "dispositions",
                    "links",
                    "integrity_issues",
                ]
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
