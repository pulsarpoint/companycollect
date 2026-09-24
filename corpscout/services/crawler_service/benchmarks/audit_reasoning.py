"""Audit frozen low/high outputs without model calls or changes to source results."""

import argparse
import json
import re
import statistics
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

from crawler_service.analytics import accepted_finding
from crawler_service.content import HtmlWindow, source_finding
from crawler_service.llm import parse_model_json
from crawler_service.models import (
    RECORD_TYPES,
    CandidateAssessment,
    Extraction,
    Finding,
    Selection,
    SiteClassification,
)
from crawler_service.storage import utc_now, write_json


def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def norm(name: str) -> str:
    return re.sub(r"[^\w+#]", "", unicodedata.normalize("NFKC", name or "").casefold())


def calls(root: Path) -> list[dict]:
    return [read(path) for path in sorted((root / "calls").glob("*.json"))]


def usage(records: list[dict]) -> dict:
    complete = [record for record in records if "usage" in record]
    duration = [record["elapsed_seconds"] for record in complete]
    estimated_cost = 0.0
    for call in complete:
        value = call["usage"]
        stamp = datetime.fromisoformat(call["started_at"])
        peak = stamp.weekday() < 5 and (1 <= stamp.hour < 4 or 6 <= stamp.hour < 10)
        cost = (
            value.get("prompt_cache_hit_tokens", 0) * 0.003
            + value.get("prompt_cache_miss_tokens", value.get("prompt_tokens", 0))
            * 0.15
            + value.get("completion_tokens", 0) * 0.60
        ) / 1_000_000
        estimated_cost += cost * (2 if peak else 1)
    return {
        "calls": len(records),
        "responses_with_usage": len(complete),
        "prompt_tokens": sum(c["usage"].get("prompt_tokens", 0) for c in complete),
        "cache_hit_tokens": sum(
            c["usage"].get("prompt_cache_hit_tokens", 0) for c in complete
        ),
        "completion_tokens": sum(
            c["usage"].get("completion_tokens", 0) for c in complete
        ),
        "reasoning_tokens": sum(
            c["usage"].get("completion_tokens_details", {}).get("reasoning_tokens", 0)
            for c in complete
        ),
        "median_response_seconds": statistics.median(duration) if duration else None,
        "sum_response_seconds": sum(duration),
        "estimated_cost_usd": estimated_cost,
        "calls_without_usage": len(records) - len(complete),
        "errors": [
            {"call_id": c["call_id"], "task": c["task"], "error": c["error"]}
            for c in records
            if "error" in c
        ],
        "length_stops": sum(c.get("finish_reason") == "length" for c in records),
        "response_models": sorted({c["response_model"] for c in complete}),
    }


def job_arm(root: Path, controls: list[dict]) -> dict:
    manifest = read(root / "manifest.json")
    result = read(root / "result.json")
    records = [
        Finding.model_validate(record) for record in result["technology_signals"]
    ]
    accepted = [record for record in records if accepted_finding(record)]
    aliases = {
        "Microsoft Entra ID": ["Entra ID"],
        "Microsoft Exchange": ["Exchange"],
        "Microsoft SQL Server": ["MS SQL", "SQL Server"],
        "SQL Server": ["MS SQL", "Microsoft SQL Server"],
        "Azure Databricks": ["Microsoft Azure Databricks"],
        "DB2": ["IBM DB2"],
        "Jakarta EE": ["JakartaEE"],
    }
    checks = []
    for control in controls:
        names = {
            norm(name)
            for name in [control["technology"], *aliases.get(control["technology"], [])]
        }
        hits = [
            record
            for record in accepted
            if norm(record.data["technology"]) in names
            and any(source.page_id == control["page_id"] for source in record.sources)
        ]
        checks.append(
            control
            | {
                "accepted": bool(hits),
                "record_ids": [r.record_id for r in hits],
                "signals": sorted({r.data["signal"] for r in hits}),
                "strict_signal_scope_match": any(
                    r.data["signal"] in control["acceptable_signals"]
                    and r.data["scope"] in control["expected_scope"]
                    for r in hits
                ),
            }
        )
    names = {
        norm(
            (r.data.get("catalog_match") or {}).get("canonical_technology")
            or r.data["technology"]
        )
        for r in accepted
    }
    grouped_calls = defaultdict(list)
    for call in calls(root):
        grouped_calls[call["task"].split(":")[0]].append(call)
    return {
        "status": manifest["status"],
        "page_count": manifest["pages"],
        "wall_seconds": (
            datetime.fromisoformat(manifest["finished_at"])
            - datetime.fromisoformat(manifest["started_at"])
        ).total_seconds(),
        "statements": len(result["page_statements"]),
        "observations": len(records),
        "accepted_observations": len(accepted),
        "distinct_accepted_names": len(names),
        "accepted_signals": dict(Counter(r.data["signal"] for r in accepted)),
        "accepted_catalog_status": dict(
            Counter(
                (r.data.get("catalog_match") or {}).get("status", "none")
                for r in accepted
            )
        ),
        "accepted_controls": sum(1 for check in checks if check["accepted"]),
        "strict_controls": sum(
            1 for check in checks if check["strict_signal_scope_match"]
        ),
        "controls": checks,
        "pending_statement_ids": result["pending_statement_ids"],
        "pending_description_ids": result["pending_description_ids"],
        "certification_observations": result["certifications_compliance"],
        "errors": result["errors"],
        "usage": usage(calls(root)),
        "usage_by_task": {
            task: usage(values) for task, values in grouped_calls.items()
        },
        "accepted_records": [r.model_dump() for r in accepted],
    }


def content(call: dict):
    response = call.get("response", {})
    choices = response.get("choices") or []
    if not choices:
        return None
    raw = choices[0].get("message", {}).get("content")
    if not isinstance(raw, str):
        return None
    try:
        return parse_model_json(raw)[0]
    except ValueError:
        return None


def repeated_link_contexts(values: list[dict]) -> dict:
    groups = defaultdict(list)
    fields = (
        "source_domain",
        "url",
        "anchor_text",
        "section_heading",
        "surrounding_text",
        "page_region",
    )
    for link in values:
        groups[tuple(link[field] for field in fields)].append(link)
    repeated = [items for items in groups.values() if len(items) > 1]
    inconsistent = [
        items
        for items in repeated
        if len({(item.get("assessment") or {}).get("relationship") for item in items})
        > 1
    ]
    return {
        "method": "Same source registrable domain, destination URL, anchor text, heading, surrounding text and page region. Other supplied fields and source page URL may differ; diagnostic consistency, not correctness.",
        "repeated_groups": len(repeated),
        "inconsistent_groups": len(inconsistent),
        "occurrences_in_inconsistent_groups": sum(len(items) for items in inconsistent),
    }


def decision_metrics(document, task: str, payload: dict) -> dict:
    schema = (
        SiteClassification
        if task == "site_classification"
        else (Selection if task == "link_assessment" else Extraction)
    )
    schema_valid = True
    try:
        schema.model_validate(document)
    except ValueError:
        schema_valid = False
    result = {"schema_valid": schema_valid, "json_document": isinstance(document, dict)}
    if not isinstance(document, dict):
        return result
    if task == "site_classification":
        result["classification"] = document
    elif task == "link_assessment":
        supplied = {c["candidate_id"]: c for c in payload["candidates"]}
        values = document.get("assessments", [])
        ids = Counter(
            item.get("candidate_id") for item in values if isinstance(item, dict)
        )
        valid = []
        for item in values:
            try:
                parsed = CandidateAssessment.model_validate(item)
            except ValueError:
                continue
            if parsed.candidate_id not in supplied or ids[parsed.candidate_id] != 1:
                continue
            valid.append(
                parsed.model_dump() | {"url": supplied[parsed.candidate_id]["url"]}
            )
        result.update(
            expected_candidates=len(supplied),
            valid_assessments=valid,
            invalid_or_missing_candidates=len(supplied) - len(valid),
        )
    else:
        html = payload["cleaned_html"]
        window = HtmlWindow(
            payload["_window_start"], payload["_window_start"] + len(html), html
        )
        page = payload["_page"]
        records = []
        invalid = Counter()
        for objective, record_schema in RECORD_TYPES.items():
            values = document.get(objective, [])
            if not isinstance(values, list):
                invalid[objective] += 1
                continue
            for item in values:
                try:
                    parsed = record_schema.model_validate(item).model_dump()
                except ValueError:
                    invalid[objective] += 1
                    continue
                finding = source_finding(objective, parsed, page=page, window=window)
                records.append(finding.model_dump() | {"objective": objective})
        result.update(
            records=records,
            invalid_record_counts=dict(invalid),
            source_matched_counts=dict(
                Counter(
                    r["objective"]
                    for r in records
                    if r["evidence_status"] == "source_matched"
                )
            ),
            needs_review_counts=dict(
                Counter(
                    r["objective"]
                    for r in records
                    if r["evidence_status"] != "source_matched"
                )
            ),
        )
    return result


def main(root: Path) -> None:
    for name in ("low", "high", "decisions-high", "links-high"):
        if read(root / name / "manifest.json")["status"] != "finished":
            raise ValueError(f"Run has not finished: {name}")
    controls = [
        c
        for name in ("technology-controls.json", "technology-controls-page2.json")
        for c in read(root / name)["controls"]
    ]
    integrity = []
    configs = {
        effort: {
            key: value
            for key, value in read(root / effort / "manifest.json")["config"].items()
            if key != "reasoning_effort"
        }
        for effort in ("low", "high")
    }
    if configs["low"] != configs["high"]:
        integrity.append("Different paired job configuration beyond effort")
    for name in ("pages.json", "technology-catalog.json"):
        if read(root / "low" / name) != read(root / "high" / name):
            integrity.append(f"Different paired {name}")
    arms = {effort: job_arm(root / effort, controls) for effort in ("low", "high")}
    initial = {}
    for effort in ("low", "high"):
        initial[effort] = {
            c["task"]: c["request"]["messages"]
            for c in calls(root / effort)
            if c["task"].startswith("page_statements:") and c["task"].endswith(":0:0")
        }
    if initial["low"] != initial["high"]:
        integrity.append("Different initial job prompts")
    replay_root = root / "decisions-high"
    originals = read(replay_root / "baseline-calls.json")
    crawl = root.parent / "handelsbanken-20260916-full/crawl"
    pages = {page["page_id"]: page for page in read(crawl / "result.json")["pages"]}
    replayed = {
        int(c["task"].split(":")[0].removeprefix("original-")): c
        for c in calls(replay_root)
    }
    comparisons = []
    for original in originals:
        high = replayed[original["call_id"]]
        if {k: v for k, v in high["request"].items() if k != "reasoning_effort"} != {
            k: v for k, v in original["request"].items() if k != "reasoning_effort"
        }:
            integrity.append(f"Changed replay request {original['call_id']}")
        payload = json.JSONDecoder().raw_decode(
            original["request"]["messages"][1]["content"].rsplit("INPUT DATA:\n", 1)[1]
        )[0]
        if original["task"].startswith("extract:"):
            page = pages[original["task"].split(":")[1]]
            full_html = (crawl / page["html_file"]).read_text(encoding="utf-8")
            start = full_html.find(payload["cleaned_html"])
            if start < 0:
                raise ValueError(f"Original HTML window missing: {original['call_id']}")
            payload.update(_page=page, _window_start=start)
        comparisons.append(
            {
                "original_call_id": original["call_id"],
                "task": original["task"],
                "low": decision_metrics(content(original), original["task"], payload),
                "high": decision_metrics(content(high), original["task"], payload),
            }
        )
    link_root = root / "links-high"
    low_links = {
        link["link_id"]: link
        for link in read(link_root / "baseline-external-links.json")
    }
    high_links = {
        link["link_id"]: link for link in read(link_root / "external-links.json")
    }
    if set(low_links) != set(high_links):
        integrity.append("External occurrence ID sets differ")
    link_changes = []
    for link_id, low in low_links.items():
        high = high_links[link_id]
        fields = ("relationship", "basis", "related_entity_name")
        changed = {
            field: {
                "low": (low.get("assessment") or {}).get(field),
                "high": (high.get("assessment") or {}).get(field),
            }
            for field in fields
            if (low.get("assessment") or {}).get(field)
            != (high.get("assessment") or {}).get(field)
        }
        if changed or low["assessment_status"] != high["assessment_status"]:
            link_changes.append(
                {
                    "link_id": link_id,
                    "url": low["url"],
                    "source_url": low["source_url"],
                    "context": low["surrounding_text"],
                    "changed": changed,
                    "low": low.get("assessment"),
                    "high": high.get("assessment"),
                    "low_status": low["assessment_status"],
                    "high_status": high["assessment_status"],
                }
            )
    original_links_root = (
        root.parent / "handelsbanken-20260916-review/external-link-review"
    )
    low_link_calls = {c["task"]: c for c in calls(original_links_root)}
    for high in calls(link_root):
        low = low_link_calls.get(high["task"])
        if low is None or high["request"]["messages"] != low["request"]["messages"]:
            integrity.append(f"Different external-link prompt: {high['task']}")
    report = {
        "created_at": utc_now(),
        "status": "complete",
        "model": "deepseek-flash",
        "control_count": len(controls),
        "integrity_errors": integrity,
        "jobs": arms,
        "decisions": comparisons,
        "decision_usage": {"low": usage(originals), "high": usage(calls(replay_root))},
        "external_links": {
            "count": len(low_links),
            "changes": link_changes,
            "relationship_changes": sum(
                "relationship" in change["changed"] for change in link_changes
            ),
            "arms": {
                name: {
                    "statuses": dict(
                        Counter(x["assessment_status"] for x in values.values())
                    ),
                    "relationships": dict(
                        Counter(
                            (x.get("assessment") or {}).get("relationship", "missing")
                            for x in values.values()
                        )
                    ),
                    "bases": dict(
                        Counter(
                            (x.get("assessment") or {}).get("basis", "missing")
                            for x in values.values()
                        )
                    ),
                    "repeated_context_consistency": repeated_link_contexts(
                        list(values.values())
                    ),
                    "usage": usage(calls(path)),
                }
                for name, values, path in (
                    ("low", low_links, original_links_root),
                    ("high", high_links, link_root),
                )
            },
        },
        "limitations": [
            "One fresh job run per effort; no statistical claim. Historical low job output is retained separately.",
            "Fixed decision replays preserve original low-run context and any original corrective feedback; this is not a new autonomous crawl.",
            "Source matching checks text presence/attribution, not semantic truth. Controls are selected recall diagnostics, not exhaustive precision scores.",
            "Estimated costs use documented Sep 16 tariff and request times, not a provider bill; missing-usage costs remain unknown.",
        ],
    }
    write_json(root / "comparison.json", report)
    experiment = read(root / "experiment.json")
    experiment.update(
        status="finished", finished_at=utc_now(), comparison_file="comparison.json"
    )
    write_json(root / "experiment.json", experiment)
    print(
        json.dumps(
            {
                "integrity_errors": integrity,
                "jobs": {
                    effort: {
                        k: arm[k]
                        for k in (
                            "statements",
                            "accepted_observations",
                            "accepted_controls",
                            "strict_controls",
                            "wall_seconds",
                            "usage",
                        )
                    }
                    for effort, arm in arms.items()
                },
                "external_relationship_changes": report["external_links"][
                    "relationship_changes"
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    main(parser.parse_args().root.resolve())
