"""Audit saved requests and score frozen, non-exhaustive source checkpoints."""

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import click
from pydantic import ValidationError

from company_objectives_lab.models import OBJECTIVES, RECORD_TYPES
from company_objectives_lab.run import correction_prompt, prepare_tasks, validate_result
from company_objectives_lab.validation import retain_attempts
from jobs_extraction_lab.corpus import content_hash, utc_now, write_json
from jobs_extraction_lab.extract import normalize_text


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def audit_run(data_dir: Path, run_id: str) -> dict:
    root = data_dir / "runs" / run_id
    settings, tasks = read_json(root / "settings.json"), read_json(root / "tasks.json")
    stage = settings["stage"]
    manifest = "candidates.json" if stage == "selection" else "extraction-inputs.json"
    if (
        content_hash((data_dir / manifest).read_text(encoding="utf-8"))
        != settings["input_manifest_sha256"]
    ):
        raise ValueError("Input manifest changed")
    if content_hash(json.dumps(tasks, sort_keys=True)) != settings["tasks_sha256"]:
        raise ValueError("Saved tasks changed")
    if tasks != prepare_tasks(
        data_dir, stage, settings["batch_size"], settings.get("batches_per_site")
    ):
        raise ValueError("Current inputs or prompts differ from frozen tasks")
    settings_hash = content_hash(json.dumps(settings, sort_keys=True))
    all_attempts, results, seen_paths = [], {}, set()
    for task in tasks:
        prompt, attempts = task["prompt"], []
        for number in (1, 2):
            path = root / "attempts" / task["task_id"] / f"{number}.json"
            if not path.exists():
                break
            record = read_json(path)
            seen_paths.add(path)
            if record["request_sha256"] != content_hash(
                prompt + settings_hash
            ) or record["prompt_sha256"] != content_hash(prompt):
                raise ValueError(f"Request hash mismatch: {path}")
            if record["attempt"] != number or record["task_id"] != task["task_id"]:
                raise ValueError(f"Attempt identity mismatch: {path}")
            expected = validate_result(
                data_dir, stage, task, record["raw_response"], record["finish_reason"]
            )
            if record["error"] is not None:
                expected = {
                    "accepted": [],
                    "rejected": [],
                    "issues": [
                        {"type": "service_error", "message": str(record["error"])}
                    ],
                }
            if any(
                record[k] != expected[k] for k in ("accepted", "rejected", "issues")
            ):
                raise ValueError(
                    f"Saved validation does not match raw response: {path}"
                )
            attempts.append(record)
            prompt = correction_prompt(task["prompt"], record)
        if attempts:
            retained = {
                "task_id": task["task_id"],
                "attempts": len(attempts),
                **retain_attempts(stage, attempts),
            }
            result_path = root / "results" / f"{task['task_id']}.json"
            if not result_path.exists() or read_json(result_path) != retained:
                raise ValueError(f"Retained result mismatch: {result_path}")
            results[task["task_id"]] = retained
            all_attempts.extend(attempts)
    if seen_paths != set((root / "attempts").glob("*/*.json")):
        raise ValueError("Unplanned or out-of-order attempt files")
    if {p.stem for p in (root / "results").glob("*.json")} != set(results):
        raise ValueError("Unexpected result files")
    return {
        "settings": settings,
        "tasks": tasks,
        "attempts": all_attempts,
        "results": results,
    }


def run_totals(run: dict) -> dict:
    attempts = run["attempts"]
    usage = [a["usage"] for a in attempts if a["usage"] is not None]
    return {
        "planned_tasks": len(run["tasks"]),
        "finished_tasks": len(run["results"]),
        "logical_requests": len(attempts),
        "http_attempts": sum(a["http_attempts"] for a in attempts),
        "service_errors": sum(a["error"] is not None for a in attempts),
        "unknown_cost_requests": sum(
            a["usage"] is None or a["usage"].get("cost") is None for a in attempts
        ),
        "http_retries_without_separate_usage": sum(
            max(0, a["http_attempts"] - 1) for a in attempts
        ),
        "known_cost_usd": round(sum(u.get("cost") or 0 for u in usage), 9),
        "prompt_tokens": sum(u.get("prompt_tokens", 0) for u in usage),
        "completion_tokens": sum(u.get("completion_tokens", 0) for u in usage),
        "reasoning_tokens": sum(
            u.get("completion_tokens_details", {}).get("reasoning_tokens", 0)
            for u in usage
        ),
        "finish_reasons": dict(Counter(str(a["finish_reason"]) for a in attempts)),
        "issues": dict(Counter(i["type"] for a in attempts for i in a["issues"])),
        "accepted_records": sum(len(r["accepted"]) for r in run["results"].values()),
    }


EXACT_FIELDS = {"field", "kind", "type", "owner_kind", "relationship", "job_url"}


def matches_fields(record: dict, expected: dict) -> bool:
    for key, wanted in expected.items():
        actual = record.get(key)
        if actual is None:
            return False
        options = wanted if isinstance(wanted, list) else [wanted]
        if key == "value" and record.get("type") == "phone":
            if not any(
                re.sub(r"\D", "", option) == re.sub(r"\D", "", actual)
                for option in options
            ):
                return False
        elif key in EXACT_FIELDS:
            if not any(
                normalize_text(actual) == normalize_text(option) for option in options
            ):
                return False
        elif not any(
            normalize_text(option) in normalize_text(actual) for option in options
        ):
            return False
    return True


def extraction_scores(run: dict, reference: dict) -> dict:
    counts: dict[str, Counter] = {obj: Counter() for obj in OBJECTIVES}
    outcomes = []
    raw_records: dict[tuple[str, str], list[dict]] = {}
    for attempt in run["attempts"]:
        # Diagnose which checkpoint fields were lost at the validation boundary.
        for entry in [*attempt.get("accepted", []), *attempt.get("rejected", [])]:
            try:
                record = (
                    RECORD_TYPES[entry["objective"]]
                    .model_validate(entry["record"])
                    .model_dump()
                )
            except ValidationError:
                continue
            raw_records.setdefault((attempt["task_id"], entry["objective"]), []).append(
                record
            )
    for result in run["results"].values():
        for entry in result["accepted"]:
            if entry["objective"] in counts:
                counts[entry["objective"]]["retained_records"] += 1
    for check in reference["checks"]:
        entries = run["results"].get(check["page_id"], {}).get("accepted", [])
        expected = [check["expected"], *check["alternatives"]]
        raw_matched = any(
            matches_fields(record, option)
            for record in raw_records.get((check["page_id"], check["objective"]), [])
            for option in expected
        )
        found = [
            e
            for e in entries
            if e["objective"] == check["objective"]
            and any(matches_fields(e["record"], option) for option in expected)
        ]
        counts[check["objective"]]["checkpoints"] += 1
        counts[check["objective"]]["checkpoints_with_response"] += any(
            a["task_id"] == check["page_id"]
            and a["error"] is None
            and a["finish_reason"] == "stop"
            and not any(
                i["type"] in {"invalid_json", "invalid_envelope"} for i in a["issues"]
            )
            for a in run["attempts"]
        )
        counts[check["objective"]]["matched"] += bool(found)
        counts[check["objective"]]["raw_schema_matched"] += raw_matched
        outcomes.append(
            {
                "check_id": check["check_id"],
                "page_id": check["page_id"],
                "objective": check["objective"],
                "matched": bool(found),
                "raw_schema_matched": raw_matched,
                "matching_records": found,
            }
        )
    negatives = []
    for check in reference["negative_checks"]:
        result = run["results"].get(check["page_id"])
        attempts = [a for a in run["attempts"] if a["task_id"] == check["page_id"]]
        # Failed requests cannot pass an absence check simply because their arrays are empty.
        observable = any(
            a["error"] is None
            and a["finish_reason"] == "stop"
            and not any(
                i["type"] in {"invalid_json", "invalid_envelope"} for i in a["issues"]
            )
            for a in attempts
        )
        entries = (
            []
            if result is None
            else [e for e in result["accepted"] if e["objective"] == check["objective"]]
        )
        violations = (
            entries
            if check.get("empty")
            else [
                e
                for e in entries
                if matches_fields(e["record"], {check["field"]: check["forbidden"]})
            ]
        )
        negatives.append(
            {
                **check,
                "observable": observable,
                "passed": observable and not violations,
                "violations": violations,
            }
        )
    return {
        "by_objective": {k: dict(v) for k, v in counts.items()},
        "checkpoint_outcomes": outcomes,
        "negative_controls": negatives,
    }


def assessment_index(run: dict) -> dict:
    domains = {t["task_id"]: t["domain"] for t in run["tasks"]}
    return {
        (domains[task_id], a["candidate_id"]): a
        for task_id, result in run["results"].items()
        for a in result["accepted"]
    }


def useful(judgement: dict) -> bool:
    return judgement["potential"] in {"high", "medium"} and judgement["role"] in {
        "direct",
        "navigation",
    }


def schedule_pages(site: dict, assessments: dict, budget: int = 20) -> list[str]:
    """Offline budget illustration: round robin across objectives, unique URLs per site."""
    ranks = {"high": 0, "medium": 1, "unknown": 2, "low": 3}
    queues = {}
    for objective in OBJECTIVES:
        candidates = [
            c
            for c in site["candidates"]
            if (site["domain"], c["candidate_id"]) in assessments
            and useful(
                assessments[(site["domain"], c["candidate_id"])]["objectives"][
                    objective
                ]
            )
        ]
        queues[objective] = sorted(
            candidates,
            key=lambda c: (
                ranks[
                    assessments[(site["domain"], c["candidate_id"])]["objectives"][
                        objective
                    ]["potential"]
                ],
                assessments[(site["domain"], c["candidate_id"])]["objectives"][
                    objective
                ]["role"]
                != "direct",
                content_hash(c["url"]),
            ),
        )
    selected = []
    while len(selected) < budget:
        before = len(selected)
        for objective in OBJECTIVES:
            queue = queues[objective]
            while queue and queue[0]["candidate_id"] in selected:
                queue.pop(0)
            if queue:
                selected.append(queue.pop(0)["candidate_id"])
            if len(selected) == budget:
                break
        if len(selected) == before:
            break
    return selected


def selection_scores(run: dict, inventory: dict, reference: dict) -> dict:
    index = assessment_index(run)
    planned_keys = {(t["domain"], c) for t in run["tasks"] for c in t["candidate_ids"]}
    selected = {
        site["domain"]: schedule_pages(site, index) for site in inventory["sites"]
    }
    url_ids = {
        c["url"]: (site["domain"], c["candidate_id"])
        for site in inventory["sites"]
        for c in site["candidates"]
    }
    counts: dict[str, Counter] = {obj: Counter() for obj in OBJECTIVES}
    outcomes = []
    for label in reference["selection"]:
        page = reference["pages"][label["page_id"]]
        key = url_ids[page["url"]]
        if key not in planned_keys:
            continue
        a = index.get(key)
        judgement = None if a is None else a["objectives"][label["objective"]]
        detected = judgement is not None and useful(judgement)
        count = counts[label["objective"]]
        if label["useful"]:
            count["known_useful"] += 1
            count["positive_assessed"] += judgement is not None
            count["identified"] += detected
            count["role_matched"] += detected and judgement["role"] == label["role"]
            count["scheduled_known_useful"] += key[1] in selected[key[0]]
        else:
            count["labelled_negatives"] += 1
            count["negative_assessed"] += judgement is not None
            count["false_positive_on_labelled_negative"] += detected
        outcomes.append(
            {**label, "judgement": judgement, "scheduled": key[1] in selected[key[0]]}
        )
    control = reference["selection_metadata_only_control"]
    control_key = url_ids[reference["pages"][control["page_id"]]["url"]]
    return {
        "candidate_assessments": len(index),
        "planned_candidates": len(planned_keys),
        "by_objective": {k: dict(v) for k, v in counts.items()},
        "label_outcomes": outcomes,
        "offline_schedule_budget_per_site": 20,
        "offline_schedule_candidate_ids": selected,
        "404_control": {**control, "assessment": index.get(control_key)},
    }


def selection_stability(first: dict, second: dict) -> dict:
    left, right = assessment_index(first), assessment_index(second)
    planned_left = {
        (t["domain"], c) for t in first["tasks"] for c in t["candidate_ids"]
    }
    planned_right = {
        (t["domain"], c) for t in second["tasks"] for c in t["candidate_ids"]
    }
    planned_common = planned_left & planned_right
    left = {k: v for k, v in left.items() if k in planned_common}
    right = {k: v for k, v in right.items() if k in planned_common}
    common = left.keys() & right.keys()
    metrics = {}
    for obj in OBJECTIVES:
        pairs = [
            (left[k]["objectives"][obj], right[k]["objectives"][obj]) for k in common
        ]
        a = {k for k in common if useful(left[k]["objectives"][obj])}
        b = {k for k in common if useful(right[k]["objectives"][obj])}
        metrics[obj] = {
            "common_assessments": len(common),
            "potential_agreement": sum(
                x["potential"] == y["potential"] for x, y in pairs
            ),
            "role_agreement": sum(x["role"] == y["role"] for x, y in pairs),
            "useful_agreement": sum(useful(x) == useful(y) for x, y in pairs),
            "useful_intersection": len(a & b),
            "useful_union": len(a | b),
            "useful_jaccard": len(a & b) / len(a | b) if a | b else None,
        }
    return {
        "planned_common": len(planned_common),
        "first_assessed_on_common_plan": len(left),
        "second_assessed_on_common_plan": len(right),
        "unassessed_in_both": len(planned_common - (left.keys() | right.keys())),
        "common_by_domain": dict(Counter(k[0] for k in common)),
        "first_only": len(left.keys() - right.keys()),
        "second_only": len(right.keys() - left.keys()),
        "by_objective": metrics,
    }


def build_report(
    data_dir: Path, reference_path: Path, run_ids: tuple[str, ...]
) -> dict:
    reference_text = reference_path.read_text(encoding="utf-8")
    if (
        content_hash(reference_text)
        != read_json(data_dir / "reference-lock.json")["sha256"]
    ):
        raise ValueError("Reference changed after it was frozen")
    reference, inventory = (
        json.loads(reference_text),
        read_json(data_dir / "candidates.json"),
    )
    manifest = read_json(data_dir / "extraction-inputs.json")
    if (
        content_hash((data_dir / "collection.json").read_text(encoding="utf-8"))
        != manifest["collection_sha256"]
    ):
        raise ValueError("Collection manifest changed")
    for page in manifest["pages"]:
        if (
            content_hash((data_dir / page["file"]).read_text(encoding="utf-8"))
            != reference["pages"][page["page_id"]]["source_sha256"]
        ):
            raise ValueError("Reference source changed")
    runs = {name: audit_run(data_dir, name) for name in run_ids}
    output = {
        "created_at": utc_now(),
        "reference_sha256": content_hash(reference_text),
        "runs": {},
        "stability": {},
    }
    selection_runs = []
    for name, run in runs.items():
        stage = run["settings"]["stage"]
        scores = (
            selection_scores(run, inventory, reference)
            if stage == "selection"
            else extraction_scores(run, reference)
        )
        if stage == "extraction" and any(
            a["started_at"] < reference["created_at"] for a in run["attempts"]
        ):
            raise ValueError("Reference was created after extraction started")
        output["runs"][name] = {
            "stage": stage,
            "totals": run_totals(run),
            "scores": scores,
        }
        if stage == "selection":
            selection_runs.append(name)
    if len(selection_runs) == 2:
        output["stability"] = selection_stability(
            *(runs[name] for name in selection_runs)
        )
    return output


@click.command()
@click.option("--data-dir", type=click.Path(path_type=Path, exists=True), required=True)
@click.option(
    "--reference", type=click.Path(path_type=Path, exists=True), required=True
)
@click.option("--run-id", multiple=True, required=True)
def main(data_dir: Path, reference: Path, run_id: tuple[str, ...]) -> None:
    report = build_report(data_dir, reference, run_id)
    write_json(data_dir / "report.json", report)
    for name, run in report["runs"].items():
        click.echo(
            json.dumps(
                {
                    "run": name,
                    **run["totals"],
                    "by_objective": run["scores"]["by_objective"],
                }
            )
        )


if __name__ == "__main__":
    main()
