"""Score native output against the frozen reference, with validation shown separately."""

import json
from collections import Counter
from pathlib import Path

import click
from pydantic import ValidationError

from company_objectives_lab.models import OBJECTIVES, RECORD_TYPES, Extraction
from company_objectives_lab.native import DATA
from company_objectives_lab.report import matches_fields
from company_objectives_lab.validation import validate_extraction
from jobs_extraction_lab.corpus import content_hash, write_json


def inspect_blocks(blocks: list, source_url: str, html: str) -> dict:
    records, gated, issues = [], [], []
    valid_envelopes = 0
    for index, block in enumerate(blocks):
        if not isinstance(block, dict) or block.get("error") is True:
            issues.append({"type": "native_error", "block_index": index})
            continue
        document = {k: v for k, v in block.items() if k != "error"}
        try:
            Extraction.model_validate(document)
        except ValidationError:
            issues.append({"type": "schema_envelope_error", "block_index": index})
        else:
            valid_envelopes += 1
        # Inspect individual native records even when peers/the envelope are malformed.
        for objective, schema in RECORD_TYPES.items():
            values = document.get(objective)
            if not isinstance(values, list):
                continue
            for value in values:
                try:
                    record = schema.model_validate(value).model_dump()
                except ValidationError as error:
                    issues.append(
                        {
                            "type": "record_schema_error",
                            "block_index": index,
                            "objective": objective,
                            "details": error.errors(
                                include_input=False, include_url=False
                            ),
                        }
                    )
                else:
                    records.append(
                        {"objective": objective, "record": record, "block_index": index}
                    )
        check = validate_extraction(json.dumps(document), "stop", source_url, html)
        gated.extend(check.get("accepted", []))
        issues.extend({**issue, "block_index": index} for issue in check["issues"])
    unique = {
        json.dumps(
            {
                "objective": r["objective"],
                "record": {k: v for k, v in r["record"].items() if k != "evidence"},
            },
            sort_keys=True,
        )
        for r in records
    }
    return {
        "records": records,
        "after_common_gate": gated,
        "issues": issues,
        "valid_envelopes": valid_envelopes,
        "duplicate_records": len(records) - len(unique),
    }


def call_summary(root: Path) -> dict:
    counts: Counter = Counter()
    costs = []
    models: Counter = Counter()
    finishes: Counter = Counter()
    for path in sorted((root / "calls").glob("*/*.json")):
        call = json.loads(path.read_text(encoding="utf-8"))
        counts["calls_started"] += 1
        if "error" in call:
            counts["call_errors"] += 1
            counts["timeouts"] += "timeout" in call["error"].lower()
        response = call.get("response")
        if response is None:
            counts["unfinished_calls"] += "error" not in call
            continue
        counts["responses"] += 1
        models[response["model"]] += 1
        usage = response.get("usage", {})
        counts["prompt_tokens"] += usage.get("prompt_tokens", 0)
        counts["completion_tokens"] += usage.get("completion_tokens", 0)
        cost = usage.get("cost")
        if cost is not None:
            costs.append(cost)
        finishes.update(c["finish_reason"] for c in response["choices"])
    return {
        **dict(counts),
        "reported_cost_usd": sum(costs),
        "calls_with_cost": len(costs),
        "models": dict(models),
        "finish_reasons": dict(finishes),
    }


def extraction_report(root: Path, reference: dict) -> dict:
    settings = json.loads((root / "settings.json").read_text(encoding="utf-8"))
    reference_text = Path("company_objectives_lab/reference-v1.json").read_text(
        encoding="utf-8"
    )
    if content_hash(reference_text) != settings["reference_sha256"]:
        raise ValueError("Reference changed")
    if (
        content_hash((DATA / "extraction-inputs.json").read_text(encoding="utf-8"))
        != settings["input_manifest_sha256"]
    ):
        raise ValueError("Input manifest changed")
    pages = {}
    for page in settings["pages"]:
        path = root / "pages" / f"{page['page_id']}.json"
        if not path.exists():
            continue
        result = json.loads(path.read_text(encoding="utf-8"))
        html = (DATA / page["file"]).read_text(encoding="utf-8")
        if (
            content_hash(html) != page["sha256"]
            or result["source_sha256"] != page["sha256"]
        ):
            raise ValueError("Snapshot changed")
        pages[page["page_id"]] = inspect_blocks(
            result["blocks"], page["source_url"], html
        )
    counts: dict[str, Counter] = {obj: Counter() for obj in OBJECTIVES}
    outcomes = []
    planned = {p["page_id"] for p in settings["pages"]}
    for check in reference["checks"]:
        if check["page_id"] not in planned:
            continue
        result = pages.get(check["page_id"], {})
        expected = [check["expected"], *check["alternatives"]]
        row = {k: check[k] for k in ("check_id", "page_id", "objective")}
        for label, field in (
            ("native_schema_match", "records"),
            ("common_gate_match", "after_common_gate"),
        ):
            row[label] = any(
                r["objective"] == check["objective"]
                and matches_fields(r["record"], option)
                for r in result.get(field, [])
                for option in expected
            )
            counts[check["objective"]][label] += row[label]
        counts[check["objective"]]["checkpoints"] += 1
        outcomes.append(row)
    negatives = []
    for check in reference["negative_checks"]:
        if check["page_id"] not in planned:
            continue
        result = pages.get(check["page_id"], {})
        rows = [
            r for r in result.get("records", []) if r["objective"] == check["objective"]
        ]
        violations = (
            rows
            if check.get("empty")
            else [
                r
                for r in rows
                if matches_fields(r["record"], {check["field"]: check["forbidden"]})
            ]
        )
        observable = result.get("valid_envelopes", 0) > 0
        negatives.append(
            {
                **check,
                "observable": observable,
                "passed": observable and not violations,
                "violations": violations,
            }
        )
    return {
        "planned_pages": len(planned),
        "completed_pages": len(pages),
        "pages_with_valid_envelopes": sum(
            p["valid_envelopes"] > 0 for p in pages.values()
        ),
        "calls": call_summary(root),
        "by_objective": {k: dict(v) for k, v in counts.items()},
        "checkpoint_outcomes": outcomes,
        "negative_controls": negatives,
        "issue_counts": dict(
            Counter(i["type"] for p in pages.values() for i in p["issues"])
        ),
        "pages": pages,
    }


def adaptive_report(root: Path, reference: dict) -> dict:
    sites = []
    for path in sorted((root / "sites").glob("*.json")):
        site = json.loads(path.read_text(encoding="utf-8"))
        fetched = []
        for snapshot in sorted((root / "snapshots" / site["domain"]).glob("*.json")):
            saved = json.loads(snapshot.read_text(encoding="utf-8"))
            result = saved.get("result", {})
            matching_ids = [
                pid
                for pid, p in reference["pages"].items()
                if saved["requested_url"] in {p["url"], p["source_url"]}
            ]
            fetched.append(
                {
                    "file": str(snapshot),
                    "url": saved["requested_url"],
                    "final_url": result.get("redirected_url") or result.get("url"),
                    "success": result.get("success"),
                    "status_code": result.get("status_code"),
                    "title": result.get("metadata", {}).get("title"),
                    "cleaned_html_chars": len(result.get("cleaned_html") or ""),
                    "internal_links": len(result.get("links", {}).get("internal", [])),
                    "previewed_internal_links": sum(
                        bool(link.get("head_data"))
                        for link in result.get("links", {}).get("internal", [])
                    ),
                    "known_useful_labels": [
                        label
                        for label in reference["selection"]
                        if label["page_id"] in matching_ids and label["useful"]
                    ],
                }
            )
        sites.append(
            {k: v for k, v in site.items() if k != "pending_links"}
            | {"fetched": fetched}
        )
    return {
        "completed_sites": len(sites),
        "calls": call_summary(root),
        "sites": sites,
        "policy": "Exact URL overlap with existing labels is a lower bound, not a recall score. Other discovered pages need source review. No assumption that native confidence proves objective completeness.",
    }


@click.command()
@click.option("--run-dir", type=click.Path(path_type=Path, exists=True), required=True)
def main(run_dir: Path) -> None:
    reference = json.loads(
        Path("company_objectives_lab/reference-v1.json").read_text(encoding="utf-8")
    )
    settings = json.loads((run_dir / "settings.json").read_text(encoding="utf-8"))
    report = (
        extraction_report(run_dir, reference)
        if settings["kind"] == "native_extraction"
        else adaptive_report(run_dir, reference)
    )
    write_json(run_dir / "report.json", report)
    click.echo(
        json.dumps(
            {
                k: v
                for k, v in report.items()
                if k not in {"pages", "sites", "checkpoint_outcomes"}
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
