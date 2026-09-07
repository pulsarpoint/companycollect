"""Audit partial retention and overlapping HTML against a separate frozen catalog."""

import json
from collections import Counter
from pathlib import Path
from typing import Any

import click
from bs4 import BeautifulSoup

from jobs_extraction_lab.corpus import content_hash, utc_now, write_json
from jobs_extraction_lab.crawl4ai_html_report import score_titles
from jobs_extraction_lab.extract import normalize_job_url
from jobs_extraction_lab.format_benchmark import format_prompt
from jobs_extraction_lab.models import JobExtraction
from jobs_extraction_lab.partial_html import merge_observations, retain_valid_records
from jobs_extraction_lab.repeat import compare_outputs
from jobs_extraction_lab.validated_html import correction_prompt, page_link_urls
from jobs_extraction_lab.validated_html_report import summarize_scores


def create_report(data_dir: Path, reference_dir: Path, run_id: str) -> dict[str, Any]:
    manifest_text = (data_dir / "manifest.json").read_text(encoding="utf-8")
    manifest = json.loads(manifest_text)
    native_dir = Path(manifest["source_dir"])
    native_text = (native_dir / "manifest.json").read_text(encoding="utf-8")
    if content_hash(native_text) != manifest["source_manifest_sha256"]:
        raise ValueError("Native manifest changed")
    previous = json.loads((native_dir / "comparison.json").read_text(encoding="utf-8"))
    if previous["manifest_sha256"] != content_hash(native_text):
        raise ValueError("Reference provenance changed")
    negative_urls = {
        normalize_job_url(n["job_url"], n["job_url"])
        for n in previous["negative_examples"]
    }
    catalog = {}
    page_html = {}
    native_pages = {p["page_id"]: p for p in json.loads(native_text)["inputs"]}
    for page in manifest["pages"]:
        native = native_pages[page["page_id"]]
        if (
            page["source_file"] != native["file"]
            or page["source_sha256"] != native["sha256"]
            or page["source_url"] != native["source_url"]
        ):
            raise ValueError("Window page provenance differs from native corpus")
        html = (native_dir / page["source_file"]).read_text(encoding="utf-8")
        if content_hash(html) != page["source_sha256"]:
            raise ValueError(f"Source HTML changed: {page['page_id']}")
        page_html[page["page_id"]] = html
        expected = {}
        labelled = BeautifulSoup(
            (
                reference_dir / "full-pages/clean_html" / f"{page['page_id']}.html"
            ).read_text(encoding="utf-8"),
            "html.parser",
        )
        for card in labelled.find_all("article"):
            link, title = card.find("a", href=True), card.find("h3")
            if link is None or title is None:
                raise ValueError("Incomplete labelled reference")
            url = normalize_job_url(str(link["href"]), page["source_url"])
            if url not in negative_urls:
                expected[url] = title.get_text(" ", strip=True)
        catalog[page["page_id"]] = expected
    if (
        content_hash(json.dumps(catalog, sort_keys=True))
        != previous["reference_catalog_sha256"]
    ):
        raise ValueError("Reference catalog differs from original benchmark")
    root = data_dir / "runs" / run_id
    settings_text = (root / "settings.json").read_text(encoding="utf-8")
    settings = json.loads(settings_text)
    baseline_text = (
        native_dir / "runs" / settings["baseline_run"] / "settings.json"
    ).read_text(encoding="utf-8")
    if content_hash(baseline_text) != settings["baseline_settings_sha256"] or settings[
        "window_manifest_sha256"
    ] != content_hash(manifest_text):
        raise ValueError("Experiment provenance changed")
    for key, value in json.loads(baseline_text).items():
        if settings[key] != value:
            raise ValueError(f"Baseline inference setting changed: {key}")
    requests: list[dict[str, Any]] = []
    unit_results: dict[str, dict[str, Any]] = {}
    for unit in manifest["units"]:
        content = (data_dir / unit["file"]).read_text(encoding="utf-8")
        source = page_html[unit["page_id"]]
        prefix = "\n".join(source[a:b] for a, b in unit["context_spans"])
        reconstructed = (prefix + "\n" if prefix else "") + source[
            unit["source_start"] : unit["core_end"]
        ]
        if reconstructed != content or content_hash(content) != unit["sha256"]:
            raise ValueError(f"Window source range/hash mismatch: {unit['unit_id']}")
        if unit["variant"] == "full" and content != source:
            raise ValueError("Full-page control is not unchanged native HTML")
        links = page_link_urls(content, unit["source_url"])
        if not set(unit["core_links"]) <= links:
            raise ValueError("Core links absent from window content")
        original = format_prompt(unit, content, settings["examples"])
        prompt = original
        records = []
        observations = []
        for number in (1, 2):
            path = root / "attempts" / unit["unit_id"] / f"{number}.json"
            if not path.exists():
                break
            text = path.read_text(encoding="utf-8")
            record = json.loads(text)
            if (
                record["input_hash"]
                != content_hash(prompt + json.dumps(settings, sort_keys=True))
                or record["content_sha256"] != unit["sha256"]
                or record["prompt_sha256"] != content_hash(prompt)
            ):
                raise ValueError(f"Attempt request hash mismatch: {path}")
            if number == 2 and (
                not records[0]["validation_issues"] or records[0]["error"] is not None
            ):
                raise ValueError("Unexpected correction call")
            parsed = retain_valid_records(
                record["raw_response"],
                record["finish_reason"],
                unit["source_url"],
                links,
            )
            if record["error"] is not None:
                parsed["issues"] = [
                    {"type": "service_error", "message": str(record["error"])}
                ]
            for saved, calculated in (
                ("accepted", "accepted"),
                ("rejected", "rejected"),
                ("validation_issues", "issues"),
            ):
                if json.dumps(record[saved], sort_keys=True) != json.dumps(
                    parsed[calculated], sort_keys=True
                ):
                    raise ValueError(f"Attempt validation mismatch: {path}")
            observations.extend(
                {
                    **accepted,
                    "unit_id": unit["unit_id"],
                    "unit_index": unit["unit_index"],
                    "attempt_number": number,
                    "core_url": normalize_job_url(
                        accepted["job"]["job_url"], unit["source_url"]
                    )
                    in unit["core_links"],
                }
                for accepted in record["accepted"]
            )
            records.append(record)
            requests.append(
                {
                    **{
                        k: v
                        for k, v in record.items()
                        if k not in {"raw_response", "accepted", "rejected"}
                    },
                    "accepted_records": len(record["accepted"]),
                    "rejected_records": len(record["rejected"]),
                    "file": str(path.relative_to(data_dir)),
                    "response_sha256": content_hash(text),
                }
            )
            prompt = correction_prompt(original, record, settings["retry_instructions"])
        completed = (root / "units" / f"{unit['unit_id']}.json").exists()
        merged = merge_observations(observations, unit["source_url"])
        if completed:
            saved = json.loads(
                (root / "units" / f"{unit['unit_id']}.json").read_text(encoding="utf-8")
            )
            if (
                saved["attempt_count"] != len(records)
                or saved["observations"] != observations
                or any(saved[k] != merged[k] for k in merged)
            ):
                raise ValueError(
                    "Saved retained records/merge differ from attempt audit"
                )
        unit_results[unit["unit_id"]] = {
            "unit": unit,
            "completed": completed,
            "records": records,
            "observations": observations,
            "merged": merged,
        }
    arms = []
    outputs = {}
    for variant in ("full", "chunked"):
        rows: list[dict[str, Any]] = []
        outputs[variant] = {}
        selected_units = [u for u in manifest["units"] if u["variant"] == variant]
        for page in manifest["pages"]:
            us = [u for u in selected_units if u["page_id"] == page["page_id"]]
            if (
                "".join(
                    page_html[page["page_id"]][u["core_start"] : u["core_end"]]
                    for u in us
                )
                != page_html[page["page_id"]]
            ):
                raise ValueError("Core windows omit or duplicate source characters")
            if set.union(*(set(u["core_links"]) for u in us)) != page_link_urls(
                page_html[page["page_id"]], page["source_url"]
            ):
                raise ValueError("Windows do not cover all original links")
            observed = [
                o for u in us for o in unit_results[u["unit_id"]]["observations"]
            ]
            phases = {
                "first": [o for o in observed if o["attempt_number"] == 1],
                "retained": observed,
                "core_only": [o for o in observed if o["core_url"]],
                "last_valid_only": [
                    o
                    for u in us
                    if unit_results[u["unit_id"]]["records"]
                    and not unit_results[u["unit_id"]]["records"][-1][
                        "validation_issues"
                    ]
                    for o in unit_results[u["unit_id"]]["observations"]
                    if o["attempt_number"] == len(unit_results[u["unit_id"]]["records"])
                ],
            }
            evaluations = {}
            for phase, candidates in phases.items():
                merged = merge_observations(candidates, page["source_url"])
                jobs = JobExtraction.model_validate({"jobs": merged["jobs"]}).jobs
                evaluations[phase] = score_titles(
                    jobs, catalog[page["page_id"]], page["source_url"]
                )
                if phase == "retained":
                    outputs[variant][page["page_id"]] = jobs
                    merged_final = merged
            rows.append(
                {
                    "page_id": page["page_id"],
                    "planned_units": len(us),
                    "completed_units": sum(
                        unit_results[u["unit_id"]]["completed"] for u in us
                    ),
                    "service_errors": sum(
                        r["error"] is not None
                        for u in us
                        for r in unit_results[u["unit_id"]]["records"]
                    ),
                    **evaluations,
                    "merged": merged_final,
                }
            )
        calls = [r for r in requests if r["variant"] == variant]
        usage = [r["usage"] for r in calls if r["usage"] is not None]
        arm: dict[str, Any] = {
            "variant": variant,
            "planned_units": len(selected_units),
            "completed_units": sum(
                unit_results[u["unit_id"]]["completed"] for u in selected_units
            ),
            "completed_pages": sum(
                p["completed_units"] == p["planned_units"] for p in rows
            ),
            "model_calls": len(calls),
            "correction_calls": sum(r["attempt_number"] == 2 for r in calls),
            "input_characters": sum(u["chars"] for u in selected_units),
            "largest_input_characters": max(u["chars"] for u in selected_units),
            "known_cost_usd": sum(u.get("cost", 0) or 0 for u in usage),
            "correction_cost_usd": sum(
                (r["usage"] or {}).get("cost", 0) or 0
                for r in calls
                if r["attempt_number"] == 2
            ),
            "outcomes_without_cost": sum(
                r["usage"] is None or r["usage"].get("cost") is None for r in calls
            ),
            "tokens": {
                key: sum(u.get(key, 0) for u in usage)
                for key in ("prompt_tokens", "completion_tokens", "total_tokens")
            },
            "reasoning_tokens": sum(
                (u.get("completion_tokens_details") or {}).get("reasoning_tokens", 0)
                for u in usage
            ),
            "max_completion_tokens_used": max(
                (u.get("completion_tokens", 0) for u in usage), default=0
            ),
            "providers": dict(Counter(r["provider"] for r in calls)),
            "actual_models": dict(Counter(r["actual_model"] for r in calls)),
            "http_attempt_counts": dict(Counter(r["http_attempts"] for r in calls)),
            "service_errors": sum(r["error"] is not None for r in calls),
            "validation_issue_types": dict(
                Counter(i["type"] for r in calls for i in r["validation_issues"])
            ),
            "conflicted_jobs": sum(len(p["merged"]["conflicts"]) for p in rows),
            "conflict_fields": dict(
                Counter(
                    f
                    for p in rows
                    for c in p["merged"]["conflicts"]
                    for f in c["differing_fields"]
                )
            ),
            "overlap_only_expected_urls": sum(
                len(
                    set(p["core_only"]["missing_urls"])
                    - set(p["retained"]["missing_urls"])
                )
                for p in rows
            ),
            "pages": rows,
        }
        for phase in ("first", "retained", "core_only", "last_valid_only"):
            arm[phase] = summarize_scores([p[phase] for p in rows])
        arms.append(arm)
    pairs = []
    healthy_pages = set.intersection(
        *(
            {
                p["page_id"]
                for p in arm["pages"]
                if p["completed_units"] == p["planned_units"]
                and p["service_errors"] == 0
            }
            for arm in arms
        )
    )
    for arm in arms:
        arm["shared_without_service_errors"] = summarize_scores(
            [p["retained"] for p in arm["pages"] if p["page_id"] in healthy_pages]
        )
        arm["shared_without_service_errors_pages"] = len(healthy_pages)
    for page in manifest["pages"]:
        pairs.append(
            {
                "page_id": page["page_id"],
                **compare_outputs(
                    outputs["full"][page["page_id"]],
                    outputs["chunked"][page["page_id"]],
                    page["source_url"],
                ),
            }
        )
    report = {
        "created_at": utc_now(),
        "manifest_sha256": content_hash(manifest_text),
        "reference_catalog_sha256": previous["reference_catalog_sha256"],
        "settings_sha256": content_hash(settings_text),
        "arms": arms,
        "requests": requests,
        "pairs": pairs,
    }
    write_json(data_dir / "comparison.json", report)
    return report


@click.command()
@click.option("--data-dir", type=click.Path(path_type=Path, exists=True), required=True)
@click.option(
    "--reference-dir", type=click.Path(path_type=Path, exists=True), required=True
)
@click.option("--run-id", required=True)
def main(data_dir: Path, reference_dir: Path, run_id: str) -> None:
    report = create_report(data_dir, reference_dir, run_id)
    click.echo(
        json.dumps(
            [{k: v for k, v in a.items() if k != "pages"} for a in report["arms"]],
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
