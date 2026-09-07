"""Score native Crawl4AI outputs; reference selectors never enter model inputs."""

import json
import re
from collections import Counter
from pathlib import Path
from statistics import median
from typing import Any
from urllib.parse import urljoin

import click
from bs4 import BeautifulSoup

from jobs_extraction_lab.compare import compare_jobs
from jobs_extraction_lab.corpus import content_hash, utc_now, write_json
from jobs_extraction_lab.extract import normalize_job_url, normalize_text
from jobs_extraction_lab.format_benchmark import format_prompt
from jobs_extraction_lab.models import Job, JobExtraction


def score_titles(
    jobs: list[Job], expected: dict[str, str], source_url: str
) -> dict[str, Any]:
    returned: dict[str, list[Job]] = {}
    for job in jobs:
        try:
            key = (
                normalize_job_url(job.job_url, source_url)
                if job.job_url
                else "missing-url"
            )
        except ValueError:
            key = f"invalid-url:{job.job_url}"
        returned.setdefault(key, []).append(job)
    matched = returned.keys() & expected.keys()
    wrong = [
        {
            "job_url": url,
            "html_title": expected[url],
            "returned_titles": [j.title for j in returned[url]],
        }
        for url in sorted(matched)
        if len(returned[url]) != 1
        or normalize_text(returned[url][0].title) != normalize_text(expected[url])
    ]
    return {
        "expected_jobs": len(expected),
        "returned_jobs": len(jobs),
        "matched_urls": len(matched),
        "correct_titles": len(matched) - len(wrong),
        "title_differences": wrong,
        "missing_urls": sorted(expected.keys() - returned.keys()),
        "unexpected_urls": sorted(returned.keys() - expected.keys()),
        "duplicate_predictions": sum(
            max(0, len(values) - 1) for values in returned.values()
        ),
    }


def create_report(
    data_dir: Path, reference_dir: Path, html_run: str, markdown_run: str
) -> dict[str, Any]:
    manifest_text = (data_dir / "manifest.json").read_text(encoding="utf-8")
    manifest = json.loads(manifest_text)
    source = json.loads((reference_dir / "manifest.json").read_text(encoding="utf-8"))
    sources = {p["page_id"]: p for p in source["source_audit"]}
    negative_path = Path(__file__).parent / "fixtures/non_job_listings.json"
    negatives = json.loads(negative_path.read_text(encoding="utf-8"))
    negative_urls = {normalize_job_url(n["job_url"], n["job_url"]) for n in negatives}
    catalog = {}
    cleaning_audit = []
    for item in manifest["inputs"]:
        page_id = item["page_id"]
        if item["source_html_sha256"] != sources[page_id]["source_html_sha256"]:
            raise ValueError(f"Source HTML differs from reference: {page_id}")
        source_html = Path(manifest["source_dir"]) / item["source_html_file"]
        if (
            content_hash(source_html.read_text(encoding="utf-8"))
            != item["source_html_sha256"]
        ):
            raise ValueError(f"Raw source changed: {page_id}")
        # These pre-existing labelled artifacts are used only for scoring.
        labelled = BeautifulSoup(
            (reference_dir / "full-pages/clean_html" / f"{page_id}.html").read_text(
                encoding="utf-8"
            ),
            "html.parser",
        )
        expected = {}
        for card in labelled.find_all("article"):
            link = card.find("a", href=True)
            title = card.find("h3")
            if link is None or title is None:
                raise ValueError(f"Incomplete reference card: {page_id}")
            expected[normalize_job_url(str(link["href"]), item["source_url"])] = (
                title.get_text(" ", strip=True)
            )
        if len(expected) != sources[page_id]["prepared_cards"]:
            raise ValueError(f"Reference card count changed: {page_id}")
        catalog[page_id] = {
            url: title for url, title in expected.items() if url not in negative_urls
        }
        cleaned_text = (data_dir / item["file"]).read_text(encoding="utf-8")
        markdown_text = (data_dir / item["markdown_file"]).read_text(encoding="utf-8")
        if (
            content_hash(cleaned_text) != item["sha256"]
            or content_hash(markdown_text) != item["markdown_sha256"]
        ):
            raise ValueError(f"Prepared content changed: {page_id}")
        cleaned = BeautifulSoup(cleaned_text, "html.parser")
        html_urls = {
            normalize_job_url(
                urljoin(item["source_url"], str(a["href"])), item["source_url"]
            )
            for a in cleaned.find_all("a", href=True)
        }
        md_urls = {
            normalize_job_url(urljoin(item["source_url"], url), item["source_url"])
            for url in re.findall(r"\]\((https?://[^\s)]+)\)", markdown_text)
        }
        cleaning_audit.append(
            {
                "page_id": page_id,
                "listing_links": len(expected),
                "html_missing_listing_urls": sorted(expected.keys() - html_urls),
                "markdown_missing_listing_urls": sorted(expected.keys() - md_urls),
            }
        )

    arms: list[dict[str, Any]] = []
    all_outcomes: list[dict[str, Any]] = []
    run_settings = []
    for variant, run_id in (("html", html_run), ("markdown", markdown_run)):
        root = data_dir / "runs" / run_id
        settings = json.loads((root / "settings.json").read_text(encoding="utf-8"))
        run_settings.append(settings)
        if settings["manifest_sha256"] != content_hash(manifest_text):
            raise ValueError(f"Manifest changed: {run_id}")
        rows = []
        for prepared in manifest["inputs"]:
            item = (
                prepared
                if variant == "html"
                else {
                    **prepared,
                    "format": "crawl4ai_native_markdown",
                    "file": prepared["markdown_file"],
                    "sha256": prepared["markdown_sha256"],
                }
            )
            path = root / "responses" / f"{item['page_id']}.json"
            if not path.exists():
                continue
            row = json.loads(path.read_text(encoding="utf-8"))
            content = (data_dir / item["file"]).read_text(encoding="utf-8")
            expected_hash = content_hash(
                format_prompt(item, content, settings["examples"])
                + json.dumps(settings, sort_keys=True)
            )
            if (
                row["input_hash"] != expected_hash
                or row["content_sha256"] != item["sha256"]
            ):
                raise ValueError(f"Response input hash mismatch: {path}")
            jobs = (
                JobExtraction.model_validate(row["extraction"]).jobs
                if row["succeeded"]
                else []
            )
            row["evaluation"] = score_titles(
                jobs, catalog[item["page_id"]], item["source_url"]
            )
            row["evaluation"]["returned_negative_urls"] = sorted(
                set(row["evaluation"]["unexpected_urls"]) & negative_urls
            )
            rows.append(row)
        all_outcomes.extend(
            {"variant": variant, "run_id": run_id, **row} for row in rows
        )
        successes = [r for r in rows if r["succeeded"]]
        usage = [r["usage"] for r in rows if r["usage"] is not None]
        arms.append(
            {
                "variant": variant,
                "run_id": run_id,
                "settings": settings,
                "planned_requests": len(manifest["inputs"]),
                "completed_requests": len(rows),
                "successful_requests": len(successes),
                "failed_requests": len(rows) - len(successes),
                "expected_jobs_all_pages": sum(len(v) for v in catalog.values()),
                "expected_jobs_successful_pages": sum(
                    r["evaluation"]["expected_jobs"] for r in successes
                ),
                "returned_jobs": sum(r["evaluation"]["returned_jobs"] for r in rows),
                "matched_urls": sum(r["evaluation"]["matched_urls"] for r in rows),
                "correct_titles": sum(r["evaluation"]["correct_titles"] for r in rows),
                "duplicate_predictions": sum(
                    r["evaluation"]["duplicate_predictions"] for r in rows
                ),
                "unexpected_urls": sum(
                    len(r["evaluation"]["unexpected_urls"]) for r in rows
                ),
                "returned_negative_urls": sum(
                    len(r["evaluation"]["returned_negative_urls"]) for r in rows
                ),
                "known_cost_usd": sum(u.get("cost", 0) or 0 for u in usage),
                "outcomes_without_cost": sum(
                    r["usage"] is None or r["usage"].get("cost") is None for r in rows
                ),
                "median_seconds": median(r["elapsed_seconds"] for r in successes)
                if successes
                else None,
                "tokens": {
                    key: sum(u.get(key, 0) for u in usage)
                    for key in ("prompt_tokens", "completion_tokens", "total_tokens")
                },
                "reasoning_tokens": sum(
                    (u.get("completion_tokens_details") or {}).get(
                        "reasoning_tokens", 0
                    )
                    for u in usage
                ),
                "max_completion_tokens_used": max(
                    (u.get("completion_tokens", 0) for u in usage), default=0
                ),
                "providers": dict(Counter(r["provider"] for r in rows)),
                "actual_models": dict(Counter(r["actual_model"] for r in rows)),
                "attempts": dict(Counter(r["attempts"] for r in rows)),
            }
        )
    for field in (
        "model",
        "provider",
        "reasoning",
        "max_tokens",
        "temperature",
        "instructions",
        "examples",
        "schema",
    ):
        if run_settings[0][field] != run_settings[1][field]:
            raise ValueError(f"Native format comparison changed {field}")
    rows_by_variant = {
        v: {r["page_id"]: r for r in all_outcomes if r["variant"] == v}
        for v in ("html", "markdown")
    }
    shared = {
        page
        for page, row in rows_by_variant["html"].items()
        if row["succeeded"]
        and page in rows_by_variant["markdown"]
        and rows_by_variant["markdown"][page]["succeeded"]
    }
    for arm in arms:
        paired = [rows_by_variant[arm["variant"]][page] for page in shared]
        arm["paired_full_pages"] = len(shared)
        arm["paired_expected_jobs"] = sum(
            r["evaluation"]["expected_jobs"] for r in paired
        )
        arm["paired_matched_urls"] = sum(
            r["evaluation"]["matched_urls"] for r in paired
        )
        arm["paired_correct_titles"] = sum(
            r["evaluation"]["correct_titles"] for r in paired
        )
    comparison_rows = []
    for variant in ("html", "markdown", "previous_custom_html"):
        expected_jobs = titles = all_six = matched_urls = 0
        differences = []
        for page_id in sorted(shared):
            source_url = (
                sources[page_id]["source_url"]
                if "source_url" in sources[page_id]
                else next(
                    i["source_url"]
                    for i in manifest["inputs"]
                    if i["page_id"] == page_id
                )
            )
            selected = {
                normalize_job_url(c["job_url"], source_url): c["title"]
                for c in sources[page_id]["selected_cards"]
                if normalize_job_url(c["job_url"], source_url) not in negative_urls
            }
            if variant == "previous_custom_html":
                row = json.loads(
                    (
                        reference_dir
                        / "runs/formats-deepseek-baidu-v1/deepseek/clean_html"
                        / f"{page_id}.json"
                    ).read_text(encoding="utf-8")
                )
            else:
                row = rows_by_variant[variant][page_id]
            jobs = JobExtraction.model_validate(row["extraction"]).jobs
            subset = [
                j
                for j in jobs
                if j.job_url and normalize_job_url(j.job_url, source_url) in selected
            ]
            title_score = score_titles(subset, selected, source_url)
            expected_jobs += len(selected)
            titles += title_score["correct_titles"]
            matched_urls += title_score["matched_urls"]
            reference = json.loads(
                (
                    reference_dir
                    / "runs/formats-v1/codex/clean_html"
                    / f"{page_id}.json"
                ).read_text(encoding="utf-8")
            )
            if not reference["succeeded"]:
                raise ValueError(f"Missing saved Astra HTML reference: {page_id}")
            comparison = compare_jobs(
                JobExtraction.model_validate(reference["extraction"]).jobs,
                subset,
                source_url,
            )
            all_six += comparison["exact_field_matches"]
            differences.extend(
                {"page_id": page_id, **match}
                for match in comparison["matches"]
                if match["differences"]
            )
            if comparison["only_codex"]:
                differences.append(
                    {"page_id": page_id, "missing_jobs": comparison["only_codex"]}
                )
        comparison_rows.append(
            {
                "variant": variant,
                "pages": len(shared),
                "jobs": expected_jobs,
                "correct_titles": titles,
                "matched_urls": matched_urls,
                "all_six_astra_agreement": all_six,
                "differences": differences,
            }
        )

    report = {
        "created_at": utc_now(),
        "manifest_sha256": content_hash(manifest_text),
        "reference_manifest_sha256": content_hash(
            (reference_dir / "manifest.json").read_text(encoding="utf-8")
        ),
        "negative_fixture_sha256": content_hash(
            negative_path.read_text(encoding="utf-8")
        ),
        "reference_catalog_sha256": content_hash(json.dumps(catalog, sort_keys=True)),
        "negative_examples": negatives,
        "cleaning_audit": cleaning_audit,
        "arms": arms,
        "shared_sample": comparison_rows,
        "outcomes": all_outcomes,
    }
    write_json(data_dir / "comparison.json", report)
    return report


@click.command()
@click.option("--data-dir", type=click.Path(path_type=Path, exists=True), required=True)
@click.option(
    "--reference-dir", type=click.Path(path_type=Path, exists=True), required=True
)
@click.option("--html-run", default="deepseek-v1")
@click.option("--markdown-run", default="deepseek-markdown-v1")
def main(data_dir: Path, reference_dir: Path, html_run: str, markdown_run: str) -> None:
    report = create_report(data_dir, reference_dir, html_run, markdown_run)
    click.echo(
        json.dumps(
            {
                "arms": [
                    {k: v for k, v in a.items() if k != "settings"}
                    for a in report["arms"]
                ],
                "shared_sample": [
                    {k: v for k, v in a.items() if k != "differences"}
                    for a in report["shared_sample"]
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
