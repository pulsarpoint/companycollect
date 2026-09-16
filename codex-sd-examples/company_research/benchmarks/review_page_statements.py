"""Replay saved page descriptions through source review and isolated normalization."""

import argparse
import asyncio
import json
import shutil
from collections import Counter
from pathlib import Path

import httpx
from dotenv import dotenv_values

from company_research.analytics import accepted_finding
from company_research.llm import ModelClient
from company_research.models import Finding, Page, ResearchConfig
from company_research.statement_review import accepted_description
from company_research.statements import consolidate_page_statements
from company_research.storage import content_hash, utc_now, write_json
from company_research.technology_catalog import TechnologyCatalog


async def run(args):
    source, root = args.snapshot.resolve(), args.output.resolve()
    if root.exists():
        raise ValueError("Output directory must be new")
    parent = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    original = (source / "statement-result.json").read_text(encoding="utf-8")
    baseline = json.loads(original)
    statements = [
        Finding.model_validate(record) for record in baseline["page_statements"]
    ]
    seed, calls = None, []
    if args.resume_from is not None:
        resume = args.resume_from.resolve()
        statements = [
            Finding.model_validate(record)
            for record in json.loads(
                (resume / "reviewed-statements.json").read_text(encoding="utf-8")
            )
        ]
        snapshots = sorted(
            (resume / "statement-normalization").glob("*.json"),
            key=lambda p: int(p.stem),
        )
        seed = (
            json.loads(snapshots[-1].read_text(encoding="utf-8")) if snapshots else None
        )
        calls = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in sorted((resume / "calls").glob("*.json"))
        ]
    pages = [Page.model_validate(page) for page in parent["pages"]]
    config = ResearchConfig.model_validate(parent["config"])
    config.statement_batch_size = 12
    config.max_model_calls = args.max_calls
    config.max_corrections = 1
    config.max_review_attempts = 2
    config.model_timeout_seconds = 120.0
    root.mkdir(parents=True)
    if args.resume_from is not None:
        for name in ("calls", "page-description-review"):
            shutil.copytree(args.resume_from / name, root / name)
    shutil.copytree(source / "html", root / "html")
    for name in ("technology-catalog.json", "expectations.json"):
        shutil.copy2(source / name, root / name)
    for page in pages:
        if (
            content_hash((root / page.html_file).read_text(encoding="utf-8"))
            != page.html_sha256
        ):
            raise ValueError("Native source HTML changed")
    shutil.copytree(
        Path(__file__).parents[1] / "src/company_research",
        root / "implementation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copy2(__file__, root / "benchmark-harness.py")
    write_json(root / "original-statements.json", [r.model_dump() for r in statements])
    manifest = {
        "package_version": "0.12.1",
        "resume_from": str(args.resume_from.resolve())
        if args.resume_from is not None
        else None,
        "inherited_calls": len(calls),
        "started_at": utc_now(),
        "finished_at": None,
        "baseline_result": str(source / "statement-result.json"),
        "baseline_sha256": content_hash(original),
        "config": config.model_dump(),
        "pages": [p.model_dump() for p in pages],
        "page_fetches": 0,
        "new_page_extractions": 0,
        "prior_phase_usage": {
            k: v for k, v in baseline["usage"].items() if k != "by_call"
        },
    }
    write_json(root / "manifest.json", manifest)
    key = dotenv_values(args.env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    print(
        f"Review and normalize {len(statements)} saved descriptions from {len(pages)} unchanged pages",
        flush=True,
    )
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = ModelClient(client, key, config, root)
        llm.calls = calls
        try:
            result = await consolidate_page_statements(
                statements,
                pages,
                TechnologyCatalog.read(root / "technology-catalog.json"),
                llm,
                root,
                seed_normalization=seed,
            )
            manifest["finished_at"] = utc_now()
        finally:
            manifest["usage"] = llm.usage()
            write_json(root / "manifest.json", manifest)
            write_json(
                root / "reviewed-statements.json", [r.model_dump() for r in statements]
            )
    write_comparison(result, manifest, root, statements, pages)


def write_comparison(result, manifest, root, statements, pages):
    expected = json.loads((root / "expectations.json").read_text(encoding="utf-8"))
    technologies = [Finding.model_validate(r) for r in result["technology_signals"]]
    certificates = [
        Finding.model_validate(r) for r in result["certifications_compliance"]
    ]
    controls = []
    for control in expected["technology_controls"]:
        matches = [
            r
            for r in technologies
            if r.data["technology"].casefold() == control["technology"].casefold()
            and any(s.page_id == control["page_id"] for s in r.sources)
        ]
        controls.append(
            control
            | {
                "extracted": bool(matches),
                "correct_accepted": any(
                    accepted_finding(r)
                    and r.data["signal"] == control["signal"]
                    and r.data["scope"] == control["scope"]
                    for r in matches
                ),
                "record_ids": [r.record_id for r in matches],
            }
        )
    credentials = []
    for standard, claim in expected["credentials"].items():
        matches = [
            r
            for r in certificates
            if ":".join(
                filter(None, [r.data["standard_name"], r.data["standard_version"]])
            ).casefold()
            == standard.casefold()
        ]
        credentials.append(
            {
                "standard": standard,
                "claim": claim,
                "correct_accepted": any(
                    accepted_finding(r)
                    and r.data["claim_type"] == claim
                    and r.data["subject_name"] == "NOVELIC"
                    for r in matches
                ),
                "record_ids": [r.record_id for r in matches],
            }
        )
    summary = {
        "pages": len(pages),
        "page_fetches": 0,
        "new_page_extractions": 0,
        "statements": len(statements),
        "accepted_descriptions": sum(accepted_description(r) for r in statements),
        "description_statuses": dict(
            Counter(
                r.data.get("description_review", {}).get("status") for r in statements
            )
        ),
        "corrected_descriptions": sum(
            bool(r.data.get("description_revisions")) for r in statements
        ),
        "technology_records": len(technologies),
        "accepted_technologies": sum(accepted_finding(r) for r in technologies),
        "accepted_certifications": sum(accepted_finding(r) for r in certificates),
        "technology_controls_correct": sum(r["correct_accepted"] for r in controls),
        "technology_controls_total": len(controls),
        "technology_checks": controls,
        "credential_checks": credentials,
        "pending_statement_count": len(result["pending_statement_ids"]),
        "dispositions": result["dispositions"],
        "usage": {k: v for k, v in manifest["usage"].items() if k != "by_call"},
        "calls_by_stage": dict(
            Counter(c["task"].split(":")[0] for c in manifest["usage"]["by_call"])
        ),
    }
    write_json(root / "comparison.json", summary)
    print(
        json.dumps(
            {
                k: v
                for k, v in summary.items()
                if k not in {"technology_checks", "dispositions"}
            },
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=Path("company_research/data/novelic-page-statements-v1-recovered"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("company_research/data/novelic-page-statements-v2"),
    )
    parser.add_argument(
        "--env-file", type=Path, default=Path("jobs_extraction_lab/.env")
    )
    parser.add_argument("--max-calls", type=int, default=60)
    parser.add_argument("--resume-from", type=Path)
    asyncio.run(run(parser.parse_args()))
