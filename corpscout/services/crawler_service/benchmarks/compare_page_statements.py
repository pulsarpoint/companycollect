"""Evaluate page-local company context and later normalization on frozen native HTML."""

import argparse
import asyncio
import json
import shutil
from collections import Counter
from pathlib import Path

import httpx
from dotenv import dotenv_values

from crawler_service.analytics import accepted_finding
from crawler_service.llm import ModelClient
from crawler_service.models import Finding, ResearchResult
from crawler_service.statements import (
    consolidate_page_statements,
    extract_page_statements,
)
from crawler_service.storage import content_hash, utc_now, write_json
from crawler_service.technology_catalog import TechnologyCatalog


async def run(args):
    source, root = args.snapshot.resolve(), args.output.resolve()
    if root.exists():
        raise ValueError("Output directory must be new")
    key = dotenv_values(args.env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    input_text = (source / "result.json").read_text(encoding="utf-8")
    baseline = ResearchResult.model_validate_json(input_text)
    selected = {"p0002", "p0003", "p0005", "p0008", "p0009", "p0022"}
    pages = [page for page in baseline.pages if page.page_id in selected]
    if len(pages) != len(selected):
        raise ValueError("Missing selected source pages")
    root.mkdir(parents=True)
    (root / "html").mkdir()
    for page in pages:
        shutil.copy2(source / page.html_file, root / page.html_file)
        if (
            content_hash((root / page.html_file).read_text(encoding="utf-8"))
            != page.html_sha256
        ):
            raise ValueError("Source HTML hash mismatch")
    shutil.copy2(source / "technology-catalog.json", root / "technology-catalog.json")
    shutil.copytree(
        Path(__file__).parents[1] / "src/crawler_service",
        root / "implementation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copy2(__file__, root / "benchmark-harness.py")
    expected = [
        {
            "page_id": record.sources[0].page_id,
            "technology": record.data["technology"],
            "signal": "advertised_expertise"
            if record.sources[0].page_id in {"p0002", "p0003"}
            else record.data["signal"],
            "scope": record.data["scope"],
        }
        for record in baseline.records.technology_signals
        if accepted_finding(record)
        and record.sources[0].page_id in {"p0002", "p0003", "p0008", "p0009"}
    ]
    expectations = {
        "frozen_before_model_calls": True,
        "technology_controls": expected,
        "credentials": {
            "ISO 9001:2015": "certification",
            "ISO 14001:2015": "certification",
            "IATF 16949:2016": "working_toward",
        },
        "manual_checks": [
            "Page-local application descriptions must follow page quotations, not generic tool definitions.",
            "No generic Cadence Design Flow Tools accepted as a specific application.",
            "Norasoft retains the in-cabin-monitoring context without inventing deployment or development claims.",
            "Credential holder, certification scope and working-toward qualifier stay intact.",
            "Check AutoPLANT draft vendor separately from its supported source relationship.",
        ],
        "comparison_limit": "Historical direct-extraction controls, not a new randomized/cost-matched baseline run.",
    }
    write_json(root / "expectations.json", expectations)
    config = baseline.config.model_copy(
        update={
            "chunk_chars": 120000,
            "max_model_calls": 60,
            "statement_batch_size": 20,
            "max_corrections": 1,
            "max_review_attempts": 2,
            "model_timeout_seconds": 120.0,
        }
    )
    manifest = {
        "package_version": "0.11.0",
        "mode": "page_statements_experiment",
        "started_at": utc_now(),
        "baseline_result": str(source / "result.json"),
        "baseline_sha256": content_hash(input_text),
        "pages": [page.model_dump() for page in pages],
        "config": config.model_dump(),
        "page_fetches": 0,
        "completed_pages": [],
        "finished_at": None,
    }
    write_json(root / "manifest.json", manifest)
    catalog = TechnologyCatalog.read(root / "technology-catalog.json")
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = ModelClient(client, key, config, root)
        statements = []
        for page in pages:
            print(
                f"Collecting page statements: {page.page_id} {page.source_url}",
                flush=True,
            )
            statements.extend(await extract_page_statements(page, llm, root))
            manifest["completed_pages"].append(page.page_id)
            manifest["usage"] = llm.usage()
            write_json(root / "manifest.json", manifest)
            write_json(
                root / "all-page-statements.json",
                [record.model_dump() for record in statements],
            )
        print(f"Normalizing {len(statements)} page statements", flush=True)
        result = await consolidate_page_statements(
            statements, pages, catalog, llm, root
        )
        manifest["finished_at"] = utc_now()
        manifest["usage"] = llm.usage()
        write_json(root / "manifest.json", manifest)
    technology = [
        Finding.model_validate(record) for record in result["technology_signals"]
    ]
    certifications = [
        Finding.model_validate(record) for record in result["certifications_compliance"]
    ]
    checks = []
    for control in expected:
        matching = [
            record
            for record in technology
            if record.data["technology"].casefold() == control["technology"].casefold()
            and any(s.page_id == control["page_id"] for s in record.sources)
        ]
        checks.append(
            control
            | {
                "extracted": bool(matching),
                "correct_accepted": any(
                    accepted_finding(record)
                    and record.data["signal"] == control["signal"]
                    and record.data["scope"] == control["scope"]
                    for record in matching
                ),
                "records": [record.record_id for record in matching],
            }
        )
    credential_checks = []
    for standard, claim in expectations["credentials"].items():
        matches = [
            record
            for record in certifications
            if ":".join(
                filter(
                    None,
                    [record.data["standard_name"], record.data["standard_version"]],
                )
            )
            .replace(" ", "")
            .casefold()
            == standard.replace(" ", "").casefold()
        ]
        credential_checks.append(
            {
                "standard": standard,
                "expected_claim": claim,
                "correct_accepted": any(
                    accepted_finding(record)
                    and record.data["claim_type"] == claim
                    and record.data["subject_name"] == "NOVELIC"
                    for record in matches
                ),
                "records": [record.record_id for record in matches],
            }
        )
    native_chars = sum(
        len((root / page.html_file).read_text(encoding="utf-8")) for page in pages
    )
    serialized_statements_chars = len(
        json.dumps([record.model_dump() for record in statements], ensure_ascii=False)
    )
    summary = {
        "pages": len(pages),
        "page_fetches": 0,
        "statements": len(statements),
        "statement_kinds": dict(Counter(record.data["kind"] for record in statements)),
        "source_matched_statements": sum(
            record.evidence_status == "source_matched" for record in statements
        ),
        "technology_records": len(technology),
        "accepted_technology_records": sum(
            accepted_finding(record) for record in technology
        ),
        "accepted_signals": dict(
            Counter(
                record.data["signal"]
                for record in technology
                if accepted_finding(record)
            )
        ),
        "certification_records": len(certifications),
        "accepted_certifications": sum(
            accepted_finding(record) for record in certifications
        ),
        "technology_controls_correct": sum(
            check["correct_accepted"] for check in checks
        ),
        "technology_controls_total": len(checks),
        "technology_checks": checks,
        "credential_checks": credential_checks,
        "excluded_or_review_statements": len(result["dispositions"]),
        "pending_statement_ids": result["pending_statement_ids"],
        "native_html_chars": native_chars,
        "serialized_statement_chars_including_provenance": serialized_statements_chars,
        "usage": {k: v for k, v in manifest["usage"].items() if k != "by_call"},
        "calls_by_stage": dict(
            Counter(call["task"].split(":")[0] for call in manifest["usage"]["by_call"])
        ),
    }
    write_json(root / "comparison.json", summary)
    print(
        json.dumps(
            {
                k: v
                for k, v in summary.items()
                if k
                not in {
                    "technology_checks",
                    "credential_checks",
                    "pending_statement_ids",
                }
            },
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=Path("data/novelic-autonomous-v17-completed"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/novelic-page-statements-v1"),
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    asyncio.run(run(parser.parse_args()))
