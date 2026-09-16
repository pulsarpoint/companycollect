"""Reuse saved page descriptions; repair evidence and rerun only final processing."""

import argparse
import asyncio
import json
import shutil
from collections import Counter
from pathlib import Path

import httpx
from dotenv import dotenv_values

from company_research.analytics import accepted_finding
from company_research.content import HtmlWindow
from company_research.llm import ModelClient, parse_model_json
from company_research.models import Finding, Page, ResearchConfig
from company_research.statements import (
    PageStatements,
    consolidate_page_statements,
    repair_statement_evidence,
    statement_finding,
)
from company_research.storage import content_hash, utc_now, write_json
from company_research.technology_catalog import TechnologyCatalog


async def run(args):
    source, root = args.snapshot.resolve(), args.output.resolve()
    if root.exists():
        raise ValueError("Output must be new")
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    root.mkdir(parents=True)
    for directory in ["html", "calls"]:
        shutil.copytree(source / directory, root / directory)
    for name in ["expectations.json", "technology-catalog.json"]:
        shutil.copy2(source / name, root / name)
    shutil.copytree(
        Path(__file__).parents[1] / "src/company_research",
        root / "implementation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copy2(__file__, root / "recovery-harness.py")
    manifest.update(
        {
            "package_version": "0.11.1",
            "parent_manifest": str(source / "manifest.json"),
            "parent_manifest_sha256": content_hash(
                (source / "manifest.json").read_text(encoding="utf-8")
            ),
            "recovery_started_at": utc_now(),
            "finished_at": None,
            "reused_extraction_calls": [],
        }
    )
    config = ResearchConfig.model_validate(manifest["config"])
    config.statement_batch_size = 12
    calls = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((root / "calls").glob("*.json"))
    ]
    config.max_model_calls = 60
    pages = [Page.model_validate(page) for page in manifest["pages"]]
    key = dotenv_values(args.env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    statements = []
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = ModelClient(client, key, config, root)
        llm.calls = list(calls)
        for page in pages:
            last = next(
                call
                for call in reversed(calls)
                if call["task"].startswith(f"page_statements:{page.page_id}:")
                and call.get("response")
                and not call.get("error")
            )
            document = PageStatements.model_validate(
                parse_model_json(last["response"]["choices"][0]["message"]["content"])[
                    0
                ]
            )
            html = (root / page.html_file).read_text(encoding="utf-8")
            if content_hash(html) != page.html_sha256:
                raise ValueError("Native HTML changed")
            window = HtmlWindow(0, len(html), html)
            records = [
                statement_finding(data, page, window) for data in document.statements
            ]
            matched = sum(
                record.evidence_status == "source_matched" for record in records
            )
            print(
                f"{page.page_id}: {matched}/{len(records)} statements pass source anchors before quotation repair",
                flush=True,
            )
            write_json(
                root / "original-page-descriptions" / f"{page.page_id}.json",
                document.model_dump(),
            )
            await repair_statement_evidence(
                records, page, window, llm, root, f"recovery-{page.page_id}"
            )
            for data, record in zip(document.statements, records, strict=True):
                if data.model_dump(exclude={"evidence"}) != record.data:
                    raise ValueError("Evidence repair changed page context")
            manifest["reused_extraction_calls"].append(last["call_id"])
            statements.extend(records)
            write_json(
                root / "page-statements" / f"{page.page_id}.json",
                {
                    "page_id": page.page_id,
                    "source_url": page.source_url,
                    "html_sha256": page.html_sha256,
                    "statements": [r.model_dump() for r in records],
                },
            )
            write_json(
                root / "all-page-statements.json", [r.model_dump() for r in statements]
            )
            manifest["usage"] = llm.usage()
            write_json(root / "manifest.json", manifest)
        # Cross-page normalization has more constraints than page extraction.
        # Record this explicit experimental setting; it is not a default change.
        config.reasoning_effort = "low"
        manifest["normalization_reasoning_effort"] = "low"
        manifest["config"] = config.model_dump()
        write_json(root / "manifest.json", manifest)
        print(
            f"Normalize {len(statements)} saved descriptions; {sum(r.evidence_status == 'source_matched' for r in statements)} with verified quotations",
            flush=True,
        )
        result = await consolidate_page_statements(
            statements,
            pages,
            TechnologyCatalog.read(root / "technology-catalog.json"),
            llm,
            root,
        )
        manifest["usage"] = llm.usage()
        manifest["finished_at"] = utc_now()
        write_json(root / "manifest.json", manifest)
    expected = json.loads((root / "expectations.json").read_text(encoding="utf-8"))
    tech = [Finding.model_validate(record) for record in result["technology_signals"]]
    certs = [
        Finding.model_validate(record) for record in result["certifications_compliance"]
    ]
    checks = []
    for item in expected["technology_controls"]:
        matches = [
            r
            for r in tech
            if r.data["technology"].casefold() == item["technology"].casefold()
            and any(s.page_id == item["page_id"] for s in r.sources)
        ]
        checks.append(
            item
            | {
                "correct_accepted": any(
                    accepted_finding(r)
                    and r.data["signal"] == item["signal"]
                    and r.data["scope"] == item["scope"]
                    for r in matches
                ),
                "extracted": bool(matches),
                "record_ids": [r.record_id for r in matches],
            }
        )
    credential_checks = []
    for standard, claim in expected["credentials"].items():
        matches = [
            r
            for r in certs
            if ":".join(
                filter(None, [r.data["standard_name"], r.data["standard_version"]])
            )
            .replace(" ", "")
            .casefold()
            == standard.replace(" ", "").casefold()
        ]
        credential_checks.append(
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
        "source_matched_statements": sum(
            r.evidence_status == "source_matched" for r in statements
        ),
        "statement_kinds": dict(Counter(r.data["kind"] for r in statements)),
        "technology_records": len(tech),
        "accepted_technology_records": sum(accepted_finding(r) for r in tech),
        "accepted_signals": dict(
            Counter(r.data["signal"] for r in tech if accepted_finding(r))
        ),
        "accepted_certifications": sum(accepted_finding(r) for r in certs),
        "technology_controls_correct": sum(c["correct_accepted"] for c in checks),
        "technology_controls_total": len(checks),
        "technology_checks": checks,
        "credential_checks": credential_checks,
        "excluded_or_review_statements": len(result["dispositions"]),
        "pending_statement_ids": result["pending_statement_ids"],
        "native_html_chars": sum(
            len((root / p.html_file).read_text(encoding="utf-8")) for p in pages
        ),
        "statement_data_chars": len(
            json.dumps([r.data for r in statements], ensure_ascii=False)
        ),
        "statement_chars_with_sources": len(
            json.dumps([r.model_dump() for r in statements], ensure_ascii=False)
        ),
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
        default=Path("company_research/data/novelic-page-statements-v1"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("company_research/data/novelic-page-statements-v1-recovered"),
    )
    parser.add_argument(
        "--env-file", type=Path, default=Path("jobs_extraction_lab/.env")
    )
    asyncio.run(run(parser.parse_args()))
