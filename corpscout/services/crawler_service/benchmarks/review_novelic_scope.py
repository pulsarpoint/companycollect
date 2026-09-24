"""Recheck selected audited claims against frozen sources; no crawl or database writes."""

import argparse
import asyncio
import json
import shutil
from pathlib import Path

import httpx
from dotenv import dotenv_values

from crawler_service.llm import ModelClient
from crawler_service.models import Finding, ResearchConfig
from crawler_service.review import review_claims, review_proposals
from crawler_service.storage import content_hash, write_json
from crawler_service.technology_catalog import TechnologyCatalog


async def run(args):
    root = args.output.resolve()
    if root.exists() and any(root.iterdir()):
        raise ValueError("Output directory must be new or empty")
    root.mkdir(parents=True, exist_ok=True)
    original = json.loads((args.snapshot / "result.json").read_text())
    audit = json.loads((args.snapshot / "source-audit-issues.json").read_text())
    if (
        content_hash((args.snapshot / "result.json").read_text())
        != audit["input_result_sha256"]
    ):
        raise ValueError("Audited result changed")
    rejected = {
        record_id
        for issue in audit["issues"]
        if issue["kind"] != "summary_relationship"
        for record_id in issue["record_ids"]
    }
    cases = []
    for objective, values in original["records"].items():
        for value in values:
            data = value["data"]
            positive = value["evidence_status"] == "source_matched" and (
                objective == "technology_signals"
                and any(
                    "antenna-design" in source["url"] for source in value["sources"]
                )
                or objective == "certifications_compliance"
                and data.get("standard_name") in {"ISO 9001:2015", "IATF 16949:2016"}
                or objective == "company_profile" and data.get("field") == "trading_name" and data.get("company") == data.get("value") == "NOVELIC"
                or objective == "company_relationships"
                and data.get("relationship") in {"parent_of", "subsidiary_of"}
                and "Sona" in json.dumps(data)
                and any(
                    "parent company" in fragment["text"].lower()
                    for source in value["sources"]
                    for fragment in source["evidence"]
                )
            )
            if value["record_id"] not in rejected and not positive:
                continue
            finding = Finding.model_validate(value)
            finding.data.pop("interpretation_review", None)
            cases.append(
                {
                    "expected": "rejected"
                    if value["record_id"] in rejected
                    else "accepted",
                    "objective": objective,
                    "finding": finding,
                }
            )
    shutil.copytree(args.snapshot / "html", root / "html")
    config = ResearchConfig(
        provider="together",
        reasoning_effort="none",
        model_timeout_seconds=120,
        max_http_attempts=1,
        max_model_calls=30,
    )
    write_json(
        root / "settings.json",
        {
            "mode": "selected_frozen_claim_review",
            "snapshot": str(args.snapshot.resolve()),
            "config": config.model_dump(),
        },
    )
    key = dotenv_values(args.env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = ModelClient(client, key, config, root)
        await review_claims(
            [case["finding"] for case in cases], llm, root, "frozen-novelic"
        )
        await review_proposals(
            [
                case["finding"]
                for case in cases
                if case["objective"] == "technology_signals"
            ],
            llm,
            root,
            "frozen-novelic",
            TechnologyCatalog.read(
                args.snapshot / "technology-catalog.json"
            ).category_options(),
        )
        output = [
            {
                "record_id": case["finding"].record_id,
                "objective": case["objective"],
                "expected": case["expected"],
                "actual": (
                    case["finding"].data.get("proposal_review")
                    or case["finding"].data["interpretation_review"]
                )["status"],
                "finding": case["finding"].model_dump(),
            }
            for case in cases
        ]
        write_json(
            root / "result.json",
            {
                "cases": output,
                "passed": sum(case["expected"] == case["actual"] for case in output),
                "total": len(output),
                "usage": llm.usage(),
            },
        )
        print(
            json.dumps(
                {
                    "passed": sum(
                        case["expected"] == case["actual"] for case in output
                    ),
                    "total": len(output),
                    "usage": llm.usage(),
                }
            ),
            flush=True,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    asyncio.run(run(parser.parse_args()))
