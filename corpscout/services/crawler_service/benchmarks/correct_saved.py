"""Apply one reviewed interpretation correction to a saved audit or extraction replay."""

import argparse
import asyncio
import json
import shutil
from pathlib import Path

import httpx
from dotenv import dotenv_values

from crawler_service.analytics import summarize_technologies
from crawler_service.content import merge_finding
from crawler_service.llm import ModelClient
from crawler_service.models import Findings, ResearchConfig, ResearchResult
from crawler_service.profiles import summarize_company
from crawler_service.review import correct_reviewed_claims, review_claims
from crawler_service.storage import content_hash, utc_now, write_json


async def run(args):
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    source = args.snapshot.resolve()
    original_text = (source / "result.json").read_text()
    result = json.loads(original_text)
    records = Findings.model_validate(result["records"])
    api_key = dotenv_values(args.env_file).get("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY is required")
    shutil.copytree(source / "html", root / "html")
    write_json(
        root / "settings.json",
        {
            "stage": "bounded_interpretation_corrections",
            "input_directory": str(source),
            "input_result_sha256": content_hash(original_text),
            "source_html_modified": False,
        },
    )
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = ModelClient(
            client,
            api_key,
            ResearchConfig(
                provider="together",
                reasoning_effort="none",
                max_model_calls=10,
                max_http_attempts=1,
                model_timeout_seconds=90,
            ),
            root,
        )
        for objective in ("company_relationships", "technology_signals"):
            if args.refresh_review:
                for record in getattr(records, objective):
                    if any(
                        source.evidence_status == "source_matched"
                        for source in record.sources
                    ):
                        record.evidence_status = "source_matched"
                        record.data.pop("interpretation_correction_attempted", None)
                await review_claims(
                    getattr(records, objective), llm, root, objective + "-refreshed"
                )
            corrections = await correct_reviewed_claims(
                getattr(records, objective), llm, root, objective
            )
            for record in corrections:
                merge_finding(getattr(records, objective), record)
        result["records"] = records.model_dump()
        result["technology_summary"] = [
            r.model_dump() for r in summarize_technologies(records.technology_signals)
        ]
        if "schema_version" in result:
            for objective, status in result["objectives"].items():
                rows = getattr(records, objective)
                supported = sum(r.evidence_status == "source_matched" for r in rows)
                status.update(
                    record_count=len(rows),
                    source_matched_count=supported,
                    needs_review_count=len(rows) - supported,
                )
                if rows:
                    status["status"] = "found" if supported else "needs_review"
            result["company_overview"] = None
            result["company_overview"] = await summarize_company(
                ResearchResult.model_validate(result), llm, root
            )
            result["run_id"] += "-corrected"
            result["finished_at"] = utc_now()
            result["output_directory"] = str(root)
        result["usage"] = {
            "previous_stages": result["usage"],
            "interpretation_corrections": llm.usage(),
            "known_cost_usd": result["usage"]["known_cost_usd"]
            + llm.usage()["known_cost_usd"],
            "unknown_cost_calls": result["usage"]["unknown_cost_calls"]
            + llm.usage()["unknown_cost_calls"],
        }
        write_json(root / "result.json", result)
        print(
            "Accepted technologies",
            [
                (r.technology, r.signal)
                for r in summarize_technologies(records.technology_signals)
            ],
            flush=True,
        )
        print(
            "Accepted relationships",
            sum(
                r.evidence_status == "source_matched"
                for r in records.company_relationships
            ),
            flush=True,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--refresh-review", action="store_true")
    asyncio.run(run(parser.parse_args()))
