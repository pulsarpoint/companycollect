"""Audit missing reviews and recover a grounded overview without crawling again."""

import argparse
import asyncio
import json
import shutil
from pathlib import Path

import httpx
from dotenv import dotenv_values
from resume_checkpoint import restore_queue

from company_research.analytics import accepted_finding
from company_research.llm import ModelClient
from company_research.models import ResearchResult, validate_specific_technology_name
from company_research.profiles import summarize_company
from company_research.research import update_statuses
from company_research.review import REVIEW_OBJECTIVES, review_claims, review_proposals
from company_research.storage import content_hash, utc_now, write_json
from company_research.technology_catalog import TechnologyCatalog


async def run(args):
    if args.output.exists():
        raise ValueError("Validation output must be a new directory")
    key = dotenv_values(args.env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    result = ResearchResult.model_validate_json(args.result.read_bytes())
    shutil.copytree(args.snapshot, args.output)
    root = args.output.resolve()
    for path in (args.result.parent / "calls").glob("*.json"):
        destination = root / "calls" / path.name
        if destination.exists() and destination.read_bytes() != path.read_bytes():
            raise ValueError(f"Conflicting call artifact: {path.name}")
        shutil.copy2(path, destination)
    source = Path(__file__).parents[1] / "src" / "company_research"
    shutil.copytree(
        source,
        root / "validation-implementation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copy2(__file__, root / "validation-harness.py")
    result.output_directory = str(root)
    result.config.provider = None
    result.config.max_http_attempts = 2
    settings = {
        "mode": "saved_source_validation",
        "package_version": "0.8.1",
        "input_result": str(args.result.resolve()),
        "input_sha256": content_hash(args.result.read_text()),
        "started_at": utc_now(),
        "initial_calls": result.usage["calls"],
        "initial_known_cost_usd": result.usage["known_cost_usd"],
        "model": result.config.model,
        "provider": "automatic OpenRouter routing",
        "no_additional_fetches": True,
        "changes": [],
        "issues": [],
    }
    write_json(root / "validation.json", settings)
    # Verify every saved page, including pages that produced no accepted findings.
    for page in result.pages:
        if (
            page.html_file
            and content_hash((root / page.html_file).read_text()) != page.html_sha256
        ):
            raise ValueError(f"Saved HTML changed: {page.page_id}")
    queue = restore_queue(result, json.loads((root / "queue.json").read_text()))
    catalog = TechnologyCatalog.read(root / "technology-catalog.json")
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = ModelClient(client, key, result.config, root)
        llm.calls = result.usage["by_call"]
        for objective in REVIEW_OBJECTIVES:
            records = getattr(result.records, objective)
            missing = []
            for record in records:
                record.data["required_reviews"] = sorted(
                    {*record.data.get("required_reviews", []), "source_meaning"}
                )
                if (
                    record.evidence_status == "source_matched"
                    and "interpretation_review" not in record.data
                ):
                    missing.append(record)
            if missing:
                print(
                    f"Reviewing {len(missing)} missing {objective} decisions",
                    flush=True,
                )
                settings["issues"].extend(
                    await review_claims(missing, llm, root, f"validation-{objective}")
                )
                settings["changes"].extend(
                    {
                        "record_id": record.record_id,
                        "reason": "missing source review completed",
                        "status": record.evidence_status,
                    }
                    for record in missing
                )
        proposals = []
        for record in result.records.technology_signals:
            try:
                validate_specific_technology_name(record.data["technology"])
            except ValueError as error:
                record.evidence_status = "needs_review"
                record.data["specificity_error"] = str(error)
                settings["changes"].append(
                    {
                        "record_id": record.record_id,
                        "reason": "generic method excluded",
                        "technology": record.data["technology"],
                    }
                )
            if (record.data.get("catalog_match") or {}).get("status") == "proposed":
                record.data["required_reviews"] = sorted(
                    {*record.data.get("required_reviews", []), "proposal_metadata"}
                )
                if (
                    record.evidence_status == "source_matched"
                    and "proposal_review" not in record.data
                ):
                    proposals.append(record)
        if proposals:
            settings["issues"].extend(
                await review_proposals(
                    proposals, llm, root, "validation", catalog.category_options()
                )
            )
        for _, records in result.records:
            for record in records:
                if record.evidence_status == "source_matched" and not accepted_finding(
                    record
                ):
                    record.evidence_status = "needs_review"
        update_statuses(result, queue, llm)
        write_json(root / "result.json", result.model_dump())
        print("Generating overview with short citation IDs", flush=True)
        try:
            result.company_overview = await summarize_company(result, llm, root)
        except Exception as error:
            settings["issues"].append(
                f"Overview: {type(error).__name__}: {str(error).replace(key, '[REDACTED]')}"
            )
        update_statuses(result, queue, llm)
        result.status = "partial"
        result.finished_at = utc_now()
        settings.update(
            finished_at=result.finished_at,
            final_calls=result.usage["calls"],
            final_known_cost_usd=result.usage["known_cost_usd"],
            overview_succeeded=result.company_overview is not None,
        )
        result.discovery["saved_source_validation"] = settings
        write_json(root / "validation.json", settings)
        write_json(root / "result.json", result.model_dump())
        print(json.dumps(settings, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("result", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--env-file", type=Path, required=True)
    asyncio.run(run(parser.parse_args()))
