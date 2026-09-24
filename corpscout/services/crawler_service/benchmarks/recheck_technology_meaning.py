"""Recheck explicitly selected source-meaning failures without repeating successful stages."""

import argparse
import asyncio
import json
import shutil
from pathlib import Path

import httpx
from dotenv import dotenv_values
from replay_technology_metadata import stats

from crawler_service.llm import ModelClient
from crawler_service.models import Finding, ResearchConfig
from crawler_service.research import process_technology_metadata
from crawler_service.review import correct_reviewed_claims, review_claims
from crawler_service.storage import content_hash, utc_now, write_json
from crawler_service.technology_catalog import TechnologyCatalog


async def run(args):
    root, source = args.output.resolve(), args.snapshot.resolve()
    if root.exists():
        raise ValueError("Output must be a new directory")
    parent_text = (source / "result.json").read_text(encoding="utf-8")
    state = json.loads(parent_text)
    if state["finished_at"] is None:
        raise ValueError("Finish the initial experiment before a focused follow-up")
    selected = json.loads(args.record_ids.read_text(encoding="utf-8"))
    ids = {item["record_id"] for item in selected}
    verified = {
        item["record_id"]
        for item in state["source_verification"]
        if item["source_revalidated"]
    }
    verified.update(
        record["record_id"]
        for record in state["records"]
        if record["data"].get("correction_of") in verified
    )
    if not ids <= verified:
        raise ValueError("Follow-up requires already verified source quotations")
    key = dotenv_values(args.env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    root.mkdir(parents=True)
    for directory in ("html", "calls"):
        shutil.copytree(source / directory, root / directory)
    for filename in (
        "baseline-records.json",
        "expectations.json",
        "technology-catalog.json",
    ):
        shutil.copy2(source / filename, root / filename)
    shutil.copytree(
        Path(__file__).parents[1] / "src" / "crawler_service",
        root / "implementation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copy2(__file__, root / "recheck-harness.py")
    write_json(root / "selected-rechecks.json", selected)
    records = [Finding.model_validate(record) for record in state["records"]]
    candidates = [record for record in records if record.record_id in ids]
    if len(candidates) != len(ids):
        raise ValueError("Every selected ID must identify one saved record")
    for record in candidates:
        for source_record in record.sources:
            html = (root / "html" / f"{source_record.page_id}.html").read_text(
                encoding="utf-8"
            )
            if content_hash(html) != source_record.html_sha256:
                raise ValueError("Saved source hash mismatch")
        if not args.metadata_only:
            record.evidence_status = "source_matched"
            record.data.pop("interpretation_review", None)
    state.update(
        {
            "package_version": "0.10.2",
            "parent_result": str(source / "result.json"),
            "parent_sha256": content_hash(parent_text),
            "followup_started_at": utc_now(),
            "parent_usage": state["usage"],
            "finished_at": None,
            "completed_followup_stages": [],
        }
    )
    catalog = TechnologyCatalog.read(root / "technology-catalog.json")
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = ModelClient(
            client, key, ResearchConfig.model_validate(state["config"]), root
        )
        llm.calls = list(state["usage"]["by_call"])

        def save():
            state["records"] = [record.model_dump() for record in records]
            state["usage"] = llm.usage()
            write_json(root / "result.json", state)

        save()
        corrections = []
        if not args.metadata_only:
            print(
                f"Recheck source meaning for {len(candidates)} selected records",
                flush=True,
            )
            # Keep source interpretation batches homogeneous by source document.
            page_ids = sorted({record.sources[0].page_id for record in candidates})
            for page_id in page_ids:
                batch = [
                    record
                    for record in candidates
                    if record.sources[0].page_id == page_id
                ]
                state["issues"].extend(
                    await review_claims(batch, llm, root, f"followup-source-{page_id}")
                )
                save()
            state["completed_followup_stages"].append("source_meaning")
            corrections = await correct_reviewed_claims(
                candidates, llm, root, "followup-signals"
            )
            records.extend(corrections)
            state["completed_followup_stages"].append("signal_correction")
            save()
            print(
                f"Created {len(corrections)} corrected observations; checking their pending metadata",
                flush=True,
            )
        state["issues"].extend(
            await process_technology_metadata(
                candidates + corrections, catalog, llm, root, "followup-metadata"
            )
        )
        state["completed_followup_stages"].append("metadata")
        state["finished_at"] = utc_now()
        save()
    comparison = stats(state, root)
    print(
        json.dumps(
            {key: value for key, value in comparison.items() if key != "rows"}, indent=2
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--record-ids", type=Path, required=True)
    parser.add_argument("--metadata-only", action="store_true")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    asyncio.run(run(parser.parse_args()))
