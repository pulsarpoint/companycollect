"""Reclassify saved complete results without collecting pages again."""

import argparse
import asyncio
import json
import os
import shutil
from pathlib import Path

import httpx
from company_research.llm import ModelClient
from company_research.models import ResearchConfig
from company_research.storage import content_hash, write_json
from company_research.technology_catalog import TechnologyCatalog
from dotenv import dotenv_values

from company_research import mentions, page_agent, page_run, s3_results


async def run(args):
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    code = root / "code"
    code.mkdir()
    for path in Path(mentions.__file__).parent.glob("*.py"):
        shutil.copyfile(path, code / path.name)
    shutil.copyfile(__file__, code / "mention_replay.py")
    catalog = TechnologyCatalog.read(args.catalog)
    write_json(root / "catalog.json", catalog.snapshot.model_dump())
    environment = {
        k: v
        for path in args.env_file
        for k, v in dotenv_values(path).items()
        if v is not None
    } | dict(os.environ)
    config = ResearchConfig(
        model="deepseek-flash",
        provider=None,
        reasoning_effort="high",
        max_model_calls=100,
        model_timeout_seconds=300,
        max_http_attempts=1,
        max_corrections=0,
        extraction_concurrency=3,
        max_output_tokens=65536,
    )
    async with httpx.AsyncClient(base_url="https://api.deepseek.com/") as client:
        llm = ModelClient(client, environment["DEEPSEEK"], config, root, api="deepseek")

        async def revise(path):
            previous = json.loads(path.read_text())
            directory = root / ("company-" + content_hash(previous["target_url"])[:12])
            directory.mkdir()
            write_json(directory / "previous-result.json", previous)
            pages = previous["pages"]
            for page in pages:
                html = page["captures"]["native_cleaned_html"]["content"]
                rendered = page["captures"]["rendered_html"]
                snapshot = page_agent.PageInput(
                    page=page["source"],
                    html=html,
                    links=page["links"],
                    headings=[],
                    target_url=previous["target_url"],
                )
                page["data"] = mentions.validate_page_records(
                    page["processing"]["response"]["document"].get("data"),
                    snapshot,
                    rendered["content"] if rendered else None,
                )
                page["processing"]["source_validation_revision"] = (
                    "separate-native-rendered/1.0"
                )
            classification = await mentions.classify_mentions(llm, pages, catalog)
            identifiers = [page["page_id"] for page in pages]
            calls = [
                call
                for call in llm.calls
                if any(identifier in call["task"] for identifier in identifiers)
            ]
            usage = {
                "calls": previous["model_usage"]["calls"] + len(calls),
                "by_call": previous["model_usage"]["by_call"] + calls,
                "includes_previous_revisions": True,
            }
            result = page_run.build_result(
                previous["target_url"],
                pages,
                classification,
                config,
                previous["run_id"],
                usage,
                previous["catalog"],
            )
            result.update(
                supersedes_revision_id=previous["revision_id"],
                revision_reason="Reclassify with original company/client/product context; deduplicate section text within each request.",
                previous_result_sha256=content_hash(path.read_text()),
                revision_usage={"calls": len(calls), "by_call": calls},
            )
            destination = directory / "result.json"
            write_json(destination, result)
            if args.bucket:
                await asyncio.to_thread(
                    s3_results.upload_result,
                    destination,
                    s3_results.s3_client(environment),
                    args.bucket,
                )
            print(
                f"Reclassified {previous['target_url']}: {len(classification['decisions'])} decisions, {len(calls)} calls",
                flush=True,
            )

        try:
            await asyncio.gather(*(revise(path) for path in args.result))
        finally:
            write_json(root / "usage.json", llm.usage())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, action="append", required=True)
    parser.add_argument("--bucket")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
