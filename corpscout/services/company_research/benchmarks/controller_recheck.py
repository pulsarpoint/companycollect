"""Check controller changes on saved HTML or a fresh homepage-only NOVELIC crawl."""

import argparse
import asyncio
import json
import logging
import shutil
import tomllib
from pathlib import Path

import httpx
from dotenv import dotenv_values
from resume_checkpoint import restore_queue

from company_research.analytics import accepted_finding
from company_research.llm import ModelClient
from company_research.models import RECORD_TYPES, Findings, ResearchResult
from company_research.research import (
    extract_saved_page,
    research_company,
    set_job_detail_context,
    update_statuses,
)
from company_research.storage import content_hash, utc_now, write_json
from company_research.technology_catalog import TechnologyCatalog


async def run(args):
    if args.output.exists():
        raise ValueError("Output directory must be new")
    key = dotenv_values(args.env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    original = ResearchResult.model_validate_json(
        (args.snapshot / "result.json").read_bytes()
    )
    config = original.config.model_copy(deep=True)
    config.provider = None if args.provider == "auto" else args.provider
    config.max_http_attempts = 2
    config.max_pages = 24
    config.max_model_calls = 200
    config.engineering_page_reserve = 3
    catalog = TechnologyCatalog.read(args.snapshot / "technology-catalog.json")
    package_version = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]["version"]
    sources = {
        p.name: p.read_text(encoding="utf-8")
        for p in (Path(__file__).parents[1] / "src/company_research").glob("*.py")
    }
    settings = {
        "mode": args.mode,
        "snapshot": str(args.snapshot.resolve()),
        "started_at": utc_now(),
        "provider": args.provider,
        "config": config.model_dump(),
        "package_version": package_version,
        "page_ids": args.page_id if args.mode == "replay" else [],
        "homepage_only": args.mode == "live",
    }
    if args.mode == "live":
        result = await research_company(
            original.input_url,
            api_key=key,
            output_dir=args.output,
            config=config,
            technology_catalog=catalog,
        )
    else:
        args.output.mkdir(parents=True)
        result = original.model_copy(deep=True)
        result.schema_version = "1.7"
        result.run_id += "-controller-replay"
        result.config = config
        result.records = Findings(**{objective: [] for objective in RECORD_TYPES})
        result.pages = []
        result.objectives = {}
        result.errors = []
        result.discovery = {}
        result.company_overview = None
        result.started_at = utc_now()
        result.output_directory = str(args.output.resolve())
        queue = restore_queue(
            original,
            json.loads((args.snapshot / "queue.json").read_text(encoding="utf-8")),
        )
        queue.config = config
        shutil.copytree(args.snapshot / "html", args.output / "html")
        write_json(
            args.output / "technology-catalog.json", catalog.snapshot.model_dump()
        )
        async with httpx.AsyncClient(
            base_url="https://openrouter.ai/api/v1/"
        ) as client:
            llm = ModelClient(client, key, config, args.output.resolve())
            for page_id in args.page_id:
                page = next(
                    p.model_copy(deep=True)
                    for p in original.pages
                    if p.page_id == page_id
                )
                page.extraction_attempts = []
                page.objectives_examined = []
                page.errors = []
                page.chunks_completed = 0
                page.extraction_status = "not_assessed"
                result.pages.append(page)
                html = (args.output / page.html_file).read_text(encoding="utf-8")
                set_job_detail_context(page, queue, html)
                logging.info("Replaying %s: %s", page_id, page.source_url)
                await extract_saved_page(
                    result, page, llm, args.output.resolve(), catalog
                )
                update_statuses(result, queue, llm)
                write_json(args.output / "result.json", result.model_dump())
        result.finished_at = utc_now()
        result.stop_reason = "selected_saved_pages_processed"
        result.status = (
            "partial"
            if any(p.extraction_status != "complete" for p in result.pages)
            else "finished"
        )
        write_json(args.output / "result.json", result.model_dump())
    settings["finished_at"] = utc_now()
    write_json(args.output / "benchmark.json", settings)
    for name, text in sources.items():
        target = args.output / "implementation" / name
        target.parent.mkdir(exist_ok=True)
        target.write_text(text, encoding="utf-8")
    write_json(
        args.output / "implementation.json",
        {
            "package_version": package_version,
            "files": {name: content_hash(text) for name, text in sources.items()},
        },
    )
    print(
        json.dumps(
            {
                "status": result.status,
                "stop_reason": result.stop_reason,
                "pages": len(result.pages),
                "accepted_technologies": sum(
                    accepted_finding(f) for f in result.records.technology_signals
                ),
                "usage": {k: v for k, v in result.usage.items() if k != "by_call"},
            },
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["live", "replay"])
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--provider", default="auto")
    parser.add_argument("--page-id", action="append", default=[])
    args = parser.parse_args()
    if args.mode == "replay" and not args.page_id:
        parser.error("Replay requires at least one --page-id")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    asyncio.run(run(args))
