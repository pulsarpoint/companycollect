"""Run all company objectives through direct DeepSeek with frozen run artifacts."""

import argparse
import asyncio
import json
import logging
import shutil
from pathlib import Path

from dotenv import dotenv_values

from crawler_service.analytics import accepted_finding
from crawler_service.models import OBJECTIVES, ResearchConfig
from crawler_service.research import research_company
from crawler_service.storage import utc_now, write_json
from crawler_service.technology_catalog import TechnologyCatalog


async def run(args: argparse.Namespace) -> None:
    root = args.output.resolve()
    key = dotenv_values(args.env_file).get("DEEPSEEK")
    if not key:
        raise ValueError("DEEPSEEK is required in the supplied environment file")
    catalog = TechnologyCatalog.read(args.catalog)
    root.mkdir(parents=True, exist_ok=False)
    shutil.copytree(
        Path(__file__).parents[1] / "src/crawler_service",
        root / "implementation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copy2(__file__, root / "run-harness.py")
    config = ResearchConfig(
        model="deepseek-flash",
        provider=None,
        reasoning_effort=args.reasoning_effort,
        max_pages=args.max_pages,
        max_external_pages=args.max_external_pages,
        max_model_calls=args.max_model_calls,
        max_candidates=2500,
        max_sitemap_urls=1000,
        max_sitemap_files=30,
        selection_batch_size=30,
        selection_batches_per_page=2,
        model_timeout_seconds=120,
        max_http_attempts=1,
        max_external_link_assessment_calls=10,
        external_link_batch_size=30,
        job_detail_reserve=7,
        engineering_page_reserve=5,
    )
    manifest = {
        "started_at": utc_now(),
        "url": args.url,
        "model_api": "deepseek",
        "config": config.model_dump(),
        "catalog_version": catalog.snapshot.version,
        "catalog_synced_at": catalog.snapshot.synced_at,
        "scope": "All built-in objectives, homepage and sitemap discovery, native cleaned HTML and rendered external-link capture. Public pages only. Documents are discovered, not parsed. No backend writes.",
        "status": "running",
    }
    write_json(root / "experiment.json", manifest)
    result = await research_company(
        args.url,
        api_key=key,
        api="deepseek",
        config=config,
        output_dir=root / "crawl",
        technology_catalog=catalog,
    )
    manifest.update(
        finished_at=utc_now(),
        status=result.status,
        stop_reason=result.stop_reason,
        pages=len(result.pages),
        accepted_counts={
            name: sum(
                accepted_finding(record) for record in getattr(result.records, name)
            )
            for name in OBJECTIVES
        },
        usage={key: value for key, value in result.usage.items() if key != "by_call"},
    )
    write_json(root / "experiment.json", manifest)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--catalog",
        type=Path,
        default=Path("data/deepseek-more-websites-20260911/technology-catalog.json"),
    )
    parser.add_argument("--max-pages", type=int, default=30)
    parser.add_argument("--max-external-pages", type=int, default=12)
    parser.add_argument("--max-model-calls", type=int, default=320)
    parser.add_argument(
        "--reasoning-effort", choices=("none", "low", "medium", "high"), default="low"
    )
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(run(parser.parse_args()))
