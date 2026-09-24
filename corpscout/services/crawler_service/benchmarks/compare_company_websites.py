"""Paired extraction on new native Crawl4AI snapshots, with frozen source controls.

Fetch first, inspect the saved HTML and write controls.json, then run compare.
This is an extraction benchmark on selected pages, not a navigation benchmark.
"""

import argparse
import asyncio
import json
import shutil
import tomllib
from pathlib import Path
from typing import Literal

import httpx
from dotenv import dotenv_values

from crawler_service.fetch import fetch_page, open_browser
from crawler_service.llm import ModelClient
from crawler_service.models import Page, ResearchConfig
from crawler_service.statements import (
    consolidate_page_statements,
    extract_page_statements,
)
from crawler_service.storage import content_hash, utc_now, write_json
from crawler_service.technology_catalog import TechnologyCatalog

SITES = {
    "memgraph": ["https://memgraph.com/", "https://memgraph.com/careers"],
    "oxide": [
        "https://oxide.computer/",
        "https://oxide.computer/careers/sw-networking",
    ],
    "rt-rk": [
        "https://www.rt-rk.com/open-positions/",
        "https://www.rt-rk.com/one-stop-developer-shopindustrial-iot-gateway-development/",
    ],
}


def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


async def fetch(args: argparse.Namespace) -> None:
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "started_at": utc_now(),
        "sites": {},
        "selection": "Three new companies, two selected public pages each. Chosen before fetching or model calls; no site-specific CSS selectors.",
    }
    write_json(root / "manifest.json", manifest)
    async with open_browser() as crawler:
        for site, urls in SITES.items():
            output = root / "sources" / site
            pages = []
            for index, url in enumerate(urls, 1):
                page = Page(
                    page_id=f"p{index:04}",
                    requested_url=url,
                    source_url=url,
                    selected_for="paired extraction benchmark",
                    fetched_at=utc_now(),
                    status_code=None,
                    fetch_status="pending",
                    extraction_status="not_assessed",
                    objectives_examined=[],
                    attempts=0,
                    chunks_planned=0,
                    chunks_completed=0,
                    html_sha256=None,
                    html_file=None,
                    errors=[],
                )
                html, links = await fetch_page(crawler, page, ResearchConfig(), output)
                pages.append(page.model_dump())
                write_json(output / "pages.json", pages)
                write_json(output / f"links-{page.page_id}.json", links)
                print(
                    f"{site}/{page.page_id}: {page.fetch_status}, {len(html)} HTML characters",
                    flush=True,
                )
            manifest["sites"][site] = {"pages": pages}
            write_json(root / "manifest.json", manifest)
    manifest["finished_at"] = utc_now()
    write_json(root / "manifest.json", manifest)


async def compare(args: argparse.Namespace) -> None:
    root = args.output.resolve()
    controls = root / "controls.json"
    if not controls.is_file():
        raise ValueError(
            "Inspect saved HTML and freeze source-read controls.json before comparing"
        )
    keys = dotenv_values(args.env_file)
    for name in ("DEEPSEEK", "OPENROUTER_API_KEY"):
        if not keys.get(name):
            raise ValueError(f"{name} is required in the explicit env file")
    runs = root / "runs"
    runs.mkdir(exist_ok=False)
    package = Path(__file__).parents[1]
    shutil.copytree(
        package / "src/crawler_service",
        root / "implementation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copy2(__file__, root / "benchmark-harness.py")
    shutil.copy2(args.catalog, root / "technology-catalog.json")
    catalog = TechnologyCatalog.read(root / "technology-catalog.json")
    config = ResearchConfig(
        reasoning_effort="low",
        statement_batch_size=8,
        max_model_calls=args.max_calls,
        max_output_tokens=65536,
        model_timeout_seconds=120,
        max_review_attempts=2,
        max_corrections=1,
    )
    manifest = {
        "started_at": utc_now(),
        "config": config.model_dump(),
        "package_version": tomllib.loads(
            (package / "pyproject.toml").read_text(encoding="utf-8")
        )["project"]["version"],
        "controls_sha256": content_hash(controls.read_text(encoding="utf-8")),
        "catalog_sha256": content_hash(
            (root / "technology-catalog.json").read_text(encoding="utf-8")
        ),
        "json_mode": "json_object",
        "page_fetches_in_comparison": 0,
        "scope": "Fresh page statements, description review, normalization, source review and local catalog/proposal checks; selected pages, not whole-site discovery or regular all-objective export.",
    }
    write_json(root / "experiment.json", manifest)
    semaphore = asyncio.Semaphore(args.concurrency)

    async def arm(site: str, api: Literal["openrouter", "deepseek"]) -> None:
        async with semaphore:
            output = runs / site / api
            output.mkdir(parents=True)
            pages = [
                Page.model_validate(p)
                for p in read(root / "sources" / site / "pages.json")
            ]
            local_config = config.model_copy(
                update={
                    "model": "deepseek-flash"
                    if api == "deepseek"
                    else "deepseek/deepseek-v4-flash-0731",
                    "provider": None if api == "deepseek" else "Wafer",
                }
            )
            progress = {
                "started_at": utc_now(),
                "api": api,
                "site": site,
                "config": local_config.model_dump(),
                "status": "running",
                "completed_extractions": [],
            }
            write_json(output / "manifest.json", progress)
            for page in pages:
                if page.html_file is None or page.fetch_status != "fetched":
                    continue
                source = root / "sources" / site / page.html_file
                if content_hash(source.read_text(encoding="utf-8")) != page.html_sha256:
                    raise ValueError(f"Changed HTML: {site}/{page.page_id}")
                (output / page.html_file).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, output / page.html_file)
            key = keys["DEEPSEEK" if api == "deepseek" else "OPENROUTER_API_KEY"]
            if key is None:
                raise ValueError("Required model credential missing")
            async with httpx.AsyncClient(
                base_url="https://api.deepseek.com/"
                if api == "deepseek"
                else "https://openrouter.ai/api/v1/"
            ) as client:
                llm = ModelClient(
                    client, key, local_config, output, api=api, json_mode="json_object"
                )
                records = []
                try:
                    for page in pages:
                        if page.html_file is None or page.fetch_status != "fetched":
                            continue
                        records.extend(await extract_page_statements(page, llm, output))
                        write_json(
                            output / "extracted-statements.json",
                            [r.model_dump() for r in records],
                        )
                        progress["completed_extractions"].append(page.page_id)
                        write_json(output / "manifest.json", progress)
                        print(
                            f"{site}/{api}: extracted {page.page_id}; {len(records)} accumulated statements",
                            flush=True,
                        )
                    result = await consolidate_page_statements(
                        records, pages, catalog, llm, output
                    )
                    write_json(output / "result.json", result)
                    progress["status"] = "finished"
                    print(
                        f"{site}/{api}: finished; {len(llm.calls)} HTTP attempts",
                        flush=True,
                    )
                except Exception as error:
                    # Independent benchmark arms must retain failures and let other arms finish.
                    progress.update(
                        status="failed", error=f"{type(error).__name__}: {error}"
                    )
                    raise
                finally:
                    progress.update(finished_at=utc_now(), usage=llm.usage())
                    write_json(output / "manifest.json", progress)
                    write_json(
                        output / "last-records.json", [r.model_dump() for r in records]
                    )
                    write_json(output / "pages.json", [p.model_dump() for p in pages])

    outcomes = await asyncio.gather(
        *(arm(site, api) for site in SITES for api in ("deepseek", "openrouter")),
        return_exceptions=True,
    )
    manifest["finished_at"] = utc_now()
    manifest["errors"] = [
        f"{type(v).__name__}: {v}" for v in outcomes if isinstance(v, BaseException)
    ]
    write_json(root / "experiment.json", manifest)
    print(
        json.dumps(
            {"finished_at": manifest["finished_at"], "errors": manifest["errors"]}
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=["fetch", "compare"])
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/deepseek-more-websites-20260911"),
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--catalog",
        type=Path,
        default=Path("data/fresh-company-statements-v1/technology-catalog.json"),
    )
    parser.add_argument("--max-calls", type=int, default=60)
    parser.add_argument("--concurrency", type=int, default=3)
    args = parser.parse_args()
    asyncio.run(fetch(args) if args.stage == "fetch" else compare(args))
