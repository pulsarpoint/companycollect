"""Compare direct DeepSeek reasoning on frozen Handelsbanken evidence.

Two fresh job-workflow arms use identical code, inputs, catalog and budgets.
Separate request replays change only effort relative to saved low requests;
these measure individual decisions, not a new autonomous crawl.
"""

import argparse
import asyncio
import json
import shutil
from pathlib import Path
from typing import Literal

import httpx
from dotenv import dotenv_values

from company_research.external_links import assess_external_links
from company_research.llm import ModelClient
from company_research.models import (
    ExternalLink,
    Extraction,
    Page,
    ResearchConfig,
    Selection,
    SiteClassification,
)
from company_research.statements import (
    consolidate_page_statements,
    extract_page_statements,
)
from company_research.storage import content_hash, utc_now, write_json
from company_research.technology_catalog import TechnologyCatalog


def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


async def run_jobs(
    root: Path, data: Path, key: str, effort: Literal["low", "high"]
) -> None:
    root.mkdir(parents=True, exist_ok=False)
    guided = data / "handelsbanken-20260916-guided"
    pages = []
    for name in ("job-technologies", "job-technologies-page2"):
        source = guided / name
        for item in read(source / "pages.json"):
            page = Page.model_validate(item)
            if page.html_file is None:
                raise ValueError(f"Missing saved HTML: {page.page_id}")
            target = root / page.html_file
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source / page.html_file, target)
            if content_hash(target.read_text(encoding="utf-8")) != page.html_sha256:
                raise ValueError(f"Input hash mismatch: {page.page_id}")
            pages.append(page)
    pages.sort(key=lambda page: page.page_id)
    catalog = TechnologyCatalog.read(
        guided / "job-technologies/technology-catalog.json"
    )
    write_json(root / "technology-catalog.json", catalog.snapshot.model_dump())
    write_json(root / "pages.json", [page.model_dump() for page in pages])
    config = ResearchConfig(
        model="deepseek-flash",
        provider=None,
        reasoning_effort=effort,
        max_model_calls=400,
        statement_batch_size=8,
        model_timeout_seconds=300,
        max_http_attempts=1,
    )
    manifest: dict = {
        "started_at": utc_now(),
        "status": "running",
        "pages": len(pages),
        "config": config.model_dump(),
        "completed_pages": [],
        "method": "Fresh paired workflow; 16 identical saved IT ads, stable page order, unchanged prompts and catalog. No page fetching or backend writes.",
    }
    write_json(root / "manifest.json", manifest)
    async with httpx.AsyncClient(base_url="https://api.deepseek.com/") as client:
        llm = ModelClient(client, key, config, root, api="deepseek")
        semaphore = asyncio.Semaphore(3)

        async def extract(page: Page):
            async with semaphore:
                records = await extract_page_statements(page, llm, root)
                manifest["completed_pages"].append(page.page_id)
                write_json(root / "manifest.json", manifest)
                print(
                    f"{effort}: extracted {page.page_id}: {len(records)} statements",
                    flush=True,
                )
                return records

        try:
            by_page = await asyncio.gather(*(extract(page) for page in pages))
            records = [record for values in by_page for record in values]
            write_json(
                root / "extracted-statements.json", [r.model_dump() for r in records]
            )
            result = await consolidate_page_statements(
                records, pages, catalog, llm, root
            )
            write_json(root / "result.json", result)
            manifest["status"] = "finished"
        except Exception as error:
            manifest.update(
                status="failed", error=str(error).replace(key, "[REDACTED]")
            )
            raise
        finally:
            manifest.update(finished_at=utc_now(), usage=llm.usage())
            write_json(root / "manifest.json", manifest)
            print(f"{effort}: {manifest['status']}, {len(llm.calls)} calls", flush=True)


async def replay_decisions(root: Path, data: Path, key: str) -> None:
    root.mkdir(parents=True, exist_ok=False)
    source = data / "handelsbanken-20260916-full/crawl"
    originals = [read(path) for path in sorted((source / "calls").glob("*.json"))]
    selected = [
        call
        for call in originals
        if "response" in call
        and (
            call["task"] in {"site_classification", "link_assessment"}
            or call["task"].startswith("extract:")
        )
    ]
    write_json(root / "baseline-calls.json", selected)
    config = ResearchConfig(
        model="deepseek-flash",
        provider=None,
        reasoning_effort="high",
        max_model_calls=len(selected),
        model_timeout_seconds=300,
        max_http_attempts=1,
        extraction_concurrency=3,
    )
    manifest = {
        "started_at": utc_now(),
        "status": "running",
        "config": config.model_dump(),
        "requests": len(selected),
        "method": "Replay frozen original requests with only reasoning_effort changed. Original low outputs are the comparator. No downstream crawl decisions or repairs are rerun.",
    }
    write_json(root / "manifest.json", manifest)
    async with httpx.AsyncClient(base_url="https://api.deepseek.com/") as client:
        llm = ModelClient(client, key, config, root, api="deepseek")

        async def replay(call: dict) -> dict:
            if "tools" in call["request"]:
                raise ValueError(
                    "This fixed-request replay excludes tool conversations"
                )
            reply = await llm._ask(
                {},
                task=f"original-{call['call_id']}:{call['task']}",
                messages=call["request"]["messages"],
                catalog=None,
            )
            schema = (
                SiteClassification
                if call["task"] == "site_classification"
                else (Selection if call["task"] == "link_assessment" else Extraction)
            )
            errors = []
            if reply.error:
                errors.append(reply.error)
            else:
                try:
                    schema.model_validate(reply.document)
                except ValueError as error:
                    errors.append(str(error))
            result = {
                "original_call_id": call["call_id"],
                "task": call["task"],
                "document": reply.document,
                "schema_errors": errors,
            }
            write_json(root / "decisions" / f"{call['call_id']:05d}.json", result)
            print(
                f"replay {call['call_id']}: {call['task']}: {len(errors)} errors",
                flush=True,
            )
            return result

        results = await asyncio.gather(*(replay(call) for call in selected))
        write_json(root / "result.json", results)
        manifest.update(status="finished", finished_at=utc_now(), usage=llm.usage())
        write_json(root / "manifest.json", manifest)


async def run_links(root: Path, data: Path, key: str) -> None:
    root.mkdir(parents=True, exist_ok=False)
    source = data / "handelsbanken-20260916-review/external-link-review"
    inventory = read(source / "unassessed-inventory.json")
    write_json(root / "unassessed-inventory.json", inventory)
    shutil.copy2(source / "external-links.json", root / "baseline-external-links.json")
    links = [ExternalLink.model_validate(item) for item in inventory]
    config = ResearchConfig(
        model="deepseek-flash",
        provider=None,
        reasoning_effort="high",
        max_model_calls=30,
        external_link_batch_size=40,
        max_external_link_assessment_calls=20,
        model_timeout_seconds=300,
        max_http_attempts=1,
    )
    manifest = {
        "started_at": utc_now(),
        "status": "running",
        "config": config.model_dump(),
        "observations": len(links),
    }
    write_json(root / "manifest.json", manifest)
    async with httpx.AsyncClient(base_url="https://api.deepseek.com/") as client:
        llm = ModelClient(client, key, config, root, api="deepseek")
        split = (len(links) + 1) // 2
        await asyncio.gather(
            *(
                assess_external_links(part, llm, root)
                for part in (links[:split], links[split:])
            )
        )
        write_json(root / "external-links.json", [link.model_dump() for link in links])
        manifest.update(status="finished", finished_at=utc_now(), usage=llm.usage())
        write_json(root / "manifest.json", manifest)
        print(
            f"high links: finished {len(links)} occurrences, {len(llm.calls)} calls",
            flush=True,
        )


async def main(args: argparse.Namespace) -> None:
    key = dotenv_values(args.env_file).get("DEEPSEEK")
    if not key:
        raise ValueError("Missing DEEPSEEK key")
    root = args.output.resolve()
    data = args.data.resolve()
    if args.part == "jobs":
        root.mkdir(parents=True, exist_ok=False)
        shutil.copy2(__file__, root / "harness.py")
        shutil.copytree(
            Path("src/company_research"),
            root / "implementation",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        for name in ("technology-controls.json", "technology-controls-page2.json"):
            shutil.copy2(data / "handelsbanken-20260916-review" / name, root / name)
        write_json(
            root / "experiment.json",
            {
                "started_at": utc_now(),
                "status": "running",
                "model": "deepseek-flash",
                "api": "https://api.deepseek.com",
                "arms": ["low", "high"],
                "method": "One fresh run per effort on the same 16 saved IT ads. Same 0.15.2 implementation, stable page order, prompts, catalog, concurrency and budgets. Not a statistical multi-run evaluation. Separate historical-low versus high fixed-request and external-link replays.",
            },
        )
        await asyncio.gather(
            *(run_jobs(root / effort, data, key, effort) for effort in ("low", "high"))
        )
    elif args.part == "decisions":
        await replay_decisions(root / "decisions-high", data, key)
    else:
        await run_links(root / "links-high", data, key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--part", choices=("jobs", "decisions", "links"), required=True)
    parser.add_argument("--data", type=Path, default=Path("data"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/handelsbanken-20260916-reasoning"),
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    asyncio.run(main(parser.parse_args()))
