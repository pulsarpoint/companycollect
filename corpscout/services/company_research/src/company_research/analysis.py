"""Analyze immutable HTML captures without starting a browser or fetching pages."""

import asyncio
import shutil
import uuid
from collections.abc import Sequence
from contextlib import ExitStack
from pathlib import Path
from typing import Literal

import click
import httpx

from company_research import mentions, page_agent
from company_research.captures import open_crawl
from company_research.llm import ModelClient
from company_research.models import ResearchConfig
from company_research.storage import content_hash, utc_now, write_json
from company_research.technology_catalog import TechnologyCatalog


def build_result(
    target: str,
    pages: list[dict],
    classification: dict,
    config: ResearchConfig,
    run_id: str,
    usage: dict,
    catalog_info: dict,
) -> dict:
    coverage = {
        page["page_id"]: page["data"]["coverage"]
        | {
            "technology_mentions": {
                key: value
                for key, value in page["technology_mentions"].items()
                if key != "mentions"
            }
        }
        for page in pages
    }
    partial = (
        classification["status"] != "processed"
        or catalog_info.get("status") == "failed"
        or any(
            item["status"] != "processed"
            for objectives in coverage.values()
            for item in objectives.values()
        )
        or any(
            page["processing"]["link_errors"]
            or page["processing"]["response"]["status"] != "processed"
            for page in pages
        )
    )
    return {
        "schema_version": "company-research-result/1.0",
        "run_id": run_id,
        "revision_id": uuid.uuid4().hex,
        "target_url": target,
        "created_at": utc_now(),
        "processing_status": "partial" if partial else "processed",
        "crawl_status": "saved_pages_only",
        "stop_reason": "supplied_snapshots_exhausted",
        "site_coverage": "not_established",
        "config": config.model_dump(),
        "pages": pages,
        "page_observations": [
            page["observations"]
            for page in pages
            if page.get("observations") is not None
        ]
        if any(page.get("observations") is not None for page in pages)
        else None,
        "technology_classification": classification,
        "coverage": coverage,
        "catalog": catalog_info,
        "model_usage": usage,
        "unvisited_links": [
            {
                "page_id": page["page_id"],
                **link,
                "visit_status": "not_visited_in_this_run",
            }
            for page in pages
            for link in page["links"]
        ],
        "records": {
            objective: [
                record
                for page in pages
                for record in page["data"]["records"][objective]
            ]
            for objective in pages[0]["data"]["records"]
        },
        "aggregation": "source_records_concatenated; no lossy entity merging",
        "missing_information": {
            objective: "unavailable_or_incomplete; see_page_coverage"
            if any(
                page["data"]["coverage"][objective]["status"] != "processed"
                for page in pages
            )
            else "no_records_on_supplied_pages; not_company_absence"
            if not any(page["data"]["records"][objective] for page in pages)
            else "records_present; see_page_coverage"
            for objective in pages[0]["data"]["records"]
        },
    }


async def analyze_pages(
    source: Path | Sequence[Path],
    *,
    output_dir: Path,
    config: ResearchConfig,
    api_key: str,
    api: Literal["deepseek", "openrouter"],
    catalog: TechnologyCatalog | None,
    catalog_info: dict,
) -> dict:
    """Read a crawl folder/manifest or explicit snapshot directories and return JSON.

    The output folder must be new. Source captures are copied and hash-checked
    before model calls. A partial crawl remains partial in the analysis output.
    """
    with ExitStack() as inputs:
        crawl = None
        if isinstance(source, Path):
            crawl, page_paths = inputs.enter_context(open_crawl(source))
        else:
            page_paths = list(source)
        if not page_paths:
            raise ValueError("Supply at least one captured page")
        if not api_key:
            raise ValueError("An analysis API key is required")
        root = output_dir.resolve()
        root.mkdir(parents=True, exist_ok=False)
        groups: dict[str, list[Path]] = {}
        if catalog is not None:
            write_json(root / "catalog.json", catalog.snapshot.model_dump())
        snapshots = root / "inputs"
        snapshots.mkdir()
        code = root / "code"
        code.mkdir()
        for path in Path(__file__).parent.glob("*.py"):
            shutil.copyfile(path, code / path.name)
        seen_pages = set()
        for index, path in enumerate(page_paths):
            frozen = snapshots / f"{index:03}-{path.name}"
            frozen.mkdir()
            for name in ("input.json", "page.html", "link-page.html"):
                source = path / name
                if source.exists():
                    shutil.copyfile(source, frozen / name)
            page = page_agent.PageInput.load(frozen)
            identity = (page.target_url, page.page["page_id"])
            if identity in seen_pages:
                raise ValueError(
                    "Duplicate page ID for the same target in supplied captures"
                )
            seen_pages.add(identity)
            rendered_path = frozen / "link-page.html"
            if rendered_path.is_file() and content_hash(
                rendered_path.read_text(encoding="utf-8")
            ) != page.page.get("link_html_sha256"):
                raise ValueError("Frozen rendered capture hash mismatch")
            groups.setdefault(page.target_url, []).append(frozen)
    manifest: dict = {
        "status": "running",
        "started_at": utc_now(),
        "api": api,
        "json_mode": "json_object",
        "config": config.model_dump(),
        "catalog": catalog_info,
        "results": [],
        "crawl": crawl,
        "source_code_sha256": {
            path.name: content_hash(path.read_text()) for path in code.iterdir()
        },
    }
    write_json(root / "manifest.json", manifest)
    base_url = (
        "https://api.deepseek.com/"
        if api == "deepseek"
        else "https://openrouter.ai/api/v1/"
    )
    async with httpx.AsyncClient(base_url=base_url) as client:
        llm = ModelClient(
            client, api_key, config, root, api=api, json_mode="json_object"
        )
        agent = mentions.MentionPageAgent(llm)
        try:

            async def run_company(target: str, paths: list[Path]) -> dict:
                company = root / ("company-" + content_hash(target)[:12])
                company.mkdir()

                async def analyze(path: Path, destination: Path) -> dict:
                    page = page_agent.PageInput.load(path)
                    rendered_path = path / "link-page.html"
                    rendered = (
                        rendered_path.read_text(encoding="utf-8")
                        if rendered_path.exists()
                        else None
                    )
                    if rendered is not None and content_hash(rendered) != page.page.get(
                        "link_html_sha256"
                    ):
                        raise ValueError("Frozen rendered capture hash mismatch")
                    result = await agent.analyze(page, rendered)
                    write_json(destination / f"page-{result['page_id']}.json", result)
                    click.echo(
                        f"Collected {path.name}: {len(result['technology_mentions']['mentions'])} raw mentions",
                        err=True,
                    )
                    return result

                pages = await asyncio.gather(
                    *(analyze(path, company) for path in paths)
                )
                classification = await mentions.classify_mentions(llm, pages, catalog)
                page_ids = [page["page_id"] for page in pages]
                calls = [
                    call
                    for call in llm.calls
                    if any(identifier in call["task"] for identifier in page_ids)
                ]
                usage = {
                    "calls": len(calls),
                    "by_call": calls,
                    "prompt_tokens": sum(
                        call.get("usage", {}).get("prompt_tokens", 0) for call in calls
                    ),
                    "completion_tokens": sum(
                        call.get("usage", {}).get("completion_tokens", 0)
                        for call in calls
                    ),
                }
                result = build_result(
                    target,
                    pages,
                    classification,
                    config,
                    uuid.uuid4().hex,
                    usage,
                    catalog_info,
                )
                result["model_api"] = api
                if crawl is not None:
                    result["crawl"] = crawl
                    result["crawl_status"] = crawl["status"]
                    if crawl["status"] != "finished":
                        result["processing_status"] = "partial"
                output = company / "result.json"
                write_json(output, result)
                manifest["results"].append(
                    {
                        "target_url": target,
                        "file": str(output),
                        "processing_status": result["processing_status"],
                    }
                )
                write_json(root / "manifest.json", manifest)
                click.echo(
                    f"Finished {target}: {len(classification['decisions'])} decisions",
                    err=True,
                )
                return result

            results = await asyncio.gather(
                *(run_company(target, paths) for target, paths in groups.items())
            )
            manifest["status"] = "finished"
        finally:
            manifest.update(finished_at=utc_now(), usage=llm.usage())
            if manifest["status"] == "running":
                manifest["status"] = "interrupted"
            write_json(root / "manifest.json", manifest)
    return results[0] if len(results) == 1 else {"results": results}
