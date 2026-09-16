"""Run independent saved pages, classify per company, and submit complete JSON."""

import argparse
import asyncio
import os
import shutil
import uuid
from pathlib import Path

import httpx
from dotenv import dotenv_values

from company_research import catalog_config, mentions, page_agent, s3_results
from company_research.llm import ModelClient
from company_research.models import ResearchConfig
from company_research.storage import content_hash, utc_now, write_json


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


async def run(args) -> None:
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    config = ResearchConfig(
        model="deepseek-flash",
        provider=None,
        reasoning_effort="high",
        max_model_calls=150,
        model_timeout_seconds=300,
        max_http_attempts=1,
        max_corrections=0,
        extraction_concurrency=3,
        max_output_tokens=65536,
    )
    environment = {
        key: value
        for path in args.env_file
        for key, value in dotenv_values(path).items()
        if value is not None
    } | dict(os.environ)
    key = environment.get("DEEPSEEK")
    if not key:
        raise ValueError("DEEPSEEK credential is required")
    catalog_error = None
    try:
        catalog = catalog_config.load_catalog(
            environment,
            root / "catalog.json" if args.catalog is None else args.catalog,
            args.offline_catalog,
        )
    except (ValueError, OSError) as error:
        catalog = None
        catalog_error = type(error).__name__
    catalog_info = {
        "status": "available" if catalog is not None else "failed",
        "version": catalog.snapshot.version if catalog else None,
        "synced_at": catalog.snapshot.synced_at if catalog else None,
        "mode": "offline" if args.offline_catalog else "live_sync",
        "error": catalog_error,
    }
    groups: dict[str, list[Path]] = {}
    if catalog is not None:
        write_json(root / "catalog.json", catalog.snapshot.model_dump())
    snapshots = root / "inputs"
    snapshots.mkdir()
    code = root / "code"
    code.mkdir()
    for path in Path(__file__).parent.glob("*.py"):
        shutil.copyfile(path, code / path.name)
    for index, path in enumerate(args.page):
        frozen = snapshots / f"{index:03}-{path.name}"
        frozen.mkdir()
        for name in ("input.json", "page.html", "link-page.html"):
            source = path / name
            if source.exists():
                shutil.copyfile(source, frozen / name)
        page = page_agent.PageInput.load(frozen)
        groups.setdefault(page.target_url, []).append(frozen)
    manifest: dict = {
        "status": "running",
        "started_at": utc_now(),
        "config": config.model_dump(),
        "catalog": catalog_info,
        "results": [],
        "source_code_sha256": {
            path.name: content_hash(path.read_text()) for path in code.iterdir()
        },
    }
    write_json(root / "manifest.json", manifest)
    async with httpx.AsyncClient(base_url="https://api.deepseek.com/") as client:
        llm = ModelClient(client, key, config, root, api="deepseek")
        agent = mentions.MentionPageAgent(llm)
        try:

            async def run_company(target: str, paths: list[Path]) -> None:
                company = root / ("company-" + content_hash(target)[:12])
                company.mkdir()

                async def analyze(path: Path, destination: Path) -> dict:
                    page = page_agent.PageInput.load(path)
                    rendered_path = path / "link-page.html"
                    rendered = (
                        rendered_path.read_text() if rendered_path.exists() else None
                    )
                    if rendered is not None and content_hash(rendered) != page.page.get(
                        "link_html_sha256"
                    ):
                        raise ValueError("Frozen rendered capture hash mismatch")
                    result = await agent.analyze(page, rendered)
                    write_json(destination / f"page-{result['page_id']}.json", result)
                    print(
                        f"Collected {path.name}: {len(result['technology_mentions']['mentions'])} raw mentions",
                        flush=True,
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
                output = company / "result.json"
                write_json(output, result)
                receipt = None
                if args.bucket:
                    try:
                        receipt = await asyncio.to_thread(
                            s3_results.upload_result,
                            output,
                            s3_results.s3_client(environment),
                            args.bucket,
                        )
                    except Exception as error:
                        receipt = {
                            "status": "failed",
                            "error_type": type(error).__name__,
                            "retry_file": str(output),
                        }
                        write_json(company / "upload-error.json", receipt)
                manifest["results"].append(
                    {
                        "target_url": target,
                        "file": str(output),
                        "processing_status": result["processing_status"],
                        "upload": receipt,
                    }
                )
                write_json(root / "manifest.json", manifest)
                print(
                    f"Finished {target}: {len(classification['decisions'])} decisions; upload={receipt['status'] if receipt else 'not_requested'}",
                    flush=True,
                )

            await asyncio.gather(
                *(run_company(target, paths) for target, paths in groups.items())
            )
            manifest["status"] = "finished"
        finally:
            manifest.update(finished_at=utc_now(), usage=llm.usage())
            if manifest["status"] == "running":
                manifest["status"] = "interrupted"
            write_json(root / "manifest.json", manifest)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--page",
        type=Path,
        action="append",
        required=True,
        help="Frozen directory containing input.json and page.html; repeat for more pages",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, action="append", default=[])
    parser.add_argument("--catalog", type=Path)
    parser.add_argument("--offline-catalog", action="store_true")
    parser.add_argument(
        "--bucket", help="Existing RustFS bucket; omit to keep local results"
    )
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
