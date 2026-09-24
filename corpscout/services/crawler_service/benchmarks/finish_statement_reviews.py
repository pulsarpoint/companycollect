"""Resume only source-interpretation corrections and unfinished catalog metadata."""

import argparse
import asyncio
import json
import shutil
from pathlib import Path

import httpx
from dotenv import dotenv_values
from review_page_statements import write_comparison

from crawler_service.analytics import (
    accepted_finding,
    source_supported_finding,
    summarize_technologies,
)
from crawler_service.llm import ModelClient
from crawler_service.models import Finding, Page, ResearchConfig
from crawler_service.research import process_technology_metadata
from crawler_service.statements import (
    correct_statement_interpretations,
    merge_statement_observation,
)
from crawler_service.storage import content_hash, utc_now, write_json
from crawler_service.technology_catalog import TechnologyCatalog


async def run(args):
    source, root = args.snapshot.resolve(), args.output.resolve()
    if root.exists():
        raise ValueError("Output directory must be new")
    original = (source / "statement-result.json").read_text(encoding="utf-8")
    result = json.loads(original)
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    config = ResearchConfig.model_validate(manifest["config"])
    config.max_model_calls = 80
    config.catalog_resolution_batch_size = 8
    pages = [Page.model_validate(p) for p in manifest["pages"]]
    statements = [Finding.model_validate(r) for r in result["page_statements"]]
    before_descriptions = [r.model_dump() for r in statements]
    records = [Finding.model_validate(r) for r in result["technology_signals"]]
    root.mkdir(parents=True)
    for name in ("html", "calls", "page-description-review"):
        shutil.copytree(source / name, root / name)
    for name in (
        "technology-catalog.json",
        "expectations.json",
        "original-statements.json",
        "reviewed-statements.json",
    ):
        shutil.copy2(source / name, root / name)
    for page in pages:
        if (
            content_hash((root / page.html_file).read_text(encoding="utf-8"))
            != page.html_sha256
        ):
            raise ValueError("Native HTML changed")
    shutil.copytree(
        Path(__file__).parents[1] / "src/crawler_service",
        root / "implementation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copy2(__file__, root / "benchmark-harness.py")
    shutil.copy2(
        Path(__file__).with_name("review_page_statements.py"),
        root / "comparison-harness.py",
    )
    manifest.update(
        {
            "package_version": "0.12.2",
            "resume_from": str(source),
            "parent_result_sha256": content_hash(original),
            "continued_at": utc_now(),
            "finished_at": None,
            "config": config.model_dump(),
            "inherited_calls": len(result["usage"]["by_call"]),
        }
    )
    write_json(root / "manifest.json", manifest)
    key = dotenv_values(args.env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = ModelClient(client, key, config, root)
        llm.calls = result["usage"]["by_call"]
        corrections = await correct_statement_interpretations(
            records, statements, pages, llm, root
        )
        for record in corrections:
            merge_statement_observation(records, record)
        print(
            f"Created and checked {len(corrections)} signal/scope corrections; resume metadata only",
            flush=True,
        )
        errors = await process_technology_metadata(
            records,
            TechnologyCatalog.read(root / "technology-catalog.json"),
            llm,
            root,
            "statement-metadata-finish",
        )
        if before_descriptions != [r.model_dump() for r in statements]:
            raise ValueError("Final processing changed page descriptions")
        result["error_history"] = [{"phase": str(source), "errors": result["errors"]}]
        result["errors"] = errors
        result["technology_signals"] = [r.model_dump() for r in records]
        result["technology_summary"] = [
            r.model_dump() for r in summarize_technologies(records)
        ]
        result["pending_technology_metadata_ids"] = [
            r.record_id
            for r in records
            if source_supported_finding(r) and not accepted_finding(r)
        ]
        result["usage"] = llm.usage()
        manifest["usage"] = llm.usage()
        manifest["finished_at"] = utc_now()
        write_json(root / "statement-result.json", result)
        write_json(root / "manifest.json", manifest)
    write_comparison(result, manifest, root, statements, pages)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=Path("data/novelic-page-statements-v2-resumed"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/novelic-page-statements-v2-final"),
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    asyncio.run(run(parser.parse_args()))
