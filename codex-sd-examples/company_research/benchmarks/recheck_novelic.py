"""Replay frozen source pages or run a bounded live crawl; never overwrite a run."""

import argparse
import asyncio
import json
import logging
import os
import sys
from contextlib import redirect_stdout
from pathlib import Path

import httpx
from dotenv import dotenv_values

from company_research.content import merge_finding, split_html
from company_research.llm import OpenRouter
from company_research.models import RECORD_TYPES, Findings, Page, ResearchConfig
from company_research.research import extract_window, research_company
from company_research.review import correct_reviewed_claims, review_claims
from company_research.storage import content_hash, write_json
from company_research.technology_catalog import TechnologyCatalog


async def run(args: argparse.Namespace) -> None:
    key = os.environ.get("OPENROUTER_API_KEY") or dotenv_values(args.env_file).get(
        "OPENROUTER_API_KEY"
    )
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    catalog = TechnologyCatalog.read(args.snapshot / "technology-catalog.json")
    config = ResearchConfig(
        provider=None if args.provider == "auto" else args.provider,
        reasoning_effort=args.reasoning,
        max_pages=args.max_pages,
        max_external_pages=8,
        max_model_calls=100,
        selection_batch_size=12,
        extraction_concurrency=1,
        model_timeout_seconds=180.0,
        max_http_attempts=1,
    )
    if args.mode == "live":
        result = await research_company(
            "https://www.novelic.com/",
            api_key=key,
            output_dir=args.output,
            config=config,
            technology_catalog=catalog,
        )
        print(result.model_dump_json(indent=2))
        return
    root = args.output.resolve()
    if root.exists() and any(root.iterdir()):
        raise ValueError("Output directory must be new or empty")
    root.mkdir(parents=True, exist_ok=True)
    records = Findings(**{objective: [] for objective in RECORD_TYPES})
    outcomes = []
    write_json(
        root / "settings.json",
        {
            "mode": "frozen_html_replay",
            "config": config.model_dump(),
            "snapshot": str(args.snapshot.resolve()),
            "catalog_version": catalog.snapshot.version,
            "catalog_synced_at": catalog.snapshot.synced_at,
            "catalog_refreshed": False,
        },
    )
    original = json.loads((args.snapshot / "result.json").read_text(encoding="utf-8"))
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = OpenRouter(client, key, config, root)
        for page_id in args.page_id:
            source = next(
                value for value in original["pages"] if value["page_id"] == page_id
            )
            page = Page.model_validate(source)
            html = (args.snapshot / page.html_file).read_text(encoding="utf-8")
            if content_hash(html) != page.html_sha256:
                raise ValueError(f"Source hash mismatch: {page_id}")
            path = root / page.html_file
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(html, encoding="utf-8")
            print(
                f"Replaying {page_id}: {page.source_url}", file=sys.stderr, flush=True
            )
            for index, window in enumerate(
                split_html(
                    html,
                    max_chars=config.chunk_chars,
                    overlap_chars=config.overlap_chars,
                )
            ):
                findings, complete, errors, assessed = await extract_window(
                    window, page, llm, root, index, catalog
                )
                for objective, finding in findings:
                    merge_finding(getattr(records, objective), finding)
                outcomes.append(
                    {
                        "page_id": page_id,
                        "window": index,
                        "complete": complete,
                        "errors": errors,
                        "assessed": sorted(assessed),
                    }
                )
                write_json(
                    root / "result.json",
                    {
                        "records": records.model_dump(),
                        "outcomes": outcomes,
                        "usage": llm.usage(),
                    },
                )
            issues = await review_claims(
                [
                    record
                    for objective in ("company_relationships", "technology_signals")
                    for record in getattr(records, objective)
                    if "interpretation_review" not in record.data
                ],
                llm,
                root,
                page_id,
            )
            outcomes.append(
                {"page_id": page_id, "stage": "claim_review", "errors": issues}
            )
            for objective in ("company_relationships", "technology_signals"):
                for correction in await correct_reviewed_claims(
                    getattr(records, objective), llm, root, f"{page_id}-{objective}"
                ):
                    merge_finding(getattr(records, objective), correction)

            write_json(
                root / "result.json",
                {
                    "records": records.model_dump(),
                    "outcomes": outcomes,
                    "usage": llm.usage(),
                },
            )
        print(records.model_dump_json(indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["replay", "live"])
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument(
        "--provider", default="parasail", help="Pin a provider; auto enables routing."
    )
    parser.add_argument("--page-id", action="append", default=[])
    parser.add_argument("--max-pages", type=int, default=16)
    parser.add_argument(
        "--reasoning", choices=["none", "low", "medium", "high"], default="low"
    )
    args = parser.parse_args()
    if args.mode == "replay" and not args.page_id:
        parser.error("replay requires at least one --page-id")
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
    with redirect_stdout(sys.stderr):
        asyncio.run(run(args))


if __name__ == "__main__":
    main()
