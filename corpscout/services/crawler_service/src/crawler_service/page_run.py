"""Analyze saved pages and return complete research JSON."""

import argparse
import asyncio
import json
import os
import sys
from contextlib import nullcontext, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

import click
from dotenv import dotenv_values

from crawler_service import catalog_config
from crawler_service.analysis import analyze_pages
from crawler_service.captures import open_crawl
from crawler_service.models import ResearchConfig


async def run(args: argparse.Namespace) -> dict:
    if args.crawl is not None:
        with open_crawl(args.crawl):
            pass
    root = args.output.resolve()
    if root.exists():
        raise ValueError(f"Analysis output directory already exists: {root}")
    config = ResearchConfig(
        model=args.model,
        provider=args.provider,
        reasoning_effort=args.reasoning_effort,
        max_model_calls=150,
        model_timeout_seconds=args.timeout,
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
    credential = "DEEPSEEK" if args.api == "deepseek" else "OPENROUTER_API_KEY"
    key = environment.get(credential)
    if not key:
        raise ValueError(f"{credential} credential is required")
    catalog_error = None
    try:
        catalog = catalog_config.load_catalog(
            environment,
            args.catalog,
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
    return await analyze_pages(
        args.crawl if args.crawl is not None else args.page,
        output_dir=root,
        config=config,
        api_key=key,
        api=args.api,
        catalog=catalog,
        catalog_info=catalog_info,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument(
        "--page",
        type=Path,
        action="append",
        help="Frozen directory containing input.json and page.html; repeat for more pages",
    )
    inputs.add_argument(
        "--crawl",
        type=Path,
        help="Crawl folder, crawl-manifest.json or result.json; analyze without fetching pages",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional directory for local diagnostics and results",
    )
    parser.add_argument("--env-file", type=Path, action="append", default=[])
    parser.add_argument("--catalog", type=Path)
    parser.add_argument("--offline-catalog", action="store_true")
    parser.add_argument("--api", choices=["deepseek", "openrouter"], default="deepseek")
    parser.add_argument(
        "--model", help="Model ID; defaults to deepseek-flash for DeepSeek"
    )
    parser.add_argument(
        "--provider", help="Pin an OpenRouter provider without fallback"
    )
    parser.add_argument(
        "--timeout", type=float, default=300, help="Per-request deadline in seconds"
    )
    parser.add_argument(
        "--reasoning-effort", choices=["none", "low", "medium", "high"], default="high"
    )
    args = parser.parse_args()
    if args.model is None:
        if args.api == "openrouter":
            parser.error("--model is required for OpenRouter")
        args.model = "deepseek-flash"
    if args.api == "deepseek" and args.provider is not None:
        parser.error("--provider applies only to OpenRouter")
    with (
        TemporaryDirectory(prefix="crawler-service-")
        if args.output is None
        else nullcontext()
    ) as temporary:
        if temporary is not None:
            args.output = Path(temporary) / "run"
        try:
            with redirect_stdout(sys.stderr):
                result = asyncio.run(run(args))
        except (ValueError, OSError, KeyError) as error:
            parser.error(str(error))
    click.echo(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
