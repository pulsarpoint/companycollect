"""Extract and normalize the same saved native job HTML with both API endpoints."""

import argparse
import asyncio
import json
import shutil
from pathlib import Path
from typing import Literal

import httpx
from dotenv import dotenv_values

from crawler_service.llm import ModelClient
from crawler_service.models import Page, ResearchConfig
from crawler_service.statements import (
    consolidate_page_statements,
    extract_page_statements,
)
from crawler_service.storage import content_hash, utc_now, write_json
from crawler_service.technology_catalog import TechnologyCatalog


async def run(args: argparse.Namespace) -> None:
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    saved = json.loads((args.source / "input.json").read_text(encoding="utf-8"))
    page = Page.model_validate(saved["page"])
    if page.html_file is None:
        raise ValueError("Missing saved native HTML")
    html = (args.source / page.html_file).read_text(encoding="utf-8")
    if content_hash(html) != page.html_sha256:
        raise ValueError("Saved job HTML changed")
    shutil.copytree(
        Path(__file__).parents[1] / "src/crawler_service",
        root / "implementation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copy2(__file__, root / "benchmark-harness.py")
    shutil.copy2(args.catalog, root / "technology-catalog.json")
    catalog = TechnologyCatalog.read(root / "technology-catalog.json")
    write_json(root / "source.json", page.model_dump())
    # These expectations were read from native source before either model call.
    # They are only used in the offline audit, never supplied to the model.
    write_json(
        root / "controls.json",
        {
            "frozen_at": utc_now(),
            "company": "NOVELIC",
            "job_title": "Senior Data Engineer/Data Architect",
            "technology_controls": [
                {"name": name, "signal": signal, "scope": "role"}
                for signal, names in {
                    "stated_use": ["AWS", "MCAP"],
                    "required_experience": [
                        "AWS",
                        "S3",
                        "IAM",
                        "Lambda",
                        "Glue",
                        "Athena",
                        "Python",
                        "Protobuf",
                        "FlatBuffers",
                        "Linux",
                    ],
                    "preferred_experience": [
                        "ROS2",
                        "rosbag2",
                        "MCAP",
                        "Foxglove",
                        "DVC",
                        "LakeFS",
                    ],
                }.items()
                for name in names
            ],
            "exclusions": [
                "Cookie-banner analytics vendors must not be attributed to the NOVELIC role",
                "Generic AWS or other cloud certifications is not a named company certification",
                "LiDAR, radar, IMU, ETL and AI are generic disciplines/formats/categories rather than specific technology identities",
            ],
            "limit": "Targeted source-read controls, not an exhaustive independent gold dataset. Existing prompts may classify named formats differently.",
        },
    )
    keys = dotenv_values(args.env_file)

    async def arm(api: Literal["openrouter", "deepseek"]) -> None:
        output = root / api
        output.mkdir()
        html_path = output / page.html_file
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text(html, encoding="utf-8")
        config = ResearchConfig(
            model="deepseek-flash"
            if api == "deepseek"
            else "deepseek/deepseek-v4-flash-0731",
            provider=None if api == "deepseek" else "Wafer",
            reasoning_effort="low",
            statement_batch_size=8,
            max_model_calls=40,
            max_output_tokens=65536,
            model_timeout_seconds=120,
            max_review_attempts=2,
            max_corrections=1,
        )
        key_name = "DEEPSEEK" if api == "deepseek" else "OPENROUTER_API_KEY"
        key = keys.get(key_name)
        if not key:
            raise ValueError(f"{key_name} is required")
        manifest = {
            "api": api,
            "started_at": utc_now(),
            "page_fetches": 0,
            "new_page_extractions": 1,
            "config": config.model_dump(),
            "json_mode": "json_object",
        }
        write_json(output / "manifest.json", manifest)
        async with httpx.AsyncClient(
            base_url="https://api.deepseek.com/"
            if api == "deepseek"
            else "https://openrouter.ai/api/v1/"
        ) as client:
            llm = ModelClient(
                client, key, config, output, api=api, json_mode="json_object"
            )
            try:
                statements = await extract_page_statements(page, llm, output)
                write_json(
                    output / "extracted-statements.json",
                    [r.model_dump() for r in statements],
                )
                print(
                    f"{api}: extracted {len(statements)} job-page statements",
                    flush=True,
                )
                result = await consolidate_page_statements(
                    statements, [page], catalog, llm, output
                )
                write_json(output / "result.json", result)
                print(
                    f"{api}: completed job-page test, {len(llm.calls)} calls",
                    flush=True,
                )
            finally:
                manifest.update(finished_at=utc_now(), usage=llm.usage())
                write_json(output / "manifest.json", manifest)

    outcomes = await asyncio.gather(
        *(arm(api) for api in ("deepseek", "openrouter")), return_exceptions=True
    )
    errors = [str(value) for value in outcomes if isinstance(value, BaseException)]
    write_json(root / "completion.json", {"finished_at": utc_now(), "errors": errors})
    if errors:
        raise RuntimeError("Some comparison arms failed; see completion.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=Path,
        default=Path("data/mcap-multiple-relationships-v013"),
    )
    parser.add_argument(
        "--catalog",
        type=Path,
        default=Path("data/fresh-company-statements-v1/technology-catalog.json"),
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/deepseek-direct-20260910-job"),
    )
    asyncio.run(run(parser.parse_args()))
