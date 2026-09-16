"""Run a frozen-page pilot without changing the crawler or submitting backend data."""

import argparse
import asyncio
import os
import time
from pathlib import Path
from typing import Literal

import httpx
from company_research.llm import ModelClient
from company_research.models import ResearchConfig
from company_research.storage import utc_now, write_json
from dotenv import dotenv_values

from page_agent_lab.agent import PageAgent, PageInput, validate_records
from page_agent_lab.audit import audit
from page_agent_lab.fixtures import prepare, read


async def run_arm(
    root: Path, mode: Literal["one_pass", "routed"], key: str, config: ResearchConfig
) -> dict:
    directory = root / mode
    directory.mkdir(exist_ok=False)
    manifest: dict = {
        "status": "running",
        "started_at": utc_now(),
        "config": config.model_dump(),
        "mode": mode,
        "completed_pages": [],
    }
    write_json(directory / "manifest.json", manifest)
    started = time.monotonic()
    async with httpx.AsyncClient(base_url="https://api.deepseek.com/") as client:
        llm = ModelClient(client, key, config, directory, api="deepseek")
        agent = PageAgent(llm)

        async def analyze(path: Path) -> None:
            page = PageInput.load(path)
            result = await agent.analyze(page, mode, directory / f"{path.name}.json")
            manifest["completed_pages"].append(path.name)
            write_json(directory / "manifest.json", manifest)
            print(
                f"{mode}: {path.name} finished; {sum(len(v) for v in result['data']['records'].values())} schema-valid records; {len(llm.calls)} requests started",
                flush=True,
            )

        try:
            await asyncio.gather(
                *(analyze(path) for path in sorted((root / "fixtures").iterdir()))
            )
            manifest["status"] = "finished"
        finally:
            manifest.update(
                finished_at=utc_now(),
                wall_seconds=round(time.monotonic() - started, 3),
                calls=len(llm.calls),
            )
            if manifest["status"] == "running":
                manifest["status"] = "interrupted"
            write_json(directory / "manifest.json", manifest)
    return manifest


async def routing_diagnostic(root: Path, key: str, config: ResearchConfig) -> None:
    """Call only reference-required analyses skipped by routing; never overwrite it."""
    controls = read(root / "controls.json")
    missed = set()
    for control in controls["positive"]:
        result = read(root / "routed" / f"{control['page']}.json")
        if result["data"]["coverage"][control["objective"]]["status"] == "not_selected":
            missed.add((control["page"], control["objective"]))
    directory = root / "routing_diagnostic"
    directory.mkdir(exist_ok=False)
    manifest = {
        "status": "running",
        "started_at": utc_now(),
        "missed_required_analyses": sorted(missed),
        "method": "Only source-control-required analyses skipped by the router. Selected specialist results are reused for comparison; this is not a third independent arm.",
    }
    write_json(directory / "manifest.json", manifest)
    async with httpx.AsyncClient(base_url="https://api.deepseek.com/") as client:
        llm = ModelClient(client, key, config, directory, api="deepseek")
        agent = PageAgent(llm)

        async def check(page_id: str, objective: str):
            page = PageInput.load(root / "fixtures" / page_id)
            reply = await agent.request(
                page, [objective], stage=f"diagnostic:{objective}"
            )
            doc = reply["document"] if isinstance(reply["document"], dict) else {}
            write_json(
                directory / f"{page_id}-{objective}.json",
                {
                    "response": reply,
                    "data": validate_records(doc.get("data"), [objective], page),
                },
            )

        await asyncio.gather(
            *(check(page_id, objective) for page_id, objective in sorted(missed))
        )
    manifest.update(status="finished", finished_at=utc_now(), calls=len(llm.calls))
    write_json(directory / "manifest.json", manifest)


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
    manifest = prepare(root, args.corpus.resolve(), args.dataset)
    modes: list[Literal["one_pass", "routed"]] = (
        ["one_pass", "routed"] if args.mode == "compare" else ["one_pass"]
    )
    manifest.update(
        status="prepared",
        config=config.model_dump(),
        modes=modes,
        protocol="Run the recorded modes on frozen pages with a shared concurrency cap of three. Same source payload and objective rules. No automatic corrections. No catalog calls. Whole pages are supplied without truncation.",
    )
    write_json(root / "experiment.json", manifest)
    if args.prepare_only:
        print(f"Prepared {len(manifest['pages'])} frozen pages in {root}")
        return
    key = os.environ.get("DEEPSEEK") or dotenv_values(args.env).get("DEEPSEEK")
    if not key:
        raise ValueError("DEEPSEEK credential is required")
    manifest.update(status="running", started_at=utc_now())
    write_json(root / "experiment.json", manifest)
    try:
        for mode in modes:
            await run_arm(root, mode, key, config)
        if "routed" in modes:
            await routing_diagnostic(root, key, config)
        manifest["status"] = "finished"
    finally:
        manifest["finished_at"] = utc_now()
        if manifest["status"] == "running":
            manifest["status"] = "interrupted"
        write_json(root / "experiment.json", manifest)
    report = audit(root)
    print(
        {
            mode: {
                key: value
                for key, value in report["arms"][mode].items()
                if key
                in [
                    "identity_controls",
                    "strict_controls",
                    "source_checked_controls",
                    "records",
                    "usage",
                    "wall_seconds",
                ]
            }
            for mode in report["arms"]
        },
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, default=Path("company_research/data"))
    parser.add_argument("--env", type=Path, default=Path("jobs_extraction_lab/.env"))
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--mode", choices=["one_pass", "compare"], default="one_pass")
    parser.add_argument(
        "--dataset", type=Path, help="Source snapshots and source-read controls JSON"
    )
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
