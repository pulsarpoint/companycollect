"""Recover an overview from saved accepted facts, without further crawling."""

import argparse
import asyncio
from pathlib import Path

import httpx
from dotenv import dotenv_values

from company_research.llm import ModelClient
from company_research.models import ResearchResult
from company_research.profiles import summarize_company
from company_research.storage import content_hash, utc_now, write_json


async def run(args):
    if args.output.exists():
        raise ValueError("Output directory must be new")
    result = ResearchResult.model_validate_json(args.result.read_bytes())
    args.output.mkdir(parents=True)
    result.config.provider = None
    key = dotenv_values(args.env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    settings = {
        "mode": "overview_only_recovery",
        "input_result": str(args.result.resolve()),
        "input_sha256": content_hash(args.result.read_text()),
        "started_at": utc_now(),
        "model": result.config.model,
        "provider": "automatic OpenRouter routing",
        "initial_calls": result.usage["calls"],
        "no_additional_fetches": True,
    }
    write_json(args.output / "settings.json", settings)
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = ModelClient(client, key, result.config, args.output.resolve())
        llm.calls = result.usage["by_call"]
        try:
            result.company_overview = await summarize_company(
                result, llm, args.output.resolve()
            )
        except Exception as error:
            result.errors.append(
                {
                    "stage": "overview_recovery",
                    "error": f"{type(error).__name__}: {str(error).replace(key, '[REDACTED]')}",
                }
            )
        result.usage = llm.usage()
        result.finished_at = utc_now()
        result.discovery["overview_recovery"] = settings | {
            "finished_at": result.finished_at,
            "final_calls": result.usage["calls"],
            "succeeded": result.company_overview is not None,
        }
        write_json(args.output / "result.json", result.model_dump())
        print(result.discovery["overview_recovery"], flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--env-file", type=Path, required=True)
    asyncio.run(run(parser.parse_args()))
