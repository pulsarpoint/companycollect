"""Read-only MCP smoke check against ClickHouse using an existing LLM proposal draft."""

import argparse
import asyncio
import json
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from company_research.storage import write_json


async def run(args: argparse.Namespace) -> None:
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    source = json.loads(args.research_result.read_text(encoding="utf-8"))
    observation = next(
        record
        for record in source["records"]["technology_signals"]
        if record["data"]["technology"] == "ADS"
        and record["evidence_status"] == "source_matched"
    )
    draft = observation["data"]["catalog_match"]["proposed_technology"]
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "company_research.catalog_mcp",
            "--env-file",
            str(args.env_file.resolve()),
            "--technology-catalog",
            str(root / "catalog.json"),
        ],
    )
    transcript = {
        "mode": "live_database_snapshot_mcp_stdio",
        "database_writes": False,
        "description_source": str(args.research_result.resolve()),
        "tool_calls": [],
    }
    async with stdio_client(params) as streams, ClientSession(*streams) as session:
        await session.initialize()
        for name, arguments in [
            ("get_catalog_info", {}),
            (
                "search_technologies",
                {
                    "queries": ["Git", "ADS", "Advanced Design System"],
                    "context": "RF microwave circuit simulation and electronic design automation",
                },
            ),
            ("list_technology_categories", {}),
            (
                "prepare_technology_proposal",
                {
                    "observed_name": "ADS",
                    "proposal": draft,
                    "reason": "No equivalent identity found after checking the original and expanded name in the catalog; metadata comes from the saved DeepSeek draft and requires administrator review.",
                    "alternative_names": ["Advanced Design System", "Keysight ADS"],
                },
            ),
        ]:
            reply = await session.call_tool(name, arguments)
            if reply.isError or reply.structuredContent is None:
                raise ValueError(f"MCP tool {name} failed: {reply.content}")
            transcript["tool_calls"].append(
                {
                    "name": name,
                    "arguments": arguments,
                    "result": reply.structuredContent,
                }
            )
            write_json(root / "result.json", transcript)
        lookup = transcript["tool_calls"][1]["result"]["results"]
        print(
            "Catalog technologies:",
            transcript["tool_calls"][0]["result"]["technology_count"],
        )
        print(
            "ADS search candidates:",
            [entry["technology"] for entry in lookup[1]["candidates"]],
        )
        print(
            "Prepared status:",
            transcript["tool_calls"][-1]["result"]["catalog_match"]["status"],
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--research-result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(run(parser.parse_args()))
