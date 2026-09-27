"""Compare the same saved DeepSeek requests with thinking disabled."""

import asyncio
import json
import sys
from pathlib import Path

import click
import httpx

from benchmarks.compare_jev import recorded_post
from crawler_service.storage import utc_now, write_json


async def run(source: Path, output: Path, credentials: dict) -> None:
    output.mkdir(parents=True, exist_ok=False)
    paths = sorted(source.glob("*.json"))
    semaphore = asyncio.Semaphore(4)
    write_json(
        output / "experiment.json",
        {
            "started_at": utc_now(),
            "source": str(source.resolve()),
            "calls": len(paths),
            "change": "Identical saved request with thinking disabled and temperature=0.",
        },
    )
    async with httpx.AsyncClient(timeout=240) as client:

        async def replay(path: Path) -> None:
            async with semaphore:
                body = json.loads(path.read_text(encoding="utf-8"))["request"]
                body.update(thinking={"type": "disabled"}, temperature=0)
                body.pop("reasoning_effort", None)
                result = await recorded_post(
                    client,
                    credentials["deepseek"]["base_url"].rstrip("/")
                    + "/chat/completions",
                    credentials["deepseek"]["api_key"],
                    body,
                    output / "calls" / path.name,
                )
                if result["choices"][0]["finish_reason"] != "stop":
                    raise ValueError(f"Incomplete response: {path.name}")
                json.loads(result["choices"][0]["message"]["content"])
                click.echo(
                    f"Non-thinking replay {path.name}: {result['usage']['total_tokens']} tokens"
                )

        results = await asyncio.gather(
            *(replay(path) for path in paths), return_exceptions=True
        )
        errors = [
            {"file": path.name, "error": f"{type(result).__name__}: {result}"}
            for path, result in zip(paths, results, strict=True)
            if isinstance(result, BaseException)
        ]
        write_json(
            output / "completion.json",
            {"finished_at": utc_now(), "calls": len(paths), "errors": errors},
        )
        if errors:
            raise RuntimeError(f"{len(errors)} failed replays; see completion.json")


@click.command()
@click.argument("source", type=click.Path(exists=True, path_type=Path))
@click.argument("output", type=click.Path(path_type=Path))
def main(source: Path, output: Path) -> None:
    asyncio.run(run(source, output, json.load(sys.stdin)))


if __name__ == "__main__":
    main()
