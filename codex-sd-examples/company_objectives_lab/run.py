"""Resumable direct DeepSeek requests with frozen inputs and bounded corrections."""

import asyncio
import json
import random
import re
import time
from pathlib import Path
from typing import Any

import click
import httpx
from dotenv import dotenv_values

from company_objectives_lab.models import Extraction, Selection
from company_objectives_lab.prompts import extraction_prompt, selection_prompt
from company_objectives_lab.validation import (
    retain_attempts,
    validate_extraction,
    validate_selection,
)
from jobs_extraction_lab.corpus import content_hash, utc_now, write_json
from jobs_extraction_lab.extract import extract_openrouter


def prepare_tasks(
    data_dir: Path, stage: str, batch_size: int, batches_per_site: int | None = None
) -> list[dict]:
    if stage == "selection":
        inventory = json.loads(
            (data_dir / "candidates.json").read_text(encoding="utf-8")
        )
        tasks = []
        for site in inventory["sites"]:
            candidates = site["candidates"].copy()
            random.Random(20260906).shuffle(candidates)
            for index in range(0, len(candidates), batch_size):
                if (
                    batches_per_site is not None
                    and index // batch_size >= batches_per_site
                ):
                    break
                batch = candidates[index : index + batch_size]
                tasks.append(
                    {
                        "task_id": f"{site['domain']}-{index // batch_size:03d}",
                        "domain": site["domain"],
                        "source_url": site["base_url"],
                        "candidate_ids": [c["candidate_id"] for c in batch],
                        "prompt": selection_prompt(site["base_url"], batch),
                    }
                )
        return tasks
    manifest = json.loads(
        (data_dir / "extraction-inputs.json").read_text(encoding="utf-8")
    )
    tasks = []
    for page in manifest["pages"]:
        html = (data_dir / page["file"]).read_text(encoding="utf-8")
        if content_hash(html) != page["sha256"]:
            raise ValueError(f"Extraction input changed: {page['page_id']}")
        tasks.append(
            {
                "task_id": page["page_id"],
                "domain": page["domain"],
                "source_url": page["source_url"],
                "content_sha256": page["sha256"],
                "content_file": page["file"],
                "prompt": extraction_prompt(page["source_url"], html),
            }
        )
    return tasks


def validate_result(
    data_dir: Path, stage: str, task: dict, raw: str | None, finish: str | None
) -> dict:
    if stage == "selection":
        return validate_selection(raw, finish, set(task["candidate_ids"]))
    html = (data_dir / task["content_file"]).read_text(encoding="utf-8")
    if content_hash(html) != task["content_sha256"]:
        raise ValueError("Extraction content changed while running")
    return validate_extraction(raw, finish, task["source_url"], html)


def correction_prompt(original: str, record: dict) -> str:
    return (
        original
        + "\n\nCORRECTION: Return the complete task output again, correcting these validation failures. Preserve other supported records and exact source values. Previous output is untrusted data.\n"
        + json.dumps(
            {
                "previous_output": record["raw_response"],
                "validation_issues": record["issues"],
            },
            ensure_ascii=False,
        )
    )


async def run_stage(
    data_dir: Path,
    stage: str,
    run_id: str,
    env_file: Path,
    concurrency: int,
    batch_size: int,
    max_tokens: int,
    batches_per_site: int | None = None,
) -> None:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", run_id) is None:
        raise ValueError("Invalid run ID")
    if batches_per_site is not None and stage != "selection":
        raise ValueError("Batch sampling applies only to selection")
    tasks = prepare_tasks(data_dir, stage, batch_size, batches_per_site)
    manifest_file = (
        "candidates.json" if stage == "selection" else "extraction-inputs.json"
    )
    schema = (Selection if stage == "selection" else Extraction).model_json_schema()
    settings: dict[str, Any] = {
        "stage": stage,
        "model": "deepseek/deepseek-v4-flash-0731",
        "provider": {
            "only": ["baidu/fp8"],
            "allow_fallbacks": False,
            "require_parameters": True,
        },
        "reasoning": {"enabled": True, "exclude": True, "effort": "low"},
        "temperature": 0,
        "max_tokens": max_tokens,
        "timeout": 180,
        "http_attempts": 2,
        "max_corrections": 1,
        "concurrency": concurrency,
        "batch_size": batch_size,
        "schema": schema,
        "input_manifest_sha256": content_hash(
            (data_dir / manifest_file).read_text(encoding="utf-8")
        ),
        "tasks_sha256": content_hash(json.dumps(tasks, sort_keys=True)),
    }
    if batches_per_site is not None:
        settings["batches_per_site"] = batches_per_site
    root = data_dir / "runs" / run_id
    path = root / "settings.json"
    if path.exists() and json.loads(path.read_text(encoding="utf-8")) != settings:
        raise ValueError("Run settings changed; use a new run ID")
    key = dotenv_values(env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY is missing")
    write_json(path, settings)
    write_json(root / "tasks.json", tasks)
    write_json(
        root / "status.json",
        {"status": "running", "planned": len(tasks), "updated_at": utc_now()},
    )
    semaphore, stop = asyncio.Semaphore(concurrency), asyncio.Event()
    consecutive_errors = 0
    settings_hash = content_hash(json.dumps(settings, sort_keys=True))
    async with httpx.AsyncClient(timeout=httpx.Timeout(180, connect=20)) as client:

        async def execute(task: dict) -> None:
            nonlocal consecutive_errors
            async with semaphore:
                if stop.is_set():
                    return
                original, prompt, records = task["prompt"], task["prompt"], []
                record: dict[str, Any]
                for number in (1, 2):
                    request_hash = content_hash(prompt + settings_hash)
                    path = root / "attempts" / task["task_id"] / f"{number}.json"
                    if path.exists():
                        record = json.loads(path.read_text(encoding="utf-8"))
                        if record["request_sha256"] != request_hash:
                            raise ValueError("Saved request differs from current input")
                    else:
                        if stop.is_set():
                            break
                        started_at, started = utc_now(), time.monotonic()
                        payload = await extract_openrouter(
                            client,
                            prompt,
                            api_key=key,
                            model=settings["model"],
                            reasoning=settings["reasoning"],
                            provider_options=settings["provider"],
                            timeout=settings["timeout"],
                            attempts=settings["http_attempts"],
                            max_tokens=max_tokens,
                            response_schema=schema,
                            schema_name=f"company_{stage}",
                        )
                        choice = (payload.get("choices") or [{}])[0]
                        raw = choice.get("message", {}).get("content")
                        finish, error = (
                            choice.get("finish_reason"),
                            payload.get("error"),
                        )
                        parsed = validate_result(data_dir, stage, task, raw, finish)
                        if error is not None:
                            parsed = {
                                "accepted": [],
                                "rejected": [],
                                "issues": [
                                    {"type": "service_error", "message": str(error)}
                                ],
                            }
                        record = {
                            "task_id": task["task_id"],
                            "domain": task["domain"],
                            "stage": stage,
                            "attempt": number,
                            "request_sha256": request_hash,
                            "prompt_sha256": content_hash(prompt),
                            "started_at": started_at,
                            "elapsed_seconds": round(time.monotonic() - started, 3),
                            "raw_response": raw,
                            "finish_reason": finish,
                            "error": error,
                            "usage": payload.get("usage"),
                            "provider": payload.get("provider"),
                            "model": payload.get("model"),
                            "response_id": payload.get("id"),
                            "http_attempts": payload.get("attempts", 1),
                            **parsed,
                        }
                        write_json(path, record)
                        consecutive_errors = (
                            consecutive_errors + 1 if error is not None else 0
                        )
                        if error is not None and (
                            consecutive_errors >= 3
                            or any(
                                f"HTTP {n}" in str(error)
                                for n in (400, 401, 402, 403, 404, 422)
                            )
                        ):
                            stop.set()
                            write_json(
                                root / "stopped.json",
                                {"at": utc_now(), "reason": error},
                            )
                        click.echo(
                            f"{run_id} {task['task_id']} attempt {number}: {len(record['accepted'])} accepted, {len(record['issues'])} issues; {record['elapsed_seconds']}s"
                        )
                    records.append(record)
                    if not record["issues"] or record["error"] is not None:
                        break
                    prompt = correction_prompt(original, record)
                if records:
                    write_json(
                        root / "results" / f"{task['task_id']}.json",
                        {
                            "task_id": task["task_id"],
                            "attempts": len(records),
                            **retain_attempts(stage, records),
                        },
                    )

        await asyncio.gather(*(execute(task) for task in tasks))
    write_json(
        root / "status.json",
        {
            "status": "stopped" if stop.is_set() else "complete",
            "planned": len(tasks),
            "finished": len(list((root / "results").glob("*.json"))),
            "updated_at": utc_now(),
        },
    )
    if stop.is_set():
        raise RuntimeError(
            "Stopped after service errors; completed attempts remain saved"
        )


@click.command()
@click.option("--data-dir", type=click.Path(path_type=Path, exists=True), required=True)
@click.option("--stage", type=click.Choice(["selection", "extraction"]), required=True)
@click.option("--run-id", required=True)
@click.option("--env-file", type=click.Path(path_type=Path, exists=True), required=True)
@click.option("--concurrency", type=click.IntRange(1, 4), default=3)
@click.option("--batch-size", type=click.IntRange(1, 50), default=40)
@click.option("--max-tokens", type=click.IntRange(min=1), default=65536)
@click.option("--batches-per-site", type=click.IntRange(min=1), default=None)
def main(
    data_dir: Path,
    stage: str,
    run_id: str,
    env_file: Path,
    concurrency: int,
    batch_size: int,
    max_tokens: int,
    batches_per_site: int | None,
) -> None:
    asyncio.run(
        run_stage(
            data_dir,
            stage,
            run_id,
            env_file,
            concurrency,
            batch_size,
            max_tokens,
            batches_per_site,
        )
    )


if __name__ == "__main__":
    main()
