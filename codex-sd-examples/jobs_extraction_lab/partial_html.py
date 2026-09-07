"""Retain valid records across correction attempts and overlapping source windows."""

import asyncio
import json
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import click
import httpx
from dotenv import dotenv_values

from jobs_extraction_lab.compare import COMPARISON_FIELDS
from jobs_extraction_lab.corpus import content_hash, utc_now, write_json
from jobs_extraction_lab.extract import (
    extract_openrouter,
    normalize_job_url,
    normalize_text,
)
from jobs_extraction_lab.format_benchmark import FORMAT_INSTRUCTIONS, format_prompt
from jobs_extraction_lab.models import JobExtraction
from jobs_extraction_lab.validated_html import (
    RETRY_INSTRUCTIONS,
    correction_prompt,
    page_link_urls,
    validate_response,
)


def retain_valid_records(
    raw: str | None, finish_reason: str | None, source_url: str, links: set[str]
) -> dict[str, Any]:
    if finish_reason != "stop" or not isinstance(raw, str):
        return {
            "accepted": [],
            "rejected": [],
            "issues": [{"type": "incomplete_response", "finish_reason": finish_reason}],
        }
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as error:
        return {
            "accepted": [],
            "rejected": [],
            "issues": [
                {
                    "type": "json_invalid",
                    "message": error.msg,
                    "line": error.lineno,
                    "column": error.colno,
                }
            ],
        }
    if (
        not isinstance(document, dict)
        or set(document) != {"jobs"}
        or not isinstance(document["jobs"], list)
    ):
        return {
            "accepted": [],
            "rejected": [],
            "issues": [
                {
                    "type": "invalid_envelope",
                    "message": "Expected an object containing only a jobs array",
                }
            ],
        }
    accepted = []
    rejected = []
    issues = []
    seen = set()
    for index, value in enumerate(document["jobs"]):
        extraction, failures = validate_response(
            json.dumps({"jobs": [value]}), "stop", source_url, links
        )
        if failures:
            failures = [{**failure, "record_index": index} for failure in failures]
            rejected.append(
                {"record_index": index, "record": value, "issues": failures}
            )
            issues.extend(failures)
            continue
        assert extraction is not None
        job = extraction.jobs[0]
        key = normalize_job_url(str(job.job_url), source_url)
        accepted.append({"record_index": index, "job": job.model_dump()})
        if key in seen:
            issues.append(
                {"type": "duplicate_job_url", "record_index": index, "url": job.job_url}
            )
        seen.add(key)
    return {"accepted": accepted, "rejected": rejected, "issues": issues}


def merge_observations(
    observations: list[dict[str, Any]], source_url: str
) -> dict[str, Any]:
    """Prefer a URL's core window, then its initial attempt; retain all alternatives."""
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for observation in observations:
        groups[normalize_job_url(observation["job"]["job_url"], source_url)].append(
            observation
        )
    jobs = []
    conflicts = []
    provenance = {}
    for url, values in groups.items():
        ranked = sorted(
            values,
            key=lambda v: (
                not v["core_url"],
                v["attempt_number"],
                v["unit_index"],
                v["record_index"],
            ),
        )
        chosen = ranked[0]
        jobs.append(chosen["job"])
        provenance[url] = [
            {k: v for k, v in value.items() if k != "job"} for value in ranked
        ]
        differing = []
        for name in COMPARISON_FIELDS:
            normalized = {
                normalize_job_url(v["job"][name], source_url)
                if name == "job_url"
                else normalize_text(v["job"][name])
                if v["job"][name] is not None
                else None
                for v in ranked
            }
            if len(normalized) > 1:
                differing.append(name)
        if differing:
            conflicts.append(
                {
                    "job_url": url,
                    "differing_fields": differing,
                    "chosen": chosen,
                    "alternatives": ranked[1:],
                }
            )
    return {"jobs": jobs, "conflicts": conflicts, "provenance": provenance}


async def run_experiment(
    data_dir: Path, run_id: str, baseline_run: str, env_file: Path, concurrency: int
) -> None:
    for name in (run_id, baseline_run):
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name) is None:
            raise ValueError("Invalid run ID")
    manifest_text = (data_dir / "manifest.json").read_text(encoding="utf-8")
    manifest = json.loads(manifest_text)
    native_dir = Path(manifest["source_dir"])
    baseline_text = (native_dir / "runs" / baseline_run / "settings.json").read_text(
        encoding="utf-8"
    )
    baseline = json.loads(baseline_text)
    if (
        content_hash((native_dir / "manifest.json").read_text(encoding="utf-8"))
        != manifest["source_manifest_sha256"]
        or baseline["manifest_sha256"] != manifest["source_manifest_sha256"]
    ):
        raise ValueError("Native source manifest differs from baseline")
    if (
        baseline["instructions"] != FORMAT_INSTRUCTIONS
        or baseline["schema"] != JobExtraction.model_json_schema()
        or baseline["temperature"] != 0
    ):
        raise ValueError("Current prompt/client differs from baseline")
    settings: dict[str, Any] = {
        **baseline,
        "window_manifest_sha256": content_hash(manifest_text),
        "baseline_run": baseline_run,
        "baseline_settings_sha256": content_hash(baseline_text),
        "concurrency": concurrency,
        "max_validation_retries": 1,
        "retry_instructions": RETRY_INSTRUCTIONS,
        "retention_policy": "Parse complete JSON; validate jobs separately; retain schema-valid jobs with absolute URLs from their own input; keep all accepted attempts; prefer core window then first attempt; retain conflicts",
    }
    settings_json = json.dumps(settings, sort_keys=True)
    root = data_dir / "runs" / run_id
    if (root / "settings.json").exists() and json.loads(
        (root / "settings.json").read_text(encoding="utf-8")
    ) != settings:
        raise ValueError("Settings changed; choose a new run ID")
    contents = {}
    for unit in manifest["units"]:
        content = (data_dir / unit["file"]).read_text(encoding="utf-8")
        if content_hash(content) != unit["sha256"]:
            raise ValueError(f"Changed input: {unit['unit_id']}")
        contents[unit["unit_id"]] = content
    api_key = dotenv_values(env_file).get("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY is missing")
    write_json(root / "settings.json", settings)
    write_json(root / "status.json", {"status": "running", "updated_at": utc_now()})
    semaphore = asyncio.Semaphore(concurrency)
    stop = asyncio.Event()
    service_errors = 0
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(settings["timeout"], connect=20)
    ) as client:

        async def extract_unit(unit: dict[str, Any]) -> None:
            nonlocal service_errors
            async with semaphore:
                if stop.is_set():
                    return
                content = contents[unit["unit_id"]]
                original = format_prompt(unit, content, settings["examples"])
                prompt = original
                links = page_link_urls(content, unit["source_url"])
                observations = []
                records: list[dict[str, Any]] = []
                for number in (1, 2):
                    path = root / "attempts" / unit["unit_id"] / f"{number}.json"
                    request_hash = content_hash(prompt + settings_json)
                    if path.exists():
                        record = json.loads(path.read_text(encoding="utf-8"))
                        if record["input_hash"] != request_hash:
                            raise ValueError(f"Attempt inputs changed: {path}")
                    else:
                        if stop.is_set():
                            if records:
                                break
                            return
                        started_at = utc_now()
                        started = time.monotonic()
                        payload = await extract_openrouter(
                            client,
                            prompt,
                            api_key=api_key,
                            model=settings["model"],
                            reasoning=settings["reasoning"],
                            provider_options=settings["provider"],
                            timeout=settings["timeout"],
                            attempts=settings["attempts"],
                            max_tokens=settings["max_tokens"],
                        )
                        choice = (payload.get("choices") or [{}])[0]
                        raw = choice.get("message", {}).get("content")
                        finish = choice.get("finish_reason")
                        parsed = retain_valid_records(
                            raw, finish, unit["source_url"], links
                        )
                        error = payload.get("error")
                        if error is not None:
                            parsed["issues"] = [
                                {"type": "service_error", "message": str(error)}
                            ]
                        record: dict[str, Any] = {
                            "unit_id": unit["unit_id"],
                            "page_id": unit["page_id"],
                            "variant": unit["variant"],
                            "attempt_number": number,
                            "input_hash": request_hash,
                            "prompt_sha256": content_hash(prompt),
                            "content_sha256": unit["sha256"],
                            "started_at": started_at,
                            "elapsed_seconds": round(time.monotonic() - started, 3),
                            "requested_model": settings["model"],
                            "actual_model": payload.get("model"),
                            "provider": payload.get("provider"),
                            "response_id": payload.get("id"),
                            "raw_response": raw,
                            "finish_reason": finish,
                            "usage": payload.get("usage"),
                            "http_attempts": payload.get("attempts", 1),
                            "accepted": parsed["accepted"],
                            "rejected": parsed["rejected"],
                            "validation_issues": parsed["issues"],
                            "error": error,
                        }
                        write_json(path, record)
                        service_errors = service_errors + 1 if error is not None else 0
                        if error is not None and (
                            service_errors >= 3
                            or any(
                                f"HTTP {s}" in str(error)
                                for s in (400, 401, 402, 403, 404, 422)
                            )
                        ):
                            write_json(
                                root / "stopped.json",
                                {"created_at": utc_now(), "reason": error},
                            )
                            stop.set()
                        click.echo(
                            f"{unit['unit_id']} attempt {number}: {len(record['accepted'])} retained; {len(record['validation_issues'])} issues; {record['elapsed_seconds']}s"
                        )
                        await asyncio.sleep(settings["interval"])
                    records.append(record)
                    observations.extend(
                        {
                            **accepted,
                            "unit_id": unit["unit_id"],
                            "unit_index": unit["unit_index"],
                            "attempt_number": number,
                            "core_url": normalize_job_url(
                                accepted["job"]["job_url"], unit["source_url"]
                            )
                            in unit["core_links"],
                        }
                        for accepted in record["accepted"]
                    )
                    if not record["validation_issues"] or record["error"] is not None:
                        break
                    prompt = correction_prompt(
                        original, record, settings["retry_instructions"]
                    )
                merged = merge_observations(observations, unit["source_url"])
                write_json(
                    root / "units" / f"{unit['unit_id']}.json",
                    {
                        "unit_id": unit["unit_id"],
                        "attempt_count": len(records),
                        "last_response_valid": not records[-1]["validation_issues"],
                        "observations": observations,
                        **merged,
                    },
                )

        await asyncio.gather(*(extract_unit(unit) for unit in manifest["units"]))
    write_json(
        root / "status.json",
        {
            "status": "stopped" if stop.is_set() else "complete",
            "updated_at": utc_now(),
            "completed_units": len(list((root / "units").glob("*.json"))),
            "planned_units": len(manifest["units"]),
        },
    )
    if stop.is_set():
        raise RuntimeError(
            "Stopped after service/routing failures; completed attempts are preserved"
        )


@click.command()
@click.option("--data-dir", type=click.Path(path_type=Path, exists=True), required=True)
@click.option("--run-id", required=True)
@click.option("--baseline-run", default="deepseek-v1")
@click.option("--env-file", type=click.Path(path_type=Path, exists=True), required=True)
@click.option("--concurrency", type=click.IntRange(1, 4), default=3)
def main(
    data_dir: Path, run_id: str, baseline_run: str, env_file: Path, concurrency: int
) -> None:
    asyncio.run(run_experiment(data_dir, run_id, baseline_run, env_file, concurrency))


if __name__ == "__main__":
    main()
