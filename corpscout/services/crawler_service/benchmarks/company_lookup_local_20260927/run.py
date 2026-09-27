"""Run the frozen Swedish cohort through the live read-only lookup endpoint."""

import argparse
import asyncio
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from uuid import uuid4

import httpx
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
COHORT = Path(__file__).with_name("cohort.json")
OUTPUT = ROOT / "data" / "company-lookup-local-20260927"


def save(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


async def main(
    cohort_path: Path,
    output: Path,
    llm_path: Path,
    decision_path: Path | None,
    decision_tasks: list[str],
) -> None:
    environment = dotenv_values(ROOT.parent / "backoffice" / ".env")
    llm = json.loads(llm_path.read_text(encoding="utf-8"))
    decision = (
        json.loads(decision_path.read_text(encoding="utf-8")) if decision_path else None
    )
    assert bool(decision) == bool(decision_tasks), (
        "Specify both the decision profile and decision steps"
    )
    cohort = json.loads(cohort_path.read_text(encoding="utf-8"))
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        batch = "lookup-local-" + uuid4().hex[:12]
        manifest = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "selection": cohort["selection"],
            "model": llm["model"],
            "profile_id": llm["profile_id"],
            "revision": llm["profile_revision"],
            "reasoning_effort": llm.get("reasoning_effort"),
            "decision_model": {
                key: decision[key]
                for key in ["model", "profile_id", "profile_revision"]
            }
            if decision
            else None,
            "decision_tasks": decision_tasks,
            "config": {"max_pages": 4, "max_model_calls": 20},
            "concurrency": 2,
            "source_sha256": {
                name: hashlib.sha256(
                    (ROOT / "src/crawler_service" / name).read_bytes()
                ).hexdigest()
                for name in [
                    "company_lookup.py",
                    "company_search.py",
                    "company_evidence.py",
                    "company_industry.py",
                    "jev.py",
                ]
            },
            "cases": [
                case | {"request_id": f"{batch}-{i:02d}"}
                for i, case in enumerate(cohort["cases"], 1)
            ],
        }
        save(manifest_path, manifest)
    assert (
        manifest["profile_id"] == llm["profile_id"]
        and manifest["revision"] == llm["profile_revision"]
        and manifest.get("decision_tasks", []) == decision_tasks
    )
    slots = asyncio.Semaphore(2)
    async with httpx.AsyncClient(
        base_url=environment["CRAWLER_API_URL"],
        headers={"Authorization": "Bearer " + environment["CRAWLER_API_TOKEN"]},
        timeout=25,
    ) as http:

        async def run(case: dict) -> dict:
            folder = output / case["root_domain"]
            folder.mkdir(exist_ok=True)
            summary_path = folder / "summary.json"
            if summary_path.exists():
                return json.loads(summary_path.read_text(encoding="utf-8"))
            async with slots:
                rid = case["request_id"]
                started = time.monotonic()
                status = await http.get(f"/v1/crawls/{rid}/status")
                if status.status_code == 404:
                    submitted = await http.post(
                        "/v1/company-lookups",
                        json={
                            "request_id": rid,
                            "domain": case["root_domain"],
                            "country": "SE",
                            "llm": llm,
                            **(
                                {
                                    "decision_llm": decision,
                                    "decision_tasks": decision_tasks,
                                }
                                if decision
                                else {}
                            ),
                            "config": manifest["config"],
                            "interactive": False,
                        },
                    )
                    submitted.raise_for_status()
                else:
                    status.raise_for_status()
                print(
                    json.dumps(
                        {
                            "event": "submitted",
                            "domain": case["root_domain"],
                            "request_id": rid,
                        }
                    ),
                    flush=True,
                )
                timed_out = False
                while True:
                    status = await http.get(f"/v1/crawls/{rid}/status")
                    status.raise_for_status()
                    job = status.json()
                    save(folder / "status.json", job)
                    if job["state"] in {"completed", "failed", "cancelled"}:
                        break
                    if time.monotonic() - started > 600 and not timed_out:
                        cancellation = await http.post(f"/v1/crawls/{rid}/cancel")
                        cancellation.raise_for_status()
                        timed_out = True
                    await asyncio.sleep(2)
                response = await http.get(f"/v1/crawls/{rid}/result")
                response.raise_for_status()
                result = response.json()
                save(folder / "result.json", result)
                trace = await http.get(
                    f"/v1/crawls/{rid}/debug",
                    params={"attempt": job["attempt"], "download": "true"},
                )
                trace.raise_for_status()
                (folder / "trace.jsonl").write_text(trace.text, encoding="utf-8")
                if job["state"] == "cancelled":
                    # Cleanup can stall after a billed, complete lookup result.
                    # Preserve it without counting the request as completed.
                    for line in trace.text.split("\n"):
                        if line.strip():
                            event = json.loads(line)
                            if event.get("stage") == "company_lookup_result":
                                save(folder / "computed-result.json", event["details"])
                outcome = (
                    "timeout"
                    if timed_out
                    else "failed"
                    if job["state"] != "completed"
                    else "exact_match"
                    if result.get("company_id") == case["company_id"]
                    else "different_company"
                    if result.get("found")
                    else "excluded_category"
                    if result.get("stop_reason") == "not_company_website"
                    else "not_found"
                )
                summary = case | {
                    "outcome": outcome,
                    "status": job["state"],
                    "lookup_status": result.get("status"),
                    "found_id": result.get("company_id"),
                    "confidence": result.get("confidence"),
                    "site_type": result.get("site_type"),
                    "reasons": result.get("reasons", []),
                    "elapsed_seconds": result.get("elapsed_seconds"),
                    "pages": len(result.get("pages", [])),
                    "searches": len(result.get("searches", [])),
                    "usage": result.get("usage", {}),
                    "s3_state": job["s3_state"],
                    "trace_url": "http://localhost:5183/admin/crawls?"
                    + urlencode(
                        {
                            "tab": "attempts",
                            "domain": case["root_domain"],
                            "source": "rest",
                            "debug_request": rid,
                            "debug_attempt": job["attempt"],
                        }
                    ),
                }
                save(summary_path, summary)
                print(
                    json.dumps(
                        {
                            k: summary[k]
                            for k in [
                                "root_domain",
                                "outcome",
                                "found_id",
                                "site_type",
                                "elapsed_seconds",
                            ]
                        }
                    ),
                    flush=True,
                )
                return summary

        summaries = await asyncio.gather(*(run(case) for case in manifest["cases"]))
    save(
        output / "summary.json",
        {
            "manifest": manifest,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "cases": summaries,
        },
    )
    print(
        json.dumps(
            {
                "completed": len(summaries),
                "outcomes": {
                    key: sum(x["outcome"] == key for x in summaries)
                    for key in sorted({x["outcome"] for x in summaries})
                },
                "output": str(output),
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort", type=Path, default=COHORT)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument(
        "--llm-envelope",
        type=Path,
        default=Path("/tmp/company-lookup-local-envelope.json"),
    )
    parser.add_argument("--decision-envelope", type=Path)
    parser.add_argument(
        "--decision-task",
        choices=["site_eligibility", "company_match"],
        action="append",
        default=[],
    )
    arguments = parser.parse_args()
    asyncio.run(
        main(
            arguments.cohort,
            arguments.output,
            arguments.llm_envelope,
            arguments.decision_envelope,
            arguments.decision_task,
        )
    )
