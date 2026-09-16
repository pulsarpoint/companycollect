"""Frozen NOVELIC source replay: separate evidence, interpretation and proposal metadata."""

import argparse
import asyncio
import json
import shutil
from collections import Counter
from pathlib import Path

import httpx
from dotenv import dotenv_values

from company_research.analytics import (
    accepted_finding,
    source_supported_finding,
    technology_submission_records,
)
from company_research.content import HtmlWindow, source_finding
from company_research.llm import ModelClient
from company_research.models import Finding, ResearchResult, TechnologySignal
from company_research.research import process_technology_metadata
from company_research.review import correct_reviewed_claims, review_claims
from company_research.storage import content_hash, utc_now, write_json
from company_research.technology_catalog import TechnologyCatalog


def prepare(source: Path, root: Path) -> dict:
    if root.exists():
        raise ValueError(
            "Output directory must be new; use --resume for the saved experiment"
        )
    baseline_text = (source / "result.json").read_text(encoding="utf-8")
    baseline = ResearchResult.model_validate_json(baseline_text)
    audit = json.loads(
        (source / "source-audit-issues.json").read_text(encoding="utf-8")
    )
    cohorts = {
        issue["kind"]: issue["record_ids"]
        for issue in audit["issues"]
        if issue["kind"]
        in {
            "expertise_is_not_deployed_stack",
            "proposal_metadata_losses",
        }
    }
    jobs = [
        r.record_id
        for r in baseline.records.technology_signals
        if accepted_finding(r) and r.data["job_title"] is not None
    ]
    engineering = [
        r.record_id
        for r in baseline.records.technology_signals
        if any(s.page_id in {"p0002", "p0003"} for s in r.sources)
    ]
    selected = set(jobs + engineering + cohorts["proposal_metadata_losses"])
    records = [
        r.model_copy(deep=True)
        for r in baseline.records.technology_signals
        if r.record_id in selected
    ]
    root.mkdir(parents=True)
    shutil.copytree(source / "html", root / "html")
    shutil.copy2(source / "technology-catalog.json", root / "technology-catalog.json")
    shutil.copytree(
        Path(__file__).parents[1] / "src" / "company_research",
        root / "implementation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copy2(__file__, root / "replay-harness.py")
    pages = {p.page_id: p for p in baseline.pages}
    verification = []
    for record in records:
        verified = False
        for source_record in record.sources:
            page = pages[source_record.page_id]
            html = (root / page.html_file).read_text(encoding="utf-8")
            if content_hash(html) != source_record.html_sha256:
                raise ValueError(f"Changed native HTML: {source_record.page_id}")
            if source_record.evidence_status != "source_matched":
                continue
            data = {
                key: record.data[key]
                for key in TechnologySignal.model_fields
                if key != "evidence"
            }
            rechecked = source_finding(
                "technology_signals",
                data | {"evidence": [q.text for q in source_record.evidence]},
                page=page.model_dump(),
                window=HtmlWindow(
                    source_record.chunk_start,
                    source_record.chunk_end,
                    html[source_record.chunk_start : source_record.chunk_end],
                ),
            )
            verified |= rechecked.evidence_status == "source_matched"
        verification.append(
            {"record_id": record.record_id, "source_revalidated": verified}
        )
        # Old metadata rejection changed the aggregate evidence flag. Restore only
        # revalidated quotations for a fresh source-meaning review, never acceptance.
        if verified:
            record.evidence_status = "source_matched"
        record.data["required_reviews"] = sorted(
            {*record.data.get("required_reviews", []), "source_meaning"}
        )
    write_json(
        root / "baseline-records.json",
        [
            r.model_dump()
            for r in baseline.records.technology_signals
            if r.record_id in selected
        ],
    )
    write_json(
        root / "expectations.json",
        {
            "frozen_before_model_calls": True,
            "cohorts": cohorts,
            "job_signal_controls": jobs,
            "engineering_records": engineering,
            "checks": [
                "The 19 previously accepted engineering claims must become advertised_expertise, not stated_use.",
                "Job-control signal and scope must remain unchanged; a role responsibility may remain stated_use.",
                "Thirty-two proposal rejections are an exploratory recovery cohort, not thirty-two expected approvals.",
                "Bad evidence, generic identity and unsupported vendor/identity assertions must remain blocked.",
                "Metadata repair may change only descriptions/categories; observed names, signal and quotations are immutable.",
                "No page fetches, PDF processing, catalog writes or administrator approvals in this replay.",
            ],
        },
    )
    config = baseline.config.model_copy(
        update={
            "max_model_calls": 60,
            "model_timeout_seconds": 120.0,
            "max_http_attempts": 2,
            "max_review_attempts": 2,
            "max_proposal_corrections": 1,
        }
    )
    state = {
        "schema_version": "1.8",
        "mode": "saved_technology_metadata_replay",
        "package_version": "0.10.0",
        "baseline_result": str((source / "result.json").resolve()),
        "baseline_sha256": content_hash(baseline_text),
        "started_at": utc_now(),
        "finished_at": None,
        "config": config.model_dump(),
        "source_verification": verification,
        "completed_stages": [],
        "issues": [],
        "records": [r.model_dump() for r in records],
        "usage": {"by_call": []},
        "page_fetches": 0,
        "jobs_unchanged_sha256": content_hash(
            json.dumps([r.model_dump() for r in baseline.records.jobs], sort_keys=True)
        ),
    }
    write_json(root / "result.json", state)
    return state


def stats(state: dict, root: Path) -> dict:
    records = [Finding.model_validate(r) for r in state["records"]]
    expectations = json.loads((root / "expectations.json").read_text(encoding="utf-8"))
    before = {
        r["record_id"]: Finding.model_validate(r)
        for r in json.loads(
            (root / "baseline-records.json").read_text(encoding="utf-8")
        )
    }
    rows = []
    for original_id, original in before.items():
        versions = [
            r
            for r in records
            if r.record_id == original_id or r.data.get("correction_of") == original_id
        ]
        accepted = [r for r in versions if accepted_finding(r)]
        rows.append(
            {
                "record_id": original_id,
                "technology": original.data["technology"],
                "source_page": original.sources[0].page_id,
                "before_accepted": accepted_finding(original),
                "before_signal": original.data["signal"],
                "before_scope": original.data["scope"],
                "after_accepted": bool(accepted),
                "after_signals": [r.data["signal"] for r in accepted],
                "after_scopes": [r.data["scope"] for r in accepted],
                "after_record_ids": [r.record_id for r in accepted],
                "versions": [
                    {
                        "record_id": r.record_id,
                        "source_supported": source_supported_finding(r),
                        "source_review": r.data.get("interpretation_review"),
                        "catalog_error": r.data.get("catalog_error"),
                        "proposal_review": r.data.get("proposal_review"),
                        "signal": r.data["signal"],
                    }
                    for r in versions
                ],
            }
        )
    by_id = {row["record_id"]: row for row in rows}
    cohorts = {
        name: {
            "total": len(ids),
            "accepted_before": sum(by_id[i]["before_accepted"] for i in ids),
            "accepted_after": sum(by_id[i]["after_accepted"] for i in ids),
        }
        for name, ids in expectations["cohorts"].items()
    }
    expertise_ids = expectations["cohorts"]["expertise_is_not_deployed_stack"]
    controls = expectations["job_signal_controls"]
    summary = {
        "cohorts": cohorts,
        "expertise_corrected": sum(
            by_id[i]["after_signals"] == ["advertised_expertise"] for i in expertise_ids
        ),
        "job_controls": len(controls),
        "job_controls_preserved": sum(
            by_id[i]["after_signals"] == [by_id[i]["before_signal"]]
            and by_id[i]["after_scopes"] == [by_id[i]["before_scope"]]
            for i in controls
        ),
        "selected_original_records": len(before),
        "output_records_including_rejected_originals": len(records),
        "accepted_records": sum(accepted_finding(r) for r in records),
        "source_supported_records": sum(source_supported_finding(r) for r in records),
        "accepted_signals": dict(
            Counter(r.data["signal"] for r in records if accepted_finding(r))
        ),
        "repaired_records": sum(
            bool(r.data.get("proposal_metadata_repairs")) for r in records
        ),
        "usage": {k: v for k, v in state["usage"].items() if k != "by_call"},
        "calls_by_stage": dict(
            Counter(c["task"].split(":")[0] for c in state["usage"]["by_call"])
        ),
        "page_fetches": 0,
        "rows": rows,
    }
    write_json(root / "comparison.json", summary)
    write_json(
        root / "technology-submission-preview.json",
        technology_submission_records(records),
    )
    return summary


async def run(args):
    root = args.output.resolve()
    state = (
        json.loads((root / "result.json").read_text(encoding="utf-8"))
        if args.resume
        else prepare(args.snapshot.resolve(), root)
    )
    if args.prepare_only:
        print(
            json.dumps(
                {
                    "selected": len(state["records"]),
                    "sources_revalidated": sum(
                        r["source_revalidated"] for r in state["source_verification"]
                    ),
                }
            ),
            flush=True,
        )
        return
    key = dotenv_values(args.env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    from company_research.models import ResearchConfig

    records = [Finding.model_validate(r) for r in state["records"]]
    catalog = TechnologyCatalog.read(root / "technology-catalog.json")
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = ModelClient(
            client, key, ResearchConfig.model_validate(state["config"]), root
        )
        llm.calls = state["usage"]["by_call"]

        def save():
            state["records"] = [r.model_dump() for r in records]
            state["usage"] = llm.usage()
            write_json(root / "result.json", state)

        for stage in ["source_review", "signal_correction", "metadata"]:
            if stage in state["completed_stages"]:
                continue
            print(
                f"Start {stage}: {len(records)} saved records, {len(llm.calls)} calls so far",
                flush=True,
            )
            if stage == "source_review":
                state["issues"].extend(
                    await review_claims(records, llm, root, "replay-source")
                )
            elif stage == "signal_correction":
                records.extend(
                    await correct_reviewed_claims(records, llm, root, "replay-signals")
                )
            else:
                state["issues"].extend(
                    await process_technology_metadata(
                        records, catalog, llm, root, "replay-metadata"
                    )
                )
            state["completed_stages"].append(stage)
            save()
            print(
                f"Finished {stage}: {len(llm.calls)} calls, ${llm.usage()['known_cost_usd']:.6f}",
                flush=True,
            )
        state["finished_at"] = utc_now()
        save()
    comparison = stats(state, root)
    print(
        json.dumps({k: v for k, v in comparison.items() if k != "rows"}, indent=2),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=Path("company_research/data/novelic-autonomous-v17-completed"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("company_research/data/novelic-metadata-v18"),
    )
    parser.add_argument(
        "--env-file", type=Path, default=Path("jobs_extraction_lab/.env")
    )
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--resume", action="store_true")
    asyncio.run(run(parser.parse_args()))
