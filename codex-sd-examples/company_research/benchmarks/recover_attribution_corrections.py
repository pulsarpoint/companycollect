"""Recheck saved actor corrections after preserving unchanged heading/job anchors."""

import argparse
import asyncio
import json
import shutil
from pathlib import Path

import httpx
from dotenv import dotenv_values

from company_research.content import HtmlWindow
from company_research.llm import ModelClient
from company_research.models import Finding, Page, ResearchConfig
from company_research.review import repair_evidence
from company_research.statement_review import (
    DescriptionCheck,
    accepted_description,
    apply_description_review,
    statement_data,
)
from company_research.statements import consolidate_page_statements
from company_research.storage import content_hash, utc_now, write_json
from company_research.technology_catalog import TechnologyCatalog


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


async def run(args):
    source, root = args.snapshot.resolve(), args.output.resolve()
    parent = read(source / "manifest.json")
    if parent.get("finished_at") is None:
        raise ValueError("Wait for the parent replay to finish")
    key = dotenv_values(args.env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    root.mkdir(parents=True, exist_ok=False)
    shutil.copytree(source / "dmc/html", root / "html")
    shutil.copytree(
        Path(__file__).parents[1] / "src/company_research",
        root / "implementation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copy2(__file__, root / "recovery-harness.py")
    originals = [
        Finding.model_validate(r) for r in read(source / "dmc/input.json")["records"]
    ]
    records = {
        r["record_id"]: Finding.model_validate(r)
        for r in read(source / "dmc/result.json")["page_statements"]
    }
    originals_by_data = {
        json.dumps(statement_data(r), sort_keys=True): r for r in originals
    }
    if args.statement_id:
        if set(args.statement_id) - set(records):
            raise ValueError("Unknown selected statement ID")
        records = {
            key: value for key, value in records.items() if key in args.statement_id
        }
    pages = [Page.model_validate(p) for p in parent["cases"]["dmc"]["pages"]]
    page_map = {p.page_id: p for p in pages}
    config = ResearchConfig.model_validate(parent["cases"]["dmc"]["config"])
    config.max_model_calls = 24
    config.max_review_attempts = 2
    config.max_corrections = 1
    manifest = {
        "package_version": "0.14.3",
        "started_at": utc_now(),
        "parent": str(source),
        "page_fetches": 0,
        "new_page_extractions": 0,
        "restored_candidate_ids": [],
        "failed_candidate_checks": [],
        "config": config.model_dump(),
        "pages": [p.model_dump() for p in pages],
        "selected_statement_ids": list(records),
    }
    pending_quotes = []
    for page in pages:
        if (
            content_hash((root / page.html_file).read_text(encoding="utf-8"))
            != page.html_sha256
        ):
            raise ValueError(f"Source changed: {page.page_id}")
    for path in sorted((source / "dmc/calls").glob("*.json")):
        call = read(path)
        if (
            not call["task"].startswith("description_review:")
            or not call["task"].endswith(":0")
            or call.get("error")
            or not call.get("response")
        ):
            continue
        data = json.loads(
            call["request"]["messages"][1]["content"].split("INPUT DATA:\n", 1)[1]
        )
        active = {
            item["statement_id"]: originals_by_data[
                json.dumps(item["data"], sort_keys=True)
            ]
            for item in data["statements"]
        }
        document = json.loads(call["response"]["choices"][0]["message"]["content"])
        for value in document["checks"]:
            check = DescriptionCheck.model_validate(value)
            original = active[check.statement_id]
            if (
                original.record_id not in records
                or accepted_description(records[original.record_id])
                or check.supported
                or check.correction is None
            ):
                continue
            candidate = original.model_copy(deep=True)
            source_ref = candidate.sources[0]
            page = page_map[source_ref.page_id]
            html = (root / page.html_file).read_text(encoding="utf-8")
            review = apply_description_review(
                candidate,
                check,
                "",
                allow_correction=True,
                page=page,
                window=HtmlWindow(
                    source_ref.chunk_start,
                    source_ref.chunk_end,
                    html[source_ref.chunk_start : source_ref.chunk_end],
                ),
            )
            if candidate.data["description_review"]["status"] == "correction_pending":
                records[candidate.record_id] = candidate
                manifest["restored_candidate_ids"].append(candidate.record_id)
            else:
                quote_candidate = review.pop("correction_candidate", None)
                if quote_candidate is not None:
                    pending_quotes.append(
                        (original, check, Finding.model_validate(quote_candidate))
                    )
                manifest["failed_candidate_checks"].append(
                    {"statement_id": candidate.record_id, "review": review}
                )
    write_json(root / "manifest.json", manifest)
    write_json(root / "input.json", [r.model_dump() for r in records.values()])
    print(
        f"Restored {len(manifest['restored_candidate_ids'])} candidates for mandatory source recheck; no new actor guesses",
        flush=True,
    )
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = ModelClient(client, key, config, root)
        try:
            # These saved corrections share a source window; never merge page/window
            # boundaries in a quotation repair request.
            groups = {}
            for original, check, candidate in pending_quotes:
                ref = candidate.sources[0]
                groups.setdefault(
                    (ref.page_id, ref.chunk_start, ref.chunk_end), []
                ).append((original, check, candidate))
            for (page_id, start, end), group in groups.items():
                page = page_map[page_id]
                html = (root / page.html_file).read_text(encoding="utf-8")
                window = HtmlWindow(start, end, html[start:end])
                await repair_evidence(
                    [("page_statements", candidate) for _, _, candidate in group],
                    window,
                    page,
                    llm,
                    root,
                    f"saved-actor-corrections-{page_id}-{start}",
                )
                for original, check, candidate in group:
                    if candidate.evidence_status != "source_matched":
                        continue
                    repaired = next(
                        s
                        for s in candidate.sources
                        if s.evidence_status == "source_matched"
                    )
                    assert check.correction is not None
                    check.correction.evidence = [
                        fragment.text for fragment in repaired.evidence
                    ]
                    corrected = original.model_copy(deep=True)
                    apply_description_review(
                        corrected,
                        check,
                        "",
                        allow_correction=True,
                        page=page,
                        window=window,
                    )
                    if (
                        corrected.data["description_review"]["status"]
                        == "correction_pending"
                    ):
                        records[corrected.record_id] = corrected
                        manifest["restored_candidate_ids"].append(corrected.record_id)
            write_json(
                root / "repaired-input.json", [r.model_dump() for r in records.values()]
            )
            result = await consolidate_page_statements(
                list(records.values()),
                pages,
                TechnologyCatalog.read(source / "technology-catalog.json"),
                llm,
                root,
            )
            write_json(root / "result.json", result)
            manifest["finished_at"] = utc_now()
        finally:
            manifest["usage"] = {k: v for k, v in llm.usage().items() if k != "by_call"}
            write_json(root / "manifest.json", manifest)
    print(json.dumps(manifest["usage"]), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=Path("company_research/data/attribution-replay-v0141"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("company_research/data/attribution-recovery-v0143"),
    )
    parser.add_argument(
        "--env-file", type=Path, default=Path("jobs_extraction_lab/.env")
    )
    parser.add_argument("--statement-id", action="append", default=[])
    asyncio.run(run(parser.parse_args()))
