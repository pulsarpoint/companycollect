"""Bounded replay of saved semantic failures; never fetches a website or submits data."""

import argparse
import asyncio
import json
import shutil
import tomllib
from pathlib import Path

import httpx
from dotenv import dotenv_values

from crawler_service.analytics import accepted_finding
from crawler_service.content import HtmlWindow
from crawler_service.llm import ModelClient
from crawler_service.models import Finding, Page, ResearchConfig
from crawler_service.review import repair_evidence, review_claims
from crawler_service.statements import (
    PageStatement,
    consolidate_page_statements,
    statement_finding,
)
from crawler_service.storage import content_hash, utc_now, write_json
from crawler_service.technology_catalog import TechnologyCatalog


def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


async def run(args):
    baseline, root = args.snapshot.resolve(), args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    api = getattr(args, "api", "openrouter")
    json_mode = getattr(args, "json_mode", None)
    key_name = "DEEPSEEK" if api == "deepseek" else "OPENROUTER_API_KEY"
    key = dotenv_values(args.env_file).get(key_name)
    if not key:
        raise ValueError(f"{key_name} is required")
    base_url = (
        "https://api.deepseek.com/"
        if api == "deepseek"
        else "https://openrouter.ai/api/v1/"
    )
    shutil.copytree(
        Path(__file__).parents[1] / "src/crawler_service",
        root / "implementation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copy2(__file__, root / "replay-harness.py")
    shutil.copy2(baseline / "technology-catalog.json", root / "technology-catalog.json")
    catalog = TechnologyCatalog.read(root / "technology-catalog.json")
    manifest: dict = {
        "started_at": utc_now(),
        "package_version": tomllib.loads(
            (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
        )["project"]["version"],
        "api": api,
        "base_url": base_url,
        "json_mode": json_mode
        or ("json_object" if api == "deepseek" else "json_schema"),
        "page_fetches": 0,
        "new_page_extractions": 0,
        "baseline": str(baseline),
        "cases": {},
    }
    semaphore = asyncio.Semaphore(2)

    async def replay(name):
        async with semaphore:
            output = root / name
            output.mkdir()
            if name == "mcap":
                source = baseline.parent / "mcap-multiple-relationships-v013"
                saved = read(source / "input.json")
                records = [Finding.model_validate(saved["statement"])]
                pages = [Page.model_validate(saved["page"])]
                config = ResearchConfig.model_validate(
                    read(source / "result.json")["config"]
                )
                original_accepted = []
            else:
                source = baseline / name / "crawl"
                crawl = read(source / "result.json")
                config = ResearchConfig.model_validate(crawl["config"])
                pages = [
                    Page.model_validate(p)
                    for p in crawl["pages"]
                    if p["fetch_status"] == "fetched"
                ]
                saved = read(baseline / name / "statements/statement-result.json")
                original_accepted = [
                    r
                    for r in saved["technology_signals"]
                    if r["record_id"]
                    in saved["accepted_record_ids"]["technology_signals"]
                ]
                ids = {
                    key for r in original_accepted for key in r["data"]["statement_ids"]
                }
                if name == "dmc":
                    ids.update(["28b60f1e0a223edd1cb06982", "a36085bdb2976fd83a1af2b9"])
                records = [
                    Finding.model_validate(r)
                    for r in saved["page_statements"]
                    if r["record_id"] in ids
                ]
            if name == "plausible":
                ids = {
                    "a408411d0c3b675c448a178e",
                    "57cdbe1eeb33e66f2cb9ab74",
                    "07ffb3ae07c762a474ba6474",
                    "ec28f99817817f087c6713b1",
                }
                pairs = [
                    (objective, Finding.model_validate(r))
                    for objective in ["company_profile", "people"]
                    for r in crawl["records"][objective]
                    if r["record_id"] in ids
                ]
                records = [r for _, r in pairs]
            relevant_pages = {s.page_id for r in records for s in r.sources}
            pages = [p for p in pages if p.page_id in relevant_pages]
            (output / "html").mkdir()
            for page in pages:
                html = (source / page.html_file).read_text(encoding="utf-8")
                if content_hash(html) != page.html_sha256:
                    raise ValueError(f"Source changed: {name}/{page.page_id}")
                shutil.copy2(source / page.html_file, output / page.html_file)
            config.provider = None if api == "deepseek" else "Wafer"
            if getattr(args, "model", None) is not None:
                config.model = args.model
            elif api == "deepseek":
                config.model = "deepseek-flash"
            config.reasoning_effort = "low"
            config.max_model_calls = {
                "dmc": 40,
                "thoughtbot": 28,
                "mcap": 8,
                "plausible": 6,
            }[name]
            config.max_review_attempts = 2
            config.max_corrections = 1
            config.statement_batch_size = 8
            config.model_timeout_seconds = 120.0
            config.max_output_tokens = 65536
            case = {
                "config": config.model_dump(),
                "pages": [p.model_dump() for p in pages],
                "selected_record_ids": [r.record_id for r in records],
                "started_at": utc_now(),
            }
            manifest["cases"][name] = case
            write_json(root / "manifest.json", manifest)
            write_json(
                output / "input.json",
                {
                    "records": [r.model_dump() for r in records],
                    "original_accepted": original_accepted,
                },
            )
            print(f"{name}: {len(records)} saved records, no page fetches", flush=True)
            async with httpx.AsyncClient(base_url=base_url) as client:
                llm = ModelClient(
                    client, key, config, output, api=api, json_mode=json_mode
                )
                try:
                    if name == "plausible":
                        # A full saved homepage supplies separate brand and fact fragments.
                        page = next(p for p in pages if p.page_id == "p0001")
                        html = (output / page.html_file).read_text(encoding="utf-8")
                        errors = await repair_evidence(
                            pairs,
                            HtmlWindow(0, len(html), html),
                            page,
                            llm,
                            output,
                            "saved-company-anchors",
                        )
                        errors.extend(
                            await review_claims(
                                [
                                    r
                                    for objective, r in pairs
                                    if objective == "company_profile"
                                ],
                                llm,
                                output,
                                "saved-company-facts",
                            )
                        )
                        result = {
                            "records": [
                                {
                                    "objective": objective,
                                    "record": r.model_dump(),
                                    "accepted": accepted_finding(r),
                                }
                                for objective, r in pairs
                            ],
                            "errors": errors,
                            "usage": llm.usage(),
                            "people_review": "Quotation anchors only; human source audit required",
                        }
                    else:
                        # Recheck stored quotations using current punctuation handling. Preserve
                        # statement IDs and original inputs; the LLM, not this harness, repairs actors.
                        page_map = {p.page_id: p for p in pages}
                        for record in records:
                            source_ref = record.sources[0]
                            page = page_map[source_ref.page_id]
                            html = (output / page.html_file).read_text(encoding="utf-8")
                            data = PageStatement.model_validate(
                                {
                                    k: record.data[k]
                                    for k in PageStatement.model_fields
                                    if k != "evidence"
                                }
                                | {
                                    "evidence": [s.text for s in source_ref.evidence][
                                        -8:
                                    ]
                                }
                            )
                            checked = statement_finding(
                                data,
                                page,
                                HtmlWindow(
                                    source_ref.chunk_start,
                                    source_ref.chunk_end,
                                    html[source_ref.chunk_start : source_ref.chunk_end],
                                ),
                            )
                            record.sources = checked.sources
                            record.evidence_status = checked.evidence_status
                        result = await consolidate_page_statements(
                            records, pages, catalog, llm, output
                        )
                    write_json(output / "result.json", result)
                    case["finished_at"] = utc_now()
                    usage = llm.usage()
                    reported_cost = (
                        f"${usage['known_cost_usd']:.6f}"
                        if usage["unknown_cost_calls"] < usage["calls"]
                        else "unavailable"
                    )
                    print(
                        f"{api}/{name}: finished; calls={len(llm.calls)}, reported_cost={reported_cost}, unknown_cost_calls={usage['unknown_cost_calls']}",
                        flush=True,
                    )
                finally:
                    case["usage"] = {
                        k: v for k, v in llm.usage().items() if k != "by_call"
                    }
                    write_json(
                        output / "last-records.json", [r.model_dump() for r in records]
                    )
                    write_json(root / "manifest.json", manifest)

    outcomes = await asyncio.gather(
        *(replay(name) for name in ["dmc", "thoughtbot", "plausible", "mcap"]),
        return_exceptions=True,
    )
    manifest["errors"] = [
        str(value) for value in outcomes if isinstance(value, BaseException)
    ]
    manifest["finished_at"] = utc_now()
    write_json(root / "manifest.json", manifest)
    print(
        json.dumps(
            {"finished_at": manifest["finished_at"], "errors": manifest["errors"]}
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=Path("data/fresh-company-statements-v1"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/attribution-replay-v0141"),
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--api", choices=["openrouter", "deepseek"], default="openrouter"
    )
    parser.add_argument("--model")
    parser.add_argument("--json-mode", choices=["json_schema", "json_object"])
    asyncio.run(run(parser.parse_args()))
