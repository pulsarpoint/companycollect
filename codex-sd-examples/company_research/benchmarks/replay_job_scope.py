"""Replay saved normalization decisions after job-scope coalescing, then review recovered claims."""

import argparse
import asyncio
import json
import shutil
from pathlib import Path

import httpx
from dotenv import dotenv_values

from company_research.analytics import accepted_finding
from company_research.llm import ModelClient
from company_research.models import Finding, Page, ResearchConfig
from company_research.research import process_technology_metadata
from company_research.review import review_claims
from company_research.statements import (
    accept_statement_decisions,
    correct_statement_interpretations,
    merge_statement_observation,
)
from company_research.storage import utc_now, write_json
from company_research.technology_catalog import TechnologyCatalog


async def replay(args):
    original = args.source.resolve()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    saved = json.loads((original / "result.json").read_text())
    statements = {
        r["record_id"]: Finding.model_validate(r) for r in saved["page_statements"]
    }
    pending = set(saved["pending_statement_ids"])
    pages = [
        Page.model_validate(p)
        for p in json.loads((original / "pages.json").read_text())
    ]
    shutil.copytree(original / "html", root / "html")
    shutil.copy2(original / "technology-catalog.json", root / "technology-catalog.json")
    shutil.copy2(__file__, root / "harness.py")
    shutil.copytree(
        Path(__file__).parents[1] / "src/company_research",
        root / "implementation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    write_json(root / "pages.json", [p.model_dump() for p in pages])
    calls = [
        json.loads(p.read_text()) for p in sorted((original / "calls").glob("*.json"))
    ]
    recovered = []
    decisions = []
    for batch_file in sorted((original / "statement-normalization").glob("*.json")):
        batch = json.loads(batch_file.read_text())
        aliases = {
            f"s{i}": statements[key] for i, key in enumerate(batch["statement_ids"], 1)
        }
        for call in calls:
            if not call["task"].startswith(f"normalize_statements:{batch_file.stem}:"):
                continue
            content = (
                ((call.get("response") or {}).get("choices") or [{}])[0]
                .get("message", {})
                .get("content")
            )
            if not content:
                continue
            try:
                document = json.loads(content)
            except ValueError:
                continue
            for alias, record in aliases.items():
                if record.record_id not in pending or alias not in document.get(
                    "decisions", {}
                ):
                    continue
                tech, certs, exclusions, issues = accept_statement_decisions(
                    {"decisions": {alias: document["decisions"][alias]}},
                    {alias: record},
                    {p.page_id: p for p in pages},
                    root,
                )
                decisions.append(
                    {
                        "statement_id": record.record_id,
                        "original_call_id": call["call_id"],
                        "issues": issues,
                        "recovered_record_ids": [r.record_id for r in tech],
                    }
                )
                if issues or certs or exclusions or not tech:
                    continue
                pending.remove(record.record_id)
                for finding in tech:
                    merge_statement_observation(recovered, finding)
    write_json(
        root / "offline-replay.json",
        {
            "source": str(original),
            "decisions": decisions,
            "pending_statement_ids": sorted(pending),
            "technology_signals": [r.model_dump() for r in recovered],
        },
    )
    config = ResearchConfig(
        model="deepseek-flash",
        provider=None,
        reasoning_effort="low",
        max_model_calls=120,
        model_timeout_seconds=120,
        max_http_attempts=1,
    )
    manifest = {
        "started_at": utc_now(),
        "status": "running",
        "source": str(original),
        "runtime_version": "0.15.2",
        "new_extraction_or_normalization_calls": 0,
        "recovered_observations_before_review": len(recovered),
        "config": config.model_dump(),
    }
    write_json(root / "manifest.json", manifest)
    key = dotenv_values(args.env_file).get("DEEPSEEK")
    if not key:
        raise ValueError("Missing DEEPSEEK")
    catalog = TechnologyCatalog.read(root / "technology-catalog.json")
    async with httpx.AsyncClient(base_url="https://api.deepseek.com/") as client:
        llm = ModelClient(
            client, key, config, root, api="deepseek", json_mode="json_object"
        )
        errors = await review_claims(recovered, llm, root, "recovered-source")
        for correction in await correct_statement_interpretations(
            recovered, list(statements.values()), pages, llm, root
        ):
            merge_statement_observation(recovered, correction)
        errors.extend(
            await process_technology_metadata(
                recovered, catalog, llm, root, "recovered-catalog"
            )
        )
        merged = [Finding.model_validate(r) for r in saved["technology_signals"]]
        for finding in recovered:
            merge_statement_observation(merged, finding)
        output = {
            "source_result": str(original / "result.json"),
            "method": "Original accepted decisions retained. Only pending decisions replayed offline with fixed scope conversion; recovered claims receive ordinary source and catalog/proposal reviews. Original files unchanged.",
            "technology_signals": [r.model_dump() for r in merged],
            "recovered_record_ids": [r.record_id for r in recovered],
            "accepted_record_ids": [r.record_id for r in merged if accepted_finding(r)],
            "pending_statement_ids": sorted(pending),
            "errors": errors,
            "usage": llm.usage(),
        }
        write_json(root / "result.json", output)
        manifest.update(
            status="finished",
            finished_at=utc_now(),
            accepted_recovered_observations=sum(accepted_finding(r) for r in recovered),
            remaining_pending_statements=len(pending),
            errors=errors,
            usage=llm.usage(),
        )
        write_json(root / "manifest.json", manifest)
        print(
            json.dumps(
                {k: v for k, v in manifest.items() if k not in {"config", "usage"}},
                indent=2,
            ),
            flush=True,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--env-file", type=Path, default=Path("jobs_extraction_lab/.env")
    )
    asyncio.run(replay(parser.parse_args()))
