"""Homepage-only discovery followed by fresh page descriptions and normalization.

The direct crawler result is the baseline; statement technology/credential results
replace those two objectives in the experimental output. No expected facts or URL
seeds are supplied to either model path. No submission or deployment is performed.
"""

import asyncio
import logging
import shutil
import time
from collections import Counter
from pathlib import Path

import click
import httpx
from dotenv import dotenv_values

from company_research.analytics import accepted_finding
from company_research.llm import ModelClient
from company_research.models import OBJECTIVES, Finding, ResearchConfig
from company_research.research import research_company
from company_research.statement_review import accepted_description
from company_research.statements import (
    consolidate_page_statements,
    extract_page_statements,
)
from company_research.storage import content_hash, utc_now, write_json
from company_research.technology_catalog import TechnologyCatalog

SITES = [
    {"id": "dmc", "url": "https://www.dmcinfo.com/", "type": "engineering"},
    {"id": "plausible", "url": "https://plausible.io/", "type": "software"},
    {"id": "thoughtbot", "url": "https://thoughtbot.com/", "type": "services"},
    {"id": "onlogic", "url": "https://www.onlogic.com/", "type": "manufacturer"},
    {"id": "ic-resources", "url": "https://ic-resources.com/", "type": "staffing"},
]


def export_company(crawl, statement_result: dict, root: Path) -> dict:
    records = {key: getattr(crawl.records, key) for key in OBJECTIVES}
    for key in ("technology_signals", "certifications_compliance"):
        records[key] = [Finding.model_validate(r) for r in statement_result[key]]
    counts, accepted, pending = {}, {}, {}
    for key, values in records.items():
        accepted[key] = [r.model_dump() for r in values if accepted_finding(r)]
        pending[key] = [r.model_dump() for r in values if not accepted_finding(r)]
        counts[key] = {
            "accepted": len(accepted[key]),
            "held": len(pending[key]),
            "direct_baseline_accepted": sum(
                accepted_finding(r) for r in getattr(crawl.records, key)
            ),
        }
    statuses = {}
    for key in OBJECTIVES:
        examined = sum(key in page.objectives_examined for page in crawl.pages)
        if key in {"technology_signals", "certifications_compliance"}:
            examined = len(list((root / "statements/page-statements").glob("*.json")))
        statuses[key] = {
            "status": "found"
            if accepted[key]
            else "needs_review"
            if pending[key]
            else "not_found_in_examined_pages"
            if examined
            else "not_examined",
            "pages_examined": examined,
            "meaning": "Limited crawl coverage; missing information is not proof of absence",
        }
    write_json(
        root / "accepted-company.json",
        {
            "schema_version": "company-statement-benchmark/1.0",
            "site_url": crawl.site_url,
            "records": accepted,
            "objectives": statuses,
            "explicit_negatives": [
                r.model_dump()
                for r in crawl.records.explicit_negatives
                if accepted_finding(r)
            ],
            "technology_summary": statement_result["technology_summary"],
            "site_profile": crawl.site_profile.model_dump()
            if crawl.site_profile
            else None,
            "pending_statement_ids": statement_result["pending_statement_ids"],
            "pending_description_ids": statement_result["pending_description_ids"],
            "limitations": [
                "Statement workflow is experimental",
                "Accepted means automated source/review checks, not independent verification",
                "New catalog proposals require administrator review",
            ],
        },
    )
    write_json(root / "held-records.json", pending)
    return counts


async def run_site(
    site: dict,
    root: Path,
    catalog: TechnologyCatalog,
    key: str,
    crawl_config: ResearchConfig,
    statement_config: ResearchConfig,
) -> dict:
    root.mkdir(parents=True)
    start = time.monotonic()
    progress = {"site": site, "started_at": utc_now(), "status": "crawling"}
    write_json(root / "progress.json", progress)
    click.echo(f"{site['id']}: starting homepage-only discovery")
    crawl = await research_company(
        site["url"],
        api_key=key,
        output_dir=root / "crawl",
        config=crawl_config,
        technology_catalog=catalog,
    )
    crawl_seconds = time.monotonic() - start
    progress.update(
        status="statements",
        crawl_status=crawl.status,
        crawl_stop_reason=crawl.stop_reason,
    )
    write_json(root / "progress.json", progress)
    statement_root = root / "statements"
    statement_root.mkdir()
    pages = [p for p in crawl.pages if p.fetch_status == "fetched" and p.html_file]
    if (root / "crawl/html").exists():
        shutil.copytree(root / "crawl/html", statement_root / "html")
    write_json(statement_root / "pages.json", [p.model_dump() for p in pages])
    statements = []
    click.echo(
        f"{site['id']}: direct crawl ended; fresh statements on {len(pages)} pages"
    )
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = ModelClient(client, key, statement_config, statement_root)
        for page in pages:
            click.echo(
                f"{site['id']}: page statements {page.page_id} {page.source_url}"
            )
            statements.extend(await extract_page_statements(page, llm, statement_root))
            write_json(
                statement_root / "extracted-statements.json",
                [r.model_dump() for r in statements],
            )
        result = await consolidate_page_statements(
            statements, pages, catalog, llm, statement_root
        )
    counts = export_company(crawl, result, root)
    hashes = {
        p.page_id: content_hash(
            (statement_root / p.html_file).read_text(encoding="utf-8")
        )
        == p.html_sha256
        for p in pages
        if p.html_file is not None
    }
    if not all(hashes.values()):
        raise ValueError("Saved source HTML changed")
    stages = {}
    for name, usage in [
        ("direct_crawl", crawl.usage),
        ("page_statements", result["usage"]),
    ]:
        stages[name] = {k: v for k, v in usage.items() if k != "by_call"}
        stages[name]["calls_by_task"] = dict(
            Counter(call["task"].split(":")[0] for call in usage.get("by_call", []))
        )
    relations = {}
    for raw in result["technology_signals"]:
        record = Finding.model_validate(raw)
        if accepted_finding(record):
            for statement_id in record.data["statement_ids"]:
                relations.setdefault(statement_id, set()).add(record.data["signal"])
    metrics = {
        "site": site,
        "crawl_status": crawl.status,
        "crawl_stop_reason": crawl.stop_reason,
        "pages_attempted": len(crawl.pages),
        "pages_fetched": len(pages),
        "source_hashes_verified": hashes,
        "statements": len(statements),
        "accepted_descriptions": sum(accepted_description(r) for r in statements),
        "pending_descriptions": len(result["pending_description_ids"]),
        "pending_statements": len(result["pending_statement_ids"]),
        "statements_with_multiple_accepted_signals": {
            k: sorted(v) for k, v in relations.items() if len(v) > 1
        },
        "counts": counts,
        "usage": stages,
        "crawl_seconds": round(crawl_seconds, 2),
        "total_seconds": round(time.monotonic() - start, 2),
        "crawl_errors": crawl.errors,
        "statement_errors": result["errors"],
    }
    write_json(root / "metrics.json", metrics)
    progress.update(status="complete", finished_at=utc_now())
    write_json(root / "progress.json", progress)
    click.echo(
        f"{site['id']}: complete; {counts['technology_signals']['accepted']} technology observations"
    )
    return metrics


async def run(
    output: Path,
    catalog_path: Path,
    env_file: Path,
    max_pages: int,
    crawl_calls: int,
    statement_calls: int,
    concurrency: int,
) -> None:
    if output.exists():
        raise ValueError("Output must be a new directory; completed runs are immutable")
    key = dotenv_values(env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY required in explicit env file")
    catalog = TechnologyCatalog.read(catalog_path)
    crawl_config = ResearchConfig(
        provider="Wafer",
        reasoning_effort="low",
        max_pages=max_pages,
        max_external_pages=3,
        max_model_calls=crawl_calls,
        model_timeout_seconds=120,
        statement_batch_size=12,
        catalog_resolution_batch_size=8,
        extraction_concurrency=2,
        chunk_chars=60000,
    )
    statement_config = crawl_config.model_copy(
        update={"max_model_calls": statement_calls}
    )
    output.mkdir(parents=True)
    shutil.copy2(catalog_path, output / "technology-catalog.json")
    package = Path(__file__).resolve().parents[1]
    shutil.copytree(
        package / "src/company_research",
        output / "implementation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copy2(__file__, output / "benchmark-harness.py")
    write_json(
        output / "manifest.json",
        {
            "package_version": "0.13.0",
            "started_at": utc_now(),
            "sites": SITES,
            "crawl_config": crawl_config.model_dump(),
            "statement_config": statement_config.model_dump(),
            "catalog_sha256": content_hash(catalog_path.read_text(encoding="utf-8")),
            "candidate_seeds": "homepage only; crawler discovers sitemaps and links",
            "expected_facts_in_prompts": False,
            "site_concurrency": concurrency,
            "comparison": "Direct extraction versus fresh page statements on the same fetched pages",
            "cost_note": "Includes two extraction paths; not a single production crawl cost",
        },
    )
    semaphore = asyncio.Semaphore(concurrency)

    async def bounded_site(site: dict) -> dict:
        async with semaphore:
            try:
                return await run_site(
                    site,
                    output / site["id"],
                    catalog,
                    key,
                    crawl_config.model_copy(),
                    statement_config.model_copy(),
                )
            except Exception as error:
                failure = {
                    "site": site,
                    "status": "failed",
                    "finished_at": utc_now(),
                    "error": str(error).replace(key, "[REDACTED]")[:2000],
                }
                write_json(output / site["id"] / "benchmark-error.json", failure)
                click.echo(
                    f"{site['id']}: benchmark error; saved partial artifacts", err=True
                )
                return failure

    results = await asyncio.gather(*(bounded_site(site) for site in SITES))
    write_json(
        output / "suite-results.json", {"finished_at": utc_now(), "sites": results}
    )
    click.echo(f"Suite finished: {output}")


@click.command()
@click.option("--output", type=click.Path(path_type=Path), required=True)
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
)
@click.option("--env-file", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--max-pages", type=click.IntRange(1, 50), default=10, show_default=True)
@click.option("--crawl-calls", type=click.IntRange(1), default=100, show_default=True)
@click.option(
    "--statement-calls", type=click.IntRange(1), default=120, show_default=True
)
@click.option("--concurrency", type=click.IntRange(1, 3), default=2, show_default=True)
def main(**kwargs):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(run(**kwargs))


if __name__ == "__main__":
    main()
