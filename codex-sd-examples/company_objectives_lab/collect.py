"""Freeze existing candidate inventories and collect native Crawl4AI snapshots."""

import asyncio
import json
from importlib.metadata import version
from pathlib import Path
from typing import Any

import click
from crawl4ai import CacheMode, CrawlerRunConfig

from ex3.crawler import _open_crawler
from jobs_extraction_lab.corpus import content_hash, utc_now, write_json


def freeze_candidates(source_dir: Path, data_dir: Path) -> None:
    target = data_dir / "candidates.json"
    if target.exists():
        raise ValueError("Candidate inventory already frozen")
    sites = []
    for path in sorted(source_dir.glob("*.json")):
        source_text = path.read_text(encoding="utf-8")
        source = json.loads(source_text)
        candidates = [
            {
                "candidate_id": f"c{index:03d}",
                "url": c["url"],
                "title": c["title"],
                "language": c["language"],
                "anchor_text": c["labels"],
                "source": c["source"],
            }
            for index, c in enumerate(source["candidates"], 1)
        ]
        sites.append(
            {
                "domain": source["domain"],
                "base_url": source["base_url"],
                "source_file": str(path.resolve()),
                "source_sha256": content_hash(source_text),
                "built_at": source["built_at"],
                "inventory_urls": source["inventory_urls"],
                "candidates": candidates,
            }
        )
    write_json(
        target,
        {
            "created_at": utc_now(),
            "policy": "All 200 candidates per existing site; upstream inventory was previously shortlisted. No gold labels or ranking scores supplied to inference.",
            "sites": sites,
        },
    )


async def collect_pages(
    sources: Path,
    data_dir: Path,
    concurrency: int,
    port: int,
    retry_failed: bool = False,
) -> None:
    if (data_dir / "extraction-inputs.json").exists():
        raise ValueError("Extraction corpus is frozen; collect into a new directory")
    source_text = sources.read_text(encoding="utf-8")
    planned = json.loads(source_text)
    config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=45_000,
        delay_before_return_html=2.0,
        check_robots_txt=True,
        verbose=False,
    )
    config_data = {
        "crawl4ai_version": version("crawl4ai"),
        "source_sha256": content_hash(source_text),
        "config": config.dump(),
    }
    settings = data_dir / "collection-settings.json"
    if (
        settings.exists()
        and json.loads(settings.read_text(encoding="utf-8")) != config_data
    ):
        raise ValueError("Collection settings changed")
    write_json(settings, config_data)
    semaphore = asyncio.Semaphore(concurrency)
    async with _open_crawler(
        headless=True, proxy=None, cdp_port=port, accept_language="en-US,en;q=0.9"
    ) as crawler:

        async def fetch(page: dict) -> None:
            async with semaphore:
                target = data_dir / "pages" / f"{page['page_id']}.json"
                if target.exists():
                    previous = json.loads(target.read_text(encoding="utf-8"))
                    if previous["success"] or not retry_failed:
                        return
                    history = data_dir / "collection-attempts" / page["page_id"]
                    history.mkdir(parents=True, exist_ok=True)
                    archive = history / f"{utc_now().replace(':', '-')}.json"
                    if archive.exists():
                        raise ValueError("Collection retry archive already exists")
                    target.rename(archive)
                record: dict[str, Any] = {
                    **page,
                    "collected_at": utc_now(),
                    "success": False,
                }
                try:
                    results = await crawler.arun(url=page["url"], config=config)  # ty: ignore[missing-argument] -- Crawl4AI decorator typing.
                    result = next(iter(results))
                    record.update(
                        success=result.success,
                        final_url=result.redirected_url or result.url,
                        redirected_url=result.redirected_url,
                        status_code=result.status_code,
                        error=result.error_message,
                    )
                    if result.success:
                        for kind, content in {
                            "html": result.html,
                            "cleaned_html": result.cleaned_html,
                            "markdown": result.markdown.raw_markdown
                            if result.markdown
                            else "",
                        }.items():
                            filename = f"{kind}/{page['page_id']}.{'md' if kind == 'markdown' else 'html'}"
                            path = data_dir / filename
                            path.parent.mkdir(parents=True, exist_ok=True)
                            path.write_text(content or "", encoding="utf-8")
                            record[kind] = {
                                "file": filename,
                                "sha256": content_hash(content or ""),
                                "chars": len(content or ""),
                            }
                        record["links"] = result.links
                except Exception as error:  # noqa: BLE001 -- Persist an outcome for every attempted URL.
                    record["error"] = f"{type(error).__name__}: {error}"
                write_json(target, record)
                click.echo(
                    f"{page['page_id']}: {'saved' if record['success'] else 'failed'}; {record.get('cleaned_html', {}).get('chars', 0)} HTML chars"
                )

        await asyncio.gather(*(fetch(page) for page in planned))
    write_json(
        data_dir / "collection.json",
        {
            "created_at": utc_now(),
            **config_data,
            "pages": [
                json.loads(
                    (data_dir / "pages" / f"{p['page_id']}.json").read_text(
                        encoding="utf-8"
                    )
                )
                for p in planned
            ],
        },
    )


def freeze_extraction_inputs(data_dir: Path) -> None:
    """Freeze the unchanged successful snapshots; retain HTTP error pages as controls."""
    target = data_dir / "extraction-inputs.json"
    if target.exists():
        raise ValueError("Extraction inputs already frozen")
    source_text = (data_dir / "collection.json").read_text(encoding="utf-8")
    collection = json.loads(source_text)
    pages = []
    for page in collection["pages"]:
        if not page["success"]:
            raise ValueError(
                f"Collection has an unresolved browser failure: {page['page_id']}"
            )
        content = page["cleaned_html"]
        if (
            content_hash((data_dir / content["file"]).read_text(encoding="utf-8"))
            != content["sha256"]
        ):
            raise ValueError(f"Snapshot changed: {page['page_id']}")
        pages.append(
            {
                "page_id": page["page_id"],
                "domain": page["domain"],
                "source_url": page["final_url"],
                "file": content["file"],
                "sha256": content["sha256"],
                "status_code": page["status_code"],
            }
        )
    write_json(
        target,
        {
            "created_at": utc_now(),
            "collection_sha256": content_hash(source_text),
            "policy": "Full unmodified native Crawl4AI cleaned_html, including HTTP error controls. Redirected URL used as link-resolution base.",
            "pages": pages,
        },
    )


@click.command()
@click.option("--sources", type=click.Path(path_type=Path, exists=True), required=True)
@click.option("--data-dir", type=click.Path(path_type=Path), required=True)
@click.option("--concurrency", type=click.IntRange(1, 4), default=3)
@click.option("--port", type=int, default=9441)
@click.option(
    "--freeze-inputs",
    is_flag=True,
    help="Freeze extraction inputs after reviewing the saved collection, without crawling again.",
)
@click.option(
    "--candidate-dir", type=click.Path(path_type=Path, exists=True), default=None
)
@click.option(
    "--retry-failed",
    is_flag=True,
    help="Archive failed browser outcomes and retry those pages.",
)
def main(
    sources: Path,
    data_dir: Path,
    concurrency: int,
    port: int,
    freeze_inputs: bool,
    candidate_dir: Path | None,
    retry_failed: bool,
) -> None:
    if candidate_dir is not None:
        freeze_candidates(candidate_dir, data_dir)
    if freeze_inputs:
        freeze_extraction_inputs(data_dir)
        return
    asyncio.run(collect_pages(sources, data_dir, concurrency, port, retry_failed))


if __name__ == "__main__":
    main()
