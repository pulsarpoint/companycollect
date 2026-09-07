"""Fetch known Careers/application links from saved records; this is a guided probe."""

import argparse
import asyncio
import json
from pathlib import Path

from bs4 import BeautifulSoup

from company_research.fetch import fetch_page, open_browser
from company_research.models import Page, ResearchConfig
from company_research.storage import utc_now, write_json


async def run(args):
    source = args.snapshot.resolve()
    result = json.loads((source / "result.json").read_text())
    queue = json.loads((source / "queue.json").read_text())
    candidates = [
        c["url"]
        for c in queue["candidates"]
        if any("careers" == label.casefold() for label in c["anchor_text"])
    ]
    candidates.extend(
        r["data"]["job_url"] for r in result["records"]["jobs"] if r["data"]["job_url"]
    )
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    outcomes = []
    async with open_browser() as crawler:
        for index, url in enumerate(dict.fromkeys(candidates)):
            page = Page(
                page_id=f"p{index + 1:04}",
                requested_url=url,
                source_url=url,
                selected_for="guided_saved_careers_or_application_link",
                fetched_at=utc_now(),
                status_code=None,
                fetch_status="pending",
                extraction_status="not_assessed",
                objectives_examined=[],
                attempts=0,
                chunks_planned=0,
                chunks_completed=0,
                html_sha256=None,
                html_file=None,
                errors=[],
            )
            html, links = await fetch_page(
                crawler,
                page,
                ResearchConfig(page_attempts=1, page_timeout_seconds=45),
                root,
            )
            soup = BeautifulSoup(html, "html.parser")
            outcomes.append(
                {
                    "page": page.model_dump(),
                    "headings": [
                        h.get_text(" ", strip=True) for h in soup.select("h1,h2,h3")
                    ],
                    "links": links,
                }
            )
            write_json(
                root / "result.json",
                {
                    "mode": "guided_fetch_probe_not_autonomous_crawl",
                    "source_run": str(source),
                    "outcomes": outcomes,
                },
            )
            print(url, page.fetch_status, len(html), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(run(parser.parse_args()))
