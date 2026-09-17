"""Replay the saved external partner page through the queue without fetching or models."""

import argparse
import json
from pathlib import Path

from bs4 import BeautifulSoup

from company_research.discovery import CrawlQueue
from company_research.models import OBJECTIVES, CandidateAssessment, ResearchConfig
from company_research.storage import content_hash, write_json


def replay(snapshot: Path, current: Path) -> dict:
    original = json.loads((snapshot / "result.json").read_text())
    partner = next(
        page for page in original["pages"] if "analog.com" in page["source_url"] and "/novelic" in page["source_url"]
    )
    current_queue = json.loads((current / "queue.json").read_text())
    assessed = next(
        candidate
        for candidate in current_queue["candidates"]
        if candidate["url"] in {partner["source_url"], partner["requested_url"]} and candidate["assessment"]
    )
    html = (snapshot / partner["html_file"]).read_text()
    if content_hash(html) != partner["html_sha256"]:
        raise ValueError("Partner source changed")
    queue = CrawlQueue(original["site_url"], ResearchConfig())
    queue.add(partner["source_url"], source=queue.site_url)
    candidate = queue.candidates[partner["source_url"]]
    candidate.assessment = CandidateAssessment.model_validate(
        assessed["assessment"] | {"candidate_id": candidate.candidate_id}
    )
    selected = queue.pick(dict.fromkeys(OBJECTIVES, 0))
    if selected is None or selected[0].url != partner["source_url"]:
        raise ValueError("Useful target partner profile was not eligible")
    queue.visited.add(partner["source_url"])
    candidate.observed_relevance = "target"
    links = BeautifulSoup(html, "html.parser").find_all("a", href=True)
    for link in links:
        queue.add(
            str(link["href"]),
            source=partner["source_url"],
            label=link.get_text(" ", strip=True),
        )
    unwanted = [
        page["source_url"]
        for page in original["pages"]
        if "analog.com" in page["source_url"]
        and page["source_url"] != partner["source_url"]
    ]
    leaked = [url for url in unwanted if url in queue.candidates]
    result = {
        "mode": "saved_html_link_replay",
        "source_url": partner["source_url"],
        "html_sha256": partner["html_sha256"],
        "assessment_from_run": str(current),
        "target_profile_allowed": True,
        "follow_scope": candidate.assessment.follow_scope,
        "anchor_count": len(links),
        "old_unrelated_page_urls": unwanted,
        "unrelated_pages_admitted": leaked,
        "excluded": dict(queue.excluded),
        "document_inventory_is_unassessed": True,
        "passed": not leaked,
    }
    write_json(current / "frozen-link-replay.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("current", type=Path)
    args = parser.parse_args()
    print(json.dumps(replay(args.snapshot, args.current), indent=2))
