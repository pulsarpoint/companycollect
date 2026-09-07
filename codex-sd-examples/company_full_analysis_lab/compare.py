"""Measure report scope and research activity; factual review stays explicit."""

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

from company_full_analysis_lab.run import LAB, load_records


def report_metrics(path: Path) -> dict:
    text = path.read_text()
    urls = sorted(set(re.findall(r"https?://[^\s<>\]\)\"]+", text)))
    return {
        "path": str(path.resolve()),
        "words": len(text.split()),
        "characters": len(text),
        "unique_report_urls": len(urls),
        "urls": urls,
        "source_domains": dict(Counter(urlsplit(url).netloc for url in urls)),
    }


def compare(run: Path, reference: Path) -> dict:
    metadata = json.loads((run / "run.json").read_text())
    reference_metadata = json.loads((reference / "metadata.json").read_text())
    if metadata["status"] != "completed":
        raise ValueError("Only completed research runs can be compared")
    if metadata["prompt_sha256"] != reference_metadata["prompt_sha256"]:
        raise ValueError(
            "Prompt mismatch: do not compare a smoke probe as the research run"
        )
    items = {
        (event["request"], event["item"]["id"]): event["item"]
        for event in load_records(run / "provider-events.jsonl")
        if event.get("type") == "response.output_item.done"
        and "id" in event.get("item", {})
    }
    searches = [
        item for item in items.values() if item.get("type") == "openrouter:web_search"
    ]
    return {
        "same_user_prompt": True,
        "controlled_model_only_comparison": False,
        "quality_score": None,
        "quality_score_note": "Word count, URL count, and search volume do not establish factual quality; see manual claim review.",
        "astra_reference": {
            **report_metrics(reference / "novelic-company-analysis.md"),
            "duration_minutes": reference_metadata["duration_ms"] / 60000,
            "cost_usd": reference_metadata["cost_usd"],
            "model": reference_metadata["model_attributed_by_user"],
        },
        "deepseek": {
            **report_metrics(run / "report.md"),
            "duration_minutes": metadata["elapsed_seconds"] / 60,
            "known_cost_usd": metadata["known_cost_usd"],
            "responses_without_reported_cost": metadata[
                "responses_without_reported_cost"
            ],
            "requests_without_completed_response": metadata["provider_requests"]
            - metadata["completed_responses"],
            "provider_requests": metadata["provider_requests"],
            "searches": len(searches),
            "search_queries": [
                item.get("action", {}).get("query") for item in searches
            ],
            "action_counts": metadata["action_counts"],
            "model": metadata["model"],
            "saved_pdfs": len(list((run / "workspace").rglob("*.pdf"))),
            "pdfs_recovered_after_run": len(
                list((run / "recovered-source-cache").glob("*.pdf"))
            ),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    parser.add_argument("--reference", type=Path, default=LAB / "reference")
    args = parser.parse_args()
    result = compare(args.run, args.reference)
    output = args.run / "comparison.json"
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(output)


if __name__ == "__main__":
    main()
