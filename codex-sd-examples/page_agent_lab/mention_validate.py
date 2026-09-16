"""Replay host reference checks on saved model decisions without new LLM calls."""

import argparse
import json
import os
from pathlib import Path

from company_research.models import ResearchConfig
from company_research.storage import write_json
from dotenv import dotenv_values

from company_research import mentions, page_run, s3_results


def revalidate(previous):
    classification = previous["technology_classification"]
    raw = {
        mention["mention_id"]: mention
        for page in previous["pages"]
        for mention in page["technology_mentions"]["mentions"]
    }
    lookups = {
        decision["mention_id"]: decision["catalog_lookup"]
        for decision in classification["decisions"]
    }
    decisions = []
    for batch in classification["batches"]:
        inputs = [
            raw[identifier]
            | {
                "source_section_ids": list(
                    dict.fromkeys(
                        [
                            *raw[identifier].get("source_section_ids", []),
                            *classification["page_context_section_ids"][identifier],
                        ]
                    )
                )
            }
            for identifier in batch["mention_ids"]
        ]
        document = batch["response"]["document"] or {}
        accepted, rejected = mentions.accept_decisions(
            document.get("decisions"), mentions.batch_validation_mentions(inputs)
        )
        decisions.extend(
            item | {"catalog_lookup": lookups[item["mention_id"]]} for item in accepted
        )
        batch["previous_rejections"] = batch["rejections"]
        batch["rejections"] = rejected
    classification["decisions"] = decisions
    classification["host_validation_version"] = (
        "same-page-context-and-primary-evidence/1.1"
    )
    classification["status"] = (
        "processed"
        if all(item["status"] == "classified" for item in decisions)
        and all(
            not batch["rejections"] and batch["response"]["status"] == "processed"
            for batch in classification["batches"]
        )
        else "partial"
    )
    result = page_run.build_result(
        previous["target_url"],
        previous["pages"],
        classification,
        ResearchConfig.model_validate(previous["config"]),
        previous["run_id"],
        previous["model_usage"],
        previous["catalog"],
    )
    result.update(
        supersedes_revision_id=previous["revision_id"],
        revision_reason="Allow supplied same-page context while requiring the original primary mention evidence; deterministic validation replay only.",
        revision_usage={"calls": 0},
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--bucket")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    environment = {
        key: value
        for key, value in dotenv_values(args.env_file).items()
        if value is not None
    } | dict(os.environ)
    client = s3_results.s3_client(environment) if args.bucket else None
    for path in sorted(args.input.glob("company-*/result.json")):
        result = revalidate(json.loads(path.read_text()))
        output = args.output / path.parent.name / "result.json"
        write_json(output, result)
        if client is not None:
            s3_results.upload_result(output, client, args.bucket)
        print(
            result["target_url"],
            {
                status: sum(
                    item["status"] == status
                    for item in result["technology_classification"]["decisions"]
                )
                for status in ("classified", "needs_review")
            },
            flush=True,
        )


if __name__ == "__main__":
    main()
