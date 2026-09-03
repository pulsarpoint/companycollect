"""Merge a follow-up extraction round into a previous consolidated report."""

from collections.abc import Collection

from ex1.models import UsefulInformation
from ex3.llm import run_structured_turn
from ex3.models import MergeAnalysis
from ex3.prompty import create_merge_prompt
from ex3.urls import url_key

MERGE_INSTRUCTIONS = (
    "Merge the two supplied extraction rounds. Do not navigate or use tools. "
    "Return only data matching the schema."
)


async def merge_information_with_llm(
    previous: UsefulInformation,
    new_round: UsefulInformation,
    *,
    processed_urls: Collection[str],
    timeout_seconds: int,
) -> tuple[UsefulInformation | None, MergeAnalysis]:
    """Merge two rounds with one Codex call; None means keep the deterministic merge."""
    outcome = await run_structured_turn(
        prompt=create_merge_prompt(
            previous=previous,
            new_round=new_round,
            processed_urls=sorted(processed_urls),
        ),
        base_instructions=MERGE_INSTRUCTIONS,
        output_model=UsefulInformation,
        timeout_seconds=timeout_seconds,
        operation_name="round merge",
    )
    if outcome.value is None:
        return None, MergeAnalysis(
            attempted=True,
            succeeded=False,
            error=outcome.error,
            token_usage=outcome.token_usage,
        )
    merged, dropped = drop_unknown_evidence(
        outcome.value, processed_urls=processed_urls
    )
    warnings = (
        [f"Dropped {dropped} item(s) with evidence outside processed pages"]
        if dropped
        else []
    )
    return merged, MergeAnalysis(
        attempted=True,
        succeeded=True,
        warnings=warnings,
        token_usage=outcome.token_usage,
        dropped_items=dropped,
    )


def drop_unknown_evidence(
    information: UsefulInformation,
    *,
    processed_urls: Collection[str],
) -> tuple[UsefulInformation, int]:
    """Remove items whose evidence URL is not a processed page."""
    allowed = {url_key(url) for url in processed_urls}

    def ok(url: str) -> bool:
        return url_key(url) in allowed

    dropped = 0
    cleaned = information.model_copy(deep=True)
    for attribute in ("contacts", "products", "jobs", "other_facts"):
        items = getattr(cleaned, attribute)
        kept = [item for item in items if ok(item.evidence.source_url)]
        dropped += len(items) - len(kept)
        setattr(cleaned, attribute, kept)
    kept_evidence = [item for item in cleaned.company.evidence if ok(item.source_url)]
    dropped += len(cleaned.company.evidence) - len(kept_evidence)
    cleaned.company.evidence = kept_evidence
    return cleaned, dropped
