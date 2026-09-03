"""LLM choice of pages from a deterministic candidate list."""

import logging
from collections.abc import Sequence

from ex3.candidates import PageCandidate
from ex3.llm import run_structured_turn
from ex3.models import LlmCallStatus, PageSelectionResponse, ScoredUrl
from ex3.prompty import create_page_selection_prompt
from ex3.urls import url_key

LOGGER = logging.getLogger(__name__)
SELECTION_INSTRUCTIONS = (
    "Choose pages only from the supplied candidates. Do not navigate or use "
    "tools. Return only data matching the schema."
)


async def select_pages_with_llm(
    candidates: Sequence[PageCandidate],
    *,
    base_url: str,
    limit: int,
    timeout_seconds: int,
) -> tuple[list[ScoredUrl], LlmCallStatus]:
    """Let the model pick up to ``limit`` candidates; validate every pick."""
    outcome = await run_structured_turn(
        prompt=create_page_selection_prompt(
            base_url, candidates=list(candidates), limit=limit
        ),
        base_instructions=SELECTION_INSTRUCTIONS,
        output_model=PageSelectionResponse,
        timeout_seconds=timeout_seconds,
        operation_name="page selection",
    )
    if outcome.value is None:
        return [], LlmCallStatus(
            attempted=True,
            succeeded=False,
            error=outcome.error,
            token_usage=outcome.token_usage,
        )

    by_key = {url_key(candidate.url): candidate for candidate in candidates}
    picks: list[ScoredUrl] = []
    warnings: list[str] = []
    seen: set[str] = set()
    for decision in outcome.value.pages:
        key = url_key(decision.url)
        candidate = by_key.get(key)
        if candidate is None:
            warnings.append(f"Ignored unknown page: {decision.url}")
            continue
        if key in seen:
            warnings.append(f"Ignored duplicate page: {decision.url}")
            continue
        if len(picks) >= limit:
            warnings.append(f"Ignored pick beyond the limit: {decision.url}")
            continue
        seen.add(key)
        reasons = ["llm", decision.reason]
        if decision.expected_fields:
            reasons.append("fields: " + ", ".join(decision.expected_fields))
        picks.append(
            ScoredUrl(
                url=candidate.url,
                score=candidate.score,
                reasons=reasons,
                language=candidate.language,
                title=candidate.title,
            )
        )
    return picks, LlmCallStatus(
        attempted=True,
        succeeded=True,
        warnings=warnings,
        token_usage=outcome.token_usage,
    )
