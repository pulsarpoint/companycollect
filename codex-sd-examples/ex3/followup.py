"""Pass two, step one: what is still missing and which pages might have it."""

import logging
from dataclasses import dataclass
from pathlib import Path

from ex3.candidates import build_followup_candidates, load_inventory_eligible
from ex3.crawler import _preferred_languages, load_manifest
from ex3.llm import run_structured_turn
from ex3.models import (
    LlmCallStatus,
    PageSelectionResponse,
    PassSuggestions,
    ResearchReport,
    SuggestedPage,
)
from ex3.prompty import create_followup_prompt
from ex3.requirements import compute_gaps
from ex3.urls import url_key

LOGGER = logging.getLogger(__name__)
FOLLOWUP_INSTRUCTIONS = (
    "Choose pages only from the supplied candidates. Do not navigate or use "
    "tools. Return only data matching the schema."
)


@dataclass(frozen=True, slots=True)
class SuggestSettings:
    manifest_path: Path
    report_path: Path
    max_suggestions: int = 10
    candidate_limit: int = 200
    timeout_seconds: int = 300


async def run_suggest(settings: SuggestSettings) -> PassSuggestions:
    """Compute gaps from a report and ask the model which pages could fill them."""
    manifest = load_manifest(settings.manifest_path)
    report = ResearchReport.model_validate_json(
        settings.report_path.read_text(encoding="utf-8")
    )
    pass_number = (
        max((page.pass_number for page in manifest.markdown_pages), default=1) + 1
    )
    processed_urls = [page.source_url for page in manifest.markdown_pages]
    gaps = compute_gaps(report.useful_information)
    base = PassSuggestions(
        manifest_path=str(settings.manifest_path.resolve()),
        report_path=str(settings.report_path.resolve()),
        pass_number=pass_number,
        gaps=gaps,
        llm=LlmCallStatus(attempted=False, succeeded=True),
    )
    if not gaps:
        LOGGER.info("No gaps left after pass %d; nothing to suggest", pass_number - 1)
        return base

    inventory_path = (
        Path(manifest.url_seeding.inventory_path)
        if manifest.url_seeding is not None and manifest.url_seeding.inventory_path
        else None
    )
    candidates = build_followup_candidates(
        discovered_urls=report.discovered_urls,
        inventory_eligible=load_inventory_eligible(inventory_path),
        processed_urls=processed_urls,
        base_url=manifest.selected_base_url,
        preferred_languages=_preferred_languages(
            manifest.language_discovery.selected_language
        ),
        limit=settings.candidate_limit,
    )
    if not candidates:
        LOGGER.info("No unprocessed candidate URLs; nothing to suggest")
        return base.model_copy(update={"candidate_count": 0})

    outcome = await run_structured_turn(
        prompt=create_followup_prompt(
            manifest.selected_base_url,
            gaps=gaps,
            processed_urls=processed_urls,
            candidates=candidates,
            limit=settings.max_suggestions,
        ),
        base_instructions=FOLLOWUP_INSTRUCTIONS,
        output_model=PageSelectionResponse,
        timeout_seconds=settings.timeout_seconds,
        operation_name=f"pass {pass_number} suggestions",
    )
    if outcome.value is None:
        return base.model_copy(
            update={
                "candidate_count": len(candidates),
                "llm": LlmCallStatus(
                    attempted=True,
                    succeeded=False,
                    error=outcome.error,
                    token_usage=outcome.token_usage,
                ),
            }
        )

    candidate_keys = {url_key(candidate.url): candidate.url for candidate in candidates}
    processed_keys = {url_key(url) for url in processed_urls}
    suggestions: list[SuggestedPage] = []
    warnings: list[str] = []
    seen: set[str] = set()
    for decision in outcome.value.pages:
        key = url_key(decision.url)
        if key in processed_keys:
            warnings.append(f"Ignored already processed page: {decision.url}")
            continue
        if key not in candidate_keys:
            warnings.append(f"Ignored unknown page: {decision.url}")
            continue
        if key in seen:
            warnings.append(f"Ignored duplicate page: {decision.url}")
            continue
        if len(suggestions) >= settings.max_suggestions:
            warnings.append(f"Ignored suggestion beyond the limit: {decision.url}")
            continue
        seen.add(key)
        suggestions.append(
            SuggestedPage(
                url=candidate_keys[key],
                reason=decision.reason,
                expected_fields=decision.expected_fields,
            )
        )
    return base.model_copy(
        update={
            "candidate_count": len(candidates),
            "llm": LlmCallStatus(
                attempted=True,
                succeeded=True,
                warnings=warnings,
                token_usage=outcome.token_usage,
            ),
            "suggestions": suggestions,
        }
    )
