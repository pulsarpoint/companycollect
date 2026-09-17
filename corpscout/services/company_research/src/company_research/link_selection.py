"""LLM link ranking shared by collection and the combined research controller."""

import json
from pathlib import Path

from pydantic import ValidationError

from company_research.discovery import CrawlQueue
from company_research.llm import ModelClient
from company_research.models import (
    OBJECTIVES,
    CandidateAssessment,
    RequestedContentAssessment,
    RequestedContentSelection,
    Selection,
)
from company_research.prompts import selection_prompt
from company_research.storage import write_json


async def assess_links(
    queue: CrawlQueue, llm: ModelClient, root: Path, *, reserved_calls: int = 3
) -> list[dict]:
    errors = []
    for _ in range(queue.config.selection_batches_per_page):
        # Preserve capacity to extract the next page instead of consuming it all on ranking.
        if llm.remaining <= reserved_calls:
            return errors
        batch = queue.assessment_batch()
        if not batch:
            return errors
        by_id = {candidate.candidate_id: candidate for candidate in batch}
        original = selection_prompt(
            queue.site_url,
            [c.prompt_data() for c in batch],
            site_profile=queue.site_profile,
            coverage=queue.coverage,
            instructions=queue.instructions,
        )
        prompt = original
        for correction in range(queue.config.max_corrections + 1):
            if llm.remaining <= reserved_calls:
                break
            reply = await llm.ask(
                prompt,
                (
                    RequestedContentSelection
                    if queue.instructions is not None
                    else Selection
                ).model_json_schema(),
                task="link_assessment",
            )
            issues = []
            values = (
                reply.document.get("assessments")
                if isinstance(reply.document, dict)
                else None
            )
            if not isinstance(values, list):
                issues.append(reply.error or "Missing assessments array")
            else:
                seen = set()
                for value in values:
                    try:
                        assessment = (
                            RequestedContentAssessment
                            if queue.instructions is not None
                            else CandidateAssessment
                        ).model_validate(value)
                    except ValidationError:
                        issues.append("Invalid candidate assessment schema")
                        continue
                    if assessment.candidate_id not in by_id:
                        issues.append("Unknown candidate ID")
                        continue
                    if assessment.candidate_id in seen:
                        issues.append("Duplicate candidate ID")
                        continue
                    seen.add(assessment.candidate_id)
                    if isinstance(assessment, CandidateAssessment) and any(
                        p.potential in {"high", "medium"} and p.role == "none"
                        for p in (getattr(assessment.objectives, o) for o in OBJECTIVES)
                    ):
                        issues.append(
                            "Useful potential needs a direct or navigation role"
                        )
                        continue
                    by_id[assessment.candidate_id].assessment = assessment
            missing = [
                cid for cid, candidate in by_id.items() if candidate.assessment is None
            ]
            if missing:
                issues.append("Missing valid assessments: " + ", ".join(missing))
            write_json(
                root
                / "assessments"
                / f"{batch[0].candidate_id}-{batch[0].assessment_attempts}-{correction}.json",
                {
                    "candidate_ids": list(by_id),
                    "issues": issues,
                    "model_error": reply.error,
                },
            )
            if not issues or reply.error is not None and reply.raw is None:
                break
            prompt = (
                original
                + "\nReturn the complete JSON again. Correct these issues:\n"
                + json.dumps(issues)
            )
        # Unassessed candidates retain uncertainty and remain eligible for bounded exploration.
        for candidate in batch:
            candidate.assessment_attempts += 1
            candidate.assessed = (
                candidate.assessment is not None
                or candidate.assessment_attempts >= queue.config.max_assessment_attempts
            )
        if issues:
            errors.append(
                {
                    "stage": "link_assessment",
                    "candidate_ids": list(by_id),
                    "issues": issues,
                }
            )
    return errors
