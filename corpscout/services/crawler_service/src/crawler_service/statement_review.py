"""Review descriptions and their actors; recheck bounded, source-grounded corrections."""

import json
import re
from collections import Counter
from pathlib import Path
from typing import Literal

from pydantic import Field, ValidationError

from crawler_service.content import HtmlWindow, normalize_evidence, source_finding
from crawler_service.llm import ModelBudgetExceeded, ModelClient, ModelUnavailable
from crawler_service.models import Finding, Page, StrictModel
from crawler_service.review import repair_evidence
from crawler_service.statement_prompts import REVIEW_PAGE_DESCRIPTIONS
from crawler_service.storage import content_hash, write_json

STATEMENT_FIELDS = (
    "kind",
    "subject_name",
    "subject_kind",
    "source_name",
    "context",
    "application_context",
    "section_heading",
    "job_title",
    "qualifiers",
)


class StatementAttribution(StrictModel):
    subject_name: str | None
    subject_kind: Literal[
        "company", "team", "person", "product", "service", "facility", "unknown"
    ]
    source_name: str | None


class DescriptionCorrection(StrictModel):
    context: str = Field(min_length=1, max_length=1600)
    application_context: str | None = Field(max_length=1000)
    qualifiers: list[str] = Field(max_length=12)
    attribution: StatementAttribution | None
    evidence: list[str] | None = Field(min_length=1, max_length=8)


class DescriptionCheck(StrictModel):
    statement_id: str
    supported: bool
    reason: str = Field(min_length=1)
    correction: DescriptionCorrection | None
    source_attribution: StatementAttribution | None


class DescriptionChecks(StrictModel):
    checks: list[DescriptionCheck]


def statement_data(record: Finding) -> dict:
    return {key: record.data[key] for key in STATEMENT_FIELDS}


def description_input_hash(record: Finding) -> str:
    return content_hash(
        json.dumps(
            {
                "data": statement_data(record),
                "sources": [
                    source.model_dump()
                    for source in record.sources
                    if source.evidence_status == "source_matched"
                ],
            },
            sort_keys=True,
            ensure_ascii=False,
        )
    )


def accepted_description(record: Finding) -> bool:
    review = record.data.get("description_review") or {}
    return (
        record.evidence_status == "source_matched"
        and review.get("status") == "accepted"
        and review.get("review_version") == 2
        and review.get("input_sha256") == description_input_hash(record)
    )


def validate_description_checks(
    values: list,
    active: dict[str, Finding],
) -> tuple[dict[str, DescriptionCheck], dict[str, str]]:
    counts = Counter(
        value.get("statement_id")
        for value in values
        if isinstance(value, dict) and isinstance(value.get("statement_id"), str)
    )
    checks, issues = {}, {}
    for value in values:
        key = value.get("statement_id") if isinstance(value, dict) else None
        if not isinstance(key, str) or key not in active:
            continue
        try:
            check = DescriptionCheck.model_validate(value)
            if counts[key] != 1 or check.supported and check.correction is not None:
                raise ValueError("Duplicate or contradictory description check")
            expected = {
                field: active[key].data[field]
                for field in StatementAttribution.model_fields
            }
            if check.supported and (
                check.source_attribution is None
                or any(
                    normalize_evidence(
                        str(check.source_attribution.model_dump()[field] or "")
                    )
                    != normalize_evidence(str(value or ""))
                    for field, value in expected.items()
                )
            ):
                check.supported = False
                check.reason = (
                    "Structured actor or source name differs or was not checked. "
                    + check.reason
                )
            checks[key] = check
        except (ValidationError, ValueError) as error:
            issues[key] = str(error)
    return checks, issues


def apply_description_review(
    record: Finding,
    check: DescriptionCheck | None,
    issue: str,
    *,
    allow_correction: bool,
    page: Page,
    window: HtmlWindow,
) -> dict:
    review = {
        "status": "processing_failed"
        if check is None
        else "accepted"
        if check.supported
        else "rejected",
        "reason": check.reason if check is not None else issue,
        "input_sha256": description_input_hash(record),
        "method": "llm_against_native_html",
        "independently_verified": False,
        "review_version": 2,
        "source_attribution": check.source_attribution.model_dump()
        if check is not None and check.source_attribution is not None
        else None,
    }
    record.data["description_review"] = dict(review)
    if (
        allow_correction
        and check is not None
        and not check.supported
        and check.correction is not None
    ):
        before = statement_data(record)
        correction = check.correction
        changes = correction.model_dump(exclude={"attribution", "evidence"})
        if correction.attribution is not None:
            if (
                correction.attribution != check.source_attribution
                or correction.evidence is None
            ):
                return review
            old_name, new_name = (
                before["source_name"],
                correction.attribution.source_name,
            )
            if old_name != new_name and (
                not old_name
                or not new_name
                or re.search(
                    r"(?<![\w+#.])"
                    + re.escape(normalize_evidence(new_name))
                    + r"(?![\w+#])",
                    normalize_evidence(old_name),
                )
                is None
            ):
                return review
            changes.update(correction.attribution.model_dump())
        evidence = correction.evidence or [
            fragment.text for fragment in record.sources[0].evidence
        ]
        # These fields are immutable and were already checked in this same window.
        # Recheck their literal presence without making the model repeat them.
        evidence = list(
            dict.fromkeys(
                evidence
                + [
                    before[key]
                    for key in ("section_heading", "job_title")
                    if before[key] is not None
                ]
            )
        )
        candidate = source_finding(
            "page_statements",
            before | changes | {"evidence": evidence},
            page=page.model_dump(),
            window=window,
        )
        if candidate.evidence_status != "source_matched":
            review["correction_issues"] = candidate.sources[0].issues
            record.data["description_review"]["correction_issues"] = review[
                "correction_issues"
            ]
            # Return a separate unapproved candidate for quotation-only repair.
            # It never replaces the original until its source fragments pass.
            review["correction_candidate"] = candidate.model_dump()
            return review
        if any(before[key] != value for key, value in changes.items()):
            record.data.setdefault("description_revisions", []).append(
                {
                    "before": before,
                    "correction": changes,
                    "review": dict(review),
                    "sources_before": [
                        source.model_dump() for source in record.sources
                    ],
                }
            )
            record.data.update(changes)
            record.sources = candidate.sources
            record.evidence_status = candidate.evidence_status
            # An edit has no approval until a separate source check succeeds.
            record.data["description_review"]["status"] = "correction_pending"
    return review


async def review_page_descriptions(
    records: list[Finding],
    pages: list[Page],
    llm: ModelClient,
    root: Path,
) -> list[str]:
    """Keep good verdicts; retry only missing/rejected descriptions, with one bounded edit."""
    page_map = {page.page_id: page for page in pages}
    grouped: dict[tuple[str, int, int], list[Finding]] = {}
    errors = []
    for record in records:
        source = next(
            (
                source
                for source in record.sources
                if source.evidence_status == "source_matched"
            ),
            None,
        )
        if record.evidence_status != "source_matched" or source is None:
            record.data["description_review"] = {"status": "evidence_pending"}
            continue
        grouped.setdefault(
            (source.page_id, source.chunk_start, source.chunk_end), []
        ).append(record)
    for (page_id, window_start, window_end), group in grouped.items():
        page = page_map[page_id]
        if page.html_file is None:
            raise ValueError(f"Missing native HTML for {page_id}")
        html = (root / page.html_file).read_text(encoding="utf-8")
        if content_hash(html) != page.html_sha256 or any(
            source.html_sha256 != page.html_sha256
            for record in group
            for source in record.sources
            if source.evidence_status == "source_matched"
        ):
            raise ValueError(f"Saved HTML hash mismatch: {page_id}")
        pending = [record for record in group if not accepted_description(record)]
        for start in range(0, len(pending), llm.config.statement_batch_size):
            batch = pending[start : start + llm.config.statement_batch_size]
            aliases = {f"s{i}": record for i, record in enumerate(batch, 1)}
            history = []
            for attempt in range(llm.config.max_review_attempts):
                active = {
                    key: record
                    for key, record in aliases.items()
                    if not accepted_description(record)
                }
                if not active:
                    break
                schema = DescriptionChecks.model_json_schema()
                schema["$defs"]["DescriptionCheck"]["properties"]["statement_id"][
                    "enum"
                ] = list(active)
                prompt = (
                    REVIEW_PAGE_DESCRIPTIONS
                    + "\nINPUT DATA:\n"
                    + json.dumps(
                        {
                            "task": "page_description_review",
                            "page_id": page_id,
                            "source_url": page.source_url,
                            "cleaned_html": html[window_start:window_end],
                            "statements": [
                                {
                                    "statement_id": key,
                                    "data": statement_data(record),
                                    "original_description": (
                                        record.data.get("description_revisions") or [{}]
                                    )[0].get("before"),
                                    "correction_allowed": attempt + 1
                                    < llm.config.max_review_attempts
                                    and len(
                                        record.data.get("description_revisions", [])
                                    )
                                    < llm.config.max_corrections,
                                }
                                for key, record in active.items()
                            ],
                        }
                    )
                )
                checks, issues = {}, {}
                try:
                    reply = await llm.ask(
                        prompt,
                        schema,
                        task=f"description_review:{page_id}:{window_start}:{start}:{attempt}",
                    )
                    values = (
                        reply.document.get("checks")
                        if isinstance(reply.document, dict) and reply.error is None
                        else None
                    )
                    if not isinstance(values, list):
                        raise ValueError(reply.error or "Missing description checks")
                    checks, issues = validate_description_checks(values, active)
                except (ValueError, ModelBudgetExceeded, ModelUnavailable) as error:
                    issues = {key: str(error) for key in active}
                correction_candidates = {}
                for key, record in active.items():
                    review = apply_description_review(
                        record,
                        checks.get(key),
                        issues.get(key, "Missing description check"),
                        allow_correction=attempt + 1 < llm.config.max_review_attempts
                        and len(record.data.get("description_revisions", []))
                        < llm.config.max_corrections,
                        page=page,
                        window=HtmlWindow(
                            window_start, window_end, html[window_start:window_end]
                        ),
                    )
                    candidate = review.pop("correction_candidate", None)
                    if candidate is not None:
                        correction_candidates[key] = Finding.model_validate(candidate)
                    history.append(
                        {
                            "statement_id": record.record_id,
                            "attempt": attempt,
                            "review": review,
                        }
                    )
                if correction_candidates:
                    window = HtmlWindow(
                        window_start, window_end, html[window_start:window_end]
                    )
                    await repair_evidence(
                        [
                            ("page_statements", candidate)
                            for candidate in correction_candidates.values()
                        ],
                        window,
                        page,
                        llm,
                        root,
                        f"description-correction-{page_id}-{window_start}-{start}-{attempt}",
                    )
                    for key, candidate in correction_candidates.items():
                        if candidate.evidence_status != "source_matched":
                            continue
                        check = checks[key].model_copy(deep=True)
                        assert check.correction is not None
                        source = next(
                            s
                            for s in candidate.sources
                            if s.evidence_status == "source_matched"
                        )
                        check.correction.evidence = [
                            fragment.text for fragment in source.evidence
                        ]
                        review = apply_description_review(
                            active[key],
                            check,
                            "",
                            allow_correction=True,
                            page=page,
                            window=window,
                        )
                        review.pop("correction_candidate", None)
                        history.append(
                            {
                                "statement_id": active[key].record_id,
                                "attempt": attempt,
                                "evidence_repaired": True,
                                "review": review,
                            }
                        )
                write_json(
                    root
                    / "page-description-review"
                    / f"{page_id}-{window_start}-{start}.json",
                    {
                        "history": history,
                        "records": [r.model_dump() for r in batch],
                    },
                )
            errors.extend(
                f"{record.record_id}: description {record.data['description_review']['status']}"
                for record in batch
                if not accepted_description(record)
            )
    return errors
