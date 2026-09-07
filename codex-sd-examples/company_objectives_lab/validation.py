"""Validate each independent assessment or record without losing its valid peers."""

import json
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup
from pydantic import ValidationError

from company_objectives_lab.models import RECORD_TYPES, CandidateAssessment
from jobs_extraction_lab.extract import normalize_text


def validate_selection(
    raw: str | None, finish: str | None, candidate_ids: set[str]
) -> dict:
    parsed = parse_envelope(raw, finish, {"assessments"})
    if parsed["issues"]:
        return parsed
    accepted, rejected, issues, seen = [], [], [], set()
    for index, value in enumerate(parsed["document"]["assessments"]):
        errors = []
        try:
            assessment = CandidateAssessment.model_validate(value)
        except ValidationError as error:
            errors = error.errors(include_input=False, include_url=False)
        else:
            if assessment.candidate_id not in candidate_ids:
                errors.append({"type": "unknown_candidate"})
            if assessment.candidate_id in seen:
                errors.append({"type": "duplicate_candidate"})
            for objective, judgement in assessment.objectives.model_dump().items():
                if (
                    judgement["potential"] in {"high", "medium"}
                    and judgement["role"] == "none"
                ) or (judgement["potential"] == "low" and judgement["role"] != "none"):
                    errors.append(
                        {"type": "inconsistent_potential_role", "objective": objective}
                    )
            if not errors:
                accepted.append(assessment.model_dump())
                seen.add(assessment.candidate_id)
        if errors:
            rejected.append({"record_index": index, "record": value, "issues": errors})
            issues.extend({"record_index": index, **error} for error in errors)
    missing = sorted(candidate_ids - seen)
    if missing:
        issues.append({"type": "missing_candidates", "candidate_ids": missing})
    return {"accepted": accepted, "rejected": rejected, "issues": issues}


def parse_envelope(raw: str | None, finish: str | None, keys: set[str]) -> dict:
    if finish != "stop" or not isinstance(raw, str):
        return {
            "accepted": [],
            "rejected": [],
            "issues": [{"type": "incomplete_response", "finish_reason": finish}],
        }
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as error:
        return {
            "accepted": [],
            "rejected": [],
            "issues": [
                {
                    "type": "invalid_json",
                    "message": error.msg,
                    "line": error.lineno,
                    "column": error.colno,
                }
            ],
        }
    if (
        not isinstance(document, dict)
        or set(document) != keys
        or any(not isinstance(v, list) for v in document.values())
    ):
        return {
            "accepted": [],
            "rejected": [],
            "issues": [{"type": "invalid_envelope"}],
        }
    return {"document": document, "issues": []}


def validate_extraction(
    raw: str | None, finish: str | None, source_url: str, html: str
) -> dict:
    parsed = parse_envelope(raw, finish, set(RECORD_TYPES))
    if parsed["issues"]:
        return parsed
    soup = BeautifulSoup(html, "html.parser")
    source_text = normalize_text(soup.get_text(" ", strip=True))
    source_html = normalize_text(html)
    urls = {source_url}
    urls.update(
        urljoin(source_url, str(link["href"])) for link in soup.find_all(href=True)
    )
    accepted, rejected, issues = [], [], []
    for objective, values in parsed["document"].items():
        for index, value in enumerate(values):
            errors = []
            try:
                record = RECORD_TYPES[objective].model_validate(value).model_dump()
            except ValidationError as error:
                errors = error.errors(include_input=False, include_url=False)
            else:
                evidence = normalize_text(record["evidence"])
                if evidence not in source_text and evidence not in source_html:
                    errors.append({"type": "evidence_absent"})
                for key in ("url", "profile_url", "job_url"):
                    value_url = record.get(key)
                    if value_url is not None and (
                        urlsplit(value_url).scheme not in {"http", "https"}
                        or value_url not in urls
                    ):
                        errors.append({"type": "url_absent", "field": key})
                if (
                    objective == "company_contacts"
                    and record["type"] in {"form", "social"}
                    and record["value"] not in urls
                ):
                    errors.append({"type": "url_absent", "field": "value"})
                if not errors:
                    accepted.append(
                        {
                            "objective": objective,
                            "record_index": index,
                            "record": record,
                        }
                    )
            if errors:
                rejected.append(
                    {
                        "objective": objective,
                        "record_index": index,
                        "record": value,
                        "issues": errors,
                    }
                )
                issues.extend(
                    {"objective": objective, "record_index": index, **error}
                    for error in errors
                )
    return {"accepted": accepted, "rejected": rejected, "issues": issues}


def retain_attempts(stage: str, attempts: list[dict]) -> dict:
    """Keep initial valid assessments; union extraction records without resolving conflicts."""
    accepted, seen, alternatives = [], set(), []
    for attempt_index, attempt in enumerate(attempts, 1):
        for entry in attempt["accepted"]:
            key = (
                entry["candidate_id"]
                if stage == "selection"
                else json.dumps(
                    {
                        "objective": entry["objective"],
                        "record": {
                            k: v for k, v in entry["record"].items() if k != "evidence"
                        },
                    },
                    sort_keys=True,
                )
            )
            observation = {**entry, "attempt_number": attempt_index}
            if key not in seen:
                accepted.append(observation)
                seen.add(key)
            else:
                alternatives.append(observation)
    return {"accepted": accepted, "alternatives": alternatives}
