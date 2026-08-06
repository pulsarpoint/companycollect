"""Shared context decisions for deterministic ESEF contact candidates."""

import re
import unicodedata


_CANDIDATE_MARKER = "__candidate__"
_SENTENCE_BOUNDARY_PATTERN = re.compile(r"(?<=[.!?])\s+|\s*[|•]\s*")
_REPORT_PRODUCTION_CREDIT_PATTERN = re.compile(
    r"(?:"
    r"\bdesign(?:ed)?\s+(?:and|&)\s+production\b|"
    r"\bdesign(?:ed)?\s+by\b|"
    r"\bgraphic\s+design\b|"
    r"\bgrafi\s*k\s+(?:och|&)\s+original\b|"
    r"\b(?:printed|produced|typeset|layout|illustrations?)\s+by\b|"
    r"\b(?:print|production|layout|photography|photo|foto(?:grafi)?|"
    r"tryck|produktion)\s*:|"
    r"\bkommunikationsbyr[åa]\b|"
    r"\bcommunications?\s+agency\b|"
    r"\banimations?studio\b|"
    r"\bpowered\s+by\b|"
    r"\b(?:xbrl|ixbrl|esef)\s+"
    r"(?:software|platform|solution|tagging|provider|service)\b"
    r")",
    re.IGNORECASE,
)


def is_report_production_credit(context: str, *candidate_values: str) -> bool:
    """Return whether a candidate occurs inside a report-production credit."""
    normalized_context = _normalized_text(context)
    candidate_found = False
    for candidate in sorted(
        {
            _normalized_text(value).strip()
            for value in candidate_values
            if value.strip() != ""
        },
        key=len,
        reverse=True,
    ):
        if candidate not in normalized_context:
            continue
        normalized_context = normalized_context.replace(
            candidate,
            f" {_CANDIDATE_MARKER} ",
        )
        candidate_found = True
    if not candidate_found:
        return False
    return any(
        _CANDIDATE_MARKER in sentence
        and _REPORT_PRODUCTION_CREDIT_PATTERN.search(sentence) is not None
        for sentence in _SENTENCE_BOUNDARY_PATTERN.split(normalized_context)
    )


def _normalized_text(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())
