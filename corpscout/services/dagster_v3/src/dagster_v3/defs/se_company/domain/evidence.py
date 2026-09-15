"""Stable model inputs: identity and observations, without extraction bookkeeping."""

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def domain_evidence(suggestions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"id": f"e{index}", **{key: row[key] for key in (
            "source", "root_domain", "website_url", "association", "is_primary", "confidence",
            "confidence_basis", "source_url", "evidence",
        )}}
        for index, row in enumerate(sorted(suggestions, key=lambda row: (row["source"], row["slot"])))
        if row["removed"] == 0 and row["source"] not in ("reviewer", "reviewer_draft")
    ]


def input_payload(company: Mapping[str, Any], root_domain: str, suggestions: Sequence[Mapping[str, Any]]) -> str:
    return json_text({"country_code": "SE", "company": dict(company), "domain": root_domain, "evidence": domain_evidence(suggestions)})


def requires_verification(suggestions: Sequence[Mapping[str, Any]]) -> bool:
    live = [row for row in suggestions if not row["removed"] and row["source"] not in ("reviewer", "reviewer_draft")]
    positive = any(row["association"] == "connected" and row["confidence"] >= 0.9 for row in live)
    negative = any(row["association"] == "not_connected" and row["confidence"] >= 0.9 for row in live)
    return bool(live) and ((not positive and not negative) or (positive and negative))
