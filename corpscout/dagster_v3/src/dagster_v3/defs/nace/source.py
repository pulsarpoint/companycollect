from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class NaceScheme:
    classification_version: str
    scheme_uri: str
    valid_from: str
    valid_to: str | None
    is_current: int


def normalize_nace_code(code: str) -> str:
    stripped = code.strip()
    return stripped if stripped.isalpha() else "".join(char for char in stripped if char.isalnum())


def nace_level(code: str) -> str:
    normalized = normalize_nace_code(code)
    if normalized.isalpha():
        return "section"
    if len(normalized) == 2:
        return "division"
    if len(normalized) == 3:
        return "group"
    if len(normalized) == 4:
        return "class"
    raise ValueError(f"Unsupported NACE code level: {code}")


def concept_code(concept_uri: str) -> str | None:
    value = concept_uri.rstrip("/").rsplit("/", maxsplit=1)[-1]
    if not value:
        return None
    if value.isalpha() or len(value) == 2:
        return value
    if len(value) in {3, 4}:
        return f"{value[:2]}.{value[2:]}"
    return value


def _section_code_for(
    code: str,
    parent_by_code: dict[str, str | None],
    section_by_code: dict[str, str],
) -> str | None:
    if code in section_by_code:
        return section_by_code[code]

    parent_code = parent_by_code.get(code)
    while parent_code:
        if parent_code in section_by_code:
            return section_by_code[parent_code]
        parent_code = parent_by_code.get(parent_code)
    return None


def build_nace_rows(
    *,
    scheme: NaceScheme,
    source_rows: list[dict[str, str]],
    source_url: str,
    source_payload_hash: str,
    source_run_id: str,
    pulled_at: str,
) -> list[dict[str, Any]]:
    if not source_rows:
        raise ValueError(f"{scheme.classification_version} returned no NACE rows")

    parent_by_code: dict[str, str | None] = {}
    section_by_code: dict[str, str] = {}
    for source_row in source_rows:
        code = source_row["notation"].strip()
        parent_concept_uri = source_row.get("broader", "").strip() or None
        parent_by_code[code] = concept_code(parent_concept_uri) if parent_concept_uri else None
        if nace_level(code) == "section":
            section_by_code[code] = code

    rows: list[dict[str, Any]] = []
    for source_row in source_rows:
        code = source_row["notation"].strip()
        normalized_code = normalize_nace_code(code)
        level = nace_level(code)
        parent_concept_uri = source_row.get("broader", "").strip() or None
        parent_code = concept_code(parent_concept_uri) if parent_concept_uri else None
        rows.append(
            {
                "classification_version": scheme.classification_version,
                "code": code,
                "normalized_code": normalized_code,
                "parent_code": parent_code,
                "level": level,
                "section_code": _section_code_for(code, parent_by_code, section_by_code),
                "description_en": source_row["label"].strip(),
                "concept_uri": source_row["concept"].strip(),
                "parent_concept_uri": parent_concept_uri,
                "source_scheme_uri": scheme.scheme_uri,
                "source_url": source_url,
                "source_payload_hash": source_payload_hash,
                "valid_from": scheme.valid_from,
                "valid_to": scheme.valid_to,
                "is_current": scheme.is_current,
                "source_run_id": source_run_id,
                "pulled_at": pulled_at,
            }
        )
    return rows
