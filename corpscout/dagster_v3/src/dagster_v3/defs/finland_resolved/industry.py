"""Primary industry (business line) extraction from raw PRH company JSON."""

import json


def primary_industry_json(raw_company: str | None) -> str | None:
    if not raw_company:
        return None
    try:
        payload = json.loads(raw_company)
    except json.JSONDecodeError:
        return None
    for key in ("businessLine", "mainBusinessLine"):
        business_line = payload.get(key)
        if isinstance(business_line, dict):
            return json.dumps(_normalize_business_line(business_line))
    business_lines = payload.get("businessLines")
    if isinstance(business_lines, list):
        for business_line in business_lines:
            if isinstance(business_line, dict):
                return json.dumps(_normalize_business_line(business_line))
    return None


def _normalize_business_line(business_line: dict[str, object]) -> dict[str, object]:
    description, language = _select_business_line_description(business_line)
    return {
        "code": business_line.get("code") or business_line.get("type"),
        "codeSet": business_line.get("codeSet") or business_line.get("typeCodeSet"),
        "description": description,
        "language": business_line.get("language") or language,
    }


def _select_business_line_description(
    business_line: dict[str, object],
) -> tuple[object, str | None]:
    direct_description = business_line.get("description")
    if direct_description:
        language = business_line.get("language")
        return direct_description, str(language) if language else None
    descriptions = business_line.get("descriptions")
    if not isinstance(descriptions, list):
        return None, None
    selected_description = _find_description_by_language(descriptions, "1")
    if selected_description is None:
        selected_description = _first_description(descriptions)
    if selected_description is None:
        return None, None
    language_code = selected_description.get("languageCode")
    return selected_description.get("description"), _business_line_language(language_code)


def _find_description_by_language(
    descriptions: list[object],
    language_code: str,
) -> dict[str, object] | None:
    for description in descriptions:
        if (
            isinstance(description, dict)
            and str(description.get("languageCode")) == language_code
            and description.get("description")
        ):
            return description
    return None


def _first_description(descriptions: list[object]) -> dict[str, object] | None:
    for description in descriptions:
        if isinstance(description, dict) and description.get("description"):
            return description
    return None


def _business_line_language(language_code: object) -> str | None:
    return {"1": "fi", "2": "sv", "3": "en"}.get(str(language_code))
