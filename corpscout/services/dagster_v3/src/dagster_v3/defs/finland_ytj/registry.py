"""JSON extraction helpers for YTJ v3 company payloads (DuckDB UDFs).

Language codes: "1" = Finnish, "2" = Swedish, "3" = English.
Register codes: "1" trade register, "5" prepayment, "6" VAT, "7" employer.
A form/entry is current when it has no endDate.
"""

import json
from typing import Any

_LANGUAGE_KEYS = {"1": "description_fi", "2": "description_sv", "3": "description_en"}
_FLAG_REGISTERS = {
    "6": "is_vat_registered",
    "5": "is_prepayment_registered",
    "7": "is_employer_registered",
}


def _int_or_zero(value: Any) -> int:
    """Safely coerce a value to int, returning 0 on failure."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _loads(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        return None
    return payload if isinstance(payload, dict) else None


def _is_current(item: dict[str, Any]) -> bool:
    return not item.get("endDate")


def legal_form_json(raw: str | None) -> str | None:
    payload = _loads(raw)
    if payload is None:
        return None
    forms = [f for f in payload.get("companyForms") or [] if isinstance(f, dict)]
    current = [f for f in forms if _is_current(f)]
    candidates = current or forms
    if not candidates:
        return None
    picked = max(
        candidates,
        key=lambda f: (str(f.get("registrationDate") or ""), _int_or_zero(f.get("version"))),
    )
    result: dict[str, Any] = {
        "code": picked.get("type"),
        "description_fi": None,
        "description_sv": None,
        "description_en": None,
        "registration_date": picked.get("registrationDate"),
    }
    for description in picked.get("descriptions") or []:
        if not isinstance(description, dict):
            continue
        key = _LANGUAGE_KEYS.get(str(description.get("languageCode")))
        if key is not None and description.get("description"):
            result[key] = description["description"]
    return json.dumps(result, ensure_ascii=False)


def registration_flags_json(raw: str | None) -> str | None:
    payload = _loads(raw)
    if payload is None:
        return None
    flags = {name: 0 for name in _FLAG_REGISTERS.values()}
    for entry in payload.get("registeredEntries") or []:
        if not isinstance(entry, dict) or not _is_current(entry):
            continue
        flag = _FLAG_REGISTERS.get(str(entry.get("register")))
        if flag is not None:
            flags[flag] = 1
    return json.dumps(flags)
