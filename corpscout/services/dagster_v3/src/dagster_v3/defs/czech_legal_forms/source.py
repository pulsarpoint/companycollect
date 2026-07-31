"""ARES's pravni forma code list, read from its ciselniky endpoint.

cz_companies stores a bare code -- `112`, never "Spolecnost s rucenim
omezenym". Like France and unlike Latvia, there is no label column beside it,
so 108,341 companies on 38 codes displayed a number with nothing to fall back
to and nothing to translate.

ARES publishes the list as JSON, so unlike INSEE there is no spreadsheet or
RDF to work around. It answers with THREE lists under the same code-list name,
distinguished by `zdrojCiselniku` -- the register each belongs to:

    res   141 items   registr ekonomickych subjektu (the statistical register)
    com    16 items   obchodni rejstrik (the commercial register)
    rzp     1 item    zivnostensky rejstrik (the trade register)

All three are merged. `res` alone covers 69 of the 71 codes our companies
carry, but 332 and 963 appear only in `com`, and taking res alone would leave
those unnamed for no reason.

A code is renamed rather than retired, so entries carry validity windows and
the same code appears several times. 352 has been Ceske drahy, then Sprava
zeleznicni dopravni cesty, then Statni organizace Sprava zeleznic -- a company
carrying it today is the last of those.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping

ARES_CISELNIK_URL = (
    "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ciselniky-nazevniky/vyhledat"
)
ARES_CISELNIK_BODY = {"kodCiselniku": "PravniForma"}
CODE_LIST_NAME = "PravniForma"
CZECH_LANGUAGE = "cs"

# 141 + 16 + 1 merges to roughly 150 distinct codes. A floor, so a revision
# does not fail the load while a truncated response still does.
MIN_LEGAL_FORM_ROWS = 120

# ARES writes an open-ended window as this rather than omitting the field.
_OPEN_ENDED = "9999-09-09"


@dataclass(frozen=True)
class LegalForm:
    code: str
    label_cs: str
    valid_from: str
    valid_to: str


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        # 9999-09-09 is a valid ISO date, but a malformed one must not take
        # the whole load down over a single entry.
        return None


def _czech_label(item: Mapping[str, Any]) -> str:
    for name in item.get("nazev") or ():
        if name.get("kodJazyka") == CZECH_LANGUAGE:
            return str(name.get("nazev") or "").strip()
    return ""


def parse_legal_forms(payload: Mapping[str, Any], today: date) -> list[LegalForm]:
    """One row per code: the name in force today, else the most recent one.

    An expired code is kept rather than dropped. Code 106's windows all closed
    in 2013 and companies still carry it, so dropping it would put those rows
    back to showing a bare number -- the register's last word on a code is a
    better answer than none.
    """
    candidates: dict[str, list[LegalForm]] = {}
    for code_list in payload.get("ciselniky") or ():
        if code_list.get("kodCiselniku") != CODE_LIST_NAME:
            continue
        for item in code_list.get("polozkyCiselniku") or ():
            code = str(item.get("kod") or "").strip()
            label = _czech_label(item)
            if not code or not label:
                continue
            candidates.setdefault(code, []).append(
                LegalForm(
                    code=code,
                    label_cs=label,
                    valid_from=str(item.get("platnostOd") or ""),
                    # A missing end date means open, as does the sentinel.
                    valid_to=str(item.get("platnostDo") or _OPEN_ENDED),
                )
            )

    forms = []
    for code, entries in candidates.items():
        forms.append(_pick_current(entries, today))
    return sorted(forms, key=lambda form: form.code)


def _pick_current(entries: list[LegalForm], today: date) -> LegalForm:
    def started(form: LegalForm) -> date | None:
        return _parse_date(form.valid_from)

    def in_force(form: LegalForm) -> bool:
        start, end = started(form), _parse_date(form.valid_to)
        if start is not None and start > today:
            return False
        return end is None or end >= today

    live = [form for form in entries if in_force(form)]
    pool = live or entries
    # Latest start wins: among live windows there is normally one, and among
    # expired ones the most recent name is the register's last word.
    return max(pool, key=lambda form: (started(form) or date.min, form.label_cs))
