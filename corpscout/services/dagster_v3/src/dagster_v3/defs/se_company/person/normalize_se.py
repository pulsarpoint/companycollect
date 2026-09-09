"""The Swedish person normalizer (spec 2026-09-09 section 4).

Pure: one source's delivered name and role fields in, a display spelling, identity tokens,
a role code and a parse status out. It splits, folds and classifies; it never invents a
name, never guesses a missing half and never drops a role it cannot map. Every behaviour
change bumps NORMALIZER_VERSION, which is what re-normalizes stored rows.

WHAT THE STATUSES MEAN (spec 4.4). `ok`: a first and a last token exist -- foldable into a
person. `partial`: one name word, or nothing but initials -- stored with its notes, never
folded into a person. `no_person`: the name field holds a role word, a number or a date, a
company suffix, or nothing at all. The August 2026 audit found roles in the name field and
dates in the role field, which is why the no_person rules read the NAME field and the role
mapping never touches it.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from dagster_v3.defs.se_company.person.roles import role_code_for

NORMALIZER_VERSION = "se-person-normalizer-v1"
PARSE_STATUSES: tuple[str, ...] = ("ok", "partial", "no_person")

_WHITESPACE = re.compile(r"\s+")
_DIGIT = re.compile(r"\d")
_SUBTOKEN_SPLIT = re.compile(r"[.\-]+")
_NON_TOKEN = re.compile(r"[^a-z0-9]+")

# Particles glue to the last name, in the display spelling and in the tokens (spec 4.1, 4.2).
_PARTICLES = frozenset({"von", "af", "de", "van", "der", "la", "le"})
# Honorifics dropped from the display spelling. Matched on the WHOLE folded word before any
# hyphen split, so the Swedish given name Ing-Marie is never mistaken for a title.
_TITLE_WORDS = frozenset({"dr", "prof", "professor", "doktor", "herr", "fru", "froken", "mr", "mrs", "ms"})
_TITLE_PHRASES = (
    ("jur", "kand"), ("civ", "ing"), ("civ", "ekon"),
    ("ekon", "dr"), ("fil", "dr"), ("med", "dr"), ("jur", "dr"),
)
# A role word in the NAME field means the row is not a person. Whole tokens, in order.
_ROLE_PHRASES = (
    ("styrelseledamot",), ("styrelseordforande",), ("styrelsesuppleant",), ("ordforande",),
    ("suppleant",), ("ledamot",), ("revisor",), ("likvidator",), ("firmatecknare",),
    ("arbetstagarrepresentant",), ("vd",), ("verkstallande", "direktor"), ("vice", "vd"),
    ("huvudansvarig", "revisor"), ("auktoriserad", "revisor"),
    ("board", "member"), ("board", "chair"), ("chairman",), ("auditor",), ("liquidator",),
    ("chief", "executive", "officer"), ("director",), ("founder",), ("owner",),
)
# Whole tokens only. `ek` is deliberately absent: Ek is a common Swedish surname.
_COMPANY_TOKENS = frozenset({"ab", "hb", "kb", "aktiebolag", "handelsbolag", "kommanditbolag"})


@dataclass(frozen=True, slots=True)
class RawPerson:
    """One suggestion row's person and role fields, as the source delivered them."""

    source: str = ""
    full_name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    birth_year: int | None = None
    wikidata_id: str | None = None
    role_original: str | None = None
    # The source's own mapping key beside the label, lifted out of the row's `data` by the
    # extractor: Bolagsverket's role_kind, Wikidata's property id, ESEF's role category.
    role_key: str | None = None


@dataclass(frozen=True, slots=True)
class NormalizedPerson:
    parse_status: str
    parse_notes: tuple[str, ...]
    first_tokens: tuple[str, ...]
    middle_tokens: tuple[str, ...]
    last_tokens: tuple[str, ...]
    display_first: str
    display_last: str
    display_name: str
    role_code: str | None


def _clean(text: str | None) -> str:
    return _WHITESPACE.sub(" ", text.strip()) if text else ""


def _fold(text: str) -> str:
    """Case-folded and diacritic-free: Håkan and Hakan meet, Ö and O meet (spec 4.2)."""
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(char for char in decomposed if not unicodedata.combining(char)).casefold()


def _fold_word(word: str) -> str:
    """One whole word folded, periods removed, hyphens KEPT (title and particle matching)."""
    return _fold(word).replace(".", "")


def _subtokens(word: str) -> list[str]:
    """The identity tokens of one word: folded, split on hyphens and periods, letters and
    digits only. Sven-Erik gives sven, erik; S.E. gives s, e."""
    pieces = _SUBTOKEN_SPLIT.split(_fold(word))
    return [token for token in (_NON_TOKEN.sub("", piece) for piece in pieces) if token]


def _drop_titles(words: list[str]) -> tuple[list[str], list[str]]:
    """(kept words, dropped titles) -- honorific phrases first, then single honorifics."""
    folded = [_fold_word(word) for word in words]
    kept: list[str] = []
    dropped: list[str] = []
    index = 0
    while index < len(words):
        phrase = next(
            (p for p in _TITLE_PHRASES if tuple(folded[index : index + len(p)]) == p), None
        )
        if phrase is not None:
            dropped.append(" ".join(phrase))
            index += len(phrase)
            continue
        if folded[index] in _TITLE_WORDS:
            dropped.append(folded[index])
            index += 1
            continue
        kept.append(words[index])
        index += 1
    return kept, dropped


def _has_role_phrase(tokens: list[str]) -> bool:
    return any(
        tuple(tokens[start : start + len(phrase)]) == phrase
        for phrase in _ROLE_PHRASES
        for start in range(len(tokens) - len(phrase) + 1)
    )


def _split_full_name(full: str) -> tuple[list[str], list[str], list[str], bool]:
    """(first words, last words, dropped titles, comma form) for a one-string name.

    A comma form is "Last, First". Otherwise the last word is the last name, preceded by any
    run of particles: "Carl von Essen" is Carl / von Essen, "von Essen" is / von Essen.
    """
    if "," in full:
        last_part, _, first_part = full.partition(",")
        last_words, dropped_last = _drop_titles(_clean(last_part).split(" ")) if _clean(last_part) else ([], [])
        first_words, dropped_first = _drop_titles(_clean(first_part).split(" ")) if _clean(first_part) else ([], [])
        return first_words, last_words, dropped_first + dropped_last, True
    words, dropped = _drop_titles(full.split(" "))
    index = max(len(words) - 1, 0)
    while index > 0 and _fold_word(words[index - 1]) in _PARTICLES:
        index -= 1
    return words[:index], words[index:], dropped, False


def normalize_se_person(raw: RawPerson) -> NormalizedPerson:
    """The whole of spec section 4 for one delivered row."""
    role_code = role_code_for(raw.source, role_original=raw.role_original, role_key=raw.role_key)
    first_in, last_in, full = _clean(raw.first_name), _clean(raw.last_name), _clean(raw.full_name)
    split_delivered = bool(first_in or last_in)
    source_text = " ".join(part for part in (first_in, last_in) if part) if split_delivered else full

    def _no_person(note: str) -> NormalizedPerson:
        # The delivered text stays in display_name so the reviewer can see what was rejected.
        return NormalizedPerson(
            parse_status="no_person", parse_notes=(note,),
            first_tokens=(), middle_tokens=(), last_tokens=(),
            display_first="", display_last="", display_name=source_text, role_code=role_code,
        )

    if not source_text:
        return _no_person("empty name")
    tokens = [token for word in source_text.split(" ") for token in _subtokens(word)]
    if _DIGIT.search(source_text):
        return _no_person("digits in the name field")
    if any(token in _COMPANY_TOKENS for token in tokens):
        return _no_person("company suffix in the name field")
    if _has_role_phrase(tokens):
        return _no_person("role word in the name field")

    if split_delivered:
        first_words, dropped_first = _drop_titles(first_in.split(" ")) if first_in else ([], [])
        last_words, dropped_last = _drop_titles(last_in.split(" ")) if last_in else ([], [])
        dropped, comma_form = dropped_first + dropped_last, False
    else:
        first_words, last_words, dropped, comma_form = _split_full_name(full)

    notes: list[str] = []
    if dropped:
        notes.append("removed title " + ", ".join(dropped))
    if comma_form:
        notes.append("comma form")

    given_tokens = [token for word in first_words for token in _subtokens(word)]
    last_tokens = tuple(token for word in last_words for token in _subtokens(word))
    first_tokens = tuple(given_tokens[:1])
    middle_tokens = tuple(given_tokens[1:])
    display_first = " ".join(first_words)
    display_last = " ".join(last_words)

    if not first_tokens or not last_tokens:
        status = "partial"
        notes.append("only one name word")
    elif all(len(token) == 1 for token in (*first_tokens, *middle_tokens)):
        status = "partial"
        notes.append("initials only")
    else:
        status = "ok"

    return NormalizedPerson(
        parse_status=status,
        parse_notes=tuple(notes),
        first_tokens=first_tokens,
        middle_tokens=middle_tokens,
        last_tokens=last_tokens,
        display_first=display_first,
        display_last=display_last,
        display_name=" ".join(part for part in (display_first, display_last) if part),
        role_code=role_code,
    )
