"""The Swedish address normalizer (spec 2026-09-06 section 4).

Pure: raw fields in, folded components, a display line, an identity key and a parse status
out. It splits, folds and classifies; it never expands abbreviations, corrects spelling or
guesses a house number (that is the geocoder's job). Every behaviour change bumps
NORMALIZER_VERSION, which is what re-normalizes stored rows.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass

NORMALIZER_VERSION = "se-address-normalizer-v1"

_UNKNOWN_TOWNS = {"okänd", "okand", "adress saknas"}
_FOREIGN_TOWNS = {"utlandet"}
_UNKNOWN_STREETS = {"okänd adress", "adress okänd", "okand adress", "adress saknas"}
_INVALID_POSTCODES = {"00000", "99999"}
_CARE_OF_PREFIX = re.compile(r"^(?:c/o|c\.o\.|co|att|attn|att:)\s+", re.IGNORECASE)
_BOX = re.compile(r"^(?:box|postbox|p\.?\s?o\.?\s?box)\s+(?P<box>[0-9]+[a-zåäö]?)$", re.IGNORECASE)
_NUMBER = re.compile(
    r"^(?P<name>.*?\S)\s+(?P<number>[0-9]+(?:\s?-\s?[0-9]+)?(?:\s?[a-zåäö])?)(?:[\s,.]+(?P<rest>\S.*))?$",
    re.IGNORECASE,
)
_UNIT = re.compile(
    r"^(?:lgh\s*[0-9]+|[0-9]+\s*tr\.?|tr\s*[0-9]+|bv|nb|t[0-9]+|[0-9]+\s*(?:vån|van)\.?|vån\s*[0-9]+|uppg\.?\s*[a-z0-9]+|ing\.?\s*[a-z0-9]+)$",
    re.IGNORECASE,
)
_GLUED_NUMBER = re.compile(r"^(?P<word>[a-zåäöé]+)(?P<number>[0-9]+[a-zåäö]?)$")
_STREET_PREFIXES = {"stora", "lilla", "norra", "södra", "östra", "västra", "gamla", "nya", "övre", "nedre", "sankt", "s:t", "st", "s:ta"}
_STREET_GENERICS = {"gata", "gatan", "väg", "vägen", "torg", "torget", "plan", "gränd", "gränden", "allé", "allén", "stig", "stigen", "led", "leden", "backe", "backen", "ring", "ringen", "park", "parken", "hamn", "hamnen", "kaj", "kajen", "bro", "bron", "esplanad", "esplanaden", "boulevard", "promenad", "promenaden", "gård", "gården"}
_KEEP_UPPER = {"ab", "hb", "kb", "ek", "ab:s"}


@dataclass(frozen=True, slots=True)
class RawAddress:
    raw_address: str | None = None
    care_of: str | None = None
    street_address: str | None = None
    postal_code: str | None = None
    post_town: str | None = None
    county: str | None = None
    country_code: str | None = None


@dataclass(frozen=True, slots=True)
class NormalizedAddress:
    care_of: str | None
    box: str | None
    street_name: str | None
    house_number: str | None
    unit: str | None
    postal_code: str | None
    city: str | None
    country_code: str
    normalized_address: str
    parse_status: str
    parse_notes: str


def _clean(value: str | None) -> str:
    """NFKC, Unicode format characters (category Cf: zero-width space and friends)
    stripped, whitespace collapse, trailing punctuation dropped; case kept."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", value)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Cf")
    text = re.sub(r"[\s ]+", " ", text).strip(" .,;:-/")
    return text


def _fold(value: str | None) -> str:
    return _clean(value).casefold()


def _display(value: str) -> str:
    """Delivered casing, except an all-caps text is title-cased (company suffixes kept upper)."""
    text = _clean(value)
    if text != text.upper() and text != text.lower():
        return text
    words = []
    for word in text.casefold().split(" "):
        if word in _KEEP_UPPER:
            words.append(word.upper())
        else:
            words.append("-".join(p[:1].upper() + p[1:] for p in word.split("-")))
    return " ".join(words)


def _split_packed(raw: str, notes: list[str]) -> RawAddress:
    parts = [p.strip() for p in raw.split("$")]
    if len(parts) != 5:
        notes.append(f"packed address has {len(parts)} parts, expected 5")
    parts += [""] * (5 - len(parts))
    line1, line2, town, code, country = parts[:5]
    return RawAddress(
        care_of=line2 or None,
        street_address=line1 or None,
        postal_code=code or None,
        post_town=town or None,
        country_code=(country.upper()[:2] if country else None),
    )


def _split_street(line: str, notes: list[str]) -> tuple[str | None, str | None, str | None, str | None]:
    """-> (box, street_name, house_number, unit) from a folded street line."""
    box = _BOX.match(line)
    if box:
        return box.group("box"), None, None, None
    line = re.sub(r"\s*,\s*", " ", line)
    m = _NUMBER.match(line)
    if not m:
        glued = _GLUED_NUMBER.match(line)
        if glued:
            return None, glued.group("word"), glued.group("number"), None
        return None, line or None, None, None
    name = m.group("name")
    number = re.sub(r"\s+", "", m.group("number"))
    rest = (m.group("rest") or "").strip(" .,")
    unit = None
    if rest:
        if _UNIT.match(rest):
            unit = re.sub(r"\s+", " ", rest)
        else:
            notes.append(f"dropped trailing text '{rest}'")
    return None, name, number, unit


def _split_care_of_street(line: str, notes: list[str]) -> tuple[str | None, str]:
    """Ratsit packs 'c/o <name> <street> <number>' into one string: split at the street."""
    stripped = re.sub(r"\s*,\s*", " ", _CARE_OF_PREFIX.sub("", line))
    tokens = stripped.split(" ")
    number_at = None
    for i, tok in enumerate(tokens):
        if i > 0 and (re.fullmatch(r"[0-9]+(?:-[0-9]+)?[a-zåäö]?", tok) or _GLUED_NUMBER.match(tok)):
            number_at = i
            break
    if number_at is None:
        notes.append("care-of without a street number")
        return stripped or None, ""
    if _GLUED_NUMBER.match(tokens[number_at]):
        start = number_at
    else:
        start = number_at - 1
        if tokens[start] in _STREET_GENERICS and start >= 1:
            start -= 1
        while start >= 1 and tokens[start - 1] in _STREET_PREFIXES:
            start -= 1
    care_of = " ".join(tokens[:start]).strip() or None
    street = " ".join(tokens[start:])
    notes.append(f"care-of split before '{street}'")
    return care_of, street


def normalize_se_address(raw: RawAddress) -> NormalizedAddress:
    notes: list[str] = []
    if raw.raw_address:
        raw = _split_packed(raw.raw_address, notes)
    care_of_display = _display(_CARE_OF_PREFIX.sub("", _clean(raw.care_of)))
    care_of = care_of_display.casefold()
    street_line = _fold(raw.street_address)
    street_display_source = _clean(raw.street_address)
    town = _fold(raw.post_town)
    town_display = _display(raw.post_town or "")
    code = re.sub(r"\D", "", raw.postal_code or "")
    country = (raw.country_code or "").strip().upper()

    if town in _FOREIGN_TOWNS:
        return NormalizedAddress(None, None, None, None, None, None, None, country or "", "", "foreign", "post town marks the address as foreign")
    if country and country != "SE":
        return NormalizedAddress(None, None, None, None, None, None, None, country, "", "foreign", f"country {country}")
    if town in _UNKNOWN_TOWNS or street_line in _UNKNOWN_STREETS:
        return NormalizedAddress(None, None, None, None, None, None, None, "SE", "", "no_address", "source marks the address as unknown")
    if not street_line and not care_of:
        return NormalizedAddress(None, None, None, None, None, None, None, "SE", "", "no_address", "")

    if _CARE_OF_PREFIX.match(street_line) and not care_of:
        care_of, street_line = _split_care_of_street(street_line, notes)
        care_of_display = _display(care_of or "")
        street_display_source = ""
    box, street_name, house_number, unit = _split_street(street_line, notes) if street_line else (None, None, None, None)

    if code and (len(code) != 5 or code in _INVALID_POSTCODES):
        notes.append(f"postcode '{raw.postal_code}' is not a valid five-digit code")
        code = ""
    if town in _UNKNOWN_TOWNS:
        town = ""

    has_location = bool(box or street_name)
    if not has_location:
        return NormalizedAddress(None, None, None, None, None, None, None, "SE", "", "no_address", "; ".join(notes))
    if code and town:
        status = "ok"
    else:
        status = "partial"
        if not code:
            notes.append("missing postcode")
        if not town:
            notes.append("missing city")

    line_parts = []
    if care_of:
        line_parts.append(f"c/o {care_of_display}")
    if box:
        line_parts.append(f"Box {box.upper()}")
    elif street_name:
        mixed_case = street_display_source not in ("", street_display_source.upper(), street_display_source.lower())
        street = _display_kept(street_display_source, street_name) if mixed_case else _display(street_name)
        if house_number:
            street += f" {house_number.upper()}"
        if unit:
            street += f" {unit}"
        line_parts.append(street)
    postal = " ".join(p for p in (f"{code[:3]} {code[3:]}" if code else "", town_display if town else "") if p)
    if postal:
        line_parts.append(postal)
    display = ", ".join(line_parts) if has_location else ""

    return NormalizedAddress(
        care_of=care_of or None,
        box=box.upper() if box else None,
        street_name=street_name or None,
        house_number=house_number.upper() if house_number else None,
        unit=unit or None,
        postal_code=code or None,
        city=town or None,
        country_code="SE",
        normalized_address=display,
        parse_status=status,
        parse_notes="; ".join(notes),
    )


def _display_kept(source_line: str, street_name: str) -> str:
    """The street name in the delivered casing: the folded name is a prefix of the folded line."""
    folded = re.sub(r"\s*,\s*", " ", source_line.casefold())
    if folded.startswith(street_name):
        return re.sub(r"\s*,\s*", " ", source_line)[: len(street_name)]
    return _display(street_name)


IDENTITY_FIELDS: tuple[str, ...] = (
    "country_code", "postal_code", "city", "street_name", "box", "house_number", "unit", "care_of",
)


def identity_components(normalized: NormalizedAddress) -> tuple[str, ...]:
    """Spec section 5.1: the eight components, NULL as ''."""
    return tuple(getattr(normalized, field_name) or "" for field_name in IDENTITY_FIELDS)


def address_key(normalized: NormalizedAddress) -> str:
    return hashlib.sha256("\n".join(identity_components(normalized)).encode("utf-8")).hexdigest()

