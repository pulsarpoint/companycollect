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

NORMALIZER_VERSION = "se-address-normalizer-v3"

_UNKNOWN_TOWNS = {"okänd", "okand", "adress saknas"}
_FOREIGN_TOWNS = {"utlandet"}
_UNKNOWN_STREETS = {"okänd adress", "adress okänd", "okand adress", "adress saknas"}
_INVALID_POSTCODES = {"00000", "99999"}
_CARE_OF_PREFIX = re.compile(r"^(?:c/o|c\.o\.|co|att|attn|att:)\s+", re.IGNORECASE)
_BOX = re.compile(
    r"^(?:box|postbox|p\.?\s?o\.?\s?box)\s+(?P<box>[0-9]+(?:\s[0-9]{2,3}(?![\s,]*[0-9]))?[a-zåäö]?)(?:[\s,]+(?P<rest>\S.*))?$",
    re.IGNORECASE,
)
# A box after a reference or name ("nabo 118849 box 843", "c/o firm box 12"): the box is the
# address, the prefix is the care-of when none was delivered.
_BOX_AFTER_PREFIX = re.compile(
    r"^(?P<prefix>.+?)[\s,]+(?:box|postbox)\s+(?P<box>[0-9]+(?:\s[0-9]{2,3}(?![\s,]*[0-9]))?[a-zåäö]?)(?:[\s,]+(?P<rest>\S.*))?$",
    re.IGNORECASE,
)
_NUMBER = re.compile(
    r"^(?P<name>.*?\S)\s+(?P<number>[0-9]+(?:\s?-\s?[0-9]+)?(?:\s?[a-zåäö])?)(?:[\s,.]+(?P<rest>\S.*))?$",
    re.IGNORECASE,
)
_UNIT = re.compile(
    r"^(?:lgh\s*[0-9]+|[0-9]+\s*tr\.?|tr\s*[0-9]+|bv|nb|n\s?b|kv|t[0-9]+|[0-9]+\s*(?:vån|van)\.?|vån\s*[0-9]+"
    r"|uppg\.?\s*[a-z0-9]+|ing\.?\s*[a-z0-9]+|plan\s*[0-9]+|ii|iii|iv|[0-9]{4})$",
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


def display_line(
    *,
    care_of: str | None,
    box: str | None,
    street_name: str | None,
    house_number: str | None,
    unit: str | None,
    postal_code: str | None,
    city: str | None,
    street_display: str | None = None,
    care_of_display: str | None = None,
    city_display: str | None = None,
) -> str:
    """The one display line of an address from its stored components: `c/o Name`, then
    `Box N` or `Street 5B unit`, then `111 22 City`, comma-joined. `street_display`,
    `care_of_display` and `city_display` supply the delivered casing for those three parts
    when the caller has it (normalize_se_address computes all three before casefolding);
    otherwise each part is title-cased from the stored (already-folded) component, like
    every other part. The fold composes a merged address this way when the union of its
    members' components differs from the published member's own."""
    parts: list[str] = []
    if care_of:
        parts.append(f"c/o {care_of_display or _display(care_of)}")
    if box:
        parts.append(f"Box {box.upper()}")
    elif street_name:
        street = street_display or _display(street_name)
        if house_number:
            street += f" {house_number.upper()}"
        if unit:
            street += f" {unit}"
        parts.append(street)
    postal = " ".join(
        p
        for p in (
            f"{postal_code[:3]} {postal_code[3:]}" if postal_code else "",
            (city_display or _display(city)) if city else "",
        )
        if p
    )
    if postal:
        parts.append(postal)
    return ", ".join(parts)


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
    line = re.sub(r"\bn\s+b$", "nb", line)  # "nedre botten" written as two letters
    box = _BOX.match(line)
    if box:
        if box.group("rest"):
            notes.append(f"dropped trailing text '{box.group('rest').strip(' .,')}'")
        return re.sub(r"\s+", "", box.group("box")), None, None, None
    prefixed = _BOX_AFTER_PREFIX.match(line)
    if prefixed:
        notes.append(f"box after '{prefixed.group('prefix')}'")
        if prefixed.group("rest"):
            notes.append(f"dropped trailing text '{prefixed.group('rest').strip(' .,')}'")
        return re.sub(r"\s+", "", prefixed.group("box")), None, None, None
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
    if _BOX_AFTER_PREFIX.match(stripped) or _BOX.match(stripped):
        # A c/o line that resolves to a box (directly, or after a reference/name) is not a
        # care-of/street pair -- the box rules in _split_street take over from here.
        return None, stripped
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
    # `care_of_display`/`town_display` hold the delivered casing (computed once, from the
    # raw, unfolded fields) for `display_line`'s `care_of_display`/`city_display`
    # arguments. They are only ever RIGHT for `care_of` when the value came straight off
    # `raw.care_of`: the two branches below that instead extract `care_of` out of the
    # street text derive it from the already-folded `street_line`, so `care_of_display`
    # stays "" (falsy) for them and `display_line` falls back to title-casing the
    # extracted (folded) text on its own -- exactly what the pre-refactor code did too,
    # since it had no truer casing to offer in that case either.
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
    # A row with neither a street line nor a care-of used to return `no_address` here. It
    # falls through instead since v3, because a delivered postal part alone is now an
    # address (see the `has_location` block below); with no usable postcode it still ends
    # as `no_address` there, and with the same empty notes it carried when it exited here.

    if _CARE_OF_PREFIX.match(street_line) and not care_of:
        care_of, street_line = _split_care_of_street(street_line, notes)
        street_display_source = ""
    box, street_name, house_number, unit = _split_street(street_line, notes) if street_line else (None, None, None, None)
    prefix_note = next((n for n in notes if n.startswith("box after '")), None)
    if prefix_note and not care_of:
        care_of = _CARE_OF_PREFIX.sub("", prefix_note[len("box after '"):-1]).strip()

    if code and (len(code) != 5 or code in _INVALID_POSTCODES):
        notes.append(f"postcode '{raw.postal_code}' is not a valid five-digit code")
        code = ""
    if town in _UNKNOWN_TOWNS:
        town = ""

    has_location = bool(box or street_name)
    if not has_location:
        # v3 (2026-09-08): a valid postcode with a known town is a real address, coarse but
        # real -- the big-company postal codes (`SEB, STIFTELSER & FÖRETAG, 106 40
        # Stockholm`) that 27,786 companies deliver as their only address. The old chain
        # published them and this one does too, as a `partial` the geocoder resolves to the
        # postcode centroid. The care-of is kept (it is who receives the mail, and the
        # identity key is what separates two tenants of one postcode); every other component
        # stays NULL, which is also what stops the fold gluing the row onto a street
        # candidate -- `partial_compatible` requires a location line on both sides.
        # Anything else without a box or a street is `no_address`, as in v2.
        if code and town:
            notes.append("no street or box")
            return NormalizedAddress(
                care_of=care_of or None, box=None, street_name=None, house_number=None, unit=None,
                postal_code=code, city=town, country_code="SE",
                normalized_address=display_line(
                    care_of=care_of or None, box=None, street_name=None, house_number=None, unit=None,
                    postal_code=code, city=town,
                    care_of_display=care_of_display or None, city_display=town_display or None,
                ),
                parse_status="partial", parse_notes="; ".join(notes),
            )
        return NormalizedAddress(None, None, None, None, None, None, None, "SE", "", "no_address", "; ".join(notes))
    if code and town:
        status = "ok"
    else:
        status = "partial"
        if not code:
            notes.append("missing postcode")
        if not town:
            notes.append("missing city")

    if street_name and not box:
        mixed_case = street_display_source not in ("", street_display_source.upper(), street_display_source.lower())
        street_display = _display_kept(street_display_source, street_name) if mixed_case else _display(street_name)
    else:
        street_display = None
    display = display_line(
        care_of=care_of or None, box=box or None, street_name=street_name or None,
        house_number=house_number or None, unit=unit or None, postal_code=code or None,
        city=town or None, street_display=street_display,
        care_of_display=care_of_display or None, city_display=town_display or None,
    )

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


LOCATION_FIELDS: tuple[str, ...] = ("country_code", "postal_code", "city", "street_name", "box", "house_number", "unit")


def location_components(normalized: NormalizedAddress) -> tuple[str, ...]:
    """The identity without care-of: what the geocoder sees. One physical address is
    matched once whoever receives mail there (spec 3.7 as amended for slice 2a)."""
    return tuple(getattr(normalized, field_name) or "" for field_name in LOCATION_FIELDS)


def location_key(normalized: NormalizedAddress) -> str:
    return hashlib.sha256("\n".join(location_components(normalized)).encode("utf-8")).hexdigest()

