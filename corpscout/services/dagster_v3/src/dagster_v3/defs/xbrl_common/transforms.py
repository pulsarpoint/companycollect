"""iXBRL transformation registry (ixt v1-v4 numeric/date families).

Dispatch is on the format's local name, lowercased with hyphens stripped, so
v1 names (``numdotdecimal``) and v2-v4 names (``num-dot-decimal``) share
handlers. Unknown transforms raise ``UnknownTransform``; the extractor turns
that into a document warning plus a raw text value — it never guesses.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

XBRL_COMMON_PARSER_VERSION = "xbrl-common-1.0.0"

_SPACES = "    "
_STRIP_RE = re.compile(f"[{_SPACES}]")


@dataclass(frozen=True)
class TransformResult:
    kind: str  # numeric | date | text | boolean | empty
    value: str


class UnknownTransform(ValueError):
    pass


_MONTHS_EN = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12, "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}
_MONTHS_SV = {
    "januari": 1, "februari": 2, "mars": 3, "april": 4, "maj": 5, "juni": 6,
    "juli": 7, "augusti": 8, "september": 9, "oktober": 10, "november": 11,
    "december": 12,
}
_MONTHS_FI = {
    "tammikuuta": 1, "helmikuuta": 2, "maaliskuuta": 3, "huhtikuuta": 4,
    "toukokuuta": 5, "kesäkuuta": 6, "heinäkuuta": 7, "elokuuta": 8,
    "syyskuuta": 9, "lokakuuta": 10, "marraskuuta": 11, "joulukuuta": 12,
    "tammikuu": 1, "helmikuu": 2, "maaliskuu": 3, "huhtikuu": 4,
    "toukokuu": 5, "kesäkuu": 6, "heinäkuu": 7, "elokuu": 8,
    "syyskuu": 9, "lokakuu": 10, "marraskuu": 11, "joulukuu": 12,
}
_MONTH_NAMES: dict[str, int] = {**_MONTHS_EN, **_MONTHS_SV, **_MONTHS_FI}

_DATE_SPLIT_RE = re.compile(r"[.\-/\s]+")
_MONTHNAME_RE = re.compile(
    r"^\s*(\d{1,2})\.?\s+([^\s\d.]+)\.?\s+(\d{4})\s*$", re.UNICODE
)


def _numeric(raw: str, *, decimal_sep: str, thousand_seps: str) -> str:
    text = _STRIP_RE.sub("", raw.strip())
    for sep in thousand_seps:
        text = text.replace(sep, "")
    if decimal_sep != ".":
        text = text.replace(decimal_sep, ".")
    if not re.fullmatch(r"-?\d+(\.\d+)?", text):
        raise ValueError(f"not a decimal after transform: {raw!r}")
    return text


def _num_dot_decimal(raw: str) -> TransformResult:
    return TransformResult("numeric", _numeric(raw, decimal_sep=".", thousand_seps=","))


def _num_comma_decimal(raw: str) -> TransformResult:
    return TransformResult("numeric", _numeric(raw, decimal_sep=",", thousand_seps="."))


def _num_unit_decimal(raw: str) -> TransformResult:
    match = re.fullmatch(r"\s*([\d.,\s   ]+?)\D+?(\d+)\s*", raw)
    if match is None:
        raise ValueError(f"not a unit-decimal value: {raw!r}")
    integer = _STRIP_RE.sub("", match.group(1)).replace(".", "").replace(",", "")
    if not integer.isdigit():
        raise ValueError(f"not a unit-decimal value: {raw!r}")
    return TransformResult("numeric", f"{integer}.{match.group(2)}")


def _date_from_parts(day: str, month: str, year: str) -> TransformResult:
    day_i, month_i, year_i = int(day), int(month), int(year)
    if not (1 <= month_i <= 12 and 1 <= day_i <= 31):
        raise ValueError(f"invalid date parts: {day}/{month}/{year}")
    return TransformResult("date", f"{year_i:04d}-{month_i:02d}-{day_i:02d}")


def _date_dmy(raw: str) -> TransformResult:
    parts = _DATE_SPLIT_RE.split(raw.strip())
    if len(parts) != 3:
        raise ValueError(f"not a day-month-year date: {raw!r}")
    return _date_from_parts(parts[0], parts[1], parts[2])


def _date_ymd(raw: str) -> TransformResult:
    parts = _DATE_SPLIT_RE.split(raw.strip())
    if len(parts) != 3:
        raise ValueError(f"not a year-month-day date: {raw!r}")
    return _date_from_parts(parts[2], parts[1], parts[0])


def _date_mdy(raw: str) -> TransformResult:
    parts = _DATE_SPLIT_RE.split(raw.strip())
    if len(parts) != 3:
        raise ValueError(f"not a month-day-year date: {raw!r}")
    return _date_from_parts(parts[1], parts[0], parts[2])


def _date_month_year(raw: str) -> TransformResult:
    parts = _DATE_SPLIT_RE.split(raw.strip())
    if len(parts) != 2:
        raise ValueError(f"not a month-year date: {raw!r}")
    month_i, year_i = int(parts[0]), int(parts[1])
    if not 1 <= month_i <= 12:
        raise ValueError(f"invalid month: {raw!r}")
    # No day component exists, so this cannot become a full date - normalize
    # to YYYY-MM and keep kind=text.
    return TransformResult("text", f"{year_i:04d}-{month_i:02d}")


def _date_day_monthname_year(raw: str) -> TransformResult:
    match = _MONTHNAME_RE.match(raw)
    if match is None:
        raise ValueError(f"not a day-monthname-year date: {raw!r}")
    month = _MONTH_NAMES.get(match.group(2).lower().rstrip("."))
    if month is None:
        raise ValueError(f"unknown month name in: {raw!r}")
    return _date_from_parts(match.group(1), str(month), match.group(3))


_HANDLERS = {
    "numdotdecimal": _num_dot_decimal,
    "numcommadecimal": _num_comma_decimal,
    "numunitdecimal": _num_unit_decimal,
    "numcommadot": _num_dot_decimal,
    "zerodash": lambda raw: TransformResult("numeric", "0"),
    "numdash": lambda raw: TransformResult("numeric", "0"),
    "fixedzero": lambda raw: TransformResult("numeric", "0"),
    "fixedempty": lambda raw: TransformResult("empty", ""),
    "fixedfalse": lambda raw: TransformResult("boolean", "false"),
    "fixedtrue": lambda raw: TransformResult("boolean", "true"),
    "booleanfalse": lambda raw: TransformResult("boolean", "false"),
    "booleantrue": lambda raw: TransformResult("boolean", "true"),
    "datedaymonthyear": _date_dmy,
    "dateyearmonthday": _date_ymd,
    "datemonthdayyear": _date_mdy,
    "datemonthyear": _date_month_year,
    "dateslasheu": _date_dmy,
    "dateslashus": _date_mdy,
    "datedoteu": _date_dmy,
    "datedotus": _date_mdy,
}
# monthname variants share one handler across languages/suffixes
for _lang in ("", "en", "sv", "fi", "no", "da"):
    _HANDLERS[f"datedaymonthnameyear{_lang}"] = _date_day_monthname_year


def apply_transform(format_qname: str, raw_text: str) -> TransformResult:
    local = format_qname.rpartition(":")[2].lower().replace("-", "")
    handler = _HANDLERS.get(local)
    if handler is None:
        raise UnknownTransform(f"unsupported ixt transform: {format_qname}")
    return handler(raw_text)
