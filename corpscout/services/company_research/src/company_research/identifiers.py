"""Company identifiers and tracker patterns aligned with cc-enrich-worker/internal/extract.

Validation checks syntax/checksums, never registry membership or company ownership.
"""

import re

VAT_FORMATS = {
    "AT": r"U\d{8}",
    "BE": r"[01]\d{9}",
    "BG": r"\d{9,10}",
    "CY": r"\d{8}[A-Z]",
    "CZ": r"\d{8,10}",
    "DE": r"\d{9}",
    "DK": r"\d{8}",
    "EE": r"\d{9}",
    "EL": r"\d{9}",
    "ES": r"[A-Z0-9]\d{7}[A-Z0-9]",
    "FI": r"\d{8}",
    "FR": r"[A-Z0-9]{2}\d{9}",
    "GB": r"(?:\d{9}|\d{12}|(?:GD|HA)\d{3})",
    "HR": r"\d{11}",
    "HU": r"\d{8}",
    "IE": r"(?:\d{7}[A-W]|[7-9][A-Z*+]\d{5}[A-W]|\d{7}[A-W][AH])",
    "IT": r"\d{11}",
    "LT": r"(?:\d{9}|\d{12})",
    "LU": r"\d{8}",
    "LV": r"\d{11}",
    "MT": r"\d{8}",
    "NL": r"\d{9}B\d{2}",
    "PL": r"\d{10}",
    "PT": r"\d{9}",
    "RO": r"\d{2,10}",
    "SE": r"\d{12}",
    "SI": r"\d{8}",
    "SK": r"\d{10}",
}
VAT_TOKEN = re.compile(r"\b(?:" + "|".join(VAT_FORMATS) + r")[A-Z0-9]{2,13}\b")
LEI_TOKEN = re.compile(r"\b[A-Z0-9]{18}[0-9]{2}\b")
IDENTIFIER_PROPERTIES = {
    "leiCode": "lei",
    "vatID": "vat",
    "taxID": "tax",
    "duns": "duns",
    "naics": "naics",
    "isicV4": "isic",
    "globalLocationNumber": "gln",
}
TRACKER_PATTERNS = {
    "ga": r"\b(G-[A-Z0-9]{6,12})\b",
    "ua": r"\b(UA-\d{4,10}-\d{1,3})\b",
    "gtm": r"\b(GTM-[A-Z0-9]{4,8})\b",
    "adsense": r"\b(ca-pub-\d{10,20})\b",
    "fb_pixel": r"fbq\(\s*['\"]init['\"]\s*,\s*['\"](\d{10,20})['\"]",
    "hotjar": r"hjid\s*[:=]\s*(\d{5,9})",
    "linkedin_insight": r"_linkedin_partner_id\s*=\s*['\"](\d{4,10})['\"]",
    "yandex_metrika": r"ym\(\s*(\d{5,9})\s*,",
    "mixpanel": r"mixpanel\.init\(\s*['\"]([a-f0-9]{32})['\"]",
    "tiktok_pixel": r"ttq\.load\(\s*['\"]([A-Z0-9]{15,25})['\"]",
    "pinterest_tag": r"pintrk\(\s*['\"]load['\"]\s*,\s*['\"](\d{10,20})['\"]",
    "snap_pixel": r"snaptr\(\s*['\"]init['\"]\s*,\s*['\"]([0-9a-f-]{36})['\"]",
    "segment": r"analytics\.load\(\s*['\"]([A-Za-z0-9]{10,40})['\"]",
    "clarity": r"clarity\.ms/tag/([a-z0-9]{8,15})",
}


def identifier_validation(kind: str, value: str) -> dict:
    """Match the Common Crawl LEI and VAT validators, with explicit validation scope."""
    if kind == "lei":
        valid = (
            bool(LEI_TOKEN.fullmatch(value))
            and int("".join(str(ord(c) - 55) if c.isalpha() else c for c in value)) % 97
            == 1
        )
        return {"valid": valid, "validation": "checksum"}
    if kind != "vat":
        return {"valid": None, "validation": "not_checked"}
    country, number = value[:2], value[2:]
    scope = "checksum" if country in {"DE", "IT"} else "format"
    valid = (
        country in VAT_FORMATS
        and re.fullmatch(VAT_FORMATS[country], number, re.ASCII) is not None
    )
    if valid and country == "DE":
        product = 10
        for digit in number[:-1]:
            product = (((int(digit) + product) % 10 or 10) * 2) % 11
        valid = (11 - product) % 10 == int(number[-1])
    elif valid and country == "IT":
        digits = [int(c) for c in reversed(number)]
        valid = (
            sum(
                digit if index % 2 == 0 else digit * 2 - (9 if digit >= 5 else 0)
                for index, digit in enumerate(digits)
            )
            % 10
            == 0
        )
    return {"valid": valid, "validation": scope}
