"""Quoted identity evidence from pages, before model interpretation of ownership."""

import re

from bs4 import BeautifulSoup

from crawler_service.company_search import swedish_company_id

IDENTITY_LABEL = re.compile(
    r"org(?:anisations?|anizations?)?[.\s-]*(?:nr|nummer|number)|"
    r"corporate\s+identity\s+number|company\s+(?:registration|number)|"
    r"momsreg(?:istrerings)?(?:nr|nummer)?|vat(?:\s*(?:id|number|no))?",
    re.I,
)
IDENTIFIER = re.compile(
    r"(?<![\w])(?:SE[\s\-\u2010-\u2015]*\d{6}[\s\-\u2010-\u2015]*\d{4}[\s\-\u2010-\u2015]*01|"
    r"\d{6}[\s\-\u2010-\u2015]*\d{4}|\d{8}[\s\-\u2010-\u2015]*\d{4})(?!\d)",
    re.I,
)


def company_page_text(html: str, *, include_navigation: bool = False) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for node in soup.select(
        "script, style, noscript, svg, "
        "#CybotCookiebotDialog, #CybotCookiebotDialogBodyUnderlay, "
        "#onetrust-consent-sdk, #onetrust-banner-sdk, #onetrust-pc-sdk, "
        "#coiOverlay, .coi-banner__wrapper, #CookieConsent"
    ):
        node.decompose()
    if not include_navigation:
        for navigation in soup.select("nav"):
            if navigation.find_parent("footer") is None:
                navigation.decompose()
    return soup.get_text(" ", strip=True)


def registration_evidence(sources: dict[str, str]) -> list[dict]:
    """Discover labelled numbers, without asserting they belong to the operator."""
    found = []
    for url, text in sources.items():
        for match in IDENTIFIER.finditer(text):
            before = text[max(0, match.start() - 100) : match.start()]
            if not match.group().upper().startswith("SE"):
                labels = list(IDENTITY_LABEL.finditer(before))
                if not labels:
                    continue
                between = before[labels[-1].end() :]
                if IDENTIFIER.search(between) or re.search(
                    r"\b(?:tel|phone|fax|gln|bankgiro|konto)\b", between, re.I
                ):
                    continue
            identifier = swedish_company_id(match.group())
            if identifier is None:
                continue
            found.append(
                {
                    "kind": "registration_number",
                    "value": match.group(),
                    "normalized_company_id": identifier,
                    "source_url": url,
                    "quote": text[max(0, match.start() - 180) : match.end() + 180],
                }
            )
    return found


def identity_excerpt(text: str, limit: int = 12000) -> str:
    """Preserve legal/contact evidence instead of spending context on navigation."""
    if len(text) <= limit:
        return text
    # Reserve both ends before adding middle-page evidence windows.
    pieces = [text[:1000], text[-2000:]]
    covered = [(0, 1000), (len(text) - 2000, len(text))]
    used = sum(map(len, pieces)) + 40
    for match in re.finditer(
        r"\b(?:ab|aktiebolag(?:et)?|head office|huvudkontor|organisationsnummer|"
        r"org[.\s]*nr|momsregnr|corporate identity|operator|operated by|personuppgiftsansvarig)\b|SE[- ]?\d{12}",
        text,
        re.I,
    ):
        if any(start <= match.start() < end for start, end in covered):
            continue
        start, end = max(0, match.start() - 250), min(len(text), match.end() + 500)
        snippet = text[start:end]
        if used + len(snippet) + 20 > limit:
            break
        pieces.append(snippet)
        covered.append((start, end))
        used += len(snippet) + 20
    return "\n[excerpt]\n".join(pieces)
