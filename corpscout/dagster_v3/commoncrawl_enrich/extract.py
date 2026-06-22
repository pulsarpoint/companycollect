import re
from dataclasses import dataclass, field
from urllib.parse import urlparse

from lxml import html as lxml_html

from commoncrawl_enrich import ico as ico_mod
from commoncrawl_enrich.models import Email, FetchedPage, Phone, Social

_EMAIL = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_EMAIL_NOISE_DOMAINS = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg")
_EMAIL_NOISE_HINTS = ("example.", "sentry.", "wixpress.", "@2x", "@3x")
_ROLE_LOCALS = {"info", "office", "kontakt", "kontakty", "sales", "obchod", "podpora",
                "support", "predaj", "sekretariat", "hello", "ahoj"}
# Slovak/Czech (+421/+420) and generic international numbers.
_PHONE = re.compile(r"(?:\+|00)\s?(?:420|421)(?:[\s\- ]?\d){8,10}")
_SOCIAL_HOSTS = {
    "facebook.com": "facebook", "linkedin.com": "linkedin", "instagram.com": "instagram",
    "twitter.com": "x", "x.com": "x", "youtube.com": "youtube", "tiktok.com": "tiktok",
}


@dataclass
class ParsedHtml:
    title: str = ""
    meta_description: str = ""
    content_language: str = ""
    text: str = ""
    links: list[str] = field(default_factory=list)


@dataclass
class DeterministicResult:
    title: str = ""
    meta_description: str = ""
    content_language: str = ""
    ico: str = ""
    dic: str = ""
    ico_checksum_valid: bool = False
    emails: list[Email] = field(default_factory=list)
    phones: list[Phone] = field(default_factory=list)
    socials: list[Social] = field(default_factory=list)
    technologies: list = field(default_factory=list)  # filled by enrich (tech.py)


def parse_html(raw: str) -> ParsedHtml:
    if not raw or not raw.strip():
        return ParsedHtml()
    try:
        doc = lxml_html.fromstring(raw)
    except Exception:  # noqa: BLE001 - malformed HTML -> empty parse
        return ParsedHtml()
    title = (doc.findtext(".//title") or "").strip()
    meta = ""
    for content in doc.xpath('//meta[translate(@name,"DESCRIPTION","description")="description"]/@content'):
        meta = content.strip()
        break
    lang = (doc.xpath("string(/html/@lang)") or "").strip()[:5]
    links = [str(h) for h in doc.xpath("//a/@href")]
    text = " ".join(doc.text_content().split())
    return ParsedHtml(title=title, meta_description=meta, content_language=lang, text=text, links=links)


def _is_noise_email(addr: str) -> bool:
    low = addr.lower()
    return low.endswith(_EMAIL_NOISE_DOMAINS) or any(h in low for h in _EMAIL_NOISE_HINTS)


def extract_emails(text: str) -> list[Email]:
    seen: dict[str, Email] = {}
    for raw in _EMAIL.findall(text):
        addr = raw.strip(".")
        if _is_noise_email(addr) or addr.lower() in seen:
            continue
        local = addr.split("@", 1)[0].lower()
        seen[addr.lower()] = Email(email=addr, is_role=local in _ROLE_LOCALS, source_method="regex")
    return list(seen.values())


def _to_e164(raw: str) -> str:
    digits = re.sub(r"[^\d+]", "", raw)
    if digits.startswith("00"):
        digits = "+" + digits[2:]
    elif not digits.startswith("+"):
        digits = "+" + digits
    return digits


def extract_phones(text: str) -> list[Phone]:
    seen: dict[str, Phone] = {}
    for raw in _PHONE.findall(text):
        e164 = _to_e164(raw)
        if e164 not in seen:
            seen[e164] = Phone(phone_raw=raw.strip(), phone_e164=e164, source_method="regex")
    return list(seen.values())


def extract_socials(links: list[str]) -> list[Social]:
    seen: dict[tuple[str, str], Social] = {}
    for href in links:
        try:  # real-world hrefs include garbage that urlparse rejects (invalid NFKC, etc.)
            parts = urlparse(href)
        except ValueError:
            continue
        host = (parts.hostname or "").lower().removeprefix("www.")
        platform = _SOCIAL_HOSTS.get(host)
        if not platform:
            continue
        handle = parts.path.strip("/").split("/")[-1]
        key = (platform, href)
        if key not in seen:
            seen[key] = Social(platform=platform, url=href, handle=handle)
    return list(seen.values())


def extract_deterministic(page: FetchedPage) -> DeterministicResult:
    parsed = parse_html(page.html)
    icos = ico_mod.extract_icos(parsed.text)
    primary_ico = icos[0] if icos else ""
    return DeterministicResult(
        title=parsed.title,
        meta_description=parsed.meta_description,
        content_language=parsed.content_language or "",
        ico=primary_ico,
        dic=ico_mod.extract_dic(parsed.text),
        ico_checksum_valid=bool(primary_ico),  # extract_icos only returns checksum-valid
        # Emails/phones from RAW html (regex ignores tags) for higher recall; IČO from
        # clean text (labels survive tag-stripping). socials from parsed <a> hrefs.
        emails=extract_emails(page.html),
        phones=extract_phones(page.html),
        socials=extract_socials(parsed.links),
    )
