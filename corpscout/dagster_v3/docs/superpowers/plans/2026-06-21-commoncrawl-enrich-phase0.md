# CommonCrawl Enrichment — Phase 0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone, single-process `commoncrawl_enrich` package that takes a ~10k-domain
manifest, fetches each homepage from CommonCrawl (index lookup → byte-range WARC fetch), extracts
contacts / IČO / technologies / metadata deterministically **and** runs a thinking-mode LLM arm for
industry + contact-recall, then writes 5 Parquet tables plus a hit-rate/uplift/**speed** report.

**Architecture:** Pure Python package at `src/dagster_v3/commoncrawl_enrich/` (outside `defs/`, so
Dagster's loader ignores it). I/O (CommonCrawl index, WARC fetch, LLM) is behind small injectable
interfaces so logic is unit-tested with fakes; a `run.py` CLI wires the real implementations. No
Dagster, no Temporal, no ClickHouse in this phase — output is Parquet + a metrics JSON.

**Tech Stack:** Python 3.14, `duckdb` (columnar-index query + manifest read), `requests` (range
fetch + CDX), `warcio` (WARC record parse), `lxml` (HTML parse), `openai` (OpenAI-compatible LLM,
thinking mode), `pyarrow` (Parquet), `concurrent.futures` (thread-pool concurrency). All present
except `warcio` (added in Task 0).

**Spec:** `docs/superpowers/specs/2026-06-21-commoncrawl-domain-enrichment-design.md`.
**Gating dependency (runtime only):** a domain manifest exported from `open_page_rank` (Parquet/CSV
with columns `root_domain, source_rank, open_page_rank`). The package and its tests do **not** need
`open_page_rank` loaded — only the final live `run.py` does.

**Working directory for all commands:** `corpscout/dagster_v3/`. All `pytest`/`python` run via `uv run`.

---

## File structure

Package — `corpscout/dagster_v3/src/dagster_v3/commoncrawl_enrich/`:
- `__init__.py` — package marker + version.
- `models.py` — frozen dataclasses shared by every module (the vocabulary).
- `ico.py` — IČO mod-11 checksum + IČO/DIČ extraction.
- `extract.py` — HTML parse (lxml) + email/phone/social extraction + `extract_deterministic`.
- `tech.py` — built-in fingerprint technology detector.
- `llm.py` — provider-agnostic OpenAI-compatible LLM arm (thinking mode): industry + contact recall.
- `index_client.py` — resolve `domain → IndexRecord` via DuckDB over the CC columnar index (+ CDX fallback).
- `warc.py` — byte-range fetch + `warcio` parse → `FetchedPage`.
- `enrich.py` — orchestrate index→fetch→deterministic→tech→LLM per domain (thread pool); injectable deps.
- `parquet_out.py` — write the 5 Parquet tables from enrichment rows (pyarrow).
- `metrics.py` — build the hit-rate / uplift / speed report dict.
- `run.py` — CLI: read manifest, wire real deps, run, write Parquet + `metrics.json`, print report.
- `README.md` — usage.

Tests — `corpscout/dagster_v3/tests/`: one `test_commoncrawl_enrich_<module>.py` per task.

---

## Task 0: Dependency + package skeleton

**Files:**
- Modify: `pyproject.toml` (add `warcio`)
- Create: `src/dagster_v3/commoncrawl_enrich/__init__.py`
- Test: `tests/test_commoncrawl_enrich_skeleton.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_commoncrawl_enrich_skeleton.py
def test_package_imports_and_warcio_available():
    import warcio  # noqa: F401  - dependency must be installed
    from dagster_v3 import commoncrawl_enrich

    assert commoncrawl_enrich.__version__ == "0.1.0"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_commoncrawl_enrich_skeleton.py -q`
Expected: FAIL (`ModuleNotFoundError: No module named 'warcio'` and/or the package).

- [ ] **Step 3: Add the dependency and package**

Run: `uv add warcio`

```python
# src/dagster_v3/commoncrawl_enrich/__init__.py
"""Standalone CommonCrawl domain-enrichment package (Phase 0 spike).

Pure Python; no Dagster/Temporal/ClickHouse. Lives outside defs/ so Dagster's
defs-loader does not treat it as assets.
"""

__version__ = "0.1.0"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_commoncrawl_enrich_skeleton.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock src/dagster_v3/commoncrawl_enrich/__init__.py tests/test_commoncrawl_enrich_skeleton.py
git commit -m "feat(commoncrawl_enrich): package skeleton + warcio dependency"
```

---

## Task 1: Data models

**Files:**
- Create: `src/dagster_v3/commoncrawl_enrich/models.py`
- Test: `tests/test_commoncrawl_enrich_models.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_commoncrawl_enrich_models.py
from dagster_v3.commoncrawl_enrich import models as m


def test_domain_enrichment_defaults_and_construction():
    target = m.DomainTarget(root_domain="example.sk", source_rank=5, open_page_rank=7.5)
    enr = m.DomainEnrichment(target=target, fetch_status="ok")
    assert enr.emails == [] and enr.technologies == []
    assert enr.ico == "" and enr.ico_checksum_valid is False
    assert enr.industry is None

    enr.emails.append(m.Email(email="info@example.sk", is_role=True, source_method="regex"))
    assert enr.emails[0].source_method == "regex"

    ind = m.IndustryGuess(label="Accounting", nace_hint="69.20", confidence=80, method="llm")
    assert ind.nace_hint == "69.20"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_commoncrawl_enrich_models.py -q`
Expected: FAIL (`ModuleNotFoundError` / attributes missing).

- [ ] **Step 3: Write the implementation**

```python
# src/dagster_v3/commoncrawl_enrich/models.py
from dataclasses import dataclass, field


@dataclass(frozen=True)
class DomainTarget:
    root_domain: str
    source_rank: int
    open_page_rank: float


@dataclass(frozen=True)
class IndexRecord:
    root_domain: str
    warc_filename: str
    offset: int
    length: int
    url: str
    http_status: int
    crawl_id: str


@dataclass(frozen=True)
class FetchedPage:
    root_domain: str
    final_url: str
    http_status: int
    headers: dict[str, str]
    html: str
    capture_date: str  # ISO date, or "" if unknown
    crawl_id: str


@dataclass(frozen=True)
class Email:
    email: str
    is_role: bool
    source_method: str  # "regex" | "llm"


@dataclass(frozen=True)
class Phone:
    phone_raw: str
    phone_e164: str
    source_method: str  # "regex" | "llm"


@dataclass(frozen=True)
class Social:
    platform: str
    url: str
    handle: str


@dataclass(frozen=True)
class Technology:
    technology: str
    category: str
    version: str
    confidence: int


@dataclass(frozen=True)
class IndustryGuess:
    label: str
    nace_hint: str
    confidence: int
    method: str  # "llm" | "none"


@dataclass
class DomainEnrichment:
    target: DomainTarget
    fetch_status: str  # "ok" | "not_in_index" | "fetch_failed" | "non_html"
    page: FetchedPage | None = None
    title: str = ""
    meta_description: str = ""
    content_language: str = ""
    ico: str = ""
    dic: str = ""
    ico_checksum_valid: bool = False
    industry: IndustryGuess | None = None
    emails: list[Email] = field(default_factory=list)
    phones: list[Phone] = field(default_factory=list)
    socials: list[Social] = field(default_factory=list)
    technologies: list[Technology] = field(default_factory=list)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_commoncrawl_enrich_models.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/commoncrawl_enrich/models.py tests/test_commoncrawl_enrich_models.py
git commit -m "feat(commoncrawl_enrich): shared data models"
```

---

## Task 2: IČO checksum + IČO/DIČ extraction

**Files:**
- Create: `src/dagster_v3/commoncrawl_enrich/ico.py`
- Test: `tests/test_commoncrawl_enrich_ico.py`

The Czech/Slovak IČO is 8 digits with a weighted mod-11 check digit: weights `8,7,6,5,4,3,2` over
the first 7 digits; `r = sum % 11`; check digit = `1` if `r==0`, `0` if `r==1`, else `11-r`; valid
iff it equals the 8th digit.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_commoncrawl_enrich_ico.py
from dagster_v3.commoncrawl_enrich import ico


def test_checksum_valid_known_icos():
    assert ico.ico_checksum_valid("31333532") is True   # ESET
    assert ico.ico_checksum_valid("45503249") is True    # Martinus
    assert ico.ico_checksum_valid("12345678") is False
    assert ico.ico_checksum_valid("3133353") is False    # too short
    assert ico.ico_checksum_valid("abcdefgh") is False


def test_extract_icos_anchored_and_spaced():
    text = "Kontakt — IČO: 31 333 532, DIČ: 2020317068. Iné číslo 99999999."
    assert ico.extract_icos(text) == ["31333532"]  # only checksum-valid, near a label


def test_extract_dic():
    assert ico.extract_dic("DIČ: 2020317068") == "2020317068"
    assert ico.extract_dic("no tax id here") == ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_commoncrawl_enrich_ico.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the implementation**

```python
# src/dagster_v3/commoncrawl_enrich/ico.py
import re

_ICO_LABEL = re.compile(r"(?:IČO|ICO|IČ|IC)\s*[:. ]*\s*([0-9][0-9  ]{6,14}[0-9])", re.IGNORECASE)
_DIC_LABEL = re.compile(r"(?:DIČ|DIC)\s*[:. ]*\s*([0-9]{9,12})", re.IGNORECASE)


def ico_checksum_valid(ico_digits: str) -> bool:
    digits = re.sub(r"\D", "", ico_digits)
    if len(digits) != 8:
        return False
    weights = (8, 7, 6, 5, 4, 3, 2)
    total = sum(int(digits[i]) * weights[i] for i in range(7))
    remainder = total % 11
    if remainder == 0:
        check = 1
    elif remainder == 1:
        check = 0
    else:
        check = 11 - remainder
    return check == int(digits[7])


def extract_icos(text: str) -> list[str]:
    """Return distinct checksum-valid 8-digit IČOs that appear next to an IČO label."""
    found: list[str] = []
    for match in _ICO_LABEL.finditer(text):
        digits = re.sub(r"\D", "", match.group(1))
        if len(digits) >= 8:
            candidate = digits[:8]
            if ico_checksum_valid(candidate) and candidate not in found:
                found.append(candidate)
    return found


def extract_dic(text: str) -> str:
    match = _DIC_LABEL.search(text)
    return match.group(1) if match else ""
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_commoncrawl_enrich_ico.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/commoncrawl_enrich/ico.py tests/test_commoncrawl_enrich_ico.py
git commit -m "feat(commoncrawl_enrich): IČO mod-11 checksum + IČO/DIČ extraction"
```

---

## Task 3: HTML parse + email/phone/social extraction

**Files:**
- Create: `src/dagster_v3/commoncrawl_enrich/extract.py`
- Test: `tests/test_commoncrawl_enrich_extract.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_commoncrawl_enrich_extract.py
from dagster_v3.commoncrawl_enrich import extract
from dagster_v3.commoncrawl_enrich.models import FetchedPage

HTML = """
<html lang="sk"><head><title>Firma s.r.o.</title>
<meta name="description" content="Účtovníctvo a dane"></head>
<body>
  Kontakt: info@firma.sk, Ján <jan.novak@firma.sk>. Tel: +421 905 123 456.
  IČO: 31 333 532 DIČ: 2020317068
  <a href="https://www.facebook.com/firmask">FB</a>
  <a href="https://www.linkedin.com/company/firma">LI</a>
  <img src="logo@2x.png">
</body></html>
"""


def _page() -> FetchedPage:
    return FetchedPage(root_domain="firma.sk", final_url="https://firma.sk/", http_status=200,
                       headers={}, html=HTML, capture_date="2025-05-01", crawl_id="CC-MAIN-2025-21")


def test_parse_html_title_meta_lang():
    parsed = extract.parse_html(HTML)
    assert parsed.title == "Firma s.r.o."
    assert parsed.meta_description == "Účtovníctvo a dane"
    assert parsed.content_language == "sk"
    assert "facebook.com/firmask" in " ".join(parsed.links)


def test_extract_emails_filters_noise_and_flags_role():
    emails = extract.extract_emails(HTML)
    addrs = {e.email for e in emails}
    assert "info@firma.sk" in addrs and "jan.novak@firma.sk" in addrs
    assert "logo@2x.png" not in addrs
    assert next(e for e in emails if e.email == "info@firma.sk").is_role is True


def test_extract_phones_and_socials():
    phones = extract.extract_phones(HTML)
    assert any(p.phone_e164 == "+421905123456" for p in phones)
    socials = extract.extract_socials(extract.parse_html(HTML).links)
    platforms = {s.platform for s in socials}
    assert platforms == {"facebook", "linkedin"}


def test_extract_deterministic_bundles_everything():
    result = extract.extract_deterministic(_page())
    assert result.title == "Firma s.r.o." and result.ico == "31333532"
    assert result.ico_checksum_valid is True and result.dic == "2020317068"
    assert result.emails and result.technologies == []  # tech added by enrich, not here
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_commoncrawl_enrich_extract.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the implementation**

```python
# src/dagster_v3/commoncrawl_enrich/extract.py
import re
from dataclasses import dataclass, field
from urllib.parse import urlparse

from lxml import html as lxml_html

from dagster_v3.commoncrawl_enrich import ico as ico_mod
from dagster_v3.commoncrawl_enrich.models import Email, FetchedPage, Phone, Social

_EMAIL = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_EMAIL_NOISE_DOMAINS = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg")
_EMAIL_NOISE_HINTS = ("example.", "sentry.", "wixpress.", "@2x", "@3x")
_ROLE_LOCALS = {"info", "office", "kontakt", "kontakty", "sales", "obchod", "podpora",
                "support", "predaj", "sekretariat", "hello", "ahoj"}
# Slovak/Czech (+421/+420) and generic international numbers.
_PHONE = re.compile(r"(?:\+|00)\s?(?:420|421)(?:[\s\- ]?\d){8,10}")
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
        host = (urlparse(href).hostname or "").lower().removeprefix("www.")
        platform = _SOCIAL_HOSTS.get(host)
        if not platform:
            continue
        handle = urlparse(href).path.strip("/").split("/")[-1]
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_commoncrawl_enrich_extract.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/commoncrawl_enrich/extract.py tests/test_commoncrawl_enrich_extract.py
git commit -m "feat(commoncrawl_enrich): HTML parse + email/phone/social/IČO extraction"
```

---

## Task 4: Technology fingerprint detector

**Files:**
- Create: `src/dagster_v3/commoncrawl_enrich/tech.py`
- Test: `tests/test_commoncrawl_enrich_tech.py`

A small built-in fingerprint set (substring in HTML or HTTP header). Behind one function so full
Wappalyzer can replace it later without changing callers.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_commoncrawl_enrich_tech.py
from dagster_v3.commoncrawl_enrich import tech


def test_detects_html_and_header_fingerprints():
    html = '<link href="/wp-content/themes/x/style.css"><script src="gtag.js"></script>'
    headers = {"server": "nginx/1.25", "x-powered-by": "PHP/8.2"}
    names = {t.technology for t in tech.detect_technologies(html, headers)}
    assert {"WordPress", "Google Analytics", "Nginx", "PHP"} <= names


def test_no_false_positive_on_blank():
    assert tech.detect_technologies("", {}) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_commoncrawl_enrich_tech.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the implementation**

```python
# src/dagster_v3/commoncrawl_enrich/tech.py
from dagster_v3.commoncrawl_enrich.models import Technology

# (technology, category, html-substrings, (header, header-substring))
_HTML_FINGERPRINTS: list[tuple[str, str, tuple[str, ...]]] = [
    ("WordPress", "CMS", ("wp-content", "wp-json", "wp-includes")),
    ("WooCommerce", "Ecommerce", ("woocommerce",)),
    ("Shopify", "Ecommerce", ("cdn.shopify.com", "shopify")),
    ("Wix", "Website builder", ("wixstatic.com", "wix.com")),
    ("Squarespace", "Website builder", ("squarespace.com",)),
    ("Next.js", "Web framework", ("/_next/",)),
    ("React", "JavaScript framework", ("data-reactroot", "__react")),
    ("jQuery", "JavaScript library", ("jquery",)),
    ("Bootstrap", "UI framework", ("bootstrap.min.css", "bootstrap.css")),
    ("Google Analytics", "Analytics", ("google-analytics.com", "gtag.js", "gtag(")),
    ("Google Tag Manager", "Tag manager", ("googletagmanager.com",)),
    ("Facebook Pixel", "Analytics", ("connect.facebook.net",)),
    ("HubSpot", "Marketing", ("hs-scripts.com",)),
]
# (technology, category, header, substring)
_HEADER_FINGERPRINTS: list[tuple[str, str, str, str]] = [
    ("Nginx", "Web server", "server", "nginx"),
    ("Apache", "Web server", "server", "apache"),
    ("Cloudflare", "CDN", "server", "cloudflare"),
    ("PHP", "Programming language", "x-powered-by", "php"),
    ("ASP.NET", "Web framework", "x-powered-by", "asp.net"),
]


def detect_technologies(html: str, headers: dict[str, str]) -> list[Technology]:
    low_html = (html or "").lower()
    low_headers = {k.lower(): str(v).lower() for k, v in (headers or {}).items()}
    out: list[Technology] = []
    seen: set[str] = set()
    for name, category, needles in _HTML_FINGERPRINTS:
        if name not in seen and any(n in low_html for n in needles):
            out.append(Technology(technology=name, category=category, version="", confidence=100))
            seen.add(name)
    for name, category, header, needle in _HEADER_FINGERPRINTS:
        if name not in seen and needle in low_headers.get(header, ""):
            out.append(Technology(technology=name, category=category, version="", confidence=100))
            seen.add(name)
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_commoncrawl_enrich_tech.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/commoncrawl_enrich/tech.py tests/test_commoncrawl_enrich_tech.py
git commit -m "feat(commoncrawl_enrich): built-in technology fingerprint detector"
```

---

## Task 5: LLM arm (thinking mode) — industry + contact recall

**Files:**
- Create: `src/dagster_v3/commoncrawl_enrich/llm.py`
- Test: `tests/test_commoncrawl_enrich_llm.py`

Provider-agnostic: `LLMArm` takes a `chat` callable `(system, user) -> str` so tests inject a fake;
`from_openai(...)` builds the real one using `openai.OpenAI` in **thinking mode** (no
`enable_thinking:False`). Response JSON is recovered with a `<think>`-stripping parser.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_commoncrawl_enrich_llm.py
from dagster_v3.commoncrawl_enrich import llm


def fake_chat(system: str, user: str) -> str:
    if "industry" in system.lower():
        return '<think>looks like an accountant</think>{"label":"Accounting","nace_hint":"69.20","confidence":85}'
    return '{"emails":["skryta@firma.sk"],"phones":["+421911000000"]}'


def test_classify_industry_parses_thinking_response():
    arm = llm.LLMArm(chat=fake_chat)
    guess = arm.classify_industry("We do bookkeeping and tax for SMEs")
    assert guess.label == "Accounting" and guess.nace_hint == "69.20"
    assert guess.confidence == 85 and guess.method == "llm"


def test_recover_contacts_tags_source_method_llm():
    arm = llm.LLMArm(chat=fake_chat)
    emails, phones = arm.recover_contacts("contact text")
    assert emails[0].email == "skryta@firma.sk" and emails[0].source_method == "llm"
    assert phones[0].phone_e164 == "+421911000000" and phones[0].source_method == "llm"


def test_industry_returns_none_method_on_bad_json():
    arm = llm.LLMArm(chat=lambda s, u: "no json here")
    guess = arm.classify_industry("text")
    assert guess.method == "none" and guess.label == ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_commoncrawl_enrich_llm.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the implementation**

```python
# src/dagster_v3/commoncrawl_enrich/llm.py
import json
import re
from collections.abc import Callable

from dagster_v3.commoncrawl_enrich.extract import _to_e164
from dagster_v3.commoncrawl_enrich.models import Email, IndustryGuess, Phone

ChatFn = Callable[[str, str], str]

_INDUSTRY_SYSTEM = (
    "You classify a company by its website text into one industry. "
    "Return ONLY JSON: {\"label\": str, \"nace_hint\": str (EU NACE rev2 code like '69.20' "
    "or section letter, '' if unknown), \"confidence\": int 0-100}."
)
_CONTACT_SYSTEM = (
    "Extract contact details from the website text, including obfuscated ones "
    "(e.g. 'info [at] x [dot] sk'). Return ONLY JSON: "
    "{\"emails\": [str], \"phones\": [str]}. Empty arrays if none."
)
_MAX_CHARS = 8000  # ~2-4k tokens; bound LLM cost


def _parse_json_object(content: str) -> dict | None:
    text = re.sub(r"<think>.*?</think>", "", content, flags=re.S).strip()
    text = re.sub(r"```(?:json)?", "", text).strip()
    match = re.search(r"\{.*\}", text, re.S)
    if match is None:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


class LLMArm:
    def __init__(self, chat: ChatFn):
        self._chat = chat

    def classify_industry(self, text: str) -> IndustryGuess:
        raw = self._chat(_INDUSTRY_SYSTEM, text[:_MAX_CHARS])
        data = _parse_json_object(raw)
        if not data or not data.get("label"):
            return IndustryGuess(label="", nace_hint="", confidence=0, method="none")
        try:
            confidence = int(data.get("confidence", 0))
        except (TypeError, ValueError):
            confidence = 0
        return IndustryGuess(
            label=str(data.get("label", "")),
            nace_hint=str(data.get("nace_hint", "")),
            confidence=max(0, min(100, confidence)),
            method="llm",
        )

    def recover_contacts(self, text: str) -> tuple[list[Email], list[Phone]]:
        data = _parse_json_object(self._chat(_CONTACT_SYSTEM, text[:_MAX_CHARS])) or {}
        emails = [
            Email(email=str(e), is_role=False, source_method="llm")
            for e in (data.get("emails") or [])
            if "@" in str(e)
        ]
        phones = [
            Phone(phone_raw=str(p), phone_e164=_to_e164(str(p)), source_method="llm")
            for p in (data.get("phones") or [])
            if any(ch.isdigit() for ch in str(p))
        ]
        return emails, phones


def from_openai(*, base_url: str, model: str, api_key: str,
                timeout_seconds: int = 180, enable_thinking: bool = True) -> LLMArm:
    """Build an LLMArm backed by an OpenAI-compatible endpoint (thinking mode by default)."""
    from openai import OpenAI

    client = OpenAI(base_url=base_url, api_key=api_key, timeout=timeout_seconds)
    # Suppress the thinking trace ONLY if explicitly disabled (qwen extra body).
    extra_body = {} if enable_thinking else {"chat_template_kwargs": {"enable_thinking": False}}

    def chat(system: str, user: str) -> str:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0,
            extra_body=extra_body,
        )
        return response.choices[0].message.content or ""

    return LLMArm(chat=chat)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_commoncrawl_enrich_llm.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/commoncrawl_enrich/llm.py tests/test_commoncrawl_enrich_llm.py
git commit -m "feat(commoncrawl_enrich): provider-agnostic LLM arm (thinking mode)"
```

---

## Task 6: CommonCrawl index client

**Files:**
- Create: `src/dagster_v3/commoncrawl_enrich/index_client.py`
- Test: `tests/test_commoncrawl_enrich_index.py`

`select_best_record(rows)` (pure: pick latest 200/HTML homepage per domain) is unit-tested; the live
DuckDB query and CDX HTTP call are thin wrappers around it.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_commoncrawl_enrich_index.py
from dagster_v3.commoncrawl_enrich import index_client
from dagster_v3.commoncrawl_enrich.models import IndexRecord


def test_select_best_record_prefers_latest_200_html_homepage():
    rows = [
        # (host, path, status, mime, timestamp, filename, offset, length, url)
        ("firma.sk", "/", "200", "text/html", "20240101000000", "f1.warc.gz", 10, 100, "https://firma.sk/"),
        ("firma.sk", "/", "200", "text/html", "20250501000000", "f2.warc.gz", 20, 200, "https://firma.sk/"),
        ("firma.sk", "/kontakt", "200", "text/html", "20250601000000", "f3.warc.gz", 0, 50, "https://firma.sk/kontakt"),
        ("firma.sk", "/", "301", "text/html", "20250701000000", "f4.warc.gz", 0, 50, "https://firma.sk/"),
    ]
    rec = index_client.select_best_record("firma.sk", rows, crawl_id="CC-MAIN-2025-21")
    assert isinstance(rec, IndexRecord)
    assert rec.warc_filename == "f2.warc.gz" and rec.offset == 20 and rec.length == 200


def test_select_best_record_none_when_no_usable_capture():
    rows = [("x.sk", "/", "404", "text/html", "20250101000000", "f.warc.gz", 0, 1, "https://x.sk/")]
    assert index_client.select_best_record("x.sk", rows, crawl_id="CC-MAIN-2025-21") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_commoncrawl_enrich_index.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the implementation**

```python
# src/dagster_v3/commoncrawl_enrich/index_client.py
import logging

import requests

from dagster_v3.commoncrawl_enrich.models import IndexRecord

LOGGER = logging.getLogger(__name__)

DEFAULT_CRAWL_ID = "CC-MAIN-2025-21"
COLUMNAR_INDEX = "s3://commoncrawl/cc-index/table/cc-main/warc/crawl={crawl}/subset=warc"
CDX_URL = "https://index.commoncrawl.org/{crawl}-index"
USER_AGENT = "corpscout-commoncrawl-enrich/0.1 (goran.raovic@gmail.com)"

# Row shape: (host, path, status, mime, timestamp, filename, offset, length, url)
IndexRow = tuple


def select_best_record(domain: str, rows: list[IndexRow], *, crawl_id: str) -> IndexRecord | None:
    """Pick the latest HTTP-200 HTML homepage ('/') capture for the domain."""
    candidates = [
        r for r in rows
        if str(r[2]) == "200" and "html" in str(r[3]).lower() and str(r[1]) in ("/", "")
    ]
    if not candidates:
        return None
    best = max(candidates, key=lambda r: str(r[4]))  # newest timestamp wins
    return IndexRecord(
        root_domain=domain, warc_filename=str(best[5]), offset=int(best[6]),
        length=int(best[7]), url=str(best[8]), http_status=200, crawl_id=crawl_id,
    )


def resolve_via_duckdb(domains: list[str], *, crawl_id: str, duckdb_con) -> dict[str, IndexRecord]:
    """Resolve homepage records for a batch of domains via the CC columnar Parquet index.

    `duckdb_con` is a duckdb connection with httpfs installed + anonymous S3 access.
    """
    path = COLUMNAR_INDEX.format(crawl=crawl_id)
    placeholders = ", ".join("?" for _ in domains)
    rows = duckdb_con.execute(
        f"""
        select url_host_name, url_path, fetch_status, content_mime_type,
               fetch_time, warc_filename, warc_record_offset, warc_record_length, url
        from read_parquet('{path}/*.parquet', hive_partitioning=1)
        where url_host_name in ({placeholders}) and url_path in ('/', '')
        """,
        domains,
    ).fetchall()
    grouped: dict[str, list[IndexRow]] = {}
    for row in rows:
        host = str(row[0]).removeprefix("www.")
        grouped.setdefault(host, []).append(row)
    out: dict[str, IndexRecord] = {}
    for domain in domains:
        rec = select_best_record(domain, grouped.get(domain, []), crawl_id=crawl_id)
        if rec is not None:
            out[domain] = rec
    return out


def resolve_via_cdx(domain: str, *, crawl_id: str, session: requests.Session | None = None) -> IndexRecord | None:
    """Per-domain fallback using the public CDX API (no AWS)."""
    http = session or requests.Session()
    response = http.get(
        CDX_URL.format(crawl=crawl_id),
        params={"url": f"{domain}/", "output": "json", "filter": "status:200", "limit": 20},
        headers={"User-Agent": USER_AGENT}, timeout=60,
    )
    if response.status_code != 200 or not response.text.strip():
        return None
    rows: list[IndexRow] = []
    for line in response.text.splitlines():
        import json
        rec = json.loads(line)
        host = (rec.get("url", "").split("/")[2] if "://" in rec.get("url", "") else domain).removeprefix("www.")
        rows.append((host, "/", rec.get("status", ""), rec.get("mime", ""), rec.get("timestamp", ""),
                     rec.get("filename", ""), rec.get("offset", 0), rec.get("length", 0), rec.get("url", "")))
    return select_best_record(domain, rows, crawl_id=crawl_id)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_commoncrawl_enrich_index.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/commoncrawl_enrich/index_client.py tests/test_commoncrawl_enrich_index.py
git commit -m "feat(commoncrawl_enrich): CC index client (columnar DuckDB + CDX fallback)"
```

---

## Task 7: WARC byte-range fetch + parse

**Files:**
- Create: `src/dagster_v3/commoncrawl_enrich/warc.py`
- Test: `tests/test_commoncrawl_enrich_warc.py`

`parse_warc_record(raw_gzip_bytes, ...)` (pure parse via `warcio`) is unit-tested with a tiny
in-memory WARC record; `fetch_page(record, session)` adds the HTTP Range GET.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_commoncrawl_enrich_warc.py
import gzip
import io

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from dagster_v3.commoncrawl_enrich import warc
from dagster_v3.commoncrawl_enrich.models import IndexRecord


def _make_warc_record_bytes() -> bytes:
    buf = io.BytesIO()
    writer = WARCWriter(buf, gzip=True)
    payload = b"<html lang='sk'><title>Firma</title></html>"
    http_headers = StatusAndHeaders(
        "200 OK", [("Content-Type", "text/html"), ("Server", "nginx")], protocol="HTTP/1.1")
    record = writer.create_warc_record(
        "https://firma.sk/", "response",
        payload=io.BytesIO(payload), length=len(payload), http_headers=http_headers)
    writer.write_record(record)
    return buf.getvalue()


def test_parse_warc_record_extracts_html_status_headers():
    rec = IndexRecord(root_domain="firma.sk", warc_filename="f.warc.gz", offset=0, length=0,
                      url="https://firma.sk/", http_status=200, crawl_id="CC-MAIN-2025-21")
    page = warc.parse_warc_record(_make_warc_record_bytes(), rec)
    assert page is not None
    assert page.http_status == 200 and "Firma" in page.html
    assert page.headers.get("Server", "").lower() == "nginx"
    assert page.root_domain == "firma.sk"


def test_parse_warc_record_non_html_returns_none():
    rec = IndexRecord(root_domain="x.sk", warc_filename="f", offset=0, length=0,
                      url="https://x.sk/", http_status=200, crawl_id="c")
    # gzip of empty/garbage -> not a WARC record -> None
    assert warc.parse_warc_record(gzip.compress(b"not a warc"), rec) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_commoncrawl_enrich_warc.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the implementation**

```python
# src/dagster_v3/commoncrawl_enrich/warc.py
import io
import logging

import requests
from warcio.archiveiterator import ArchiveIterator

from dagster_v3.commoncrawl_enrich.models import FetchedPage, IndexRecord

LOGGER = logging.getLogger(__name__)

DATA_HOST = "https://data.commoncrawl.org"
USER_AGENT = "corpscout-commoncrawl-enrich/0.1 (goran.raovic@gmail.com)"


def parse_warc_record(raw_gzip_bytes: bytes, record: IndexRecord) -> FetchedPage | None:
    """Parse a single (gzipped) WARC response record into a FetchedPage; None if not HTML."""
    try:
        for warc_record in ArchiveIterator(io.BytesIO(raw_gzip_bytes)):
            if warc_record.rec_type != "response":
                continue
            http = warc_record.http_headers
            content_type = (http.get_header("Content-Type") or "").lower() if http else ""
            if "html" not in content_type:
                return None
            status = int((http.get_statuscode() or "0")) if http else 0
            headers = {k: v for k, v in (http.headers if http else [])}
            body = warc_record.content_stream().read()
            html = body.decode("utf-8", "replace")
            capture = warc_record.rec_headers.get_header("WARC-Date") or ""
            return FetchedPage(
                root_domain=record.root_domain, final_url=record.url, http_status=status,
                headers=headers, html=html, capture_date=capture[:10], crawl_id=record.crawl_id,
            )
    except Exception as exc:  # noqa: BLE001 - malformed record -> treat as miss
        LOGGER.debug("WARC parse failed for %s: %s", record.root_domain, exc)
        return None
    return None


def fetch_page(record: IndexRecord, *, session: requests.Session | None = None,
               timeout_seconds: int = 60) -> FetchedPage | None:
    """Byte-range GET the single WARC record from the free CloudFront mirror, then parse it."""
    http = session or requests.Session()
    end = record.offset + record.length - 1
    response = http.get(
        f"{DATA_HOST}/{record.warc_filename}",
        headers={"User-Agent": USER_AGENT, "Range": f"bytes={record.offset}-{end}"},
        timeout=timeout_seconds,
    )
    if response.status_code not in (200, 206) or not response.content:
        return None
    return parse_warc_record(response.content, record)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_commoncrawl_enrich_warc.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/commoncrawl_enrich/warc.py tests/test_commoncrawl_enrich_warc.py
git commit -m "feat(commoncrawl_enrich): WARC byte-range fetch + warcio parse"
```

---

## Task 8: Per-domain orchestration

**Files:**
- Create: `src/dagster_v3/commoncrawl_enrich/enrich.py`
- Test: `tests/test_commoncrawl_enrich_enrich.py`

`enrich_domains(targets, *, resolve, fetch, llm, max_workers)` injects the index resolver, the
fetcher, and the LLM arm so the whole flow is tested with fakes. The LLM arm runs **always** for
industry; contact-recall runs **only** when the deterministic arm found no email/phone.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_commoncrawl_enrich_enrich.py
from dagster_v3.commoncrawl_enrich import enrich, llm
from dagster_v3.commoncrawl_enrich.models import DomainTarget, FetchedPage, IndexRecord

HTML_WITH_CONTACT = "<html lang='sk'><title>A</title><body>IČO: 31 333 532 info@a.sk</body></html>"
HTML_NO_CONTACT = "<html lang='sk'><title>B</title><body>Vitajte</body></html>"


def _targets():
    return [
        DomainTarget("a.sk", 1, 9.0),   # in index, has contact
        DomainTarget("b.sk", 2, 8.0),   # in index, no contact -> LLM recall
        DomainTarget("gone.sk", 3, 7.0)  # not in index
    ]


def _fake_resolve(domains, *, crawl_id):
    out = {}
    for d in domains:
        if d in ("a.sk", "b.sk"):
            out[d] = IndexRecord(d, "f.warc.gz", 0, 1, f"https://{d}/", 200, crawl_id)
    return out


def _fake_fetch(record, **_):
    html = HTML_WITH_CONTACT if record.root_domain == "a.sk" else HTML_NO_CONTACT
    return FetchedPage(record.root_domain, record.url, 200, {"server": "nginx"}, html, "2025-05-01", record.crawl_id)


def _fake_chat(system, user):
    if "industry" in system.lower():
        return '{"label":"Test","nace_hint":"00.00","confidence":50}'
    return '{"emails":["found@b.sk"],"phones":[]}'


def test_enrich_domains_full_flow():
    results = {e.target.root_domain: e for e in enrich.enrich_domains(
        _targets(), resolve=_fake_resolve, fetch=_fake_fetch, llm=llm.LLMArm(_fake_chat),
        crawl_id="CC-MAIN-2025-21", max_workers=2)}

    assert results["gone.sk"].fetch_status == "not_in_index"
    a = results["a.sk"]
    assert a.fetch_status == "ok" and a.ico == "31333532"
    assert any(e.email == "info@a.sk" and e.source_method == "regex" for e in a.emails)
    assert a.industry.label == "Test"
    assert any(t.technology == "Nginx" for t in a.technologies)
    # b had no deterministic contact -> LLM recall added one tagged 'llm'
    b = results["b.sk"]
    assert any(e.email == "found@b.sk" and e.source_method == "llm" for e in b.emails)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_commoncrawl_enrich_enrich.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the implementation**

```python
# src/dagster_v3/commoncrawl_enrich/enrich.py
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from dagster_v3.commoncrawl_enrich import extract, tech
from dagster_v3.commoncrawl_enrich.llm import LLMArm
from dagster_v3.commoncrawl_enrich.models import DomainEnrichment, DomainTarget, FetchedPage, IndexRecord

LOGGER = logging.getLogger(__name__)

ResolveFn = Callable[..., dict[str, IndexRecord]]
FetchFn = Callable[..., FetchedPage | None]


def _enrich_one(target: DomainTarget, record: IndexRecord | None, *, fetch: FetchFn, llm: LLMArm) -> DomainEnrichment:
    if record is None:
        return DomainEnrichment(target=target, fetch_status="not_in_index")
    page = fetch(record)
    if page is None:
        return DomainEnrichment(target=target, fetch_status="fetch_failed")

    det = extract.extract_deterministic(page)
    enr = DomainEnrichment(
        target=target, fetch_status="ok", page=page,
        title=det.title, meta_description=det.meta_description, content_language=det.content_language,
        ico=det.ico, dic=det.dic, ico_checksum_valid=det.ico_checksum_valid,
        emails=list(det.emails), phones=list(det.phones), socials=list(det.socials),
        technologies=tech.detect_technologies(page.html, page.headers),
    )
    parsed_text = extract.parse_html(page.html).text
    enr.industry = llm.classify_industry(parsed_text)
    if not enr.emails and not enr.phones:  # contact-recall only on the deterministic-miss residual
        extra_emails, extra_phones = llm.recover_contacts(parsed_text)
        enr.emails.extend(extra_emails)
        enr.phones.extend(extra_phones)
    return enr


def enrich_domains(targets: list[DomainTarget], *, resolve: ResolveFn, fetch: FetchFn,
                   llm: LLMArm, crawl_id: str, max_workers: int = 16) -> list[DomainEnrichment]:
    records = resolve([t.root_domain for t in targets], crawl_id=crawl_id)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(
            lambda t: _enrich_one(t, records.get(t.root_domain), fetch=fetch, llm=llm),
            targets,
        ))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_commoncrawl_enrich_enrich.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/commoncrawl_enrich/enrich.py tests/test_commoncrawl_enrich_enrich.py
git commit -m "feat(commoncrawl_enrich): per-domain orchestration (deterministic + LLM arm)"
```

---

## Task 9: Parquet output

**Files:**
- Create: `src/dagster_v3/commoncrawl_enrich/parquet_out.py`
- Test: `tests/test_commoncrawl_enrich_parquet.py`

Write the 5 tables (`domain_enrichment` spine + `domain_emails`/`phones`/`socials`/`technologies`)
matching the spec's column names.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_commoncrawl_enrich_parquet.py
import duckdb

from dagster_v3.commoncrawl_enrich import parquet_out
from dagster_v3.commoncrawl_enrich.models import (
    DomainEnrichment, DomainTarget, Email, IndustryGuess, Technology)


def test_write_parquet_emits_five_tables(tmp_path):
    enr = DomainEnrichment(
        target=DomainTarget("a.sk", 1, 9.0), fetch_status="ok", title="A",
        content_language="sk", ico="31333532", ico_checksum_valid=True,
        industry=IndustryGuess("Accounting", "69.20", 80, "llm"),
        emails=[Email("info@a.sk", True, "regex")],
        technologies=[Technology("Nginx", "Web server", "", 100)],
    )
    paths = parquet_out.write_parquet([enr], tmp_path)
    con = duckdb.connect()
    spine = con.execute(f"select root_domain, ico, industry_label, email_count, technology_count "
                        f"from read_parquet('{paths['domain_enrichment']}')").fetchone()
    assert spine == ("a.sk", "31333532", "Accounting", 1, 1)
    emails = con.execute(f"select root_domain, email, source_method "
                         f"from read_parquet('{paths['domain_emails']}')").fetchall()
    assert emails == [("a.sk", "info@a.sk", "regex")]
    techs = con.execute(f"select technology from read_parquet('{paths['domain_technologies']}')").fetchone()
    assert techs == ("Nginx",)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_commoncrawl_enrich_parquet.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the implementation**

```python
# src/dagster_v3/commoncrawl_enrich/parquet_out.py
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from dagster_v3.commoncrawl_enrich.models import DomainEnrichment


def _spine_rows(enrichments: list[DomainEnrichment]) -> list[dict]:
    rows = []
    for e in enrichments:
        ind = e.industry
        rows.append({
            "root_domain": e.target.root_domain, "source_rank": e.target.source_rank,
            "open_page_rank": e.target.open_page_rank,
            "tld": e.target.root_domain.rsplit(".", 1)[-1] if "." in e.target.root_domain else "",
            "homepage_url": e.page.final_url if e.page else "",
            "capture_date": e.page.capture_date if e.page else "",
            "http_status": e.page.http_status if e.page else 0,
            "content_language": e.content_language, "title": e.title,
            "meta_description": e.meta_description, "ico": e.ico, "dic": e.dic,
            "ico_checksum_valid": int(e.ico_checksum_valid),
            "industry_label": ind.label if ind else "",
            "industry_nace_hint": ind.nace_hint if ind else "",
            "industry_confidence": ind.confidence if ind else 0,
            "industry_method": ind.method if ind else "none",
            "email_count": len(e.emails), "phone_count": len(e.phones),
            "social_count": len(e.socials), "technology_count": len(e.technologies),
            "fetch_status": e.fetch_status,
        })
    return rows


def write_parquet(enrichments: list[DomainEnrichment], out_dir: str | Path) -> dict[str, str]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    emails = [{"root_domain": e.target.root_domain, "email": m.email,
               "is_role": int(m.is_role), "source_method": m.source_method}
              for e in enrichments for m in e.emails]
    phones = [{"root_domain": e.target.root_domain, "phone_raw": p.phone_raw,
               "phone_e164": p.phone_e164, "source_method": p.source_method}
              for e in enrichments for p in e.phones]
    socials = [{"root_domain": e.target.root_domain, "platform": s.platform,
                "url": s.url, "handle": s.handle}
               for e in enrichments for s in e.socials]
    techs = [{"root_domain": e.target.root_domain, "technology": t.technology,
              "category": t.category, "version": t.version, "confidence": t.confidence}
             for e in enrichments for t in e.technologies]

    tables = {
        "domain_enrichment": _spine_rows(enrichments),
        "domain_emails": emails, "domain_phones": phones,
        "domain_socials": socials, "domain_technologies": techs,
    }
    paths: dict[str, str] = {}
    for name, rows in tables.items():
        path = out / f"{name}.parquet"
        pq.write_table(pa.Table.from_pylist(rows), path)
        paths[name] = str(path)
    return paths
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_commoncrawl_enrich_parquet.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/commoncrawl_enrich/parquet_out.py tests/test_commoncrawl_enrich_parquet.py
git commit -m "feat(commoncrawl_enrich): Parquet output for the 5 tables"
```

---

## Task 10: Metrics / speed report

**Files:**
- Create: `src/dagster_v3/commoncrawl_enrich/metrics.py`
- Test: `tests/test_commoncrawl_enrich_metrics.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_commoncrawl_enrich_metrics.py
from dagster_v3.commoncrawl_enrich import metrics
from dagster_v3.commoncrawl_enrich.models import (
    DomainEnrichment, DomainTarget, Email, IndustryGuess)


def _enr(domain, status, emails=(), industry=None, ico=""):
    return DomainEnrichment(target=DomainTarget(domain, 1, 1.0), fetch_status=status,
                            emails=list(emails), industry=industry, ico=ico)


def test_report_counts_and_uplift():
    enrichments = [
        _enr("a.sk", "ok", emails=[Email("x@a.sk", False, "regex")], ico="31333532",
             industry=IndustryGuess("Acc", "69.20", 90, "llm")),
        _enr("b.sk", "ok", emails=[Email("y@b.sk", False, "llm")],
             industry=IndustryGuess("", "", 0, "none")),
        _enr("gone.sk", "not_in_index"),
    ]
    report = metrics.build_report(enrichments, wall_clock_seconds=10.0)
    assert report["total"] == 3
    assert report["found_in_index"] == 2 and report["fetched_ok"] == 2
    assert report["with_valid_ico"] == 1
    assert report["with_email"] == 2
    assert report["email_llm_uplift"] == 1   # b.sk's email came only from the LLM
    assert report["industry_classified"] == 1
    assert report["domains_per_second"] == 0.3
    assert "projected_100k_hours" in report and "projected_10m_hours" in report
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_commoncrawl_enrich_metrics.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the implementation**

```python
# src/dagster_v3/commoncrawl_enrich/metrics.py
from dagster_v3.commoncrawl_enrich.models import DomainEnrichment


def build_report(enrichments: list[DomainEnrichment], *, wall_clock_seconds: float) -> dict:
    total = len(enrichments)
    found = [e for e in enrichments if e.fetch_status != "not_in_index"]
    ok = [e for e in enrichments if e.fetch_status == "ok"]
    with_email = [e for e in ok if e.emails]
    # uplift = domains whose ONLY contacts came from the LLM
    email_llm_uplift = sum(
        1 for e in ok if e.emails and all(m.source_method == "llm" for m in e.emails)
    )
    industry_classified = sum(1 for e in ok if e.industry and e.industry.method == "llm")
    dps = round(total / wall_clock_seconds, 4) if wall_clock_seconds > 0 else 0.0
    return {
        "total": total,
        "found_in_index": len(found),
        "fetched_ok": len(ok),
        "with_valid_ico": sum(1 for e in ok if e.ico_checksum_valid),
        "with_email": len(with_email),
        "with_phone": sum(1 for e in ok if e.phones),
        "with_social": sum(1 for e in ok if e.socials),
        "with_technology": sum(1 for e in ok if e.technologies),
        "email_llm_uplift": email_llm_uplift,
        "industry_classified": industry_classified,
        "wall_clock_seconds": round(wall_clock_seconds, 2),
        "domains_per_second": dps,
        "projected_100k_hours": round(100_000 / dps / 3600, 2) if dps else None,
        "projected_10m_hours": round(10_000_000 / dps / 3600, 2) if dps else None,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_commoncrawl_enrich_metrics.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/commoncrawl_enrich/metrics.py tests/test_commoncrawl_enrich_metrics.py
git commit -m "feat(commoncrawl_enrich): hit-rate / uplift / speed report"
```

---

## Task 11: CLI runner

**Files:**
- Create: `src/dagster_v3/commoncrawl_enrich/run.py`
- Test: `tests/test_commoncrawl_enrich_run.py`

`load_targets(manifest_path, limit)` reads the domain manifest (Parquet/CSV with
`root_domain, source_rank, open_page_rank`) via DuckDB; `main()` wires real index/fetch/LLM. The
test exercises `load_targets` + the wiring with monkeypatched I/O (no network).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_commoncrawl_enrich_run.py
import json

import duckdb

from dagster_v3.commoncrawl_enrich import run


def test_load_targets_from_parquet(tmp_path):
    manifest = tmp_path / "m.parquet"
    duckdb.connect().execute(
        f"copy (select 'a.sk' root_domain, 1 source_rank, 9.0 open_page_rank "
        f"union all select 'b.sk', 2, 8.0) to '{manifest}' (format parquet)")
    targets = run.load_targets(str(manifest), limit=1)
    assert len(targets) == 1 and targets[0].root_domain == "a.sk"


def test_run_pipeline_writes_parquet_and_metrics(tmp_path, monkeypatch):
    from dagster_v3.commoncrawl_enrich import enrich, llm
    from dagster_v3.commoncrawl_enrich.models import DomainEnrichment, DomainTarget

    def fake_enrich_domains(targets, **_):
        return [DomainEnrichment(target=t, fetch_status="not_in_index") for t in targets]

    monkeypatch.setattr(run.enrich, "enrich_domains", fake_enrich_domains)
    targets = [DomainTarget("a.sk", 1, 9.0)]
    report = run.run_pipeline(targets, out_dir=tmp_path, resolve=None, fetch=None, llm=None,
                              crawl_id="c", max_workers=1)
    assert report["total"] == 1
    assert (tmp_path / "domain_enrichment.parquet").exists()
    assert json.loads((tmp_path / "metrics.json").read_text())["total"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_commoncrawl_enrich_run.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the implementation**

```python
# src/dagster_v3/commoncrawl_enrich/run.py
import argparse
import json
import logging
import os
import time
from pathlib import Path

import duckdb
import requests

from dagster_v3.commoncrawl_enrich import enrich, index_client, metrics, parquet_out, warc
from dagster_v3.commoncrawl_enrich.llm import from_openai
from dagster_v3.commoncrawl_enrich.models import DomainTarget

LOGGER = logging.getLogger(__name__)


def load_targets(manifest_path: str, *, limit: int) -> list[DomainTarget]:
    rows = duckdb.connect().execute(
        "select root_domain, source_rank, open_page_rank "
        "from read_parquet(?) order by open_page_rank desc limit ?"
        if manifest_path.endswith(".parquet") else
        "select root_domain, source_rank, open_page_rank "
        "from read_csv_auto(?) order by open_page_rank desc limit ?",
        [manifest_path, limit],
    ).fetchall()
    return [DomainTarget(str(r[0]), int(r[1]), float(r[2])) for r in rows]


def run_pipeline(targets, *, out_dir, resolve, fetch, llm, crawl_id, max_workers) -> dict:
    start = time.monotonic()
    enrichments = enrich.enrich_domains(
        targets, resolve=resolve, fetch=fetch, llm=llm, crawl_id=crawl_id, max_workers=max_workers)
    parquet_out.write_parquet(enrichments, out_dir)
    report = metrics.build_report(enrichments, wall_clock_seconds=time.monotonic() - start)
    Path(out_dir, "metrics.json").write_text(json.dumps(report, indent=2))
    return report


def _build_duckdb_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("install httpfs; load httpfs; set s3_region='us-east-1'; set s3_use_ssl=true;")
    return con


def main() -> None:
    parser = argparse.ArgumentParser(description="CommonCrawl enrichment Phase 0 spike runner")
    parser.add_argument("--manifest", required=True, help="Parquet/CSV: root_domain,source_rank,open_page_rank")
    parser.add_argument("--out", required=True, help="output directory for Parquet + metrics.json")
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--crawl-id", default=index_client.DEFAULT_CRAWL_ID)
    parser.add_argument("--max-workers", type=int, default=16)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    targets = load_targets(args.manifest, limit=args.limit)
    con = _build_duckdb_connection()
    session = requests.Session()
    llm = from_openai(
        base_url=os.environ["COMMONCRAWL_LLM_BASE_URL"],
        model=os.environ["COMMONCRAWL_LLM_MODEL"],
        api_key=os.environ.get("COMMONCRAWL_LLM_API_KEY", "not-needed"),
        enable_thinking=True,
    )
    report = run_pipeline(
        targets, out_dir=args.out,
        resolve=lambda domains, *, crawl_id: index_client.resolve_via_duckdb(domains, crawl_id=crawl_id, duckdb_con=con),
        fetch=lambda record: warc.fetch_page(record, session=session),
        llm=llm, crawl_id=args.crawl_id, max_workers=args.max_workers,
    )
    LOGGER.info("Report: %s", json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_commoncrawl_enrich_run.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/commoncrawl_enrich/run.py tests/test_commoncrawl_enrich_run.py
git commit -m "feat(commoncrawl_enrich): CLI runner (manifest -> Parquet + metrics)"
```

---

## Task 12: Package README + full test sweep

**Files:**
- Create: `src/dagster_v3/commoncrawl_enrich/README.md`

- [ ] **Step 1: Write the README**

```markdown
# commoncrawl_enrich (Phase 0 spike)

Standalone, single-process domain enrichment over CommonCrawl. No Dagster/Temporal/ClickHouse.

## Run a ~10k spike
1. Export a manifest from open_page_rank (Parquet/CSV with `root_domain, source_rank, open_page_rank`).
2. Set the LLM env (local or hosted OpenAI-compatible endpoint):
   - `COMMONCRAWL_LLM_BASE_URL`, `COMMONCRAWL_LLM_MODEL`, `COMMONCRAWL_LLM_API_KEY` (optional).
3. Run:
   ```bash
   uv run python -m dagster_v3.commoncrawl_enrich.run \
     --manifest top10k.parquet --out ./cc_out --limit 10000 --max-workers 16
   ```
4. Inspect `./cc_out/metrics.json` (hit-rate, regex-vs-LLM uplift, speed + projected 100k/10M hours)
   and the 5 Parquet tables.

## Switching local -> hosted LLM
Change only the three `COMMONCRAWL_LLM_*` env vars. Thinking mode is on by default
(`from_openai(enable_thinking=True)`).

## Notes
- Index resolution uses the CC columnar Parquet via DuckDB httpfs (anonymous S3); `index_client.resolve_via_cdx` is the per-domain fallback.
- Technology detection is a built-in fingerprint set (`tech.py`); swap in full Wappalyzer behind `detect_technologies` later.
```

- [ ] **Step 2: Run the full package test sweep**

Run: `uv run pytest tests/test_commoncrawl_enrich_*.py -q`
Expected: PASS (all module tests green).

- [ ] **Step 3: Verify the package is NOT loaded as Dagster defs**

Run: `uv run dg check defs`
Expected: "All definitions loaded successfully." (the package lives outside `defs/`, so it's ignored).

- [ ] **Step 4: Commit**

```bash
git add src/dagster_v3/commoncrawl_enrich/README.md
git commit -m "docs(commoncrawl_enrich): package README + Phase 0 usage"
```

---

## Done — Phase 0 deliverable

The `commoncrawl_enrich` package runs a ~10k spike from a manifest and emits 5 Parquet tables + a
`metrics.json` with hit-rate, regex-vs-LLM uplift, and speed (incl. projected 100k/10M wall-clock).
**Decision gate:** read `metrics.json`. If the numbers justify it, proceed to Phase 1 (Temporal
chunked orchestration + Dagster Parquet→ClickHouse load + `company_website_domains` linking), per
the design doc §6/§9.

**Live-run prerequisites (not needed for tests):** a domain manifest exported from `open_page_rank`,
and a reachable OpenAI-compatible LLM endpoint via the `COMMONCRAWL_LLM_*` env vars.
