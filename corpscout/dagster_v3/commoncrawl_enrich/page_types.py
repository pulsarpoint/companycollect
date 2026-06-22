"""Structural page-type detection from WET plaintext.

A page type is the *shape* of a page that carries no classifiable business content:
parked / for-sale domains, registrar parking, intentional placeholders, default
(misconfigured) web servers, open directory listings. This is orthogonal to
industry — a page with real content, including adult or gambling, is NOT a page
type; it gets a NACE code. Page types are mined here by high-precision keyword
signals (the SEED), then embedded into a `nace_embed.PrototypeSet` so the
classifier can route them to their type instead of forcing a wrong NACE code.

Signals are deliberately high-precision (false positives would poison the
prototypes); recall is recovered downstream by embedding-NN expansion.
"""

import re

from commoncrawl_enrich import segment

# (class label, regex). Case-insensitive, DOTALL. Grouped so each type forms its own
# prototype cluster and gets its own returned label.
PAGE_TYPE_SIGNALS: list[tuple[str, str]] = [
    ("for_sale", r"\bbuy this domain\b"),
    ("for_sale", r"\bpurchase this domain\b"),
    ("for_sale", r"this domain (?:is|may be|could be) for sale"),
    ("for_sale", r"\bdomain(?: name)? is for sale\b"),
    ("for_sale", r"the domain .{0,60}? is for sale"),
    ("for_sale", r"\bthis domain has expired\b"),
    ("for_sale", r"\binterested in this domain\b"),
    ("parking_provider", r"\bsedoparking\b"),
    ("parking_provider", r"\bparkingcrew\b"),
    ("parking_provider", r"\bafternic\b"),
    ("parking_provider", r"\bhugedomains\b"),
    ("parking_provider", r"\bcashparking\b"),
    ("parking_provider", r"this (?:web ?)?page is parked"),
    ("parking_provider", r"\bthis domain (?:is )?parked\b"),
    ("parking_provider", r"\bparked free\b"),
    ("parking_provider", r"\bdomain parking\b"),
    ("parking_provider", r"\bparked by (?:the )?(?:domain|registrant|owner)\b"),
    # intentional placeholders (owner put up a holding page on purpose)
    ("under_construction", r"\bunder construction\b"),
    ("under_construction", r"\bwebsite coming soon\b"),
    # default / unconfigured web servers — potential MISCONFIGURED or abandoned
    # service (security-relevant: exposed default install, forgotten vhost).
    ("default_server", r"\bwelcome to nginx\b"),
    ("default_server", r"apache2?.{0,30}default page"),
    ("default_server", r"\btest page for the (?:apache|nginx)\b"),
    ("default_server", r"\bapache http server test page\b"),
    ("default_server", r"\b(?:iis windows server|iisstart|internet information services)\b"),
    ("default_server", r"\bsite not configured\b"),
    ("default_server", r"\bno (?:web)?site (?:is )?configured\b"),
    ("default_server", r"\byour new website is ready\b"),
    ("default_server", r"\bweb server'?s default (?:web ?)?page\b"),
    # directory listings: legit infra, not a business homepage -> own class.
    # require the corroborating listing structure to avoid matching prose.
    ("directory_listing", r"\bindex of /\S*.{0,120}(?:parent directory|last modified)"),
]
_COMPILED = [(label, re.compile(pat, re.I | re.S)) for label, pat in PAGE_TYPE_SIGNALS]
_MAX_CHARS = 8000
_HEAD_CHARS = 2000  # parking/placeholder text is at the top; scan only the head


def match_signal(text: str) -> str | None:
    """First matching page-type class label for the page text, or None."""
    head = text[:_HEAD_CHARS]
    for label, rx in _COMPILED:
        if rx.search(head):
            return label
    return None


def scan_wet(source: str, *, scan_limit: int, max_exemplars: int = 4000,
             content_cap: int = 0, session=None) -> dict:
    """One pass over a WET file. Returns:

    - ``exemplars``: page-type pages (keyword-matched), deduped by root_domain, with
      ``signal`` (class), ``url``, ``root_domain``, ``text``.
    - ``content``: up to ``content_cap`` homepages that did NOT match any signal,
      deduped by domain — the real-content negatives for threshold calibration.
    """
    from warcio.archiveiterator import ArchiveIterator

    stream = segment._open_stream(source, session)
    exemplars: list[dict] = []
    content: list[dict] = []
    seen_ex: set[str] = set()
    seen_content: set[str] = set()
    scanned = 0
    try:
        for record in ArchiveIterator(stream):
            if record.rec_type != "conversion":
                continue
            scanned += 1
            uri = record.rec_headers.get_header("WARC-Target-URI") or ""
            text = record.content_stream().read().decode("utf-8", "replace")
            label = match_signal(text)
            root, _ = segment._host(uri)
            if label:
                if root not in seen_ex:
                    seen_ex.add(root)
                    exemplars.append({"url": uri, "root_domain": root, "signal": label,
                                      "text": text[:_MAX_CHARS].strip()})
            elif (content_cap and len(content) < content_cap
                  and segment._is_homepage(uri) and root not in seen_content
                  and len(text.strip()) >= 200):
                seen_content.add(root)
                content.append({"url": uri, "root_domain": root, "text": text[:_MAX_CHARS].strip()})
            if len(exemplars) >= max_exemplars and len(content) >= content_cap:
                break
            if scanned >= scan_limit:
                break
    finally:
        stream.close()
    return {"exemplars": exemplars, "content": content, "scanned": scanned}
