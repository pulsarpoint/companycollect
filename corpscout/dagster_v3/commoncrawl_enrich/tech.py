from commoncrawl_enrich.models import Technology

# (technology, category, html-substrings)
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


def _to_multidict(headers: dict[str, str] | None) -> dict[str, list[str]]:
    return {k: [v] for k, v in (headers or {}).items()}


_CLIENT: object = None  # lazy: WappalyzerClient | False (no service configured)


def _wappalyzer_client():
    global _CLIENT
    if _CLIENT is None:
        from commoncrawl_enrich.wappalyzer_client import WappalyzerClient
        _CLIENT = WappalyzerClient.from_env() or False
    return _CLIENT or None


def detect_technologies(html: str, headers: dict[str, str]) -> list[Technology]:
    """Detect technologies via the wappalyzer-service (wappalyzergo) when configured
    (COMMONCRAWL_WAPPALYZER_URL); otherwise fall back to the built-in fingerprints.
    For bulk WARC processing prefer WappalyzerClient.analyze_batch directly."""
    client = _wappalyzer_client()
    if client is not None:
        return client.analyze(_to_multidict(headers), html or "")
    return _builtin_detect(html, headers)


def _builtin_detect(html: str, headers: dict[str, str]) -> list[Technology]:
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
