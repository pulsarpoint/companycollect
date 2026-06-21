from dagster_v3.commoncrawl_enrich.models import Technology

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
