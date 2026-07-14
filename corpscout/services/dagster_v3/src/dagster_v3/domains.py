from __future__ import annotations

import ipaddress
import urllib.parse

import tldextract

_EXTRACT = tldextract.TLDExtract(
    suffix_list_urls=(),
    cache_dir=None,
)


def normalized_url_parts(raw: str | None) -> tuple[str, str, str]:
    value = (raw or "").strip()
    if not value:
        return "", "", ""
    # Source contact data is messy (addresses, free text mislabeled as a URL). urllib
    # raises ValueError when the authority has an invalid port/host (e.g. spaces) — a
    # malformed value carries no domain, so degrade to empty rather than crash callers.
    try:
        parsed = urllib.parse.urlparse(value)
        normalized = value if parsed.scheme else f"https://{value}"
        normalized_parsed = urllib.parse.urlparse(normalized)
        host = (normalized_parsed.hostname or "").lower()
        if not host:
            return "", "", ""

        netloc = host
        if normalized_parsed.port is not None:
            netloc = f"{host}:{normalized_parsed.port}"
        normalized_url = urllib.parse.urlunparse(
            normalized_parsed._replace(scheme=normalized_parsed.scheme.lower(), netloc=netloc)
        )
        return normalized_url, host, normalized_parsed.path
    except ValueError:
        return "", "", ""


def normalized_url(raw: str | None) -> str:
    return normalized_url_parts(raw)[0]


def website_host(raw: str | None) -> str:
    return normalized_url_parts(raw)[1]


def website_path(raw: str | None) -> str:
    return normalized_url_parts(raw)[2]


def root_domain(raw: str | None) -> str:
    _, host, _ = normalized_url_parts(raw)
    if not host or host == "localhost":
        return ""
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        return ""

    extracted = _EXTRACT(host)
    domain = extracted.top_domain_under_public_suffix
    return domain.lower() if domain else ""
