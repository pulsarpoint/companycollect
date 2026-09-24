"""Stable requested-page identity, independent of redirects and scan outcome."""

import ipaddress
from urllib.parse import urlsplit


def page_identity(requested_url: str) -> tuple[str, str]:
    if not requested_url or any(
        ord(char) <= 32 or ord(char) == 127 for char in requested_url
    ):
        raise ValueError(f"Invalid requested page URL: {requested_url!r}")
    parsed = urlsplit(requested_url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(f"Invalid requested page URL: {requested_url!r}")
    host = parsed.hostname.rstrip(".").lower()
    if ":" in host:
        host = f"[{ipaddress.IPv6Address(host).compressed}]"
    else:
        host = host.encode("idna").decode("ascii")
        if not host or any(char in host for char in "/\\?#%"):
            raise ValueError(f"Invalid requested page host: {requested_url!r}")
    port = parsed.port
    authority = (
        host
        if port is None or port == {"http": 80, "https": 443}[parsed.scheme]
        else f"{host}:{port}"
    )
    origin = f"{parsed.scheme}://{authority}"
    query = "?" + parsed.query if "?" in requested_url.split("#", 1)[0] else ""
    return origin, origin + (parsed.path or "/") + query
