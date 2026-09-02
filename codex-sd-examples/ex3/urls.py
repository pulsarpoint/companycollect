"""URL normalization helpers shared by crawling, selection and analysis."""

from urllib.parse import urlsplit, urlunsplit


def normalize_start_url(url: str) -> str:
    """Normalize and validate an absolute HTTP(S) URL."""
    try:
        parsed = urlsplit(url.strip())
        port = parsed.port
    except ValueError as error:
        raise ValueError(f"Start URL is not valid: {url}") from error

    scheme = parsed.scheme.casefold()
    hostname = parsed.hostname
    if scheme not in {"http", "https"} or hostname is None:
        raise ValueError(f"Start URL must be an absolute HTTP(S) URL: {url}")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Start URL must not contain credentials")

    normalized_hostname = hostname.casefold()
    if ":" in normalized_hostname:
        normalized_hostname = f"[{normalized_hostname}]"
    default_port = (scheme == "http" and port == 80) or (
        scheme == "https" and port == 443
    )
    netloc = normalized_hostname
    if port is not None and not default_port:
        netloc = f"{normalized_hostname}:{port}"
    return urlunsplit((scheme, netloc, parsed.path or "/", parsed.query, ""))


def canonical_domain(hostname: str) -> str:
    """Return the registrable-looking domain without a leading ``www.``."""
    return hostname.rstrip(".").casefold().removeprefix("www.")


def same_domain_tree(candidate: str, searched: str) -> bool:
    """Report whether two canonical domains belong to one host tree."""
    return (
        candidate == searched
        or candidate.endswith(f".{searched}")
        or searched.endswith(f".{candidate}")
    )


def url_key(url: str) -> str:
    """Return a comparison key that ignores scheme, ``www.`` and trailing slashes."""
    parsed = urlsplit(url.strip())
    hostname = canonical_domain(parsed.hostname or "")
    path = parsed.path.rstrip("/") or "/"
    return urlunsplit(("", hostname, path, parsed.query, "")).removeprefix("//")
