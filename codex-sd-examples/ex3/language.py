import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from urllib.parse import parse_qsl, urljoin, urlsplit, urlunsplit

from ex3.models import LanguageCandidate, LanguageCandidateMethod

ENGLISH_LABELS = frozenset(
    {
        "en",
        "eng",
        "english",
        "english language",
        "english version",
        "global english",
    }
)
LOCALE_QUERY_KEYS = frozenset({"lang", "language", "locale"})


@dataclass(slots=True)
class _Anchor:
    href: str
    hreflang: str
    title: str
    text_parts: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class _ParsedLink:
    tag: str
    href: str
    hreflang: str
    rel: str
    title: str
    text: str


class _LanguageHintParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.document_language: str | None = None
        self.links: list[_ParsedLink] = []
        self._anchor: _Anchor | None = None

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        attributes = {key.casefold(): value or "" for key, value in attrs}
        normalized_tag = tag.casefold()

        if normalized_tag == "html" and self.document_language is None:
            language = attributes.get("lang", "").strip()
            self.document_language = language or None
            return

        if normalized_tag == "link":
            href = attributes.get("href", "").strip()
            if href:
                self.links.append(
                    _ParsedLink(
                        tag="link",
                        href=href,
                        hreflang=attributes.get("hreflang", "").strip(),
                        rel=attributes.get("rel", "").strip(),
                        title=attributes.get("title", "").strip(),
                        text="",
                    )
                )
            return

        if normalized_tag == "a":
            href = attributes.get("href", "").strip()
            if href:
                self._anchor = _Anchor(
                    href=href,
                    hreflang=attributes.get("hreflang", "").strip(),
                    title=attributes.get("title", "").strip(),
                )

    def handle_data(self, data: str) -> None:
        if self._anchor is not None:
            self._anchor.text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() != "a" or self._anchor is None:
            return

        self.links.append(
            _ParsedLink(
                tag="a",
                href=self._anchor.href,
                hreflang=self._anchor.hreflang,
                rel="",
                title=self._anchor.title,
                text=" ".join(self._anchor.text_parts),
            )
        )
        self._anchor = None


def inspect_language_page(
    page_url: str,
    html: str,
) -> tuple[str | None, list[LanguageCandidate]]:
    """Extract the declared language and ranked English-version candidates."""
    parser = _LanguageHintParser()
    parser.feed(html)

    candidates_by_url: dict[str, LanguageCandidate] = {}
    for link in parser.links:
        candidate_url = _normalize_candidate_url(link.href, base_url=page_url)
        if candidate_url is None or candidate_url == page_url:
            continue

        match = _classify_english_link(link, candidate_url)
        if match is None:
            continue
        method, score = match
        candidate = LanguageCandidate(
            url=candidate_url,
            detection_method=method,
            score=score,
        )
        existing = candidates_by_url.get(candidate_url)
        if existing is None or candidate.score < existing.score:
            candidates_by_url[candidate_url] = candidate

    return parser.document_language, sorted(
        candidates_by_url.values(),
        key=lambda candidate: (candidate.score, candidate.url),
    )


def is_english_language(language: str | None) -> bool:
    if language is None:
        return False
    normalized = language.strip().replace("_", "-").casefold()
    return normalized == "en" or normalized.startswith("en-")


def _classify_english_link(
    link: _ParsedLink,
    url: str,
) -> tuple[LanguageCandidateMethod, int] | None:
    hreflang = link.hreflang.replace("_", "-").casefold()
    rel_tokens = {token.casefold() for token in link.rel.split()}
    if link.tag == "link" and "alternate" in rel_tokens and _is_english_code(hreflang):
        return "alternate_hreflang", 0
    if link.tag == "a" and _is_english_code(hreflang):
        return "anchor_hreflang", 10

    label = _normalize_label(f"{link.text} {link.title}")
    if label in ENGLISH_LABELS or label.startswith("english "):
        return "language_link", 20
    if _url_looks_english(url):
        return "english_url", 30
    return None


def _is_english_code(value: str) -> bool:
    return value == "en" or value.startswith("en-")


def _normalize_label(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip().casefold()


def _url_looks_english(url: str) -> bool:
    parsed = urlsplit(url)
    hostname = (parsed.hostname or "").casefold()
    if hostname.startswith("en."):
        return True

    path_segments = {
        segment.casefold() for segment in parsed.path.split("/") if segment
    }
    if path_segments & {"en", "en-us", "en-gb", "english"}:
        return True

    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.casefold() in LOCALE_QUERY_KEYS and _is_english_code(
            value.replace("_", "-").casefold()
        ):
            return True
    return False


def _normalize_candidate_url(url: str, *, base_url: str) -> str | None:
    try:
        parsed = urlsplit(urljoin(base_url, url.strip()))
        _ = parsed.port
    except ValueError:
        return None

    if parsed.scheme.casefold() not in {"http", "https"}:
        return None
    if parsed.hostname is None:
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    return urlunsplit(
        (
            parsed.scheme.casefold(),
            parsed.netloc.casefold(),
            parsed.path or "/",
            parsed.query,
            "",
        )
    )
