"""Crawler interpretation of raw browser-service captures."""

from dataclasses import dataclass, field
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup, Comment

from crawler_service.captures import page_inventory
from crawler_service.external_links import resolved_http_url


class BrowserUnavailable(RuntimeError):
    """Reopen the tab using the same assigned browser before retrying."""


@dataclass(kw_only=True)
class PageCapture:
    url: str
    html: str
    cleaned_html: str
    status_code: int | None
    headers: dict[str, str]
    metadata: dict
    links: list[dict]
    error: str | None
    redirects: list[dict] = field(default_factory=list)
    navigation_attempts: list[dict] = field(default_factory=list)

    @property
    def successful(self) -> bool:
        return (
            self.error is None
            and self.status_code is not None
            and 200 <= self.status_code < 400
        )


def simplify_html(html: str, source_url: str) -> str:
    """Keep readable structure and useful attributes; raw evidence stays separate."""
    soup = BeautifulSoup(html, "html.parser")
    base = soup.find("base", href=True)
    origin = (
        resolved_http_url(str(base["href"]), source_url) if base else None
    ) or source_url
    for tag in soup.find_all(
        ["script", "style", "template", "svg", "canvas", "noscript"]
    ):
        tag.decompose()
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    attributes = {
        "href",
        "src",
        "alt",
        "title",
        "aria-label",
        "role",
        "lang",
        "datetime",
        "colspan",
        "rowspan",
        "scope",
        "open",
    }
    for tag in soup.find_all(True):
        tag.attrs = {
            key: value for key, value in tag.attrs.items() if key in attributes
        }
        for name in ("href", "src"):
            if name not in tag.attrs:
                continue
            try:
                target = urljoin(origin, str(tag[name]))
                scheme = urlsplit(target).scheme.casefold()
            except ValueError:
                del tag[name]
                continue
            if scheme in {"http", "https", "mailto", "tel"}:
                tag[name] = target
            else:
                del tag[name]
    return str(soup)


def interpret_capture(
    *,
    url: str,
    html: str,
    status_code: int | None,
    headers: dict[str, str],
    error: str | None,
) -> PageCapture:
    soup = BeautifulSoup(html, "html.parser")
    description = soup.find("meta", attrs={"name": "description"})
    inventory, _ = page_inventory(html, url, html_kind="rendered_html")
    return PageCapture(
        url=url,
        html=html,
        cleaned_html=simplify_html(html, url),
        status_code=status_code,
        headers=headers,
        metadata={
            "title": soup.title.get_text(" ", strip=True) if soup.title else None,
            "description": description.get("content") if description else None,
        },
        links=[
            {
                "href": link["url"],
                "text": link["anchor_text"],
                "context": link["context"],
            }
            for link in inventory
            if link["url"] is not None
        ],
        error=error,
    )
