"""Local HTML capture format shared by the crawler and saved-page analysis."""

import json
from pathlib import Path

from bs4 import BeautifulSoup

from company_research.external_links import link_context, resolved_http_url
from company_research.models import Page
from company_research.storage import content_hash, write_json


def page_inventory(
    html: str, source_url: str, *, html_kind: str
) -> tuple[list[dict], list[dict]]:
    """Observe navigation on this capture; never fetch or modify source HTML."""
    soup = BeautifulSoup(html, "html.parser")
    for element in soup.find_all(["script", "style", "template"]):
        element.decompose()
    base = soup.find("base", href=True)
    origin = resolved_http_url(str(base["href"]), source_url) if base else source_url
    origin = origin or source_url
    headings = [
        {"section_id": f"h{index:04}", "text": tag.get_text(" ", strip=True)}
        for index, tag in enumerate(soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]))
    ]
    links = []
    for tag in soup.find_all(["a", "iframe", "button"]):
        raw = str(tag.get("src" if tag.name == "iframe" else "href", ""))
        label = tag.get_text(" ", strip=True) or str(
            tag.get("aria-label") or tag.get("title") or ""
        )
        url = resolved_http_url(raw, origin) if raw else None
        if tag.name == "button":
            region = " ".join(
                str(parent.get("class", "")) + " " + str(parent.get("aria-label", ""))
                for parent in tag.parents
                if hasattr(parent, "get")
            )
            if not any(
                word in (label + " " + region).casefold()
                for word in [
                    "next",
                    "nästa",
                    "load more",
                    "visa fler",
                    "pagination",
                    "paginering",
                ]
            ):
                continue
            kind = "control"
        elif url is None:
            continue
        else:
            kind = "iframe" if tag.name == "iframe" else "url"
        links.append(
            {
                "link_id": f"l{len(links):04}",
                "url": url,
                "raw_href": raw or None,
                "kind": kind,
                "anchor_text": label,
                "source_url": source_url,
                "context": link_context(tag),
                "inventory_source": html_kind,
            }
        )
    return links, headings


def capture_metadata(page: Page) -> dict:
    """Keep crawl outcomes independent of the legacy page extraction fields."""
    return page.model_dump(
        exclude={
            "extraction_status",
            "objectives_examined",
            "chunks_planned",
            "chunks_completed",
            "job_detail",
            "extraction_attempts",
        }
    )


def save_capture(root: Path, page: Page, html: str, target_url: str) -> dict:
    """Publish a replayable page; keep rendered evidence separate from cleaned HTML."""
    folder = root / "pages" / page.page_id
    folder.mkdir(parents=True, exist_ok=False)
    (folder / "page.html").write_text(html, encoding="utf-8")
    metadata = capture_metadata(page)
    metadata["html_file"] = "page.html"
    metadata["html_sha256"] = content_hash(html)
    rendered_file = root / "link-html" / f"{page.page_id}.html"
    rendered = (
        rendered_file.read_text(encoding="utf-8") if rendered_file.is_file() else None
    )
    if rendered is not None:
        (folder / "link-page.html").write_text(rendered, encoding="utf-8")
        metadata["link_html_file"] = "link-page.html"
        metadata["link_html_sha256"] = content_hash(rendered)
    links, _ = page_inventory(
        rendered if rendered is not None else html,
        page.source_url,
        html_kind="rendered_html" if rendered is not None else "cleaned_html",
    )
    _, headings = page_inventory(html, page.source_url, html_kind="cleaned_html")
    write_json(
        folder / "input.json",
        {
            "page": metadata,
            "target_url": target_url,
            "links": links,
            "headings": headings,
        },
    )
    return {
        **metadata,
        "snapshot": str(folder.relative_to(root)),
        "html_file": str((folder / "page.html").relative_to(root)),
        **(
            {"link_html_file": str((folder / "link-page.html").relative_to(root))}
            if rendered is not None
            else {}
        ),
    }


def save_crawl_result(root: Path, manifest: dict) -> None:
    """Publish one portable JSON document including the collected HTML content."""
    documents = [
        {
            "page_id": page["page_id"],
            "url": page["source_url"],
            "html": (root / page["html_file"]).read_text(encoding="utf-8"),
            "html_sha256": page["html_sha256"],
        }
        for page in manifest["pages"]
        if page["fetch_status"] == "fetched"
    ]
    write_json(
        root / "result.json",
        {
            "schema_version": "company-crawl-result/1.0",
            "crawl": manifest,
            "documents": documents,
        },
    )


def load_crawl(path: Path) -> tuple[dict, list[Path]]:
    """Validate a movable crawl folder before starting any paid analysis."""
    path = path / "crawl-manifest.json" if path.is_dir() else path
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != "company-crawl/1.0"
    ):
        raise ValueError("Expected a company-crawl/1.0 manifest")
    if manifest.get("status") in {"running", "skip_crawling", "needs_review"}:
        raise ValueError(f"Cannot analyze crawl with status {manifest['status']}")
    root = path.parent.resolve()
    folders = []
    seen = set()
    for page in manifest["pages"]:
        if page["fetch_status"] != "fetched":
            continue
        folder = (root / page["snapshot"]).resolve()
        if not folder.is_relative_to(root) or folder in seen:
            raise ValueError("Invalid or duplicate capture path in crawl manifest")
        seen.add(folder)
        for filename in ("input.json", "page.html", "link-page.html"):
            if not (folder / filename).resolve().is_relative_to(root):
                raise ValueError("Capture file is outside the crawl folder")
        metadata = json.loads((folder / "input.json").read_text(encoding="utf-8"))
        if metadata["target_url"] != manifest["site_url"]:
            raise ValueError("Capture target does not match the crawl manifest")
        for field in ("page_id", "source_url", "html_sha256"):
            if metadata["page"][field] != page[field]:
                raise ValueError(f"Capture {field} does not match the crawl manifest")
        if (
            content_hash((folder / "page.html").read_text(encoding="utf-8"))
            != page["html_sha256"]
        ):
            raise ValueError("Captured HTML hash mismatch")
        rendered_hash = metadata["page"].get("link_html_sha256")
        if rendered_hash is not None:
            if (
                rendered_hash != page.get("link_html_sha256")
                or content_hash((folder / "link-page.html").read_text(encoding="utf-8"))
                != rendered_hash
            ):
                raise ValueError("Captured rendered HTML hash mismatch")
        folders.append(folder)
    if not folders:
        raise ValueError("Crawl contains no successfully captured pages")
    return manifest, folders
