"""Preserve job-card boundaries from the three platforms in the frozen corpus."""

import json
import re
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import click
from bs4 import BeautifulSoup

from jobs_extraction_lab.corpus import content_hash, load_pages, utc_now, write_json
from jobs_extraction_lab.extract import normalize_job_url, normalize_text
from jobs_extraction_lab.models import Page

FORMATS = ("original_markdown", "clean_markdown", "clean_html")

# Previously reviewed failure cases; other pages use their middle nonempty window.
REVIEW_TITLES = {
    "ashby-resend": "Security Engineer, Platform",
    "greenhouse-webflow": "Corporate Account Executive - West",
    "greenhouse-algolia": "Senior GTM Data Operations Manager",
    "ashby-granola": "AI Engineer",
    "ashby-encord": "Special Projects, GTM",
    "lever-finn": "Senior Product Manager",
    "ashby-railway": "Senior Full-Stack Engineer",
}


@dataclass(frozen=True)
class JobCard:
    url: str
    headings: tuple[str, ...]
    title: str
    badges: tuple[str, ...]
    body: tuple[str, ...]


def read_job_cards(page: Page, html: str) -> list[JobCard]:
    """Use observed platform markup; fail if any collected job URL is lost."""
    soup = BeautifulSoup(html, "html.parser")
    known = {normalize_job_url(url, page.final_url) for url in page.job_links}
    cards = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        url = urljoin(page.final_url, str(anchor["href"]))
        key = normalize_job_url(url, page.final_url)
        if key not in known or key in seen:
            continue
        if page.platform == "ashby":
            title = anchor.select_one(".ashby-job-posting-brief-title")
            details = anchor.select(".ashby-job-posting-brief-details p")
            heading = anchor.find_previous("h2", class_="ashby-department-heading")
            levels = (
                heading.select(".ashby-department-heading-level") if heading else []
            )
            headings = tuple(node.get_text(" ", strip=True) for node in levels)
            if not headings and heading is not None:
                headings = (heading.get_text(" ", strip=True),)
            badges: tuple[str, ...] = ()
        elif page.platform == "greenhouse":
            title = anchor.select_one("p.body--medium")
            details = anchor.select("p.body--metadata")
            heading = anchor.find_previous("h3")
            if page.id == "greenhouse-circleci":
                title = anchor.find("h3")
                details = anchor.select("p.type-p")
                heading = anchor.find_previous("h2")
            headings = (heading.get_text(" ", strip=True),) if heading else ()
            badges = tuple(
                node.get_text(" ", strip=True) for node in anchor.select(".tag-text")
            )
        elif page.platform == "lever":
            title = anchor.select_one("[data-qa='posting-name']")
            details = anchor.select(".posting-categories > span")
            group = anchor.find_parent("div", class_="postings-group")
            if group is None:
                raise ValueError(f"Missing Lever group: {url}")
            department = group.select_one(".large-category-header")
            if department is None:
                department = group.find_previous("div", class_="large-category-header")
            team = group.select_one(".posting-category-title")
            headings = tuple(
                node.get_text(" ", strip=True)
                for node in (department, team)
                if node is not None
            )
            badges = ()
        else:
            raise ValueError(f"Unsupported platform: {page.platform}")
        if title is None or not details:
            raise ValueError(f"Unrecognized job card: {url}")
        title_copy = BeautifulSoup(str(title), "html.parser")
        for badge in title_copy.select(".tag-container"):
            badge.decompose()
        title_text = title_copy.get_text(" ", strip=True)
        if not title_text:
            raise ValueError(f"Empty title: {url}")
        cards.append(
            JobCard(
                url,
                headings,
                title_text,
                badges,
                tuple(node.get_text(" ", strip=True) for node in details),
            )
        )
        seen.add(key)
    if seen != known:
        raise ValueError(
            f"HTML card extraction lost {len(known - seen)} collected URLs on {page.id}"
        )
    return cards


def render_cards(cards: list[JobCard], *, html: bool) -> str:
    """Render the same source blocks in either syntax, without inferring fields."""
    sections = []
    for card in cards:
        if html:
            nodes = (
                [f"<h2>{escape(' > '.join(card.headings))}</h2>"]
                if card.headings
                else []
            )
            nodes.append(f"<h3>{escape(card.title)}</h3>")
            nodes.extend(f"<aside>{escape(badge)}</aside>" for badge in card.badges)
            nodes.extend(f"<p>{escape(text)}</p>" for text in card.body)
            nodes.append(f'<a href="{escape(card.url, quote=True)}">Job link</a>')
            sections.append("<article>\n" + "\n".join(nodes) + "\n</article>")
        else:
            nodes = ["## " + " > ".join(card.headings)] if card.headings else []
            nodes.append("### " + card.title)
            nodes.extend("> " + badge for badge in card.badges)
            nodes.extend(card.body)
            nodes.append(f"[Job link]({card.url})")
            sections.append("\n\n".join(nodes))
    return "\n\n".join(sections) + "\n"


def prepare_formats(
    source_dir: Path, window_dir: Path, output_dir: Path
) -> dict[str, Any]:
    if output_dir.exists():
        raise ValueError(
            "Use a new output directory to preserve frozen experiment inputs"
        )
    windows = load_pages(window_dir)
    inputs = []
    audit: list[dict[str, Any]] = []
    files: dict[str, str] = {}
    for page in load_pages(source_dir):
        original = (source_dir / page.markdown_file).read_text(encoding="utf-8")
        if content_hash(original) != page.markdown_sha256:
            raise ValueError(f"Markdown changed: {page.id}")
        html = (source_dir / page.html_file).read_text(encoding="utf-8")
        cards = read_job_cards(page, html)
        choices = [
            w for w in windows if w.id.startswith(page.id + "--") and w.job_links
        ]
        if not choices:
            raise ValueError(f"No saved windows for {page.id}")
        target = REVIEW_TITLES.get(page.id)
        if target is not None:
            target_urls = {card.url for card in cards if target in card.title}
            window = max(choices, key=lambda w: len(set(w.job_links) & target_urls))
            if not set(window.job_links) & target_urls:
                raise ValueError(f"Review target not found: {page.id}")
        else:
            window = choices[len(choices) // 2]
        selected_urls = {
            normalize_job_url(url, page.final_url) for url in window.job_links
        }
        selected = [
            card
            for card in cards
            if normalize_job_url(card.url, page.final_url) in selected_urls
        ]
        if len(selected) != len(selected_urls):
            raise ValueError(f"Selected card mismatch: {page.id}")
        legacy = (window_dir / window.markdown_file).read_text(encoding="utf-8")
        if content_hash(legacy) != window.markdown_sha256:
            raise ValueError(f"Window changed: {window.id}")
        soup = BeautifulSoup(html, "html.parser")
        prose = [
            node.get_text(" ", strip=True)
            for node in soup.select("div[class*='Description_'] p")
            if node.get_text(strip=True)
        ]
        visible_legacy = normalize_text(re.sub(r"[\\*_]", "", legacy))
        selected_prose = [
            text for text in prose if normalize_text(text) in visible_legacy
        ]
        texts = {
            "original_markdown": legacy,
            "clean_markdown": render_cards(selected, html=False)
            + "\n\n".join(selected_prose),
            "clean_html": render_cards(selected, html=True)
            + "\n".join(f"<p>{escape(text)}</p>" for text in selected_prose),
        }
        for name, text in texts.items():
            for card in selected:
                if name != "original_markdown" and normalize_text(
                    card.title
                ) not in normalize_text(
                    BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
                    if name == "clean_html"
                    else text
                ):
                    raise ValueError(f"Title lost in {name}: {card.url}")
            filename = (
                f"inputs/{name}/{page.id}.{'html' if name == 'clean_html' else 'md'}"
            )
            files[filename] = text
            inputs.append(
                {
                    "page_id": page.id,
                    "window_id": window.id,
                    "source_url": page.final_url,
                    "platform": page.platform,
                    "format": name,
                    "file": filename,
                    "sha256": content_hash(text),
                    "chars": len(text),
                    "job_links": [c.url for c in selected],
                }
            )
        for name, text in (
            ("original_markdown", original),
            ("clean_markdown", render_cards(cards, html=False) + "\n\n".join(prose)),
            (
                "clean_html",
                render_cards(cards, html=True)
                + "\n".join(f"<p>{escape(text)}</p>" for text in prose),
            ),
        ):
            files[
                f"full-pages/{name}/{page.id}.{'html' if name == 'clean_html' else 'md'}"
            ] = text
        audit.append(
            {
                "page_id": page.id,
                "source_html_sha256": content_hash(html),
                "source_markdown_sha256": page.markdown_sha256,
                "observed_job_links": len(page.job_links),
                "prepared_cards": len(cards),
                "selected_window": window.id,
                "selection": "previously reviewed failure case"
                if target
                else "middle nonempty window",
                "preserved_prose": selected_prose,
                "selected_cards": [
                    {
                        "job_url": c.url,
                        "title": c.title,
                        "headings": c.headings,
                        "badges": c.badges,
                        "body": c.body,
                    }
                    for c in selected
                ],
            }
        )
    metadata = {
        "created_at": utc_now(),
        "source_dir": str(source_dir.resolve()),
        "window_dir": str(window_dir.resolve()),
        "formats": FORMATS,
        "pages": len(audit),
        "total_prepared_cards": sum(p["prepared_cards"] for p in audit),
        "selected_cards": sum(len(p["selected_cards"]) for p in audit),
        "comparison": "One matched group per page. Original Markdown is unchanged. Clean Markdown and HTML share identical source blocks and restored full headings. This compares preprocessing pipelines; clean Markdown versus clean HTML isolates serialization more closely.",
        "inputs": inputs,
        "source_audit": audit,
    }
    for filename, text in files.items():
        path = output_dir / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    write_json(output_dir / "manifest.json", metadata)
    return metadata


@click.command()
@click.option(
    "--source-dir",
    type=click.Path(path_type=Path),
    default=Path(__file__).parent / "data",
)
@click.option(
    "--window-dir",
    type=click.Path(path_type=Path),
    default=Path(__file__).parent / "data/segmented-v1",
)
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
def main(source_dir: Path, window_dir: Path, output_dir: Path) -> None:
    result = prepare_formats(source_dir, window_dir, output_dir)
    click.echo(
        json.dumps(
            {k: v for k, v in result.items() if k not in {"inputs", "source_audit"}},
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
