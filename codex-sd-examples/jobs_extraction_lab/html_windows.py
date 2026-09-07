"""Lossless source ranges for generic, overlapping HTML extraction windows."""

import json
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import click

from jobs_extraction_lab.corpus import content_hash, utc_now, write_json
from jobs_extraction_lab.extract import normalize_job_url
from jobs_extraction_lab.validated_html import page_link_urls


@dataclass
class HtmlSpan:
    tag: str
    start: int
    end: int
    children: list["HtmlSpan"] = field(default_factory=list)


class HtmlSourceSpans(HTMLParser):
    """Track original character offsets without serializing or editing the DOM."""

    def __init__(self, html: str) -> None:
        super().__init__(convert_charrefs=False)
        self.html = html
        self.line_starts = [0] + [m.end() for m in re.finditer("\n", html)]
        self.root = HtmlSpan("document", 0, len(html))
        self.stack = [self.root]
        self.headings: list[HtmlSpan] = []
        self.links: list[tuple[int, str]] = []
        self.feed(html)
        self.close()

    def source_offset(self) -> int:
        line, column = self.getpos()
        return self.line_starts[line - 1] + column

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        start = self.source_offset()
        node = HtmlSpan(tag, start, len(self.html))
        self.stack[-1].children.append(node)
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value is not None:
                    self.links.append((start, value))
        if re.fullmatch(r"h[1-6]", tag) and not any(
            n.tag in {"a", "tr"} for n in self.stack
        ):
            self.headings.append(node)
        if tag in {
            "area",
            "base",
            "br",
            "col",
            "embed",
            "hr",
            "img",
            "input",
            "link",
            "meta",
            "param",
            "source",
            "track",
            "wbr",
        }:
            tag_text = self.get_starttag_text()
            assert tag_text is not None
            node.end = start + len(tag_text)
        else:
            self.stack.append(node)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if self.stack[-1].start == self.source_offset() and self.stack[-1].tag == tag:
            tag_text = self.get_starttag_text()
            assert tag_text is not None
            self.stack.pop().end = self.source_offset() + len(tag_text)

    def handle_endtag(self, tag: str) -> None:
        end = self.html.find(">", self.source_offset()) + 1
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                for node in self.stack[index:]:
                    node.end = end
                del self.stack[index:]
                break


def source_atoms(node: HtmlSpan, max_chars: int) -> list[tuple[int, int]]:
    if node.end - node.start <= max_chars or node.tag in {
        "a",
        "tr",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
    }:
        return [(node.start, node.end)]
    spans = []
    cursor = node.start
    for child in node.children:
        if child.start < cursor or child.end > node.end:
            raise ValueError("Overlapping HTML source spans; cannot safely segment")
        if cursor < child.start:
            spans.append((cursor, child.start))
        spans.extend(source_atoms(child, max_chars))
        cursor = child.end
    if cursor < node.end:
        spans.append((cursor, node.end))
    return spans


def split_html(
    html: str, source_url: str, *, core_chars: int, atom_chars: int, overlap_chars: int
) -> list[dict[str, Any]]:
    if not 0 < atom_chars <= core_chars or not 0 <= overlap_chars < core_chars:
        raise ValueError(
            "Require 0 < atom size <= core size and 0 <= overlap < core size"
        )
    parsed = HtmlSourceSpans(html)
    atoms = source_atoms(parsed.root, atom_chars)
    if "".join(html[start:end] for start, end in atoms) != html:
        raise ValueError("Segmentation did not preserve the complete source")
    units = []
    index = 0
    while index < len(atoms):
        stop = index + 1
        while stop < len(atoms) and atoms[stop][1] - atoms[index][0] <= core_chars:
            stop += 1
        start, end = atoms[index][0], atoms[stop - 1][1]
        overlap = index
        while overlap > 0 and start - atoms[overlap][0] < overlap_chars:
            overlap -= 1
        physical_start = atoms[overlap][0]
        context: dict[int, HtmlSpan] = {}
        for heading in parsed.headings:
            if heading.end > physical_start:
                continue
            level = int(heading.tag[1])
            context = {rank: value for rank, value in context.items() if rank < level}
            context[level] = heading
        heading_spans = []
        context_chars = 0
        for heading in reversed(list(context.values())):
            if context_chars + heading.end - heading.start <= 1500:
                heading_spans.insert(0, (heading.start, heading.end))
                context_chars += heading.end - heading.start
        prefix = "\n".join(html[a:b] for a, b in heading_spans)
        content = (prefix + "\n" if prefix else "") + html[physical_start:end]
        links = page_link_urls(content, source_url)
        core_links = set()
        for position, href in parsed.links:
            if start <= position < end:
                try:
                    url = normalize_job_url(urljoin(source_url, href), source_url)
                except ValueError:
                    continue
                if url in links:
                    core_links.add(url)
        units.append(
            {
                "core_start": start,
                "core_end": end,
                "source_start": physical_start,
                "context_spans": heading_spans,
                "core_links": sorted(core_links),
                "source_overlap_chars": start - physical_start,
                "content": content,
            }
        )
        index = stop
    return units


def prepare_windows(
    data_dir: Path,
    output_dir: Path,
    *,
    core_chars: int,
    atom_chars: int,
    overlap_chars: int,
) -> dict[str, Any]:
    if output_dir.exists():
        raise ValueError("Choose a new output directory to preserve frozen inputs")
    source_text = (data_dir / "manifest.json").read_text(encoding="utf-8")
    source = json.loads(source_text)
    pages = []
    units = []
    files = {}
    for page in source["inputs"]:
        html = (data_dir / page["file"]).read_text(encoding="utf-8")
        if (
            page["format"] != "crawl4ai_cleaned_html"
            or content_hash(html) != page["sha256"]
        ):
            raise ValueError(f"Native HTML changed: {page['page_id']}")
        pages.append(
            {
                "page_id": page["page_id"],
                "source_url": page["source_url"],
                "source_file": page["file"],
                "source_sha256": page["sha256"],
                "source_chars": len(html),
            }
        )
        variants: dict[str, list[dict[str, Any]]] = {
            "full": [
                {
                    "core_start": 0,
                    "core_end": len(html),
                    "source_start": 0,
                    "context_spans": [],
                    "core_links": sorted(page_link_urls(html, page["source_url"])),
                    "source_overlap_chars": 0,
                    "content": html,
                }
            ],
            "chunked": split_html(
                html,
                page["source_url"],
                core_chars=core_chars,
                atom_chars=atom_chars,
                overlap_chars=overlap_chars,
            ),
        }
        for variant, fragments in variants.items():
            for index, fragment in enumerate(fragments):
                content = fragment.pop("content")
                unit_id = f"{page['page_id']}-{variant}-{index:03d}"
                filename = f"inputs/{unit_id}.html"
                files[filename] = content
                units.append(
                    {
                        **fragment,
                        "unit_id": unit_id,
                        "page_id": page["page_id"],
                        "unit_index": index,
                        "variant": variant,
                        "format": "crawl4ai_cleaned_html"
                        if variant == "full"
                        else "crawl4ai_html_fragment",
                        "source_url": page["source_url"],
                        "file": filename,
                        "sha256": content_hash(content),
                        "chars": len(content),
                        "bytes": len(content.encode("utf-8")),
                    }
                )
    manifest = {
        "created_at": utc_now(),
        "source_dir": str(data_dir.resolve()),
        "source_manifest_sha256": content_hash(source_text),
        "core_chars": core_chars,
        "atom_chars": atom_chars,
        "overlap_chars": overlap_chars,
        "context_max_chars": 1500,
        "policy": "Lossless source-range cores; whole anchors, table rows and headings; generic subtree boundaries; preceding non-anchor headings; no CSS selectors or reference labels",
        "pages": pages,
        "units": units,
    }
    for filename, content in files.items():
        path = output_dir / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    write_json(output_dir / "manifest.json", manifest)
    return manifest


@click.command()
@click.option("--data-dir", type=click.Path(path_type=Path, exists=True), required=True)
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
@click.option("--core-chars", type=click.IntRange(min=1), default=6000)
@click.option("--atom-chars", type=click.IntRange(min=1), default=2000)
@click.option("--overlap-chars", type=click.IntRange(min=0), default=1200)
def main(
    data_dir: Path,
    output_dir: Path,
    core_chars: int,
    atom_chars: int,
    overlap_chars: int,
) -> None:
    manifest = prepare_windows(
        data_dir,
        output_dir,
        core_chars=core_chars,
        atom_chars=atom_chars,
        overlap_chars=overlap_chars,
    )
    click.echo(
        f"Prepared {len(manifest['pages'])} pages and {len(manifest['units'])} full-page/chunk units"
    )


if __name__ == "__main__":
    main()
