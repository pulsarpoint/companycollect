#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import NamedTuple
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from lxml import html as lxml_html


JUSTICE_BASE_URL = "https://or.justice.cz"
JUSTICE_UI_BASE_URL = f"{JUSTICE_BASE_URL}/ias/ui/"
DEFAULT_OUTPUT_DIR = Path("data/czech_justice_pdf_probe")
DEFAULT_USER_AGENT = "corpscout-czech-justice-pdf-probe/0.1"
DEFAULT_TIMEOUT_SECONDS = 120
PDF_CHUNK_BYTES = 1024 * 1024

DEFAULT_SAMPLE_ICOS = (
    "27074358",  # Asseco Central Europe, a.s.
    "45274649",  # CEZ, a. s.
    "00177041",  # Skoda Auto a.s.
    "45317054",  # Komercni banka, a.s.
    "45244782",  # Ceska sporitelna, a.s.
    "60193336",  # O2 Czech Republic a.s.
    "26168685",  # Seznam.cz, a.s.
    "26185610",  # AGROFERT, a.s.
    "70994226",  # Ceske drahy, a.s.
    "64949681",  # T-Mobile Czech Republic a.s.
)

FINANCIAL_LABEL_TERMS = (
    "ucetni zaverka",
    "vyrocni zprava",
)


class FinancialDocument(NamedTuple):
    document_id: str
    year: str
    detail_url: str
    label: str


class DownloadedPdf(NamedTuple):
    ico: str
    year: str
    document_id: str
    file_path: Path
    size_bytes: int
    source_url: str
    reused: bool


def normalize_ico(value: str) -> str:
    digits = re.sub(r"\D", "", value)
    if not 1 <= len(digits) <= 8:
        raise ValueError(f"invalid Czech ICO: {value!r}")
    return digits.zfill(8)


def pdf_output_path(
    *,
    output_dir: Path,
    ico: str,
    year: str,
    document_id: str,
) -> Path:
    return (
        output_dir
        / f"ico_prefix={ico[:2]}"
        / f"ico={ico}"
        / f"year={year}"
        / f"document={document_id}.pdf"
    )


def extract_subjekt_id(html: str) -> str | None:
    match = re.search(r"subjektId=(\d+)", html)
    return match.group(1) if match is not None else None


def extract_financial_documents(html: str, *, base_url: str) -> list[FinancialDocument]:
    tree = lxml_html.fromstring(html)
    documents: list[FinancialDocument] = []
    seen_document_ids: set[str] = set()
    for link in tree.xpath("//a[@href]"):
        href = str(link.get("href") or "")
        if "vypis-sl-detail" not in href or "dokument=" not in href:
            continue
        detail_url = urljoin(base_url, href)
        document_id = _query_param(detail_url, "dokument")
        if document_id is None or document_id in seen_document_ids:
            continue
        label = _nearest_row_text(link)
        if not is_financial_document_label(label):
            continue
        seen_document_ids.add(document_id)
        documents.append(
            FinancialDocument(
                document_id=document_id,
                year=year_from_text(label),
                detail_url=detail_url,
                label=" ".join(label.split()),
            )
        )
    return sorted(documents, key=lambda item: (item.year, item.document_id), reverse=True)


def is_financial_document_label(value: str) -> bool:
    normalized = _ascii_lower(value)
    return any(term in normalized for term in FINANCIAL_LABEL_TERMS)


def year_from_text(value: str) -> str:
    bracket_match = re.search(r"\[(19\d{2}|20\d{2})\]", value)
    if bracket_match is not None:
        return bracket_match.group(1)
    match = re.search(r"\b(19\d{2}|20\d{2})\b", value)
    return match.group(1) if match is not None else "unknown"


def fetch_pdf_documents_for_ico(
    *,
    session: requests.Session,
    ico: str,
    output_dir: Path,
    max_documents_per_company: int,
    timeout_seconds: int,
    refresh: bool,
    request_delay_seconds: float,
) -> list[DownloadedPdf]:
    detail_url = f"{JUSTICE_UI_BASE_URL}rejstrik-$firma?ico={ico}"
    detail_html = _get_text(session, detail_url, timeout_seconds=timeout_seconds)
    subjekt_id = extract_subjekt_id(detail_html)
    if subjekt_id is None:
        print(f"{ico}: no subjektId found", file=sys.stderr)
        return []

    if request_delay_seconds > 0:
        time.sleep(request_delay_seconds)

    listing_url = f"{JUSTICE_UI_BASE_URL}vypis-sl-firma?subjektId={subjekt_id}"
    listing_html = _get_text(session, listing_url, timeout_seconds=timeout_seconds)
    documents = extract_financial_documents(listing_html, base_url=listing_url)
    if max_documents_per_company > 0:
        documents = documents[:max_documents_per_company]
    if not documents:
        print(f"{ico}: no financial PDFs discovered", file=sys.stderr)
        return []

    downloaded: list[DownloadedPdf] = []
    for document in documents:
        if request_delay_seconds > 0:
            time.sleep(request_delay_seconds)
        document_html = _get_text(
            session,
            document.detail_url,
            timeout_seconds=timeout_seconds,
        )
        download_url = extract_pdf_download_url(document_html, base_url=document.detail_url)
        if download_url is None:
            print(f"{ico}: document {document.document_id} has no PDF link", file=sys.stderr)
            continue

        target_path = pdf_output_path(
            output_dir=output_dir,
            ico=ico,
            year=document.year,
            document_id=document.document_id,
        )
        if target_path.exists() and not refresh:
            downloaded.append(
                DownloadedPdf(
                    ico=ico,
                    year=document.year,
                    document_id=document.document_id,
                    file_path=target_path,
                    size_bytes=target_path.stat().st_size,
                    source_url=download_url,
                    reused=True,
                )
            )
            continue

        if request_delay_seconds > 0:
            time.sleep(request_delay_seconds)
        try:
            size_bytes = download_pdf(
                session=session,
                url=download_url,
                target_path=target_path,
                timeout_seconds=timeout_seconds,
            )
        except ValueError as exc:
            print(
                f"{ico}: skipping document {document.document_id}: {exc}",
                file=sys.stderr,
            )
            continue
        downloaded.append(
            DownloadedPdf(
                ico=ico,
                year=document.year,
                document_id=document.document_id,
                file_path=target_path,
                size_bytes=size_bytes,
                source_url=download_url,
                reused=False,
            )
        )
    return downloaded


def extract_pdf_download_url(html: str, *, base_url: str) -> str | None:
    tree = lxml_html.fromstring(html)
    for link in tree.xpath("//a[@href]"):
        href = str(link.get("href") or "")
        if "/ias/content/download" in href or "content/download?id=" in href:
            return urljoin(base_url, href)
    return None


def download_pdf(
    *,
    session: requests.Session,
    url: str,
    target_path: Path,
    timeout_seconds: int,
) -> int:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    response = session.get(url, timeout=timeout_seconds, stream=True)
    response.raise_for_status()
    written = 0
    with tmp_path.open("wb") as out:
        for chunk in response.iter_content(chunk_size=PDF_CHUNK_BYTES):
            if not chunk:
                continue
            out.write(chunk)
            written += len(chunk)
    if tmp_path.read_bytes()[:4] != b"%PDF":
        tmp_path.unlink(missing_ok=True)
        raise ValueError(f"download did not produce a PDF: {url}")
    tmp_path.replace(target_path)
    return written


def write_summary(output_dir: Path, rows: list[DownloadedPdf]) -> Path:
    summary_path = output_dir / "summary.csv"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "ico",
                "year",
                "document_id",
                "size_bytes",
                "size_mb",
                "reused",
                "file_path",
                "source_url",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "ico": row.ico,
                    "year": row.year,
                    "document_id": row.document_id,
                    "size_bytes": row.size_bytes,
                    "size_mb": f"{row.size_bytes / 1024 / 1024:.2f}",
                    "reused": row.reused,
                    "file_path": row.file_path,
                    "source_url": row.source_url,
                }
            )
    return summary_path


def main() -> int:
    args = parse_args()
    icos = [normalize_ico(ico) for ico in (args.ico or DEFAULT_SAMPLE_ICOS)]
    icos = icos[: args.max_companies]
    session = requests.Session()
    session.headers["User-Agent"] = args.user_agent

    all_downloads: list[DownloadedPdf] = []
    for index, ico in enumerate(icos, start=1):
        print(f"[{index}/{len(icos)}] probing ICO {ico}")
        try:
            rows = fetch_pdf_documents_for_ico(
                session=session,
                ico=ico,
                output_dir=args.output_dir,
                max_documents_per_company=args.max_documents_per_company,
                timeout_seconds=args.timeout_seconds,
                refresh=args.refresh,
                request_delay_seconds=args.request_delay_seconds,
            )
        except Exception as exc:  # noqa: BLE001 - this is an exploratory probe
            print(f"{ico}: failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            continue
        for row in rows:
            action = "reused" if row.reused else "downloaded"
            print(
                f"  {action}: ico={row.ico} year={row.year} "
                f"document={row.document_id} size_mb={row.size_bytes / 1024 / 1024:.2f}"
            )
        all_downloads.extend(rows)

    summary_path = write_summary(args.output_dir, all_downloads)
    total_bytes = sum(row.size_bytes for row in all_downloads)
    print()
    print(f"documents: {len(all_downloads)}")
    print(f"total_mb: {total_bytes / 1024 / 1024:.2f}")
    if all_downloads:
        print(f"avg_mb: {total_bytes / len(all_downloads) / 1024 / 1024:.2f}")
    print(f"summary: {summary_path}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Probe Czech Justice Sbírka listin financial PDFs for a small ICO sample "
            "and report real downloaded sizes."
        )
    )
    parser.add_argument(
        "--ico",
        action="append",
        help="Czech ICO to probe. Can be repeated. Defaults to a 10-company sample.",
    )
    parser.add_argument(
        "--max-companies",
        type=int,
        default=10,
        help="Maximum number of ICOs to probe.",
    )
    parser.add_argument(
        "--max-documents-per-company",
        type=int,
        default=1,
        help="Maximum financial PDFs per company. Use 0 for all discovered documents.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Local output directory for downloaded PDFs and summary.csv.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout per request.",
    )
    parser.add_argument(
        "--request-delay-seconds",
        type=float,
        default=0.5,
        help="Delay between Justice requests.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-download PDFs that already exist locally.",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="HTTP User-Agent header.",
    )
    return parser.parse_args()


def _get_text(session: requests.Session, url: str, *, timeout_seconds: int) -> str:
    response = session.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    if response.encoding is None:
        response.encoding = "utf-8"
    return response.text


def _nearest_row_text(link: lxml_html.HtmlElement) -> str:
    rows = link.xpath("ancestor::tr[1]")
    if rows:
        return rows[0].text_content()
    return link.text_content()


def _query_param(url: str, name: str) -> str | None:
    values = parse_qs(urlparse(url).query).get(name)
    return values[0] if values else None


def _ascii_lower(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = "".join(char for char in normalized if not unicodedata.combining(char))
    return ascii_text.lower()


if __name__ == "__main__":
    raise SystemExit(main())
