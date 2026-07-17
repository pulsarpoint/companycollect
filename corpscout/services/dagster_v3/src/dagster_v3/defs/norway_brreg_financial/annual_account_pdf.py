from __future__ import annotations

import csv
from collections.abc import Callable
import hashlib
from io import StringIO
import subprocess
from typing import Any

import pymupdf

OCR_LANGUAGES = "nor+eng"
OCR_PAGE_SEGMENTATION_MODE = 4
OCR_TIMEOUT_SECONDS = 300
RENDER_DPI = 200
NATIVE_TEXT_MIN_CHARACTERS = 20


def extract_annual_account_pdf(
    pdf_body: bytes,
    *,
    org_number: str,
    legal_name: str,
    filing_year: int,
    source_pdf_url: str,
    source_run_id: str,
    retrieved_at: str,
    ocr_image: Callable[[bytes], str],
) -> dict[str, Any]:
    """Extract native text or OCR word geometry from one BRREG annual-account PDF."""
    pages: list[dict[str, Any]] = []
    native_text_page_count = 0
    ocr_page_count = 0
    with pymupdf.open(stream=pdf_body, filetype="pdf") as document:
        for page_number, page in enumerate(document, start=1):
            native_words = page.get_text("words", sort=True)
            native_text = page.get_text("text", sort=True).strip()
            if _has_usable_native_text(native_text):
                pages.append(
                    _native_text_page(
                        page=page,
                        page_number=page_number,
                        text=native_text,
                        words=native_words,
                    )
                )
                native_text_page_count += 1
                continue

            image_body, image_width, image_height = _page_image(document, page)
            pages.append(
                _ocr_page(
                    page_number=page_number,
                    image_width=image_width,
                    image_height=image_height,
                    tsv=ocr_image(image_body),
                )
            )
            ocr_page_count += 1

    return {
        "schema_version": 1,
        "document_id": f"no-brreg-annual-account:{org_number}:{filing_year}",
        "country_iso2": "NO",
        "source_system": "brreg_annual_accounts_copy",
        "source_run_id": source_run_id,
        "org_number": org_number,
        "legal_name": legal_name,
        "filing_year": filing_year,
        "source_pdf_url": source_pdf_url,
        "source_pdf_sha256": hashlib.sha256(pdf_body).hexdigest(),
        "source_pdf_size_bytes": len(pdf_body),
        "retrieved_at": retrieved_at,
        "pdf_page_count": len(pages),
        "native_text_page_count": native_text_page_count,
        "ocr_page_count": ocr_page_count,
        "extraction": {
            "pdf_engine": "pymupdf",
            "pdf_engine_version": pymupdf.VersionBind,
            "ocr_engine": "tesseract",
            "ocr_languages": OCR_LANGUAGES,
            "ocr_page_segmentation_mode": OCR_PAGE_SEGMENTATION_MODE,
            "bbox_coordinate_space": "normalized_page",
        },
        "pages": pages,
    }


def tesseract_ocr_image(image_body: bytes) -> str:
    """Return Tesseract TSV for one image supplied through standard input."""
    try:
        result = subprocess.run(
            [
                "tesseract",
                "stdin",
                "stdout",
                "-l",
                OCR_LANGUAGES,
                "--psm",
                str(OCR_PAGE_SEGMENTATION_MODE),
                "tsv",
            ],
            input=image_body,
            check=True,
            capture_output=True,
            timeout=OCR_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as error:
        raise RuntimeError(
            "Tesseract is required for Norway BRREG annual-account OCR"
        ) from error
    except subprocess.CalledProcessError as error:
        stderr = error.stderr.decode("utf-8", "replace").strip()
        raise RuntimeError(f"Tesseract OCR failed: {stderr}") from error
    return result.stdout.decode("utf-8", "replace")


def _has_usable_native_text(text: str) -> bool:
    return len("".join(text.split())) >= NATIVE_TEXT_MIN_CHARACTERS


def _native_text_page(
    *,
    page: pymupdf.Page,
    page_number: int,
    text: str,
    words: list[tuple[Any, ...]],
) -> dict[str, Any]:
    page_width = float(page.rect.width)
    page_height = float(page.rect.height)
    normalized_words = [
        {
            "text": str(word[4]),
            "bbox": _normalized_bbox(
                left=float(word[0]),
                top=float(word[1]),
                right=float(word[2]),
                bottom=float(word[3]),
                page_width=page_width,
                page_height=page_height,
            ),
            "confidence": 100.0,
            "block_number": int(word[5]),
            "paragraph_number": 0,
            "line_number": int(word[6]),
            "word_number": int(word[7]),
        }
        for word in words
        if str(word[4]).strip() != ""
    ]
    return {
        "page_number": page_number,
        "extraction_method": "native_text",
        "width": page_width,
        "height": page_height,
        "text": text,
        "mean_word_confidence": 100.0 if normalized_words else None,
        "words": normalized_words,
    }


def _page_image(
    document: pymupdf.Document,
    page: pymupdf.Page,
) -> tuple[bytes, int, int]:
    for image in page.get_images(full=True):
        xref = int(image[0])
        if not _image_covers_page(page, xref):
            continue
        extracted = document.extract_image(xref)
        image_body = extracted.get("image")
        if isinstance(image_body, bytes):
            return image_body, int(extracted["width"]), int(extracted["height"])

    pixmap = page.get_pixmap(
        dpi=RENDER_DPI,
        colorspace=pymupdf.csGRAY,
        alpha=False,
    )
    return pixmap.tobytes("png"), pixmap.width, pixmap.height


def _image_covers_page(page: pymupdf.Page, xref: int) -> bool:
    page_area = float(page.rect.width * page.rect.height)
    if page_area <= 0:
        return False
    return any(
        float(rect.width * rect.height) / page_area >= 0.95
        for rect in page.get_image_rects(xref)
    )


def _ocr_page(
    *,
    page_number: int,
    image_width: int,
    image_height: int,
    tsv: str,
) -> dict[str, Any]:
    if image_width <= 0 or image_height <= 0:
        raise RuntimeError(f"Invalid OCR image dimensions on page {page_number}")
    rows = list(csv.DictReader(StringIO(tsv), delimiter="\t"))
    words: list[dict[str, Any]] = []
    lines: dict[tuple[int, int, int], list[str]] = {}
    for row in rows:
        text = str(row.get("text", "")).strip()
        if row.get("level") != "5" or text == "":
            continue
        confidence = float(row["conf"])
        block_number = int(row["block_num"])
        paragraph_number = int(row["par_num"])
        line_number = int(row["line_num"])
        words.append(
            {
                "text": text,
                "bbox": _normalized_bbox(
                    left=float(row["left"]),
                    top=float(row["top"]),
                    right=float(row["left"]) + float(row["width"]),
                    bottom=float(row["top"]) + float(row["height"]),
                    page_width=float(image_width),
                    page_height=float(image_height),
                ),
                "confidence": confidence,
                "block_number": block_number,
                "paragraph_number": paragraph_number,
                "line_number": line_number,
                "word_number": int(row["word_num"]),
            }
        )
        lines.setdefault(
            (block_number, paragraph_number, line_number), []
        ).append(text)

    confidences = [word["confidence"] for word in words if word["confidence"] >= 0]
    return {
        "page_number": page_number,
        "extraction_method": "tesseract_ocr",
        "width": image_width,
        "height": image_height,
        "text": "\n".join(" ".join(line_words) for line_words in lines.values()),
        "mean_word_confidence": (
            round(sum(confidences) / len(confidences), 2) if confidences else None
        ),
        "words": words,
    }


def _normalized_bbox(
    *,
    left: float,
    top: float,
    right: float,
    bottom: float,
    page_width: float,
    page_height: float,
) -> list[float]:
    return [
        round(left / page_width, 6),
        round(top / page_height, 6),
        round(right / page_width, 6),
        round(bottom / page_height, 6),
    ]
