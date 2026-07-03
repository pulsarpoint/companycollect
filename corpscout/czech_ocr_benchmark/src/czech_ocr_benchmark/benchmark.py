from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
import time
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pypdfium2 as pdfium


CZECH_FINANCIAL_TERMS = (
    "aktiva",
    "aktiva celkem",
    "vlastní kapitál",
    "tržby",
    "výnosy",
    "výsledek hospodaření",
    "závazky",
    "cizí zdroje",
    "oběžná aktiva",
    "dlouhodobý majetek",
)

OCR_LANGUAGES = "ces+eng"
DEFAULT_MAX_PAGES = 5


@dataclass(frozen=True)
class OcrRunResult:
    engine: str
    pdf_path: Path
    text_path: Path | None
    status: str
    runtime_seconds: float
    pages_processed: int | None
    error_message: str
    score: dict[str, Any]


def discover_pdf_files(pdf_dir: Path) -> list[Path]:
    return sorted(path for path in pdf_dir.rglob("*.pdf") if path.is_file())


def metadata_from_pdf_path(path: Path) -> dict[str, str]:
    parts = dict(_key_value_part(part) for part in path.parts if "=" in part)
    document_id = path.stem.removeprefix("document=")
    return {
        "ico": parts.get("ico", ""),
        "ico_prefix": parts.get("ico_prefix", ""),
        "year": parts.get("year", ""),
        "document_id": document_id,
    }


def score_ocr_text(text: str) -> dict[str, Any]:
    normalized = _normalized_text(text)
    term_hits = {
        term: len(re.findall(re.escape(_normalized_text(term)), normalized))
        for term in CZECH_FINANCIAL_TERMS
    }
    return {
        "char_count": len(text),
        "line_count": len([line for line in text.splitlines() if line.strip()]),
        "numeric_token_count": len(re.findall(r"\b\d[\d\s.,]*\b", text)),
        "financial_term_hits": term_hits,
        "financial_term_total": sum(term_hits.values()),
    }


def run_benchmark(
    *,
    pdf_dir: Path,
    output_dir: Path,
    engines: Iterable[str],
    max_pages: int,
) -> dict[str, Any]:
    pdf_paths = discover_pdf_files(pdf_dir)
    if not pdf_paths:
        raise ValueError(f"no PDF files found under {pdf_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    engine_names = [engine.strip() for engine in engines if engine.strip()]
    results: list[dict[str, Any]] = []
    for pdf_path in pdf_paths:
        for engine in engine_names:
            if engine == "ocrmypdf":
                result = run_ocrmypdf(pdf_path=pdf_path, output_dir=output_dir)
            elif engine == "paddle":
                result = run_paddleocr(
                    pdf_path=pdf_path,
                    output_dir=output_dir,
                    max_pages=max_pages,
                )
            else:
                raise ValueError(f"unknown OCR engine: {engine}")
            results.append(_result_row(result))
            _write_json(output_dir / "latest-result.json", results[-1])

    report = {
        "pdf_dir": str(pdf_dir),
        "output_dir": str(output_dir),
        "pdf_count": len(pdf_paths),
        "engines": engine_names,
        "max_pages": max_pages,
        "results": results,
        "summary": summarize_results(results),
    }
    _write_json(output_dir / "report.json", report)
    return report


def run_ocrmypdf(*, pdf_path: Path, output_dir: Path) -> OcrRunResult:
    engine_dir = output_dir / "ocrmypdf"
    engine_dir.mkdir(parents=True, exist_ok=True)
    text_path = engine_dir / f"{_pdf_slug(pdf_path)}.txt"
    if shutil.which("ocrmypdf") is None:
        return _missing_engine_result(
            engine="ocrmypdf",
            pdf_path=pdf_path,
            text_path=text_path,
            error_message="ocrmypdf executable is not on PATH",
        )

    start = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="czech_ocrmypdf_") as tmp_dir:
        output_pdf = Path(tmp_dir) / "ocr.pdf"
        command = [
            "ocrmypdf",
            "--force-ocr",
            "--skip-big",
            "200",
            "--jobs",
            "1",
            "--sidecar",
            str(text_path),
            "-l",
            OCR_LANGUAGES,
            str(pdf_path),
            str(output_pdf),
        ]
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
    runtime = time.perf_counter() - start
    if completed.returncode != 0:
        return OcrRunResult(
            engine="ocrmypdf",
            pdf_path=pdf_path,
            text_path=text_path,
            status="failed",
            runtime_seconds=runtime,
            pages_processed=None,
            error_message=(completed.stderr or completed.stdout).strip(),
            score=score_ocr_text(""),
        )
    text = text_path.read_text(encoding="utf-8") if text_path.exists() else ""
    return OcrRunResult(
        engine="ocrmypdf",
        pdf_path=pdf_path,
        text_path=text_path,
        status="success",
        runtime_seconds=runtime,
        pages_processed=None,
        error_message="",
        score=score_ocr_text(text),
    )


def run_paddleocr(*, pdf_path: Path, output_dir: Path, max_pages: int) -> OcrRunResult:
    engine_dir = output_dir / "paddle"
    engine_dir.mkdir(parents=True, exist_ok=True)
    text_path = engine_dir / f"{_pdf_slug(pdf_path)}.txt"
    start = time.perf_counter()
    try:
        from paddleocr import PaddleOCR
    except Exception as exc:  # noqa: BLE001 - benchmark should report unavailable engines
        return _missing_engine_result(
            engine="paddle",
            pdf_path=pdf_path,
            text_path=text_path,
            error_message=f"PaddleOCR import failed: {type(exc).__name__}: {exc}",
        )

    try:
        ocr = _paddle_ocr_instance(PaddleOCR)
        page_texts = []
        for page_path in render_pdf_pages(pdf_path=pdf_path, max_pages=max_pages):
            page_texts.append(_paddle_text_for_image(ocr, page_path))
        text = "\n".join(page_texts)
        text_path.write_text(text, encoding="utf-8")
        runtime = time.perf_counter() - start
        return OcrRunResult(
            engine="paddle",
            pdf_path=pdf_path,
            text_path=text_path,
            status="success",
            runtime_seconds=runtime,
            pages_processed=len(page_texts),
            error_message="",
            score=score_ocr_text(text),
        )
    except Exception as exc:  # noqa: BLE001 - benchmark result should keep going
        runtime = time.perf_counter() - start
        return OcrRunResult(
            engine="paddle",
            pdf_path=pdf_path,
            text_path=text_path,
            status="failed",
            runtime_seconds=runtime,
            pages_processed=None,
            error_message=f"{type(exc).__name__}: {exc}",
            score=score_ocr_text(""),
        )


def render_pdf_pages(*, pdf_path: Path, max_pages: int) -> list[Path]:
    page_paths: list[Path] = []
    temp_dir = Path(tempfile.mkdtemp(prefix="czech_paddle_pages_"))
    document = pdfium.PdfDocument(pdf_path)
    page_count = len(document)
    limit = page_count if max_pages == 0 else min(page_count, max_pages)
    for index in range(limit):
        page = document[index]
        bitmap = page.render(scale=2)
        image = bitmap.to_pil()
        page_path = temp_dir / f"page-{index + 1:04d}.png"
        image.save(page_path)
        page_paths.append(page_path)
    return page_paths


def summarize_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for engine in sorted({str(result["engine"]) for result in results}):
        engine_results = [result for result in results if result["engine"] == engine]
        success_results = [result for result in engine_results if result["status"] == "success"]
        runtime = sum(float(result["runtime_seconds"]) for result in engine_results)
        summary[engine] = {
            "documents": len(engine_results),
            "success": len(success_results),
            "failed": len(engine_results) - len(success_results),
            "runtime_seconds": round(runtime, 3),
            "avg_runtime_seconds": round(runtime / len(engine_results), 3),
            "total_chars": sum(int(result["score"]["char_count"]) for result in success_results),
            "total_financial_term_hits": sum(
                int(result["score"]["financial_term_total"]) for result in success_results
            ),
            "total_numeric_tokens": sum(
                int(result["score"]["numeric_token_count"]) for result in success_results
            ),
        }
    return summary


def _paddle_ocr_instance(paddle_ocr_class: Any) -> Any:
    try:
        return paddle_ocr_class(
            lang="latin",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=False,
        )
    except ValueError:
        return paddle_ocr_class(lang="latin")


def _paddle_text_for_image(ocr: Any, image_path: Path) -> str:
    if hasattr(ocr, "predict"):
        result = ocr.predict(str(image_path))
    else:
        result = ocr.ocr(str(image_path), cls=False)
    strings = list(_extract_strings(result))
    return "\n".join(strings)


def _extract_strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            yield cleaned
        return
    if isinstance(value, dict):
        for key in ("rec_texts", "texts"):
            item = value.get(key)
            if isinstance(item, list):
                for text in item:
                    yield from _extract_strings(text)
        for item in value.values():
            yield from _extract_strings(item)
        return
    if isinstance(value, list | tuple):
        for item in value:
            yield from _extract_strings(item)
        return
    json_method = getattr(value, "json", None)
    if callable(json_method):
        yield from _extract_strings(json_method())


def _result_row(result: OcrRunResult) -> dict[str, Any]:
    metadata = metadata_from_pdf_path(result.pdf_path)
    return {
        **metadata,
        "engine": result.engine,
        "pdf_path": str(result.pdf_path),
        "text_path": str(result.text_path) if result.text_path is not None else "",
        "status": result.status,
        "runtime_seconds": round(result.runtime_seconds, 3),
        "pages_processed": result.pages_processed,
        "error_message": result.error_message,
        "score": result.score,
    }


def _missing_engine_result(
    *,
    engine: str,
    pdf_path: Path,
    text_path: Path,
    error_message: str,
) -> OcrRunResult:
    return OcrRunResult(
        engine=engine,
        pdf_path=pdf_path,
        text_path=text_path,
        status="unavailable",
        runtime_seconds=0,
        pages_processed=None,
        error_message=error_message,
        score=score_ocr_text(""),
    )


def _write_json(path: Path, body: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(body, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _key_value_part(value: str) -> tuple[str, str]:
    key, part_value = value.split("=", 1)
    return key, part_value


def _normalized_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    without_marks = "".join(char for char in normalized if not unicodedata.combining(char))
    return without_marks.lower()


def _pdf_slug(pdf_path: Path) -> str:
    metadata = metadata_from_pdf_path(pdf_path)
    return (
        f"ico={metadata['ico']}_year={metadata['year']}_"
        f"document={metadata['document_id']}"
    )
