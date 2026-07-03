from __future__ import annotations

import base64
import json
import re
import shutil
import subprocess
import tempfile
import time
import unicodedata
import urllib.error
import urllib.request
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
DEFAULT_GLM_VLLM_API_URL = "http://localhost:8000/v1/chat/completions"
DEFAULT_GLM_VLLM_MODEL = "glm-ocr"
DEFAULT_GLM_VLLM_MAX_TOKENS = 8192
DEFAULT_GLM_VLLM_PROMPT = "Text Recognition:"
DEFAULT_GLM_VLLM_TIMEOUT_SECONDS = 300


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
    page_start: int = 1,
    glm_layout_device: str = "cpu",
    glm_config_path: Path | None = None,
    glm_api_url: str | None = None,
    glm_vllm_api_url: str = DEFAULT_GLM_VLLM_API_URL,
    glm_vllm_model: str = DEFAULT_GLM_VLLM_MODEL,
    glm_vllm_max_tokens: int = DEFAULT_GLM_VLLM_MAX_TOKENS,
    glm_vllm_prompt: str = DEFAULT_GLM_VLLM_PROMPT,
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
                    page_start=page_start,
                )
            elif engine == "glm":
                result = run_glmocr(
                    pdf_path=pdf_path,
                    output_dir=output_dir,
                    max_pages=max_pages,
                    page_start=page_start,
                    layout_device=glm_layout_device,
                    config_path=glm_config_path,
                    api_url=glm_api_url,
                )
            elif engine == "glm-vllm":
                result = run_glm_vllm(
                    pdf_path=pdf_path,
                    output_dir=output_dir,
                    max_pages=max_pages,
                    page_start=page_start,
                    api_url=glm_vllm_api_url,
                    model=glm_vllm_model,
                    max_tokens=glm_vllm_max_tokens,
                    prompt=glm_vllm_prompt,
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
        "page_start": page_start,
        "glm_layout_device": glm_layout_device,
        "glm_config_path": str(glm_config_path) if glm_config_path is not None else "",
        "glm_api_url": glm_api_url or "",
        "glm_vllm_api_url": glm_vllm_api_url,
        "glm_vllm_model": glm_vllm_model,
        "glm_vllm_max_tokens": glm_vllm_max_tokens,
        "glm_vllm_prompt": glm_vllm_prompt,
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


def run_paddleocr(
    *,
    pdf_path: Path,
    output_dir: Path,
    max_pages: int,
    page_start: int = 1,
) -> OcrRunResult:
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
        with tempfile.TemporaryDirectory(prefix="czech_paddle_pages_") as temp_dir:
            for page_path in render_pdf_pages(
                pdf_path=pdf_path,
                max_pages=max_pages,
                page_start=page_start,
                output_dir=Path(temp_dir),
            ):
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


def run_glmocr(
    *,
    pdf_path: Path,
    output_dir: Path,
    max_pages: int,
    page_start: int = 1,
    layout_device: str = "cpu",
    config_path: Path | None = None,
    api_url: str | None = None,
) -> OcrRunResult:
    engine_dir = output_dir / "glm"
    engine_dir.mkdir(parents=True, exist_ok=True)
    text_path = engine_dir / f"{_pdf_slug(pdf_path)}.txt"
    start = time.perf_counter()
    try:
        from glmocr import GlmOcr
    except Exception as exc:  # noqa: BLE001 - benchmark should report unavailable engines
        return _missing_engine_result(
            engine="glm",
            pdf_path=pdf_path,
            text_path=text_path,
            error_message=f"glmocr import failed: {type(exc).__name__}: {exc}",
        )

    try:
        with tempfile.TemporaryDirectory(prefix="czech_glm_pages_") as temp_dir:
            page_paths = render_pdf_pages(
                pdf_path=pdf_path,
                max_pages=max_pages,
                page_start=page_start,
                output_dir=Path(temp_dir),
            )
            if not page_paths:
                raise ValueError("selected page window contains no pages")
            with GlmOcr(
                config_path=str(config_path) if config_path is not None else None,
                api_url=api_url,
                layout_device=layout_device,
            ) as parser:
                result = parser.parse([str(page_path) for page_path in page_paths])
        text = _glmocr_result_text(result)
        text_path.write_text(text, encoding="utf-8")
        _write_glmocr_json_sidecar(text_path.with_suffix(".json"), result)
        runtime = time.perf_counter() - start
        return OcrRunResult(
            engine="glm",
            pdf_path=pdf_path,
            text_path=text_path,
            status="success",
            runtime_seconds=runtime,
            pages_processed=len(page_paths),
            error_message="",
            score=score_ocr_text(text),
        )
    except Exception as exc:  # noqa: BLE001 - benchmark result should keep going
        runtime = time.perf_counter() - start
        return OcrRunResult(
            engine="glm",
            pdf_path=pdf_path,
            text_path=text_path,
            status="failed",
            runtime_seconds=runtime,
            pages_processed=None,
            error_message=f"{type(exc).__name__}: {exc}",
            score=score_ocr_text(""),
        )


def run_glm_vllm(
    *,
    pdf_path: Path,
    output_dir: Path,
    max_pages: int,
    page_start: int = 1,
    api_url: str = DEFAULT_GLM_VLLM_API_URL,
    model: str = DEFAULT_GLM_VLLM_MODEL,
    max_tokens: int = DEFAULT_GLM_VLLM_MAX_TOKENS,
    prompt: str = DEFAULT_GLM_VLLM_PROMPT,
) -> OcrRunResult:
    engine_dir = output_dir / "glm-vllm"
    engine_dir.mkdir(parents=True, exist_ok=True)
    text_path = engine_dir / f"{_pdf_slug(pdf_path)}.txt"
    sidecar_path = text_path.with_suffix(".json")
    start = time.perf_counter()
    try:
        with tempfile.TemporaryDirectory(prefix="czech_glm_vllm_pages_") as temp_dir:
            page_paths = render_pdf_pages(
                pdf_path=pdf_path,
                max_pages=max_pages,
                page_start=page_start,
                output_dir=Path(temp_dir),
            )
            if not page_paths:
                raise ValueError("selected page window contains no pages")
            responses = [
                _post_glm_vllm_payload(
                    api_url=api_url,
                    payload=_glm_vllm_payload(
                        image_path=page_path,
                        model=model,
                        max_tokens=max_tokens,
                        prompt=prompt,
                    ),
                )
                for page_path in page_paths
            ]
        text = "\n\n".join(_glm_vllm_response_text(response) for response in responses)
        text_path.write_text(text, encoding="utf-8")
        _write_json(sidecar_path, {"responses": responses})
        runtime = time.perf_counter() - start
        return OcrRunResult(
            engine="glm-vllm",
            pdf_path=pdf_path,
            text_path=text_path,
            status="success",
            runtime_seconds=runtime,
            pages_processed=len(responses),
            error_message="",
            score=score_ocr_text(text),
        )
    except Exception as exc:  # noqa: BLE001 - benchmark result should keep going
        runtime = time.perf_counter() - start
        return OcrRunResult(
            engine="glm-vllm",
            pdf_path=pdf_path,
            text_path=text_path,
            status="failed",
            runtime_seconds=runtime,
            pages_processed=None,
            error_message=f"{type(exc).__name__}: {exc}",
            score=score_ocr_text(""),
        )


def render_pdf_pages(
    *,
    pdf_path: Path,
    max_pages: int,
    page_start: int,
    output_dir: Path,
) -> list[Path]:
    page_paths: list[Path] = []
    document = pdfium.PdfDocument(pdf_path)
    for index in selected_page_indices(
        page_count=len(document),
        page_start=page_start,
        max_pages=max_pages,
    ):
        page = document[index]
        bitmap = page.render(scale=2)
        image = bitmap.to_pil()
        page_path = output_dir / f"page-{index + 1:04d}.png"
        image.save(page_path)
        page_paths.append(page_path)
    return page_paths


def selected_page_indices(*, page_count: int, page_start: int, max_pages: int) -> list[int]:
    if page_start < 1:
        raise ValueError("page_start is one-based and must be >= 1")
    start_index = page_start - 1
    if start_index >= page_count:
        return []
    end_index = page_count if max_pages == 0 else min(page_count, start_index + max_pages)
    return list(range(start_index, end_index))


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
            lang="cs",
            ocr_version="PP-OCRv6",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=False,
        )
    except ValueError:
        return paddle_ocr_class(lang="cs")


def _paddle_text_for_image(ocr: Any, image_path: Path) -> str:
    if hasattr(ocr, "predict"):
        result = ocr.predict(str(image_path))
    else:
        result = ocr.ocr(str(image_path), cls=False)
    strings = list(_extract_strings(result))
    return "\n".join(strings)


def _glmocr_result_text(result: Any) -> str:
    for value in _preferred_glmocr_text_values(result):
        if isinstance(value, str) and value.strip():
            return value
        strings = list(_extract_strings(value))
        if strings:
            return "\n".join(strings)
    strings = list(_extract_strings(result))
    if strings:
        return "\n".join(strings)
    return str(result)


def _preferred_glmocr_text_values(result: Any) -> Iterable[Any]:
    if isinstance(result, dict):
        for key in ("markdown", "md", "text", "content", "json_result"):
            if key in result:
                yield result[key]
        return
    for attr in ("markdown", "md", "text", "content", "json_result"):
        yield getattr(result, attr, None)


def _write_glmocr_json_sidecar(path: Path, result: Any) -> None:
    payload = _glmocr_json_payload(result)
    if payload is None:
        return
    try:
        _write_json(path, payload)
    except TypeError:
        return


def _glmocr_json_payload(result: Any) -> Any | None:
    if isinstance(result, dict | list):
        return result
    model_dump = getattr(result, "model_dump", None)
    if callable(model_dump):
        return model_dump()
    json_result = getattr(result, "json_result", None)
    if json_result is not None:
        return json_result
    return None


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


def _glm_vllm_payload(
    *,
    image_path: Path,
    model: str,
    max_tokens: int,
    prompt: str,
) -> dict[str, Any]:
    encoded_image = base64.b64encode(image_path.read_bytes()).decode("ascii")
    return {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{encoded_image}",
                        },
                    },
                ],
            }
        ],
        "max_tokens": max_tokens,
        "temperature": 0.0,
        "top_p": 0.00001,
    }


def _post_glm_vllm_payload(*, api_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        api_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(  # noqa: S310 - user supplies benchmark endpoint explicitly
            request,
            timeout=DEFAULT_GLM_VLLM_TIMEOUT_SECONDS,
        ) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"GLM vLLM request failed: HTTP {exc.code} {exc.reason}: {body}"
        ) from exc


def _glm_vllm_response_text(response: dict[str, Any]) -> str:
    choices = response.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError("GLM vLLM response does not contain choices")
    first_choice = choices[0]
    if not isinstance(first_choice, dict):
        raise ValueError("GLM vLLM first choice is not an object")
    message = first_choice.get("message")
    if not isinstance(message, dict):
        raise ValueError("GLM vLLM first choice does not contain message")
    content = message.get("content")
    if not isinstance(content, str):
        raise ValueError("GLM vLLM message content is not a string")
    return content.strip()


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
