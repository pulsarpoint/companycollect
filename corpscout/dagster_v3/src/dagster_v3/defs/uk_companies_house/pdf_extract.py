"""PoC: extract financials from PDF-only accounts via OCR + LLM.

Many Companies House filings are scanned-image PDFs (no XBRL). This pipeline:
  PDF -> page images (pdftoppm) -> OCR text (tesseract) -> LLM -> metric JSON.
Lower precision than XBRL (OCR noise, layout variety, model judgement), so results
are tagged source_slug='uk_companies_house_accounts_pdf' and carry an LLM confidence
— treat as a targeted/on-demand fallback, not a bulk-trusted source.
"""

import json
import logging
import re
import subprocess
import tempfile
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from openai import OpenAI

from dagster_v3.defs.uk_companies_house import tables

LOGGER = logging.getLogger(__name__)

METRIC_FIELDS = tables.UK_FINANCIAL_METRIC_NAMES
# qwen-style reasoning models: disable the thinking trace so we get clean JSON.
NO_THINK_EXTRA_BODY = {"chat_template_kwargs": {"enable_thinking": False}}
_SCALE = {"units": 1, "thousands": 1000, "millions": 1_000_000}


def ocr_pdf_bytes(pdf_bytes: bytes, *, max_pages: int = 12, dpi: int = 200) -> str:
    """Rasterize the first pages (pdftoppm) and OCR them (tesseract) into text."""
    with tempfile.TemporaryDirectory(prefix="uk_pdf_ocr_") as tmpdir:
        tmp = Path(tmpdir)
        pdf_path = tmp / "in.pdf"
        pdf_path.write_bytes(pdf_bytes)
        subprocess.run(
            ["pdftoppm", "-png", "-r", str(dpi), "-f", "1", "-l", str(max_pages),
             str(pdf_path), str(tmp / "pg")],
            check=True, capture_output=True,
        )
        texts: list[str] = []
        for image in sorted(tmp.glob("pg-*.png")):
            result = subprocess.run(
                ["tesseract", str(image), "stdout"], capture_output=True, text=True
            )
            texts.append(result.stdout)
    return "\n".join(texts)


def _parse_json_object(content: str) -> dict[str, Any] | None:
    text = content.strip()
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.S).strip()
    text = re.sub(r"```(?:json)?", "", text).strip()
    match = re.search(r"\{.*\}", text, re.S)
    if match is None:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


def _to_decimal(value: Any, scale_factor: int) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value)) * scale_factor
    except (InvalidOperation, ValueError, TypeError):
        return None


def extract_financials_from_text(
    text: str,
    *,
    base_url: str,
    model: str,
    api_key: str,
    timeout_seconds: int = 180,
    client: OpenAI | None = None,
) -> dict[str, Any] | None:
    """Ask the LLM to extract a metric JSON from OCR'd accounts text."""
    llm = client or OpenAI(base_url=base_url, api_key=api_key, max_retries=0)
    metric_keys = ", ".join(METRIC_FIELDS)
    prompt = (
        "/no_think\n"
        "You extract financial figures from OCR'd UK company accounts text. "
        "Return ONLY a JSON object (no thinking, no markdown) with keys: "
        "period_end_date (YYYY-MM-DD or null), currency (ISO 4217 code), "
        "unit_scale (one of: units, thousands, millions), confidence (0-1), and "
        f"these metric keys as a plain number or null: {metric_keys}. "
        "Use the statutory/reported figures for the most recent reporting year. "
        "No thousands separators.\n\nTEXT:\n" + text[:12000]
    )
    response = llm.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=800,
        extra_body=NO_THINK_EXTRA_BODY,
        timeout=timeout_seconds,
    )
    return _parse_json_object(response.choices[0].message.content or "")


def extract_pdf_financials(
    pdf_bytes: bytes,
    *,
    base_url: str,
    model: str,
    api_key: str,
    max_pages: int = 12,
    log: Callable[..., object] | None = None,
) -> dict[str, Any] | None:
    """OCR the PDF and extract normalized metrics (scale applied) + provenance."""
    text = ocr_pdf_bytes(pdf_bytes, max_pages=max_pages)
    if not text.strip():
        return None
    data = extract_financials_from_text(
        text, base_url=base_url, model=model, api_key=api_key
    )
    if not data:
        return None
    scale_factor = _SCALE.get(str(data.get("unit_scale", "")).lower(), 1)
    metrics = {m: _to_decimal(data.get(m), scale_factor) for m in METRIC_FIELDS}
    if all(v is None for v in metrics.values()):
        return None
    result = {
        "period_end_date": data.get("period_end_date"),
        "currency": (data.get("currency") or "GBP").upper(),
        "confidence": data.get("confidence"),
        "metrics": metrics,
    }
    if log is not None:
        log(
            "Extracted PDF financials: currency=%s confidence=%s non_null=%s",
            result["currency"],
            result["confidence"],
            sum(1 for v in metrics.values() if v is not None),
        )
    return result
