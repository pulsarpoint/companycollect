from __future__ import annotations

import argparse
import json
from pathlib import Path

from czech_ocr_benchmark.benchmark import (
    DEFAULT_GLM_VLLM_API_URL,
    DEFAULT_GLM_VLLM_MAX_TOKENS,
    DEFAULT_GLM_VLLM_MODEL,
    DEFAULT_GLM_VLLM_PROMPT,
    DEFAULT_MAX_PAGES,
    run_benchmark,
)


def main() -> int:
    args = parse_args()
    engines = args.engines.split(",")
    report = run_benchmark(
        pdf_dir=args.pdf_dir,
        output_dir=args.output_dir,
        engines=engines,
        max_pages=args.max_pages,
        page_start=args.page_start,
        glm_layout_device=args.glm_layout_device,
        glm_config_path=args.glm_config,
        glm_api_url=args.glm_api_url,
        glm_vllm_api_url=args.glm_vllm_api_url,
        glm_vllm_model=args.glm_vllm_model,
        glm_vllm_max_tokens=args.glm_vllm_max_tokens,
        glm_vllm_prompt=args.glm_vllm_prompt,
    )
    print(json.dumps(report["summary"], ensure_ascii=False, indent=2, sort_keys=True))
    print(f"report: {args.output_dir / 'report.json'}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark OCR on Czech Justice PDFs.")
    parser.add_argument(
        "--pdf-dir",
        type=Path,
        default=Path("../dagster_v3/data/czech_justice_pdf_probe"),
        help="Directory containing probe PDFs.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/results"),
        help="Directory for text outputs and report.json.",
    )
    parser.add_argument(
        "--engines",
        default="ocrmypdf,paddle",
        help="Comma-separated engines: ocrmypdf,paddle,glm,glm-vllm.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help="Max pages per PDF for PaddleOCR. Use 0 for all pages. OCRmyPDF processes full PDFs.",
    )
    parser.add_argument(
        "--page-start",
        type=int,
        default=1,
        help="One-based first page for image-based engine page rendering.",
    )
    parser.add_argument(
        "--glm-layout-device",
        default="cpu",
        help="GLM-OCR layout device, for example cpu or cuda:0.",
    )
    parser.add_argument(
        "--glm-config",
        type=Path,
        default=None,
        help="Optional GLM-OCR config.yaml path.",
    )
    parser.add_argument(
        "--glm-api-url",
        default=None,
        help="Optional GLM-OCR MaaS-compatible API URL.",
    )
    parser.add_argument(
        "--glm-vllm-api-url",
        default=DEFAULT_GLM_VLLM_API_URL,
        help="OpenAI-compatible GLM-OCR vLLM chat completions endpoint.",
    )
    parser.add_argument(
        "--glm-vllm-model",
        default=DEFAULT_GLM_VLLM_MODEL,
        help="Model name served by the GLM-OCR vLLM endpoint.",
    )
    parser.add_argument(
        "--glm-vllm-max-tokens",
        type=int,
        default=DEFAULT_GLM_VLLM_MAX_TOKENS,
        help="Maximum output tokens for each GLM-OCR vLLM page request.",
    )
    parser.add_argument(
        "--glm-vllm-prompt",
        default=DEFAULT_GLM_VLLM_PROMPT,
        help="Prompt sent with each rendered page to the GLM-OCR vLLM endpoint.",
    )
    return parser.parse_args()
