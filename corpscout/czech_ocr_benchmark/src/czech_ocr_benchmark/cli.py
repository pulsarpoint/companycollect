from __future__ import annotations

import argparse
import json
from pathlib import Path

from czech_ocr_benchmark.benchmark import DEFAULT_MAX_PAGES, run_benchmark


def main() -> int:
    args = parse_args()
    engines = args.engines.split(",")
    report = run_benchmark(
        pdf_dir=args.pdf_dir,
        output_dir=args.output_dir,
        engines=engines,
        max_pages=args.max_pages,
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
        help="Comma-separated engines: ocrmypdf,paddle.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help="Max pages per PDF for PaddleOCR. Use 0 for all pages. OCRmyPDF processes full PDFs.",
    )
    return parser.parse_args()
