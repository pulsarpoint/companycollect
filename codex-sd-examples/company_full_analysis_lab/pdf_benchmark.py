"""Compare the existing Dagster PDF extractor with Docling on archived sources."""

import argparse
import hashlib
import json
import logging
import platform
import runpy
import subprocess
import threading
import time
from importlib.metadata import version
from io import BytesIO
from pathlib import Path
from typing import Any

import psutil
import pymupdf

LOGGER = logging.getLogger(__name__)

SAMPLES = (
    ("acquisition", "sona_ar_2425_full.pdf", 196, "native"),
    ("net_assets", "sona_ar_2526_full.pdf", 177, "native"),
    ("aoc1", "sona_ar_2526_full.pdf", 184, "native"),
    ("scan_balance", "novelic-serbia-document-wMpyMY.pdf", 1, "sideways_scan"),
    ("scan_income", "novelic-serbia-document-wMpyMY.pdf", 2, "sideways_scan"),
    ("scan_cashflow", "novelic-serbia-document-wMpyMY.pdf", 3, "scan"),
)
SOURCE_URLS = {
    "sona_ar_2425_full.pdf": "https://sonacomstar.com/annual-report-24-25/assets/pdf/Sona-Comstar-AR-24-25.pdf",
    "sona_ar_2526_full.pdf": "https://sonacomstar.com/annual-report-25-26/assets/pdf/Sona-Comstar-AR-25-26.pdf",
    "novelic-serbia-document-wMpyMY.pdf": "https://sonacomstar.com/files/documents/novelic-serbia-document-wMpyMY.pdf",
}


def sample_memory(stop: threading.Event, values: list[int]) -> None:
    """Sample this process and live OCR children; includes resident shared pages."""
    process = psutil.Process()
    while not stop.is_set():
        total = process.memory_info().rss
        for child in process.children(recursive=True):
            try:
                total += child.memory_info().rss
            except psutil.NoSuchProcess:
                continue
        values.append(total)
        stop.wait(0.05)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--engine", choices=("dagster", "docling", "docling-rapidocr"), required=True
    )
    parser.add_argument("--sources", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", nargs="*")
    parser.add_argument("--no-cell-matching", action="store_true")
    parser.add_argument("--psm", type=int, default=4)
    parser.add_argument(
        "--rotate",
        type=int,
        default=0,
        help="Diagnostic clockwise page rotation; source stays unchanged",
    )
    args = parser.parse_args()
    output = args.output.resolve() / (
        args.engine
        + (f"-rotate{args.rotate}" if args.rotate else "")
        + ("-no-cell-matching" if args.no_cell_matching else "")
        + (f"-psm{args.psm}" if args.psm != 4 else "")
    )
    output.mkdir(parents=True, exist_ok=False)
    baseline_path = (
        Path(__file__).resolve().parents[2]
        / "corpscout/services/dagster_v3/src/dagster_v3/defs/"
        "norway_brreg_financial/annual_account_pdf.py"
    )
    baseline = runpy.run_path(str(baseline_path)) if args.engine == "dagster" else None
    if baseline is not None and args.psm != 4:
        baseline["tesseract_ocr_image"].__globals__["OCR_PAGE_SEGMENTATION_MODE"] = (
            args.psm
        )
        baseline["extract_annual_account_pdf"].__globals__[
            "OCR_PAGE_SEGMENTATION_MODE"
        ] = args.psm
    metadata: dict[str, Any] = {
        "engine": args.engine,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "pymupdf": version("pymupdf"),
        "tesseract": subprocess.run(
            ["tesseract", "--version"], check=True, capture_output=True, text=True
        ).stdout.splitlines()[0],
        "ocr_languages": ["en"]
        if args.engine == "docling-rapidocr"
        else ["nor", "eng"],
        "ocr_psm": None if args.engine == "docling-rapidocr" else args.psm,
        "diagnostic_rotation_degrees": args.rotate,
        "memory_method": "Peak sampled RSS of process plus children, 50ms; includes imports/models",
        "sources_archived_locally": True,
        "rustfs_upload_performed": False,
        "samples": [],
    }
    converter = None
    started = time.perf_counter()
    if args.engine != "dagster":
        # Load the heavyweight pipeline only in its own benchmark process.
        from docling.datamodel.accelerator_options import AcceleratorOptions
        from docling.datamodel.base_models import InputFormat
        from docling.datamodel.pipeline_options import (
            PdfPipelineOptions,
            RapidOcrOptions,
            TableFormerMode,
            TableStructureOptions,
            TesseractCliOcrOptions,
        )
        from docling.document_converter import DocumentConverter, PdfFormatOption

        options = PdfPipelineOptions(
            do_ocr=True,
            do_table_structure=True,
            table_structure_options=TableStructureOptions(
                mode=TableFormerMode.ACCURATE,
                do_cell_matching=not args.no_cell_matching,
            ),
            enable_remote_services=False,
            accelerator_options=AcceleratorOptions(device="cpu", num_threads=4),
            ocr_options=(
                RapidOcrOptions(lang=["en"], backend="onnxruntime", use_cls=True)
                if args.engine == "docling-rapidocr"
                else TesseractCliOcrOptions(
                    lang=["nor", "eng"], psm=args.psm, scale=200 / 72
                )
            ),
        )
        converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=options)}
        )
        print("Initializing Docling and downloading local models if absent", flush=True)
        converter.initialize_pipeline(InputFormat.PDF)
        metadata["docling"] = version("docling")
        metadata["pipeline_options"] = options.model_dump(
            mode="json", serialize_as_any=True
        )
    else:
        metadata["baseline_source_path"] = str(baseline_path)
        metadata["baseline_source_sha256"] = hashlib.sha256(
            baseline_path.read_bytes()
        ).hexdigest()
    metadata["initialization_seconds"] = round(time.perf_counter() - started, 3)
    (output / "run.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    for sample_id, filename, page_number, kind in SAMPLES:
        if args.samples is not None and sample_id not in args.samples:
            continue
        directory = output / sample_id
        directory.mkdir()
        original_path = (args.sources / filename).resolve()
        original = original_path.read_bytes()
        # Both engines receive the same single-page PDF bytes. This is an in-memory
        # benchmark selection, not a replacement for the original archived document.
        with (
            pymupdf.open(stream=original, filetype="pdf") as source,
            pymupdf.open() as selected,
        ):
            selected.insert_pdf(
                source, from_page=page_number - 1, to_page=page_number - 1
            )
            if args.rotate:
                selected[0].set_rotation((selected[0].rotation + args.rotate) % 360)
                pixmap = selected[0].get_pixmap(dpi=200)
                with pymupdf.open() as upright:
                    page = upright.new_page(
                        width=selected[0].rect.width, height=selected[0].rect.height
                    )
                    page.insert_image(page.rect, stream=pixmap.tobytes("png"))
                    body = upright.tobytes(no_new_id=True)
            else:
                body = selected.tobytes(no_new_id=True)
            selected[0].get_pixmap(dpi=150).save(directory / "page.png")
        record: dict[str, Any] = {
            "sample": sample_id,
            "kind": kind,
            "original_path": str(original_path),
            "original_url": SOURCE_URLS[filename],
            "original_sha256": hashlib.sha256(original).hexdigest(),
            "original_pdf_page": page_number,
            "selection_pdf_page": 1,
            "selected_sha256": hashlib.sha256(body).hexdigest(),
        }
        memory: list[int] = []
        stop = threading.Event()
        monitor = threading.Thread(
            target=sample_memory, args=(stop, memory), daemon=True
        )
        monitor.start()
        started = time.perf_counter()
        print(f"Starting {args.engine}: {sample_id}", flush=True)
        try:
            if converter is not None:
                from docling_core.types.doc import ContentLayer
                from docling_core.types.io import DocumentStream

                result = converter.convert(
                    DocumentStream(name=f"{sample_id}.pdf", stream=BytesIO(body))
                )
                document = result.document
                payload = document.export_to_dict()
                text = document.export_to_markdown(
                    included_content_layers=set(ContentLayer)
                )
                for index, table in enumerate(document.tables):
                    (directory / f"table-{index}.html").write_text(
                        table.export_to_html(doc=document), encoding="utf-8"
                    )
                record.update(
                    status=str(result.status),
                    table_count=len(document.tables),
                    errors=[str(e) for e in result.errors],
                )
                (directory / "confidence.json").write_text(
                    result.confidence.model_dump_json(indent=2), encoding="utf-8"
                )
            else:
                assert baseline is not None
                payload = baseline["extract_annual_account_pdf"](
                    body,
                    org_number="benchmark",
                    legal_name="NOVELIC / Sona report sample",
                    filing_year=2026,
                    source_file_name=filename,
                    source_pdf_url=SOURCE_URLS[filename],
                    source_run_id="pdf-parser-benchmark",
                    retrieved_at="",
                    ocr_image=baseline["tesseract_ocr_image"],
                )
                text = "\n\n".join(page["text"] for page in payload["pages"])
                record.update(
                    status="completed",
                    table_count=0,
                    native_text_pages=payload["native_text_page_count"],
                    ocr_pages=payload["ocr_page_count"],
                )
            (directory / "document.json").write_text(
                json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            (directory / "document.md").write_text(text, encoding="utf-8")
            record["output_characters"] = len(text)
        except Exception as error:
            # Keep other independent samples running, but retain every failure.
            LOGGER.exception("Extraction failed for %s", sample_id)
            record.update(status="failed", error=f"{type(error).__name__}: {error}")
        finally:
            record["seconds"] = round(time.perf_counter() - started, 3)
            stop.set()
            monitor.join()
            record["peak_rss_mib"] = round(max(memory, default=0) / 1024**2, 1)
        metadata["samples"].append(record)
        (output / "run.json").write_text(
            json.dumps(metadata, indent=2), encoding="utf-8"
        )
        print(json.dumps(record), flush=True)


if __name__ == "__main__":
    main()
