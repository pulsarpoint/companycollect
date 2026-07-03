# Czech OCR Benchmark

Benchmarks OCR engines on Czech Justice scanned financial PDFs.

The benchmark is intentionally separate from Dagster. It reads PDFs already
downloaded by `dagster_v3/scripts/czech_justice_pdf_probe.py`, runs OCR engines,
and writes JSON reports with runtime and rough text-quality signals.

## System Dependencies

For OCRmyPDF/Tesseract:

```bash
brew install qpdf tesseract-lang
```

The local machine already has `tesseract` and Ghostscript. `tesseract-lang` adds
the Czech `ces` language pack.

## Python Environment

PaddleOCR currently has better compatibility on Python 3.12 than Python 3.14, so
this package pins Python to `>=3.12,<3.13`.

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/czech_ocr_benchmark
uv sync --python 3.12
```

## Run

Use the 8 PDFs produced by the probe:

```bash
uv run czech-ocr-benchmark \
  --pdf-dir ../dagster_v3/data/czech_justice_pdf_probe \
  --output-dir data/results \
  --engines ocrmypdf,paddle \
  --max-pages 5
```

Use `--max-pages 0` to process all pages for engines that support page limiting.
OCRmyPDF always processes the full PDF.

The benchmark writes:

- `data/results/report.json` - aggregate report
- `data/results/ocrmypdf/*.txt` - OCRmyPDF sidecar text
- `data/results/paddle/*.txt` - PaddleOCR text

## Quality Signals

The report counts occurrences of Czech financial labels such as:

- `aktiva`
- `vlastní kapitál`
- `tržby`
- `výnosy`
- `výsledek hospodaření`
- `závazky`

This is not final metric extraction. It is only enough to compare OCR engine
speed and whether the extracted text is useful for later JSON extraction.
