# Local PDF extraction: Dagster/Tesseract and Docling

Run date: 2026-09-07. This is a targeted extraction benchmark on six pages from
three previously downloaded NOVELIC/Sona PDFs, with additional diagnostic runs.
It is not a general OCR accuracy estimate or a new DeepSeek company-analysis run.

**Docling improves document structure substantially, but neither tested OCR
backend produces consistently reliable financial figures on these scans.**
The acquisition/revenue and net-assets tables benefit immediately. Dense scans
need orientation handling, better text recognition and validation; a successful
conversion status does not establish that the output is usable.

## Existing code and RustFS

Dagster uses `ObjectStoreResource` in
[common/resources.py](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3/src/dagster_v3/defs/common/resources.py:50):
`CORPSCOUT_S3_ENDPOINT`, `CORPSCOUT_S3_ACCESS_KEY`, `CORPSCOUT_S3_SECRET_KEY`,
region `us-east-1`, and boto3 path-style addressing. The infrastructure README
documents `http://rustfs:9000` as the internal S3 API; port 9001 is the console.
The PDF service should use this existing connection contract and its own bucket.

The closest extraction code is
[Norway annual_account_pdf.py](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3/src/dagster_v3/defs/norway_brreg_financial/annual_account_pdf.py:19).
It reads native text and word coordinates with PyMuPDF, falling back to local
Tesseract when a page has fewer than 20 non-whitespace characters. It records
word geometry, OCR confidence, source URL, hash and extraction settings.
The pipeline downloads to RustFS first, verifies hashes, and stores extracted
JSON. Norway's downstream financial parser additionally interprets word geometry
using source-specific rules; that country-specific interpretation is outside
this benchmark.

Two existing choices must change for the research service:

- Norway has a PDF cleanup step after JSON verification. Reports require the
  original PDFs to remain available, so do not apply this cleanup to evidence.
- The UK PDF proof of concept flattens OCR to text, defaults to 12 pages, sends
  12,000 characters to an LLM and caps its answer at 800 tokens. Those limits and
  its missing OCR return-code check are unsuitable for full-document research.

No Dagster production code or infrastructure was changed. This benchmark used
the existing local PDF cache; it did not upload anything to RustFS or read its
credentials. Archive upload remains part of the proposed service, not this
parser-only experiment.

## Method

- macOS arm64, 36 GiB physical RAM; Python 3.12.12, PyMuPDF 1.28.2,
  Tesseract 5.5.1, Docling 2.126.0. Dependencies were installed in a separate
  ignored virtual environment; all versions are recorded in the run artifacts.
- Baseline: load the existing Norway extractor directly without importing
  Dagster's resources or launching assets. Preserve its `nor+eng`, PSM 4 and
  page-image/native-text decisions.
- Main Docling comparison: local Tesseract with the same languages and PSM 4,
  OCR scale corresponding to 200 DPI, TableFormer accurate mode, cell matching
  enabled, CPU with four threads. Remote document processing is disabled.
- The same selected PDF bytes are supplied to both extractors; original hashes,
  selected-byte hashes and original physical page positions are recorded.
  Whole two-page spreads remain intact. Original PDF files are unchanged.
- Both pipelines can retain native text. Rendering/region selection differs:
  Norway prefers an embedded full-page image; Docling locates OCR regions and
  reconstructs layout. This compares pipelines, not just OCR recognition models.
- Docling output includes structured JSON, table HTML, Markdown with all content
  layers, confidence reports and logs. Baseline JSON retains its word geometry.
- Times are one measured pass per page, after pipeline initialization, including
  extraction and output serialization. Reading the original, preparing the
  selection and making the review preview are outside the timed section.
  Peak memory samples process-plus-child RSS every 50 ms; it is approximate and
  includes loaded models, with possible shared-page double counting.

## Main six-page comparison

| Source page | Existing extractor | Docling + Tesseract | What the output shows |
|---|---:|---:|---|
| FY2025 annual report, PDF 196, printed 388–389 | 0.05 s | 5.97 s | Docling separates acquisition payments and revenue into their own tables; baseline text interleaves material from both printed pages. |
| FY2026 annual report, PDF 177, printed 350–351 | 0.05 s | 14.44 s | Docling preserves the nested net-assets/profit headers, separate years and NOVELIC row values. |
| FY2026 annual report, PDF 184, printed 364–365 | 0.07 s | 12.66 s | Docling still merges adjacent AOC-1 fields into individual cells; baseline has the values but no reconstructed table. |
| FY2026 NOVELIC scan, PDF 1: balance matrix sideways | 0.77 s | 3.21 s | Both outputs are unusable for reliable extraction. |
| FY2026 NOVELIC scan, PDF 2: income matrix sideways | 0.95 s | 2.93 s | Both outputs are unusable; Docling emits only 67 Markdown characters despite success status. |
| FY2026 NOVELIC scan, PDF 3: upright cash flow | 1.31 s | 4.27 s | Docling preserves more usable rows and closing cash, but still misreads a financing total. |
| **Total** | **3.19 s** | **43.47 s** | Extraction only, excluding initialization. |

Maximum sampled RSS: **277 MiB baseline**, **1,384 MiB Docling**. Initial Docling
setup including model initialization and any required downloads took **52.89 s**;
subsequent diagnostic processes initialized cached models in roughly **5.9–6.4 s**.
These figures are a local smoke benchmark, not production throughput estimates.

## Checks against the source pages

The source pages were rendered and visually inspected. Examples below describe
specific checked facts; they are not a percentage accuracy score.

1. **Acquisition versus revenue:** Docling table 1 associates `Cash paid to
   Founders` with `2,109.62`; table 2 associates `Total Revenue` with `484.31`.
   It retains the million-INR unit and the revenue period of 6 September 2023
   through 31 March 2024 in surrounding text. This directly addresses the
   earlier company report's table confusion.
2. **Net assets versus profit:** Docling's FY2026 table retains NOVELIC's
   `2.39%` and `1,464.32` under net-assets headers, with `(186.35)` under profit
   and loss. The previous year's `2.52%`, `1,423.84` and `7.29` remain in a
   separate table. It does not label the percentage as revenue.
3. **AOC-1 merged fields:** the original NOVELIC column contains turnover
   `755.77`, profit before tax `(186.23)`, taxation `-`, and profit after tax
   `(186.23)`. Docling transposes the table but places `755.77 (186.23)` in one
   cell and `- (186.23)` in another. Share capital and reserves also merge.
   A downstream model might infer the intended fields, but the parser has not
   produced unambiguous cells.
4. **Cash-flow closing balance:** the source reads `76.642` in thousands of RSD.
   The baseline OCR emits `176642`; Docling + Tesseract retains `76.642`.
5. **Cash-flow financing total:** the source reads `182.354`. Docling +
   Tesseract emits `182384`; the baseline omits the amount in its text output.
   The OCR confidence report still grades the Docling page as good.
6. **Signs matter:** the cash-flow source places some minus signs separately
   from the amounts. Docling + Tesseract retains some signs in a separate column;
   a consumer must keep that column, not read only the rightmost numeric cell.

The original report URLs and archived-local PDF hashes are in every run record.
The previously broken annual-report directory links were not reused as sources.

## Follow-up diagnostics

These were selected after observing failures; keep them separate from the main
six-page comparison.

**Turning off Docling cell matching did not fix AOC-1.** It produced 14 columns
instead of 13 in the transposed table, but dropped NOVELIC's `755.77` turnover
from the table. This option must not become a global default based on this case.

**Rotation alone did not fix the dense scans with Tesseract PSM 4.** Both engines
received the same manually selected 90-degree correction, rasterized at 200 DPI
and embedded in a new in-memory PDF. The source archive was untouched. Their
output remained poor. This diagnostic is not an implemented automatic
orientation detector. A direct Tesseract OSD probe on the income preview also
failed with “Too few characters.”

**Sparse-text PSM 11 recovered more numbers, but not reliable headers.** After
the same rotation, the baseline emitted 1,223/1,090 characters for balance/income;
Docling produced 36×13 and 20×14 tables. The income revenue amounts became
readable, but the revenue label and several company/period headers were missing
or damaged. More output and more detected cells do not establish correctness.

**Docling + RapidOCR was a useful additional check.** This uses local ONNX Runtime
and an English recognizer, so it is a different OCR configuration, not a repeat
of the controlled Tesseract comparison. On the original sideways scans it still
produced badly mixed text and columns. After the same rotation it reconstructed
substantially more readable balance and income matrices (37×14 and 20×17 cells).
It preserved the income table's company names, reporting periods and RSD/kEUR
distinction much better, but inserted an extra digit in the consolidated revenue
cell: `903.3698` instead of `903.698`.

On the upright cash-flow page RapidOCR recovered `182.354` and `76.642`, but
turned investing cash `9.090` into `060.6` and lost some standalone minus signs.
The alternatives make different mistakes; neither should be trusted merely
because the other failed. The corrected scan pair took 26.70 s with Docling +
RapidOCR, versus 22.87 s with Docling + Tesseract PSM 11, excluding initialization
and orientation preprocessing.

## Implication for the PDF service

Use the existing RustFS conventions and preserve original PDF bytes permanently
for the lifetime of their reports. Keep extraction and business interpretation
separate, as Norway already does.

Docling is worth testing as the general layout/table stage. Keep OCR configurable:
Tesseract remains a cheap baseline and RapidOCR is a promising option for dense
upright scans. Do not choose a single OCR backend for all pages from this small
sample. Benchmark automatic orientation correction and region handling next;
the manual rotation here does not yet solve that service requirement.

Require evidence checks before publishing financial claims: metric, entity,
period, unit and sign must be recoverable together. Retain low-confidence or
ambiguous pages as extraction issues. Capture model warnings as well as the
conversion status; Docling returned success for the unreadable scans, and its
table confidence was unavailable in this configuration.

For cash-flow tables, a reconciliation check would catch the observed errors:
`46,115 - 160,917 + 9,090 + 182,354 = 76,642` in thousands of RSD. Flag a mismatch
for review; do not silently adjust numbers to force the equation to balance.

The next LLM comparison should feed these retained tables and nearby text to
DeepSeek with required cell/page citations and compare against the same checked
facts. This run did not invoke DeepSeek, so improved parser structure has not
yet demonstrated improved end-to-end company reports.

Follow-up: the [OpenRouter PDF experiment](PDF_OPENROUTER_RESULTS.md) now compares
Mistral OCR with these retained local outputs through DeepSeek. It separates raw
recognition, orientation handling, numeric normalization and model/provider failures.

## Artifacts and reproduction

- [Benchmark runner](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/pdf_benchmark.py)
- [Baseline measurements](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/pdf-parser-20260907/dagster/run.json)
- [Docling measurements](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/pdf-parser-20260907/docling/run.json)
- [Dependency versions](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/pdf-parser-20260907/requirements.lock.txt)
- [Service design](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/PDF_SERVICE_DESIGN.md)

Run from `companycollect/codex-sd-examples`, using a new output directory:

```sh
company_full_analysis_lab/data/pdf-benchmark-runtime/bin/python \
  company_full_analysis_lab/pdf_benchmark.py \
  --engine docling \
  --sources company_full_analysis_lab/data/openrouter-20260907T165736Z/recovered-source-cache \
  --output company_full_analysis_lab/data/pdf-parser-new-run
```

Use `--engine dagster` for the existing extractor. Diagnostic options include
`--samples scan_balance scan_income --rotate 90`, `--psm 11`,
`--no-cell-matching`, and `--engine docling-rapidocr`. The main baseline bypasses
no data and preserves the existing algorithm; PSM diagnostics override only its
in-memory setting. Use `OMP_NUM_THREADS=4` as in this run.

Each engine directory contains `run.json`; each sample contains `document.json`,
`document.md`, and a review image. Docling also produces table HTML and confidence
JSON. Logs retain parser warnings. Early run metadata used Pydantic's base-class
serialization, which omitted subclass options; the full settings above and
runner configuration specify those runs. The runner now serializes subclass
settings explicitly for subsequent runs.
