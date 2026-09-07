# Asynchronous PDF extraction service

Proposed contract, not an implemented service. This adds a document reader to
the optional company research experiment. It does not change core crawler
extraction or deploy infrastructure.

## Ownership

The PDF service downloads documents, archives the original bytes in our existing
RustFS service through its S3 API, and then runs local extraction. A background worker in the research
runner polls for completion without invoking an LLM. The research agent chooses
documents and interprets the returned evidence. A document analysis agent can
examine difficult tables or verify claims after extraction.

Keep a small HTTP API with ordinary MCP tools exposing the same operations.
Use service-owned job IDs so research jobs do not depend on a parser vendor's
operation IDs. The first local comparison against the difficult NOVELIC pages is
documented in `PDF_BENCHMARK_RESULTS.md`. Docling improves table structure, but
the scanned financial pages still require quality checks and further OCR work.

```text
PDF URL → download → archive original in S3 → local OCR/layout/table extraction
        → archive extraction results → document analysis → company report
```

## Local extraction

### Existing Dagster implementation

The closest reference is Norway BRREG's
`corpscout/services/dagster_v3/src/dagster_v3/defs/norway_brreg_financial/annual_account_pdf.py`:

- PyMuPDF reads embedded text and word bounding boxes per page. Pages with fewer
  than 20 non-whitespace characters fall back to local Tesseract.
- OCR uses `nor+eng`, segmentation mode 4, TSV word coordinates and confidence.
  It extracts a page-covering image when present, otherwise renders at 200 DPI.
- Output retains page text, words, normalized coordinates, source URL, PDF
  SHA-256, retrieval time and extraction settings. Extraction covers all pages.
- `annual_account_pipeline.py` separates download/storage from parsing, verifies
  stored bytes against the catalog hash, and persists extracted JSON in RustFS.
- A separate geometry-based financial parser in `annual_account_financials.py`
  assigns labels/amounts to year columns; concept mapping and normalization are
  later stages. These Norway-specific rules are not a general table parser.

Reuse these storage, provenance and staged-processing conventions. Benchmark the
existing extractor as the baseline against Docling with Tesseract before choosing
the general document parser. OCR engine and document pipeline are different
layers: Docling can use Tesseract while adding layout and table recognition.

For general research, improve the native-text quality check: a page containing
only a short digital header can still have a scanned table. Word confidence on
native text is currently assigned 100, which must not be interpreted as verified
financial accuracy. Make OCR languages configurable for each document.

The UK `uk_companies_house/pdf_extract.py` is a narrower proof of concept:
`pdftoppm → Tesseract → plain text → LLM`. It defaults to the first 12 pages,
sends only 12,000 characters to the LLM, and limits its response to 800 tokens.
Its OCR call does not check the return code; page/table provenance is lost when
texts are joined. Do not carry these limitations into this service.

### Candidate table extraction

OCR alone does not solve table interpretation: recognizing a number correctly
still leaves its row, column, unit and reporting period to recover. Use Docling's
PDF pipeline with OCR and table structure recognition enabled, and test its
accurate table mode. Models can be downloaded ahead of time and run locally.
[Docling pipeline options](https://docling-project.github.io/docling/usage/advanced_options/).

Use embedded text where reliable, OCR scanned regions, and retry selected pages
with full-page OCR when the text layer is missing or corrupt. Record each
attempt's options and quality warnings. Full-page OCR is a supported option,
but the retry policy belongs to our service.
[Docling full-page OCR example](https://docling-project.github.io/docling/_generated/examples/full_page_ocr/).

Preserve structured document JSON and table cells as the evidence record;
Markdown and table HTML are additional reading formats. Keep headings, captions,
units and footnotes even when they sit outside detected table boundaries. Do not
assume a Markdown export preserves every block from the structured result.

If the benchmark still loses table structure, compare a local PaddleOCR-VL
configuration on the same pages. This is a fallback candidate, not a second
backend to implement immediately or a claim of better accuracy.
[PaddleOCR-VL local deployment](https://github.com/PaddlePaddle/PaddleOCR/blob/main/docs/version3.x/pipeline_usage/PaddleOCR-VL.en.md).

### Benchmark findings and quality checks

The 2026-09-07 comparison used the current Dagster extractor, Docling with the
same Tesseract languages/segmentation, and focused RapidOCR diagnostics. Docling
separated acquisition and revenue tables and retained net-assets headers. It
still merged AOC-1 fields and produced financial OCR errors. Manually correcting
scan orientation helped RapidOCR considerably, but did not eliminate errors;
automatic correction is not implemented by this benchmark.

The service should retain orientation and extraction-attempt metadata, preserve
coordinates back to the original PDF, and flag missing/damaged table headers,
ambiguous columns, suspect signs and failed reconciliations. Conversion success
and OCR confidence alone do not validate a claim. Keep OCR configurable while
testing more documents; the current evidence does not justify a universal engine
choice. Do not silently repair numbers to make a balance equation pass.

## Archive before extraction

1. Download to a temporary file while computing SHA-256. Validate that the
   response contains a PDF, including after redirects; a successful HTTP status
   and a `.pdf` suffix do not rule out an HTML error page.
2. Store the unchanged bytes under a content-addressed key such as
   `documents/sha256/<sha256>/original.pdf` in the configured S3-compatible bucket.
   Treat this key as immutable: new content gets a new key. Retain an object
   version ID when the bucket supplies one.
3. After a successful upload (or verification that the identical object already
   exists), persist its location, hash and retrieval record. Only then begin
   parsing. An archive failure is retryable within the job deadline; it must not
   silently fall back to an unarchived local-only result.
4. Parse those same verified bytes. Archive raw parser output, normalized JSON,
   Markdown/table HTML and any retained evidence images under
   `extractions/<sha256>/<result_id>/`. Persist the artifact manifest before
   publishing a completed job. Local working files are temporary or a cache.

### RustFS configuration and existing code

Use the same environment contract as Dagster's
`corpscout/services/dagster_v3/src/dagster_v3/defs/common/resources.py`:

| Setting | Convention |
|---|---|
| Endpoint | `CORPSCOUT_S3_ENDPOINT` |
| Access key | `CORPSCOUT_S3_ACCESS_KEY` |
| Secret key | `CORPSCOUT_S3_SECRET_KEY` |
| Region | `us-east-1` in the shared resource |
| Client | `boto3.client("s3", endpoint_url=..., config=Config(s3={"addressing_style": "path"}))` |
| PDF bucket | Proposed explicit `PDF_S3_BUCKET=source-company-research`; not created yet |

The RustFS infrastructure README documents the internal API at
`http://rustfs:9000` and console at port 9001. Read the actual endpoint from the
environment; do not hardcode the service hostname or use the console for S3
requests. Load credentials through service configuration, never report JSON.

`ObjectStoreResource` already supports byte/file uploads, reads, bucket creation
and listings. Norway's `financial_storage.py` adds PDF validation, immutable
write checks, source catalogs and separate PDF/JSON keys in `source-norway-brreg`.
Webtech also has a small standalone boto3 client in
`corpscout/services/webtech/s3_store.py`. Follow the shared connection contract
in the standalone PDF service without importing Dagster and country-specific
resource classes. Do not inherit the shared resource's Finland bucket default.
The current shared writer does not return a version ID or expose content-type
options; PDF metadata, upload verification and report access need explicit handling.

### Retention differs from source cache cleanup

Norway's `remove_processed_annual_account_pdfs` deletes PDFs once matching JSON
has been verified. Its cleanup asset must not be reused for research evidence:
keep original PDFs for as long as their reports are retained.

Dagster's deployment runbook classifies raw object-store snapshots as rebuildable
cache and excludes them from scheduled backup. Research evidence needs an
explicit retention and recovery policy before production, because the publisher
may remove or replace the source. Use a dedicated bucket so cache cleanup and
expiry rules cannot remove PDFs cited in reports. This proposal does not change
existing bucket lifecycle rules or infrastructure.

Keep a retrieval record for each source URL and fetch time even when several
URLs yield the same PDF bytes. Document identity is the content hash; source
identity is the retrieval record. Reports must reference the exact version that
was analyzed, including after the publisher replaces a PDF at the same URL.

Store the durable `s3://bucket/key` and optional version ID in report JSON. For
readers, provide an **Original PDF** link and an **Archived PDF** link through a
stable application document route that resolves that record and serves or signs
access to the archived object. A private bucket does not need to become public.
Do not use a presigned URL as the only archive reference: it expires. Until an
application route exists, include the S3 URI in exported reports; any temporary
download URL must include its expiry.
[S3 presigned URL lifetime](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html).

## API contract

| MCP tool | HTTP equivalent | Result |
|---|---|---|
| `pdf_submit(url, pages?, idempotency_key?)` | `POST /pdf-jobs` | Job ID, current status, suggested polling interval |
| `pdf_status(job_id)` | `GET /pdf-jobs/{job_id}` | State, available progress, completion summary or error |
| `pdf_read(job_id, pages?, table_ids?, cursor?)` | `GET /pdf-jobs/{job_id}/result` | Manifest or requested pages/tables with provenance |

For large results, `pdf_read` is paginated. With no selection, it returns a
manifest of pages, headings and tables; subsequent calls retrieve the desired
content. Never silently truncate a document to a fixed token count. Page
selection uses one-based PDF page positions. Preserve printed page labels
separately when available; an annual report can have two printed pages on one
physical PDF page.

## Example submission

```json
{
  "url": "https://example.com/annual-report.pdf",
  "idempotency_key": "research-run-123:annual-report"
}
```

Response (`202 Accepted` when newly queued):

```json
{
  "job_id": "pdf_7f12",
  "status": "queued",
  "poll_after_seconds": 10
}
```

Completed status response:

```json
{
  "job_id": "pdf_7f12",
  "status": "completed",
  "result_available": true,
  "coverage": {
    "requested_pages": 12,
    "processed_pages": 12,
    "failed_pages": []
  },
  "warnings": []
}
```

The numbers and identifiers above are illustrative. Do not invent progress
percentages if the provider exposes only a state.

## Job lifecycle and waiting

States: `queued → running → completed | failed`. If some requested pages cannot
be parsed, persist their failures and report `coverage` and warnings explicitly.
Completion means extraction has ended and results are available; it does not
mean every financial claim is validated.

While running, expose a `stage`: `downloading`, `archiving`, `extracting`, or
`storing_results`. These are progress details, not extra job states. If all
requested pages fail, mark the job failed and retain its archive reference and
diagnostics. Partial success can complete with explicit failed-page coverage.
An archived PDF remains available even if extraction fails.

1. The research agent submits a PDF and gets a job ID immediately.
2. The runner persists the link between that job and the research run.
3. The agent continues researching other pages and companies' related sources.
4. A background task checks status using the suggested interval. It respects
   provider backoff/Retry-After and retries transient transport failures within
   a deadline. A failed status request is not automatically a failed extraction.
5. Completion or terminal failure produces one event for the research run.
   Deliver it through the runner's supported continuation mechanism; do not
   launch a new LLM turn for every pending status.
6. The agent reads relevant results and adds evidence-backed claims. Before
   finalizing a report, the runner checks pending required documents. It either
   waits for them or explicitly marks the report incomplete under its deadline.

No dedicated LLM is needed to ask whether a job is finished. After extraction,
an optional document analysis agent receives a bounded task, relevant tables and
surrounding text, and references to the exact archived PDF and extraction result.
For ambiguous tables it can inspect page images using an image-capable tool/model
or request another local extraction attempt. It must keep units, periods, entity
scope and source cells attached to its claims, and flag unresolved ambiguity.
Store this analysis separately from the parser output; do not overwrite source
evidence with an interpretation. Waiting itself stays in ordinary code.

## Result contract

Preserve:

- Original URL, final download URL, retrieval time and document content hash.
- Archived PDF bucket, key, S3 URI, optional object version ID and byte size.
- Retrieval/source ID, plus archive references for extraction artifacts.
- Parser name/version, requested options and immutable result ID.
- PDF page position, printed page label if known, section headings and text.
- Tables with stable IDs, cells, row/column indices, merged spans and headers.
- Captions, nearby units and footnotes, or references to the corresponding text
  blocks. They may lie outside the parser's detected table boundary.
- Page/cell coordinates when available, extraction warnings and failed pages.
- Raw parser output and the original PDF in S3 for later inspection.

A report source entry should carry the following shape. Values are illustrative;
the hash placeholder stands for the full SHA-256 hex digest, and `archive_url`
requires the application route described above to exist.

```json
{
  "source_id": "source_123",
  "original_url": "https://example.com/annual-report.pdf",
  "resolved_url": "https://cdn.example.com/reports/annual-report.pdf",
  "retrieved_at": "2026-09-07T12:00:00Z",
  "sha256": "<sha256>",
  "size_bytes": 1234567,
  "archive": {
    "bucket": "source-company-research",
    "key": "documents/sha256/<sha256>/original.pdf",
    "s3_uri": "s3://source-company-research/documents/sha256/<sha256>/original.pdf",
    "version_id": null
  },
  "archive_url": "https://app.example.com/research/documents/source_123/original",
  "extraction": {
    "job_id": "pdf_7f12",
    "result_id": "extract_456",
    "parser": "docling",
    "parser_version": "<installed-version>",
    "manifest_s3_uri": "s3://source-company-research/extractions/<sha256>/extract_456/manifest.json"
  }
}
```

Claims cite `source_id`, `result_id`, one-based physical page positions, printed
page labels when known, and block/table/cell IDs. Table and block IDs are stable
within an immutable extraction result, not assumed identical across parser
versions. The report renderer resolves each source ID to both document links.

Business interpretation remains a separate step. DeepSeek adds company/entity,
metric, value, units, period and consolidation scope while retaining references
to the source cells and surrounding text. An OCR or layout failure must remain
`extraction_failed`; it is not evidence that information is not public.

## Small first implementation

Use one API process, a durable jobs table and a local extraction worker. SQLite
on a persistent volume is enough for a single-instance experiment; PDFs and
results live in S3-compatible storage. Persist accepted jobs before returning
their IDs and record stages, archive references, attempts and worker leases.
After a restart, reclaim expired leases and resume from durable artifacts or
rerun incomplete local parsing. Do not assume the parser can resume halfway
through a page. No additional queue service is necessary for the first benchmark.

Use an idempotency key to make submission retries safe. Reuse completed parsing
by document hash plus parser/model versions, options and requested pages, after
checking the current content; a URL alone is not a permanent cache key. Keep
artifacts independently of a
research agent's temporary workspace. Check public download URLs and redirects
at the service boundary, and apply download size/time limits.

Start with ordinary MCP tools returning IDs. Native MCP Tasks support is a
separate compatibility decision; its current extension is a draft and requires
explicit client/server support. [MCP Tasks extension](https://tasks.extensions.modelcontextprotocol.io/specification/draft/tasks).

Local extraction is the intended first backend. The asynchronous API belongs to
our service; a local parser does not itself need to expose asynchronous jobs.

## Acceptance test

Use the exact NOVELIC pages where the prior run failed. Verify that:

- Submission returns before extraction finishes and polling requires no LLM.
- Restarting the API/worker preserves job identity and eventual results.
- Retried submissions do not duplicate the same logical extraction.
- Parsing never starts before the original PDF is durably archived.
- Downloaded, archived and parsed bytes have the same SHA-256; a parser failure
  still leaves the original document accessible through its source record.
- Changed bytes at the same URL create a new document version; identical bytes
  from different URLs reuse the PDF object while retaining each source record.
- Finalization waits for extraction artifacts to be stored; an S3 write failure
  cannot produce a completed job pointing to missing results.
- Acquisition payments and revenue remain in their separate tables with labels.
- Net assets retain their metric headers, rather than becoming revenue.
- Source URLs, page labels, units and footnotes survive result retrieval.
- Report JSON preserves the original URL and S3 reference. When the document
  route exists, both report links resolve to the intended source/version and
  archived access can be refreshed after a signed download URL expires.
- Failed pages and parser errors remain visible to the research agent.

Measure local extraction time and peak memory alongside structural correctness
on native-text pages, scanned pages and the financial spreads. Then rerun
DeepSeek's extraction on these results before repeating full company research.
Better parsing still requires validation of the resulting claims.
