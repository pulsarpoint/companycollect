# Mistral OCR through OpenRouter, followed by DeepSeek

Run date: 2026-09-07. Six selected physical PDF pages from the same three
NOVELIC/Sona sources used in the local PDF benchmark, with 22 manually checked
numeric targets. This is a small diagnostic experiment, not a general OCR
accuracy estimate or a rerun of the full company report.

**Mistral OCR is useful, but sending arbitrary PDF pages directly to it did not
solve the extraction problem. Page orientation, document layout and numeric
normalization remain necessary.** Separating a two-page spread and rotating its
sideways table fixed all three checked AOC-1 values. On the upright cash-flow
page Mistral preserved all six checked raw amounts and signs, but DeepSeek
misinterpreted their thousands separators.

## What ran

- OCR: OpenRouter Chat Completions with `file-parser`, explicitly selecting
  `pdf.engine = "mistral-ocr"`. The service did not disclose an OCR model version.
- Interpretation: `deepseek/deepseek-v4-flash-0731`. The completed six-page
  Mistral interpretation used Parasail, temperature 0, reasoning disabled,
  JSON-schema output and no caller-specified output-token cap.
- The same instructions, schema and per-page questions were also applied to
  cached Docling + Tesseract output. Supplemental runs used the previously
  rotated Docling + RapidOCR scans.
- Expected values were stored separately and were never sent to DeepSeek.
  Questions identify the requested company, metric, period or currency, but
  contain no expected amounts. This evaluates targeted extraction rather than
  open-ended discovery of every fact on a page.
- No Codex SDK, web browsing or other model tools were used for interpretation.
  DeepSeek received text, including the full recovered OCR text; it did not
  receive page images. Original PDF URLs and page positions remain in local
  provenance records.
- All six original and selected PDF hashes match the preceding local benchmark.
  The originals remain unchanged. No RustFS upload, deployment or production
  crawler change was performed.

The initial low-reasoning run exposed a separate execution problem: one request
spent all 32,768 provider-default completion tokens on reasoning, returned
`finish_reason=length`, and produced no extracted facts. Six other pending
requests were cancelled locally. Their remote completion and cost are unknown.
Those attempts remain in the artifacts and are excluded from accuracy results.

A subsequent DeepInfra run encountered upstream shared-pool rate limits.
OpenRouter returned the completed OCR in error-response `file_annotations` for
all six pages. We recovered that text and finished interpretation separately,
without paying to parse those PDFs again. For acquisition and AOC-1, the first
available OCR response was retained; for the other pages, the first recoverable
error annotation was used. We did not select the best of repeated OCR outputs.

## Findings against the source pages

| Page | Mistral OCR and downstream result | Existing local extraction comparison |
|---|---|---|
| Acquisition and revenue, FY2025 PDF 196 | Both checked amounts are preserved: founders' payment `2,109.62` and subsidiary revenue `484.31`. The completed non-reasoning extraction keeps them separate. | Docling also preserves the amounts and the subsidiary's exact revenue period. The earlier low-reasoning Mistral interpretation incorrectly attributed revenue to Sona, despite the necessary context being present in OCR. |
| Net assets and profit, FY2026 PDF 177 | Five of six numeric targets match. OCR changes the FY2026 loss `(186.35)` to `(188.35)` and DeepSeek repeats it. Two percentage facts also omit the required multiplier of 1. | Docling preserves all six checked numeric values. The completed DeepInfra non-reasoning extraction returns all six, though it also leaves the percentage multipliers null. |
| AOC-1, FY2026 PDF 184 | On the untouched spread, names and figures are heavily corrupted; none of the three requested facts is recovered. | Docling preserves the values but merges neighboring fields. Both the initial low-reasoning extraction and the completed Parasail non-reasoning retry recovered turnover `755.77`, PBT `(186.23)` and PAT `(186.23)` from those merged cells. |
| Sideways balance sheet, scan PDF 1 | OCR invents or corrupts headers, including `USD` instead of `RSD`, and damages totals. DeepSeek returns no RSD/EUR total-assets facts. | Unrotated Docling is unreadable too. Previously rotated RapidOCR text lets DeepSeek recover both checked totals: `2,088,249` thousand RSD and `17,784` thousand EUR. |
| Sideways income statement, scan PDF 2 | Revenue digits `903.698` and `7.708` survive, but the RSD/EUR headers do not; PBT loses its minus sign and becomes `218.693`. DeepSeek declines the requested currency-specific facts. | Rotated RapidOCR retains more useful headers but still has a malformed revenue cell and lost signs. Its DeepSeek output also mishandles grouping separators. |
| Upright cash flow, scan PDF 3 | All six checked raw amounts and signs survive. DeepSeek treats dots as decimal points, making every normalized amount 1,000 times too small. | Docling + Tesseract misreads financing cash as `182384`. Its DeepSeek interpretation also misreads grouping separators and drops the separate minus sign on PBT. |

The untouched Mistral input produces **7/22 exact normalized values** and **5/22
values with the required currency and unit multiplier**. These are strict output
contract checks, not OCR character accuracy. In particular, the six cash-flow
failures are downstream normalization failures even though their raw readings
are correct. Company attribution, exact periods and evidence quality were
reviewed separately; a numeric match alone is not a verified financial claim.

The completed local comparison yields **11/22 exact normalized values** and
**9/22 values with the required currency and multiplier**. Five local pages use
the same Parasail endpoint as Mistral's interpretation; the net-assets page uses
its completed DeepInfra non-reasoning result because Parasail repeatedly rejected
that request. Model ID, prompt, schema and reasoning-disabled configuration match.
The composite comparison and its exact per-page artifacts are in `comparison.json`;
the provider difference prevents treating the totals as a pure parser-only score.

The acquisition payment illustrates why period checks are separate: the final
Mistral interpretation assigns it to the year ended March 2025 even though its
own warning correctly identifies the consideration table as March 2024. That fact
passes the numeric check but fails the date attribution check.

## Diagnostic: split the AOC-1 spread and rotate the table

The physical PDF page contains printed pages 364 and 365. The left page's
subsidiary table is sideways; the right page's associates table is upright.
We split at the vertical midpoint, rotated the left page clockwise by 90 degrees,
and sent both resulting pages to the same OCR service. No table rows, columns or
company-specific regions were selected or discarded.

With reasoning disabled and Parasail selected, **all three AOC-1 amounts and
signs were recovered**, versus zero on the original spread. The successful
request took **10.93 seconds** and reported **$0.00457084**, including $0.004 OCR
for the two resulting pages.

This was a manually chosen diagnostic. It does not demonstrate an implemented
automatic orientation detector. The extracted unit scale is also inferred from
the report context rather than explicitly printed beside the AOC-1 table;
DeepSeek records that uncertainty. Matching our expected scale is not sufficient
to promote that inference to explicit source evidence.

## Numeric normalization and evidence failures

The cash-flow page uses dots to group thousands. Its closing balance is `76.642`
in thousands of RSD, meaning **76,642 thousand RSD**, or **76,642,000 RSD**.
DeepSeek returns `value_in_displayed_units = "76.642"` and
`unit_multiplier = 1000`, which represents only 76,642 RSD. The prompt explicitly
asks the model to infer grouping conventions, but that instruction is not enough.

The known cash reconciliation is:

```text
46,115 - 160,917 + 9,090 + 182,354 = 76,642    [thousands of RSD]
```

Reconciliation alone cannot detect a uniform factor-of-1,000 error: every number
can be scaled incorrectly while the equation still balances. We need both unit
validation and arithmetic validation. Preserve original strings, resolve the
document's decimal/grouping convention explicitly, and use Decimal arithmetic.
Ambiguous conventions must remain unresolved rather than silently guessed.

The rotated RapidOCR income test exposes another evidence issue. The IFRS revenue
cell actually contains `903.3698`; the neighboring locally consolidated cell
contains `903.698`. DeepSeek returns `903.698` and describes it as the IFRS cell.
That happens to match the PDF's correct amount, but its claimed cell evidence
does not match the supplied OCR. A correct-looking number can still have an
invalid evidence trail.

Do not interpret `not_found` on damaged OCR as evidence that the original document
lacks the requested information. Parser quality failures need a separate
extraction-issue state before the company report is assembled.

## Cost, timing and practical limits

- The successful six-page Mistral-text interpretation reports **$0.00306494**
  for DeepSeek and **59.83 seconds** summed request time; its sequential run took
  60.14 seconds. Those figures exclude the earlier OCR and failed attempts.
- At OpenRouter's documented $2/1,000-page OCR rate, a clean six-page workflow
  would have about **$0.01506** in OCR plus this measured interpretation cost.
  This is a projected clean-run total, not the actual total spend of the experiment.
- Reported costs from completed/incomplete attempts are retained in
  `attempt-ledger.json`: **$0.037014465 is known** across all exploratory and
  comparison attempts. There were 47 submitted attempts: 22 completed, one
  incomplete, 18 failed and six cancelled locally. Failed calls and the six
  locally cancelled calls lack full
  billing data, including some calls whose OCR succeeded before inference failed.
  Actual experiment spend therefore cannot be fully reconciled from responses.
- The returned Mistral text totals 33,115 characters versus 74,279 for Docling.
  Much of Docling's Markdown size is table padding. Shorter text is not evidence
  of better completeness or lower token count.
- Provider rate limits prevented every local comparison from completing on one
  endpoint. The page-by-page findings identify the differing settings; do not
  treat them as a controlled model latency or general accuracy ranking.
- OpenRouter annotations expose text/image parts, not the detailed table-cell
  geometry and confidence available in Docling JSON. Our one-page request mapping
  supplies physical page provenance; word/cell coordinates are not established.

OpenRouter documents both PDF parsing and recovery of parsed annotations after a
downstream failure in its [PDF API documentation](https://openrouter.ai/docs/guides/overview/multimodal/pdfs).

## What this changes in the service design

Keep the local parsing path and test Mistral as an additional OCR option.
Preprocess spreads and orientation before OCR; retain native text where useful.
Store raw OCR independently of LLM interpretation so provider failures do not
force document reprocessing. Normalize financial numbers explicitly and verify
signs, units, entity/period attribution and quoted cell evidence before accepting
facts. Keep OCR failures separate from absent information.

The next implementation should target automatic orientation/layout handling and
number normalization, using these failures as regression cases. The current
experiment does not justify replacing the PDF service with a single PDF-to-LLM
request.

## Files and reproduction

- [Runner](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/pdf_openrouter_benchmark.py)
- [Checked facts and questions](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/pdf_fact_checks.json)
- [Complete Mistral-text interpretation](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/pdf-openrouter-20260907/mistral-parasail-off/run.json)
- [Per-page composite comparison](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/pdf-openrouter-20260907/comparison.json)
- [Successful split-spread diagnostic](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/pdf-openrouter-20260907/mistral-split-aoc1-parasail-off/record.json)
- [Attempt and cost ledger](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/pdf-openrouter-20260907/attempt-ledger.json)
- [Input integrity checks](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/pdf-openrouter-20260907/integrity-check.json)
- [Previous local benchmark](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/PDF_BENCHMARK_RESULTS.md)

From `companycollect/codex-sd-examples`, using a new output directory:

```sh
company_full_analysis_lab/data/pdf-benchmark-runtime/bin/python \
  -m company_full_analysis_lab.pdf_openrouter_benchmark \
  --mode mistral \
  --sources company_full_analysis_lab/data/openrouter-20260907T165736Z/recovered-source-cache \
  --output company_full_analysis_lab/data/pdf-openrouter-new-run \
  --reasoning-effort off --provider parasail --concurrency 1 --timeout 180
```

For cached text, use `--mode local --local-output <directory>` instead of
`--mode mistral --sources ...`. Each sample directory must contain `document.md`.
Use `--samples` to select cases. Credentials are read from the existing ignored
jobs-extraction `.env` or the environment and never written to request artifacts.
The runner preserves full responses and any returned OCR on failure; it does not
automatically retry paid requests. The wall timeout is an execution guard, not
an output-token cap.
