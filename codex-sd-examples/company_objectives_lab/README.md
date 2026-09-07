# Company objective benchmark

An isolated DeepSeek experiment covering company profile, company contacts,
locations, products/services, people, company relationships, and jobs. It reuses
the existing OpenRouter HTTP client and Crawl4AI browser setup. Production `ex3`
selection and extraction behavior is unchanged.

The two tasks run independently:

1. Assess each candidate link against all seven objectives, distinguishing direct
   evidence from navigation and uncertainty.
2. Extract attributed records from complete, native Crawl4AI `cleaned_html`.
   Selection hypotheses and evaluation labels are never supplied to extraction.

See [RESULTS.md](RESULTS.md) for the measured results and remaining limitations.
The subsequent native Crawl4AI comparison is in [NATIVE_RESULTS.md](NATIVE_RESULTS.md).

## Frozen inputs

`data/v1/` contains eight company candidate inventories with 200 candidates each,
plus 40 rendered destination pages, five per company. The inventories come from
the existing `experiments/page-selection/candidates/` snapshots. These were already
shortlisted upstream; this benchmark does **not** measure discovery recall over
the original full sitemaps. Earlier heuristic scores and draft gold labels are
excluded from inference inputs.

The 40 destinations are listed in [sources.json](sources.json). Crawl4AI 0.9.3
produced the complete cleaned HTML directly. No site-specific CSS selectors,
main-content cropping, custom HTML simplification, or markdown conversion is used
for model input. Human-readable source inspection used main elements where present;
that inspection does not alter the saved input. Full HTML, native cleaned HTML,
native markdown, URL, status, timestamp, configuration and hashes are saved.

Six initial browser failures succeeded when retried serially. Vestas job openings
redirects to `careers.vestas.com`; the collector uses `redirected_url` as the base
for relative links. This page was captured again after fixing that metadata. A
Vestas customer URL returned HTTP 404 and is deliberately retained as an extraction
control. Rendering success alone does not imply a successful information fetch.

`reference-v1.json` contains **143 source-reviewed positive fact checkpoints**, 82
page/objective selection labels, and four negative extraction controls. Codex
reviewed these against the saved source, before inspecting selection responses and
before extraction inference began. This is a non-exhaustive pilot reference, not
independent human annotation or a complete census of every fact on every page.
The ignored `data/v1/reference-lock.json` pins its hash and creation time.

Checkpoint matching requires the listed fields in the same record, including
owner/company/role where specified. It uses normalized substring matching, explicit
alternative names, punctuation-insensitive phone digits, exact enumerated types
and job URLs, and explicitly equivalent inverse relationships. It is a coverage
check, not a universal exact-field accuracy score. Unlabelled records are not
automatically false positives.

## Run

From the `codex-sd-examples` directory, use the existing Python 3.12 environment:

```sh
.venv/bin/python -m company_objectives_lab.run \
  --data-dir company_objectives_lab/data/v1 \
  --stage selection --run-id selection-v1 \
  --env-file jobs_extraction_lab/.env

.venv/bin/python -m company_objectives_lab.run \
  --data-dir company_objectives_lab/data/v1 \
  --stage extraction --run-id extraction-v1 \
  --env-file jobs_extraction_lab/.env

.venv/bin/python -m company_objectives_lab.run \
  --data-dir company_objectives_lab/data/v1 \
  --stage selection --run-id selection-repeat-v1 \
  --batches-per-site 1 \
  --env-file jobs_extraction_lab/.env

.venv/bin/python -m company_objectives_lab.report \
  --data-dir company_objectives_lab/data/v1 \
  --reference company_objectives_lab/reference-v1.json \
  --run-id selection-v1 --run-id selection-repeat-v1 --run-id extraction-v1
```

The repeat samples the same shuffled first batch of 40 candidates from each site:
320 candidates across all eight companies. It is a repeatability probe, not a
second full 1,600-candidate test. Compare decisions on the common successfully
assessed candidates; missing responses are reported separately.

Settings pin `deepseek/deepseek-v4-flash-0731`, Baidu FP8 with no provider fallback,
low reasoning, temperature zero, batches of 40, and concurrency three. The 65,536
output-token budget is a benchmark setting, **not a model maximum**. Each logical
request has a 180-second total deadline and at most two HTTP attempts within that
deadline. One correction is allowed for JSON/schema/source-validation errors.
Service failures do not trigger a correction. All settings and prompt hashes are
saved; a changed configuration requires a new run ID.

Rerunning an identical command reads saved attempts and continues pending tasks.
It never silently replaces a failed attempt. Three consecutive service failures,
or a permanent HTTP error, stop new requests; in-flight requests finish. Inspect
the stored error before resuming. Costs from requests with no usage response are
unknown and excluded from the reported known-cost sum.

To collect a new corpus, choose a new data directory and refresh the reference:

```sh
.venv/bin/python -m company_objectives_lab.collect \
  --sources company_objectives_lab/sources.json \
  --data-dir company_objectives_lab/data/v2 \
  --candidate-dir experiments/page-selection/candidates \
  --concurrency 3 --port 9441

# After inspecting the saved collection:
.venv/bin/python -m company_objectives_lab.collect \
  --sources company_objectives_lab/sources.json \
  --data-dir company_objectives_lab/data/v2 --freeze-inputs
```

Create the source-review reference and its hash lock for that new snapshot before
evaluating it. Existing reference hashes intentionally reject changed
pages; do not reuse old labels without reviewing their new source.

For browser failures, rerun collection with `--retry-failed --concurrency 1` before
freezing. Failed outcomes are archived. Successfully rendered pages, including HTTP
error controls, are not automatically replaced.

## Validation and evaluation

Each candidate must appear once with all seven objectives. Each extraction record
is independently checked against its Pydantic schema, its source quotation and any
supplied URL. Evidence checks normalize Unicode, case and whitespace; they do not
prove semantic attribution. A real quotation can still be attached to a wrong
company, person, relationship, or copied value.

Valid records survive failures elsewhere in the response and a failed correction.
Malformed or truncated JSON is not partially salvaged. Initial valid candidate
assessments are retained; corrections fill missing assessments. Extraction retains
the union of exact records excluding their evidence field. Differing values remain
separate observations for review; there is no automatic entity reconciliation or
conflict resolution.

The report revalidates raw responses and checks saved task/request/content hashes
and retained results. It reports each objective independently, service failures,
unknown costs, coverage of the fixed checkpoints, limited labelled-negative checks,
and selection repeatability. A separate diagnostic compares schema-valid raw
records with the checkpoints before evidence/URL validation. Those raw matches
do not bypass validation or change the retained results.

A 20-page-per-site offline scheduling illustration
cycles through objectives, orders high before medium and direct before navigation,
and breaks ties by a URL hash. A multipurpose page consumes one slot. This is an
offline budget illustration, not an adaptive crawl or a calibrated ranking model.

It does not yet measure exhaustive record precision, full sitemap recall, cross-page
deduplication, conflicts over time, discovery through navigation/pagination, or
end-to-end objective completeness. An empty extraction, timeout, or 404 never proves
that a company has no jobs, contacts, people or related companies.

```sh
.venv/bin/python -m unittest company_objectives_lab.tests
uvx --offline ruff check company_objectives_lab jobs_extraction_lab/extract.py
uvx --offline ty check company_objectives_lab
```
