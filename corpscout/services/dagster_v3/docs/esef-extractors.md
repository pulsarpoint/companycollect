# ESEF extractors

Spec: `docs/superpowers/specs/2026-09-13-esef-extractors-and-domains-design.md`.

ESEF is a document store plus independent extractors. `corpscout.esef_filings` indexes the
filings; the weekly parse (`esef_document_artifacts_s3`) archives every report package in the
`source-esef-filings` bucket under `report_package_object_key(package_sha256)` and produces the
facts (`esef_facts`, versioned by `ARTIFACT_SCHEMA_VERSION`). Every other product is an
extractor: one asset, one table, one version constant, its own sensor.

## The pattern (`esef_domains`)

| Piece | Where |
| --- | --- |
| Version | `domains_extraction.ESEF_DOMAINS_EXTRACTOR_VERSION` |
| Per-document function (light, runs in a spawned child) | `defs/esef_filings/domains_extraction.py` |
| Package opener (light) | `defs/esef_filings/report_package.py` |
| Timeout wrapper (stdlib) | `defs/common/child_timeout.py` |
| Asset + job + selection SQL | `defs/esef_filings/domains_extractor.py` (`esef_domains_clickhouse`, `esef_domains_job`) |
| Writer (stage + EXCHANGE, per document set) | `defs/esef_filings/document_rows.py` |
| Sensor | `defs/esef_filings/domains_sensor.py` (`esef_domains_stale_sensor`, 30 min, one run of ≤ 5,000 documents at a time) |
| Table + view | migration 000405: `corpscout.esef_domains`, `corpscout.se_esef_domains` |

Stale = an available document (package archived, facts settled, `period_end <= today()`)
whose rows are missing, carry another `extractor_version`, or were extracted from another
`package_sha256` (a changed package is stale too). Facts have settled when the document's
newest `esef_facts.resolved_at` is more than one hour old: the publish multi-asset writes
`esef_facts` before `esef_document_contact_candidates`, and a document extracted in between
would lose its e-mail domains for good at this version. Every attempted document leaves a row:
real domains, or one marker row (`registrable_domain = ''`, `extraction_status` `empty` /
`failed` / `timed_out` + `error_message`), so a document is attempted once per version.

## Operating it

- **Re-run everything:** bump the version constant, deploy; the sensor drains the stale set.
- **Re-run some documents:** launch `esef_domains_clickhouse` with
  `source_document_ids: [fxo_id, ...]` (stale or not), `retry_failed: true` or
  `refresh_existing: true`.
- **Failed / timed-out documents** stay at their status until a version bump or an explicit
  re-run: `retry_failed: true` re-extracts every document whose rows at the current version
  are `failed` / `timed_out` (added to any `source_document_ids`); read `error_message` first.
  A run interrupted mid-way leaves its unwritten documents stale and the next run retries them.
- **Config bounds:** `workers` 1–8 (default 4), `batch_size` 1–1000 (default 250 documents per
  replace), `parse_timeout_seconds` 60–3600 (default 300), `max_documents` ≤ 100,000 (newest
  `period_end` first), `retry_failed` and `refresh_existing` (default false).
- **Missing vs transient downloads:** only a package missing from the object store
  (`NoSuchKey` / 404 / `NotFound`, `common.resources.is_missing_object_error`) or a SHA-256
  mismatch is the document's own problem and becomes a `failed` marker row. Any other
  object-store or disk error fails the run: Dagster's retry policy re-runs it, the batches
  already written persist and the rest stay stale.
- **Circuit breaker:** when at least max(10, ceil(25 % of the batch)) documents of a batch time
  out or lose their child "without a result" (an OOM-kill), the run raises "host incident
  suspected" and writes nothing for that batch; the retry policy and the sensor's one-hour
  failure cooldown take over. Fewer -- a handful of genuinely pathological documents -- still
  get their markers.
- **Throughput:** ≈ 3.5 h for the whole corpus at 4 workers: lxml only, but every document
  pays ≈ 1 s of child bootstrap and the downloads are serial. Packages are downloaded at most
  `2 × workers` ahead.
- **Sensor skip reasons:** each `SkipReason` names the three counts driving the decision --
  stale documents, runs in flight, and failures in the last hour -- e.g. `esef_domains: 0 stale
  documents, 0 run(s) in flight, 0 failed in the last hour`.

## Adding an extractor

1. A light module with the pure per-document function and a `<NAME>_EXTRACTOR_VERSION`.
2. A migration with the table (`source_document_id`, `package_sha256`, `lei`, `period_end`,
   `fiscal_year`, `extraction_status`, `extractor_version`, `source_run_id`, `extracted_at`,
   `resolved_at DEFAULT now64(3)` plus the product's columns) and its `se_esef_<table>` view
   (`tables.SE_ESEF_VIEWS` + `country_views.build_se_esef_view_sql`).
3. An asset that selects stale documents (copy `stale_documents_sql`), runs the function through
   `run_in_child_with_timeout` in a pool, and writes with `replace_document_rows`.
4. A sensor (copy `domains_sensor.py`).
5. Consumers read the view, never the artifact.
