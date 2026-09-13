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

Stale = an available document (package archived, facts present, `period_end <= today()`)
whose rows are missing or carry another `extractor_version`. Every attempted document leaves a
row: real domains, or one marker row (`registrable_domain = ''`, `extraction_status` `empty` /
`failed` / `timed_out` + `error_message`), so a document is attempted once per version.

## Operating it

- **Re-run everything:** bump the version constant, deploy; the sensor drains the stale set.
- **Re-run some documents:** launch `esef_domains_clickhouse` with
  `source_document_ids: [fxo_id, ...]` (stale or not) or `refresh_existing: true`.
- **Failed / timed-out documents** stay at their status until a version bump or an explicit
  re-run (look at `error_message`); a run interrupted mid-way leaves its unwritten documents
  stale and the next run retries them.
- **Throughput:** lxml only, ~1-3 s per document per worker plus the child's import cost;
  4 workers ≈ 2-3 h for the whole corpus. Packages are downloaded at most `2 × workers` ahead.
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
