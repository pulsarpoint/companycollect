# Crawl normalization parser and assets

Approved scope: delivery step 2 of [the source design](../../../src/dagster_v3/defs/website_crawl/docs/website-crawl-normalization-design.md). Execute inline with test-first checks. Migration 452 is already deployed.

## File boundaries

- `defs/website_crawl/normalization/tables.py`: explicit migration-pinned column/type contracts and DuckDB staging types.
- `parser.py`: deterministic attempt, classification and page-observation projections.
- `structured.py`: structured scalar and JobPosting/legacy vacancy projections.
- `load.py`: bounded pending catalog selection, exact archive reads, staging/export and scan-last publication.
- `assets.py`: non-subsettable nine-output multi-asset and normalization-only job.
- `tests/test_website_crawl_normalization.py`: synthetic schema-faithful fixtures and parser behavior.
- `tests/test_website_crawl_normalization_load.py`: real ClickHouse/DuckDB publication and asset contract tests.

## Steps

- [x] Write failing parser tests for first-page classification, all activities, provenance, nested scalar values, structured jobs, failures, malformed/unknown schemas and mismatched identities.
- [x] Implement `parse_attempt(source, payload, normalization_id, revision, run_id)` returning complete row dictionaries for all nine migration-owned tables. No crawling, model inference, company matching or schema DDL in the producer.
- [x] Test and implement catalog anti-join by source revision/parser version, exact archive lookup, bounded force replay and publication after all detail writes/count checks. Serialize the operation with a Dagster pool and existing PostgreSQL advisory-lock resource for non-Dagster callers.
- [x] Test native type round trips through explicit DuckDB staging and the existing ClickHouse append exporter. Use disposable tables, including interrupted publication and zero-row corrected outputs.
- [x] Wire all nine table-named assets with metadata into `website_crawl_normalized`; register a job selecting only this multi-asset. No sensor yet: automated discovery/backfill is delivery step 3.
- [x] Run parser/loading/migration regression tests and `uv run dg check defs`.
- [x] Deploy definitions, normalize bounded saved 2525.se/100.se attempts, compare archive/result counts and show that a repeat run skips already published revisions.
- [x] Commit explicit paths and update the source design with validation evidence and remaining steps.

## Runtime contract

Default batch limit 25 (maximum 100), optional crawl type/domain/request/attempt filters, and a default-false force flag restricted to explicit domain/request selection. Source catalog errors, unsupported JSON schemas and inaccessible uploaded archives fail visibly without publishing empty replacement data. Terminal attempts without an uploaded archive can still normalize the catalog's saved sections and diagnostics; a later source revision makes them eligible again.

Each corrected attempt uses a new UUID and increasing scan revision under the shared publication lock. Rows load into an in-memory DuckDB stage with explicit schema before ClickHouse inserts. The small bounded stage is not a durable dataset and needs no asset of its own. Details publish first, with native counts checked against expected rows, then one scan row commits visibility. A retry skips already published work or writes a new unpublished UUID; it never truncates historical tables.

## Validation completed

- Parser/loading/schema/result regression suite: 88 tests passed. After final parser refinements, all 17 parser tests passed again. Ruff checks pass for the implementation and new tests.
- `uv run dg check defs`: all YAML and definitions loaded successfully.
- Targeted Ansible hot-sync: 33 OK, 11 changed, zero failed; the running supervisor was preserved.
- Live Dagster run `df4436b0-b398-4abc-b062-8569ab9c1a3a`: SUCCESS, five saved attempts across 100.se and 2525.se.
- Verified native published counts: 5 scans, 5 profiles, 20 activity occurrences, 40 pages, 394 contact occurrences, 0 identifiers, 2,394 structured scalar properties, 783 links, 0 structured jobs. Counts match deterministic archive replay.
- The latest 100.se attempt is `needs_review`; its retained usable basic-info attempt remains `skip_crawling`.
- Repeat run `fd6f85e9-2e37-469a-9128-ed132d28bc90`: SUCCESS, materialization metadata reports zero attempts and zero rows written. Raw scan count remains five, all revision 1.
- Real target validation corrected two integration assumptions before deployment: the named collection includes the `company-crawls/` prefix, and this Dagster version requires list-of-string config plus validation rather than list-of-Literal config.

Automatic scheduling and unstructured job extraction remain the next tasks. No crawls or LLM calls were launched by this validation.
