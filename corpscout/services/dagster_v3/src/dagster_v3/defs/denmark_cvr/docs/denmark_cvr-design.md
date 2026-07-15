# Denmark DataCVR search source design

## 1. Source overview

- **Country / registry:** Denmark — DataCVR at Virk.
- **Module:** `defs/denmark_cvr/`.
- **ClickHouse tables:** None in this first raw-capture slice.
- **Dataset:** `https://datacvr.virk.dk/soegeresultater` bootstraps a browser session; the page then calls `/gateway/soeg/fritekst` and returns JSON. The public search flow does not require credentials in the prototype. Its licensing terms are not encoded in the API response and must be assessed separately before redistributing the captured data.
- **Entity keys:** company `cvr`, person `enhedsnummer`, production unit `pNummer`.
- **Observed size:** highly dependent on the substring search term. The 2026-07-15 probe reported 622,176 results for `a`, 1,370 for `æ`, and 3,574 for `0`.

## 2. Ingest mode

- **Chosen:** partitioned browser-backed API capture.
- **Partition key:** one DataCVR free-text search term from `0`-`9`, `a`-`z`, `æ`, `ø`, and `å`.
- **Semantics:** DataCVR treats these as substring searches, not name-prefix filters. Raw partitions therefore overlap. This is intentional; the next phase will normalize entity rows in DuckDB and deduplicate them by their source identifiers.
- **Backfill policy:** `BackfillPolicy.multi_run(max_partitions_per_run=1)` with the `denmark_cvr_search` pool.
- **Why:** the browser session is required to establish the request context used by the gateway. There is no bulk file in the prototype being migrated.

## 3. Loading and raw storage

- `DenmarkCvrSearchResource` launches CloakBrowser once per partition, opens the public result page, and performs sequential credentialed JSON POST requests in that page. Requests use the prototype's proven fixed size of 3,000 and advance the sequential `sideIndex` page number by one.
- Each response is validated with the discriminated `SearchResponse` model before its exact UTF-8 body is written to S3-compatible object storage.
- Page objects use `denmark_cvr/search/search_term=<term>/run_id=<run-id>/page=<index>.json` in bucket `source-denmark-cvr`.
- `manifest.json` is written last and records page keys, advertised total, aggregate entity-type counts, and byte counts. It is the completion marker for the run-scoped partition capture.
- A schema-invalid response is preserved as `.invalid.json`, then the asset fails without writing a completion manifest.
- If the gateway returns an empty page before the first page's advertised total is exhausted, the completed manifest and materialization metadata record `is_truncated=true`.

## 4. Deferred normalization

- DuckDB staging, entity normalization, and deduplication are explicitly deferred to the next phase.
- Deduplication will use stable source identifiers (`cvr`, `enhedsnummer`, and `pNummer`), not names or payload equality.
- No empty DuckDB marker, dbt project, ClickHouse table, or export asset is introduced in this slice.

## 5. Translation, contacts, and currency

- Danish status, company-form, industry, and role values require a later translation inventory. Proper names and addresses must not be translated.
- Structured contact fields are present for company and production-unit results (`email`, `telefonnummer`). Canonical contact/domain extraction is deferred until normalized company rows exist.
- The search result contains no financial amounts, so native-currency and USD conversion are not applicable.

## 6. Scheduling and operations

- No job or schedule is registered. Operators materialize individual search-term partitions or launch a throttled manual backfill.
- Logs and Dagster metadata contain only search terms, page indices, object keys, counts, and byte sizes. Raw response bodies, names, addresses, email addresses, phone numbers, cookies, and browser state are never logged.

## 7. Issues found

- A bounded live probe proved that a one-character query is substring-based: only 4/5 sampled `a`, 1/5 sampled `æ`, and 3/5 sampled `0` results began with the query term. The source is therefore modeled as overlapping search-term partitions.
- Large searches can return hundreds of thousands of advertised results. The first response's total determines the expected page count with `ceil(total / 3000)`.
- A bounded probe in the standalone Cloak project confirmed that `sideIndex` values 0 through 4 each return 3,000 results for the `a` partition. Pages can contain repeated source identifiers, which are intentionally preserved for later DuckDB deduplication.
- CloakBrowser 0.4.10 resolves, imports, and launches successfully with this project's Python 3.14 runtime.

## 8. Verification

- Focused behavior and definition tests: `tests/test_denmark_cvr.py`.
- Definition validation: `uv run dg check defs` and `uv run dg list defs --json`.
- Manual validation: materialize one low-volume search term after configuring the shared object store, then compare the manifest counts with its page objects without logging their contents.
