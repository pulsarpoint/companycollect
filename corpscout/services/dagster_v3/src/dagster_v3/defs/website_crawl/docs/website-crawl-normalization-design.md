# Crawl normalization for company enrichment

Status: approved, 2026-09-25. Database contracts are implemented and deployed in migration 000452 (nine tables and 19 views).
Parser/assets, incremental processing and offline job extraction are subsequent tasks.

## Existing source boundary

`website_full_crawl_results`, `website_jobs_crawl_results`, and
`website_site_info_results` contain crawl attempt identity, outcome, timestamps,
archive location, and JSON columns (`site_info`, `pages`, `page_observations`,
`model_usage`). Their corresponding Dagster assets ingest completed responses.
The complete `company-crawl-result` JSON and HTML documents remain in object storage.

Inspection of the saved 2525.se full crawl found 20 documents containing
`company-page-observations/1.1`: page metadata, JSON-LD entities, contacts,
identifiers, document/financial links and observation errors. It contains no
extracted company/person/product records. Some observation sections are null
because their analysis is deferred. Null must not become a claim of absence.
Full-crawl classification may live under `crawl.site_gate.profile` when
`crawl.site_info` is absent. The parser must account for both representations.

## Recommended boundary

Normalize the existing observations deterministically, without another crawl or
LLM call. Keep website observations distinct from accepted company facts.
Company matching and enrichment consume these tables in a subsequent phase.

Purpose, site types, research profiles and business activities are already explicit
fields of `SiteClassification`; normalization must preserve all of them rather
than leaving purpose and activities embedded in a description. Jobs are also a
required output: normalize structured vacancy records where present and add a
separate offline extraction asset for vacancies available only in saved page text.

Prefer incremental normalization over parsing JSON in every Backoffice query:
it gives typed contracts, reusable joins and a independently replayable parser.
Ordinary SQL views alone are simpler but repeat parsing and do not provide
per-attempt parsing status. Expanding the crawler to write all downstream tables
would couple browsing availability to warehouse transformations.

## Proposed ClickHouse datasets

All names below are in `corpscout`. Each durable table has a same-named Dagster
asset in the `website_crawl_normalized` group.

| Table | Grain and principal columns |
| --- | --- |
| `website_crawl_scans` | One published normalization per `(crawl_type, domain, request_id, attempt)`. Requested/final URL, input revision, work key, crawl run ID, started/finished timestamps, service state, crawl status, successful flag, stop/error reason, page count, archive path, schema/parser version and normalization ID. |
| `website_crawl_site_profiles` | One first-page classification per attempt. Source URL, evidence status, crawl decision, `site_types Array(String)`, `research_profiles Array(String)`, `purpose_original Nullable(String)`, operator name, description, evidence, `full_crawl_all` and whether eligibility was overridden. Missing historical override values remain unknown. |
| `website_crawl_business_activities` | One reported business activity per classification and source array index. `activity_original`, source URL, first-page scope, and classification evidence. Retain the evidence as classification-level evidence; do not pretend the source supplied a separate citation for each activity. These are activity descriptions, not inferred NACE codes. |
| `website_crawl_pages` | One page per attempt and page ID. Requested/final/canonical URL, HTTP/fetch/observation status, fetched time, title, description, language and page errors. |
| `website_crawl_contacts` | One contact occurrence per page and source locator. Contact type, observed value, raw value, extraction source and structured-data entity reference where present. This is not yet a company contact. |
| `website_crawl_identifiers` | One identifier occurrence per page and source locator. Identifier type, observed value, raw value and extraction source. No unverified conversion into a registry-company relationship. |
| `website_crawl_structured_data` | One scalar property value per page, entity and property path. Entity ID/types, script index, entity path, property path, array index, value type and typed string/number/boolean value columns. Numbers also retain their exact source text (`value_number_original`) so floating-point representation cannot destroy a large identifier. Preserve explicit null and parse status; do not copy arbitrary raw JSON blobs into this table. |
| `website_crawl_links` | One observed link occurrence per page and source locator. Target URL, label, link category (document, financial, canonical, alternate-language, other metadata link), declared document type/language/relationship when present. |
| `website_crawl_jobs` | One observed vacancy occurrence per source page and source record/entity locator. Source job ID and URL when supplied, title, employer, location, department, employment/workplace type, description, posted/expiry dates, source method and evidence. All unsupported fields remain nullable. Preserve the posting's employer independently from the crawled domain. |

Every detail row carries the attempt identity, `normalization_id`, page ID/source
URL where applicable, and a stable occurrence identifier. Evidence locators are
explicit source coordinates rather than inferred ownership. Raw HTML, page text
and complete JSON remain in the archive; downstream text extraction can retrieve
them through the scan's archive path and page ID.

Additional fields must follow the observed schema, not guesses about what a
future crawler might emit. Monetary strings in structured data remain source
properties; interpreting prices or company financials requires a later semantic
step with explicit currency and conversion. Preserve original-language text.
Translation uses the existing shared translation service/cache when enrichment
needs it; normalization does not start paid model tasks.

## Purpose, activities and jobs

Keep **site purpose**, **site type** and **business activity** distinct. For example,
a site can have type `online_store`, purpose "sell vehicle accessories online"
and business activity "retail of vehicle accessories". Store all supported
classification fields even when the full crawl is skipped. Preserve null purpose,
empty activity arrays and `unknown` site type faithfully when classification
cannot establish them. Research profiles are crawler classifications, not industry
codes. Mapping activities to NACE is a separate downstream classification step.

The existing `Job` model defines employer, title, location, department, employment
type, workplace type, job URL and evidence. Its existence does not mean current
crawl archives contain extracted jobs: current collection deliberately defers
job interpretation to offline processing. The jobs table must consume both jobs
crawls and full crawls; basic info can contribute explicit structured postings
only when actually present in the captured page.

The deterministic normalization pass projects JSON-LD `JobPosting` entities and
supported legacy job-record sections into `website_crawl_jobs`. Preserve source
record identity and locators; do not invent a posting from a careers navigation
link. Extended fields such as description and posted/expiry dates are populated
only from sources that supply them. Preserve unparseable source date text as well
as a nullable parsed date. Vacancy absence and expiry must not be inferred from
an incomplete, failed or unrelated crawl.

An independently runnable `website_crawl_job_extractions` asset/table will hold
text-derived vacancy records for pages without adequate structured data. It uses
the already archived page content and the existing selected/verified encrypted
LLM configuration, with model/extractor version, evidence and per-attempt
extraction outcome. It does not perform another crawl. Its publication is separate
from deterministic normalization: model failure cannot hide successfully normalized
pages or structured vacancies. A serving view reconciles the two sources using
explicit source IDs/URLs while retaining evidence and provenance from each.
Do not collapse distinct vacancies merely because they share a title and employer.

Expose extraction coverage (`not_processed`, `completed`, `partial`, `failed`)
separately from vacancy count. Zero structured job rows means no structured
postings were found, not "the company has no open jobs". This offline extraction
step is part of the requested crawl-data scope, before company enrichment.

## Processing and publication

1. Read the three existing result tables as the authoritative attempt catalog.
   Prefer their stored JSON for available sections; fetch the exact archived
   object for missing sections such as full classification. Never scan an entire
   object-store prefix on each run.
2. Select attempts without a published normalization for the current parser
   version and source revision. Support bounded domain/request/attempt filters
   for validation and repair. Process all crawl types through the same parser.
3. Validate source identity and supported schema versions before producing rows.
   Unknown schemas, malformed JSON and archive failures fail the normalization
   attempt visibly and remain retryable; they never become successful empty data.
4. Stage a bounded batch with explicit DuckDB schemas. Use the existing
   ClickHouse resource/export patterns; migration files own production DDL.
   A shared Dagster pool serializes access to staging and publication.
5. Load detail rows under a new normalization ID. Publish the corresponding
   `website_crawl_scans` row only after every detail dataset for that attempt has
   loaded and its row counts have been checked. Detail serving views join the
   published normalization ID, so interrupted loads are invisible. A retry or
   parser correction cannot retain deleted facts from an older normalization.
6. Emit materialization counts for each table, including valid zero-row outputs.
   Failed crawl attempts still produce scan/page diagnostics when their source
   data is readable. Their available observations remain inspectable as history.

A single coordinated multi-asset operation can parse a batch once and represent
all nine deterministic table outputs in Dagster. It is deliberately non-subsettable:
publication covers the complete normalized attempt, not independently selected
fragments. The final schema migration must include the serving views and
publication identity, with tests exercising interrupted writes and parser replay.

Migration 000452 implements `ReplacingMergeTree(normalization_revision)` on scans,
keyed by `(domain, crawl_type, request_id, attempt)`. A serialized writer must
assign a strictly increasing UInt64 revision for each corrected normalization of
an attempt and use a fresh normalization UUID. A retry of the identical publication
may reuse its revision/UUID. Conflicting content must never reuse a published UUID.
Detail tables deduplicate by attempt identity, UUID, page ID and row index.
Readers use `FINAL` through the serving views, so correctness does not depend
on background merges.

Every detail table has a `_published` view containing the published normalization
of every attempt, including failed-crawl diagnostics, and a `_current` view.
`website_crawl_scans_latest` resolves the newest attempt per domain/crawl type;
`website_crawl_scans_latest_usable` selects successful completed deep results
(and skipped basic-info classifications). A third selector,
`website_crawl_scans_latest_profile`, allows successful, source-matched skipped
classifications for all crawl types. Site-profile and business-activity current
views use that selector: a newly recognized shop updates its classification even
when its full crawl is skipped. Other current views retain the preceding usable
deep dataset. The source scan identity makes this distinction explicit to consumers.

The migration does not create text-job extraction tables yet: their separate
outcome/publication contract belongs with that extractor's migration. The
deterministic jobs table exposes structured extraction coverage through the scan's
`structured_jobs_status`; its default `not_available` never means no open jobs.

Use incremental batches, not one dynamic Dagster partition per website/attempt.
The existing result catalog plus parser/source version determines outstanding
work. A sensor should run the normalization-only job when pending attempts exist,
with bounded batches and overlap prevention. It must discover results even when
an upstream batch run fails after writing some completed attempts. Existing
archives use the same route for backfill; normalization must never launch a crawl.

## Current data and company enrichment

Expose latest attempt and latest eligible data separately. A failed, partial or
needs-review attempt does not replace a previous usable snapshot. A successfully
classified skipped site can provide basic site information, but must not be
treated as a completed company full crawl. Selection is per domain and crawl
type; one-page basic info must not replace a full-crawl dataset.

Retain attempt history in the normalized tables. Each current-data view selects
one eligible attempt and its published normalization, avoiding accidental unions
of stale contacts across attempts. Partial observations remain available in
history; using them for enrichment requires an explicit downstream policy.

Later company-enrichment assets join through the existing company-domain
associations and preserve field-level evidence. A contact found on a shared
website, a supplier mention or an Organization JSON-LD object is not proof that
the value belongs to every company associated with that domain. Entity ownership
and confidence need resolution before publishing accepted company fields.

## Delivery sequence and acceptance

1. Database migrations: typed tables, publication/current views, schema-contract
   tests. No replacement of current crawl-result tables or UI readers.
2. Parser and table assets: fixtures for full/jobs/basic results, 2525.se,
   failures, skipped sites, purpose, multiple site types/activities, missing
   sections, JSON-LD graphs and nested arrays, structured JobPosting records and
   supported legacy jobs; preserve first-page evidence scope and nullable fields;
   verify native ClickHouse types and provenance against a disposable database.
3. Incremental automation and backfill: demonstrate idempotent replay, a failed
   detail-table write, late-arriving results, source/schema changes, and safe
   resume without rerunning browsing or LLM work. Validate Dagster definitions.
4. Offline job extraction: archived-text parsing, verified LLM selection, explicit
   evidence and coverage, retry-safe publication and reconciliation with structured
   vacancies. Test careers links without postings, multiple vacancies on one page,
   duplicated sources, partial extraction and failed-model retry.
5. Company-enrichment consumers: a separate change specifying field selection,
   domain/entity matching, confidence and provenance before updating company info.

First live validation should normalize only the named 2525.se attempt and
representative 100.se success/failure attempts, compare normalized counts against
the archives, and confirm that the last usable data survives a newer failure.
Enable ongoing automation only after that bounded validation succeeds.
