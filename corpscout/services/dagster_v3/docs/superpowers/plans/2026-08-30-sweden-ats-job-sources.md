# Sweden ATS job sources: Greenhouse, Lever, Ashby, SmartRecruiters

**Goal:** Add current job postings from Greenhouse, Lever, Ashby, and
SmartRecruiters as four independent Dagster sources. Each source owns its pipeline,
RustFS data, DuckDB staging database, and ClickHouse tables. Jobs are linked to Swedish
companies where the link is defensible.

**Explicit boundary:** do not merge, cluster, deduplicate, rank, or roll up jobs across
sources. Do not change the Platsbanken pipeline or its existing company hiring views. If
the same vacancy appears in Platsbanken and three ATS sources, all four source records
remain visible.

**Initial scope:** boards discovered from known Swedish company domains. Retain every
posting from those boards and mark whether its workplace is confidently in Sweden. A
later project can decide whether foreign-employer discovery or a coherent cross-source
job list is useful.

---

## 1. Why board discovery belongs inside each source

None of the four providers exposes a global Sweden feed. Every request requires an
employer-specific identifier:

| provider | board identifier | current public read shape |
|---|---|---|
| Greenhouse | `board_token` | `GET https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs?content=true` |
| Lever | site slug plus global/EU instance | `GET https://api[.eu].lever.co/v0/postings/{site}?mode=json` |
| Ashby | job-board name | `GET https://api.ashbyhq.com/posting-api/job-board/{name}?includeCompensation=true` |
| SmartRecruiters | `companyIdentifier` | paginated list plus one detail request per posting |

Official references:

- [Greenhouse Job Board API](https://docs.greenhouse.io/job-board.html)
- [Lever Postings API](https://github.com/lever/postings-api)
- [Ashby Job Postings API](https://developers.ashbyhq.com/docs/public-job-posting-api)
- [SmartRecruiters Posting API](https://developers.smartrecruiters.com/docs/posting-api)

Each source therefore owns a board registry and its company links. Do not create one
shared ATS board registry in the first release.

### Source-owned board tables

Every provider gets two migration-owned tables:

```text
corpscout.se_<provider>_boards
  board_key, api_region, careers_url,
  lifecycle_status, first_seen_at, last_seen_at,
  last_success_at, consecutive_failures

corpscout.se_<provider>_board_company_links
  board_key, company_id, company_domain,
  evidence_url, match_method, match_confidence,
  review_status, reviewed_by, reviewed_at
```

Boards and company links are separate because one corporate board can represent several
Swedish legal entities. Jobs may still be ingested from an ambiguous board, but they may
receive a `company_id` only from an approved, unambiguous link.

### Discovery rollout per provider

1. Begin with a versioned, reviewed seed file containing 5–10 real Swedish boards. Every
   seed includes the Swedish `company_id`, official company domain, board URL, and
   evidence URL.
2. After the source pipeline works, discover candidates from provider links found on
   verified company websites and careers pages.
3. Treat web-technology detections as additional evidence, not company-identity proof.
4. Send shared-group, redirected, ambiguous, or conflicting candidates to review.
5. Never fall back to employer-name matching.

---

## 2. Four independent source packages

Create:

```text
defs/sweden_greenhouse/
defs/sweden_lever/
defs/sweden_ashby/
defs/sweden_smartrecruiters/
```

Each package owns its HTTP client, board discovery, raw bucket, DuckDB file,
concurrency pool, normalization SQL, ClickHouse exporter, jobs, schedules, fixtures,
tests, and required design doc.

| provider | DuckDB | pool | RustFS bucket |
|---|---|---|---|
| Greenhouse | `data/sweden_greenhouse_source.duckdb` | `sweden_greenhouse_duckdb` | `source-sweden-greenhouse` |
| Lever | `data/sweden_lever_source.duckdb` | `sweden_lever_duckdb` | `source-sweden-lever` |
| Ashby | `data/sweden_ashby_source.duckdb` | `sweden_ashby_duckdb` | `source-sweden-ashby` |
| SmartRecruiters | `data/sweden_smartrecruiters_source.duckdb` | `sweden_smartrecruiters_duckdb` | `source-sweden-smartrecruiters` |

Do not add a `jobs_common` orchestration package in the first slice. Small mechanical
utilities may be shared later only after repeated implementations prove the boundary.
Provider requests, schemas, lifecycle rules, assets, tables, and jobs remain source-owned.

---

## 3. Separate ClickHouse table family per source

Each provider writes only its own tables:

```text
se_<provider>_board_snapshots
  one row per attempted board snapshot, including result and RustFS provenance

se_<provider>_job_ad_versions
  one immutable content version per provider posting

se_<provider>_job_ad_events
  first_seen, content_changed, closed_by_absence, and reopened transitions

se_<provider>_job_ad_current
  one current active source posting, directly queryable by company_id

se_<provider>_job_ad_location_versions
  one primary or secondary location per job version

se_<provider>_job_ad_compensation_versions
  salary, equity, or bonus components where that provider publishes them
```

That produces these independent families:

```text
se_greenhouse_*
se_lever_*
se_ashby_*
se_smartrecruiters_*
```

There is no `se_ats_*` combined table and no source-neutral job table.

Useful fields may be similar, but each source keeps its own column contract and provider
vocabulary. Every job version/current row carries:

```text
source_job_ad_id, version_uid,
source_published_at, source_updated_at, first_seen_at,
headline_original, description_text_original, detected_language,
job_url, application_url,
company_id, company_match_status, company_match_method,
source_url, source_object_key, source_run_id, ingested_at
```

Provider-specific fields remain provider-specific. For example, Greenhouse offices,
Lever commitments, Ashby compensation tiers, and SmartRecruiters functions do not need
to be flattened into a lowest-common-denominator schema.

Raw payload JSON and payload hashes remain in DuckDB/RustFS, not ClickHouse. Stable
version/event UIDs remain in the source table because they make that source's snapshot
history idempotent; they are not cross-source job IDs.

---

## 4. Snapshot lifecycle within one source

The APIs expose currently published jobs rather than reliable deletion streams. Within
each provider, a successful board poll is an authoritative current snapshot:

- a new provider job ID emits `first_seen`;
- changed source content appends a new version and `content_changed` event;
- a previously active source ID absent from a successful snapshot emits
  `closed_by_absence` with `is_end_estimated=1`;
- a closed source ID returning later emits `reopened`;
- failed, unauthorized, rate-limited, malformed, or incomplete fetches emit no closures
  and preserve that provider board's last good current rows;
- a schema-valid HTTP 200 response with an empty job list is authoritative and may close
  all jobs previously active on that board.

The valid-empty case is a documented exception to the normal “refuse empty replacement”
guard. An empty employer board is a valid state; an invalid response or empty result for
the entire provider is not.

No lifecycle event from one provider changes a row in any other provider or in
Platsbanken.

---

## 5. Standard asset chain per provider

Each provider starts as a non-partitioned current-snapshot pipeline:

```text
sweden_<provider>_board_snapshots_s3
  -> sweden_<provider>_raw_duckdb
  -> sweden_<provider>_normalized_duckdb
  -> sweden_<provider>_clickhouse
```

The final asset publishes only `se_<provider>_*` tables. It has no dependency on
Platsbanken, another ATS source, or `company_job_history/current/monthly`.

Implementation rules:

- Use `dlt.sources.helpers.requests` for retry/backoff, explicit connect/read timeouts,
  a descriptive user agent, and bounded concurrency.
- Store each board response before normalization at
  `snapshots/board=<key>/retrieved_at=<timestamp>/jobs.json`, plus a run manifest.
- Resume retries from verified RustFS checkpoints.
- Load JSON through DuckDB's set-based JSON reader and normalize with provider-owned,
  set-based SQL. Do not transform job rows in Python loops.
- Put every asset opening a provider DuckDB file in that provider's single-slot pool.
- Keep schedules stopped until a manual canary materializes successfully.
- If one provider exceeds 500 boards or a 30-minute run, split its registry into
  deterministic hash batches. Do not create one Dagster partition per employer.

Successful boards may advance while failed boards retain their previous current rows.
Fail the provider run for a provider-wide outage, schema drift, or inability to write the
manifest. Surface individual board failures through that provider's asset checks and
review queue.

---

## 6. Provider implementation order

### 6.1 Greenhouse

Build first because public GET endpoints require no authentication and the list endpoint
can include descriptions, departments, and offices with `content=true`.

Deliverables:

- `se_greenhouse_*` migrations and module;
- board-token parser for Greenhouse-hosted URLs;
- one list request per board, with optional bounded detail enrichment for fields absent
  from the list response (`first_published`, application deadline, pay ranges);
- source-owned mapping for offices, departments, language, `updated_at`, URLs, and
  multiple locations;
- active, empty, changed, invalid-token, and failed-fetch fixtures;
- manual job and design doc; no enabled schedule.

Gate: 5–10 reviewed Swedish boards, idempotent rerun, correct company links, and no false
closures after an injected board failure.

### 6.2 Lever

Add global and EU endpoints explicitly; the site slug alone does not identify the
instance. Page with `skip`/`limit` until exhausted.

Deliverables:

- `se_lever_*` migrations and module;
- parser for `jobs.lever.co` and `jobs.eu.lever.co` URLs;
- paginated published-postings fetch;
- source-owned mapping for `categories.allLocations`, ISO country, commitment,
  department, team, workplace type, URLs, and salary range;
- separate HTML/plain-text retention;
- the same lifecycle and failure fixtures as Greenhouse.

Gate: global and EU fixtures/live boards tested; pagination produces no gaps or duplicate
Lever source IDs.

### 6.3 Ashby

Ashby returns all currently published jobs for one job-board name in one request and can
include compensation data.

Deliverables:

- `se_ashby_*` migrations and module;
- parser for `jobs.ashbyhq.com/{board}`;
- snapshot with `includeCompensation=true`;
- source-owned mapping for primary/secondary locations, remote/workplace type,
  department, team, employment type, publication time, URLs, and compensation tiers;
- retain `isListed=false` direct-link postings in Ashby source history but exclude them
  from `se_ashby_job_ad_current` unless product requirements say otherwise.

Gate: multiple locations and multi-component compensation round-trip through Ashby's
tables without flattening or loss.

### 6.4 SmartRecruiters

Build last because it combines pagination with job-detail calls, and current public docs
support API-key/OAuth authentication. R0 must establish whether target public boards are
anonymously readable. If credentials are required, use environment-variable resources
without committing secrets.

Deliverables:

- `se_smartrecruiters_*` migrations and module;
- parser for `careers.smartrecruiters.com/{companyIdentifier}`;
- paginated active-postings list followed by bounded detail requests;
- source-owned mapping for release date, location, remote flag, department, function,
  employment/experience level, URL, description sections, and compensation;
- 401/403 handling distinct from 404 board removal and 429/5xx transient errors.

Gate: authentication behavior is documented from a canary, and a failed detail call
cannot close a previously active SmartRecruiters posting.

---

## 7. The only cross-domain relationship: job to company

Provider payloads generally do not expose a Swedish organization number. The trusted
identity bridge is:

```text
se_companies.company_id
  -> reviewed official company domain
  -> provider board link found on that domain
  -> provider board key
  -> provider job
```

Publishing rules for every source:

- write `company_id` only for an approved board-company link;
- retain unmatched and ambiguous jobs in that provider's source tables with an empty
  `company_id` and explicit match status;
- quarantine boards connected to several Swedish legal entities unless a provider field
  proves a per-job entity mapping;
- never infer company identity from employer-name similarity;
- retain evidence URL, match method, confidence, and review status;
- connect jobs to companies with `company_id`; do not create canonical jobs or
  cross-source source-membership records.

For workplace scope, preserve all structured locations and add a source-specific
`is_sweden_workplace` plus classification basis:

1. explicit Sweden country code/address country;
2. structured Swedish city/region;
3. lower-confidence textual location fallback;
4. remote/global/unknown remains unknown.

---

## 8. Platsbanken and presentation boundary

Leave these existing Platsbanken-owned outputs unchanged:

```text
se_platsbanken_job_ad_*
company_job_history
company_job_current
company_hiring_monthly
sweden_platsbanken_company_jobs_clickhouse
```

The four new sources must not write to, rebuild, depend on, or change those tables and
assets.

For company presentation, query each source independently by `company_id`:

```text
Platsbanken       -> existing Platsbanken company-job tables
Greenhouse        -> se_greenhouse_job_ad_current
Lever             -> se_lever_job_ad_current
Ashby             -> se_ashby_job_ad_current
SmartRecruiters   -> se_smartrecruiters_job_ad_current
```

The UI/API may show separate source sections or concatenate responses while preserving
each row and its source label. It must not suppress duplicates. A vacancy appearing in
several sources is intentionally shown several times.

Do not change the existing Platsbanken-derived `has_job_ads` flag or monthly hiring
metrics in this project. Source-specific counts can be displayed beside each source.
Any future combined list, canonical job identity, or deduplicated analytics requires a
separate design and explicit product decision based on measured source data.

### Compensation

Store every monetary component in the provider's compensation table as native
min/max/currency/interval. Add USD values in a separate source-owned conversion asset
using the shared exchange-rate client, keyed on publication date or first-seen date when
publication is absent. Equity percentages and textual bonus claims remain distinct and
are never coalesced into salary.

### Translation

Retain provider language and original text. Bulk description translation is deferred.
Any later translation uses the shared `text_translations` cache while preserving source
table ownership; do not add `_en` columns directly to provider job tables.

---

## 9. Tests and acceptance criteria

### Per-source contract tests

For every provider:

- board URL parsing and identifier validation;
- pagination where applicable;
- retryable 429/5xx versus terminal 401/403/404 handling;
- schema-valid empty board versus malformed empty response;
- raw manifest/object checksum and replay;
- set-based provider-specific normalization;
- stable version/event UIDs within the provider;
- idempotent rerun creates no duplicate provider versions/events;
- content change creates one new provider version;
- successful absence creates one estimated closure;
- failed snapshot creates no closure;
- reappearance creates one reopen event;
- approved company link writes the exact `company_id`;
- ambiguous company link remains unlinked;
- ClickHouse migration column order matches the provider export tuples.

### Source-isolation tests

- every provider asset writes only its own `se_<provider>_*` tables;
- provider jobs and schedules contain no assets from another source;
- provider assets have no Platsbanken dependency;
- Platsbanken definitions and serving tables remain unchanged;
- the same fixture vacancy loaded into two sources remains present in both source tables;
- there is no combined `se_ats_*`, canonical-job, or deduplication asset/table.

### Required repository verification

```text
uv run pytest tests/test_sweden_<provider>_*.py
uv run pytest tests/test_sweden_job_source_isolation.py
uv run pytest tests/test_clickhouse_migrations.py
uv run dg check defs
```

Run the full Dagster suite before enabling a provider. Apply only that provider's
migrations, deploy with its schedule stopped, then materialize its explicit asset chain
through the production Dagster UI.

### Live acceptance metrics per source

Every provider materialization exposes only its own metrics:

- boards attempted/succeeded/failed/empty;
- jobs active/new/changed/closed/reopened;
- raw bytes and request count;
- approved, ambiguous, and missing company attribution;
- Sweden workplace versus remote/unknown;
- runtime, retries, 429s, and schema failures.

Release a provider from canary only when:

- rerunning an unchanged snapshot is idempotent;
- failed/incomplete fetches close no jobs;
- every linked company in the canary is auditable from stored evidence;
- provider table counts reconcile with sampled career pages;
- that provider can be deployed, materialized, disabled, and rebuilt without touching any
  other job source.

---

## 10. Scheduling and rollout

1. Keep all four schedules default-stopped.
2. Run Greenhouse manually for seven days on the reviewed seed set.
3. Enable Greenhouse daily only after its canary passes.
4. Repeat independently for Lever, Ashby, and SmartRecruiters in that order.
5. Run each provider's board discovery weekly and snapshots daily, staggered by at least
   15 minutes. Choose exact cron times after measuring that provider's runtime.
6. Alert independently on provider-wide failure, schema drift, stale success, abnormal
   job-count collapse, or a growing review queue.

Suggested release units:

| release | independently shippable result |
|---|---|
| R0 | provider-specific reviewed board seeds, fixtures, and live API/auth spike |
| R1 | Greenhouse pipeline and `se_greenhouse_*` tables |
| R2 | Lever pipeline and `se_lever_*` tables |
| R3 | Ashby pipeline and `se_ashby_*` tables |
| R4 | SmartRecruiters pipeline and `se_smartrecruiters_*` tables |
| R5 | source-specific company presentation/read paths, without combined storage |
| R6 | automated discovery expansion, source-owned USD conversion, and schedules |

Rough engineering size is 10–17 focused days plus a seven-day canary per provider.
Legal/API access decisions and reviewed board-company links remain the main external
variables.

---

## 11. Explicit non-goals

- no cross-source job table or view;
- no canonical job ID;
- no merging, clustering, ranking, or deduplication;
- no changes to Platsbanken company hiring views or rollups;
- no cross-source hiring counts;
- no application submission endpoints;
- no candidate or applicant data;
- no scraping LinkedIn, Indeed, Jobbsafari, or provider admin interfaces;
- no employer-name-only company matching;
- no one-Dagster-partition-per-company design;
- no exact closing time inferred from a failed request;
- no bulk description-translation backlog;
- no foreign-employer discovery in the first release.
