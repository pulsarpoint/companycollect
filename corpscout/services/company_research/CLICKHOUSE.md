# Website results and S3 queries

Version 0.26 adds the following objects through Corpscout migrations 420/421:

| Object | Purpose |
| --- | --- |
| `corpscout.website_crawl_results` | Result history with each JSON section in a separate column. |
| `corpscout.website_crawl_results_latest` | Latest result per website and stage (`crawl`, `analysis`, `error`). |
| `corpscout.website_crawl_results_s3_archive` | External view querying the actual compressed JSON objects in S3. New uploads appear without importing them. |

The S3 view uses `s3()` and pins the query settings needed for correct filtered
reads on ClickHouse 26.5.1, following the existing Brave history mapping. It does
not copy or delete S3 objects. See the official
[S3 table function](https://clickhouse.com/docs/sql-reference/table-functions/s3)
and [JSONAsString format](https://clickhouse.com/docs/interfaces/formats/JSONAsString).

## Identity and JSON columns

`domain` is the input hostname: lowercase, IDNA encoded, with its trailing dot and
initial `www.` removed. Real subdomains remain distinct. `website_url` retains the
normalized target. Externally hosted job pages stay attributed to the requested
website; redirected URLs remain in the crawl metadata.

`result_id` hashes the canonical complete JSON, so reimports use the same key. It
is not the compressed S3 checksum. `ReplacingMergeTree(ingested_at)` is ordered by
`(domain, result_kind, result_id)`. Use `FINAL` for deduplicated history; the latest
view already does this. It orders by source completion time, so a late import of
an older result cannot replace a newer result. New raw crawls do not hide existing
analysis. The latest view returns a complete result per stage, not a profile
assembled from sections of different runs.

JSON sections use validated `Nullable(String)` columns, matching existing Corpscout
conventions and preserving source nulls, dotted keys, evidence and references.

| Columns | Source |
| --- | --- |
| `company_profile`, `company_contacts`, `locations`, `products_services`, `people`, `company_relationships`, `jobs`, `technology_signals`, `certifications_compliance`, `document_links`, `explicit_negatives` | Corresponding arrays under `records`. |
| `site_info`, `company_overview`, `site_profile`, `coverage`, `technology_classification`, `catalog`, `config`, `model_usage`, `entities`, `objectives`, `discovery`, `missing_information` | Corresponding objects; legacy `usage`/`technology_catalog` map to `model_usage`/`catalog`. |
| `crawl` | Complete crawl manifest for a crawl bundle. |
| `pages`, `documents`, `unvisited_links`, `technology_summary`, `external_links`, `errors` | Corresponding arrays, including HTML in `documents`. |
| `metadata` | Remaining source fields, including scalars and unknown sections. |
| `page_observations` (migration 422, v0.27) | Per-page deterministic observations projected from crawl inputs or preserved analysis output. See [fields and queries](PAGE_OBSERVATIONS.md). |

SQL `NULL` means absent or explicitly null. JSON `[]` means the source supplied an
empty array; it does not establish company-wide absence. Raw crawls have
`jobs = NULL` until LLM extraction runs. Imports do not infer facts or deduplicate
source records. Check `status`, coverage and saved instructions for completeness.

Other columns include `request_id`, `run_id`, `revision_id`, `schema_version`,
`status`, `started_at`, `finished_at`, `ingested_at`, and `source_path`. The last
is the exact S3 `_path` for remote imports or an absolute local path. Standalone
results without a service request ID retain an empty `request_id`.

Migration 426 adds nullable `attempt` to the S3 view and reads request IDs from
both legacy `REQUEST_ID/result.json.gz` and current
`REQUEST_ID/attempts/0001/result.json.gz` paths. A request ID in the JSON takes
precedence over the path fallback. Legacy paths have `attempt = NULL`.

## Queries

```sql
SELECT domain, status, JSONLength(jobs) AS job_record_count,
       company_profile, products_services, jobs
FROM corpscout.website_crawl_results_latest
WHERE domain = 'novelic.com' AND result_kind = 'analysis';

SELECT JSONExtractString(job, 'data', 'title') AS title,
       JSONExtractString(job, 'data', 'job_url') AS job_url, job
FROM corpscout.website_crawl_results_latest
ARRAY JOIN JSONExtractArrayRaw(ifNull(jobs, '[]')) AS job
WHERE domain = 'novelic.com' AND result_kind = 'analysis';

SELECT domain, request_id, status, finished_at, source_path
FROM corpscout.website_crawl_results FINAL
WHERE domain = 'novelic.com' ORDER BY finished_at DESC;

SELECT domain, request_id, attempt, status, _path,
       JSONLength(result_json, 'documents') AS collected_pages
FROM corpscout.website_crawl_results_s3_archive WHERE domain = 'novelic.com';

SELECT JSONExtractString(document, 'url') AS page_url,
       JSONExtractString(document, 'html') AS html
FROM corpscout.website_crawl_results_s3_archive
ARRAY JOIN JSONExtractArrayRaw(result_json, 'documents') AS document
WHERE _path = 'crawls/company-crawls/REQUEST_ID/attempts/0001/result.json.gz';
```

Use an actual `_path` in the last query. Path filters narrow S3 reads; domain-only
filters can scan multiple objects because domains are stored inside the JSON.

## Backoffice viewer

At `/admin/crawls`, **S3 saved · View** opens the exact object from the attempt's
delivery receipt. The server uses a parameterized `_path` filter on
`website_crawl_results_s3_archive`; S3 credentials remain in the ClickHouse named collection.
The viewer shows crawl details, all collected pages, extracted observations, text,
simplified/rendered HTML source, links and full JSON. Downloading JSON reads the
same original object through ClickHouse. HTML is escaped rather than executed.
This works directly after upload without importing into `website_crawl_results`.

See [the viewer validation](S3_RESULTS_UI_VALIDATION_20260919.md) for the live
Novelic check and migration 426 verification.

## Setup and import

The ClickHouse infrastructure playbook enables named-collection administration for
the configured administrator. Application accounts receive no new grants. The
named collection keeps S3 credentials on the server, outside migrations and view
definitions. Provision it before applying migration 420:

```bash
uv sync --extra service
uv run --extra service company-research-clickhouse \
  --env-file ../../.env --env-file .env configure-s3 \
  --bucket crawls --endpoint-url http://rustfs:9000 --prefix company-crawls
```

The private environment supplies `CLICKHOUSE_NATIVE_URL` (or
`CLICKHOUSE_MIGRATE_URL`), `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and optional
`AWS_SESSION_TOKEN`. Use reader credentials; temporary credentials need rotation.
The collection `company_crawl_results` points to the bucket/prefix root, with URL
and credentials marked non-overridable. Provisioning disables query logging and
refuses to silently replace an existing collection. Use `ALTER NAMED COLLECTION`
explicitly for changes. Apply the migrations through the normal Corpscout migration
tool. Rollback removes ClickHouse data/definitions but retains S3 objects and the
operator-owned collection.

New uploads are immediately visible in the S3 view. Loading the stored section
table is **explicit for now**, with no background importer or change to the crawler's
JetStream acknowledgement contract:

```bash
uv run --extra service company-research-clickhouse --env-file ../../.env \
  import-file ./data/my-analysis/result.json

uv run --extra service company-research-clickhouse --env-file ../../.env \
  import-s3 --path crawls/company-crawls/REQUEST_ID/result.json.gz
```

`import-file` accepts multiple files, including `.json.gz`; repeat `--path` for
multiple S3 objects. The future LLM consumer can call `result_row()` and
`insert_result()` on its final JSON. Supported inputs are portable crawl results,
crawl manifests, current `company-research-result/1.0` analyses, compatible legacy
full-research results 1.4–1.11, and error results with a website URL. Diagnostic
files sharing a version string are rejected. New service errors include their URL
and completion time so they can also be mapped.

## Verification

`tests/test_clickhouse_results.py` checks lossless mapping. With
`CRAWL_CLICKHOUSE_TEST_URL` set, it creates a temporary database, applies the real
migrations and tests constraints, reimports, source-time ordering, stage separation,
hostname consistency and direct/filtered RustFS reads. It removes only that test
database. The S3 check uses the retained NOVELIC smoke objects.

The [live receipt](data/clickhouse-results-20260917/summary.json) records two NOVELIC
S3 bundles and one saved partial GLM analysis. The analysis contains 32 source job
records and 287 service records across pages, not those counts of unique jobs or
services. Its partial status and coverage remain intact. No new crawl or LLM calls
were needed to populate the table.

Validation passed: 237 package tests (12 existing/optional integration skips), all
11 ClickHouse mapping/integration tests with the live connection enabled, and 132
shared migration tests. Dagster definitions loaded successfully. The live migration
ledger is clean at 421; saved job/service sections match the source JSON exactly.

## Domain relationship context

The importer now projects `documents[].input.links` from portable captures into
`external_links`, with `context_version=website-domain-context-v1`. Source page,
page/link IDs, original headings/text, capture timestamp and HTML hash travel with
each occurrence. The normalized source host is the actual captured page's host,
not necessarily the originally requested website. The full original `documents`
remain stored unchanged. Reimport older bundles to populate this additive
projection without crawling or using a model again.

Migration 000425 and the Dagster `website_domain_relationships_job` consume this
projection for free-text explanations with checked citations. The first company
publication adapter covers Sweden and requires a unique active exact source-host
association. External destinations never become company-owned domains merely
because a link was observed. See
[website relationship design](../dagster_v3/src/dagster_v3/defs/website_relationships/design.md)
for the explicit import → bounded analysis flow and attribution limits.
