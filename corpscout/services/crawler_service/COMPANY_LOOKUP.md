# Website-to-company lookup

Backoffice **Crawls → Crawl → Find company** submits one domain for basic site information and a
company match proposal. Registry queries remain read-only; findings are published separately. Select the processing LLM and optionally Jev independently
for homepage classification and company candidate ranking. The country selector
currently supports only Sweden.

The asynchronous `POST /v1/company-lookups` endpoint accepts the same encrypted
LLM profiles and bearer authentication as standard crawler requests:

```json
{
  "request_id": "company-lookup-example-001",
  "domain": "example.se",
  "country": "SE",
  "llm": {
    "provider": "openrouter",
    "base_url": "https://openrouter.ai/api/v1",
    "model": "your-processing-model",
    "api_key_encrypted": "your-encrypted-profile-key"
  },
  "config": {"max_pages": 4, "max_model_calls": 20}
}
```

Use a real saved profile through Backoffice, which verifies and encrypts its
credentials. An optional `decision_llm` must be a saved Jev profile. Select its
steps with `decision_tasks: ["site_eligibility", "company_match"]`, or either step
individually. For older clients, omitting `decision_tasks` when providing Jev
continues to select classification only. Other steps use the processing model.
Keyless processing models
are supported. Country is required, case-insensitive, and values other than `SE`
are rejected. The domain may be a bare hostname or homepage URL, without a path,
query, credentials or port; collection starts with HTTPS.

The response is HTTP 202 with the standard crawler job and its `Location` header.
Reusing the same request ID and payload returns the existing request. Poll
`GET /v1/crawls/{request_id}/status`, then read
`GET /v1/crawls/{request_id}/result` when terminal. Job `purpose` is
`company_lookup`; a completed job can have either `matched` or `not_found` output.
An infrastructure or model failure produces a failed job, never a successful
negative match.

## Processing and evidence

1. Fetch the homepage and classify its primary purpose. Classification describes
   the site; it never excludes a company-operator lookup. Shops, news, forums,
   streaming, mixed and other content sites may be the company's primary website.
   Classification keeps product and navigation context; a retailer's corporate
   footer or organisation number does not change its `online_store` category.
   Jev uses a classification-only question, separate from the full-research gate.
2. For every category, read the homepage/footer and follow only same-site contact,
   about and legal links up to
   the page limit (default 4, hard maximum 10). Link ranking is deterministic;
   catalog/product pages, articles, streams and discussion threads are not followed,
   even when their titles mention a company or contact. Jev does not select links
   in this profile. Normal browser retries,
   robots.txt checks and CAPTCHA handling remain available.
3. Remove known cookie-consent overlays, scripts and navigation from model text;
   preserve footer and targeted legal/contact excerpts. Discover labelled Swedish
   organisation/VAT numbers directly and search their IDs before model extraction.
   `registration_evidence` records their exact source and quote without asserting
   ownership. Extract the website operator's names, organisation numbers and address, with
   source URLs and verbatim quotations. Reject facts whose quotations cannot
   be verified against fetched page text. Validate facts independently so a malformed
   fact cannot discard valid evidence. Blank explanation strings are logged and
   omitted without discarding supported facts. Retry an empty/unverified extraction once.
   Identify the store, publisher or platform operator, not product brands,
   marketplace sellers or subjects mentioned in articles, videos and user posts.
   Customers, suppliers, designers and payment providers are excluded by instructions;
   explicit designer credit quotations are rejected by code too.
4. Search only `corpscout.se_companies_serving`. This is the existing Swedish
   company-serving table, rather than a new `se_companies` table. Run at most
   ten directly discovered identifier queries, at most three additional extracted-ID
   queries and three name queries; each returns at most ten candidates. Skip duplicate
   ID queries, and skip fuzzy name queries when verified IDs already resolve.
   Name search normalizes Swedish legal suffixes and punctuation and ranks
   fuzzy candidates, including `Investmentaktiebolaget` versus `Investment AB`.
   Exact normalized names rank first. Each query has a 15-second server deadline and uses
   parameterized SQL with `readonly=2`.
   Explicit Swedish VAT identifiers (`SE` + ten digits + `01`) are normalized
   to the underlying organisation number before searches and conflict checks.
   The original value and its normalized ID remain in the identity evidence;
   equivalent identifiers produce one query. Bare twelve-digit sole-trader IDs
   retain all digits. See [Skatteverket's identifier format](https://www4.skatteverket.se/rattsligvagledning/edition/2026.7/324657.html).
5. Load all candidate industries in one read-only, parameterized query from
   `se_company_industry_display_current`, joined to version-specific labels in
   `nace_categories`. Preserve primary and secondary codes. Validate each label
   against its declared NACE revision; codes absent from that revision, conflicting
   labels and unknown versions are displayed and traced, but excluded from model
   decisions. This is a reference consistency check, not verification of the source
   classification version. The existing serving data contains mixed revisions.
6. Ask the processing model (or Jev, when selected) to compare the observed identity with the retrieved
   candidates. Require confidence of at least 0.85 plus a matching organisation
   number, an explicit legal name unique among returned candidates, or an exact
   name and corroborating street address. Check every supported evidence route
   independently of the model's chosen `basis` label. Preserve confidence gating,
   ambiguous legal-name rejection and conflicting operator-ID rejection. Raw discovered
   IDs are search hints, not automatically verified operator facts. Conflicting verified identifiers or
   unsupported proposals return `not_found` with the reasons and candidates.
   Same-name ambiguity reports the conflicting registry IDs and the missing evidence.

   Extract up to five verbatim `business_activity` facts alongside identity facts.
   Keep industry data out of the identity-matching prompt. Compare activities with
   reliable primary/secondary industries in a separate small model request, with
   no company names or addresses in its candidate context. Skip this request when
   activities or reliable codes are absent. This separation prevents missing NACE
   data from causing the identity model to reject an otherwise supported match.
   Missing activity, missing codes,
   invalid/unavailable model checks and uncertain versions are neutral. Holding/head-office
   classifications cannot create a conflict merely because the website describes
   operating subsidiaries. A supported industry conflict rejects a name-only
   proposal for review; it never overrides a verified operator organisation number.
   Industry agreement alone cannot establish identity or resolve duplicate legal
   names without corroborating identity evidence.

Confidence is an uncalibrated model estimate, not a measured probability. A
proposal is not an automatically established company-domain association. Missing
footer evidence, an inaccessible contact page, alternate registered names or
several operating entities can legitimately produce no confirmed match. This
endpoint is for single-domain tests; it does not enqueue all `.se` domains.

Jev produces a distribution over the actual candidate IDs and an explicit `none`
option. `candidate_assessments` contains each model score and its typed evidence
assessment; `no_match_probability` contains the `none` score. These are choice
probabilities from the provider, not its separate distribution-concentration
`confidence` field, and are not calibrated against this company-matching task.
An invalid/missing distribution fails explicitly. Evidence checks still apply.
See the [OpenRouter Jev API guide](https://openrouter.ai/blog/tutorials/how-to-use-jev/).

## Output and tracing

Output includes `status` (`matched`, `not_found`, `failed`), `found`, `company_id`,
`confidence`, `site_type`, `crawl_scope: "operator_identity"`, detailed `reasons`, classification, verified identity
facts, directly observed registration evidence, candidates, the raw assessment,
optional Jev ranking, `industry_assessments` (model and validated status, reasons,
versioned codes and reference consistency), database searches, pages, model usage
and timings. Backoffice shows an expandable NACE industry section with quoted
website activities, candidate codes and excluded mapping warnings. `models`
records the requested and actually reported processing model names; a mismatch
also generates a warning in the trace and is visible in Backoffice. Unmatched
and failed results have null company ID and confidence. Failed results retain
whatever evidence and searches were collected before the failure.

Debug tracing is always enabled. The normal file-backed debug API and Backoffice
trace viewer show browser/model steps, prompts, responses and progress. Every
database query records SQL, parameters, query ID, table, returned candidates,
HTTP status, success/failure and elapsed milliseconds. Authentication secrets
are redacted from traces. Use `GET /v1/crawls/{id}/debug?download=true` for the full
trace, or `stream=true` for live SSE. To retrieve a specific attempt's output,
use `GET /v1/crawls/{id}/debug?attempt=1&result=true`.

Every lookup now includes `site_info` and `site_info_result`, the portable
`company-crawl-result/1.2` output used by the basic crawler. Homepage classification
uses the same schema, exact-quote validation, description, purpose, operator and
business-activity fields. Optional Jev classification runs in addition to this
summary. A shop/news decision does not stop operator identity research. A successful
basic result survives later registry/matching failures. Unavailable pages and failed
classification produce an explicit failed/needs-review basic result, not a success.

Results and traces remain in local attempt files. `company-lookups.sqlite3` uses
WAL and synchronous FULL for durable batch inputs and result delivery receipts.
Findings are appended to ClickHouse; company-domain links are never accepted here.
The result API overlays the current `publication` receipt and
`persisted_to_database` value. Historical version-1.0 tests are not backfilled.
Local artifacts are still not uploaded to S3 by this lookup path; the normal crawl
normalizer can read basic information/pages/observations from the inline catalog.
Both result families refer to the same model calls: lookup usage includes the basic
classification, so their costs must not be added together.

## Configuration

Set `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, and `CLICKHOUSE_PASSWORD` on the crawler.
Prefer credentials restricted to reading `corpscout.se_companies_serving`,
`corpscout.se_company_industry_display_current`, `corpscout.nace_categories`, and
`corpscout.company_domains_resolved` (active association precheck).
The Ansible equivalents are `crawler_service_clickhouse_url`,
`crawler_service_clickhouse_user`, and `crawler_service_clickhouse_password` in
the ignored `ansible/secrets.yml`. Apply migration 459 before enabling publication.

The status API advertises `company_lookup_available`; Backoffice disables the
Find company option when the database endpoint is not configured. Ordinary
crawl profiles do not require this database configuration.


## Durable batches and Dagster

`POST /v1/company-lookup-batches` accepts `batch_id`, `domains` (1–1,000 distinct
hostnames), `country`, `llm`, optional `decision_llm`/`decision_tasks`, and `config`
with the same limits as a single lookup. `input_id` and `run_id` carry lineage.
Example: use the single-lookup body above, replace `request_id` and `domain` with
`"batch_id": "se-lookup-001", "domains": ["example.se", "another.se"]`.

The complete request is committed to SQLite before acknowledgement. A dedicated
pool of **four** lookup workers, configurable with `CRAWL_LOOKUP_CONCURRENCY`,
takes the next site as soon as a worker finishes. `CRAWL_LOOKUP_TIMEOUT_SECONDS`
bounds matching (default 900 seconds). A combined full crawl retains its normal
collection limits; saved findings survive subsequent cleanup failures. Single lookup tests share this
pool. Each site keeps the standard request history and full debug trace.

`GET /v1/company-lookup-batches/{batch_id}` returns total, dispatched, processed,
matched, not_found, failed, skipped, state and publication error. All attempted
sites, including failures, must have durable local outcomes before batch delivery
starts. Basic rows and child findings are inserted first, lookup summaries last.
`published` means all inserts were acknowledged. HTTP/ClickHouse failures retain
the SQLite records and retry delivery; no model calls repeat. ReplacingMergeTree
attempt identities make lost-ack replay logically idempotent (query `FINAL`).

`DELETE /v1/company-lookup-batches/{batch_id}` cancels active work and excludes
unstarted members. Completed findings are still published. Resubmitting the exact
batch re-admits its unstarted members after model validation; completed/failed
attempts are preserved. Use a new execution for an intentional recrawl. Conflicting
inputs under an existing batch ID return 409. Service restart marks interrupted
attempts failed and continues unstarted entries; already saved results are recovered.

Company matching is an optional extension of **basic and full crawling**. There is
no separate ClickHouse input table or Dagster input queue. The existing
`website_site_info_requests` and `website_full_crawl_requests` tables (and their
`_current` views) expose `match_company` (default false), `company_country` (SE for
now), and `skip_company_matching_if_mapped` (default true). Dagster execution
settings may override them; the selected settings are frozen on resume and enter
the content work key, so a basic-only success cannot satisfy a matching request.

For direct tests, `POST /v1/crawls` accepts the usual basic/full request plus:

```json
{"company_lookup": {"country": "SE", "skip_if_mapped": true, "max_pages": 4}}
```

The requested crawl always runs. Its homepage classification and any captured
identity pages are reused. Matching can follow additional About, contact and legal
pages (four identity pages total by default, maximum ten), even when the full
crawl stops a shop/news site's content crawl. Processing and Jev share the request's
model-call budget across collection and matching. Jev `company_match` remains an
optional decision step. Matching never inserts a company-domain association.

When enabled, the precheck queries `company_domains_resolved` for **active SE
connections**, including current review overrides. A proposal is insufficient.
Existing connections produce `already_mapped`, a list of existing company IDs and
the logged query. A failed precheck is a matching failure, not a skipped match;
the ordinary crawl is retained. Turn the skip option off to re-evaluate a mapping.

The existing Dagster basic/full result assets submit **200** matching requests at a
time to `POST /v1/crawl-batches`, with `entries` containing `request` (the complete
CrawlRequest), `crawl_type`, `input_revision`, and `work_key`. The batch also carries
`batch_id`, `input_id` (existing task ID), and `run_id`. The crawler commits this
payload to SQLite and uses the four-worker pool described above. Its
`GET /v1/crawl-batches/{batch_id}` endpoint is polled **every two seconds**; DELETE
cancels the batch. Legacy company-lookup batch endpoints remain compatible.

The next batch waits for `published` and confirmation of both ordinary and matching
rows in ClickHouse. Matching assets belong to the `website_crawl` group and receive
materialization events after publication. The full variant also reports a basic
result observation. Resume uses the existing crawl task/execution. Saved-model
runs register the batch with Backoffice's normal LLM monitoring and cancellation.
Matching failures count as failed task outcomes without discarding a good crawl.

## ClickHouse publication

Apply migration `000459_corpscout_website_company_lookup_results` after the existing
crawl migrations. The crawler writes:

- `website_site_info_results`: standard basic result for every matching attempt.
- `website_full_crawl_results`: standard full result when full collection was requested.
  Both carry `company_matching_status`; crawl success stays independent of matching.
- `website_company_lookup_results`: country/domain/attempt outcome, proposal,
  confidence, existing company IDs when skipped, model usage, costs and lineage.
- `website_company_lookup_candidates`: alternatives, registry attributes and NACE checks.
- `website_company_lookup_evidence`: quoted identities and observed identifiers with URLs.
- `website_company_lookup_searches`: SQL, typed parameter values encoded in a string
  map, query IDs, timing, status, returned company IDs and errors.

`website_company_lookup_results_latest` includes the newest attempt for each
country/domain. `website_company_lookup_proposals` selects only matched rows from
those newest attempts. A newer unresolved attempt never resurrects an older proposal.
Joining these proposals into `se_domains` or another country list is a separate step.

Set `CLICKHOUSE_RESULTS_URL`, `CLICKHOUSE_RESULTS_USER`, and
`CLICKHOUSE_RESULTS_PASSWORD` on the crawler. Use a separate writer with `INSERT`
only on the six result tables above, against the same database Dagster reads.
The corresponding Ansible variables start with `crawler_service_clickhouse_results_`.
No runtime DDL or registry/domain-table writes are performed. Dagster owns the
existing crawl queue membership and request tables.
Single tests retain pending local delivery when a writer is unavailable; batch
admission requires a configured writer endpoint. Backoffice shows pending/error
status until publication succeeds.
