# Company actions

Sweden → Processing (`/admin/se/processing`) is the home for global workflows.
Each area has Sync inputs and Full processing. The launch dialog posts to
`/admin/se/company-actions`; the server launches one global Dagster job.
Table filters and selected company IDs are never forwarded. Companies and People
pages keep browsing and company-specific reviewer actions.

The page reads live status and history from Dagster, refreshing every 5 seconds
while runs are active and every 15 seconds otherwise (only while visible). Active
runs are fetched separately from the latest 20 runs per action; last successful
sync and processing are fetched independently. Recent history shows completed runs
with workflow/status filters, timestamps in Stockholm time, duration, operator
and a Dagster link. Model and prompt settings stay in SQLite.

Launches check both jobs for the workflow and reject queued/active duplicates with
HTTP 409. A per-workflow lock covers lookup and submission on the single backoffice
server. This is not a distributed lock or a restriction on direct Dagster launches.
If the application is replicated, coordinate that lock across replicas. Dagster
lookup failures prevent submission; the UI retains last known rows and disables
affected actions until a successful refresh. Successful submissions immediately
appear as queued runs while live status catches up.

| Area | Sync inputs | Full processing |
| --- | --- | --- |
| Company info | `se_company_basic_info_sync_job`: five SQL source extractors | `se_company_basic_info_refresh_job`: same sync, LLM descriptions, publish |
| Finance | `se_company_financial_sync_job`: Ratsit USD conversion, four source extractors | `se_company_financial_refresh_job`: same sync, publish |
| Addresses | `se_company_address_sync_job`: four source extractors, normalize | `se_company_address_refresh_job`: same sync, warm geocodes, publish |
| People | `se_company_person_sync_job`: four source extractors, normalize, input hashes | `se_company_person_refresh_job`: same sync, LLM matching, publish |
| Domains | `se_company_domain_sync_job`: Wikidata, ESEF, Common Crawl source observations and precedence | `se_company_domain_refresh_job`: same sync → `se_company_domain_verification` → `se_company_domain_publish` |

Sync reads already ingested source tables; it does not download or crawl original
registries. Source extractors retain their existing change detection. Publication records the Dagster run ID in entity history tables. Domains visits
company pages directly; the other entities retain their existing bucket scans.
Domains requires ClickHouse migration 000408 before deploying its readers or assets.

Info full processing selects a saved SQLite LLM profile and an explicit description
processing limit (default 5,000; range 1–1,000,000). This limit only bounds the LLM
step; SQL source sync and publishing have global scope. Info retains its built-in
Dagster company-description prompt, two-source eligibility gate and observation
cache. Its existing scope selects new/changed source descriptions; changing the
chosen model alone does not force every previously processed company into scope.
The LLM asset uses a serial concurrency pool to prevent overlapping cache misses.

People full processing selects a saved SQLite profile and People prompt. The exact
prompt and model configuration travel in the request; a stale prompt revision is
rejected. Only match new or changed input defaults to enabled. See
[People actions](people-actions.md) for hash comparisons and forced matching.
Finance and Addresses never use an LLM, so they show no model or prompt fields.

Address sync reads SCB, Bolagsverket, Ratsit and ESEF suggestions with 10,000 companies
per source page, then incrementally normalizes them. Full processing warms location
keys in batches of 150,000 using the existing OSM workbench, then folds all 64 buckets
with 20,000 companies per page. Cache hits reuse geocodes; the existing matcher handles
misses. The OSM snapshot freshness check remains included. Warming and publishing share
the OSM concurrency pool with reference-data refreshes. These actions do not download
a new OSM extract; the separate weekly OSM/geocoding job retains that responsibility.

Only the credential environment variable's name travels in the run configuration;
its value must be available on the Dagster worker. Run tags record operator, request
ID, country, area and operation. A successful submission displays the Dagster run link.

The redundant Info, Finance and Address extract jobs and stopped weekly schedules were
removed. Each area now has exactly two named global jobs. Internal partitioned and
targeted Info/Finance/Address folds remain for dependencies, repair and reviewer corrections.

Domain full processing verifies all eligible uncertain or conflicting associations across
all companies. There is no run-wide request cutoff or request-limit field. Each request
checks one company–domain pair; successful saved answers are reused when unchanged, and
failed answers can retry. Pagination bounds reads without limiting the total work. The
launcher does not set an output-token cap; the provider determines the default output allowance. Saved
Domain prompts live in the `domain_prompt` SQLite table, managed at
`/admin/settings/domain-prompts`. A selected prompt revision must still match on submit;
its complete text and the model configuration travel with the Dagster request.

Domain processing reuses exact evidence/prompt/model fingerprints. Renaming a prompt or
incrementing its revision without changing its text does not cause paid processing.
Verification can be disabled: source precedence and current-evidence cached answers still
apply. Reviewer decisions always win. Unresolved associations remain inactive and appear
in the review queue. Provider failures and invalid answers retry on the next run while
successful answers are reused. Removing the previous fixed token cap preserves reuse of
saved successes with the same evidence, prompt and other model settings.
Each model attempt stores the request snapshot, prompt, raw response, usage and run ID.

The live `company_domains_resolved` view exposes the SE entity plus current review rules,
so backoffice results and reviews are immediate. The broader `company_domains` and
`company_domain_current` serving tables are updated by the existing serving refresh.
Migration 408 imports existing associations and reviews before this cutover.

Domain scoring is a separate Dagster asset, `se_company_domain_verification`, shown as
“Score domains with the LLM” in task progress. It stores each verdict, confidence (0–1),
reason and cited evidence in the existing verification table. The publish step consumes
those saved results without calling a model. Both receive the same profile so publication
cannot use an answer from an older prompt/model when verification is capped.

LLM attempt results are stored in ClickHouse `corpscout.se_company_domain_verification`:
one append-only row per attempt with company/domain, exact input and prompt, hashes,
verdict/confidence/reason, citations, model, token usage, raw response, error, time and
Dagster run ID. The current published association is in `corpscout.se_company_domain`,
and publication changes are in `corpscout.se_company_domain_history`. Recovered saved
responses retain their original attempt status; the usable verdict is reflected in
publication and its history. SQLite stores the saved prompt templates.
