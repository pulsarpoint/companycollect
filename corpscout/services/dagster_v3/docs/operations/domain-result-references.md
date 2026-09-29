# Website references and country evidence rollout

Migrations `000466_corpscout_domain_result_references` and
`000467_corpscout_compact_domain_sources` were applied on 2026-09-28 with their
coordinated backfill and crawler/Dagster deployment. The compact source index is
active; affected queues, publishers and original automation states were restored.
The shared migration ledger is at 468, clean (468 belongs to the concurrent DNS rollout).
See [production rollout record](#production-rollout-record-2026-09-28) for checks and limitations.

The preparation sections below retain the implementation sequence and test history for the
[implementation plan](../../../../docs/superpowers/plans/2026-09-28-domain-crawl-and-proposal-references.md).
References there to local-only or pending deployment describe the pre-rollout state.

## Prepared schema

An ordinary, explicit `website_id String` is added to crawl results, company lookup
datasets, nine normalized tables, recurring requests, frozen inputs, submissions and
legacy saved results. New inserts must supply a nonempty ID. Writers must register and
validate the website and its central domain first: a CHECK is not a cross-table foreign
key. Resolve domain identity through `websites`; no copied canonical `root_domain` or
computed `domain_id` is added to these result tables.

Existing URL/hostname values remain request evidence. Latest/current result views now
select by website ID. Recurring request and frozen-input sorting keys append `website_id`
to preserve separate origins. `request_identity_version=1` retains the old domain-based
request ID for backfilled/in-flight work; new queue entries use version 2 (website-based).
Old parts retain empty IDs until controlled backfill. Since sorting-key columns cannot
be updated in place, rebuild these four queue tables in bounded stages at cutover, copying
all settings and receipts. Do not activate these readers while any old IDs remain empty.
Normalized pages retain their local `page_id` and gain nullable `resource_page_id` for the
actual visited central page; failure records without page URL evidence keep it null.

The empty `se_company_domain_sources` table is the prepared replacement for
`se_company_domain_suggestion`. It stores required registered domain IDs, optional website
IDs and the existing source claim, evidence, version and withdrawal fields. It does not
copy or confirm any existing suggestion. Stable company/source/slot ownership allows a
source to revise its claim without changing other sources. Latest-row reads must select
before filtering tombstones; background merges are not a uniqueness constraint.

Views expose stored website IDs. The S3 archive view only reads an explicit `website_id`
from JSON; it does not guess identities from hostnames. Immutable old JSON is resolved
through the backfilled saved result record. The local tests use an empty local S3 fixture.

Migration 466 adds no `source_kind`, no change to the company-filter query and no per-attempt
source-index publication. The existing global index still has its deployed schema until
its writers/readers are migrated. Its final replacement contains one domain/table
contribution, with no confidence, company identity or association state.

## Deployment prerequisites and order

1. Recheck migration numbering. Migration 465 is reserved by another domain-services task;
   coordinate the shared ledger rather than forcing it forward or backward.
2. Prepare all website-aware writers and backfill tooling before this DDL. The old
   normalizer rejects the extended schema; deploy the prepared version-2 writer with it. Old insert shapes are incompatible with required IDs.
3. Pause affected publishers, retaining completed results and queue membership. Confirm
   the `company_crawl_results` named collection exists; view creation can inspect it.
4. Apply preparation DDL using the migration runner. Register/backfill identities and
   resolve historical exceptions in bounded resumable ranges. Verify parent membership.
5. Copy claims with their evidence/tombstones, switch all adapters and the fold to the new
   source table, and verify existing reviews, candidates and outputs are preserved.
6. In a later forward migration, rebuild the summary keys and compact global index with
   their reader changes. The UI/filter must no longer read company decisions from
   `domains_sources` before its old columns are removed.
7. Reconcile late arrivals, activate compatible readers/writers, verify progress and retire
   legacy layouts only after parity checks. Keep updates recoverable from durable evidence.

`corpscout_domain_reference_writer` grants identity SELECT/INSERT on `domains`, `websites`
and `pages`. It has no source-index write, review, DDL or delete privileges and is not
assigned to any user by this migration. The role is for central registration, not for
publishing company summaries. Inventory replacements need coordinated registration guards.

The down migration refuses destructive rollback. Retain additive fields and evidence.
Rolling application code back to a writer that omits website IDs is not safe after DDL.
No production DDL, backfill or paid API calls are performed by the local verification.

## Local batch registration foundation

`corpscout/python/corpscout_identity/registration.py` computes domain/website/page IDs locally from normalized strings.
Even a new domain's ID is known before any insert. Registration then checks existing parents
for a complete batch and inserts only missing identities, in domain/website/page order.
It rejects malformed input, conflicting stored references and duplicate central identities.
It neither writes `domains_sources` nor changes company association decisions.

Queue admission and publication are wired locally. Dagster registers requested URLs in
batches before writing presets/frozen membership. The REST API verifies and registers
parents before creating jobs or durable batches. SQLite requests and result envelopes
carry the website ID; it is checked against the normalized URL. Requested identity stays
unchanged across cross-domain redirects. Newly visited pages and redirects are registered
from saved results before child-table writes. A failed registration leaves completed
SQLite payloads pending for publication; it does not repeat browser/model work.

The result HTTP destination independently verifies all referenced parents before writing.
Site info and detail rows precede the matching completion marker. Normalized details precede
the scan marker. Dagster confirms the request/website pair before advancing a batch.
The inventory fold reads published normalized crawl pages to update observation/fetch
metadata on previously assumed parents. Registration alone leaves existing metadata intact.
Backoffice history selects one origin, preserves exact request/attempt URLs, retains the
last good result alongside newer failures, and renders stored ClickHouse fields when no
S3 object exists.

Registration, the existing Swedish parent publisher and final domain/web inventory swaps
now use the same PostgreSQL transaction advisory lock in local code. Set `PROCESSING_PG_URL`
to the same processing database for all cooperating publishers. Missing configuration or
an unavailable lock blocks writes. `INVENTORY_LOCK_TIMEOUT_MS` defaults to 30000, with an
allowed range of 1–300000. Do not substitute a different graph-catalog database. No lock
table or PostgreSQL schema migration is required. The crawler service uses this same shared implementation; both packages include it in
their built wheels. Its native connection needs the registration role. The HTTP result
writer also needs SELECT on central domains/websites/pages to verify its destination.

Inventory staging happens outside the lock. Under the lock, a sorted join carries missing
current identities into the stage before exchange, so a concurrent registration is not
removed. The final scan's runtime on the full production inventory has not been measured.
Activation must also verify recovery from a lost coordinator/process with a ClickHouse
query still in flight: a PostgreSQL advisory lock is not a distributed transaction and
does not cancel that query. The completed-insert lost-acknowledgement test is narrower.

## Verification

Run from `services/dagster_v3`:

```sh
uv run pytest -q tests/test_domain_result_references.py tests/test_clickhouse_migrations.py
uv run pytest -q tests/test_website_crawl_normalization_load.py
uv run pytest -q tests/test_identity_registration.py tests/test_domains_inventory.py tests/test_web_inventory.py
uv run pytest -q tests/test_domain_sources.py tests/test_domains_company_filter.py tests/test_se_company_domain.py
uv run pytest -q tests/test_crawl_identity_pipeline.py tests/test_crawl_draft_queue.py
uv run dg check defs
```

The disposable ClickHouse tests exercise DDL replay, preserved old data/keys, rejection of
missing IDs, separate website identities under one domain, attempt view behavior, claim
updates/withdrawals, unchanged deployed source-index/filter behavior and archive extraction.
Parent validation is exercised through native registration and actual ClickHouse HTTP
publication. The historical backfill and country/index cutover remain pending; these local
tests do not mean the production pipeline can already use the new schema.

Validated locally on 2026-09-28: 7 reference/evidence integration tests, 134 migration
contract tests and 47 normalization regression tests passed. Dagster definition loading,
focused Ruff checks and whitespace checks passed. Production was not changed.

The subsequent registration foundation passed 33 identity/inventory tests and 67 existing
domain/country tests against disposable databases. Concurrent registration, registration
during inventory construction, lock timeout, retry after completed inserts, parent joins
and malformed-reference rejection are covered. Definition loading and focused Ruff checks
also passed. Production-scale timings, historical backfill and coordinated activation remain pending.

Task 3 verification:

Local validation: disposable ClickHouse/PostgreSQL tests cover admission across five
origins, native/HTTP destination verification, replay, inventory metadata folding and
invalid-parent rejection. The final pipeline/registration run passed 23 tests; the
reference/queue/inventory/freshness run passed its other 62 tests before the corrected
inventory fixture was rerun successfully. The broader migration/normalization/result suite
passed 249 tests, with its sole outdated assertion corrected and passing in that focused
rerun. Crawler lookup checks passed 36 tests with one opt-in live test skipped; service
checks passed 18; importer checks passed 8 with eight opt-in live checks skipped. Actual
ClickHouse HTTP publication was exercised by the disposable integration tests. Backoffice
history passed 26 tests and TypeScript checking. Dagster definitions loaded, both wheels
were built and their shared package contents verified, Ansible syntax checks passed, and
focused Ruff/whitespace checks passed. No production database or service was changed.

## Packaging and activation checks

Crawler API admission now requires `CLICKHOUSE_NATIVE_URL` and `PROCESSING_PG_URL`; these
must address the same inventories/coordinator as Dagster. Keep credentials in the existing
protected service environment. Native registration needs SELECT/INSERT on the central
inventory; the separate HTTP results credential needs SELECT there plus its existing
result INSERT grants. No crawler gets source-index, review or DDL permissions.

Dagster's full deployment synchronizes `../../python/corpscout_identity` relative to its
application directory, then rebuilds the installed package while stopped. Content-only
hot-sync refuses a shared-package change. Crawler immutable wheels include the package.
Do not hot-copy Python code into active jobs to activate this migration.

The preparatory DDL adds offset projections for website origin/ID and page URL/ID lookups.
Materialize them for historical parts during controlled cutover, then measure native batch
registration, HTTP parent checks, and website-scoped history queries on production-sized
data. Benchmarking and coordinator-loss recovery remain activation gates, not claims made
by the small disposable-database tests. PostgreSQL/ClickHouse writes are not atomic together.

## Country evidence rollout (Task 4, prepared locally)

The normal domain adapters and fold now target `se_company_domain_sources` and its
central-label read view. Do not activate before copying the legacy evidence. The explicit
`se_company_domain_sources_backfill` asset is preview-only by default and excluded from
normal jobs. Pause old source writers, copy in bounded company pages with `execute=true`,
resume with the acknowledged `after_company_id`, then compare current claim keys, removals,
source IDs, evidence, timestamps and review outputs. Retain Brave checkpoints and paid
verification rows. Newer destination versions survive replay of older source records.

Saved crawler matching is a fifth source. A new `not_found` withdraws its prior claim;
failed/cancelled/already-mapped attempts retain existing evidence without creating new
support. The adapter and fold make no browser calls; optional separate domain verification
keeps its existing policy. Do not confuse a proposed pair with a human-confirmed association.

The compact global source index is prepared in Task 5 below; the physical
summary/history/review key migration remains pending. Keep the legacy suggestion table until the coordinated copy, source switch and
parity checks are complete, then retire it through a forward migration. This task does
not authorize a production deployment or database deletion.


## Compact source-index rollout (Task 5, prepared locally)

Migration `000467_corpscout_compact_domain_sources` creates the empty
`domains_sources_next` table. It also changes `domains_company_filter` to read the country
summary plus live review rules; that query works before and after the index swap.
The deployed rich index remains intact. Do not activate the new index publisher against
that old schema: it explicitly refuses it.

At the coordinated Task 7 maintenance window:

1. Pause old country/index publishers and Backoffice review writes. Record high-water
   marks and check all known source partitions. Keep other durable queues/results intact.
2. Apply the preparation migration. Run `domains_sources_backfill` first with `execute=false`
   to inspect counts, then with `execute=true`. Default pages cover up to 100,000 domain IDs,
   capped at 1,000,000 per invocation. Resume with the logged acknowledged `after_domain_id`.
   A preview cursor is not an execution checkpoint. Replay a failed range from its previous
   acknowledged cursor. The copy retains all source names, minimum first-seen, maximum
   last-seen and the newest original publication metadata. It checks every copied field
   and both directions of parity. Missing parents block the copy.
3. Complete a full copy with writers paused. Check remaining count is zero, no orphan
   parents or extra destination ranges exist, and retain the backfill logs/cursors. A
   `complete` result after a manually supplied cursor only covers the suffix; it does not
   prove earlier ranges were copied. Replaying from the beginning validates all ranges.
4. With dependent readers paused, `EXCHANGE TABLES corpscout.domains_sources AND
   corpscout.domains_sources_next`. Then rename the old layout at `domains_sources_next`
   to `domains_sources_legacy`. Record each acknowledged step. On acknowledgment loss,
   inspect `system.columns` before retrying: the compact layout has exactly six columns.
   Never blindly replay EXCHANGE, which would reverse a successful swap. These commands
   are a cutover procedure, not run automatically by migration or backfill.
5. Activate compatible Backoffice/Dagster code together. Run the compact publisher for
   the selected contributors, refresh country-based filters and compare counts/reviews.
   Swedish refresh publishes only its partition. Verify inventories/crawls still progress.
   Keep `domains_sources_legacy` until parity, recovery and performance checks pass; a
   later explicit forward migration may retire it.

Source-index builds and the backfill share `domains_publish` with inventory/country
publication. Rejection/withdrawal retains discovery membership; company filters still
exclude inactive country associations. Two companies in Sweden collapse to one source
membership. A separate country contribution is preserved independently and does not by
itself create an associated company count. Register future country readers explicitly.

These changes are local. Production-scale copy/refresh benchmarks, table swap and deployment
remain pending. No live jobs were interrupted and no production table was altered.

Task 5 local verification: 157 final compact-index, migration, country-filter and evidence
checks passed; inventory/search/fold regressions passed in the preceding run. Backoffice
passed 14 tests and TypeScript checking. Dagster definitions include the independent
publisher and explicit backfill asset; definition validation and focused lint passed.
No production performance or cutover success is claimed by these disposable tests.


## Production rollout record: 2026-09-28

The crawler was stopped before DDL and SQLite enrichment. Affected Dagster work was
paused for the shared worker restart, retaining frozen membership, request IDs, settings
and saved results. Migrations 466 and 467 were applied using the normal migration runner.
The separate DNS deployment advanced the shared ledger to 468; it was verified clean.
Existing DNS definitions were preserved in the shared deployment.

Data checks and cutover:

- Validated 365,635 historical URL strings and registered their central parents.
- Rebuilt all 22 affected request/result/detail tables with nonempty website references.
  Both-direction comparisons and row counts passed before each exchange. All 364,070
  frozen crawler inputs survived; historical requests retain identity version 1.
- Registered observed pages and redirects; populated all 40 historical normalized page
  references. Central website/page projections cover every active inventory row.
- Copied 177,937 source claims for 109,172 companies, preserving evidence, source keys,
  timestamps and withdrawals. Both-direction metadata parity and parent checks passed.
  Corrected 38 noncanonical historical Wikidata hosts from their saved URL evidence;
  none had attached human rules or precedence overrides. Original evidence remains saved.
- Validated all 64 disjoint source-index ranges, including exact six-field parity and
  parent membership, then exchanged the index under the shared publication lock.
  The compact index contains 168,236,683 memberships: 119,711,896 graph-node sources,
  48,459,179 Common Crawl domain sources and 65,608 Swedish country-table sources.
- Ran the normal Swedish source-index publisher successfully and refreshed
  `domains_company_filter` without errors. The final completed refresh took about
  82 seconds under concurrent load; query performance still warrants observation.

Crawler and Dagster full deployments completed successfully, including the shared identity
package. The installed crawler registered an existing website and replayed one already
saved publication through the real ClickHouse writer successfully, without browser/model
calls. The HTTP result credential needed these verification grants in addition to its
existing result-write grants:

```sql
GRANT SELECT(domain_id) ON corpscout.domains TO crawler_lookup_writer;
GRANT SELECT(website_id, domain_id) ON corpscout.websites TO crawler_lookup_writer;
GRANT SELECT(page_id, website_id, page_url) ON corpscout.pages TO crawler_lookup_writer;
```

These grants do not permit central-inventory insertion or schema changes. Native parent
registration uses its separately configured credential and the shared processing-database
advisory lock. Future provisioning must preserve both connection configurations and the
HTTP verification grants.

Active replacement runs at final verification:

- Crawler: `b31a1682-d644-4ad6-b65d-6ebe60d10c83`.
- Brave: `a6911a12-aac6-4d66-a457-28d225b97b2b`.
- IP enrichment: `d06109ff-8580-46aa-a296-ff06e3adcb9c`.
- Sweden refresh: `2fb803dd-ac19-42a3-8b5a-2c50e8c2b892`.

All 17 other paused jobs have replacement runs. Initial Norway replacements failed before
processing because the operational resume helper omitted partition-range tags; corrected
replacements preserve the original range and backfill tags. The IP resume helper initially
dropped an explicit unlimited request setting; its corrected replacement preserves the
original setting and reuses completed work. All 76 sensor/schedule states match the saved
baseline (including the originally stopped ones).

Brave finished its recovered batch, published it, and started the next batch at roughly
10 entries/minute. The crawler continued its 200-entry batch, reaching 25 local outcomes
at the final inspection. Its batch remains in progress and is not yet fully published.
Per-site failures include inaccessible homepages, model timeout/format/length failures and
five company-search queries exceeding the existing 15-second ClickHouse limit; those
search errors were Code 159 timeouts, not missing-table or privilege errors. These outcome
failures are retained and were not silently retried or converted into successful scans.

Recovery data retained:

- Crawler SQLite backups under `/var/lib/company-research/migration-466-backup/`.
- ClickHouse `migration466_original_*` clones and exchanged `migration466_stage_*` tables.
- Old source index at `corpscout.domains_sources_legacy` and old country claim evidence.
- Protected local rollout receipts/checkpoints under `/tmp/domain-reference-rollout/`.

No legacy data was deleted and no destructive rollback was attempted. PostgreSQL advisory
locking does not make PostgreSQL and ClickHouse writes atomic; a lost coordinator with an
in-flight ClickHouse query still needs explicit operational reconciliation. Full inventory
replacement performance and fault-injection recovery are not established by this rollout.

Additional focused rollout checks passed: identity tests (20), crawler batch tests
(13, one opt-in skip), country-claim tests (7), restored DNS tests (19), Dagster definition
validation and whitespace checks. Actual production table/reference/projection audits and
service-health checks passed after activation.


## Direct source publication and persistent review (2026-09-29)

`se_company_domain_refresh_job` runs the five source adapters, precedence and
publication, then refreshes the source index and company filter. Publication depends
directly on the adapters and no longer waits for `se_company_domain_verification`.
That optional asset remains available independently, including its saved paid answers;
ordinary Backoffice domain processing does not select a model or call it. Existing
confidence and conflicting-evidence policies still distinguish proposals from accepted
associations. A fresh independent verification is followed by a separate publication.

`se_company_domain_rule` owns company/domain review decisions. `action='rejected'`
with `removed=0` means “Doesn’t belong” until an operator clears or changes the review.
A source withdrawal, changed evidence, a different source, or a higher source score
cannot release this rejection. Clearing a rule uses `removed=1`; source-level `removed`
continues to mean only that source has withdrawn its claim.

Migration 469 adds nullable `confidence_override` to the review rule. NULL uses the
calculated score; zero is a valid explicit score. The resolved view applies the override
without overwriting source confidence/evidence, and exposes derived `is_removed` for
rejected pairs. `domains_company_filter` excludes those pairs. Other companies on the
same domain remain counted, and central domain/source memberships remain intact.
Backoffice applies review rules immediately and requests a filter refresh; the filter
changes when that refresh finishes. Rejections remain visible in review history.

Apply migration 469 before deploying the publisher's new rule projection. It is additive,
so old explicit-column writers and active crawls can continue. Hot-sync Dagster definitions;
no crawler or Brave restart is required. Do not reuse an old refresh-run configuration
containing a `se_company_domain_verification` op; start domain processing from Backoffice
or select the standalone verification asset explicitly.


Deployment verified: migration ledger `469, dirty=0`; first filter refresh succeeded.
Dagster hot-sync completed without restarting its supervisor. The deployed publisher
has the five source adapters plus precedence as its six dependencies; verification is
only in the standalone asset job. Existing crawler run
`b31a1682-d644-4ad6-b65d-6ebe60d10c83` and Brave run
`a6911a12-aac6-4d66-a457-28d225b97b2b` remained STARTED. Read-only browser inspection
confirmed the controls on Addtech's Domains tab. No company review was changed during
production validation. Local validation: 200 Python/ClickHouse tests and 100 Backoffice
tests passed, plus TypeScript and Dagster definition checks.
