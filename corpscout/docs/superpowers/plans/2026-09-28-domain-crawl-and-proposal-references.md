# Website references and company-domain evidence

Date: 2026-09-28.
Status: migrations 000466 and 000467, historical backfills, the compact index cutover,
and coordinated crawler/Dagster deployment completed on 2026-09-28. See the
[production rollout record](../../../services/dagster_v3/docs/operations/domain-result-references.md#production-rollout-record-2026-09-28)
for verification, recovery backups and resumed runs. The shared migration ledger is clean
at 000468, which belongs to the concurrent DNS work.

The task descriptions below retain their preparation history; references to local-only
or pending activation describe that earlier stage. Remaining work is end-to-end validation
of a newly completed production batch and Task 6 filter semantics, the country summary/
history/review physical-key migration, production-scale performance and interruption
recovery checks, and eventual retirement of retained legacy layouts after validation.

## Agreed model

Keep durable source evidence separate from rebuildable summaries. Source adapters write
claims in batches. They do not each maintain company summaries or the global source
index. A proposed company/domain pair is not necessarily an established connection.

| Dataset | Responsibility | Publication owner |
| --- | --- | --- |
| `domains` | Central registrable-domain identity and information. | Inventory and guarded identity registration. |
| `websites` | Individual origins, including subdomains, each referencing a central domain. | Website inventory and guarded registration. |
| `pages` | Canonical page URLs belonging to websites. | Page inventory and guarded registration. |
| Crawl/lookup results | Durable attempts and findings about the requested website. | Crawler result publishers. |
| `se_company_domain_sources` | Source claims and evidence for Swedish company/domain pairs. | Source adapters, each owning its own claims. |
| `se_company_domain` | Computed current candidate relationship, combined confidence and review/verification status. | Existing country fold, extended for the new inputs. |
| `domains_sources` | Computed index of tables which contributed each domain. | Dedicated contribution-index publication after country summaries. |

```text
Brave / crawl matching / ESEF / Wikidata / other saved evidence
                         |
             se_company_domain_sources
                         |
                se_company_domain
                         |
     domains_sources (domain_id, 'se_company_domain')
```

Identity registration precedes referenced evidence publication. The arrows above are
processing dependencies, not a transaction across tables. The country summary and
contribution index can be regenerated without another crawl or paid model request.

## Identity and evidence contract

- Crawl requests, submissions, frozen queue entries, raw results, lookup records and
  normalized scan/detail records carry an explicit required `website_id` for the
  requested website. Resolve its `domain_id` through `websites`. Do not add canonical
  `root_domain` copies or locally materialized domain hashes to these records.
- `https://shop.example.se`, `https://careers.example.se` and `https://example.se` are
  different websites under one domain. Preserve existing origin normalization, including
  scheme and nondefault port. Never use a bare host as a substitute for an origin.
- Keep original requested/final URLs as evidence. Redirects do not silently reassign the
  requested attempt or assert ownership of another domain. Page-level evidence links to
  the actual visited resource through `pages`, even when it has a different website.
- The existing normalized `page_id` is attempt-local (for example `p0001`), not the central
  `pages.page_id`. In Task 3 add a distinctly named canonical page reference to the page
  observation mapping and join other extracted details through their observation key.
  Preserve local IDs and failed fetch observations. Do not invent a page URL when none
  was recorded or overwrite local IDs with canonical IDs.
- Company source claims require the registered `domain_id`. `website_id` is optional
  because some sources provide only a domain. When present, validate that it belongs to
  the proposed domain. A reference to a visited website is evidence, not proof that all
  websites under its parent belong to that company.
- Compute IDs locally from canonical identity strings, including for new domains:
  `domain_id = sha256(root_domain)` and `website_id = sha256(website_origin)`, encoded
  as lowercase hexadecimal. They are not database-generated IDs. All services must use
  the same URL normalization and pinned public-suffix policy.
- Register and validate parent membership in bounded batches before inserting children.
  Knowing a deterministic ID does not prove its parent row exists. An empty, unknown or
  mismatched reference blocks publication, with completed work retained for retry.
  ClickHouse CHECK constraints reject missing IDs but cannot prove parent membership.
- Preserve request IDs, work keys, retries, last-success behavior and selected-attempt
  URLs. Website-scoped reads must not merge separate origins. Failed/cancelled/unmatched
  attempts still have valid requested websites and remain available in history.
- Historical records with missing identities are migration exceptions only. Backfill
  from saved URL evidence, report exceptions explicitly and resolve them before enabling
  dependent readers. No new result is accepted with a missing reference.

## Country evidence and computed relationships

Evolve `se_company_domain_suggestion` into `se_company_domain_sources`; do not operate
both as independent authorities. Migration 466 prepares the new empty table, retaining
stable `(company_id, source, slot)` ownership, original record references, evidence,
confidence, removals and extraction version. Existing writers still use the old table
until the coordinated copy and switch. Retire it only after all consumers are migrated.

Each source can revise or withdraw its own claims. Select the latest version before
filtering removals. Repeated attempts, duplicated basic/full lookup output and several
records copied from the same source must not inflate independent supporting-source counts.
A Brave answer can yield several claims, all retaining its original answer ID.

`se_company_domain` has one logical current row per `(company_id, domain_id)`, including
unverified candidates. Its existence says a relationship was proposed. Confidence is a
0–1 source/derived score, not automatic human confirmation or a calibrated probability.
Preserve the existing association verdict: confidence in a negative verdict is not the
probability of a positive connection. Keep reviewer decisions separate from model claims.

The fold computes this summary from current claims, saved verification and durable review
rules. Human decisions remain authoritative and survive rebuilds. Preserve the current
review store during this migration; moving transactional operator edits to PostgreSQL
requires its own data-copy/read-switch task, with an outbox/checkpoint for propagation.
Do not silently move or discard existing reviews as part of reference DDL.

## Global contribution index

Target grain: one logical row per `(domain_id, source_table)`, with discovery timestamps
and publication metadata only. For example, Swedish proposals contribute the table name
`se_company_domain`; the global index does not need to know whether Brave, ESEF or a crawl
produced them. Norway can contribute another row for the same domain.

There is no `source_kind`, company ID, confidence, association verdict, review status or
per-attempt source row in the target index. Presence means that a table contributed that
domain, regardless of confidence or subsequent rejection of a company claim. It must not
be interpreted as a confirmed relationship or reset first-seen timestamps on every refresh.

The deployed index currently contains company-specific fields used by readers. Do not
remove them in migration 466 or break those readers. A later forward cutover migration
will rebuild the compact index from the complete known contributor set, copy distinct
memberships/timestamps, switch its publishers/readers and retire the old shape.

Global company counts and filters must read country summaries with their defined status
predicates, not infer connections from index membership. Existing `domains_company_filter`
can remain a derived filter cache if benchmarks justify it; it is not a global company
entity. Build it from the registered country summaries, preserving distinct country/company
identity. Detail queries use the index to discover relevant tables through an allowlisted
mapping, never interpolate an arbitrary stored table name into SQL.

## ClickHouse write and read strategy

- Calculate identities and register missing requested domains/websites when preparing a
  queue batch, before crawling begins. Carry the IDs with the durable queue entries. A
  batch of 200–500 requests needs batch membership checks/inserts, not a lookup for an ID
  on every result or a database round trip inside each browser's processing loop.
- Register newly discovered websites/pages, including redirect destinations, in a batch
  before publishing their findings. Retain the original requested website reference on
  the attempt. If registration fails, retry publication from saved results without
  repeating the crawl or model calls.
- Preserve completed raw results in the existing durable queue until publication is
  acknowledged. Derived publication failures do not cause recrawling or another model call.
- Stable claim keys, deterministic versions, bounded checkpoints and reconciliation make
  retries safe. ReplacingMergeTree background merges are not a uniqueness guarantee:
  current reads must explicitly select latest rows (FINAL or an equivalent bounded query).
- Process affected companies/pairs after evidence changes. Recompute a company's primary
  choice when necessary, not just the changed pair. Summary history records actual changes.
- Start with explicit Dagster processing of saved evidence. An incremental materialized
  view joining tables does not recompute when every joined input changes or a source row
  is withdrawn. Refreshable views may suit small derived indexes after measuring their cost.
- The UI reads the country summary and loads detailed sources on demand. Add denormalized
  filter outputs/projections only for measured expensive reads, not another authoritative
  relationship table. No dependency on experimental multi-table transactions.

Reference guidance: [insert batching](https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy),
[transaction guarantees](https://clickhouse.com/docs/guides/developer/transactional),
[join tradeoffs](https://clickhouse.com/docs/best-practices/minimize-optimize-joins), and
[incremental view semantics](https://clickhouse.com/docs/materialized-view/incremental-materialized-view).

## Task 1 — Prepare schema and migration contracts

Prepared locally in `000466_corpscout_domain_result_references`:

1. Required ordinary `website_id String` on basic/full/jobs results, four lookup datasets,
   nine normalized datasets, recurring requests, frozen task inputs, submissions and the
   legacy saved-result history. CHECK rejects missing/empty values on new inserts.
2. Expose stored website IDs through current/latest/published views with Task 3 website-scoped
   selection and extended queue keys. Old parts require backfill before activation. Archive reads extract an explicit website ID
   from new JSON only; historical identity comes from backfilled saved result records.
3. Create empty `se_company_domain_sources` with required domain reference and optional
   website reference, retaining source claims and withdrawal/version semantics. No claim
   is automatically copied, confirmed or folded by DDL.
4. Remove the abandoned `source_kind` addition, per-attempt global index writes, filter
   query modification and grants allowing crawlers to write the global source index.
5. Keep old tables/readers intact until cutover. The current normalizer fails its schema
   check on the new layout; it cannot continue silently writing records without IDs.

Not implemented by this preparatory migration: writer membership validation, historical
backfill, new adapters, compact global index, revised website read keys, or country summary-key
migration. Those require the following tasks. This file must not be deployed alone.
Migration 465 is reserved by the domain-services work: coordinate numbering/ledger before
any rollout, without rewriting already applied migrations 461–464.

Local validation: 7 disposable-ClickHouse reference/evidence tests, 134 migration contract
tests and 47 normalization regression tests passed. `uv run dg check defs`, focused Ruff
checks and `git diff --check` also passed. These validate preparation, not the pending
publisher/backfill/cutover implementation.

## Task 2 — Batch registration and publication recovery

Local foundation prepared; Task 3 connects it to crawl queues below. Not deployed:

- `corpscout/python/corpscout_identity/registration.py` calculates canonical domain, origin and page identities
  without a database lookup. Registration validates the complete input batch, rejects
  conflicting/duplicate stored parents, inserts only missing parents in order, and checks
  their membership before returning. Domain-only evidence does not invent a website.
- `corpscout/python/corpscout_identity/coordination.py` coordinates registration and the final inventory exchanges
  with a PostgreSQL transaction advisory lock. Existing country registration uses the same
  guard. `PROCESSING_PG_URL` must select the same database for every publisher; an absent
  connection or lock timeout stops publication. `INVENTORY_LOCK_TIMEOUT_MS` defaults to
  30000 and accepts 1–300000. The Dagster pool alone cannot guard external service writes.
- Inventory staging is built outside the guard. Immediately before exchange, each stage
  retains identities missing from it but present in the current inventory, preserving
  registrations made during the build. Parent publication precedes child publication.
- Existing identity metadata is not overwritten by a registration retry. Inventory folds
  remain responsible for combining observation timestamps from durable crawl evidence.
  Registration does not publish country summaries or global source-index contributions.

Local validation: 33 identity/inventory tests and 67 existing domain/country tests passed
using disposable ClickHouse and PostgreSQL. Coverage includes concurrent registration,
lock timeout, completed-insert acknowledgement loss/replay, malformed references and
registration during both inventory builds. Focused Ruff and Dagster definition checks
passed. These tests do not establish atomic transactions across the two databases.

Before activation, benchmark the final inventory reconciliation at production scale and
test recovery from coordinator/process loss while a ClickHouse write is still in flight.
The advisory lock does not itself cancel an orphaned ClickHouse query. Required service
integration and durable queue/publication recovery are part of Task 3.

## Task 3 — Website-aware crawl writers and history

Implemented locally; activation remains part of Task 7. One shared Python identity package
is included in both service wheels. Queue and REST admission, SQLite payloads, ordinary and
matching publication, normalized page references, and website-specific history are wired.
No production migration, deployment or job restart has been performed for this task.

Implemented contracts:

- Dagster queue preparation and REST admission register requested identities in batches
  before accepting work. Frozen inputs, SQLite queues, retries and saved envelopes carry
  the website ID; new request keys distinguish HTTP/HTTPS, subdomains and ports.
- Ordinary, matching and legacy result writers register visited pages and redirect targets
  from saved output before publishing it. The HTTP destination independently checks parent
  membership. Registration failure retains finished work for publication without recrawling.
- Normalization carries website IDs and maps page observations to central pages. The web
  inventory fold includes published crawler observations alongside Common Crawl and webtech.
- Child datasets/site info precede lookup completion; normalized details precede scan
  completion. Queue checkpoints advance only after acknowledged required writes. No
  per-attempt writes are made to `domains_sources`.
- Backoffice history selects a requested website while retaining exact request/attempt URLs.
  Last good results and newer failures remain accessible. Stored ClickHouse fields provide
  details when no S3 object is available.

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

## Task 4 — Migrate country claims and rebuild the summary

Source writers, saved-match adapter and fold changes are implemented locally. Migration
466 now includes the central-label read view; activation/backfill remain pending.

- Brave, Wikidata, ESEF and Common Crawl write registered references to
  `se_company_domain_sources`. Slots, original record IDs, evidence, confidence,
  versions and source withdrawals remain available. Actual website references are
  validated; domain-only evidence does not fabricate a website identity.
- `se_company_domain_suggestions_crawler_lookup` consumes saved Swedish matches without
  network/model calls. Newest requested-website attempt wins. Changed owners and explicit
  not-found results withdraw old claims. Failed/cancelled/already-mapped attempts only
  preserve existing evidence; they cannot manufacture supporting evidence. Repeated
  attempts and multiple websites count once as an independent supporting source.
- Existing precedence and reviewer authority remain unchanged; crawler matching is added
  at priority 700, below Wikidata and above Common Crawl. The fold reads the new evidence
  projection, updates affected companies' primary choice and records actual changes.
- Explicit bounded `se_company_domain_sources_backfill` copies current legacy claims,
  including withdrawn/reviewer rows, with original timestamps/IDs. Resume from acknowledged
  company IDs. Normal sync/refresh jobs exclude this cutover-only operation.
- Backoffice sync/process includes the saved-match adapter and accepts its source filter.

Validation: existing fold and paid-answer/reviewer regressions passed. All four source
adapters were exercised against disposable ClickHouse, including both join null settings,
Brave checkpoint failure/replay, ESEF failed extraction preservation and source withdrawal.
The final reference/backfill/recovery and migration-contract run passed 147 tests;
Backoffice launch/reader/filter checks passed 21 tests and TypeScript checking passed.
Production data and running jobs were not changed.

Task 7 still must run the copy with old writers paused, compare claims/reviews and retain
Brave checkpoints, then activate all new writers/readers together. The later forward
migration must rebuild summary/history/rule physical keys where required and retire the
old suggestion table only after parity and consumer checks. No table was dropped locally
as a substitute for proving that production evidence has been migrated.

## Task 5 — Derive the compact global contribution index

Implemented locally; migration 467, bounded copy and reader changes are prepared together.
Production activation remains Task 7.

- `domains_sources` is now an independent asset. It derives one row per domain/table from
  saved summaries or validated inventory tags. Swedish refresh selects only its country
  partition; Common Crawl is processed in bounded hash ranges. No crawl or LLM calls.
- Index updates retain historical memberships and first-seen timestamps, even after
  withdrawal/rejection. Other countries and bulk partitions are not rewritten by a
  Swedish publication. The compact schema contains no company IDs or decisions.
- Migration 467 prepares `domains_sources_next`. The explicit preview-first backfill
  collapses all old source records, preserving timestamps/publication metadata and every
  contributor, then checks both directions of row parity and parent membership per range.
  Resume cursors advance only after acknowledged writes and checks. Old data is retained.
- Global company counts and filters read `se_company_domain_resolved`, including live
  reviewer overrides. Backoffice no longer writes company decisions into the global index.
  Future countries must be explicitly registered in the publisher and the company reader
  unions; names from index rows are never executed as SQL identifiers.

Validation: 157 final compact-index/country-filter/evidence/migration checks passed against
local disposable databases. The inventory/search/fold regression checks also passed in
the preceding run (its migration convention failure was corrected and included in the
final run). Backoffice checks passed 14 tests, TypeScript passed, Dagster definitions
loaded and focused lint/whitespace checks passed. Recovery tests cover lost insert and
partition acknowledgments, multi-page resume, source isolation and reviewer overrides.

The table-name swap and legacy-index retirement are deliberately outside the preparation
migration. Pause old writers, copy/verify, activate the new publisher/readers together,
and retain the old layout until validation completes. Broader UI predicate work is Task 6.

## Task 6 — Backoffice readers and filters

Read country summaries for confidence/status and detailed sources for explanations.
Resolve website/domain labels through central identities. Route global company lookups
through contributing country tables or a measured derived filter cache.

Keep filter meanings explicit: proposed domains, confirmed connections and matching
attempts are different predicates. Matching-attempt filters include all terminal outcomes
and country scope. Counts, rows, select-all and queue submission use identical predicates.
Keep existing proposal visibility, newest failure/last usable crawl and attempt URLs.

## Task 7 — Backfill, coordinated activation and verification

1. Inventory all source rows, reviews, writers and high-water marks. Retain durable queues.
2. Prepare/test new writers and consumers before applying DDL. Pause affected publishers
   during reference cutover; never allow old writers to keep publishing missing IDs.
3. Register central identities and backfill historical website/domain references in bounded,
   resumable batches. Rebuild the four queue tables whose sorting keys now include website ID;
   preserve legacy request identity version 1 and frozen payloads. Materialize central lookup
   projections and verify their query plans. Preserve unresolved evidence for explicit resolution.
4. Copy country claims, compare full logical keys, evidence, withdrawals and review outputs,
   then switch all source adapters/fold readers together. Do not leave two write authorities.
5. Rebuild and switch the compact contribution index and derived filters after all readers
   are ready. Reconcile late arrivals from the recorded high-water marks before resuming.
6. Validate no missing parents, mismatched website/domain references, lost attempts/reviews,
   duplicate current claims, unexpected confirmations or changed global filter semantics.
7. Test restart/replay at each publication boundary, newest-failure behavior, changed Brave
   answers, source withdrawals and a full rebuild with no model calls. Benchmark insert
   batches and representative filtered UI queries before/after.
8. Resume and verify existing crawler and Brave work progresses. Only after parity retire
   old layouts through forward migrations. An application rollback cannot restore an old
   writer that omits required IDs; keep publication paused or deploy a compatible writer.

Definition of done: results identify actual websites, country claims remain inspectable,
company/domain summaries and global table contributions are rebuildable, and a failure in
any derived stage never requires paying for the same crawl or model work again.
