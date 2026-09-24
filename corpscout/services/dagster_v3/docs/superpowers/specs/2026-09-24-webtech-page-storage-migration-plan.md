# Webtech page storage migration plan

Date: 2026-09-24  
Status: Implemented, deployed, cut over, and verified by a successful Dagster replay on 2026-09-24. See `docs/operations/webtech-page-storage.md` for the executable rollout and rollback procedure.  
Scope: Page identity and retained scan history in the existing webtech storage model.

## 1. Scope and decision

Change only these ClickHouse objects and the code that writes/reads them:

1. `corpscout.webtech_domain_scan_results`
2. `corpscout.webtech_domain_technologies`
3. `corpscout.webtech_domain_technologies_current` (ordinary view)

Keep one row per detected technology. Do not introduce technology arrays, new website/domain inventories, physical current-result tables, or changes to `technology_catalog`. No Common Crawl table changes. Existing table names remain the canonical names after cutover.

This plan supersedes the implementation scope of the broader domain-inventory proposal for this phase. It prepares storage for multiple pages; the scanner still visits its existing single target per domain. Multi-page crawling, submission APIs and manifests are a separate feature.

## 2. Page identity

Add these required `String` columns to both physical tables:

| Column | Meaning | Example |
| --- | --- | --- |
| `website_origin` | Normalized origin of the requested page: scheme, host, effective port | `https://shop.example.com` |
| `page_url` | Normalized full requested page URL, including path/query | `https://shop.example.com/products?id=7` |

Use requested-page identity consistently for the scan and all its detection rows. Preserve existing `requested_url`, `final_url` and `final_hostname` as evidence. `root_domain` continues to mean the requested candidate's root; this migration must not silently change its meaning.

Why requested-page identity: a scan of `/shop` needs the same identity when it succeeds, fails before navigation, or redirects elsewhere. Keying exclusively on the final URL would leave a previous redirect destination appearing current when the next attempt fails or redirects differently.

Technologies are observations from `final_url`, obtained while scanning `page_url`. Readers must retain that distinction. Display/filter an observed website through `final_hostname`/the origin of `final_url`; never interpret the new requested `website_origin` as proof that a redirected target's technology is deployed there. Cross-root redirects remain visible as redirects. Multiple targets resolving to the same final page remain separate scan targets in this phase; domain/site rollups deduplicate technology names/IDs rather than summing rows.

Normalization contract, shared by new writes and backfill:

- HTTP/HTTPS only; lowercase scheme/host and apply the existing IDNA policy.
- Remove a hostname's trailing DNS dot and omit default ports; preserve non-default ports.
- Use `/` for an empty path and remove URL fragments.
- Preserve path case, trailing slash, query order/values/repeated keys, and escaped reserved characters.
- Do not strip all query parameters, merge `www` with an apex host, or collapse HTTP into HTTPS.
- Invalid or missing requested URLs cannot be replaced with a guessed homepage. Attempt recovery from the referenced report, otherwise record the exception and require an explicit resolution before claiming full backfill coverage.

The `page_url` column stores the complete URL, not just `/path`.

## 3. Exact schema changes

### Scan results

Existing sorting/replacement key:

```text
(crawl_id, root_domain, detector_version)
```

Proposed sorting/replacement key:

```text
(root_domain, website_origin, page_url, crawl_id, detector_version, scan_id)
```

Keep `ENGINE = ReplacingMergeTree(recorded_at)` and all existing payload/status/reference columns. The key now preserves separate scans and separate requested pages. Only repeated writes of the same page/scan identity replace one another.

Keep `crawl_id` in the key. Existing pilot data explicitly contains empty `scan_id` values; removing crawl identity could merge unrelated legacy observations. This is a deliberate refinement of the earlier abbreviated key suggestion. Do not fabricate new IDs or change existing scan lineage during this migration.

### Technology detections

Existing key:

```text
(root_domain, crawl_id, detector_version, scan_id, detected_name)
```

Proposed key:

```text
(root_domain, website_origin, page_url, crawl_id, detector_version, scan_id, detected_name)
```

Keep the existing engine, catalog association, categories, confidence, version, timestamps and report fields. New page fields must exactly match the owning scan result. A retry with the same logical identity must have the same report checksum/content; reject conflicting payloads rather than relying on replacement ordering to choose a winner.

Example now safely representable:

```text
example.com | https://example.com | https://example.com/      | scan-123 | React | 18
example.com | https://example.com | https://example.com/admin | scan-123 | React | 19
```

No partition-layout redesign or speculative indexes are required for the initial migration. Benchmark root/page reads on shadow tables before deciding whether an additional projection is justified.

## 4. Current-results view

Keep `webtech_domain_technologies_current` as a regular view for this phase. Change its selection from current domain/crawl/detector to latest committed scan per requested page:

1. Deduplicate scan-result retries with `FINAL` or an equivalent exact fold.
2. Select one entire winning scan row per `(root_domain, website_origin, page_url)`.
3. Order by `scanned_at`, then stable identity fields such as `crawl_id`, `detector_version`, `scan_id` and checksum for deterministic timestamp ties. Do not use the time of backfill/re-indexing as scan recency.
4. Join detection rows to that winner on domain, website, page, crawl, detector, scan ID and report checksum.
5. Expose the existing columns plus `website_origin` and `page_url` using an explicit column list.

Choose the whole scan row with a row-number or tuple-based aggregation; independent `argMax` calls with different/null handling must not combine fields from different scans.

`crawl_id` and `detector_version` remain historical attributes, not separate groups for deciding current. In this webtech-only scope there is one scanner family.

Important behavior:

- A complete newer scan with zero technologies has a scan row and yields zero current detection rows.
- A failed newer scan becomes the latest attempt. Do not fall back to older positive detections by filtering failures/empty results before selecting the winner.
- Partial results retain their recorded `analysis_status`/`analysis_complete` and any detections. Zero partial detections are not proof of successful absence.
- Updating `/admin` does not alter the selected scan for `/`.
- Late-arriving older results do not replace a newer scan's current state.

This does not promise to solve global domain-list filter timeouts. The large ordinary-view join remains; the dedicated fast domain inventory is deferred.

## 5. Writer and backfill-code changes

Use one page-normalization function and the same derived identity for both inserts.

Update [storage.py](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3/src/dagster_v3/defs/webtech/storage.py), [technologies.py](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3/src/dagster_v3/defs/webtech/technologies.py), and [backfill.py](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3/src/dagster_v3/defs/webtech/backfill.py):

- Extend column contracts and row builders with the two page fields.
- Validate that all detection rows belong to the same page identity, scan and report as their scan summary.
- Preserve the existing order: validate the full report, insert all detections, then publish its scan-result row. The summary row is the publication marker.
- A failed technology insert must prevent publication of the marker. On retry, reuse identity/hash and safely repeat acknowledged detection writes.
- Always write the scan result for zero-detection and failed outcomes.
- Include page identity in backfill cursors, completeness checks and resume comparisons wherever page-oriented records are read.
- Keep current scanner manifests/report versions working by deriving page identity from their existing `requested_url`. No scanner service API deployment is required just to populate these columns.
- The current final manifest validates one result per root domain. Do not advertise end-to-end multi-page scanning until that separate manifest/submission limitation is extended. New storage tests can still verify multiple pages under the same scan ID directly.

## 6. Migration mechanism

Use new shadow tables with the new keys, not an in-place destructive conversion. Allocate the next available migration versions when implementation begins; do not edit previously applied migrations.

### Phase A — preflight and shadow schema

- Capture live DDL, dependent views/readers, row/key counts, outcome counts and stored report references.
- Check actual URL quality, empty scan IDs, duplicate/conflicting report identities and required disk headroom.
- Create `webtech_domain_scan_results_v2`, `webtech_domain_technologies_v2`, and `webtech_domain_technologies_current_v2`.
- Assert schema compatibility before backfill. Do not activate readers yet.

### Phase B — backfill with preserved provenance

- Copy surviving source scan rows using exact replacement semantics and copy existing detection history with normalized page identities.
- Preserve original timestamps, IDs, checksums, object references and payloads. Do not stamp old scans with a new `scanned_at`.
- Prefer bulk `INSERT SELECT`/bounded transformations. If URL normalization requires Python, stream bounded batches; do not collect all reports or rows in memory.
- Retain all existing detection history, including rows whose old scan summary has already been replaced. Such rows remain historical evidence but must not become current without a validated scan publication marker.
- Recover missing summaries from retained reports/manifests where practical, preserving checksum validation. Never synthesize a successful scan or infer an empty historical scan from missing detection rows.
- Report separately: copied surviving scan summaries, copied detections, recovered older summaries, and unresolved rows/history gaps. Old empty/failed summaries may be unrecoverable because the existing table replaced them.
- Run idempotently; retry the same page/scan identities. Compare exact logical rows, not physical `count()` alone on a replacement engine.

### Phase C — catch-up and writer preparation

- Deploy page-aware writers with an explicit temporary dual-write configuration for old and shadow schemas. Continue serving old tables.
- Within each destination, write detections before the summary. Only acknowledge successful indexing once both required destinations are written; failures retry from the durable report.
- Because the scanner still emits one target per root, the legacy destination remains compatible during this limited transition. Do not enable multi-page producers before cutover.
- Record and drain indexing runs that started with older code. A timestamp watermark alone is insufficient: compare logical keys and report checksums at final catch-up, including late arrivals/retries.

### Phase D — controlled cutover

1. Hold new webtech indexing/backfill jobs and wait for in-flight writers to finish. Scanner workers may continue saving durable object-store reports; do not cancel unrelated scans.
2. Run the final key/checksum reconciliation and verify the new current view.
3. Put affected webtech readers behind a short maintenance gate during the swap. Two table exchanges plus a view replacement are **not** one cross-table transaction.
4. Exchange each canonical table with its validated shadow table, retaining the old physical tables for rollback. Capture table UUIDs and each completed operation so interruption can be resumed without blindly swapping twice.
5. Explicitly recreate the canonical current view against the new canonical tables. Verify its definitions/dependencies after the exchange; do not assume existing view dependencies automatically retarget correctly.
6. Activate the page-aware writer for canonical names, turn off dual writes, and enable updated readers.
7. Resume indexing and run one bounded materialization/readback check.

The cutover should be an explicit audited deployment step after validation, not an automatic side effect of creating empty tables. Keep old tables under documented legacy names after the exchange. Retire temporary `_v2` views only when their dependencies have been checked.

## 7. Reader changes necessary for compatibility

The new scan table contains history. `FINAL` alone will no longer mean “latest scan for a domain”; it only removes retries of the same scan identity. Audit every reader accordingly.

In [webtech.server.ts](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice/app/lib/webtech.server.ts):

- Return page identity with scan/detection records.
- Select latest attempts per page before applying final-host filters, so a changed/failed redirect cannot expose an obsolete successful result.
- Add exact page constraints to detection lookup, including website/page as well as scan identity/hash.
- Preserve the existing domain detail behavior for the current single-page producer. When multiple page observations exist, group/show their page identities rather than labeling a single selected scan as covering the whole domain.

In [workspace-domains.server.ts](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice/app/lib/workspace-domains.server.ts):

- Continue counting distinct current technologies per root/site rather than summing page rows.
- Keep observed-site grouping based on final URLs/hosts and preserve visible page attribution.
- Treat site discovery from historical scan rows as historical hostname discovery, not current technology state.

No new general-purpose filter UI, domain inventory or website-table rollout is included. Minimal page labels/grouping and selection constraints are in scope because otherwise the expanded result grain would be misrepresented.

## 8. Validation and go/no-go checks

Use disposable ClickHouse integration tests with production-shaped schemas, not only mocked SQL.

Required cases:

- Two pages in one domain/scan detecting the same name with different versions remain separate.
- A second scan of one page preserves its previous history and leaves other pages unchanged.
- New empty scan clears older current detections; new failure does not silently select an older success.
- Partial scans retain their status; failed publication cannot become current.
- Old detector/crawl generations do not remain independently current for the same page.
- Identical insert retries yield one logical scan and detection set; conflicting identity/checksum fails validation.
- Legacy empty scan IDs from different crawls remain distinct.
- URL identity covers root paths, queries, Unicode hosts, ports, fragments and redirects, including failure after a previous redirect.
- Late older scans and backfill replay cannot override newer observations.
- Reader joins return only the requested page's detections, and site filters do not resurrect stale redirects.
- Interrupted backfill, dual-write failure and each cutover step can resume or roll back.

Before cutover, reconcile all surviving source keys/payloads against the shadow tables. Account explicitly for invalid URL exceptions and recoverable/unrecoverable history. Verify every published scan's stored detection set against its validated report/technology count. Compare old/new current sets; expected differences across detector/crawl generations must be explained, not dismissed as migration loss.

Run relevant webtech writer/backfill/model tests, Dagster definition validation, backoffice query tests/typecheck/build, and a UI check of existing domain/site details. Measure root/page lookup latency and view resource use; reject serious regressions. Global webtech filtering is a known separate performance task.

## 9. Rollback and retention

Before cutover, rollback means disabling dual writes and returning to the original code; shadow tables remain for diagnosis.

After cutover:

1. Hold new indexing and affected readers.
2. Capture/replay post-cutover results from durable reports into the legacy format before reverting. The single-page producer remains a requirement during the rollback observation window.
3. Restore old table identities, the original view definition and matching reader/writer code using the recorded UUID/swap log.
4. Validate readiness and resume indexing.

Do not drop new results or use a blind down migration after production writes. Keep both legacy physical tables and new history until rollback coverage is verified. A later cleanup migration can remove legacy storage after the observation period; deletion is not part of this plan.

## 10. Deliverables and completion criteria

Deliverables: additive shadow-schema migrations; one shared page identity normalizer; updated writer/backfill contracts; idempotent backfill/cutover tooling; page-aware current view and readers; integration/regression tests; reconciliation report; rollback instructions.

Complete when canonical webtech tables retain distinct page/scan history, current detections select the latest attempt per page, existing consumers continue to work, and live readback matches the report contents. No claim of complete historical recovery is made beyond the recoverable source/report evidence.
