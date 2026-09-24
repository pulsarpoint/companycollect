# Webtech page storage — migration 436

The canonical scan and detection tables gain `website_origin` and `page_url`, both derived from the requested URL. Raw requested and final URLs remain available. The current view chooses one latest attempt per requested page across crawls and detector versions. A failed or empty newer attempt does not fall back to an older detection. Scanning additional pages is a separate producer feature; today's manifest still has one result per root domain.

## Rollout

1. Apply ClickHouse migrations 436 and 437 with the existing golang-migrate runner. It creates empty shadow tables and a shadow view only; 437 improves filtered lookups with whole-row argMax.
2. Run `uv run python scripts/migrate-webtech-pages.py backfill --ledger <durable-path>/backfill.json`. It streams bounded native batches and preserves every existing field. An interruption can be retried: identical rows replace the same logical keys. Invalid requested URLs stop the copy; never substitute a homepage.
3. Deploy the page-aware writer. `WEBTECH_WRITE_MODE=dual` writes legacy canonical tables and new `_v2` shadows, detections before the summary in each destination. Both destinations must acknowledge. Default `canonical` requires the new schema at canonical names.
4. Hold indexing and any standalone webtech replay processes. Touch `/tmp/corpscout-webtech-writes-paused` on **every writer host**; updated writers refuse publication while it exists. Wait for previously started writers to finish and inspect active Dagster jobs. This file does not stop browser workers saving durable reports. If the old writer is active, drain it before relying on the file.
5. Repeat backfill/catch-up after draining old writers, then run `reconcile --ledger <durable-path>/reconciliation.json`. It compares all source columns with bounded exact `EXCEPT DISTINCT`, checks conflicting report identities, and checks detection counts for every summary. Extra orphan reports remain history and are reported separately.
6. Gate affected readers with `/tmp/corpscout-webtech-readers-paused` on each backoffice host. Run `cutover --ledger <durable-path>/cutover.json --writers-held --readers-held`. Both hold flags are operator assertions; they are not distributed locks.
7. The cutover ledger saves original UUIDs and view DDL **before** exchanging tables. Each retry checks UUIDs before exchanging, so a crash after an exchange cannot swap a table back accidentally. The canonical view is explicitly recreated. The temporary shadow view is retired.
8. Activate the page-aware reader, use `WEBTECH_WRITE_MODE=canonical`, remove the gates, and replay one bounded durable manifest. Check its scan and detection readback, source counts, and page lookup latency.

After exchange, the physical legacy tables are retained at `webtech_domain_scan_results_v2` and `webtech_domain_technologies_v2`. The `_v2` suffix is now a rollback location, **not a write destination**. Do not run dual mode or backfill into those names after cutover. Table UUIDs in the cutover ledger are authoritative.

## Rollback

Hold writers/readers again. Run `rollback --ledger <same-cutover-ledger> --writers-held --readers-held`. The tool first copies post-cutover data into the legacy layout, then exchanges the original UUIDs back and restores the original view. It refuses rollback once multiple requested pages exist for a legacy domain/crawl/detector key. It retains the new physical history at `_v2`, even after rollback. Restore the corresponding legacy writer/reader code before releasing the gates.

Do not invoke migration 436's down migration: it deliberately refuses a blind destructive rollback. Do not drop either physical copy during this observation period.

## Validation and limitations

Tests exercise real ClickHouse page/scan keys, latest empty/failed/partial attempts, unpublished detections, late older scans, stable legacy empty IDs, retries, conflict rejection, dual-write failure, URL normalization, and interrupted exchange retries. Backoffice tests cover page separation and filtering redirect hosts after latest-attempt selection.

The migration preserves surviving source rows. The old scan key may already have replaced older empty/failed scans; no such missing history is fabricated. Retained report references can support later recovery where available. Global domain filtering performance is outside this migration; the current object remains an ordinary view.

## Completed rollout — 2026-09-24

Migrations 436 and 437 are applied. The canonical tables use the new keys and the current view uses whole-row `argMax`. Both legacy physical tables remain at `_v2`. The Dagster content deployment passed local/remote definition validation without restarting its supervisor; write/read gates were removed after cutover.

The exact comparison preserved 977,577 scan summaries and 8,050,732 detection rows, with zero missing payloads, conflicting reports, incomplete summaries, or orphan reports. The current view contains 8,044,511 detections for 700,700 domains. Its lower count reflects latest-attempt selection across older detector generations, not removal of history. The WordPress UI showed 11 current technologies while its history retained 22 rows.

Validation: 174 backend/migration tests, 22 frontend tests, typecheck, build, browser readback, and successful deployed asset run `2e20345a-e36d-406e-9f8c-4381e6bfc401` (`hash_072`). See `webtech-page-validation-2026-09-24.json` for replay counts and measurements, `webtech-page-reconciliation-2026-09-24.json` for the full-copy comparison, and `webtech-page-cutover-2026-09-24.json` for the UUID ledger and original view DDL.

A domain-filtered view lookup measured about 0.11 seconds, matching the prior 0.12 seconds. Full current-view enumeration measured 9.5 seconds versus 3.6 seconds previously. The detail reader uses exact page/scan keys directly; the separate global domain-filter performance work remains outstanding.
