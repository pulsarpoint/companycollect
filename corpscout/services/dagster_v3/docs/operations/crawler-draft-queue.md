# Crawler draft queues

Backoffice **SE → Domains → Add to crawl queue** launches only `website_crawl_input_job`.
Choose full crawl, jobs, or basic site info. Each type has one open draft per `queue_scope`
(default `workspace`). Multiple table selections and explicit URLs append to that draft.
Duplicate domains are retained once; the first queued URL wins until processing finishes.
Adding inputs never checks freshness or starts a crawl.

## Storage

- ClickHouse `website_crawl_task_domains`: queue membership, selected URL and source/receipt.
- Existing `website_*_requests`: recurring domain presets, preserved when importing again.
- PostgreSQL `processing.tasks`: lifecycle, execution profile, counters and manifest references.
- PostgreSQL `processing.input_submissions`: idempotent import receipts.
- S3 bucket `website-crawl-queues`: immutable source and execution snapshots.
- Existing `website_crawl_submissions` and `website_*_results`: durable API receipts and outcomes.

Apply ClickHouse migration **445** before deploying. PostgreSQL migrations 124/125 are shared
with Webtech and require no crawler-specific changes. No crawler service/API change is needed.
Legacy input assets and combined workflows keep their existing behavior; only the new input
asset uses draft queues. Legacy tasks are not silently adopted or deleted.

## Import

Materialize `website_crawl_input` with a stable `submission_id` and `crawl_type` plus either:

```yaml
crawl_type: full
submission_id: <uuid>
targets: [example.com, https://shop.example.com/catalog]
```

or `source_relation`, `website_column`, and `ids`/`filters`/`select_all` (with optional
`source_final`, `excluded_ids`, `max_domains` and `se_domain_filters`). The same normalization
as the existing crawl input is used: hostname identity, stripping leading `www`, one URL per
hostname, HTTPS preferred. Different pages of one hostname are not separate queue entries.
Retry a failed import using its original submission ID and config; saved source manifests
prevent source changes from changing a partially imported selection. Imports and freeze share
a PostgreSQL advisory lock. Outstanding failed/preparing imports block Start.

## Start and recover

**Queues → Crawler** selects the current draft for the chosen crawl type. **Configure processing**
opens the sheet for LLM/model, page selection, limits, concurrency and freshness settings.
It launches only the corresponding results job. The results asset freezes the draft, saves
its profile and execution ID, then prepares immutable requests and skip decisions. Successful
results matching the effective crawl configuration can be skipped; force refresh bypasses that
check. Disabled presets are visible as skipped in the saved execution plan.

A frozen task releases the default draft slot so new additions can form the next task. A pipeline
failure retains inputs and published outcomes. Re-run the same task with the same profile;
`execution_id` can be omitted to resume its saved execution. Poll/wait timeouts may change.
Already published requests are not dispatched again. Pending requests reuse their stable API
identities. Counts advance only after ClickHouse acknowledges results (batched async inserts
with `wait_for_async_insert=1`).

After every input has a saved terminal result or skip decision, the task becomes completed
(or completed with errors). Only that task's membership is removed, synchronously verified,
then `inputs_purged_at` is recorded. A failed cleanup can be retried without crawling again.
Task metadata, source snapshots, presets and results are retained. History is read from Dagster
and remains visible after cleanup, with a link to each run. Add domains to a new draft to rescan
completed work or retry individual website failures.
