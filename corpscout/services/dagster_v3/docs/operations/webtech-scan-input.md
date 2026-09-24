# Task-scoped Webtech input

Migration 438 creates `corpscout.webtech_scan_input` and adds `task_id` / `input_id`
to scan summaries. Deploy the scanner's manifest protocol 3 before starting task
scans. Existing protocol-2 Common Crawl jobs remain compatible; use the new task
workflow for new submissions. Old scan history remains intact.

## Prepare

Run `webtech_scan_input_job` with explicit domains/URLs:

```yaml
ops:
  webtech_scan_input:
    config:
      task_id: "bdcc7c63-b24e-4f67-9c95-7a7bd34ebd0f"
      targets: ["novelic.com", "https://shop.example.com/products"]
      source_name: "manual"
      force_rescan: false
```

Or select from any permitted ClickHouse source:

```yaml
ops:
  webtech_scan_input:
    config:
      task_id: "e9e51f8b-e76f-43e1-bfe0-c18349f8c0ab"
      source_relation: "corpscout.commoncrawl_domain_graph_signals"
      target_column: "root_domain"
      source_record_id_column: "root_domain"
      source_final: true
      filters:
        crawl_id: ["CC-MAIN-2026-apr-may-jun"]
        root_domain: ["novelic.com"]
```

There is **no implicit harmonic-rank limit**. An optional `harmonic_rank_limit`
applies only to Common Crawl imports. Other sources supply their target column,
optional source-record ID and exact-match filters. An unrestricted selection
requires `select_all: true`; `max_rows` limits source rows before normalization.
SQL identifiers are validated and values parameterized. Sources must belong to
`corpscout` and differ from the input table.

Domain-only targets become `https://domain/`. Normalization preserves scheme,
non-default port, path and query, removes fragments, and derives the registrable
root with the bundled Public Suffix List. Duplicate pages are merged per task;
the first source record in sorted order supplies provenance. Invalid targets fail
preparation, rather than being silently omitted. Different tasks may submit the
same page independently.

By default, preparation skips pages scanned with the same detector during the
past calendar month, including failures. `force_rescan` bypasses this. Freshness
is applied once: a selected task never rereads the source or recomputes its
selection on retry. New configuration or detector version requires a new task ID.

## Process and resume

Run `webtech_scan_results_job` with the same task ID:

```yaml
ops:
  webtech_scan_results:
    config:
      task_id: "bdcc7c63-b24e-4f67-9c95-7a7bd34ebd0f"
```

Selection metadata and its fingerprint use the existing PostgreSQL processing
store, as with Brave; bulk input rows live in ClickHouse. A task supports up to
one million pages, processed in 128 root-domain hash buckets. Each nonempty bucket
has an immutable RustFS manifest keyed by task and bucket. The existing `crawl_id`
result field holds a source-neutral `webtech-<task_id>` batch label for these scans.

The scanner keys task work and result objects by input ID, preserving multiple
pages under one domain. Explicit URLs are scanned exactly, without the legacy
domain-only HTTP scheme fallback. Redirect destinations remain observations.

Retry the results job with the same task ID after interruption. Persisted page
results and completed remote manifests are reused; result indexing is idempotent.
The asset logs indexed inputs versus the frozen total after each bucket. Preparing
input retries fence stale inserts and remove only unfinished rows for that task.
Selected inputs cannot be overwritten by the input asset.

`processing.tasks.status = selected` means membership is prepared, not that the
scanner is pending or completed. Dagster/scanner progress describes execution;
ClickHouse summaries record durable per-input results. Workspace → Webtech → Inputs
shows persisted rows, task/source filters and result availability. “No indexed
result” can mean not scanned, running, or awaiting result ingestion.

## Deployment verification — 2026-09-24

Migration 438 was applied, the scanner deployed with protocol 3, and the Dagster
code location hot-reloaded while preserving its supervisor. A live submission
of `novelic.com` completed both jobs successfully:

- Task: `e1a85901-378c-4aab-bd91-7ec4a51ab90a`
- Input run: `8d6e6a50-6ead-48fe-8033-82db1428046e`
- Results run: `d8346878-34f9-4059-b151-67e60017ce90`
- Scan: `19fc1f3ad833465f9c46ba4e66980473`
- Requested page: `https://novelic.com/`
- Final page: `https://www.novelic.com/`
- Outcome: `success`, with 25 detected technologies.

The stored summary contains the original task/input identities. Browser
verification confirmed the Inputs table shows the submitted page and
“Results indexed”. The smoke submission uses source `webtech-input-validation`.
