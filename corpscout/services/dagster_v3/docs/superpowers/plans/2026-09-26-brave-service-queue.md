# Brave service batch queue

Status: deployed and verified on 2026-09-26. The operator chose sequential
batches with service-owned ClickHouse publication before a future Temporal migration.
The earlier rolling-refill/Dagster-writer proposal is superseded by this contract.

## Implemented contract

1. Dagster freezes the existing task/selection and verified encrypted LLM profile.
2. Submit 500 inputs by default (`input_batch_size`, maximum 500; 200 is supported).
   SQLite persists the batch and stable input/result/request IDs before acceptance.
3. Four browser workers consume the shared local queue, one per route by default.
   Each saves its outcome before taking another item. Browser reuse remains ten
   requests by default, optionally twenty; failures/cancellation close early.
4. Once all inputs have an outcome, the service publishes them to
   `corpscout.company_brave_search_results`, repairs the successful-answer projection,
   and verifies every result ID. Failed writes retry saved results, without recrawling.
5. Dagster sends a heartbeat/progress request every two seconds. Logs report batch
   processed/total, running/pending, success/failure, publication and entries/minute.
6. Only after service completion and independent ClickHouse verification does Dagster
   send the next batch. There is deliberately no overlap or early refill.
7. Existing final validation, company-membership retention and input cleanup run
   after the entire frozen task is accounted for.

## Recovery and controls

- A repeated batch ID must have identical membership/configuration; replay is idempotent.
- One active batch per service; one unfinished batch per execution. Conflicting
  submissions wait without adding a second active backlog.
- Resume first discovers an unfinished service batch, including unpublished results.
- SQLite and original request JSON survive restarts. Recover a durable response
  before searching again; cancelled/interrupted attempts get deterministic retry IDs.
- Completed local batches have a seven-day cache retention; unpublished work is retained.
- PostgreSQL admission runs before each request and every two seconds. Model disable,
  removal, revision invalidation or owner stop cancels active work and stops claims.
- Dagster renews a 60-second lease. Controller loss pauses work; explicit resume is required.
- Controller IDs fence late cancellation. A new Backoffice owner must match the task
  and model dependency, and cannot replace a still-active previous owner.
- Individual request registration preserves existing CAPTCHA/token/route statistics
  and Dagster logs. No plaintext LLM keys or database credentials enter batch payloads.
- Batch completion means both local processing and verified ClickHouse publication.
  Individual search failures are outcomes; service/storage failures remain resumable.

## Deployment requirements

Configure the browser service with `BRAVE_CLICKHOUSE_URL`, `BRAVE_CLICKHOUSE_USER`,
`BRAVE_CLICKHOUSE_PASSWORD`, and `LLM_CONTROL_PG_URL`, using the existing publisher/
processing-worker grants. Keep the existing shared LLM encryption key. No new central
schema is required; SQLite schema belongs to the service.

Stage the service and Dagster code, coordinate a graceful stop of the current run,
activate the configured service, then resume the same frozen task. Validate a small
batch and compare throughput after startup.

## Verification

- Real SQLite: 500 entries, four active workers, fast-worker refill while another waits.
- Publication happens after all results; errors and nonempty successes preserve the result contract.
- Restart, response checkpoint recovery, stale-controller fencing, cancellation and lease expiry.
- Model disable stops active requests and future claims.
- Native browser execution through the batch API, including process recycling after ten requests.
- Dagster + SQLite + PostgreSQL + ClickHouse integration, including sequential batches,
  lost publication acknowledgement, projection repair and a new Backoffice resume owner.
- Run focused Brave/queue tests and `uv run dg check defs` before deployment.

## Earlier browser-reuse rollout (already deployed)

- Browser release: `58e1815f8624fd4d307d7b5b7dc6a6f3f809d932c13eefa0330e36060823fb6e`.
- Dagster hot-sync succeeded without restarting its supervisor.
- Original run `8404ac50-a1ec-436a-a59b-57978a00e830` stopped gracefully with zero
  active browser requests before service activation; 2,978 saved outcomes remained.
- Resumed run: `ea20ff85-78a7-4054-981c-226421e47bc1`, preserving task
  `077bb1e4-6a7d-461b-930e-22348d32bf9e`, execution
  `053853b3-fc9c-4c4e-a49d-acebc383773d` and all 662,237 frozen inputs.
- At 17:44 UTC the run was STARTED, with 32 new successful outcomes and no new
  failed outcomes. All four route workers reused their browser processes. Service
  logs confirmed the first close after request 10 and a new process generation
  under the same session with its request counter reset to 1.
- At 17:43 UTC the first 17 measured gaps averaged 0.348 seconds, compared with
  approximately 6.4 seconds before deployment. A progress interval reported
  10.49 entries/minute versus the earlier 30-minute baseline of 7.63/minute.
  These are early observations, not a sustained throughput guarantee.

## Service-batch rollout (2026-09-26)

- Implementation committed to `main` as `9079031d6`, after incorporating the newer
  IP-enrichment changes. The unrelated local `corpscout/searcher/` directory was excluded.
- Browser release: `df7ad5b1bd2a630763e74d7c082fa3e3ea0b9a45500800e35925660b1a2e4020`.
  PostgreSQL admission and ClickHouse publication settings are installed through
  the ignored Ansible secrets file; no credentials are stored in Git.
- The previous run `ea20ff85-78a7-4054-981c-226421e47bc1` failed before activation
  with `Brave route crawl_proxy1 failed (OperationalError)`. Its 4,208 saved
  results remained intact, and zero browser requests were active at activation.
- Browser activation and Dagster hot-sync both passed. Dagster's supervisor was
  preserved. Post-merge checks passed: 157 focused Python/migration tests,
  77 Backoffice queue tests, and `dg check defs`.
- Live verification run `51ed3dc7-eae0-4af6-aa5d-57cbf91f0dbe` processed batch
  `7421c254-72aa-5f85-856c-eabe4ed3b3ad`. All eight outcomes reached ClickHouse:
  seven successes, one CAPTCHA failure, eight distinct result IDs, no empty
  successes, and seven matching rows in the successful-answer projection.
  It was then stopped gracefully to change the operational batch size.
- Resumed run: `020b0c4f-b1b5-485d-9c43-a9ad335eecb3`, batch size 500.
  Task `077bb1e4-6a7d-461b-930e-22348d32bf9e` and execution
  `053853b3-fc9c-4c4e-a49d-acebc383773d` remain unchanged. All 662,237 unique
  frozen inputs were verified present after the canary.
- At 19:46:36 UTC, batch `1cc03553-30c1-5b4c-9546-5e0a655876dd` was running:
  two successful local results, four active browsers, 494 pending, no failures.
  This batch is not yet published; publication occurs after all 500 finish.
  Dagster logs show service-batch counts and processing speed. The CAPTCHA
  statistics sensor is running and its latest tick saved 40 request observations.
- SQLite database, WAL and shared-memory files under `/var/lib/browser-service/`
  are owned by `browser-service` with mode 0600. No sustained-throughput claim is
  made from this short deployment verification.
