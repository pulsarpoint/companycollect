# Queue-First Source Translation Design

## Goal

Replace the current synchronous translation-batch loop with a queue-first flow that is easier to reason about and can keep GPU translation workers busy without making Temporal coordinate every small step.

The desired shape is:

1. Build translation batches.
2. Publish batches to a durable NATS JetStream input queue.
3. Let the translation service pull the next batch only when it is ready to process it.
4. Publish translation output to a durable output queue.
5. Let a result handler save translations, apply `_en` columns, and retry failed entries by publishing new input batches.

## Current Context

BRREG and Ariregister use a shared source translation pattern:

- source-specific tables expose missing translation fields;
- a SQLite workset stores translation terms, bindings, and batches for a translation run;
- uncached terms are sent to the NATS translation service;
- successful results are saved into source-specific `translation_terms` tables and applied into `_en` columns;
- source-specific materialized views summarize translation status for UI and task-state reporting.

The current workflow still behaves like request/reply:

```text
claim batch -> call NATS request/reply -> wait -> save -> claim next batch
```

That is safe but too segmented. Temporal waits for each batch and the GPU can become idle between small units of work.

## Recommended Architecture

Use JetStream as the active work queue and keep the database/SQLite state for audit, idempotency, source-specific application, and UI visibility.

```text
translation workflow/action
  -> builds translation batches
  -> publishes all batches to source.translation.input

translation service
  -> pull-consumes source.translation.input
  -> processes one batch per available GPU slot
  -> publishes result to source.translation.output
  -> acks input only after output publish succeeds

translation result handler
  -> consumes source.translation.output
  -> saves successful terms
  -> applies translated values to _en columns
  -> republishes retryable failures as new input batches
  -> acks output only after DB commit and retry publish succeed
```

This removes the need for a dispatcher loop that checks whether NATS is empty. The queue controls work availability. The translation service controls GPU utilization by pulling only when it can process another batch.

## Queue Design

Use NATS JetStream with file-backed streams.

Input subject:

```text
source.translation.input
```

Output subject:

```text
source.translation.output
```

Optional terminal failure subject:

```text
source.translation.failed
```

The stream should be configured with explicit limits:

- file storage;
- bounded `max_msg_size`;
- bounded `max_bytes`;
- bounded `max_age`;
- duplicate detection enabled through `Nats-Msg-Id`;
- work-queue style retention where processed messages are acknowledged and removed according to stream policy.

JetStream is the short-lived work queue, not the long-term translation database.

## Batch Size

Publish one message per batch, not one message per term.

Initial defaults should be conservative:

- `max_terms_per_batch`: 100-500;
- `max_batch_payload_bytes`: 20-100 KB;
- `max_attempts`: 3 or 5;
- one active batch per GPU worker unless the translation service explicitly supports more parallelism.

Example:

```text
5,000,000 translation terms / 250 terms per batch = 20,000 queue messages
```

That is a better target than creating millions of individual term messages.

## Input Message

Each input message represents one translation batch.

```json
{
  "schema_version": "source-translation-batch/v1",
  "batch_id": "uuid",
  "source": "brreg",
  "source_lang": "no",
  "target_lang": "en",
  "provider": "default",
  "model": "",
  "prompt_version": "v1",
  "attempt": 1,
  "max_attempts": 3,
  "terms": [
    {
      "term_key": "sha256",
      "source_text": "Aksjeselskap",
      "source_text_normalized": "aksjeselskap"
    }
  ],
  "metadata": {
    "workflow_id": "temporal-workflow-id",
    "workset_path": "/var/lib/corpscout/worksets/brreg-translation-..."
  }
}
```

Use `batch_id` as the JetStream duplicate message id. Retries should get a new `batch_id` and carry `parent_batch_id` in metadata if needed.

## Output Message

The translation service publishes one output message per processed input batch.

```json
{
  "schema_version": "source-translation-result/v1",
  "batch_id": "uuid",
  "source": "brreg",
  "source_lang": "no",
  "target_lang": "en",
  "provider": "default",
  "model": "model-name",
  "prompt_version": "v1",
  "attempt": 1,
  "results": [
    {
      "term_key": "sha256",
      "source_text": "Aksjeselskap",
      "source_text_normalized": "aksjeselskap",
      "translated_text": "Limited liability company",
      "status": "succeeded"
    }
  ],
  "failures": [
    {
      "term_key": "sha256",
      "source_text": "text",
      "source_text_normalized": "text",
      "status": "failed_retryable",
      "error_code": "model_timeout",
      "error": "translation timed out"
    }
  ]
}
```

The result handler must treat output messages as at-least-once. Duplicate outputs must not apply translations twice or create duplicate cache rows.

## Persistence Model

Keep durable state outside JetStream.

Postgres source tables remain the authoritative company data. Source-specific `translation_terms` tables remain the persistent translation cache and audit table.

SQLite can remain a run-scoped workset:

- snapshot of missing translation fields for one workflow/run;
- term dedupe and batching state;
- mapping from translated terms back to source rows and target `_en` columns;
- local batch status for run-level debug.

The first implementation should keep SQLite run-scoped, not a global forever queue. New source records that appear while a run is active are picked up by the next translation run.

## Result Handling

The result handler owns database mutation.

For each output message:

1. Start a DB transaction where possible.
2. Upsert successful and failed terms into the source-specific `translation_terms` table.
3. Apply successful translations into supported source `_en` columns.
4. Mark batch/run state in SQLite or a small Postgres batch-audit table.
5. For retryable failures with `attempt < max_attempts`, publish a new input batch containing only failed terms.
6. For terminal failures or exhausted attempts, persist `failed_terminal`.
7. Refresh the source translation-status materialized view once after the result batch is saved and applied.
8. Ack the output message only after DB changes and retry publishes succeed.

If retry publish fails, the output message must remain unacked so the result handler can replay safely.

## Materialized View Policy

Materialized views are for UI and reporting, not for dispatch control.

Do not refresh a materialized view after each translation-term insert. Refresh at coarse boundaries:

- before building a workset, if the workset uses the materialized view to select missing fields;
- after a result batch is saved and applied;
- after a full translation run completes, if the run applies many batches and we choose to defer intermediate refreshes.

The queue and result handler can continue working while the materialized view is briefly stale.

## Retry Behavior

Retries happen by publishing a new input batch for failed terms.

Rules:

- retry only `failed_retryable`;
- increment `attempt`;
- carry parent batch metadata for traceability;
- stop at `max_attempts`;
- persist exhausted entries as `failed_terminal`;
- do not retry entries that already have a successful cached translation for the same `(source, source_lang, target_lang, prompt_version, term_key)`.

This keeps retry logic visible and prevents infinite queue loops.

## Idempotency

All queue processing must be idempotent.

Input publishing:

- use `Nats-Msg-Id` with the batch id for duplicate detection;
- record batch ids in SQLite or a Postgres audit table.

Result handling:

- upsert translation terms by the existing unique key `(source, source_lang, target_lang, prompt_version, term_key)`;
- apply `_en` updates only when target columns are empty or still need the same translation;
- track processed output batch ids so duplicate result messages are ignored or treated as no-ops;
- ack messages only after durable state changes commit.

## Failure Scenarios

Translation service crashes after pulling input:

- JetStream redelivers after ack wait expires.

Translation service translates but crashes before publishing output:

- input message remains unacked and is redelivered.

Translation service publishes output but crashes before acking input:

- input may be redelivered and output may be duplicated; result handler idempotency handles this.

Result handler crashes before DB commit:

- output remains unacked and is redelivered.

Result handler commits DB but crashes before ack:

- output is redelivered; processed batch id and term upserts make it a no-op.

## Source Responsibilities

The shared queue machinery should stay source-agnostic.

Each source still owns:

- loading missing translation fields;
- defining source language and target language;
- reading and writing persistent translation cache rows;
- applying translated values to supported `_en` columns;
- refreshing its own status materialized view.

This keeps BRREG, Ariregister, Sweden, and future sources aligned without dynamic SQL updates for arbitrary tables.

## Operational Visibility

Expose queue and translation status in logs and UI/API metrics:

- input queue pending messages;
- input queue ack-pending messages;
- output queue pending messages;
- output queue ack-pending messages;
- batches published;
- batches completed;
- retry batches published;
- terminal failures;
- average batch processing time;
- materialized-view refresh time.

The UI should not need exact real-time materialized-view freshness during translation. Queue and batch metrics explain live processing better.

## Migration Path

Phase 1: Add queue contracts and JetStream client helpers.

Phase 2: Add result handler with idempotent save/apply/retry behavior.

Phase 3: Change BRREG and Ariregister workflows from request/reply translation to queue publishing.

Phase 4: Move materialized-view refresh to batch or run boundaries and remove any refresh work from per-term insert paths.

Phase 5: Add Sweden source translation using the same queue contract after Sweden normalized source tables and `_en` columns are ready.

## Testing

Required test coverage:

- batch builder creates bounded batch messages and does not emit one message per term;
- translation service client uses JetStream publish/ack semantics correctly;
- result handler saves successful terms and applies `_en` columns;
- result handler republishes retryable failures with incremented attempts;
- result handler marks exhausted failures as terminal;
- duplicate output messages are idempotent;
- materialized-view refresh happens once per configured boundary, not once per term insert;
- workflow/action publishes all batches and returns queue publication counts;
- source-specific adapters preserve BRREG and Ariregister translation behavior.

## Non-Goals

- Do not use JetStream as the long-term translation cache.
- Do not use the materialized view as queue-dispatch control.
- Do not create one NATS message per translation term.
- Do not refactor unrelated source ingestion flows.
- Do not require Sweden translation until Sweden normalized source tables and translatable `_en` columns are defined.
