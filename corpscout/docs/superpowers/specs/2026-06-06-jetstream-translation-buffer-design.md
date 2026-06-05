# JetStream Translation Buffer Design

## Goal

Keep the local LLM continuously utilized while preserving Postgres as the source of truth for translation queue state.

The current request/reply NATS path makes each Temporal workflow wait for the translation service to finish a batch. That creates fragmented utilization when workflows are preparing queues, claiming batches, waiting on retries, or blocked by capacity checks. JetStream should introduce a small delivery buffer so the translation service always has a next batch ready.

## Core Decision

JetStream is a work buffer only. It is not the reliability source and it does not own retry state.

Postgres remains authoritative for:

- company queue entry status: `pending`, `running`, `succeeded`, `failed`
- batch id ownership
- stale running reset
- translation term storage
- binding application
- final completion state

The translation service stays simple:

- pull one JetStream job at a time
- validate the payload
- ack the input message early
- call the LLM
- publish one result message
- move to the next job

It does not connect to Postgres and does not implement source-specific queue logic.

## Architecture

Add two scheduler-side background components:

1. Translation dispatcher
2. Translation result collector

The dispatcher keeps each source topped up to a small configured buffer target. The initial target is two running/published batches per source.

The result collector consumes translation result messages, saves translated terms, applies bindings, and completes or releases the Postgres batch.

The translation service processes exactly one batch at a time by default. LLM concurrency is controlled by the translation service worker configuration, not by the number of buffered JetStream jobs.

## Flow

1. A user starts a BRREG or Ariregister translation workflow.
2. The workflow refreshes/prepares the Postgres translation queue.
3. The dispatcher observes each enabled source.
4. For a source with fewer than `source_buffer_target` running batches, the dispatcher claims pending queue entries in Postgres.
5. Claiming changes those entries to `running`, assigns a `batch_id`, and returns company ids plus request metadata.
6. The dispatcher publishes a translation job to JetStream.
7. The translation service pull-consumes one job, validates payload, and acks the JetStream message early.
8. The translation service calls the LLM and publishes a translation result message.
9. The result collector consumes the result, saves terms, applies bindings, and marks the Postgres batch complete.
10. A watchdog resets stale `running` batches back to `pending` after the configured lease timeout.

## Buffering Rules

Default values:

- `source_buffer_target = 2`
- `translation_batch_lease_seconds = 1800`
- translation service concurrency = 1

The source buffer target means "published or running batches per source", not "active LLM calls per source".

Example:

- BRREG can keep two batches ready.
- Ariregister can keep two batches ready.
- If more sources are added, each source can keep two batches ready.
- The translation service still processes one batch at a time unless its concurrency is explicitly increased.

This keeps the LLM fed without letting source count directly multiply LLM concurrency.

## JetStream Subjects

Use source-neutral subjects:

- Jobs: `source.translation.jobs`
- Results: `source.translation.results`

Each job payload includes:

- `job_id`
- `batch_id`
- `source`
- `source_lang`
- `target_lang`
- `provider`
- `model`
- `prompt_version`
- `company_ids`
- `terms`

Each result payload includes:

- `job_id`
- `batch_id`
- `source`
- `provider`
- `model`
- `prompt_version`
- `results`
- `failures`
- `duration_ms`

The result collector must treat `batch_id` idempotently because a stale reset or duplicate result can happen.

## Acknowledgement Policy

The translation service acks the job after payload validation, before the LLM call.

This is intentional. Postgres owns recovery. If the translation service crashes after acking a job, the batch remains `running` in Postgres and the watchdog resets it after the lease expires.

The lease should not be too short because full batches can take time when the local LLM is fully utilized. The default should remain around 30 minutes, with 15 minutes as the practical lower bound.

## Error Handling

Dispatcher publish failure:

- release the claimed Postgres batch back to `pending`
- log once at the dispatcher boundary
- retry later

Translation service payload validation failure:

- ack the invalid input
- publish a failed result when `batch_id` is available
- otherwise log and drop, because Postgres recovery cannot map the payload

LLM failure:

- publish failures for the batch terms when possible
- result collector saves failed term rows and releases or fails the batch according to existing retry policy

Result collector failure:

- do not ack the result message until Postgres updates succeed
- JetStream redelivers the result
- Postgres writes stay idempotent by `batch_id` and term key

Stale running batch:

- watchdog resets `running` rows older than `translation_batch_lease_seconds` to `pending`
- dispatcher can claim them again

## Temporal Role

Temporal should start and observe translation runs, not drive every LLM call.

Workflows still prepare the Postgres queue and can report high-level progress. The steady-state buffer filling and result consumption should be scheduler background work so LLM utilization does not depend on workflow activity timing.

## Testing

Scheduler tests:

- dispatcher claims until each source reaches `source_buffer_target`
- dispatcher does not exceed per-source buffer target
- dispatcher releases a batch when JetStream publish fails
- result collector completes a batch idempotently
- result collector does not ack failed Postgres writes
- watchdog resets stale running batches

Translation service tests:

- pull handler validates and early-acks valid jobs
- service processes one job at a time by default
- service publishes success results
- service publishes structured failures for LLM errors
- invalid payloads with a usable `batch_id` publish failed results

Integration tests:

- start BRREG and Ariregister queues
- verify each source can buffer two jobs
- verify translation service processes jobs sequentially
- verify stale Postgres reset recovers an acked-but-lost job

## Out Of Scope

- using JetStream as the canonical queue
- exactly-once delivery
- Postgres access from the translation service
- result/history metrics tables beyond the existing queue state
- increasing LLM concurrency by default
