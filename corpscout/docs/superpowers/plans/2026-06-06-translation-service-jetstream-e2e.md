# Translation Service JetStream E2E App Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Go E2E app under `/Users/graovic/pulsarpoint/ppoint/companycollect/e2e/translation_e2e` that can be compiled or run with `go run cmd/main.go`, publish 50 translation batches into an isolated JetStream input queue, read results from an isolated output queue, validate translation result shape, and produce a detailed speed/correctness report.

**Architecture:** The E2E app is independent from scheduler packages and defines the small source-translation job/result JSON contracts locally, because it lives outside the scheduler `internal` import boundary. The translation-service worker still needs configurable JetStream input/output queue settings, so the plan includes a small Python worker config change that keeps production defaults while allowing the E2E app to target isolated queues.

**Tech Stack:** Go command-line app, `github.com/nats-io/nats.go`, NATS JetStream, Python translation-service worker, translation-service-configured provider/model.

---

## File Structure

- Modify `data-pipelines/services/translation-service/src/corpscout_translation_service/nats_worker.py`
  - Add env-configurable JetStream job/result streams and subjects.
  - Keep existing production defaults.

- Modify `data-pipelines/services/translation-service/tests/test_nats_worker.py`
  - Verify defaults and env overrides.

- Create `e2e/translation_e2e/go.mod`
  - Standalone Go module for this E2E app.

- Create `e2e/translation_e2e/README.md`
  - What the app tests.
  - How to start NATS and the translation worker.
  - How to run the app.
  - Expected report fields and exit behavior.

- Create `e2e/translation_e2e/cmd/main.go`
  - CLI entry point.
  - Parses command-line args and runs the test.

- Create `e2e/translation_e2e/internal/config/config.go`
  - CLI flags and validation.

- Create `e2e/translation_e2e/internal/contracts/contracts.go`
  - Local JSON contract structs for JetStream translation jobs/results.

- Create `e2e/translation_e2e/internal/fixtures/fixtures.go`
  - Prebuilds 50 deterministic batches before injection starts.

- Create `e2e/translation_e2e/internal/jetstream/jetstream.go`
  - Ensures input/output streams.
  - Purges streams for an isolated run.
  - Publishes jobs.
  - Reads input queue depth.
  - Pull-consumes results.

- Create `e2e/translation_e2e/internal/runner/runner.go`
  - Orchestrates injection and result consumption.
  - Publishes one new input message only when input queue depth is at or below `--max-input-messages`.
  - Enforces timeout.

- Create `e2e/translation_e2e/internal/validate/validate.go`
  - Validates output result format and correlation with sent batches.

- Create `e2e/translation_e2e/internal/report/report.go`
  - Builds detailed terminal and JSON reports.

---

## CLI Contract

The app must run with:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/e2e/translation_e2e
go run cmd/main.go \
  --nats-url nats://localhost:4222 \
  --input-queue e2e.source.translation.jobs \
  --output-queue e2e.source.translation.results
```

Required and important arguments:

- `--nats-url`: NATS URL. Default `nats://localhost:4222`.
- `--input-queue`: JetStream input subject/queue. Default `e2e.source.translation.jobs`.
- `--output-queue`: JetStream output subject/queue. Default `e2e.source.translation.results`.
- `--input-stream`: JetStream stream containing the input queue. Default `E2E_SOURCE_TRANSLATION_JOBS`.
- `--output-stream`: JetStream stream containing the output queue. Default `E2E_SOURCE_TRANSLATION_RESULTS`.
- `--max-input-messages`: Maximum live input messages allowed before publishing a new batch. Default `1`.
- `--batches`: Number of prebuilt batches. Default `50`.
- `--terms-per-batch`: Terms per batch. Default `4`.
- `--timeout`: End-to-end timeout. Default `5m`.
- `--provider`: Translation provider sent in jobs. Default `default`, which the translation service resolves from its own config.
- `--model`: Translation model sent in jobs. Default `default`, which the translation service resolves from its own config.
- `--allowed-providers`: Comma-separated provider values the E2E runner may send. Default `default,local,deepseek,deepseek-v4-flash`.
- `--allowed-models`: Comma-separated model values the E2E runner may send. Default `default,qwen3:6b,deepseek-chat,deepseek-v4-flash`.
- `--prompt-version`: Prompt version sent in jobs. Default `v1`.
- `--source`: Source name sent in jobs. Default `e2e`.
- `--report-json`: Optional path to write JSON report.
- `--purge`: Whether to purge E2E streams before the run. Default `true`.

Exit behavior:

- Exit `0` when all sent batches receive valid results before timeout.
- Exit nonzero when any result is malformed, missing, duplicated, timed out, contains failures, or uses provider/model values not allowed by the runner config.

---

## Task 1: Make Translation Worker JetStream Queues Configurable

**Files:**
- Modify `data-pipelines/services/translation-service/src/corpscout_translation_service/nats_worker.py`
- Modify `data-pipelines/services/translation-service/tests/test_nats_worker.py`

- [ ] Add tests for `jetstream_translation_config_from_env()` defaults:
  - `source.translation.jobs`
  - `source.translation.results`
  - `SOURCE_TRANSLATION`
  - durable `translation-service`

- [ ] Add tests for env overrides:
  - `TRANSLATION_JETSTREAM_JOB_SUBJECT`
  - `TRANSLATION_JETSTREAM_RESULT_SUBJECT`
  - `TRANSLATION_JETSTREAM_JOB_STREAM`
  - `TRANSLATION_JETSTREAM_RESULT_STREAM`
  - `TRANSLATION_JETSTREAM_DURABLE`

- [ ] Implement `JetStreamTranslationConfig` and `jetstream_translation_config_from_env()`.

- [ ] Update worker startup to:
  - ensure configured streams,
  - pull-subscribe configured input queue,
  - publish results to configured output queue.

- [ ] Verify:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/data-pipelines/services/translation-service
uv run pytest tests/test_nats_worker.py -q
uv run pytest -q
```

---

## Task 2: Create Standalone Go App Skeleton

**Files:**
- Create `e2e/translation_e2e/go.mod`
- Create `e2e/translation_e2e/cmd/main.go`
- Create `e2e/translation_e2e/internal/config/config.go`

- [ ] Create module:

```go
module github.com/pulsarpoint/companycollect/e2e/translation_e2e

go 1.23

require (
	github.com/cockroachdb/errors v1.11.3
	github.com/nats-io/nats.go v1.39.1
)
```

- [ ] Add CLI config parser with defaults from the CLI contract.

- [ ] Add validation:
  - `--batches > 0`
  - `--terms-per-batch > 0`
  - `--timeout > 0`
  - input and output queues are different
  - input and output streams are different
  - `--max-input-messages >= 0`
  - `--provider` is present in `--allowed-providers`
  - `--model` is present in `--allowed-models`
  - `mock` and `mock-model` are rejected unless the translation service later explicitly supports them as normal deployable values

- [ ] Add `cmd/main.go` that loads config and calls `runner.Run(ctx, cfg)`.

- [ ] Verify:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/e2e/translation_e2e
go mod tidy
go run cmd/main.go --help
```

Expected: help text lists all flags.

---

## Task 3: Add Local Translation Contracts

**Files:**
- Create `e2e/translation_e2e/internal/contracts/contracts.go`

- [ ] Define local structs matching translation-service JetStream JSON:
  - `TranslationJob`
  - `TranslationJobTerm`
  - `TranslationResult`
  - `TranslationResultTerm`
  - `TranslationFailureResult`

- [ ] Keep field names identical:
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
  - `results`
  - `failures`
  - `duration_ms`

- [ ] Add small encode/decode unit tests in the same module so contract drift is caught locally.

---

## Task 4: Prebuild 50 Translation Batches

**Files:**
- Create `e2e/translation_e2e/internal/fixtures/fixtures.go`
- Create `e2e/translation_e2e/internal/fixtures/fixtures_test.go`

- [ ] Build all batches before publishing starts.

- [ ] Default fixture behavior:
  - `50` batches,
  - `4` terms per batch,
  - deterministic `job_id` and `batch_id`,
  - deterministic SHA-256 `term_key`,
  - one `company_id` per batch,
  - `source_lang` and `target_lang` are set inside each generated job, not from command-line flags,
  - default generated jobs use `source_lang=et` and `target_lang=en`,
  - source text contains a 9-digit organization number.

- [ ] Do not use the translation-service test-only mock provider. The default fixture should work with real configured translation-service providers by validating output format and non-empty translations, not exact translated text.

- [ ] Verify fixture tests:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/e2e/translation_e2e
go test ./internal/fixtures -count=1
```

---

## Task 5: Add JetStream Adapter

**Files:**
- Create `e2e/translation_e2e/internal/jetstream/jetstream.go`

- [ ] Connect to NATS using `--nats-url`.

- [ ] Ensure input stream:
  - stream name from `--input-stream`,
  - subject from `--input-queue`,
  - `nats.WorkQueuePolicy`,
  - file storage.

- [ ] Ensure output stream:
  - stream name from `--output-stream`,
  - subject from `--output-queue`,
  - `nats.WorkQueuePolicy`,
  - file storage.

- [ ] If `--purge=true`, purge both streams before starting.

- [ ] Implement:
  - `InputDepth(ctx) (uint64, error)`
  - `PublishJob(ctx, job) error`
  - `FetchResults(ctx, max int) ([]ResultMessage, error)`
  - `Close()`

- [ ] Result messages must be acknowledged only after validation succeeds.

---

## Task 6: Implement Injection and Result Loop

**Files:**
- Create `e2e/translation_e2e/internal/runner/runner.go`

- [ ] Start timer when the run starts, before the first input publish attempt.

- [ ] Prebuild all fixture jobs before entering the injection loop.

- [ ] Injection behavior:
  - read input queue depth,
  - if depth `<= --max-input-messages`, publish exactly one new batch,
  - otherwise sleep one second and check again,
  - stop after all prebuilt batches are published.

- [ ] Result behavior:
  - continuously pull results from output queue,
  - decode JSON,
  - correlate by `job_id` and `batch_id`,
  - validate result format,
  - ack only after validation succeeds,
  - finish when all sent batches have valid output.

- [ ] Timeout behavior:
  - default timeout `5m`,
  - configurable by `--timeout`,
  - on timeout report sent/received/missing batch IDs and exit nonzero.

---

## Task 7: Validate Output Format

**Files:**
- Create `e2e/translation_e2e/internal/validate/validate.go`
- Create `e2e/translation_e2e/internal/validate/validate_test.go`

- [ ] Validate result-level fields:
  - `job_id` matches sent job,
  - `batch_id` matches sent job,
  - `source` matches,
  - `provider` is non-empty,
  - `prompt_version` matches,
  - `company_ids` match,
  - `duration_ms >= 0`,
  - `status` is `succeeded`, `partial`, or `failed`.

- [ ] Validate terms:
  - every input term has one output result or failure,
  - no unknown term keys,
  - no duplicate term keys,
  - successful term has `translated_text`,
  - failure term has `status`.

- [ ] For normal configured providers, additionally require:
  - result status `succeeded`,
  - no failures,
  - every successful term has a non-empty `translated_text`.

---

## Task 8: Produce Detailed Report

**Files:**
- Create `e2e/translation_e2e/internal/report/report.go`

- [ ] Print a detailed terminal report:

```text
Translation E2E Report
Run ID: ...
NATS URL: ...
Input queue: ...
Output queue: ...
Batches planned: 50
Batches sent: 50
Batches received: 50
Terms sent: 200
Terms succeeded: 200
Terms failed: 0
Started at: ...
Finished at: ...
Elapsed: ...
Batches/sec: ...
Terms/sec: ...
Min batch latency: ...
P50 batch latency: ...
P95 batch latency: ...
Max batch latency: ...
Missing batches: []
Invalid batches: []
Status: PASS
```

- [ ] If `--report-json` is set, write the same metrics as JSON.

- [ ] Include enough detail to diagnose failures:
  - missing job IDs,
  - duplicate job IDs,
  - invalid result errors,
  - timeout reason,
  - last observed input queue depth.

---

## Task 9: Add README

**Files:**
- Create `e2e/translation_e2e/README.md`

- [ ] Explain what the app tests:
  - translation service JetStream input/output processing,
  - output contract correctness,
  - throughput and latency,
  - bounded input queue feeding.

- [ ] Include worker startup example:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/data-pipelines/services/translation-service
NATS_URL=nats://localhost:4222 \
TRANSLATION_DEFAULT_PROVIDER=local \
TRANSLATION_DEFAULT_MODEL=qwen3:6b \
TRANSLATION_PROVIDER_LOCAL_BASE_URL=http://100.77.62.33:8888 \
TRANSLATION_PROVIDER_LOCAL_MODEL=qwen3:6b \
TRANSLATION_JETSTREAM_JOB_STREAM=E2E_SOURCE_TRANSLATION_JOBS \
TRANSLATION_JETSTREAM_JOB_SUBJECT=e2e.source.translation.jobs \
TRANSLATION_JETSTREAM_RESULT_STREAM=E2E_SOURCE_TRANSLATION_RESULTS \
TRANSLATION_JETSTREAM_RESULT_SUBJECT=e2e.source.translation.results \
TRANSLATION_JETSTREAM_DURABLE=e2e-translation-service \
uv run corpscout-translation-service worker
```

- [ ] Include app run example:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/e2e/translation_e2e
go run cmd/main.go \
  --nats-url nats://localhost:4222 \
  --input-queue e2e.source.translation.jobs \
  --output-queue e2e.source.translation.results \
  --max-input-messages 1 \
  --timeout 5m \
  --provider default \
  --model default
```

---

## Task 10: Full Verification

- [ ] Verify Python translation service:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/data-pipelines/services/translation-service
uv run pytest -q
```

- [ ] Verify Go app unit tests:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/e2e/translation_e2e
go test ./... -count=1
```

- [ ] Verify standalone app help:

```bash
go run cmd/main.go --help
```

- [ ] Verify real E2E run with NATS + worker:

```bash
go run cmd/main.go \
  --nats-url nats://localhost:4222 \
  --input-stream E2E_SOURCE_TRANSLATION_JOBS \
  --input-queue e2e.source.translation.jobs \
  --output-stream E2E_SOURCE_TRANSLATION_RESULTS \
  --output-queue e2e.source.translation.results \
  --batches 50 \
  --max-input-messages 1 \
  --timeout 5m \
  --provider default \
  --model default \
  --report-json /tmp/translation-e2e-report.json
```

Expected:

- Terminal report prints `Status: PASS`.
- JSON report is written.
- Exit code is `0`.
- Batches sent = batches received = `50`.
- Terms succeeded = `50 * terms_per_batch`.
- Provider/model in the report match the values resolved and returned by the translation service.

---

## Design Notes

- The app lives in `/Users/graovic/pulsarpoint/ppoint/companycollect/e2e/translation_e2e`, a subfolder dedicated to this specific E2E test.
- The app is intentionally standalone and does not import scheduler `internal` packages.
- The CLI uses `queue` terminology for the user-facing JetStream subjects because that is what the runner publishes to and consumes from.
- Separate stream names remain configurable because JetStream needs streams behind those queues.
- Input stream uses `WorkQueuePolicy` so `StreamInfo.State.Msgs` acts as live queue depth.
- The timer starts when the app starts the run and finishes when all output batches are received and validated.
- Default timeout is `5m`, configurable with `--timeout`.
- Default max input queue messages is `1`, configurable with `--max-input-messages`.
- Default provider/model are `default`/`default`; `mock` is not an E2E option because this runner should exercise the real translation-service provider path.

## Self-Review

- Spec coverage: The revised plan places the app under the top-level `e2e` folder, creates a specific `translation_e2e` subfolder, supports `go run cmd/main.go`, provides CLI arguments for NATS URL/input/output queues/max input messages/timeout/provider/model, prebuilds 50 batches, validates translated output, rejects mock defaults, and reports speed.
- Placeholder scan: No unresolved placeholder steps remain.
- Type consistency: Local Go contracts match the Python translation-service JetStream JSON fields.
