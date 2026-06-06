# Translation Service JetStream E2E

Standalone Go E2E runner for the Corpscout translation service JetStream worker.

The runner verifies:

- translation service input/output JetStream processing,
- output JSON contract correctness,
- bounded input queue feeding,
- batch throughput and latency,
- missing, duplicate, malformed, or failed results.

It does not use Postgres, Temporal, or scheduler internals.

## Start NATS

Use any NATS server with JetStream enabled. For local development:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/data-pipelines/services
docker compose up -d nats
```

## Start Translation Worker

Start the worker with the same isolated streams and subjects used by this runner:

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

## Run

Against the existing `companycollect` translation worker, use:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/e2e/translation_e2e
./example-start.sh
```

The example script uses the production translation JetStream subjects:

- `SOURCE_TRANSLATION`
- `source.translation.jobs`
- `source.translation.results`
- `translation-service` input durable consumer

It always passes `--purge=false` so it does not delete messages from the shared server stream.
For that shared stream, queue depth is measured from the translation-service consumer pending counts, because the production stream keeps already-acked historical messages.

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/e2e/translation_e2e
go run cmd/main.go \
  --nats-url nats://localhost:4222 \
  --input-stream E2E_SOURCE_TRANSLATION_JOBS \
  --input-queue e2e.source.translation.jobs \
  --output-stream E2E_SOURCE_TRANSLATION_RESULTS \
  --output-queue e2e.source.translation.results \
  --input-consumer e2e-translation-service \
  --max-input-messages 1 \
  --timeout 5m \
  --provider default \
  --model default \
  --report-json /tmp/translation-e2e-report.json
```

The runner prebuilds all batches before publishing starts. It checks the input stream depth once per second and publishes exactly one new batch only when the input queue depth is at or below `--max-input-messages`.

Default batch settings:

- `--batches 50`
- `--terms-per-batch 4`
- generated job language pair: `source_lang=et`, `target_lang=en`

`source_lang` and `target_lang` are part of the generated input data, not command-line arguments.

## Benchmark Runs

Use `benchmark-start.sh` to run the same E2E test repeatedly with different parameters and collect the outputs in one folder:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/e2e/translation_e2e
./benchmark-start.sh
```

Default benchmark matrix:

- `TERMS_PER_BATCH_VALUES=1,2,4,8,16`
- `MAX_INPUT_MESSAGES_VALUES=0,1,2,4`
- `BATCHES=30`
- `REPEATS=1`
- `TIMEOUT=10m`

Override values with environment variables:

```bash
TERMS_PER_BATCH_VALUES=4,8,16 \
MAX_INPUT_MESSAGES_VALUES=1,2,4 \
BATCHES=50 \
REPEATS=2 \
PAUSE_SECONDS=10 \
./benchmark-start.sh
```

The script writes to `output/benchmark-<timestamp>/` by default. That folder contains:

- `summary.csv`
- `analysis-prompt.md`
- one folder per scenario with `stdout.log`, `report.json`, and `scenario.env`

After the benchmark finishes, ask the LLM to analyze that output folder and start with `analysis-prompt.md`.

## Report

The terminal report includes:

- batches planned/sent/received,
- terms sent/succeeded/failed,
- elapsed time,
- batches/sec,
- terms/sec,
- min/P50/P95/max batch latency,
- missing batches,
- invalid batches,
- last observed input queue depth,
- final `PASS` or `FAIL` status.

The process exits `0` only when all sent batches receive valid successful results before timeout.
