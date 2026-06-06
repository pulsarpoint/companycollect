# Go Direct LLM Translation Benchmark

Standalone Go benchmark for the local OpenAI-compatible translation LLM.

This app bypasses Corpscout, NATS, Temporal, and the Python translation service. It sends direct `/v1/chat/completions` requests from Go using the same prompt shape as the translation service.

## Single Scenario

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/data-pipelines/golang-translate
./example-start.sh
```

Defaults:

- `BASE_URL=http://100.77.62.33:8888`
- `MODEL=qwen3:6b`
- `INPUT=input.json`
- `STRATEGY=sequential`
- `ITEMS=32`
- `BATCH_SIZE=4`
- `PARALLEL=1`
- `TIMEOUT=5m`
- `REQUEST_TIMEOUT=120s`

Override values with environment variables:

```bash
STRATEGY=parallel \
ITEMS=64 \
BATCH_SIZE=8 \
PARALLEL=2 \
TIMEOUT=10m \
./example-start.sh
```

## Strategies

- `single`: sends all selected terms in one request. Use this to test long prompt behavior and JSON completeness.
- `sequential`: splits selected terms into fixed-size batches and sends one request at a time. Use this to measure request overhead by batch size.
- `parallel`: splits selected terms into fixed-size batches and sends multiple concurrent requests. Use this to find local LLM saturation limits.

## Benchmark Matrix

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/data-pipelines/golang-translate
./benchmark-start.sh
```

Default scenarios:

- `single-long`: one long request with 64 terms.
- `sequential-short-2`: sequential 2-term requests.
- `sequential-medium-8`: sequential 8-term requests.
- `sequential-large-16`: sequential 16-term requests.
- `parallel-short-4x2`: 4-term requests with parallelism 2.
- `parallel-short-4x4`: 4-term requests with parallelism 4.
- `parallel-medium-8x2`: 8-term requests with parallelism 2.
- `parallel-large-16x2`: 16-term requests with parallelism 2.

The benchmark writes to `output/benchmark-<timestamp>/` by default. That folder contains:

- `summary.csv`
- `analysis-prompt.md`
- one folder per scenario with `stdout.log`, `report.json`, `responses.json`, and `scenario.env`

After the run finishes, ask the LLM to analyze the output folder and start with `analysis-prompt.md`.

## Adaptive Batch Search

Use `scale-start.sh` to find a better sequential batch size without brute-forcing every value:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/data-pipelines/golang-translate
./scale-start.sh
```

Defaults:

- `ITEMS=128`
- `START_BATCH_SIZE=16`
- `MAX_BATCH_SIZE=128`
- `MIN_STEP=4`
- `MAX_ROUNDS=12`
- `DEGRADATION_PERCENT=5`

The search starts at `START_BATCH_SIZE`, doubles upward until a request fails or terms/sec drops more than `DEGRADATION_PERCENT` below the best result, then tests midpoint batch sizes around the best result until the search interval reaches `MIN_STEP`.

Example:

```bash
ITEMS=128 \
START_BATCH_SIZE=16 \
MAX_BATCH_SIZE=128 \
MIN_STEP=4 \
./scale-start.sh
```

The script writes to `output/scale-<timestamp>/` by default. That folder contains `summary.csv`, `results.tsv`, `analysis-prompt.md`, and one folder per tested batch size.

`input.json` has 64 base terms. If `ITEMS` is larger than 64, the runner cycles through the fixture and appends unique suffixes to repeated IDs, so `ITEMS=128`, `256`, and larger values still produce unique expected translation IDs.

## Dry Run

Validate the bash control flow without spending LLM time:

```bash
DRY_RUN=true ./benchmark-start.sh
```

## Direct Go Command

```bash
go run ./cmd/main.go \
  --base-url http://100.77.62.33:8888 \
  --model qwen3:6b \
  --input input.json \
  --strategy sequential \
  --items 32 \
  --batch-size 4 \
  --parallel 1 \
  --report-json /tmp/golang-translate-report.json \
  --responses-json /tmp/golang-translate-responses.json
```
