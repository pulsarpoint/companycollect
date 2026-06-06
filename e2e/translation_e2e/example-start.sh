#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${script_dir}"

run_id="$(date -u +%Y%m%d%H%M%S)"

nats_url="${NATS_URL:-nats://companycollect:4222}"
input_stream="${INPUT_STREAM:-SOURCE_TRANSLATION}"
output_stream="${OUTPUT_STREAM:-SOURCE_TRANSLATION}"
input_queue="${INPUT_QUEUE:-source.translation.jobs}"
output_queue="${OUTPUT_QUEUE:-source.translation.results}"
input_consumer="${INPUT_CONSUMER:-translation-service}"
result_durable="${RESULT_DURABLE:-translation-e2e-results-${run_id}}"
max_input_messages="${MAX_INPUT_MESSAGES:-1}"
batches="${BATCHES:-50}"
terms_per_batch="${TERMS_PER_BATCH:-4}"
timeout="${TIMEOUT:-5m}"
provider="${PROVIDER:-default}"
model="${MODEL:-qwen3:6b}"
report_json="${REPORT_JSON:-/tmp/translation-e2e-report-${run_id}.json}"

echo "Starting translation E2E run against ${nats_url}"
echo "Input:  ${input_stream}/${input_queue}"
echo "Output: ${output_stream}/${output_queue}"
echo "Input consumer: ${input_consumer}"
echo "Report: ${report_json}"

exec go run ./cmd/main.go \
  --nats-url "${nats_url}" \
  --input-stream "${input_stream}" \
  --input-queue "${input_queue}" \
  --output-stream "${output_stream}" \
  --output-queue "${output_queue}" \
  --input-consumer "${input_consumer}" \
  --result-durable "${result_durable}" \
  --max-input-messages "${max_input_messages}" \
  --batches "${batches}" \
  --terms-per-batch "${terms_per_batch}" \
  --timeout "${timeout}" \
  --provider "${provider}" \
  --model "${model}" \
  --purge=false \
  --report-json "${report_json}"
