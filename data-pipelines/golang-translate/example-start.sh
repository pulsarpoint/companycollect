#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${script_dir}"

run_id="$(date -u +%Y%m%d%H%M%S)"

base_url="${BASE_URL:-http://100.77.62.33:8888}"
model="${MODEL:-qwen3:6b}"
input="${INPUT:-input.json}"
strategy="${STRATEGY:-sequential}"
items="${ITEMS:-32}"
batch_size="${BATCH_SIZE:-4}"
parallel="${PARALLEL:-1}"
timeout="${TIMEOUT:-5m}"
request_timeout="${REQUEST_TIMEOUT:-120s}"
scenario="${SCENARIO:-manual-${run_id}}"
description="${DESCRIPTION:-Manual direct LLM translation benchmark run.}"
report_json="${REPORT_JSON:-/tmp/golang-translate-report-${run_id}.json}"
responses_json="${RESPONSES_JSON:-/tmp/golang-translate-responses-${run_id}.json}"

echo "Starting direct Go LLM translation benchmark"
echo "Base URL: ${base_url}"
echo "Model: ${model}"
echo "Scenario: ${scenario}"
echo "Description: ${description}"
echo "Strategy: ${strategy}"
echo "Items: ${items}"
echo "Batch size: ${batch_size}"
echo "Parallel: ${parallel}"
echo "Report: ${report_json}"
echo "Responses: ${responses_json}"

exec go run ./cmd/main.go \
  --base-url "${base_url}" \
  --model "${model}" \
  --input "${input}" \
  --strategy "${strategy}" \
  --items "${items}" \
  --batch-size "${batch_size}" \
  --parallel "${parallel}" \
  --timeout "${timeout}" \
  --request-timeout "${request_timeout}" \
  --scenario "${scenario}" \
  --description "${description}" \
  --report-json "${report_json}" \
  --responses-json "${responses_json}"
