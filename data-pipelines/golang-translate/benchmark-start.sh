#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${script_dir}"

run_id="$(date -u +%Y%m%d%H%M%S)"
output_dir="${OUTPUT_DIR:-${script_dir}/output/benchmark-${run_id}}"
timeout="${TIMEOUT:-10m}"
request_timeout="${REQUEST_TIMEOUT:-180s}"
pause_seconds="${PAUSE_SECONDS:-5}"
dry_run="${DRY_RUN:-false}"

mkdir -p "${output_dir}"

summary_csv="${output_dir}/summary.csv"
analysis_prompt="${output_dir}/analysis-prompt.md"

printf 'scenario,strategy,items,batch_size,parallel,status,exit_code,elapsed,requests_per_second,terms_per_second,p95_request_latency,report_json,responses_json,stdout_log\n' > "${summary_csv}"

cat > "${analysis_prompt}" <<EOF
# Direct Go LLM Translation Benchmark Analysis Request

Analyze the direct LLM benchmark outputs in this folder.

This benchmark bypasses Corpscout, NATS, Temporal, and the Python translation service.
It calls the same OpenAI-compatible local LLM endpoint from Go using the same prompt shape as the translation service.

Focus on:

- Compare single long prompts against many shorter prompts.
- Compare sequential requests against parallel requests.
- Identify the batch size and parallelism where terms/sec improves without causing high p95 latency or failures.
- Identify malformed or incomplete JSON responses.
- Recommend production translation settings to test next through the full JetStream E2E harness.

The main index is:

- summary.csv

Each scenario folder contains:

- stdout.log
- report.json
- responses.json
- scenario.env

Benchmark started at UTC run id: ${run_id}
EOF

extract_line_value() {
  local label="$1"
  local file="$2"
  awk -F': ' -v label="${label}" '$1 == label { value=$2 } END { print value }' "${file}"
}

write_scenario_env() {
  local path="$1"
  local scenario="$2"
  local strategy="$3"
  local items="$4"
  local batch_size="$5"
  local parallel="$6"
  local description="$7"

  {
    printf 'RUN_ID=%s\n' "${run_id}"
    printf 'SCENARIO=%s\n' "${scenario}"
    printf 'DESCRIPTION=%s\n' "${description}"
    printf 'STRATEGY=%s\n' "${strategy}"
    printf 'ITEMS=%s\n' "${items}"
    printf 'BATCH_SIZE=%s\n' "${batch_size}"
    printf 'PARALLEL=%s\n' "${parallel}"
    printf 'TIMEOUT=%s\n' "${timeout}"
    printf 'REQUEST_TIMEOUT=%s\n' "${request_timeout}"
    printf 'BASE_URL=%s\n' "${BASE_URL:-http://100.77.62.33:8888}"
    printf 'MODEL=%s\n' "${MODEL:-qwen3:6b}"
    printf 'INPUT=%s\n' "${INPUT:-input.json}"
  } > "${path}"
}

scenarios=(
  "single-long|single|64|64|1|One long prompt with all fixture terms in one request; tests long context and JSON completeness."
  "sequential-short-2|sequential|64|2|1|Many tiny sequential requests; tests HTTP and scheduling overhead."
  "sequential-medium-8|sequential|64|8|1|Medium sequential batches; tests a conservative production-like batch size."
  "sequential-large-16|sequential|64|16|1|Large sequential batches; tests whether larger prompts improve throughput."
  "parallel-short-4x2|parallel|64|4|2|Small batches with two concurrent requests; tests mild LLM overlap."
  "parallel-short-4x4|parallel|64|4|4|Small batches with four concurrent requests; tests local LLM saturation pressure."
  "parallel-medium-8x2|parallel|64|8|2|Medium batches with two concurrent requests; tests buffered production behavior."
  "parallel-large-16x2|parallel|64|16|2|Large batches with two concurrent requests; tests long prompts under concurrency."
)

echo "Direct Go LLM benchmark output: ${output_dir}"
echo "timeout per scenario: ${timeout}"
echo "request timeout: ${request_timeout}"
echo "scenario count: ${#scenarios[@]}"

for entry in "${scenarios[@]}"; do
  old_ifs="${IFS}"
  IFS='|' read -r scenario strategy items batch_size parallel description <<< "${entry}"
  IFS="${old_ifs}"

  scenario_dir="${output_dir}/${scenario}"
  report_json="${scenario_dir}/report.json"
  responses_json="${scenario_dir}/responses.json"
  stdout_log="${scenario_dir}/stdout.log"
  scenario_env="${scenario_dir}/scenario.env"

  mkdir -p "${scenario_dir}"
  write_scenario_env "${scenario_env}" "${scenario}" "${strategy}" "${items}" "${batch_size}" "${parallel}" "${description}"

  echo
  echo "=== ${scenario} ==="
  echo "${description}"

  if [[ "${dry_run}" == "true" ]]; then
    echo "DRY_RUN=true, skipping execution" | tee "${stdout_log}"
    status="DRY_RUN"
    exit_code="0"
    elapsed=""
    requests_per_second=""
    terms_per_second=""
    p95_request_latency=""
  else
    set +e
    SCENARIO="${scenario}" \
      DESCRIPTION="${description}" \
      STRATEGY="${strategy}" \
      ITEMS="${items}" \
      BATCH_SIZE="${batch_size}" \
      PARALLEL="${parallel}" \
      TIMEOUT="${timeout}" \
      REQUEST_TIMEOUT="${request_timeout}" \
      REPORT_JSON="${report_json}" \
      RESPONSES_JSON="${responses_json}" \
      ./example-start.sh 2>&1 | tee "${stdout_log}"
    exit_code="${PIPESTATUS[0]}"
    set -e

    status="$(extract_line_value "Status" "${stdout_log}")"
    elapsed="$(extract_line_value "Elapsed" "${stdout_log}")"
    requests_per_second="$(extract_line_value "Requests/sec" "${stdout_log}")"
    terms_per_second="$(extract_line_value "Terms/sec" "${stdout_log}")"
    p95_request_latency="$(extract_line_value "P95 request latency" "${stdout_log}")"
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${scenario}" \
    "${strategy}" \
    "${items}" \
    "${batch_size}" \
    "${parallel}" \
    "${status}" \
    "${exit_code}" \
    "${elapsed}" \
    "${requests_per_second}" \
    "${terms_per_second}" \
    "${p95_request_latency}" \
    "${report_json}" \
    "${responses_json}" \
    "${stdout_log}" >> "${summary_csv}"

  if [[ "${pause_seconds}" != "0" ]]; then
    sleep "${pause_seconds}"
  fi
done

echo
echo "Benchmark complete."
echo "Output folder: ${output_dir}"
echo "Summary CSV: ${summary_csv}"
echo "Analysis prompt: ${analysis_prompt}"
