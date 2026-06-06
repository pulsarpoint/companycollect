#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${script_dir}"

run_id="$(date -u +%Y%m%d%H%M%S)"
output_dir="${OUTPUT_DIR:-${script_dir}/output/benchmark-${run_id}}"

terms_per_batch_values="${TERMS_PER_BATCH_VALUES:-1,2,4,8,16}"
max_input_message_values="${MAX_INPUT_MESSAGES_VALUES:-0,1,2,4}"
repeats="${REPEATS:-1}"
batches="${BATCHES:-30}"
timeout="${TIMEOUT:-10m}"
pause_seconds="${PAUSE_SECONDS:-5}"
dry_run="${DRY_RUN:-false}"

mkdir -p "${output_dir}"

summary_csv="${output_dir}/summary.csv"
analysis_prompt="${output_dir}/analysis-prompt.md"

printf 'scenario,repeat,batches,terms_per_batch,max_input_messages,status,exit_code,elapsed,batches_per_second,terms_per_second,p95_batch_latency,last_input_depth,report_json,stdout_log\n' > "${summary_csv}"

cat > "${analysis_prompt}" <<EOF
# Translation Benchmark Analysis Request

Analyze the translation benchmark outputs in this folder.

Focus on:

- Compare throughput by terms/sec and batches/sec.
- Compare p95 batch latency and max batch latency.
- Identify failed or partial runs before ranking.
- Recommend the best strategy for full production translation.
- Recommend the next benchmark matrix if results are inconclusive.

The main index is:

- summary.csv

Each scenario folder contains:

- stdout.log
- report.json
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
  local terms_per_batch="$2"
  local max_input_messages="$3"
  local repeat="$4"

  {
    printf 'RUN_ID=%s\n' "${run_id}"
    printf 'REPEAT=%s\n' "${repeat}"
    printf 'BATCHES=%s\n' "${batches}"
    printf 'TERMS_PER_BATCH=%s\n' "${terms_per_batch}"
    printf 'MAX_INPUT_MESSAGES=%s\n' "${max_input_messages}"
    printf 'TIMEOUT=%s\n' "${timeout}"
    printf 'PAUSE_SECONDS=%s\n' "${pause_seconds}"
    printf 'NATS_URL=%s\n' "${NATS_URL:-nats://companycollect:4222}"
    printf 'INPUT_STREAM=%s\n' "${INPUT_STREAM:-SOURCE_TRANSLATION}"
    printf 'OUTPUT_STREAM=%s\n' "${OUTPUT_STREAM:-SOURCE_TRANSLATION}"
    printf 'INPUT_QUEUE=%s\n' "${INPUT_QUEUE:-source.translation.jobs}"
    printf 'OUTPUT_QUEUE=%s\n' "${OUTPUT_QUEUE:-source.translation.results}"
    printf 'INPUT_CONSUMER=%s\n' "${INPUT_CONSUMER:-translation-service}"
    printf 'PROVIDER=%s\n' "${PROVIDER:-default}"
    printf 'MODEL=%s\n' "${MODEL:-qwen3:6b}"
  } > "${path}"
}

terms_values=()
max_input_values=()
old_ifs="${IFS}"
IFS=',' read -r -a terms_values <<< "${terms_per_batch_values}"
IFS=',' read -r -a max_input_values <<< "${max_input_message_values}"
IFS="${old_ifs}"

echo "Translation benchmark output: ${output_dir}"
echo "terms_per_batch values: ${terms_per_batch_values}"
echo "max_input_messages values: ${max_input_message_values}"
echo "repeats: ${repeats}"
echo "batches per scenario: ${batches}"
echo "timeout per scenario: ${timeout}"

for repeat in $(seq 1 "${repeats}"); do
  for terms_per_batch in "${terms_values[@]}"; do
    for max_input_messages in "${max_input_values[@]}"; do
      scenario="tpb-${terms_per_batch}_buffer-${max_input_messages}_repeat-${repeat}"
      scenario_dir="${output_dir}/${scenario}"
      report_json="${scenario_dir}/report.json"
      stdout_log="${scenario_dir}/stdout.log"
      scenario_env="${scenario_dir}/scenario.env"
      result_durable="translation-e2e-benchmark-${run_id}-${scenario}"

      mkdir -p "${scenario_dir}"
      write_scenario_env "${scenario_env}" "${terms_per_batch}" "${max_input_messages}" "${repeat}"

      echo
      echo "=== ${scenario} ==="

      if [[ "${dry_run}" == "true" ]]; then
        echo "DRY_RUN=true, skipping execution" | tee "${stdout_log}"
        status="DRY_RUN"
        exit_code="0"
        elapsed=""
        batches_per_second=""
        terms_per_second=""
        p95_batch_latency=""
        last_input_depth=""
      else
        set +e
        BATCHES="${batches}" \
          TERMS_PER_BATCH="${terms_per_batch}" \
          MAX_INPUT_MESSAGES="${max_input_messages}" \
          TIMEOUT="${timeout}" \
          REPORT_JSON="${report_json}" \
          RESULT_DURABLE="${result_durable}" \
          ./example-start.sh 2>&1 | tee "${stdout_log}"
        exit_code="${PIPESTATUS[0]}"
        set -e

        status="$(extract_line_value "Status" "${stdout_log}")"
        elapsed="$(extract_line_value "Elapsed" "${stdout_log}")"
        batches_per_second="$(extract_line_value "Batches/sec" "${stdout_log}")"
        terms_per_second="$(extract_line_value "Terms/sec" "${stdout_log}")"
        p95_batch_latency="$(extract_line_value "P95 batch latency" "${stdout_log}")"
        last_input_depth="$(extract_line_value "Last input depth" "${stdout_log}")"
      fi

      printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "${scenario}" \
        "${repeat}" \
        "${batches}" \
        "${terms_per_batch}" \
        "${max_input_messages}" \
        "${status}" \
        "${exit_code}" \
        "${elapsed}" \
        "${batches_per_second}" \
        "${terms_per_second}" \
        "${p95_batch_latency}" \
        "${last_input_depth}" \
        "${report_json}" \
        "${stdout_log}" >> "${summary_csv}"

      if [[ "${pause_seconds}" != "0" ]]; then
        sleep "${pause_seconds}"
      fi
    done
  done
done

echo
echo "Benchmark complete."
echo "Output folder: ${output_dir}"
echo "Summary CSV: ${summary_csv}"
echo "Analysis prompt: ${analysis_prompt}"
