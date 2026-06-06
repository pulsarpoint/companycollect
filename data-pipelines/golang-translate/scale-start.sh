#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${script_dir}"

run_id="$(date -u +%Y%m%d%H%M%S)"
output_dir="${OUTPUT_DIR:-${script_dir}/output/scale-${run_id}}"
items="${ITEMS:-128}"
start_batch_size="${START_BATCH_SIZE:-16}"
max_batch_size="${MAX_BATCH_SIZE:-${items}}"
min_step="${MIN_STEP:-4}"
max_rounds="${MAX_ROUNDS:-12}"
degradation_percent="${DEGRADATION_PERCENT:-5}"
timeout="${TIMEOUT:-20m}"
request_timeout="${REQUEST_TIMEOUT:-300s}"
pause_seconds="${PAUSE_SECONDS:-10}"
dry_run="${DRY_RUN:-false}"

mkdir -p "${output_dir}"

summary_csv="${output_dir}/summary.csv"
results_tsv="${output_dir}/results.tsv"
analysis_prompt="${output_dir}/analysis-prompt.md"

printf 'scenario,items,batch_size,status,exit_code,elapsed,requests_per_second,terms_per_second,p95_request_latency,report_json,responses_json,stdout_log\n' > "${summary_csv}"
printf 'batch_size\tstatus\tterms_per_second\tp95_request_latency\texit_code\n' > "${results_tsv}"

cat > "${analysis_prompt}" <<EOF
# Direct Go LLM Adaptive Batch Search Analysis Request

Analyze this adaptive batch-size search folder.

This benchmark uses one direct Go caller, one local OpenAI-compatible LLM endpoint, and sequential requests.
It keeps selected term count fixed and searches for the best terms-per-request value.

Search settings:

- ITEMS=${items}
- START_BATCH_SIZE=${start_batch_size}
- MAX_BATCH_SIZE=${max_batch_size}
- MIN_STEP=${min_step}
- MAX_ROUNDS=${max_rounds}
- DEGRADATION_PERCENT=${degradation_percent}

Focus on:

- Identify the highest reliable terms/sec result.
- Check whether larger prompts started failing or merely became slower.
- Compare throughput against p95 request latency.
- Recommend the batch size to test next in the full Python/JetStream translation pipeline.
- Recommend whether another search should run with a larger ITEMS value.

Main indexes:

- summary.csv
- results.tsv

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

percent_slower_than_best() {
  local value="$1"
  local best="$2"
  awk -v value="${value}" -v best="${best}" 'BEGIN {
    if (best <= 0) {
      print 0
    } else {
      print ((best - value) / best) * 100
    }
  }'
}

is_number() {
  awk -v value="$1" 'BEGIN { exit !(value ~ /^[0-9]+([.][0-9]+)?$/) }'
}

already_tested() {
  local batch_size="$1"
  awk -F'\t' -v batch_size="${batch_size}" 'NR > 1 && $1 == batch_size { found = 1 } END { exit !found }' "${results_tsv}"
}

best_batch_size() {
  awk -F'\t' 'NR > 1 && $2 == "PASS" && $3 + 0 > best { best = $3 + 0; batch = $1 } END { print batch }' "${results_tsv}"
}

best_terms_per_second() {
  awk -F'\t' 'NR > 1 && $2 == "PASS" && $3 + 0 > best { best = $3 + 0 } END { if (best == "") { print 0 } else { print best } }' "${results_tsv}"
}

nearest_lower_batch() {
  local batch_size="$1"
  awk -F'\t' -v batch_size="${batch_size}" 'NR > 1 && $1 + 0 < batch_size && $1 + 0 > lower { lower = $1 + 0 } END { print lower }' "${results_tsv}"
}

nearest_higher_batch() {
  local batch_size="$1"
  awk -F'\t' -v batch_size="${batch_size}" 'NR > 1 && $1 + 0 > batch_size && (higher == "" || $1 + 0 < higher) { higher = $1 + 0 } END { print higher }' "${results_tsv}"
}

midpoint() {
  local left="$1"
  local right="$2"
  local step="$3"
  awk -v left="${left}" -v right="${right}" -v step="${step}" 'BEGIN {
    mid = int((left + right) / 2)
    if (step > 1) {
      mid = int(mid / step) * step
      if (mid <= left) {
        mid = left + step
      }
      if (mid >= right) {
        mid = right - step
      }
    }
    print mid
  }'
}

write_scenario_env() {
  local path="$1"
  local scenario="$2"
  local batch_size="$3"
  local description="$4"

  {
    printf 'RUN_ID=%s\n' "${run_id}"
    printf 'SCENARIO=%s\n' "${scenario}"
    printf 'DESCRIPTION=%s\n' "${description}"
    printf 'STRATEGY=sequential\n'
    printf 'ITEMS=%s\n' "${items}"
    printf 'BATCH_SIZE=%s\n' "${batch_size}"
    printf 'PARALLEL=1\n'
    printf 'TIMEOUT=%s\n' "${timeout}"
    printf 'REQUEST_TIMEOUT=%s\n' "${request_timeout}"
    printf 'BASE_URL=%s\n' "${BASE_URL:-http://100.77.62.33:8888}"
    printf 'MODEL=%s\n' "${MODEL:-qwen3:6b}"
    printf 'INPUT=%s\n' "${INPUT:-input.json}"
  } > "${path}"
}

run_batch_size() {
  local batch_size="$1"
  if already_tested "${batch_size}"; then
    echo "Skipping already tested batch size ${batch_size}"
    return 0
  fi

  local scenario="adaptive-items-${items}-batch-${batch_size}"
  local description="Adaptive sequential direct LLM run with ${items} selected terms and ${batch_size} terms per request."
  local scenario_dir="${output_dir}/${scenario}"
  local report_json="${scenario_dir}/report.json"
  local responses_json="${scenario_dir}/responses.json"
  local stdout_log="${scenario_dir}/stdout.log"
  local scenario_env="${scenario_dir}/scenario.env"

  mkdir -p "${scenario_dir}"
  write_scenario_env "${scenario_env}" "${scenario}" "${batch_size}" "${description}"

  echo
  echo "=== ${scenario} ==="
  echo "${description}"

  local status
  local exit_code
  local elapsed
  local requests_per_second
  local terms_per_second
  local p95_request_latency

  if [[ "${dry_run}" == "true" ]]; then
    echo "DRY_RUN=true, skipping execution" | tee "${stdout_log}"
    status="DRY_RUN"
    exit_code="0"
    elapsed=""
    requests_per_second=""
    terms_per_second="0"
    p95_request_latency=""
  else
    set +e
    SCENARIO="${scenario}" \
      DESCRIPTION="${description}" \
      STRATEGY="sequential" \
      ITEMS="${items}" \
      BATCH_SIZE="${batch_size}" \
      PARALLEL="1" \
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

  if [[ -z "${terms_per_second}" ]]; then
    terms_per_second="0"
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${scenario}" \
    "${items}" \
    "${batch_size}" \
    "${status}" \
    "${exit_code}" \
    "${elapsed}" \
    "${requests_per_second}" \
    "${terms_per_second}" \
    "${p95_request_latency}" \
    "${report_json}" \
    "${responses_json}" \
    "${stdout_log}" >> "${summary_csv}"

  printf '%s\t%s\t%s\t%s\t%s\n' \
    "${batch_size}" \
    "${status}" \
    "${terms_per_second}" \
    "${p95_request_latency}" \
    "${exit_code}" >> "${results_tsv}"

  if [[ "${pause_seconds}" != "0" ]]; then
    sleep "${pause_seconds}"
  fi
}

echo "Direct Go LLM adaptive batch search output: ${output_dir}"
echo "items: ${items}"
echo "start batch size: ${start_batch_size}"
echo "max batch size: ${max_batch_size}"
echo "min step: ${min_step}"
echo "max rounds: ${max_rounds}"
echo "degradation percent: ${degradation_percent}"
echo "timeout per scenario: ${timeout}"
echo "request timeout: ${request_timeout}"

current="${start_batch_size}"
last_pass_batch=""
last_pass_terms="0"

while (( current <= max_batch_size )); do
  run_batch_size "${current}"
  status="$(awk -F'\t' -v batch="${current}" '$1 == batch { status = $2 } END { print status }' "${results_tsv}")"
  terms_per_second="$(awk -F'\t' -v batch="${current}" '$1 == batch { terms = $3 } END { print terms }' "${results_tsv}")"

  if [[ "${dry_run}" == "true" ]]; then
    current=$(( current * 2 ))
    continue
  fi

  if [[ "${status}" != "PASS" ]]; then
    break
  fi

  best_terms="$(best_terms_per_second)"
  slower="$(percent_slower_than_best "${terms_per_second}" "${best_terms}")"
  if is_number "${slower}" && awk -v slower="${slower}" -v limit="${degradation_percent}" 'BEGIN { exit !(slower > limit) }'; then
    break
  fi

  last_pass_batch="${current}"
  last_pass_terms="${terms_per_second}"
  current=$(( current * 2 ))
done

round=1
while (( round <= max_rounds )); do
  best_batch="$(best_batch_size)"
  if [[ -z "${best_batch}" ]]; then
    break
  fi

  lower="$(nearest_lower_batch "${best_batch}")"
  higher="$(nearest_higher_batch "${best_batch}")"
  candidate=""

  if [[ -n "${higher}" && $(( higher - best_batch )) -gt "${min_step}" ]]; then
    candidate="$(midpoint "${best_batch}" "${higher}" "${min_step}")"
  elif [[ -n "${lower}" && $(( best_batch - lower )) -gt "${min_step}" ]]; then
    candidate="$(midpoint "${lower}" "${best_batch}" "${min_step}")"
  elif [[ -z "${higher}" && "${best_batch}" -lt "${max_batch_size}" ]]; then
    next=$(( best_batch + min_step ))
    if (( next <= max_batch_size )); then
      candidate="${next}"
    fi
  fi

  if [[ -z "${candidate}" || "${candidate}" -le 0 || "${candidate}" -gt "${max_batch_size}" ]]; then
    break
  fi
  if already_tested "${candidate}"; then
    break
  fi

  run_batch_size "${candidate}"
  round=$(( round + 1 ))
done

best_batch="$(best_batch_size)"
best_terms="$(best_terms_per_second)"

echo
echo "Adaptive search complete."
echo "Output folder: ${output_dir}"
echo "Summary CSV: ${summary_csv}"
echo "Results TSV: ${results_tsv}"
echo "Analysis prompt: ${analysis_prompt}"
echo "Best batch size: ${best_batch:-none}"
echo "Best terms/sec: ${best_terms}"
