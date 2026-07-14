#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat <<'EOF'
Usage:
  scripts/trigger-translator-workflow.sh

Signals (or starts) the single shared translation queue workflow.

Environment overrides:
  TEMPORAL_ADDRESS                 default: localhost:7233
  TEMPORAL_NAMESPACE               default: default
  TEMPORAL_CLI                     default: temporal
  TRANSLATOR_BATCH_SIZE            default: 50
  TRANSLATOR_TIMEOUT_SECONDS       default: 120
  TRANSLATOR_BATCHES_PER_RUN       default: 500
  TRANSLATOR_FLUSH_EVERY_BATCHES   default: 10
  TRANSLATOR_WORKFLOW_TYPE         default: TranslationWorkflow
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
	usage
	exit 0
fi

if [[ $# -gt 0 ]]; then
	echo "unexpected arguments: $*" >&2
	usage >&2
	exit 2
fi

temporal_cli="${TEMPORAL_CLI:-temporal}"
if ! command -v "$temporal_cli" >/dev/null 2>&1; then
	echo "Temporal CLI not found: $temporal_cli" >&2
	echo "Install temporal CLI or set TEMPORAL_CLI to its path." >&2
	exit 127
fi

temporal_address="${TEMPORAL_ADDRESS:-localhost:7233}"
temporal_namespace="${TEMPORAL_NAMESPACE:-default}"
workflow_id="translator/process"
task_queue="translator-process"
workflow_type="${TRANSLATOR_WORKFLOW_TYPE:-TranslationWorkflow}"
batch_size="${TRANSLATOR_BATCH_SIZE:-50}"
timeout_seconds="${TRANSLATOR_TIMEOUT_SECONDS:-120}"
batches_per_run="${TRANSLATOR_BATCHES_PER_RUN:-500}"
flush_every_batches="${TRANSLATOR_FLUSH_EVERY_BATCHES:-10}"
signal_name="new-items"

if [[ ! "$batch_size" =~ ^[1-9][0-9]*$ ]]; then
	echo "TRANSLATOR_BATCH_SIZE must be a positive integer, got: $batch_size" >&2
	exit 2
fi

if [[ ! "$timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
	echo "TRANSLATOR_TIMEOUT_SECONDS must be a positive integer, got: $timeout_seconds" >&2
	exit 2
fi

if [[ ! "$batches_per_run" =~ ^[1-9][0-9]*$ ]]; then
	echo "TRANSLATOR_BATCHES_PER_RUN must be a positive integer, got: $batches_per_run" >&2
	exit 2
fi

if [[ ! "$flush_every_batches" =~ ^[1-9][0-9]*$ ]]; then
	echo "TRANSLATOR_FLUSH_EVERY_BATCHES must be a positive integer, got: $flush_every_batches" >&2
	exit 2
fi

workflow_input="{\"BatchSize\":$batch_size,\"TimeoutSeconds\":$timeout_seconds,\"BatchesPerRun\":$batches_per_run,\"FlushEveryBatches\":$flush_every_batches}"

exec "$temporal_cli" workflow signal-with-start \
	--address "$temporal_address" \
	--namespace "$temporal_namespace" \
	--workflow-id "$workflow_id" \
	--type "$workflow_type" \
	--task-queue "$task_queue" \
	--input "$workflow_input" \
	--signal-name "$signal_name" \
	--signal-input 'null'
