#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat <<'EOF'
Usage:
  scripts/trigger-brreg-workflow.sh [run|load-queue]

Environment overrides:
  TEMPORAL_ADDRESS                 default: localhost:7233
  TEMPORAL_NAMESPACE               default: default
  TEMPORAL_CLI                     default: temporal
  TRANSLATOR_BATCH_SIZE            default: 50
  TRANSLATOR_TIMEOUT_SECONDS       default: 120
  BRREG_TRANSLATOR_WORKFLOW_ID     default: translator/norway_brreg
  BRREG_TRANSLATOR_TASK_QUEUE      default: translator-norway-brreg
  BRREG_TRANSLATOR_WORKFLOW_TYPE   default: NorwayBRREGWorkflow
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
	usage
	exit 0
fi

action="${1:-run}"
case "$action" in
run | load-queue)
	;;
*)
	echo "unsupported action: $action" >&2
	usage >&2
	exit 2
	;;
esac

temporal_cli="${TEMPORAL_CLI:-temporal}"
if ! command -v "$temporal_cli" >/dev/null 2>&1; then
	echo "Temporal CLI not found: $temporal_cli" >&2
	echo "Install temporal CLI or set TEMPORAL_CLI to its path." >&2
	exit 127
fi

temporal_address="${TEMPORAL_ADDRESS:-localhost:7233}"
temporal_namespace="${TEMPORAL_NAMESPACE:-default}"
workflow_id="${BRREG_TRANSLATOR_WORKFLOW_ID:-translator/norway_brreg}"
task_queue="${BRREG_TRANSLATOR_TASK_QUEUE:-translator-norway-brreg}"
workflow_type="${BRREG_TRANSLATOR_WORKFLOW_TYPE:-NorwayBRREGWorkflow}"
batch_size="${TRANSLATOR_BATCH_SIZE:-50}"
timeout_seconds="${TRANSLATOR_TIMEOUT_SECONDS:-120}"
signal_name="source-action"

if [[ ! "$batch_size" =~ ^[1-9][0-9]*$ ]]; then
	echo "TRANSLATOR_BATCH_SIZE must be a positive integer, got: $batch_size" >&2
	exit 2
fi

if [[ ! "$timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
	echo "TRANSLATOR_TIMEOUT_SECONDS must be a positive integer, got: $timeout_seconds" >&2
	exit 2
fi

workflow_input="{\"BatchSize\":$batch_size,\"TimeoutSeconds\":$timeout_seconds}"
signal_input="{\"Action\":\"$action\"}"

exec "$temporal_cli" workflow signal-with-start \
	--address "$temporal_address" \
	--namespace "$temporal_namespace" \
	--workflow-id "$workflow_id" \
	--type "$workflow_type" \
	--task-queue "$task_queue" \
	--input "$workflow_input" \
	--signal-name "$signal_name" \
	--signal-input "$signal_input"
