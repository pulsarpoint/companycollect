#!/usr/bin/env bash

set -euo pipefail

operations_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
start_bucket="${DNS_SEEN_BACKFILL_START_BUCKET:-0}"
end_bucket="${DNS_SEEN_BACKFILL_END_BUCKET:-15}"
phase="${DNS_SEEN_BACKFILL_PHASE:-both}"
client_image="${CLICKHOUSE_CLIENT_IMAGE:-clickhouse/clickhouse-server:26.5}"

url_decode() {
    local encoded="${1//+/ }"
    printf '%b' "${encoded//%/\\x}"
}

native_url="${CLICKHOUSE_NATIVE_URL:-}"
if [[ -n "${native_url}" ]]; then
    connection="${native_url#*://}"
    authority="${connection%%\?*}"
    query="${connection#*\?}"
    url_host="${authority%%:*}"
    url_port="${authority##*:}"
    url_user=""
    url_password=""
    url_database=""
    url_secure=""

    IFS='&' read -r -a query_parameters <<< "${query}"
    for parameter in "${query_parameters[@]}"; do
        key="${parameter%%=*}"
        value="${parameter#*=}"
        case "${key}" in
            username) url_user="$(url_decode "${value}")" ;;
            password) url_password="$(url_decode "${value}")" ;;
            database) url_database="$(url_decode "${value}")" ;;
            secure) url_secure="$(url_decode "${value}")" ;;
        esac
    done
fi

clickhouse_host="${CLICKHOUSE_HOST:-${url_host:-}}"
clickhouse_port="${CLICKHOUSE_NATIVE_PORT:-${url_port:-}}"
clickhouse_user="${CLICKHOUSE_USER:-${url_user:-}}"
clickhouse_password="${CLICKHOUSE_PASSWORD:-${url_password:-}}"
clickhouse_database="${CLICKHOUSE_DATABASE:-${url_database:-}}"
clickhouse_secure="${CLICKHOUSE_SECURE:-${url_secure:-false}}"

: "${clickhouse_host:?CLICKHOUSE_NATIVE_URL or CLICKHOUSE_HOST is required}"
: "${clickhouse_port:?CLICKHOUSE_NATIVE_URL or CLICKHOUSE_NATIVE_PORT is required}"
: "${clickhouse_user:?CLICKHOUSE_NATIVE_URL or CLICKHOUSE_USER is required}"
: "${clickhouse_password:?CLICKHOUSE_NATIVE_URL or CLICKHOUSE_PASSWORD is required}"
: "${clickhouse_database:?CLICKHOUSE_NATIVE_URL or CLICKHOUSE_DATABASE is required}"

if [[ ! "${start_bucket}" =~ ^([0-9]|1[0-5])$ ]]; then
    echo "START_BUCKET must be an integer from 0 through 15" >&2
    exit 2
fi
if [[ ! "${end_bucket}" =~ ^([0-9]|1[0-5])$ ]]; then
    echo "END_BUCKET must be an integer from 0 through 15" >&2
    exit 2
fi
if (( start_bucket > end_bucket )); then
    echo "START_BUCKET must not be greater than END_BUCKET" >&2
    exit 2
fi
if [[ "${phase}" != "both" && "${phase}" != "backfill" && "${phase}" != "validate" ]]; then
    echo "PHASE must be one of: both, backfill, validate" >&2
    exit 2
fi

docker_args=(
    run
    --rm
    --interactive
    --volume "${operations_dir}:/operations:ro"
)
if [[ -n "${COMPANYCOLLECT_HOST_IP:-}" ]]; then
    docker_args+=(--add-host "${clickhouse_host}:${COMPANYCOLLECT_HOST_IP}")
fi

client_args=(
    clickhouse-client
    --host "${clickhouse_host}"
    --port "${clickhouse_port}"
    --user "${clickhouse_user}"
    --password "${clickhouse_password}"
    --database "${clickhouse_database}"
    --progress
)
if [[ "${clickhouse_secure}" == "true" || "${clickhouse_secure}" == "1" ]]; then
    client_args+=(--secure)
fi

docker "${docker_args[@]}" "${client_image}" "${client_args[@]}" --query "
    SELECT throwIf(
        count() != 4,
        'DNS seen-window tables are missing; apply ClickHouse migration 161 first'
    )
    FROM system.tables
    WHERE database = 'corpscout'
      AND name IN
      (
          'commoncrawl_domain_dns_record_ingest',
          'commoncrawl_domain_dns_records',
          'commoncrawl_domain_dns_records_v2',
          'commoncrawl_domain_dns_record_sightings'
      )
    FORMAT Null
"

for (( bucket = start_bucket; bucket <= end_bucket; bucket++ )); do
    echo "DNS seen-window backfill bucket ${bucket}/15 (${phase})"

    if [[ "${phase}" == "both" || "${phase}" == "backfill" ]]; then
        docker "${docker_args[@]}" "${client_image}" "${client_args[@]}" \
            --param_bucket="${bucket}" \
            --queries-file=/operations/dns_records_seen_window_backfill_bucket.sql
    fi

    if [[ "${phase}" == "both" || "${phase}" == "validate" ]]; then
        docker "${docker_args[@]}" "${client_image}" "${client_args[@]}" \
            --param_bucket="${bucket}" \
            --queries-file=/operations/dns_records_seen_window_validate_bucket.sql
    fi

    echo "DNS seen-window backfill bucket ${bucket}/15 complete"
done
