#!/usr/bin/env bash
#
# load-domain-ranks.sh — load a CommonCrawl DOMAIN-level webgraph ranks file into ClickHouse
# (corpscout.commoncrawl_domain_graph_signals — migration 000073).
#
# The *-domain-ranks.txt release is tab-separated with a '#'-prefixed header and columns:
#   harmonicc_pos  harmonicc_val  pr_pos  pr_val  host_rev  n_hosts
# host_rev is the registered domain REVERSED (com.example); this un-reverses it to root_domain (example.com),
# correctly for multi-part TLDs too (uk.co.bbc -> bbc.co.uk).
# Source: https://commoncrawl.org/web-graphs  (download the DOMAIN-level ranks .txt.gz, then gunzip).
#
# Streams the file with clickhouse-local (fast C++ TSV reader) and bulk-inserts to the remote ClickHouse via
# remote() over the native protocol. Idempotent — ReplacingMergeTree dedupes on (root_domain, crawl_id).
#
# Usage:
#   ./load-domain-ranks.sh <ranks-file> <crawl-id>
#   ./load-domain-ranks.sh /opt/cc-main-2026-apr-may-jun-domain-ranks.txt CC-MAIN-2026-apr-may-jun
#   DRY=1 ./load-domain-ranks.sh <ranks-file> <crawl-id>     # preview 5 transformed rows, no insert
#
# Prereqs:
#   - the static clickhouse binary:  curl -s https://clickhouse.com/ | sh   (or set CLICKHOUSE_BIN=/path)
#   - commoncrawl/.env with CLICKHOUSE_HOST / _NATIVE_PORT / _USER / _PASSWORD
#   - the table exists:  (cd .. && make clickhouse-migrate-up)
set -euo pipefail

FILE="${1:?usage: load-domain-ranks.sh <ranks-file> <crawl-id>  (DRY=1 to preview)}"
CRAWL_ID="${2:?usage: load-domain-ranks.sh <ranks-file> <crawl-id>  (DRY=1 to preview)}"
[ -f "$FILE" ] || { echo "no such file: $FILE" >&2; exit 1; }

# clickhouse-local binary: $CLICKHOUSE_BIN, else `clickhouse` on PATH, else /opt/clickhouse.
CH_BIN="${CLICKHOUSE_BIN:-$(command -v clickhouse || echo /opt/clickhouse)}"
[ -x "$CH_BIN" ] || { echo "clickhouse binary not found at '$CH_BIN' — set CLICKHOUSE_BIN, or run: curl -s https://clickhouse.com/ | sh" >&2; exit 1; }

# ClickHouse connection from commoncrawl/.env (next to this script).
set -a; . "$(dirname "$0")/.env"; set +a
: "${CLICKHOUSE_HOST:?}" "${CLICKHOUSE_NATIVE_PORT:?}" "${CLICKHOUSE_USER:?}" "${CLICKHOUSE_PASSWORD:?}"

SCHEMA='harmonicc_pos UInt32, harmonicc_val Float64, pr_pos UInt32, pr_val Float64, host_rev String, n_hosts UInt32'
UNREV="arrayStringConcat(arrayReverse(splitByChar('.', host_rev)), '.')"   # com.example -> example.com

if [ -n "${DRY:-}" ]; then
  echo "DRY: previewing 5 transformed rows from $FILE (no insert)"
  "$CH_BIN" local --query "
    SELECT $UNREV AS root_domain, harmonicc_val, harmonicc_pos, pr_val, pr_pos, n_hosts
    FROM file('$FILE','TabSeparated','$SCHEMA')
    LIMIT 5 SETTINGS input_format_tsv_skip_first_lines = 1"
  exit 0
fi

echo "loading $FILE -> corpscout.commoncrawl_domain_graph_signals (crawl_id=$CRAWL_ID) via $CLICKHOUSE_HOST:$CLICKHOUSE_NATIVE_PORT ..."
"$CH_BIN" local --query "
INSERT INTO FUNCTION remote('$CLICKHOUSE_HOST:$CLICKHOUSE_NATIVE_PORT','corpscout.commoncrawl_domain_graph_signals','$CLICKHOUSE_USER','$CLICKHOUSE_PASSWORD')
  (crawl_id, root_domain, cc_harmonic_centrality, cc_harmonic_rank, cc_pagerank, cc_pagerank_rank, n_hosts, source_run_id, resolved_at)
SELECT '$CRAWL_ID', $UNREV, harmonicc_val, harmonicc_pos, pr_val, pr_pos, n_hosts, '$(basename "$FILE")', now64(3)
FROM file('$FILE','TabSeparated','$SCHEMA')
SETTINGS input_format_tsv_skip_first_lines = 1"
echo "done."
