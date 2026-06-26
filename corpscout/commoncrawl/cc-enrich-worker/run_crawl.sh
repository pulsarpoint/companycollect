#!/usr/bin/env bash
# Drive CommonCrawl enrichment over many index shards — resumable.
#
#   cp ../.env.example ../.env && edit it          # shared config (auto-loaded below)
#   ./run_crawl.sh tech     0-299                  # CPU-bound tech pass  (most cores)
#   ./run_crawl.sh industry 0-299                  # GPU-bound industry pass (the 5090)
#
# Run the two modes as SEPARATE processes (e.g. two tmux panes) so tech pegs the cores
# while industry feeds the 5090. Each is independent and restartable: a shard whose
# <out>.loaded marker exists is skipped; ReplacingMergeTree dedupes any re-load.
#
# Tunables via env: CRAWL, WHERE (worklist SQL filter; empty = ALL domains), DATA,
# BUILDER_DIR, MAX_PAGES (tech pages/domain, default 25; 0=all), TECH_CONC, IND_CONC, EMBED_CONC.
#
# industry and tech build SEPARATE worklists: industry = 1 representative page/domain (homepage),
# tech = up to MAX_PAGES/domain (homepage + legal/contact pages + shallow) so Wappalyzer + the
# LEI/VAT/profile extractors see the pages that actually carry that data.
set -uo pipefail

# Load the shared config (COMMONCRAWL_EMBED_*, CLICKHOUSE_*, AWS_*) — the single source of truth
# the dagster reference-build also reads, so worker and reference embeddings use the same model.
[ -f ../.env ] && { set -a; . ../.env; set +a; }

MODE="${1:?usage: run_crawl.sh <industry|tech> <lo-hi>}"
RANGE="${2:?usage: run_crawl.sh <industry|tech> <lo-hi>}"
CRAWL="${CRAWL:-CC-MAIN-2026-25}"
WHERE="${WHERE:-}" # empty = all domains (global dataset); e.g. "content_languages like '%eng%'"
DATA="${DATA:-data/crawl}"
BUILDER_DIR="${BUILDER_DIR:-../index-builder}" # standalone Python worklist builder
WORKER="$PWD/cc-enrich-worker"

# OUTPUTS: the <table>:<parquet-suffix> pairs a mode produces (loaded in order).
case "$MODE" in
industry) OUTPUTS=("commoncrawl_domains:domains")
          PASS_ARGS=(--concurrency "${IND_CONC:-16}" --embed-concurrency "${EMBED_CONC:-128}") ;;
tech)     OUTPUTS=("commoncrawl_technologies:tech" "commoncrawl_company_identifiers:identifiers" "commoncrawl_company_profile:profiles")
          PASS_ARGS=(--tech-engine fast --concurrency "${TECH_CONC:-128}") ;;
*) echo "MODE must be industry|tech"; exit 1 ;;
esac

lo="${RANGE%-*}"; hi="${RANGE#*-}"
mkdir -p "$DATA"
DATA_ABS="$(cd "$DATA" && pwd)"
BUILDER_ABS="$(cd "$BUILDER_DIR" && pwd)"

ch() {
	clickhouse-client --host "${CLICKHOUSE_HOST:?set CLICKHOUSE_HOST}" \
		--port "${CLICKHOUSE_NATIVE_PORT:-9002}" --user "${CLICKHOUSE_USER:-default}" \
		--password "${CLICKHOUSE_PASSWORD:-}" --database "${CLICKHOUSE_DATABASE:-corpscout}" "$@"
}

for ((p = lo; p <= hi; p++)); do
	shard="$DATA_ABS/shard_${MODE}_${p}.parquet" # per-mode: industry=1 page/domain, tech=many
	out="$DATA/${MODE}_${p}"
	primary="${out}-${OUTPUTS[0]##*:}.parquet" # produced whenever the pass runs
	marker="${out}.loaded"
	[ -f "$marker" ] && { echo "[$MODE $p] already loaded — skip"; continue; }

	# 1. worklist (atomic rename; safe if both modes race on the same shard)
	if [ ! -f "$shard" ]; then
		echo "[$MODE $p] generating worklist…"
		tmp="${shard}.tmp.$$"
		if (cd "$BUILDER_ABS" && uv run python -m index_builder --mode "$MODE" \
			--max-pages "${MAX_PAGES:-25}" --crawl "$CRAWL" --part "$p" --where "$WHERE" --out "$tmp"); then
			mv -f "$tmp" "$shard"
		else
			echo "[$MODE $p] worklist FAILED — skip"; rm -f "$tmp"; continue
		fi
	fi

	# 2. run the pass once (produces every output for the mode)
	if [ ! -f "$primary" ]; then
		echo "[$MODE $p] $MODE pass…"
		if ! "$WORKER" "$MODE" "${PASS_ARGS[@]}" --worklist "$shard" --crawl-id "$CRAWL" --out "$out"; then
			echo "[$MODE $p] worker FAILED — skip"; continue
		fi
	fi

	# 3. load every output into ClickHouse, then mark done (empty parquet = skip, normal)
	ok=1
	for pair in "${OUTPUTS[@]}"; do
		tbl="${pair%%:*}"; pq="${out}-${pair##*:}.parquet"
		[ -s "$pq" ] || { echo "[$MODE $p] $(basename "$pq") empty — skip"; continue; }
		echo "[$MODE $p] loading $(basename "$pq") → $tbl"
		ch --query "INSERT INTO $tbl FROM INFILE '$pq' FORMAT Parquet" || { ok=0; echo "[$MODE $p] load $tbl FAILED"; }
	done
	if [ "$ok" = 1 ]; then
		touch "$marker"; echo "[$MODE $p] DONE"
	else
		echo "[$MODE $p] a load failed — will retry next run"
	fi
done
echo "[$MODE] range $RANGE complete"
