#!/usr/bin/env bash
# Drive CommonCrawl enrichment over many index shards — resumable. Lives in commoncrawl/ because it
# orchestrates all three packages (index-builder worklists + cc-enrich-worker + ClickHouse load).
#
#   cp .env.example .env && edit it                # shared config (auto-loaded below)
#   make -C cc-enrich-worker build                 # build the worker binary once
#   ./run_crawl.sh tech     0-299                  # CPU-bound tech pass  (most cores)
#   ./run_crawl.sh industry 0-299                  # GPU-bound industry pass (the 5090)
#
# Run the two modes as SEPARATE processes (e.g. two tmux panes) so tech pegs the cores
# while industry feeds the 5090. Each is independent and restartable: a shard whose
# <out>.loaded marker exists is skipped; ReplacingMergeTree dedupes any re-load.
# Paths are anchored to this script's directory, so it works from any working directory.
#
# Tunables via env: CRAWL, DATA, BUILDER_DIR,
# WORKER, MAX_PAGES (tech pages/domain, default 25; 0=all), TECH_CONC, IND_CONC, EMBED_CONC.
#
# industry and tech build SEPARATE worklists: industry = 1 representative page/domain (homepage),
# tech = up to MAX_PAGES/domain (homepage + legal/contact pages + shallow) so Wappalyzer + the
# LEI/VAT/profile extractors see the pages that actually carry that data.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # commoncrawl/

# Load the shared config (COMMONCRAWL_EMBED_*, CLICKHOUSE_*, AWS_*) — the single source of truth
# the dagster reference-build also reads, so worker and reference embeddings use the same model.
[ -f "$HERE/.env" ] && { set -a; . "$HERE/.env"; set +a; }

MODE="${1:?usage: run_crawl.sh <industry|tech> <lo-hi>}"
RANGE="${2:?usage: run_crawl.sh <industry|tech> <lo-hi>}"
CRAWL="${CRAWL:-CC-MAIN-2026-25}"
DATA="${DATA:-$HERE/data/crawl}" # commoncrawl/data/crawl (gitignored); never write data inside a code dir
BUILDER_DIR="${BUILDER_DIR:-$HERE/index-builder}" # standalone Python worklist builder
WORKER="${WORKER:-$HERE/cc-enrich-worker/bin/cc-enrich-worker}" # built by `make -C cc-enrich-worker build`

case "$MODE" in
industry) PASS_ARGS=(--concurrency "${IND_CONC:-64}" --embed-concurrency "${EMBED_CONC:-128}") ;;
tech)     PASS_ARGS=(--tech-engine fast --concurrency "${TECH_CONC:-128}") ;;
*) echo "MODE must be industry|tech"; exit 1 ;;
esac

lo="${RANGE%-*}"; hi="${RANGE#*-}"
mkdir -p "$DATA"
DATA_ABS="$(cd "$DATA" && pwd)"
BUILDER_ABS="$(cd "$BUILDER_DIR" && pwd)"

for ((p = lo; p <= hi; p++)); do
	shard="$DATA_ABS/shard_${MODE}_${p}.parquet" # per-mode: industry=1 page/domain, tech=many
	out="$DATA_ABS/out_${MODE}_${p}"             # per-shard output DIR (fixed filenames inside)
	marker="${out}.loaded"
	[ -f "$marker" ] && { echo "[$MODE $p] already loaded — skip"; continue; }

	# 1. worklist (atomic rename; safe if both modes race on the same shard)
	if [ ! -f "$shard" ]; then
		echo "[$MODE $p] generating worklist…"
		tmp="${shard}.tmp.$$"
		if (cd "$BUILDER_ABS" && uv run python -m index_builder --mode "$MODE" \
			--max-pages "${MAX_PAGES:-25}" --crawl "$CRAWL" --part "$p" --out "$tmp"); then
			mv -f "$tmp" "$shard"
		else
			echo "[$MODE $p] worklist FAILED — skip"; rm -f "$tmp"; continue
		fi
	fi

	# 2. run the pass AND load it into ClickHouse in one step (--load; no clickhouse-client).
	#    Fresh output dir each attempt (an explicit --out must be empty); the marker gates redo.
	rm -rf "$out"
	echo "[$MODE $p] $MODE pass + load…"
	if "$WORKER" "$MODE" "${PASS_ARGS[@]}" --worklist "$shard" --crawl-id "$CRAWL" --out "$out" --load; then
		touch "$marker"; echo "[$MODE $p] DONE"
	else
		echo "[$MODE $p] FAILED — will retry next run"
	fi
done
echo "[$MODE] range $RANGE complete"
