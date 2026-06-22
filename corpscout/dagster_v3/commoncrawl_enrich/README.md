# commoncrawl_enrich (Phase 0 spike)

Standalone, single-process domain enrichment over CommonCrawl. No Dagster/Temporal/ClickHouse.

## Run a ~10k spike
1. Export a manifest from open_page_rank (Parquet/CSV with `root_domain, source_rank, open_page_rank`).
2. Set the LLM env (local or hosted OpenAI-compatible endpoint):
   - `COMMONCRAWL_LLM_BASE_URL`, `COMMONCRAWL_LLM_MODEL`, `COMMONCRAWL_LLM_API_KEY` (optional).
3. Run:
   ```bash
   uv run python -m commoncrawl_enrich.run \
     --manifest top10k.parquet --out ./cc_out --limit 10000 --max-workers 16
   ```
4. Inspect `./cc_out/metrics.json` (hit-rate, regex-vs-LLM uplift, speed + projected 100k/10M hours)
   and the 5 Parquet tables.

## Switching local -> hosted LLM
Change only the three `COMMONCRAWL_LLM_*` env vars. Thinking mode is on by default
(`from_openai(enable_thinking=True)`).

## Notes
- Index resolution uses the CC columnar Parquet via DuckDB httpfs (anonymous S3); `index_client.resolve_via_cdx` is the per-domain fallback.
- Technology detection is a built-in fingerprint set (`tech.py`); swap in full Wappalyzer behind `detect_technologies` later.
