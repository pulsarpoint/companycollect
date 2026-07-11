# cc-crawl — orchestrator contract

`cc-crawl` is the small Go driver that preserves the original per-part processing and load lifecycle while
`cc-enrich-worker` reads raw records from local-network RustFS instead of Common Crawl S3.

## Stable external CLI

Existing commands remain valid:

```bash
cc-crawl -mode industry -parts 0-299 -crawl CC-MAIN-2026-25
cc-crawl -mode tech -tech-conc 32 -parts 5 -crawl CC-MAIN-2026-25
```

Required flags remain `-mode`, `-parts`, and `-crawl`. The operational flags `-base`, `-worker`,
`-max-pages`, `-ind-conc`, `-embed-conc`, `-tech-conc`, and `-tech-chunk` remain. `-builder-dir` is accepted
as a deprecated compatibility flag but is no longer used because worklist generation belongs to
`cc-download-worker`.

`-max-pages N` derives RustFS selection `pagesN`. The default remains 25 and must match the selection
staged by the downloader.

## Per-part state machine

For each requested part `P`:

1. If local `out_<mode>_<P>.loaded` exists, skip the whole part before invoking the worker or reading RustFS.
2. Remove a stale local output directory.
3. Run produce:

   ```text
   cc-enrich-worker <mode> <processing flags> \
     --crawl-id <crawl> --selection pagesN --part P --out out_<mode>_<P>
   ```

4. Continue only when produce exits 0 and writes `domains.parquet`; otherwise skip the loader.
5. Run `cc-enrich-worker load --dir out_<mode>_<P>`.
6. Only after a successful load, create the empty local `out_<mode>_<P>.loaded` marker.

This is deliberately the same produce → verify → load → local marker flow used before the RustFS input
change. Local Parquet remains inspectable and reloadable. The orchestrator does not use RustFS processor
markers, leases, or distributed ownership because operators assign non-overlapping part ranges to machines.

Embed mode retains its existing exception: it runs one produce command, never loads ClickHouse, and treats
a valid `embeddings.parquet` or `embeddings_fp16.parquet` as completion.

## Boundaries

- `cc-download-worker` uses AWS credentials to build selections and download source WARC ranges.
- `cc-enrich-worker` uses `CORPSCOUT_S3_*` to read ready raw packs from RustFS.
- `cc-crawl` only checks local state and executes the worker; it performs no S3, RustFS, Parquet, or
  ClickHouse operations itself.
- `cc-enrich-worker load` is the only result-writing ClickHouse boundary.

## Logging

Structured JSON logs go to stdout and
`<base>/<crawl>/crawl/logs/crawl_<mode>_<lo>-<hi>_<timestamp>.log`. Each external command is logged with
its exact arguments and exit code. Each part ends as done, skipped, or failed, and the run logs aggregate
counts.

Example successful events:

```json
{"level":"INFO","msg":"produce.run","mode":"tech","part":5,"cmd":"... --selection pages25 --part 5 ..."}
{"level":"INFO","msg":"produce.exit","mode":"tech","part":5,"exit":0,"produced":102804}
{"level":"INFO","msg":"load.exit","mode":"tech","part":5,"exit":0,"rows_to_clickhouse":408019}
{"level":"INFO","msg":"done","mode":"tech","part":5,"produced":102804,"rows_to_clickhouse":408019}
```

Exit codes determine success. Parsed counts are informational only.
