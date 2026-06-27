# cc-crawl — orchestrator design plan

Replace `run_crawl.sh` with a small **standalone Go binary** that drives the per-part crawl loop with
proper, auditable logging. Bash is awkward for the structured logging + per-chunk status we want.

## Goals

- Per part, log **each external command we run, with its full parameters, and its exit code** — so the
  log proves *what ran, with which args, and how it exited*. Status is driven by exit codes, never guessed.
- A clean per-part **outcome line** answering: skipped? and if run — how many **domains were fetched
  from S3** and how many **rows were stored in ClickHouse**.
- Outcome + command/exit lines go to **stdout AND a log file** under `data/logs/`. The subprocesses'
  own verbose output keeps streaming to stdout/stderr **as it is now**.
- Resumable, same `.loaded` marker semantics.

## Prerequisite change to `cc-enrich-worker` (the one edit we make to it)

**Remove the `--load` auto-loader from the pass commands** (`industry`/`tech`/`both`): the pass now
*only* produces the `out_<mode>_<p>/` parquet folder; it no longer touches ClickHouse. Loading is
**exclusively** the existing `load --dir` subcommand.

Why: cc-crawl runs the worker **twice** per chunk — produce, then load — as two separately-checked
steps, so **a bad/partial produce never gets loaded**. Coupling produce+load behind one `--load`
flag hides which half failed and can load a half-built folder.

- main.go: drop the `--load` flag and the post-pass `load.FromDir(outDir)` block. Keep the `load`
  subcommand untouched. Update help text + any test referencing `--load`. `make vet build test` green.

## What cc-crawl orchestrates — per part `p` in `[lo, hi]`

1. **skip** if `data/crawl/out_<mode>_<p>.loaded` exists → `SKIP`.
2. **worklist** — if `shard_<mode>_<p>.parquet` missing → run `index_builder` (atomic tmp+rename).
   Log the command + exit; on failure → `FAILED`, next part.
3. **produce (exec #1)** — `cc-enrich-worker <mode> <pass-args> --worklist <shard> --crawl-id <CRAWL>
   --out <out>` (**no `--load`**). Log the command + exit + domains parsed from its output.
   **Gate:** proceed only if exit==0 **and** `<out>/domains.parquet` exists; else `FAILED`, **skip the
   loader**, next part.
4. **load (exec #2)** — `cc-enrich-worker load --dir <out>`. Log the command + exit + rows parsed.
   On failure → `FAILED` (no marker), next part.
5. both ok → `touch <out>.loaded` → `DONE`.

So each chunk = **two worker executions**, each with its own logged command + exit code, and the
loader is gated on a good produce.

## CLI

Proper `flag`-package parsing (gives `-h`/usage; every setting is a flag with an env default):

```
cc-crawl -mode industry -parts 0-299 -crawl CC-MAIN-2026-25
cc-crawl -mode tech     -parts 5     -crawl CC-MAIN-2026-25     # bare N = single part
```

**Required:** `-mode` (industry|tech), `-parts` (lo-hi or N), `-crawl` (no default — must be given,
or via env `CRAWL`). Optional, each a flag defaulting from the same-named env var:
`-data` (`data/crawl`), `-builder-dir` (`index-builder`), `-worker`
(`cc-enrich-worker/bin/cc-enrich-worker`), `-max-pages` (25), `-ind-conc` (64), `-embed-conc` (128),
`-tech-conc` (128). Run from `commoncrawl/` (defaults are relative to there).

## Logging / monitoring

Structured **JSON** logs via the stdlib **`log/slog`** (`slog.NewJSONHandler`) — written to **stdout
AND** a per-run file `data/logs/crawl_<mode>_<lo>-<hi>_<ts>.log`. Each part logger carries
`mode`/`part` context (`lg.With(...)`).

- Each external command is one `*.run` event with the **exact command line**, then a `*.exit` event
  with the **exit code** + elapsed — the log proves what ran and how it exited. `failed` is logged at
  `ERROR` level; status is driven by the **exit code**, never the count parse.
- The subprocesses' own verbose output still streams to stdout as text; cc-crawl additionally captures
  the worker's output (separate buffers, no `os/exec` race) to fill `domains_from_s3` /
  `rows_to_clickhouse`.

### Per-part events (JSON)

```json
{"level":"INFO","msg":"produce.run","mode":"industry","part":5,"cmd":"cc-enrich-worker industry --concurrency 64 --embed-concurrency 128 --worklist …/shard_industry_5.parquet --crawl-id CC-MAIN-2026-25 --out …/out_industry_5"}
{"level":"INFO","msg":"produce.exit","mode":"industry","part":5,"exit":0,"domains_from_s3":102804,"elapsed":"19m2s"}
{"level":"INFO","msg":"load.run","mode":"industry","part":5,"cmd":"cc-enrich-worker load --dir …/out_industry_5"}
{"level":"INFO","msg":"load.exit","mode":"industry","part":5,"exit":0,"rows_to_clickhouse":408019,"elapsed":"45s"}
{"level":"INFO","msg":"done","mode":"industry","part":5,"domains_from_s3":102804,"rows_to_clickhouse":408019}
```
- skip → `{"msg":"skip","reason":"already loaded",…}`
- produce failure (loader skipped) → `{"level":"ERROR","msg":"failed","step":"produce","exit":1,"loader":"skipped",…}`
- end of run → `{"msg":"complete","done":7,"skipped":2,"failed":1}`

### Count parsing

- `domains_from_s3` ← **produce** output: `done:\s+(\d+)\s+domains`.
- `rows_to_clickhouse` ← **load** output: sum of `loaded\s+(\d+)\s+rows`.

A parse miss shows `0`; it never changes the outcome (that's the exit code).

## File layout

```
commoncrawl/cc-crawl/
  go.mod          # module cc-crawl, go 1.24, stdlib only
  main.go         # arg/env parse, per-part loop, exec+log helpers, count parsing
  Makefile        # build -> bin/cc-crawl
```

## Migration (now)

- Make the cc-enrich-worker `--load` removal + add `cc-crawl`.
- **Remove `run_crawl.sh` now** (replaced).
- Update `commoncrawl/` run docs to `cc-crawl`.

## Verification

- cc-enrich-worker `make vet build test` green after `--load` removal; cc-crawl `go build`/`go vet` clean.
- On the box, run a tiny range hitting all paths: a `.loaded` part (SKIP), a fresh part (two execs,
  DONE with non-zero S3/DB counts), and a forced produce failure (→ FAILED, loader skipped, no marker).
- Confirm the log file == the stdout outcome/exec/exit lines; worker detail still on stdout.
