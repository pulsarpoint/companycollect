# embedding-tools

Offline utilities for the stored CommonCrawl page-embedding parquet files. Standalone `uv` project — no
changes to `cc-enrich-worker` / `cc-crawl`. (The A/B *experiments* live in `../embedding-ab`; this is the
production-side tooling.)

## convert_fp16.py — fp32 → fp16
Halves the embedding files with **zero NACE loss** (proven in `embedding-ab/precision_ab.py`: fp16 matched
fp32 on 0/20000 top-1 picks). Reads `embeddings_fp32.parquet`, narrows the `embedding` column from
`list<float32>` to `list<float16>`, and writes `embeddings_fp16.parquet` **beside** the original (all other
columns unchanged; the fp32 file is left in place).

`convert_one(src)` reads the fp32 parquet and downcasts the `embedding` column in place — a pyarrow
`cast(list<float16>, safe=False)` narrows the child floats and reuses the offsets, so no numpy/flatten and
every other column is untouched — then writes the sibling `_fp16.parquet` (parquet `version="2.6"`, which
carries the HALF_FLOAT type).

Multiple files are converted **in parallel** (one process each — `WORKERS`, default 8). Independent files
use the NVMe queue depth so wall-clock drops. **Memory is the real limit, not cores:** each worker reads the
whole decompressed file + the fp16 copy + zstd write buffers, and pyarrow's pool doesn't return freed memory
to the OS — so a ~1.6 GB file peaks at **~3–9 GB resident per worker**. Size `WORKERS` to RAM: on the **30 GB
box use `WORKERS=2`** (≈18 GB peak); the default 8 only fits a big-RAM host.

## Setup + run
```bash
cd corpscout/commoncrawl/embedding-tools
uv sync                       # pyarrow

# one file:
uv run python convert_fp16.py /path/to/embeddings_fp32.parquet
# whole tree (shell expands the glob):
uv run python convert_fp16.py /path/to/data/embedding/out_industry_*/embeddings_fp32.parquet
```
On the box, `data/` is root-owned — run with `sudo` (and tune `WORKERS`):
```bash
sudo WORKERS=8 .venv/bin/python convert_fp16.py data/embedding/out_industry_*/embeddings_fp32.parquet
```
Each file prints `MB -> MB (%)`. After verifying, delete the fp32 originals to reclaim disk (the script
never deletes anything itself). 300 chunks of fp32 ≈ 600 GB → fp16 ≈ 300 GB.

### Bulk-convert a whole tree (`find` | `xargs`)
To convert every embedding parquet under `data/embedding/`, hand the **whole list to one invocation** and
let the `WORKERS` pool fan out — do **not** wrap it in a bash `for` loop (that runs them one at a time and
throws away the parallelism):

```bash
# from embedding-tools/ (root on the box, venv active). Match the legacy name embeddings.parquet;
# for files from the updated worker, use embeddings_fp32.parquet instead.
find ../data/ -type f -name embeddings.parquet -print0 | WORKERS=8 xargs -0 python ./convert_fp16.py
```

`xargs` appends every path to a single `python ./convert_fp16.py file1 file2 …` call, and the
`ProcessPoolExecutor(max_workers=WORKERS)` processes **all** of them — 8 in flight at a time. As each worker
finishes a file it pulls the next off the queue, so e.g. 150 files → 8-at-a-time → *all* converted (not
"first 8 only"). Notes: put `WORKERS=` **before** `xargs` so the child `python` inherits it; `-print0`/`-0`
is space-safe; if the list ever exceeds the shell's arg limit `xargs` just splits it into a few batches,
each still 8-at-a-time.

Then **verify** the outputs and **prune** the fp32 with the two scripts below — never a blind `rm`, which
would delete an fp32 whose fp16 is partial/corrupt.

## verify_fp16.py — confirm a conversion is correct
For each `embeddings.parquet` it checks the `embeddings_fp16.parquet` sibling: it opens, the `embedding`
column is float16, the **row count matches** the fp32, and a sampled vector round-trips (max abs diff
~6e-5, cosine ~1.0). Footers + a small sample only → fast, memory-tiny, serial. Reports `OK` /
`MISSING` (not converted yet) / `BAD` (exists but wrong — e.g. a partial write from a killed run).
```bash
find ../data/ -type f -name embeddings.parquet -print0 | xargs -0 python ./verify_fp16.py
# per-file lines, then a summary:   OK 11   MISSING(not converted) 101   BAD 0
```

## prune_fp32.py — delete fp32 only where conversion finished
Removes an `embeddings.parquet` **only** if its `embeddings_fp16.parquet` opens **and** has the same row
count (footers only — no data read). A partial/corrupt fp16 → its fp32 is **kept**, so it can't lose data.
**Dry-run by default**; `--apply` to delete (`sudo` for root-owned `data/`).
```bash
# dry run — lists what WOULD be removed:
find ../data/ -type f -name embeddings.parquet -print0 | xargs -0 python ./prune_fp32.py
# apply:
find ../data/ -type f -name embeddings.parquet -print0 | xargs -0 sudo .venv/bin/python ./prune_fp32.py --apply
```

## Procedure — convert → verify → prune (resumable, no redo)
`prune_fp32.py` deletes a finished file's fp32, so a **done** dir holds only `_fp16.parquet` and a
**not-done** dir holds only `embeddings.parquet`. Re-running the convert over `embeddings.parquet`
therefore **skips the done ones automatically** (their fp32 is gone) — no redo, no flag, `convert_fp16.py`
unchanged. The loop:

1. **Convert** the remaining fp32 (size `WORKERS` to RAM — `WORKERS=2` on the 30 GB box):
   ```bash
   find ../data/ -type f -name embeddings.parquet -print0 | WORKERS=2 xargs -0 python ./convert_fp16.py
   ```
2. **Verify** — expect `BAD 0`:
   ```bash
   find ../data/ -type f -name embeddings.parquet -print0 | xargs -0 python ./verify_fp16.py
   ```
3. **If any BAD** (a killed run leaves a partial `_fp16.parquet`): delete those fp16, then step 1 redoes them:
   ```bash
   find ../data/ -name embeddings.parquet -print0 | xargs -0 python ./verify_fp16.py 2>&1 \
     | grep -a '^BAD' | grep -oE 'data/embedding/[^ ]*_fp16[.]parquet' | xargs -r sudo rm -v
   ```
4. **Prune** the verified fp32 to reclaim disk:
   ```bash
   find ../data/ -type f -name embeddings.parquet -print0 | xargs -0 sudo .venv/bin/python ./prune_fp32.py --apply
   ```
5. Repeat from step 1 until `verify` shows `MISSING 0`. Safe to kill mid-convert and resume — the partial it
   leaves is flagged `BAD` by step 2 and cleaned by step 3.

## Compatibility
The fp16 `embedding` column is parquet `HALF_FLOAT` — read it back with pyarrow/numpy (the re-classification
path). It is **not** loaded into ClickHouse (embeddings never were). Older DuckDB builds may not read
`HALF_FLOAT`; use pyarrow if you need to inspect.
