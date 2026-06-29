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

Multiple files are converted **in parallel** (one process each — `WORKERS`, default 8). The total I/O is
fixed, but independent files use the NVMe queue depth so wall-clock drops. Memory ≈ `WORKERS` × one file,
so lower `WORKERS` if RAM is tight.

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

Then spot-check a couple of `_fp16` outputs and reclaim disk by deleting the fp32 originals:
```bash
find ../data/ -type f -name embeddings.parquet -delete     # ONLY after verifying the _fp16 files
```

## Compatibility
The fp16 `embedding` column is parquet `HALF_FLOAT` — read it back with pyarrow/numpy (the re-classification
path). It is **not** loaded into ClickHouse (embeddings never were). Older DuckDB builds may not read
`HALF_FLOAT`; use pyarrow if you need to inspect.
