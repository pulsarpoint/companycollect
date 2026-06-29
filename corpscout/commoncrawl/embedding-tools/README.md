# embedding-tools

Offline utilities for the stored CommonCrawl page-embedding parquet files. Standalone `uv` project — no
changes to `cc-enrich-worker` / `cc-crawl`. (The A/B *experiments* live in `../embedding-ab`; this is the
production-side tooling.)

## convert_fp16.py — fp32 → fp16
Halves the embedding files with **zero NACE loss** (proven in `embedding-ab/precision_ab.py`: fp16 matched
fp32 on 0/20000 top-1 picks). Reads `embeddings.parquet`, narrows the `embedding` column from
`list<float32>` to `list<float16>`, and writes `embeddings_fp16.parquet` **beside** the original (all other
columns unchanged; the fp32 file is left in place).

Per file:
- `load_as_fp16(src) -> (table, fp16_array)` — read the **fp32** parquet; embedding downcast to a numpy fp16 array.
- `save_fp16(src, table, arr) -> out_path` — write `<src_stem>_fp16.parquet` in the same folder.
- `convert_one(src)` — the full job for one file.

Multiple files are converted **in parallel** (one process each — `WORKERS`, default 8). The total I/O is
fixed, but independent files use the NVMe queue depth so wall-clock drops. Memory ≈ `WORKERS` × one file
(~2–4 GB each), so lower `WORKERS` if RAM is tight.

## Setup + run
```bash
cd corpscout/commoncrawl/embedding-tools
uv sync                       # pyarrow + numpy

# one file:
uv run python convert_fp16.py /path/to/embeddings.parquet
# whole tree (shell expands the glob):
uv run python convert_fp16.py /path/to/data/embedding/out_industry_*/embeddings.parquet
```
On the box, `data/` is root-owned — run with `sudo` (and tune `WORKERS`):
```bash
sudo WORKERS=8 .venv/bin/python convert_fp16.py data/embedding/out_industry_*/embeddings.parquet
```
Each file prints `MB -> MB (%)`. After verifying, delete the fp32 originals to reclaim disk (the script
never deletes anything itself). 300 chunks of fp32 ≈ 600 GB → fp16 ≈ 300 GB.

## Compatibility
The fp16 `embedding` column is parquet `HALF_FLOAT` — read it back with pyarrow/numpy (the re-classification
path). It is **not** loaded into ClickHouse (embeddings never were). Older DuckDB builds may not read
`HALF_FLOAT`; use pyarrow if you need to inspect.
