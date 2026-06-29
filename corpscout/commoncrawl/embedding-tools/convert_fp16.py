#!/usr/bin/env python3
"""Convert stored fp32 page-embedding parquet(s) to fp16 — halves disk with zero NACE loss (verified in
embedding-ab/precision_ab.py: fp16 == fp32 on 0/20000 top-1 picks).

Per file (independent):
  load_as_fp16(src)     -> (table, fp16_array)   read the fp32 parquet; embedding downcast to a numpy fp16 array
  save_fp16(src, t, a)  -> out_path              write <src_dir>/<src_stem>_fp16.parquet (embedding as fp16)

The conversion is I/O-bound and the total bytes (read fp32 + write fp16) are fixed — but files are
independent, so multiple are converted in PARALLEL (one process each) to use the NVMe's queue depth and
drop wall-clock. Memory ≈ WORKERS × one-file. The fp32 originals are left in place.

  python convert_fp16.py <embeddings.parquet> [more.parquet ...]
  WORKERS=8 python convert_fp16.py data/embedding/out_industry_*/embeddings.parquet     # shell expands the glob
"""
import os
import sys
import concurrent.futures as cf
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def load_as_fp16(src):
    """Read the fp32 parquet at `src` and load its `embedding` column into a contiguous numpy array,
    DOWNCAST to fp16 (shape [rows, dim]). Returns (full pyarrow table, fp16 array)."""
    table = pq.read_table(src)
    ec = table.column("embedding").combine_chunks()          # list<float32> -> single Array
    n = len(ec)
    arr = ec.flatten().to_numpy(zero_copy_only=False).astype(np.float16).reshape(n, -1)
    return table, arr


def save_fp16(src, table, arr):
    """Write <src_dir>/<src_stem>_fp16.parquet: the same table with `embedding` replaced by the fp16 array
    (column order preserved). Returns the output path."""
    n, dim = arr.shape
    values16 = pa.array(arr.reshape(-1), type=pa.float16())
    offsets = pa.array(np.arange(n + 1, dtype=np.int64) * dim, type=pa.int32())  # int32 list offsets
    emb16 = pa.ListArray.from_arrays(offsets, values16)
    i = table.column_names.index("embedding")
    table = table.set_column(i, pa.field("embedding", pa.list_(pa.float16())), pa.chunked_array([emb16]))
    out = os.path.splitext(src)[0] + "_fp16.parquet"
    pq.write_table(table, out, compression="zstd")
    return out


def convert_one(src):
    """Full per-file job: load fp32 -> fp16, save sibling _fp16.parquet. Returns a status line."""
    table, arr = load_as_fp16(src)
    out = save_fp16(src, table, arr)
    s0, s1 = os.path.getsize(src), os.path.getsize(out)
    return f"{out}  {s0/1e6:.0f}MB -> {s1/1e6:.0f}MB ({100*s1/s0:.0f}%)  rows={arr.shape[0]} dim={arr.shape[1]}"


if __name__ == "__main__":
    files = sys.argv[1:]
    if not files:
        sys.exit("usage: convert_fp16.py <embeddings.parquet> [more.parquet ...]   (env WORKERS, default 8)")
    workers = max(1, min(int(os.environ.get("WORKERS", "8")), len(files)))
    if workers == 1:
        for src in files:
            print(convert_one(src), flush=True)
    else:
        with cf.ProcessPoolExecutor(max_workers=workers) as ex:
            for msg in ex.map(convert_one, files):
                print(msg, flush=True)
