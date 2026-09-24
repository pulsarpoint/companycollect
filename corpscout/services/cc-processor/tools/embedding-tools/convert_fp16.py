#!/usr/bin/env python3
"""Convert stored fp32 page-embedding parquet(s) to fp16 — halves disk with zero NACE loss (verified in
embedding-ab/precision_ab.py: fp16 == fp32 on 0/20000 top-1 picks).

convert_one(src): read the fp32 parquet, downcast the `embedding` column in place (list<float32> ->
list<float16> — pyarrow casts the child and reuses the offsets; every other column untouched), and write
the sibling fp16 file (`embeddings.parquet` -> `embeddings_fp16.parquet`). The fp32 original is left in place.

The job is I/O-bound and files are independent, so multiple are converted in PARALLEL (one process each,
`WORKERS`, default 8) to use the NVMe queue depth. Memory ≈ WORKERS × one file.

  python convert_fp16.py <embeddings.parquet> [more ...]
  WORKERS=8 python convert_fp16.py data/embedding/out_industry_*/embeddings.parquet   # shell expands the glob
"""
import os
import sys
import concurrent.futures as cf
import pyarrow as pa
import pyarrow.parquet as pq


def convert_one(src):
    """Downcast one fp32 embeddings parquet to fp16; write the sibling _fp16.parquet. Returns a status line."""
    t = pq.read_table(src)
    i = t.column_names.index("embedding")
    emb16 = t.column(i).cast(pa.list_(pa.float16()), safe=False)  # safe=False: a stray edge value can't abort the batch
    t = t.set_column(i, "embedding", emb16)
    stem = os.path.splitext(src)[0]
    out = stem + "_fp16.parquet"  # embeddings.parquet -> embeddings_fp16.parquet
    pq.write_table(t, out, compression="zstd", version="2.6")      # 2.6: parquet format that carries HALF_FLOAT
    s0, s1 = os.path.getsize(src), os.path.getsize(out)
    return f"{out}  {s0/1e6:.0f}MB -> {s1/1e6:.0f}MB ({100*s1/s0:.0f}%)  rows={t.num_rows}"


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
