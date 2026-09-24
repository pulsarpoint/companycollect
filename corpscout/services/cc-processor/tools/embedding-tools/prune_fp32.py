#!/usr/bin/env python3
"""Delete fp32 embedding parquet(s) whose fp16 sibling is present, valid, and row-count-matching — i.e.
only where conversion actually FINISHED. Protects against a partial/corrupt fp16 (e.g. a killed convert
run): an fp32 is never removed unless its sibling `embeddings_fp16.parquet` opens AND has the same row
count. Reads only parquet footers (no data), so it's fast and memory-tiny.

Dry-run by default — pass --apply to actually delete.

  python prune_fp32.py <fp32.parquet> [more ...]                       # dry run: list what WOULD be removed
  find ../data/ -name embeddings.parquet | xargs python prune_fp32.py            # dry run
  find ../data/ -name embeddings.parquet | xargs python prune_fp32.py --apply    # delete
"""
import os
import sys
import pyarrow.parquet as pq

apply = "--apply" in sys.argv
files = [a for a in sys.argv[1:] if a != "--apply"]

removed = kept = 0
for f32 in files:
    stem = os.path.splitext(f32)[0]
    f16 = (stem[:-5] if stem.endswith("_fp32") else stem) + "_fp16.parquet"  # same mapping as convert_fp16
    if not os.path.exists(f16):
        print(f"SKIP  no fp16 yet        {f32}")
        kept += 1
        continue
    try:
        n16 = pq.read_metadata(f16).num_rows
        n32 = pq.read_metadata(f32).num_rows
    except Exception as e:
        print(f"SKIP  fp16 unreadable    {f16}  ({type(e).__name__})")
        kept += 1
        continue
    if n16 == 0 or n16 != n32:
        print(f"SKIP  rows {n32} != {n16}    {f32}")
        kept += 1
        continue
    if apply:
        os.remove(f32)
        print(f"removed  {f32}  ({n16} rows verified in fp16)")
    else:
        print(f"would remove  {f32}  ({n16} rows ok)")
    removed += 1

verb = "removed" if apply else "would remove"
tail = "done" if apply else "DRY RUN — re-run with --apply to delete"
print(f"\n{verb} {removed}, kept {kept}.  {tail}")
