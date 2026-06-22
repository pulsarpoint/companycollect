#!/usr/bin/env python
"""Mine structural page-type exemplars (parked / for-sale / directory_listing /
default_server / under_construction) from WET plaintext.

The classify path embeds WET plaintext, so page-type prototypes must come from the
SAME modality (not HTML). This is the high-precision SEED phase; recall is recovered
downstream by embedding-NN expansion. Detection logic lives in
`commoncrawl_enrich.page_types`; this is a thin CLI over it.

Run:
    uv run python scripts/mine_parked.py --scan 60000 --out data/commoncrawl/parked_exemplars.parquet
"""
import argparse
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from commoncrawl_enrich import page_types, segment


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source", default=None, help="WET path/URL (default: first WET of latest crawl)")
    ap.add_argument("--segment-index", type=int, default=0)
    ap.add_argument("--scan", type=int, default=40000, help="max WET records to scan")
    ap.add_argument("--max-exemplars", type=int, default=4000)
    ap.add_argument("--out", default="data/commoncrawl/parked_exemplars.parquet")
    args = ap.parse_args()

    source = args.source or segment.first_wet_url(segment_index=args.segment_index)
    print(f"mining page-type exemplars from {source}")
    result = page_types.scan_wet(source, scan_limit=args.scan, max_exemplars=args.max_exemplars)
    rows = result["exemplars"]
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), args.out)

    dist = Counter(r["signal"] for r in rows)
    print(f"exemplars: {len(rows)} (deduped by domain) -> {args.out}")
    print(f"signal distribution: {dict(dist)}")
    for r in rows[:8]:
        snippet = " ".join(r["text"].split())[:90]
        print(f"  [{r['signal']:18}] {r['root_domain'][:28]:28} {snippet}")


if __name__ == "__main__":
    main()
