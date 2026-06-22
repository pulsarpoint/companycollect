#!/usr/bin/env python
"""Calibrate the page-type detector across several WET files.

Mines page-type exemplars + real-content negatives from N WET segments, splits the
exemplars into TRAIN prototypes and HELD-OUT positives, then sweeps the similarity
threshold reporting:
  - recall on HELD-OUT page-type pages (generalisation, not memorising training text)
  - false-positive rate on real-content homepages
  - per-class held-out recall

Confirms the page-type logic before it goes into the pipeline. Writes the merged
exemplars + a production PrototypeSet (built from ALL exemplars) for reuse.

Run (embedding model must be up on :8000):
    export EMBED_BASE_URL=http://100.77.62.33:8000/v1
    uv run python scripts/calibrate_page_types.py --segments 5 --scan 30000 --content-cap 300
"""
import argparse
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from commoncrawl_enrich import nace_embed as ne
from commoncrawl_enrich import page_types, segment


def mine_segments(n_segments: int, scan: int, content_cap: int) -> tuple[list[dict], list[dict]]:
    crawl = segment.latest_crawl()
    exemplars: list[dict] = []
    content: list[dict] = []
    seen_ex: set[str] = set()
    seen_ct: set[str] = set()
    for i in range(n_segments):
        url = segment.first_wet_url(crawl=crawl, segment_index=i)
        print(f"[{i + 1}/{n_segments}] scanning {url.rsplit('/', 1)[-1]}")
        res = page_types.scan_wet(url, scan_limit=scan, content_cap=content_cap)
        for r in res["exemplars"]:
            if r["root_domain"] not in seen_ex:
                seen_ex.add(r["root_domain"]); exemplars.append(r)
        for r in res["content"]:
            if r["root_domain"] not in seen_ct:
                seen_ct.add(r["root_domain"]); content.append(r)
        print(f"    +{len(res['exemplars'])} exemplars, +{len(res['content'])} content "
              f"(totals: {len(exemplars)} / {len(content)})")
    return exemplars, content


def split_train_test(exemplars: list[dict], every: int = 3) -> tuple[list[dict], list[dict]]:
    """Deterministic per-class split: every `every`-th exemplar -> held-out test."""
    by_class: dict[str, list[dict]] = defaultdict(list)
    for r in sorted(exemplars, key=lambda r: (r["signal"], r["root_domain"])):
        by_class[r["signal"]].append(r)
    train, test = [], []
    for rows in by_class.values():
        for idx, r in enumerate(rows):
            (test if idx % every == 0 else train).append(r)
    return train, test


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--segments", type=int, default=5)
    ap.add_argument("--scan", type=int, default=30000)
    ap.add_argument("--content-cap", type=int, default=300)
    ap.add_argument("--exemplars-out", default="data/commoncrawl/parked_exemplars.parquet")
    ap.add_argument("--content-out", default="data/commoncrawl/page_type_content.parquet")
    ap.add_argument("--prototypes-out", default="data/commoncrawl/page_type_prototypes.npz")
    args = ap.parse_args()

    # reuse cached mine (exemplars + content) so iterating the embedding/threshold side
    # doesn't re-download 5 WET files
    if Path(args.exemplars_out).exists() and Path(args.content_out).exists():
        exemplars = pq.read_table(args.exemplars_out).to_pylist()
        content = pq.read_table(args.content_out).to_pylist()
        print(f"using cached mine: {len(exemplars)} exemplars, {len(content)} content")
    else:
        exemplars, content = mine_segments(args.segments, args.scan, args.content_cap)
        Path(args.exemplars_out).parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(exemplars), args.exemplars_out)
        pq.write_table(pa.Table.from_pylist(content), args.content_out)
    if len(exemplars) < 10:
        print(f"too few exemplars ({len(exemplars)}) to calibrate"); return
    print(f"\nmined {len(exemplars)} exemplars  {dict(Counter(r['signal'] for r in exemplars))}")
    print(f"mined {len(content)} real-content negatives")

    cl = ne.EmbeddingClient(base_url="http://100.77.62.33:8000/v1")
    train, test = split_train_test(exemplars)
    print(f"split: {len(train)} train prototypes, {len(test)} held-out positives")

    protos = ne.build_prototype_set("", cl, rows=train)
    # held-out positives and content negatives are PAGES at inference -> instruction prefix
    pos = cl.embed([r["text"] for r in test], instruction=ne.PAGE_INSTRUCTION)
    neg = cl.embed([r["text"] for r in content], instruction=ne.PAGE_INSTRUCTION)
    pos_score, pos_label = protos.best(pos)
    neg_score, _ = protos.best(neg)
    test_labels = [r["signal"] for r in test]

    print(f"\nheld-out positive max-sim: mean={pos_score.mean():.3f} "
          f"p10={np.percentile(pos_score, 10):.3f} p50={np.percentile(pos_score, 50):.3f}")
    print(f"content negative max-sim:  mean={neg_score.mean():.3f} "
          f"p90={np.percentile(neg_score, 90):.3f} max={neg_score.max():.3f}")
    print("\nthreshold   held-out recall    content FP")
    for thr in (0.45, 0.50, 0.55, 0.60, 0.65, 0.70):
        recall = float((pos_score >= thr).mean())
        fp = float((neg_score >= thr).mean())
        print(f"  {thr:.2f}      {recall:6.0%} ({int((pos_score>=thr).sum())}/{len(pos_score)})"
              f"        {fp:5.1%} ({int((neg_score>=thr).sum())}/{len(neg_score)})")

    thr = ne.DEFAULT_PAGE_TYPE_THRESHOLD
    print(f"\nper-class held-out recall @ {thr}:")
    by = defaultdict(lambda: [0, 0])
    for s, lbl in zip(pos_score, test_labels):
        by[lbl][1] += 1; by[lbl][0] += int(s >= thr)
    for lbl, (hit, tot) in sorted(by.items()):
        print(f"  {lbl:18} {hit}/{tot} ({hit / tot:.0%})")

    # production prototypes from ALL exemplars
    ne.build_prototype_set("", cl, rows=exemplars).save(args.prototypes_out)
    print(f"\nsaved production prototypes ({len(exemplars)}) -> {args.prototypes_out}")


if __name__ == "__main__":
    main()
