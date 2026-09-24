# Embedding A/B — findings

Three questions about the stored CommonCrawl page embeddings, answered on real data. All offline except
one ~5k-page neutral re-embed.

**Setup:** part `out_industry_68` (~83,613 domains), model `qwen3-embedding-8b` (4096-dim), `MAX_CHARS=1000`,
NACE matrix = 1025 codes from `corpscout.nace_category_embeddings` (embedded *plain* / document-side).
The worker embeds pages as **queries**: `Instruct: Classify the business into its industry category\nQuery: {page}`.

---

## 1. Instruction vs neutral — the instruction is load-bearing for NACE, neutral is far more general

Compared the **stored instructed** vectors against **neutral** vectors (same pages re-fetched + embedded
*plain*, no instruction). Sample: 5,000 domains.

**Fairness check (extraction):** python-extracted-instructed vs stored-instructed cosine **0.992** (median
0.997). The re-fetch's text matches the Go worker's, so the differences below are the *instruction*, not
the pipeline.

**NACE classification (instructed wins):**
| metric | instructed | neutral |
|---|---|---|
| top-1 code agreement | — | 39.0% |
| division agreement | — | 55.9% |
| top-3 overlap (jaccard) | — | 46.5% |
| mean top-1 score | **0.685** | 0.453 |
| mean margin (top1−top2) | **0.021** | 0.015 |

Neutral picks a different top NACE 61% of the time, with much lower scores + flatter margins. Fundamental:
the NACE matrix is stored plain, so instructed = **query↔doc** (Qwen3's trained retrieval setup → sharp),
neutral = **doc↔doc** (mushier). Dropping the instruction clearly degrades NACE.

**General quality (neutral wins):**
| metric | instructed | neutral | better |
|---|---|---|---|
| anisotropy (mean pairwise cosine) | 0.484 | **0.316** | lower = more spread |
| effective rank (entropy) /4096 | 135 | **477** (~3.5×) | higher = more spread |
| near-dup separation (median NN cosine) | 0.930 | **0.650** | clearer gap to >0.99 dups |
| clustering silhouette (k=50) | **0.248** | 0.098 | instructed clusters by industry |

Neutral uses ~3.5× more of the space and separates near-duplicates far better — the genuinely
general-purpose vector. Instructed only wins clustering *because* it's industry-collapsed.

**Conclusion:** no single vector serves both. Options: (1) **two embeddings** — instructed at ingest for
NACE, neutral stored for general use (2× embed GPU); (2) store **instructed only** (good for industry
similarity, weak for near-dup / other taxonomies / search); (3) store **neutral only** (great general
store, relies on the NACE result already saved in ClickHouse at ingest). Purposes leaning on the
industry-orthogonal uses (near-dup, re-classify, semantic search) favor option 1.

---

## 2. General quality of the *existing* (instructed) embeddings — usable, but specialized

Intrinsic, no content needed (n=8,000):
- **anisotropy 0.48**, effective rank **~46–138 / 4096** → heavily collapsed toward the industry task.
- **Similar-domain: strong** — median top-1 NN cosine **0.936**; NN are industry-coherent (architecture
  firms cluster with architecture firms, aesthetic-medicine with aesthetic-medicine, etc.).
- **Near-dup: usable** — **7.2%** have a neighbour >0.99 (a separable duplicate tail).
- **Clustering: moderate** — k=50 silhouette **0.247**.

So the existing vectors are good for *industry*-flavored general use, weaker for anything orthogonal to
industry — consistent with §1.

---

## 3. Precision — fp16 is lossless for NACE, int8 near-lossless

Round-tripped the stored fp32 vectors through fp16 / per-vector int8 and re-classified (n=20,000):
| format | top-1 agreement vs fp32 | changed | self-cosine | margin | bytes/domain | full crawl (19M) |
|---|---|---|---|---|---|---|
| fp32 | (baseline) | — | — | 0.0238 | 16 KB | 304 GB |
| **fp16** | **100.000%** | 0 / 20000 | 1.000000 | 0.0238 | 8 KB | **152 GB** |
| **int8** | 99.685% | 63 / 20000 | 0.999884 | 0.0238 | 4 KB | **76 GB** |

**fp16 is completely lossless** for NACE (zero changes, identical margin) → half the storage for free.
**int8** flips only 0.3% — and since the margin is unchanged, those are near-ties, not degradation →
4× storage for a negligible reshuffle. Format-agnostic: holds for neutral vectors too.

**Plan:** keep storing **raw fp32** (never recompute the expensive artifact), then **bulk-convert offline**
to fp16 or int8 after processing — a CPU-only pass. int8 needs an extra per-vector `scale` (float32) column
for dequant; fp16 needs nothing.

---

## Scripts (this folder)
`embed_neutral_block.py` (the one GPU step), `crosscheck.py`, `nace_ab.py`, `general_ab.py`,
`general_quality.py`, `intrinsic_purposes.py`, `precision_ab.py`. Run via the `uv` project (`uv sync`).
