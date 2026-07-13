# embedding-ab — instructed vs neutral embedding A/B test

Standalone scripts (NO changes to `cc-enrich-worker` / `cc-crawl` / `index-builder`) to decide whether to
store the industry-**instructed** page embedding or a **neutral** (plain document) one — by testing both on
NACE classification AND general-purpose use (similar-domain, clustering, near-dup, re-classification,
semantic search).

## Why
The worker embeds pages with `Instruct: Classify the business into its industry category\nQuery: {page}`
(Qwen3 query-side instruction). Those vectors are industry-conditioned — collapsed toward the task. For a
stored vector reused for general analysis, a **neutral** (no-instruction) vector may be more useful. These
scripts measure the difference so we don't re-embed ~19M pages twice.

## Scripts
| script | what | GPU/network |
|---|---|---|
| `general_quality.py <emb.parquet> [N]` | anisotropy (mean pairwise cosine), effective rank, NN examples | none (read-only) |
| `intrinsic_purposes.py <emb.parquet> [N]` | similar-domain (top-1 NN tightness), near-dup (high-cosine tail), clustering (k-means silhouette) | none (read-only) |
| `embed_neutral_block.py <instructed.parquet> <out.parquet> [LIMIT]` | re-fetch the SAME pages, extract text, embed PLAIN (neutral), save a parallel parquet **+ the text** | the ONE GPU+fetch spend |
| `nace_ab.py` *(planned)* | classify instructed vs neutral vs the NACE matrix; top-1 / division / margin | none |
| `crosscheck.py` *(planned)* | embed ~200 pages WITH instruction in Python, cosine vs stored instructed → validate text extraction matches the Go worker | tiny |
| `general_ab.py` *(planned)* | anisotropy/rank/NN for both instructed and neutral, side by side | none |

## Env (read from environment; source the processor `.env`)
`COMMONCRAWL_EMBED_BASE_URL`, `COMMONCRAWL_EMBED_MODEL`, `COMMONCRAWL_EMBED_MAX_CHARS`, `AWS_*`, `CLICKHOUSE_*`.

## Setup — this is a `uv` project
```bash
cd corpscout/commoncrawl/embedding-ab
uv sync                 # creates .venv and installs pyarrow / numpy / scikit-learn / requests / boto3 / warcio / bs4
uv add <pkg>            # to add another dependency later
```

## Run (on the box, where the endpoint + S3 + ClickHouse are reachable)
```bash
cd /opt/companycollect/corpscout/commoncrawl/embedding-ab
set -a; . ../cc-processor/.env; set +a  # endpoint + AWS + ClickHouse creds
DATA=../data/embedding/out_industry_68/embeddings.parquet
uv run python general_quality.py    "$DATA"
uv run python intrinsic_purposes.py "$DATA"
# the one GPU+fetch step (produces neutral vectors + text):
uv run python embed_neutral_block.py "$DATA" ../data/embedding_neutral/out_industry_68/embeddings.parquet 50   # validate on 50 first
```

## Findings so far (out_industry_68, instructed embeddings, n=8000)
- **General quality:** anisotropy (mean pairwise cosine) **0.48**, effective rank **~46–138 / 4096** → heavily collapsed toward the industry task.
- **Similar-domain:** median top-1 NN cosine **0.936** (strong); NN are industry-coherent.
- **Near-dup:** **7.2%** of vectors have a neighbour >0.99, 25.7% >0.97 → a separable duplicate tail exists.
- **Clustering:** k=50 silhouette **0.247** ("ok"), balanced clusters.
- **Caveat:** all intrinsic — confirming whether high-cosine neighbours are *truly* similar (vs just
  industry-similar) needs the page **content**, which isn't stored. `embed_neutral_block.py` re-fetches it.
