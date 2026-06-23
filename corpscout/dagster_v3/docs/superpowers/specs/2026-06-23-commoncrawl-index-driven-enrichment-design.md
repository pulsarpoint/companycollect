# CommonCrawl Index-Driven Domain Enrichment — Design

**Status:** design. Supersedes the WET-file-processing approach for the *industry +
homepage signals* product. WET/WARC whole-file processing is shelved as a later
"comprehensive per-page tech/contacts" enrichment (the existing
`2026-06-23-commoncrawl-wet-worker-phase1.md` plan).

## Why this exists (the pivot)

Measured fact: in a WET file, only **~2% of domains have a `/` record; 98% appear only
under sub-paths** (redirect targets, lang prefixes, CMS slugs). So "process WET, filter to
homepages" covers almost nothing, and "process all pages, group by domain" means parsing ~3B
pages + a 2 TB shuffle just to find one page per domain.

CommonCrawl already publishes that grouping as a queryable index: the **columnar URL index**
(`s3://commoncrawl/cc-index/table/cc-main/warc/`, Athena DDL `ccindex`; off-AWS the 300
parquet parts are listed in `crawl-data/CC-MAIN-YYYY-WW/cc-index-table.paths.gz` and readable
over HTTP via DuckDB). It has one row per captured URL with `url_host_registered_domain`,
`url_path`, `fetch_status`, `content_mime_detected`, `content_languages`, `fetch_redirect`,
and the exact WARC location (`warc_filename`, `warc_record_offset`, `warc_record_length`).

So we **query the index to pick one representative page per domain**, then fetch **only those
~40M records** via byte-range reads — no 14 TB WET download, no 3B-page parse, no group-by.

## Architecture

```
1. Worklist query (Athena on AWS / DuckDB over the 300 parquet parts off-AWS)
      one row per domain: (root_domain, url, warc_filename, offset, length, languages)   (~40M)
2. Fetch+classify workers: S3 byte-range GET that ONE WARC record -> warcio -> HTML+headers
      -> text + emails + technologies(headers+body) -> embed -> classify
      -> commoncrawl_domains row (+ commoncrawl_technologies rows for the homepage)
3. Load the per-domain Parquet to ClickHouse.
```

Run **in `us-east-1`** (CommonCrawl's bucket is there — region-local S3 reads are free + fast;
cross-region would cost egress). Embedding stays on the on-prem DGX, reached from the EC2
worker over **Tailscale** (text payloads are tiny); an AWS GPU embedder is a later option.

### The worklist query (shallowest 200-HTML page per domain)
```sql
SELECT root_domain, url, warc_filename, warc_record_offset, warc_record_length, content_languages
FROM (
  SELECT url_host_registered_domain AS root_domain, url, warc_filename,
         warc_record_offset, warc_record_length, content_languages,
         ROW_NUMBER() OVER (
           PARTITION BY url_host_registered_domain
           ORDER BY length(url_path) - length(replace(url_path,'/','')) ASC,  -- fewest segments
                    length(url_path) ASC                                       -- then shortest
         ) AS rn
  FROM ccindex
  WHERE crawl='CC-MAIN-2026-25' AND subset='warc'
    AND fetch_status = 200
    AND content_mime_detected IN ('text/html','application/xhtml+xml')
) WHERE rn = 1
```
`K=1` per domain to start (cheapest, ~40M embeds ≈ 4 days). `K` is a tunable knob (top-K
shallowest → mean-pool) if accuracy needs it. Filters keep only real content pages; a TLD or
`LIMIT` predicate scopes the validation subset.

### Fetching one record
The index gives a WARC location. `s3.get_object(Bucket='commoncrawl', Key=warc_filename,
Range='bytes=offset-(offset+length-1))'` returns the **gzipped WARC record**; `warcio`'s
`ArchiveIterator` over that byte buffer yields the single `response` record → `http_headers` +
HTML body. Off-AWS the same range read works against `https://data.commoncrawl.org/<warc_filename>`.

### Reuse (no new classification logic)
`extract.parse_html`/`extract_emails`, `classifier.PageClassifier` (keyword → embedding
page-type/NACE → LLM tail), `nace_embed`, `tech`/`wappalyzer_client`. They run on the *fetched*
record instead of WET pages. Output rows match the existing migrations: `commoncrawl_domains`
(046) for industry + emails, `commoncrawl_technologies` (047) for the homepage tech.

## Components
- `index_enrich/worklist.py` — build the worklist: DuckDB window query over the index parquet
  parts (off-AWS / on-AWS) → worklist Parquet; the Athena SQL is documented for the AWS path.
- `index_enrich/fetch.py` — `fetch_warc_record(s3_or_http, warc_filename, offset, length) ->
  (html: str, headers: dict)`.
- `index_enrich/classify.py` — `enrich_domain(html, headers, *, root_domain, url, crawl_id,
  classifier, wappalyzer) -> (domain_row, tech_rows)` reusing extract/classifier/tech.
- `index_enrich/worker.py` — read a worklist shard → fetch+enrich each → write Parquet (+ CLI).

## Data flow / error handling
- A fetch failure (404/410/truncated record) → skip that domain, log, continue (don't fail the
  shard). Deterministic output name per shard so retries overwrite.
- Empty/again-parked page → page-type filter routes it to `Unknown`; still one row.
- Embedding endpoint required (DGX over Tailscale); LLM tail optional.

## Validation first (the first runnable step)
Run the worklist query scoped to **one TLD or `LIMIT 10000`**, fetch+classify those domains,
and measure: fetch throughput from EC2, embed rate to the DGX over Tailscale, Parquet size/row,
and classification quality on real fetched pages. Project to ~40M → size the worker fleet and
the embedding window. Same evidence-first pattern as the embedding/page-type work.

## Tradeoffs (honest)
- **~40M byte-range GETs** — cheap + fast on AWS S3 (region-local); off-AWS via the CDN it's
  many small requests (be gentle / parallelize). Strongest reason to run on AWS.
- We extract text from the **WARC HTML** ourselves (vs CommonCrawl's WET text) — fine.
- **K=1** ⇒ industry + *homepage* tech/emails. Comprehensive **per-page** tech/contacts is the
  shelved WARC-streaming pass, not this.
- Embedding (~40M, ~4 days, DGX-bound) is the only heavy step — now controlled, not in the fan-out.
