"""CommonCrawl page classification reference data.

Builds and stores the reference data the WET classifier loads into RAM:
- `nace_category_embeddings` — the NACE industry reference matrix (embedded once).
- `page_type_exemplars` — structural page-type prototypes (parked / for_sale /
  directory_listing / default_server / under_construction), built from a committed
  seed (`seeds/page_type_exemplars.jsonl`) for reproducible builds.

Both are exported to ClickHouse (`corpscout`, migrations 000044/000045) and loaded
by the classifier via `commoncrawl_enrich.nace_embed`. The embedding endpoint is
env-driven (COMMONCRAWL_EMBED_BASE_URL); the live WET-mining expansion path adds
more exemplars on top of the committed seed.
"""
