"""Build the CommonCrawl NACE + page-type reference embeddings into ClickHouse.

NACE categories (read from corpscout.nace_categories) and the committed page-type seed are
embedded via the env-configured endpoint and written to corpscout.nace_category_embeddings /
corpscout.page_type_exemplars — the reference tables the Go cc-enrich-worker loads to classify.
Standalone (no dagster); shares the COMMONCRAWL_EMBED_* endpoint with the worker.
"""

from .build import rebuild_nace, rebuild_page_types
from .embed import EmbeddingClient, division, reference_text

__all__ = ["EmbeddingClient", "division", "reference_text", "rebuild_nace", "rebuild_page_types"]
