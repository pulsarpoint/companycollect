"""OpenAI-compatible embedding client for the CommonCrawl reference matrices.

A focused slice of the old commoncrawl_enrich.nace_embed: just what's needed to turn NACE
category text + page-type seeds into L2-normalized document vectors. Reference vectors are
embedded plain (no instruction prefix); the Go worker adds the query-side instruction to
pages. The endpoint is env-driven (COMMONCRAWL_EMBED_*) and shared with the worker, so the
reference vectors and the page vectors come from the same model.
"""
import os
import re

import numpy as np

_EMBED_BATCH = 128
_EMBED_MAX_CHARS = 6000  # keep input under the model's token limit


def division(code: str) -> str:
    """2-digit NACE division from a possibly-messy code ('63.12'->'63', 'C15.20'->'15')."""
    s = (code or "").strip()
    m = re.search(r"\d{2}", s)
    return m.group(0) if m else s[:1].upper()


def reference_text(row, variant: str = "hier") -> str:
    """(code, leaf, section_desc, parent_desc) -> text to embed. 'hier' prepends the
    section/parent path (section > parent > leaf); 'bare' is just the leaf description."""
    _code, leaf, section_desc, parent_desc = row
    if variant == "bare":
        return leaf
    path = [p for p in (section_desc, parent_desc) if p and p != leaf]
    return " > ".join([*path, leaf]) if path else leaf


def _normalize(a: np.ndarray) -> np.ndarray:
    return a / (np.linalg.norm(a, axis=1, keepdims=True) + 1e-9)


class EmbeddingClient:
    """OpenAI-compatible embeddings endpoint (e.g. vLLM serving Qwen3-Embedding)."""

    def __init__(self, *, base_url, api_key="x", model=None, batch=_EMBED_BATCH,
                 timeout=120, max_chars=None):
        from openai import OpenAI

        self._client = OpenAI(base_url=base_url, api_key=api_key, timeout=timeout)
        self._model = model or self._client.models.list().data[0].id
        self._batch = batch
        self._max_chars = max_chars or int(
            os.environ.get("COMMONCRAWL_EMBED_MAX_CHARS", _EMBED_MAX_CHARS))

    @classmethod
    def from_env(cls, *, batch=_EMBED_BATCH, timeout=120) -> "EmbeddingClient":
        """Build from COMMONCRAWL_EMBED_BASE_URL (+ optional COMMONCRAWL_EMBED_MODEL)."""
        base = os.environ.get("COMMONCRAWL_EMBED_BASE_URL")
        if not base:
            raise RuntimeError("COMMONCRAWL_EMBED_BASE_URL is not set")
        return cls(base_url=base, api_key=os.environ.get("COMMONCRAWL_EMBED_API_KEY", "x"),
                   model=os.environ.get("COMMONCRAWL_EMBED_MODEL") or None,
                   batch=batch, timeout=timeout)

    @property
    def model(self) -> str:
        return self._model

    def embed(self, texts, instruction=None) -> np.ndarray:
        texts = [t[:self._max_chars] for t in texts]
        if instruction:
            texts = [f"Instruct: {instruction}\nQuery: {t}" for t in texts]
        out = []
        for i in range(0, len(texts), self._batch):
            chunk = texts[i:i + self._batch]
            out.extend(d.embedding for d in self._client.embeddings.create(
                model=self._model, input=chunk).data)
        return _normalize(np.asarray(out, dtype="float32"))
